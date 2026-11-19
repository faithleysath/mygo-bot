use crate::{
    App, Error, Result,
    api::Config,
    events::Notice,
    store::{self, Db},
};
use chrono::{Local, SecondsFormat};
use futures_util::{StreamExt, TryStreamExt};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use std::{io, time::Duration};
use tokio::{
    sync::broadcast,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
};

pub struct Input {
    pub config: Config,
    pub effort: Option<String>,
    pub end: i64,
}
#[derive(Default)]
pub struct Output {
    pub answer: bool,
    pub final_item: Option<Value>,
    pub tools: bool,
    pub tasks: JoinSet<Result<()>>,
}
pub enum Failure {
    Transient(String),
    Fatal(Error),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Fatal(e)
    }
}
fn fatal(msg: impl Into<String>) -> Failure {
    Failure::Fatal(Error::internal(msg))
}
const IDLE: Duration = Duration::from_secs(60);
const FRAME_LIMIT: usize = 16 * 1024 * 1024;

async fn body(db: Db, sid: i64, input: &Input) -> Result<reqwest::Body> {
    let tool = json!({"type":"function","name":"current_time","description":"Read the current local time, including the UTC offset.","parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false}});
    let mut header = json!({"model":input.config.model,"instructions":input.config.instructions,"tools":[tool],"stream":true,"store":false});
    if let Some(e) = &input.effort {
        header["reasoning"] = json!({"effort":e});
    }
    let mut prefix = header.to_string().into_bytes();
    prefix.pop();
    prefix.extend_from_slice(b",\"input\":[");
    store::JsonRows::ModelInput
        .body(db, sid, 0, input.end, prefix, b"]}")
        .await
}
#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    delta: String,
    item: Option<Value>,
    message: Option<String>,
    // Terminal events can repeat the entire output. Only error handling needs fields from it.
    #[serde(borrow)]
    response: Option<&'a RawValue>,
}

// Peek only at a complete terminal event already delivered to the decoder.
// Fragmented, multiline or unfamiliar event layouts use the regular parser.
fn completed_in_buffer(buffer: &[u8]) -> bool {
    let mut lines = buffer.split_inclusive(|&byte| byte == b'\n');
    let Some(mut line) = lines.next() else {
        return false;
    };
    if line.starts_with(b"event:") {
        if line.len() - 1 > FRAME_LIMIT || std::str::from_utf8(line).is_err() {
            return false;
        }
        let Some(next) = lines.next() else {
            return false;
        };
        line = next;
    }
    let Some(data) = line.strip_prefix(b"data:") else {
        return false;
    };
    if !matches!(lines.next(), Some(b"\n" | b"\r\n")) || line.len() - 1 > FRAME_LIMIT {
        return false;
    }
    serde_json::from_slice::<Frame>(data).is_ok_and(|f| f.kind == "response.completed")
}

fn has_answer(item: &Value) -> bool {
    item["type"] == "message"
        && item["role"] == "assistant"
        && item["content"].as_array().is_some_and(|parts| {
            parts
                .iter()
                .any(|p| matches!(p["type"].as_str(), Some("output_text" | "refusal")))
        })
}

impl App {
    pub async fn stream(
        &self,
        sid: i64,
        input: Input,
        attempt: u64,
        channel: &broadcast::Sender<Notice>,
        out: &mut Output,
    ) -> std::result::Result<(), Failure> {
        let body = body(self.db.clone(), sid, &input).await?;
        let mut request = self
            .client
            .post(format!(
                "{}/responses",
                input.config.base_url.trim_end_matches('/')
            ))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .body(body);
        if !input.config.api_key.is_empty() {
            request = request.bearer_auth(input.config.api_key);
        }
        let res = timeout(Duration::from_secs(30), request.send())
            .await
            .map_err(|_| Failure::Transient("model response timeout".into()))?
            .map_err(|e| Failure::Transient(e.without_url().to_string()))?;
        let status = res.status();
        if !status.is_success() {
            let reason = format!("model HTTP {status}");
            return Err(
                if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
                    Failure::Transient(reason)
                } else {
                    fatal(reason)
                },
            );
        }
        if !res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|s| s.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            return Err(fatal("expected a Responses event stream"));
        }
        let bytes = res.bytes_stream().map_err(io::Error::other);
        let mut lines = FramedRead::new(
            StreamReader::new(bytes),
            LinesCodec::new_with_max_length(FRAME_LIMIT),
        );
        let mut data = String::new();
        let mut deadline = Instant::now() + IDLE;
        loop {
            let line = timeout_at(deadline, lines.next())
                .await
                .map_err(|_| Failure::Transient("model stream idle timeout".into()))?
                .ok_or_else(|| Failure::Transient("incomplete model stream".into()))?
                .map_err(|e| Failure::Transient(e.to_string()))?;
            if line.starts_with(':') {
                deadline = Instant::now() + IDLE;
                continue;
            }
            if let Some(field) = line.strip_prefix("data:") {
                if data.len() + field.len() + 1 > FRAME_LIMIT {
                    return Err(fatal("model event exceeds size limit"));
                }
                data.push_str(field.strip_prefix(' ').unwrap_or(field));
                data.push('\n');
                continue;
            }
            if !line.is_empty() || data.is_empty() {
                continue;
            }
            if data.trim_end() == "[DONE]" {
                return Err(Failure::Transient("missing response.completed".into()));
            }
            let frame: Frame<'_> = serde_json::from_str(&data)
                .map_err(|e| fatal(format!("invalid model event: {e}")))?;
            match frame.kind.as_str() {
                "response.completed" => return Ok(()),
                "response.failed" | "response.incomplete" | "error" => {
                    let response = frame
                        .response
                        .and_then(|raw| serde_json::from_str::<Value>(raw.get()).ok());
                    let detail = response
                        .as_ref()
                        .and_then(|r| r.pointer("/error/message"))
                        .and_then(Value::as_str)
                        .or(frame.message.as_deref())
                        .unwrap_or("model request unsuccessful");
                    return Err(fatal(format!("{}: {detail}", frame.kind)));
                }
                "response.output_text.delta" => {
                    if channel.receiver_count() > 0 {
                        Notice::publish(
                            channel,
                            "text.delta",
                            json!({"attempt":attempt,"text":frame.delta}),
                        );
                    }
                }
                "response.output_item.done" => {
                    let item = frame.item.ok_or_else(|| fatal("missing output item"))?;
                    if !out.tools && has_answer(&item) && completed_in_buffer(lines.read_buffer()) {
                        out.answer = true;
                        out.final_item = Some(item);
                        return Ok(());
                    }
                    self.accept(sid, item, out).await?
                }
                _ => (),
            }
            data.clear();
            deadline = Instant::now() + IDLE;
        }
    }

    async fn accept(&self, sid: i64, item: Value, out: &mut Output) -> Result<()> {
        let kind = item["type"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::internal("missing output item type"))?;
        if kind == "function_call" {
            let call = item["call_id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::internal("missing call_id"))?
                .to_owned();
            let name = item["name"].as_str().unwrap_or_default().to_owned();
            let args = item["arguments"].as_str().unwrap_or_default().to_owned();
            let call_key = call.clone();
            let id = self
                .db
                .call(move |s| {
                    let sql =
                        "SELECT payload FROM items WHERE session_id=? AND kind='function_call'
                           AND json_extract(payload,'$.call_id')=?";
                    let existing: Option<Value> = s
                        .conn
                        .query_row(sql, (sid, call_key), |r| r.get(0))
                        .optional()?;
                    if let Some(saved) = existing {
                        if saved["name"] != item["name"] || saved["arguments"] != item["arguments"]
                        {
                            return Err(Error::internal(
                                "model reused call_id for a different tool call",
                            ));
                        }
                        return Ok(None);
                    }
                    let id = store::add_item(&s.conn, sid, "function_call", &item, None)?;
                    s.item_saved(sid, id)?;
                    Ok(Some(id))
                })
                .await?;
            out.tools = true;
            let Some(id) = id else {
                return Ok(());
            };
            let db = self.db.clone();
            out.tasks.spawn(async move {
                let valid_args = serde_json::from_str::<serde_json::Map<String, Value>>(&args)
                    .is_ok_and(|o| o.is_empty());
                let output = if name == "current_time" && valid_args {
                    Local::now().to_rfc3339_opts(SecondsFormat::Secs, false)
                } else {
                    "Tool error: expected current_time with empty arguments {}.".to_owned()
                };
                let payload = json!({"type":"function_call_output","call_id":call,"output":output});
                db.call(move |s| {
                    let id =
                        store::add_item(&s.conn, sid, "function_call_output", &payload, Some(id))?;
                    s.item_saved(sid, id)?;
                    Ok(())
                })
                .await
            });
            return Ok(());
        }
        let kind = if kind == "message" && item["role"] == "assistant" {
            out.answer |= has_answer(&item);
            "assistant_message"
        } else {
            if matches!(
                kind,
                "user_message" | "withdrawal" | "assistant_message" | "function_call_output"
            ) {
                return Err(Error::internal("unexpected reserved output item type"));
            }
            kind
        }
        .to_owned();
        self.db
            .call(move |s| {
                let tx = s.conn.transaction()?;
                let id = store::add_context_item(&tx, sid, &kind, &item)?;
                tx.commit()?;
                s.item_saved(sid, id)?;
                Ok(())
            })
            .await
    }
}
