use crate::{
    App, Error, Result,
    calls::Report,
    store::{self, Db},
    tools,
};
use futures_util::{StreamExt, TryStreamExt};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::{
    codec::{FramedRead, LinesCodec},
    io::StreamReader,
    sync::CancellationToken,
};

pub struct Input {
    pub call_id: i64,
    pub model: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub effort: Option<String>,
    pub end: i64,
}
#[derive(Default)]
pub struct Output {
    pub report: Report,
    pub answer: bool,
    pub final_item: Option<Value>,
    pub tools: bool,
    pub tasks: JoinSet<Result<()>>,
}
pub enum Failure {
    Cancelled,
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
    let mut header =
        json!({"model":input.model,"tools":tools::definitions(),"stream":true,"store":false});
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
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    item: Option<Value>,
    message: Option<String>,
    response: Option<ResponseMetadata>,
}

// Responses terminal events may repeat the whole model output. Deserialize only billing metadata.
#[derive(Deserialize)]
struct ResponseMetadata {
    id: Option<String>,
    usage: Option<Value>,
    error: Option<Value>,
}
impl Frame {
    fn observe(&mut self, report: &mut Report) {
        if let Some(r) = &mut self.response {
            if let Some(id) = r.id.take() {
                report.response_id = Some(id);
            }
            if let Some(usage) = r.usage.take() {
                report.usage = Some(usage);
            }
        }
    }
}

// Fragmented or multiline events use the regular parser.
fn completed_in_buffer(buffer: &[u8]) -> Option<Frame> {
    let mut lines = buffer.split_inclusive(|&byte| byte == b'\n');
    let mut line = lines.next()?;
    if line.starts_with(b"event:") {
        if line.len() - 1 > FRAME_LIMIT || std::str::from_utf8(line).is_err() {
            return None;
        }
        line = lines.next()?;
    }
    let data = line.strip_prefix(b"data:")?;
    if !matches!(lines.next(), Some(b"\n" | b"\r\n")) || line.len() - 1 > FRAME_LIMIT {
        return None;
    }
    serde_json::from_slice::<Frame>(data)
        .ok()
        .filter(|f| f.kind == "response.completed")
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
        out: &mut Output,
        hard: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
        let body = body(self.db.clone(), sid, &input).await?;
        let mut request = self
            .client
            .post(format!(
                "{}/responses",
                input.base_url.trim_end_matches('/')
            ))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .body(body);
        if let Some(key) = input.api_key {
            request = request.bearer_auth(key);
        }
        let res = timeout(Duration::from_secs(30), request.send())
            .await
            .map_err(|_| Failure::Transient("model response timeout".into()))?
            .map_err(|e| Failure::Transient(e.without_url().to_string()))?;
        let status = res.status();
        out.report.http_status = Some(status.as_u16());
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
            let mut frame: Frame = serde_json::from_str(&data)
                .map_err(|e| fatal(format!("invalid model event: {e}")))?;
            frame.observe(&mut out.report);
            match frame.kind.as_str() {
                "response.completed" => return Ok(()),
                "response.failed" | "response.incomplete" | "error" => {
                    let detail = frame
                        .response
                        .as_ref()
                        .and_then(|r| r.error.as_ref())
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .or(frame.message.as_deref())
                        .unwrap_or("model request unsuccessful");
                    return Err(fatal(format!("{}: {detail}", frame.kind)));
                }
                "response.output_item.done" => {
                    let item = frame.item.ok_or_else(|| fatal("missing output item"))?;
                    if !out.tools
                        && has_answer(&item)
                        && let Some(mut terminal) = completed_in_buffer(lines.read_buffer())
                    {
                        terminal.observe(&mut out.report);
                        out.answer = true;
                        out.final_item = Some(item);
                        return Ok(());
                    }
                    self.accept(sid, input.call_id, item, out, hard).await?
                }
                _ => (),
            }
            data.clear();
            deadline = Instant::now() + IDLE;
        }
    }

    async fn accept(
        &self,
        sid: i64,
        llm_call_id: i64,
        item: Value,
        out: &mut Output,
        hard: &CancellationToken,
    ) -> Result<()> {
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
                .write(
                    move |conn| {
                        let sql =
                            "SELECT payload FROM items WHERE session_id=? AND kind='function_call'
                           AND json_extract(payload,'$.call_id')=?";
                        let existing: Option<Value> = conn
                            .query_row(sql, (sid, call_key), |r| r.get(0))
                            .optional()?;
                        if let Some(saved) = existing {
                            if saved["name"] != item["name"]
                                || saved["arguments"] != item["arguments"]
                            {
                                return Err(Error::internal(
                                    "model reused call_id for a different tool call",
                                ));
                            }
                            return Ok(None);
                        }
                        Ok(Some(store::add_item(
                            conn,
                            sid,
                            "function_call",
                            &item,
                            None,
                            Some(llm_call_id),
                        )?))
                    },
                    move |s, id| {
                        if let Some(id) = id {
                            s.item_saved(sid, id)?;
                        }
                        Ok(id)
                    },
                )
                .await?;
            out.tools = true;
            let Some(id) = id else {
                return Ok(());
            };
            let app = self.clone();
            let hard = hard.clone();
            out.tasks.spawn(async move {
                let call = crate::sandbox::Call { sid, id, call };
                tools::execute(app, call, &name, &args, hard).await
            });
            return Ok(());
        }
        let kind = if kind == "message" && item["role"] == "assistant" {
            out.answer |= has_answer(&item);
            "assistant_message"
        } else {
            if matches!(
                kind,
                "user_message"
                    | "withdrawal"
                    | "assistant_message"
                    | "function_call_output"
                    | "sandbox_event"
            ) {
                return Err(Error::internal("unexpected reserved output item type"));
            }
            kind
        }
        .to_owned();
        self.db
            .call(move |s| {
                let tx = s.conn.transaction()?;
                let id = store::add_context_item(&tx, sid, &kind, &item, llm_call_id)?;
                tx.commit()?;
                s.item_saved(sid, id)?;
                Ok(())
            })
            .await
    }
}
