use crate::{
    App, Error, Result,
    calls::Report,
    id::{CallId, ContextNodeId, SessionId},
    store::{self, Db},
    tools,
};
use futures_util::{StreamExt, TryStreamExt};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, sync::OnceLock, time::Duration};
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
    pub call_id: CallId,
    pub model: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub effort: Option<String>,
    pub end: i64,
    pub head: Option<ContextNodeId>,
}
#[derive(Default)]
pub struct Output {
    pub report: Report,
    pub answer: bool,
    pub final_item: Option<Value>,
    pub final_output_index: Option<u32>,
    pub tools: bool,
    pub tasks: JoinSet<Result<()>>,
    pub current_head: Option<ContextNodeId>,
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
const INSTRUCTIONS: &str = concat!(
    "You are running in MyGo-Bot, a session-based agent runtime operating on a cluster of peer nodes whose environment interface is `codemode`.\n",
    "There is no implicit or privileged local host: every filesystem, process, HTTP, MCP, and MyGo API call targets an explicit node via `cluster.node(target)`.\n\n",
    "Discovery & node selection:\n",
    "- Call `await cluster.nodes()` in the same `codemode` script as your first task step (rather than spending a separate probe-only turn) to inspect each node's `id`, `name`, `description`, `os`, `arch`, and `online` status. Do not assume working directory, installed tools, session settings, or MCP integrations on any node.\n",
    "- Choose nodes intentionally: if only one online node fits (or the user already named a node for ongoing work), use it directly; if multiple candidate nodes could apply and intent is ambiguous—especially before side-effecting or destructive actions—use node descriptions to decide or ask the user. If node names collide (`ENODEAMBIGUOUS`), target by `id`.\n",
    "- Check `await node.mcp.servers()` on the target node when external tools or data may help (nodes can have both cluster-shared and node-local MCP servers); inspect schemas on demand via `await node.mcp.tools(server, ...)`.\n",
    "- Inspect or manage a node's MyGo-Bot instance via `await node.mygo.request(...)` (read `/openapi.json` for routes and schemas).\n\n",
    "Execution:\n",
    "- Compose related steps—including concurrent cross-node work (`Promise.all`) and direct cross-node streaming (e.g. `await dst.fs.write(dstPath, await src.fs.read(srcPath, { stream: true }))`)—in a single `codemode` script and return only the distilled result. Offload heavy CPU/memory work to `node.proc.exec`.\n",
    "- Adapt commands, executable names, and path conventions to each target node's reported `os` and `arch` (e.g. on Windows nodes `.bat`/`.cmd` require `cmd.exe`).\n",
    "- Keep `background: false` by default. Never set `background: true` if your next step is simply `codemode_wait({})`—foreground mode finishes fast work (<90s) in one step and automatically detaches longer tasks without losing work. Use `background: true` only when you have immediate independent work to run concurrently or are starting a persistent listener/service (keep services running in the foreground of a background task rather than detaching with `&`).\n",
    "- Never poll in loops with `codemode` or `codemode_task_status`; wait with `codemode_wait({})` or end your turn to hand control back (future task events and completions automatically wake an idle session).\n\n",
    "Verification & accuracy:\n",
    "- An interrupted, timed-out, or disconnected remote call (`ENODEUNREACH` or unknown outcome) may already have taken effect on the target node; verify state on that node before repeating side-effecting actions.\n",
    "- Compute aggregations and unit conversions inside `codemode` with consistent units instead of doing mental arithmetic in the final reply.\n",
    "- Before recommending destructive or maintenance shell commands to the user, verify unfamiliar subcommands read-only on the intended node (e.g. checking tool availability or `--help`) rather than guessing flags."
);

async fn body(db: Db, sid: SessionId, input: &Input) -> Result<reqwest::Body> {
    static STATIC_TOOLS_AND_DEV: OnceLock<Vec<u8>> = OnceLock::new();
    let tools_and_dev = STATIC_TOOLS_AND_DEV.get_or_init(|| {
        let tools = serde_json::to_string(&tools::definitions()).expect("serialize tool definitions");
        let dev = json!({"role":"developer","content":[{"type":"input_text","text":INSTRUCTIONS}]})
            .to_string();
        format!(r#","tools":{tools},"stream":true,"store":false,"input":[{dev}"#).into_bytes()
    });
    let mut prefix = Vec::with_capacity(tools_and_dev.len() + 128);
    prefix.extend_from_slice(b"{\"model\":");
    serde_json::to_writer(&mut prefix, &input.model)
        .map_err(|e| Error::internal(format!("encode model name: {e}")))?;
    if let Some(e) = &input.effort {
        prefix.extend_from_slice(b",\"reasoning\":{\"effort\":");
        serde_json::to_writer(&mut prefix, e)
            .map_err(|err| Error::internal(format!("encode effort: {err}")))?;
        prefix.push(b'}');
    }
    prefix.extend_from_slice(tools_and_dev);
    crate::media::model_input(db, sid, input.end, input.head, prefix).await
}
#[derive(Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    item: Option<Value>,
    output_index: Option<u32>,
    delta: Option<String>,
    message: Option<String>,
    response: Option<ResponseMetadata>,
}

struct DeltaBatcher<'a> {
    streams: &'a crate::events::Hub,
    sid: SessionId,
    call_id: CallId,
    key: Option<(u32, &'static str)>,
    buf: String,
    flush_at: Option<Instant>,
}

impl<'a> DeltaBatcher<'a> {
    fn new(streams: &'a crate::events::Hub, sid: SessionId, call_id: CallId) -> Self {
        Self {
            streams,
            sid,
            call_id,
            key: None,
            buf: String::new(),
            flush_at: None,
        }
    }

    fn push(&mut self, output_index: u32, field: &'static str, delta: &str) {
        if delta.is_empty() {
            return;
        }
        if self.key.is_some_and(|k| k != (output_index, field)) {
            self.flush();
        }
        if self.key.is_none() {
            self.key = Some((output_index, field));
            self.flush_at = Some(Instant::now() + Duration::from_millis(30));
        }
        self.buf.push_str(delta);
        if self.buf.len() >= 256 {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if let Some((idx, field)) = self.key.take() {
            self.flush_at = None;
            if !self.buf.is_empty() {
                let delta = std::mem::take(&mut self.buf);
                self.streams
                    .append_delta(self.sid, self.call_id, idx, field, &delta);
            }
        }
    }
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
        sid: SessionId,
        input: Input,
        out: &mut Output,
        hard: &CancellationToken,
        run_gen: u64,
    ) -> std::result::Result<(), Failure> {
        out.current_head = input.head;
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
        let mut batcher = DeltaBatcher::new(&self.streams, sid, input.call_id);
        loop {
            let flush_at = batcher.flush_at;
            let line = tokio::select! {
                biased;
                _ = async {
                    match flush_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    batcher.flush();
                    continue;
                }
                line = timeout_at(deadline, lines.next()) => {
                    line.map_err(|_| Failure::Transient("model stream idle timeout".into()))?
                        .ok_or_else(|| Failure::Transient("incomplete model stream".into()))?
                        .map_err(|e| Failure::Transient(e.to_string()))?
                }
            };
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
                "response.completed" => {
                    batcher.flush();
                    return Ok(());
                }
                "response.failed" | "response.incomplete" | "error" => {
                    batcher.flush();
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
                "response.output_item.added" => {
                    batcher.flush();
                    if let Some(item) = frame.item {
                        let idx = frame.output_index.unwrap_or(0);
                        let kind = item["type"].as_str().unwrap_or("message").to_owned();
                        let name = item["name"].as_str().map(str::to_owned);
                        let call_id_str = item["call_id"].as_str().map(str::to_owned);
                        self.streams
                            .item_started(sid, input.call_id, idx, kind, name, call_id_str);
                    }
                }
                "response.output_text.delta" | "response.refusal.delta" => {
                    if let Some(delta) = frame.delta.as_deref() {
                        batcher.push(frame.output_index.unwrap_or(0), "text", delta);
                    }
                }
                "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                    if let Some(delta) = frame.delta.as_deref() {
                        batcher.push(frame.output_index.unwrap_or(0), "reasoning", delta);
                    }
                }
                "response.function_call_arguments.delta" => {
                    if let Some(delta) = frame.delta.as_deref() {
                        batcher.push(frame.output_index.unwrap_or(0), "arguments", delta);
                    }
                }
                "response.output_item.done" => {
                    batcher.flush();
                    let idx = frame.output_index.unwrap_or(0);
                    let item = frame.item.ok_or_else(|| fatal("missing output item"))?;
                    if !out.tools
                        && has_answer(&item)
                        && let Some(mut terminal) = completed_in_buffer(lines.read_buffer())
                    {
                        terminal.observe(&mut out.report);
                        out.answer = true;
                        out.final_item = Some(item);
                        out.final_output_index = Some(idx);
                        return Ok(());
                    }
                    self.accept(sid, input.call_id, item, out, hard, run_gen)
                        .await?;
                    self.streams.finish_item(sid, input.call_id, idx);
                }
                _ => (),
            }
            data.clear();
            deadline = Instant::now() + IDLE;
        }
    }

    async fn accept(
        &self,
        sid: SessionId,
        llm_call_id: CallId,
        item: Value,
        out: &mut Output,
        hard: &CancellationToken,
        run_gen: u64,
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
            let expected_head = out.current_head;
            let call_key = call.clone();
            let id = self
                .db
                .write_for_run(
                    sid,
                    run_gen,
                    move |conn| {
                        let cur_head: Option<ContextNodeId> = conn
                            .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
                            .query_row([sid], |r| r.get(0))?;
                        if cur_head != expected_head {
                            return Err(Error::conflict("session branch changed during execution"));
                        }
                        let sql =
                            "SELECT payload FROM items WHERE session_id=? AND kind='function_call'
                           AND json_extract(payload,'$.call_id')=? LIMIT 1";
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
                        let id = store::add_item(
                            conn,
                            sid,
                            "function_call",
                            &item,
                            None,
                            Some(llm_call_id),
                        )?;
                        Ok(Some(id))
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
            if hard.is_cancelled() {
                return Ok(());
            }
            let app = self.clone();
            let hard = hard.clone();
            out.tasks.spawn(async move {
                let call = crate::codemode::Call { sid, id, call };
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
                    | "codemode_event"
            ) {
                return Err(Error::internal("unexpected reserved output item type"));
            }
            kind
        }
        .to_owned();
        let expected_head = out.current_head;
        let (id, next_head) = self
            .db
            .write_for_run(
                sid,
                run_gen,
                move |conn| {
                    store::add_context_item_at(
                        conn,
                        sid,
                        &kind,
                        &item,
                        llm_call_id,
                        Some(expected_head),
                    )
                },
                move |s, (id, next_head)| {
                    s.item_saved(sid, id)?;
                    Ok((id, next_head))
                },
            )
            .await?;
        let _ = id;
        out.current_head = next_head;
        Ok(())
    }
}
