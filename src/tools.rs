use crate::{
    App, Result,
    codemode::{self, Call},
    id::ItemId,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

fn definition(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({"type":"function", "name":name, "description":description, "strict":false,
        "parameters":{"type":"object", "properties":properties, "required":required, "additionalProperties":false}})
}

pub fn definitions() -> Value {
    let id = json!({"type":"string", "description":"Task ID from this session (cm_...)."});
    json!([
        definition(
            "codemode",
            concat!(
                "Execute JavaScript as an async function body in a fresh QuickJS runtime. Top-level await and return work.\n\n",
                "Runtime:\n",
                "- 96 MiB RAM, 256 KiB stack; no fetch, URL, Buffer or setTimeout; use built-in APIs, TextEncoder, TextDecoder, Date, console and await sleep(ms).\n",
                "- At most 16 concurrent host calls or open streams per task (32 I/O slots globally); use proc.exec for heavy CPU or memory work.\n",
                "- Native file and HTTP APIs work on devices without shell utilities.\n\n",
                "Files (fs):\n",
                "- fs.read/write handle regular files only: await fs.read(path) for bytes, {encoding:'utf8'} for text, or {stream:true} for an async iterable of Uint8Array chunks.\n",
                "- await fs.write(path,textOrBytesOrStream,{append}) returns bytes written. Example: await fs.write(dst,await fs.read(src,{stream:true})).\n",
                "- for await (const {name,type} of fs.readDir(path)) iterates one directory. await fs.stat(path,{followSymlinks}) returns {type,size,mtimeMs}; types are file, directory, symlink or other.\n",
                "- await fs.mkdir(path,{recursive}), fs.rename(from,to), fs.remove(path,{recursive}). File errors have a code such as ENOENT, EACCES or EXDEV.\n\n",
                "HTTP (http):\n",
                "- await http.request({url,method,headers,body,timeout_ms,stream}) (string header values, text/Uint8Array/stream body, no auto-redirects) returns status, headers and body.\n",
                "- A buffered response has byte body and synchronous text()/json(). stream:true returns headers early and a single-use async byte iterable. Buffered reads and bodies are limited to 8 MiB.\n",
                "- Use for await on streams; break closes them, or call await body.return() to discard an unread response.\n",
                "- Use new TextDecoder().decode(chunk,{stream:true}) with the same decoder for each chunk, then decoder.decode() to flush.\n\n",
                "Processes (proc):\n",
                "- await proc.exec(program,args,{cwd,env,stdin,timeout_ms}) runs an executable directly (no shell; text/Uint8Array stdin",
                cfg_select! {
                    windows => { "; Windows .bat/.cmd require cmd.exe" }
                    _ => { "" }
                },
                "), kills its process group on exit, and returns {exit_code,signal,timed_out,stdout,stderr,truncated} (1 MiB cap per stream; on timeout, kills the process group and returns timed_out: true with captured output).\n\n",
                "MCP (mcp):\n",
                "- await mcp.servers() lists enabled MCP servers by name and description. await mcp.tools(server,{query?,cursor?}) returns {tools,nextCursor} with names, descriptions and inputSchema; pass nextCursor as cursor to continue.\n",
                "- await mcp.call(server,tool,args) calls by exact names with an object and returns {content,structuredContent?,isError}. Check isError for tool failures.\n",
                "- mcp.resources(server,{cursor?}), mcp.resourceTemplates(server,{cursor?}) and mcp.prompts(server,{cursor?}) return one page under resources, resourceTemplates or prompts plus nextCursor.\n",
                "- await mcp.readResource(server,uri) returns {contents:[{uri,mimeType?,content}]}. Expand a template's uriTemplate into a concrete URI first.\n",
                "- await mcp.getPrompt(server,name,args?) accepts string argument values and returns {description?,messages:[{role,content}]}.\n",
                "- MCP text and image/audio/video blocks are ready to return or emit. Binary data is Uint8Array. Other resource types give {type:'blob',mime_type,data} for script use. Save up to 8 MiB with fs.write(path,block.data); larger blobs can stream via await fs.write(path,(async function*(){yield block.data})()).\n",
                "- A resource_link has a uri to read explicitly; an embedded resource has resource.content. Return just the blocks or JSON fields you select. Example: return (await mcp.getPrompt(server,name)).messages[0].content.\n\n",
                "Tasks & MyGo (task, mygo):\n",
                "- await task.list() lists active tasks in this session; await task.status(id) returns the full live snapshot or final summary (calls share a 100ms throttle).\n",
                "- mygo.sessionId and mygo.taskId identify the current session and task. await mygo.request({path,method,headers,body,timeout_ms,stream}) calls MyGo's own HTTP API in-process (no host or token needed; plain objects/JSON bodies serialize with application/json; stream:true is required for SSE endpoints such as /sessions/{sid}/events).\n",
                "- Read /openapi.json via mygo.request for all routes and schemas (sessions, messages, run/pause, history, context, events, providers, models, config, mcp, calls, attachments).\n\n",
                "Output & events:\n",
                "- Return text, JSON-serializable values, or one or more content blocks (task output budget: 52 MiB). Wrap binary data in a media block. Console output (latest 256 KiB) accompanies the result.\n",
                "- await emitEvent(textOrBlocks) queues a separate event in this session (rate-limited: burst 8, 1/s refill, max 128 queued messages per session; nonempty).\n",
                "- Blocks: {type:'text',text} (nonempty), {type:'image'|'audio'|'video',attachment_id}, or {type:'image'|'audio'|'video',mime_type,data:Uint8Array} (up to 50 MiB, when supported by the session model)."
            ),
            json!({
                "code": {"type":"string", "description":"Async JavaScript function body (nonempty, up to 256 KiB)."},
                "background": {"type":"boolean", "description":"Default false waits up to 90s in the foreground, then automatically detaches to the background without losing work. Never set true if your next step is simply codemode_wait({}); set true only for immediate concurrent work or a persistent listener/service."},
                "label": {"type":"string", "description":"Optional short name shown in task lists and lease warnings (up to 256 bytes)."}
            }),
            &["code"]
        ),
        definition(
            "codemode_task_list",
            "List active codemode tasks in this session, including tasks stopping.",
            json!({}),
            &[]
        ),
        definition(
            "codemode_wait",
            concat!(
                "Wait in the current step for active codemode tasks, task events, or user messages without polling. ",
                "Call with {} (omit trigger_code) to wait for background task results or events; requires pending input or an active task, and returns on queued events, user messages, session pause, all tasks finishing, or a 60-minute limit. ",
                "With trigger_code, race a one-shot async JS script (even with no active tasks) against those events; any competing event cancels the script and discards its output."
            ),
            json!({"trigger_code":{"type":"string", "description":concat!(
                "Optional one-shot nonempty async JS body up to 256 KiB (omit when simply waiting for task completion/events). ",
                "Same runtime as codemode (task.status/list, fs, http, proc, mcp, mygo, sleep; no emitEvent, mygo.taskId is null). ",
                "Not a periodic callback: returning any value (including null/undefined) or reaching the end wakes immediately with reason 'trigger', return value, and console output—write your own loop to poll a custom condition. ",
                "Keep side-effect-free (never recovered or replayed if interrupted). ",
                "Example: while (true) { const s = await task.status('cm_01h455vb4pex5vsknk084sn02q'); if (s.running_commands?.some(c => /ERR/.test(c.stderr))) return s; await sleep(500); }"
            )}}),
            &[]
        ),
        definition(
            "codemode_task_status",
            "Inspect one task's current or final state. Live snapshots include console output and running proc.exec stdout/stderr. A live task with unchanged output and state omits repeat text and returns unchanged_since_last_status: true. A completion event already queued for the next step is reported with delivery: queued. Use for on-demand inspection; call codemode_wait instead of polling.",
            json!({"id":id}),
            &["id"]
        ),
        definition(
            "codemode_task_kill",
            "Stop a task and its managed commands. Returns the final result if reaped within three seconds; otherwise a completion event follows.",
            json!({"id":id}),
            &["id"]
        ),
        definition(
            "codemode_task_renew",
            "Replace a live task's remaining lease with this duration, starting now. The initial lease is two hours.",
            json!({
                "id":id,
                "duration_seconds":{
                    "type":"integer",
                    "minimum":1,
                    "maximum":86400,
                    "description":"New remaining lease duration from now, in seconds (1 to 86400)."
                }
            }),
            &["id", "duration_seconds"]
        )
    ])
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wait {
    trigger_code: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Renewal {
    id: String,
    duration_seconds: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Script {
    pub code: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub background: bool,
}

enum Action {
    Run(Script),
    List,
    Status(ItemId),
    Wait(Option<String>),
    Kill(ItemId),
    Renew(ItemId, u32),
}

pub(crate) fn task_id(id: &str) -> std::result::Result<ItemId, String> {
    ItemId::from_task_id(id)
}

fn action(name: &str, arguments: &str) -> std::result::Result<Action, String> {
    fn parse<T: serde::de::DeserializeOwned>(s: &str) -> std::result::Result<T, String> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }
    match name {
        "codemode" => {
            let script: Script = parse(arguments)?;
            if script.code.trim().is_empty()
                || script.code.len() > 256 * 1024
                || script.label.len() > 256
            {
                return Err(
                    "expected nonempty code up to 256 KiB and label up to 256 bytes".into(),
                );
            }
            Ok(Action::Run(script))
        }
        "codemode_task_list" => {
            let _: Empty = parse(arguments)?;
            Ok(Action::List)
        }
        "codemode_task_status" => Ok(Action::Status(task_id(&parse::<Target>(arguments)?.id)?)),
        "codemode_wait" => {
            let code = parse::<Wait>(arguments)?.trigger_code;
            if code
                .as_ref()
                .is_some_and(|code| code.trim().is_empty() || code.len() > 256 * 1024)
            {
                return Err("expected nonempty trigger_code up to 256 KiB".into());
            }
            Ok(Action::Wait(code))
        }
        "codemode_task_kill" => Ok(Action::Kill(task_id(&parse::<Target>(arguments)?.id)?)),
        "codemode_task_renew" => {
            let r: Renewal = parse(arguments)?;
            if !(1..=86400).contains(&r.duration_seconds) {
                return Err("duration_seconds must be between 1 and 86400".into());
            }
            Ok(Action::Renew(task_id(&r.id)?, r.duration_seconds))
        }
        _ => Err(format!("unknown tool {name}")),
    }
}

pub async fn execute(
    app: App,
    call: Call,
    name: &str,
    arguments: &str,
    hard: CancellationToken,
) -> Result<()> {
    if hard.is_cancelled() {
        return Ok(());
    }
    match action(name, arguments) {
        Ok(Action::Run(script)) => codemode::run(app, call, script, hard).await,
        Ok(Action::Status(id)) => codemode::status(app, call, id, hard).await,
        Ok(Action::Wait(code)) => codemode::wait(app, call, code, hard).await,
        Ok(Action::List) => codemode::list(app, call).await,
        Ok(Action::Kill(id)) => codemode::kill(app, call, id, hard).await,
        Ok(Action::Renew(id, seconds)) => codemode::renew(app, call, id, seconds, hard).await,
        Err(e) => {
            app.db
                .call(move |s| call.reply(s, json!({"status":"error","error":e})))
                .await
        }
    }
}
