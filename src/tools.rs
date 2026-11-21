use crate::{
    App, Result,
    codemode::{self, Call},
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
                "Execute JavaScript as an async function body in a fresh QuickJS runtime. Top-level await/return work. ",
                "Use Date for time. Host APIs: await fs.read(path,{encoding:'utf8'}), fs.write(path,textOrBytes,{append}), ",
                "http.request({url,method,headers,body,timeout_ms}) => {status,headers,body,text(),json()}, ",
                "proc.exec(program,args,{cwd,env,stdin,timeout_ms}), bash(script,options) => {exit_code,signal,stdout,stderr,truncated}, sleep(ms). ",
                "Use managed commands for heavy computation. result(textOrBlocks) appends output; return textOrBlocks finishes. ",
                "await emitEvent(textOrBlocks) queues a separate event in this session. Blocks: {type:'text',text}, ",
                "{type:'image'|'audio'|'video',attachment_id} or {type,mime_type,data:Uint8Array}. ",
                "Long listeners should use background:true. After 90s a running task goes to background automatically. ",
                "Results and lease reminders wake the session. End your turn if there is nothing else to do."
            ),
            json!({"code":{"type":"string"},"background":{"type":"boolean"},"label":{"type":"string"}}),
            &["code"]
        ),
        definition(
            "codemode_task_list",
            "List active codemode tasks in this session, including tasks stopping.",
            json!({}),
            &[]
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
            json!({"id":id,"duration_seconds":{"type":"integer","minimum":1,"maximum":86400}}),
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
    Kill(i64),
    Renew(i64, u32),
}

fn action(name: &str, arguments: &str) -> std::result::Result<Action, String> {
    fn parse<T: serde::de::DeserializeOwned>(s: &str) -> std::result::Result<T, String> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }
    fn target(id: &str) -> std::result::Result<i64, String> {
        id.strip_prefix("cm_")
            .and_then(|n| n.parse::<i64>().ok())
            .filter(|n| *n > 0 && format!("cm_{n}") == id)
            .ok_or_else(|| "expected a cm_ task ID".to_owned())
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
        "codemode_task_kill" => Ok(Action::Kill(target(&parse::<Target>(arguments)?.id)?)),
        "codemode_task_renew" => {
            let r: Renewal = parse(arguments)?;
            if !(1..=86400).contains(&r.duration_seconds) {
                return Err("duration_seconds must be between 1 and 86400".into());
            }
            Ok(Action::Renew(target(&r.id)?, r.duration_seconds))
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
