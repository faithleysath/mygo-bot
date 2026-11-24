mod capabilities;
mod client;
mod config;
mod content;
pub(crate) mod diagnostics;
mod errors;
mod oauth;
mod stdio;
pub use config::routes;

use crate::{
    App, Error, Result,
    codemode::Reply,
    id::{ItemId, McpServerId, SessionId},
    store::Db,
};
use capabilities::AttemptError;
use client::{Client, McpError};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::OnceCell,
    time::{Instant, timeout},
};
use tokio_util::sync::CancellationToken;

// Media travels separately from the JSON returned to QuickJS. Base64 counts against the wire budget.
const BYTE_LIMIT: usize = 2 * 1024 * 1024;
const WIRE_LIMIT: usize = 16 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Default)]
struct Slot {
    client: OnceCell<Client>,
    retired: AtomicBool,
    // Only echo error field names already public in this session's tool schemas.
    public_fields: std::sync::Mutex<HashSet<String>>,
}
impl Slot {
    fn remember_fields(&self, schema: &serde_json::Map<String, Value>) {
        if let Some(fields) = schema.get("properties").and_then(Value::as_object) {
            let mut public = self.public_fields.lock().unwrap();
            for name in fields.keys().filter(|n| n.len() <= 64) {
                if public.len() >= 512 {
                    break;
                }
                public.insert(name.clone());
            }
        }
    }
    fn can_report_field(&self, name: &str) -> bool {
        // These conventional parameter names also give a usable hint on a direct first call.
        matches!(
            name,
            "query" | "name" | "uri" | "id" | "path" | "cursor" | "limit" | "arguments"
        ) || self.public_fields.lock().unwrap().contains(name)
    }
    fn available(&self) -> bool {
        !self.retired.load(Ordering::Acquire) && self.client.get().is_none_or(|c| !c.is_closed())
    }
    fn check(&self) -> std::result::Result<(), String> {
        if self.available() {
            Ok(())
        } else {
            Err("MCP configuration changed or connection closed".into())
        }
    }
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}

// Owned by the database worker: admission and committed configuration changes are ordered.
// An in-flight operation retains its Arc while a replacement can open a fresh connection.
#[derive(Default)]
pub struct Connections {
    slots: HashMap<(SessionId, McpServerId), (Arc<Slot>, Instant)>,
    oauth: oauth::Runtime,
}
impl Connections {
    fn oauth(&mut self) -> &mut oauth::Runtime {
        &mut self.oauth
    }
    fn acquire(&mut self, sid: SessionId, id: McpServerId) -> Result<Arc<Slot>> {
        let key = (sid, id);
        let now = Instant::now();
        if let Some((slot, touched)) = self.slots.get_mut(&key)
            && slot.available()
        {
            *touched = now;
            return Ok(slot.clone());
        }
        if self.slots.len() >= MAX_CONNECTIONS && !self.slots.contains_key(&key) {
            let idle = self
                .slots
                .iter()
                .filter(|(_, (slot, _))| Arc::strong_count(slot) == 1)
                .min_by_key(|(_, (_, last))| *last)
                .map(|(key, _)| *key)
                .ok_or_else(|| Error::conflict("MCP connection limit reached"))?;
            self.slots.remove(&idle);
        }
        let slot = Arc::new(Slot::default());
        self.slots.insert(key, (slot.clone(), now));
        Ok(slot)
    }
    fn forget(&mut self, id: McpServerId) {
        self.slots.retain(|(_, server), (slot, _)| {
            if *server == id {
                slot.retire();
                return false;
            }
            true
        });
    }
    pub fn forget_session(&mut self, sid: SessionId) {
        self.slots.retain(|(session, _), (slot, _)| {
            if *session == sid {
                slot.retire();
                return false;
            }
            true
        });
    }
}

pub async fn close(db: &Db) -> Result<()> {
    let slots = db.call(|s| Ok(std::mem::take(&mut s.mcp.slots))).await?;
    futures_util::future::join_all(slots.into_values().map(|(slot, _)| async move {
        slot.retire();
        match Arc::try_unwrap(slot) {
            Ok(slot) => {
                if let Some(client) = slot.client.into_inner() {
                    client.close(Duration::from_secs(1)).await;
                }
            }
            Err(slot) => {
                if let Some(client) = slot.client.get() {
                    client.cancel();
                }
            }
        }
    }))
    .await;
    Ok(())
}

pub async fn reap_idle(db: Db, stop: CancellationToken) {
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
        }
        if db
            .call(|s| {
                s.mcp.slots.retain(|_, (slot, last)| {
                    if Arc::strong_count(slot) > 1 {
                        *last = Instant::now();
                    }
                    last.elapsed() < IDLE_TIMEOUT && slot.available()
                });
                Ok(())
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn connect(app: &App, endpoint: config::Endpoint) -> std::result::Result<Client, String> {
    let client = match endpoint.transport {
        config::Transport::Http {
            url,
            headers,
            oauth,
        } => {
            let http = oauth::HttpClient::new(
                app.client.clone(),
                app.db.clone(),
                endpoint.id,
                endpoint.revision,
                oauth,
                endpoint.allow_disabled,
            )
            .await?;
            Client::connect_http(http, url, headers).await
        }
        config::Transport::Stdio(config) => {
            // PATH and cwd can be on a slow filesystem. Starting the child must not stall the
            // daemon's single I/O thread. Its RAII guard also owns cleanup if this wait is cancelled.
            let spawned = tokio::task::spawn_blocking(move || stdio::spawn(config))
                .await
                .map_err(|_| "MCP stdio launch worker stopped")?
                .map_err(|e| format!("MCP stdio launch failed ({})", e.kind()))?;
            Client::connect_stdio(spawned).await
        }
    }
    .map_err(errors::handshake)?;
    Ok(client)
}

#[derive(Deserialize)]
struct Discovery {
    #[serde(default)]
    query: String,
    cursor: Option<String>,
}
async fn discover(
    client: &Client,
    slot: &Slot,
    query: Discovery,
    allowed: Option<&BTreeSet<String>>,
) -> std::result::Result<Reply, AttemptError> {
    let filter = query.query.to_lowercase();
    let mut cursor = query.cursor;
    let mut seen = HashSet::new();
    let mut count = 0;
    for _ in 0..64 {
        slot.check()?;
        seen.insert(cursor.clone());
        let mut page = client.list_tools(cursor).await?;
        let raw_tools = page
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .ok_or(McpError::InvalidResponse)?;
        if raw_tools.len() > 4096 {
            return Err("MCP server returned more than 4096 tools in one page"
                .to_owned()
                .into());
        }
        count += raw_tools.len();
        cursor = capabilities::next_cursor(&page)?;
        if cursor.is_some() && seen.contains(&cursor) {
            return Err("MCP tool list repeated a cursor".to_string().into());
        }
        let mut tools = Vec::new();
        for tool in raw_tools {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                return Err(McpError::InvalidResponse.into());
            };
            if !allowed.is_none_or(|names| names.contains(name)) {
                continue;
            }
            let description = tool.get("description").and_then(Value::as_str);
            if !(filter.is_empty()
                || name.to_lowercase().contains(&filter)
                || description.is_some_and(|s| s.to_lowercase().contains(&filter)))
            {
                continue;
            }
            let Some(schema) = tool.get("inputSchema").and_then(Value::as_object) else {
                return Err(McpError::InvalidResponse.into());
            };
            slot.remember_fields(schema);
            tools.push(json!({"name":name,"description":description,"inputSchema":schema}));
        }
        if !tools.is_empty() || cursor.is_none() || count >= 4096 {
            return Ok(Reply::value(json!({"tools":tools,"nextCursor":cursor})));
        }
    }
    // A sparse name filter can span more pages than one scan. The caller can continue from here.
    Ok(Reply::value(json!({"tools":[],"nextCursor":cursor})))
}

pub async fn execute(
    app: &App,
    sid: SessionId,
    origin: ItemId,
    op: &str,
    args: Value,
    stage: impl Fn(&'static str) + Send + Sync,
) -> std::result::Result<Reply, String> {
    if op == "mcp.servers" {
        return app
            .db
            .call(|s| config::enabled_servers(&s.conn))
            .await
            .map(Reply::value)
            .map_err(|e| e.to_string());
    }
    let server = args["server"]
        .as_str()
        .ok_or("expected an MCP server name")?;
    let tool = if op == "mcp.call" {
        Some(
            args["tool"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or("expected a nonempty MCP tool name")?,
        )
    } else {
        None
    };
    let name = server.to_owned();
    let tool_name = tool.map(str::to_owned);
    let (slot, mut endpoint) = app
        .db
        .call(move |s| {
            let endpoint = config::endpoint(&s.conn, &name)?;
            if let (Some(allowed), Some(tool)) = (&endpoint.allowed_tools, tool_name)
                && !allowed.contains(&tool)
            {
                return Err(Error::bad("MCP tool is not allowed for this service"));
            }
            Ok((s.mcp.acquire(sid, endpoint.id)?, endpoint))
        })
        .await
        .map_err(|e| e.to_string())?;
    let allowed = endpoint.allowed_tools.take();
    let duration = Duration::from_millis(endpoint.timeout_ms.into());
    let started = Instant::now();
    let mut sent = false;
    let result = timeout(duration, async {
        stage("connecting");
        slot.check()?;
        let client = slot
            .client
            .get_or_try_init(|| connect(app, endpoint))
            .await?;
        slot.check()?;
        let attempt = match op {
            "mcp.tools" => {
                stage("discovering");
                let query = serde_json::from_value(args.clone())
                    .map_err(|_| "expected string query and cursor")?;
                discover(client, &slot, query, allowed.as_ref()).await
            }
            _ => {
                stage(match op {
                    "mcp.resources" | "mcp.resourceTemplates" | "mcp.prompts" => "discovering",
                    _ => "requesting",
                });
                capabilities::attempt(client, op, &args, || {
                    slot.check()?;
                    sent = true;
                    Ok(())
                })
                .await
            }
        };
        let reply = attempt.map_err(|e| match e {
            AttemptError::Local(reason) => reason,
            AttemptError::Service(err) => errors::request(err, &slot, op, sent),
        })?;
        if reply.value.to_string().len() > BYTE_LIMIT {
            return Err(if sent {
                "MCP tool replied but its JSON exceeds the byte limit; do not replay a write"
            } else {
                "MCP response JSON exceeds the byte limit"
            }
            .to_owned());
        }
        Ok(reply)
    })
    .await
    .unwrap_or_else(|_| {
        Err(if sent {
            "MCP call timed out; result unknown; request was not replayed"
        } else {
            "MCP connection or request timed out"
        }
        .into())
    });
    let status = match &result {
        Ok(v) if v.value["isError"] == true => "tool_error",
        Ok(_) => "ok",
        Err(_) => "error",
    };
    tracing::info!(
        %sid,
        %origin,
        server,
        tool = tool.unwrap_or(""),
        op,
        status,
        elapsed_ms = started.elapsed().as_millis(),
        "MCP operation"
    );
    result
}
