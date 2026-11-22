mod config;
pub use config::routes;

use crate::{App, Error, Result, store::Db};
use rmcp::{
    RoleClient,
    model::{
        CallToolRequestParams, CallToolResult, ContentBlock, PaginatedRequestParams,
        ProtocolVersion, Tool,
    },
    service::{
        ClientCacheConfig, ClientLifecycleMode, ClientServiceExt, RunningService, ServiceError,
    },
    transport::{
        StreamableHttpClientTransport,
        streamable_http_client::{StreamableHttpClientTransportConfig, StreamableHttpError},
    },
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
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

const BYTE_LIMIT: usize = 2 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
type Client = RunningService<RoleClient, ()>;

#[derive(Default)]
struct Slot {
    client: OnceCell<Client>,
    retired: AtomicBool,
}
impl Slot {
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
pub struct Connections(HashMap<(i64, i64), (Arc<Slot>, Instant)>);
impl Connections {
    fn acquire(&mut self, sid: i64, id: i64) -> Result<Arc<Slot>> {
        let key = (sid, id);
        let now = Instant::now();
        if let Some((slot, touched)) = self.0.get_mut(&key)
            && slot.available()
        {
            *touched = now;
            return Ok(slot.clone());
        }
        if self.0.len() >= MAX_CONNECTIONS && !self.0.contains_key(&key) {
            let idle = self
                .0
                .iter()
                .filter(|(_, (slot, _))| Arc::strong_count(slot) == 1)
                .min_by_key(|(_, (_, last))| *last)
                .map(|(key, _)| *key)
                .ok_or_else(|| Error::conflict("MCP connection limit reached"))?;
            self.0.remove(&idle);
        }
        let slot = Arc::new(Slot::default());
        self.0.insert(key, (slot.clone(), now));
        Ok(slot)
    }
    fn forget(&mut self, id: i64) {
        self.0.retain(|(_, server), (slot, _)| {
            if *server == id {
                slot.retire();
                return false;
            }
            true
        });
    }
}

pub async fn close(db: &Db) -> Result<()> {
    let slots = db.call(|s| Ok(std::mem::take(&mut s.mcp.0))).await?;
    futures_util::future::join_all(slots.into_values().map(|(slot, _)| async move {
        slot.retire();
        match Arc::try_unwrap(slot) {
            Ok(slot) => {
                if let Some(mut client) = slot.client.into_inner() {
                    let _ = client.close_with_timeout(Duration::from_secs(1)).await;
                }
            }
            Err(slot) => {
                if let Some(client) = slot.client.get() {
                    client.cancellation_token().cancel();
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
                s.mcp.0.retain(|_, (slot, last)| {
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

async fn connect(
    client: reqwest::Client,
    endpoint: config::Endpoint,
) -> std::result::Result<Client, String> {
    let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint.url)
        .reinit_on_expired_session(false);
    config.custom_headers = endpoint.headers;
    config.max_json_response_size = BYTE_LIMIT;
    config.max_sse_event_size = BYTE_LIMIT;
    config.max_error_response_size = 1024;
    let transport = StreamableHttpClientTransport::with_client(client, config);
    let client = ()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: ProtocolVersion::KNOWN_VERSIONS
                    .iter()
                    .rev()
                    .cloned()
                    .collect(),
                legacy_version: Some(ProtocolVersion::LATEST_WITH_INITIALIZE),
            },
        )
        .await
        .map_err(|e| {
            if e.is_authorization_required() {
                "MCP authentication failed; check configured headers"
            } else {
                "MCP handshake failed; check endpoint, transport and credentials"
            }
        })?;
    // Modern dynamic catalogs require a separate subscription for invalidations.
    // Read those directly. On legacy connections rmcp invalidates cached pages on notification.
    let dynamic = client.peer_info().is_some_and(|info| {
        !info.protocol_version.has_initialize()
            && info
                .capabilities
                .tools
                .as_ref()
                .and_then(|tools| tools.list_changed)
                == Some(true)
    });
    let cache = if dynamic {
        ClientCacheConfig::disabled()
    } else {
        ClientCacheConfig::default()
            .with_default_ttl(Duration::from_secs(5))
            .with_max_ttl(Duration::from_secs(30))
            .with_max_entries(4)
            .with_serve_stale_on_error(false)
    };
    client.set_response_cache_config(cache).await;
    Ok(client)
}

#[derive(Deserialize)]
struct Discovery {
    #[serde(default)]
    query: String,
    cursor: Option<String>,
}
#[derive(Deserialize)]
struct Invocation {
    tool: String,
    args: Map<String, Value>,
}
enum Request {
    List(Discovery),
    Call(Invocation),
}

// The SDK manages per-peer tool-page caching, TTLs and list-change invalidation.
// Bound scanning even if the server keeps returning empty pages or repeating cursors.
async fn discover(
    client: &Client,
    slot: &Slot,
    request: &Request,
) -> std::result::Result<Option<Value>, String> {
    let (query, mut cursor) = match request {
        Request::List(d) => (d.query.to_lowercase(), d.cursor.clone()),
        Request::Call(_) => (String::new(), None),
    };
    let mut seen = HashSet::new();
    let mut count = 0;
    for _ in 0..64 {
        slot.check()?;
        seen.insert(cursor.clone());
        let page = client
            .list_tools(cursor.map(|c| PaginatedRequestParams::default().with_cursor(Some(c))))
            .await
            .map_err(|e| request_error(e, slot, false))?;
        count += page.tools.len();
        if count > 4096 {
            return Err("MCP tool list exceeds 4096 tools".into());
        }
        cursor = page.next_cursor;
        if cursor.is_some() && seen.contains(&cursor) {
            return Err("MCP tool list repeated a cursor".into());
        }
        match request {
            Request::Call(invocation) => {
                if page.tools.iter().any(|tool| tool.name == invocation.tool) {
                    return Ok(None);
                }
                if cursor.is_none() {
                    return Err("unknown MCP tool".into());
                }
            }
            Request::List(_) => {
                let tools: Vec<_> = page.tools.into_iter()
                    .filter(|tool| matches_query(tool, &query))
                    .map(|tool| {
                        json!({"name":tool.name, "description":tool.description, "inputSchema":tool.input_schema})
                    })
                    .collect();
                if !tools.is_empty() || cursor.is_none() {
                    return Ok(Some(json!({"tools":tools,"nextCursor":cursor})));
                }
            }
        }
    }
    Err("MCP tool scan exceeds 64 pages".into())
}
fn matches_query(tool: &Tool, query: &str) -> bool {
    query.is_empty()
        || tool.name.to_lowercase().contains(query)
        || tool
            .description
            .as_ref()
            .is_some_and(|s| s.to_lowercase().contains(query))
}

fn request_error(e: ServiceError, slot: &Slot, sent: bool) -> String {
    if let ServiceError::McpError(error) = e {
        return format!("MCP protocol error ({})", error.code.0);
    }
    if matches!(
        e,
        ServiceError::TransportClosed | ServiceError::TransportSend(_)
    ) {
        slot.retire();
    }
    if e.is_authorization_required() {
        return "MCP authentication failed; check configured headers".into();
    }
    let mut cause = std::error::Error::source(&e);
    while let Some(err) = cause {
        if let Some(StreamableHttpError::ResponseBodyTooLarge { max_size }) =
            err.downcast_ref::<StreamableHttpError<reqwest::Error>>()
        {
            let outcome = if sent {
                "; result unknown; request was not replayed"
            } else {
                ""
            };
            return format!("MCP response exceeds {max_size} bytes{outcome}");
        }
        cause = err.source();
    }
    if sent {
        "MCP call interrupted or response unreadable; result unknown; request was not replayed"
            .into()
    } else {
        "MCP discovery failed; check connection and server capabilities".into()
    }
}

fn script_result(result: CallToolResult) -> Value {
    let content: Vec<_> = result
        .content
        .into_iter()
        .map(|block| {
            let kind = match block {
                ContentBlock::Text(t) => return json!({"type":"text","text":t.text}),
                ContentBlock::Image(_) => "image",
                ContentBlock::Audio(_) => "audio",
                ContentBlock::Resource(_) => "resource",
                ContentBlock::ResourceLink(_) => "resource_link",
                _ => "unknown",
            };
            json!({"type":"text","text":format!("Unsupported MCP content block: {kind}")})
        })
        .collect();
    let mut value = json!({"content":content,"isError":result.is_error.unwrap_or(false)});
    if let Some(structured) = result.structured_content {
        value["structuredContent"] = structured;
    }
    value
}

pub async fn execute(
    app: &App,
    sid: i64,
    origin: i64,
    op: &str,
    args: Value,
) -> std::result::Result<Value, String> {
    if op == "mcp.servers" {
        return app
            .db
            .call(|s| config::enabled_servers(&s.conn))
            .await
            .map_err(|e| e.to_string());
    }
    let server = args["server"]
        .as_str()
        .ok_or("expected an MCP server name")?
        .to_owned();
    let request = match op {
        "mcp.tools" => Request::List(
            serde_json::from_value(args).map_err(|_| "expected string query and cursor")?,
        ),
        "mcp.call" => Request::Call(
            serde_json::from_value(args)
                .map_err(|_| "expected an MCP tool name and an arguments object")?,
        ),
        _ => return Err("unknown MCP operation".into()),
    };
    let lookup = server.clone();
    let (slot, endpoint) = app
        .db
        .call(move |s| {
            let endpoint = config::endpoint(&s.conn, &lookup)?;
            Ok((s.mcp.acquire(sid, endpoint.id)?, endpoint))
        })
        .await
        .map_err(|e| e.to_string())?;
    let duration = Duration::from_millis(endpoint.timeout_ms.into());
    let start = Instant::now();
    let tool = match &request {
        Request::List(_) => "",
        Request::Call(i) => &i.tool,
    };
    let mut sent = false;
    let result = timeout(duration, async {
        slot.check()?;
        let client = slot
            .client
            .get_or_try_init(|| connect(app.client.clone(), endpoint))
            .await?;
        if let Some(page) = discover(client, &slot, &request).await? {
            return Ok(page);
        }
        let Request::Call(call) = &request else {
            unreachable!()
        };
        slot.check()?;
        sent = true;
        // Call the peer's single-round API. Later attempts always require a new script call.
        let params =
            CallToolRequestParams::new(call.tool.clone()).with_arguments(call.args.clone());
        client
            .peer()
            .call_tool(params)
            .await
            .map(script_result)
            .map_err(|e| request_error(e, &slot, true))
    })
    .await
    .unwrap_or_else(|_| {
        Err(if sent {
            "MCP call timed out; result unknown; request was not replayed"
        } else {
            "MCP connection or discovery timed out"
        }
        .into())
    })
    .and_then(|value| {
        if value.to_string().len() <= BYTE_LIMIT {
            Ok(value)
        } else {
            Err("MCP result exceeds byte limit".into())
        }
    });
    let status = match &result {
        Ok(v) if v["isError"] == true => "tool_error",
        Ok(_) => "ok",
        Err(_) => "error",
    };
    tracing::info!(
        sid,
        origin,
        server,
        tool,
        status,
        elapsed_ms = start.elapsed().as_millis(),
        "MCP operation"
    );
    result
}
