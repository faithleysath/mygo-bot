use super::{oauth, stdio};
use crate::{App, Error, Result, id::McpServerId};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/mcp/servers", get(list_servers).post(create_server))
        .route(
            "/mcp/servers/{id}",
            get(get_server).patch(patch_server).delete(delete_server),
        )
        .merge(oauth::routes())
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum McpTransport {
    #[default]
    StreamableHttp,
    Stdio,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct McpServerConfig {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "enabled")]
    enabled: bool,
    #[serde(default)]
    shared: bool,
    #[serde(default)]
    transport: McpTransport,
    url: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    oauth: Option<oauth::Config>,
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cwd: Option<String>,
    allowed_tools: Option<BTreeSet<String>>,
    #[serde(default = "timeout_ms")]
    timeout_ms: u32,
}
fn enabled() -> bool {
    true
}
fn timeout_ms() -> u32 {
    60_000
}
type Fields = Json<Map<String, Value>>;

#[derive(Serialize)]
struct McpServerInfo {
    id: McpServerId,
    name: String,
    description: String,
    enabled: bool,
    shared: bool,
    transport: McpTransport,
    /// 查询参数可能含凭据，读取时省略。
    url: Option<String>,
    has_url_query: bool,
    has_headers: bool,
    has_oauth: bool,
    command: Option<String>,
    arg_count: usize,
    has_env: bool,
    cwd: Option<String>,
    allowed_tools: Option<BTreeSet<String>>,
    timeout_ms: u32,
    revision: i64,
}
pub(super) struct Endpoint {
    pub id: McpServerId,
    pub revision: i64,
    pub timeout_ms: u32,
    pub allow_disabled: bool,
    pub allowed_tools: Option<BTreeSet<String>>,
    pub transport: Transport,
}
pub(super) enum Transport {
    Http {
        url: String,
        headers: HashMap<HeaderName, HeaderValue>,
        oauth: bool,
    },
    Stdio(stdio::StdioConfig),
}

fn parse_headers(raw: &BTreeMap<String, String>) -> Result<HashMap<HeaderName, HeaderValue>> {
    let mut headers = HashMap::new();
    for (key, value) in raw {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| Error::bad("invalid MCP header name"))?;
        if name.as_str().starts_with("mcp-")
            || matches!(
                name.as_str(),
                "accept"
                    | "content-type"
                    | "host"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "last-event-id"
            )
        {
            return Err(Error::bad("HTTP transport header cannot be overridden"));
        }
        let mut val =
            HeaderValue::from_str(value).map_err(|_| Error::bad("invalid MCP header value"))?;
        val.set_sensitive(true);
        if headers.insert(name, val).is_some() {
            return Err(Error::bad("duplicate MCP header name"));
        }
    }
    Ok(headers)
}

struct Stored {
    id: McpServerId,
    revision: i64,
    cfg: McpServerConfig,
}
const SELECT: &str = "SELECT id,name,description,enabled,shared,transport,config_json,allowed_tools,timeout_ms,revision FROM mcp_servers";
fn stored(r: &Row) -> rusqlite::Result<Stored> {
    let mut value: Value = r.get(6)?;
    let obj = value.as_object_mut().ok_or(rusqlite::Error::InvalidQuery)?;
    obj.insert("name".into(), json!(r.get::<_, String>(1)?));
    obj.insert("description".into(), json!(r.get::<_, String>(2)?));
    obj.insert("enabled".into(), json!(r.get::<_, bool>(3)?));
    obj.insert("shared".into(), json!(r.get::<_, bool>(4)?));
    obj.insert("transport".into(), json!(r.get::<_, String>(5)?));
    obj.insert(
        "allowed_tools".into(),
        r.get::<_, Option<Value>>(7)?.unwrap_or(Value::Null),
    );
    obj.insert("timeout_ms".into(), json!(r.get::<_, u32>(8)?));
    let cfg = serde_json::from_value(value).map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(Stored {
        id: r.get(0)?,
        revision: r.get(9)?,
        cfg,
    })
}
impl Stored {
    fn info(self) -> McpServerInfo {
        let c = self.cfg;
        let mut has_url_query = false;
        let url = c.url.map(|url| match url.split_once('?') {
            Some((path, _)) => {
                has_url_query = true;
                path.to_owned()
            }
            None => url,
        });
        McpServerInfo {
            id: self.id,
            revision: self.revision,
            name: c.name,
            description: c.description,
            enabled: c.enabled,
            shared: c.shared,
            transport: c.transport,
            url,
            has_url_query,
            has_headers: !c.headers.is_empty(),
            has_oauth: c.oauth.is_some(),
            command: c.command,
            arg_count: c.args.len(),
            has_env: !c.env.is_empty(),
            cwd: c.cwd,
            allowed_tools: c.allowed_tools,
            timeout_ms: c.timeout_ms,
        }
    }
    fn endpoint(self, allow_disabled: bool) -> Result<Endpoint> {
        let c = self.cfg;
        let transport = match c.transport {
            McpTransport::StreamableHttp => Transport::Http {
                url: c
                    .url
                    .ok_or_else(|| Error::internal("missing stored MCP URL"))?,
                headers: parse_headers(&c.headers)?,
                oauth: c.oauth.is_some(),
            },
            McpTransport::Stdio => Transport::Stdio(stdio::StdioConfig {
                command: c
                    .command
                    .ok_or_else(|| Error::internal("missing stored MCP command"))?,
                args: c.args,
                env: c.env,
                cwd: c.cwd,
            }),
        };
        Ok(Endpoint {
            id: self.id,
            revision: self.revision,
            timeout_ms: c.timeout_ms,
            allow_disabled,
            allowed_tools: c.allowed_tools,
            transport,
        })
    }
}
fn load(conn: &Connection, id: McpServerId) -> Result<Stored> {
    Ok(conn
        .prepare_cached(&format!("{SELECT} WHERE id=?"))?
        .query_row([id], stored)?)
}
fn by_id(conn: &Connection, id: McpServerId) -> Result<McpServerInfo> {
    Ok(load(conn, id)?.info())
}
pub(super) fn endpoint_by_id(conn: &Connection, id: McpServerId) -> Result<Endpoint> {
    load(conn, id)?.endpoint(true)
}
pub(super) fn endpoint(conn: &Connection, name: &str) -> Result<Endpoint> {
    conn.prepare_cached(&format!("{SELECT} WHERE enabled AND name=?1"))?
        .query_row([name], stored)
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Error::bad("MCP server is unknown or disabled"),
            other => other.into(),
        })?
        .endpoint(false)
}
pub(super) fn enabled_servers(conn: &Connection) -> Result<Value> {
    let mut stmt =
        conn.prepare_cached("SELECT name,description FROM mcp_servers WHERE enabled ORDER BY name")?;
    let mut rows = stmt.query([])?;
    let mut servers = Vec::new();
    let mut bytes = 2;
    while let Some(row) = rows.next()? {
        let server = json!({"name":row.get::<_,String>(0)?,"description":row.get::<_,String>(1)?});
        bytes += server.to_string().len() + 1;
        if bytes > super::BYTE_LIMIT {
            return Err(Error::bad("MCP server list exceeds byte limit"));
        }
        servers.push(server);
    }
    Ok(Value::Array(servers))
}
fn connection_document(c: &McpServerConfig) -> Value {
    match c.transport {
        McpTransport::StreamableHttp => json!({"url":c.url,"headers":c.headers,"oauth":c.oauth}),
        McpTransport::Stdio => json!({"command":c.command,"args":c.args,"env":c.env,"cwd":c.cwd}),
    }
}
fn validate(c: &mut McpServerConfig) -> Result<()> {
    if c.name.trim().is_empty()
        || c.name.trim() != c.name
        || c.name.len() > 128
        || c.description.len() > 4096
        || !(1..=86_400_000).contains(&c.timeout_ms)
    {
        return Err(Error::bad("invalid MCP name, description or timeout"));
    }
    if c.allowed_tools.as_ref().is_some_and(|names| {
        names
            .iter()
            .any(|s| s.is_empty() || s.trim() != s || s.len() > 256)
    }) {
        return Err(Error::bad(
            "expected nonempty exact MCP tool names up to 256 bytes",
        ));
    }
    match c.transport {
        McpTransport::StreamableHttp => {
            if c.command.is_some() || !c.args.is_empty() || !c.env.is_empty() || c.cwd.is_some() {
                return Err(Error::bad("stdio fields require the stdio transport"));
            }
            let url = Url::parse(c.url.as_deref().unwrap_or(""))
                .map_err(|_| Error::bad("invalid MCP URL"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::bad(
                    "expected HTTP(S) MCP URL without user info or fragment",
                ));
            }
            let headers = parse_headers(&c.headers)?;
            if let Some(oauth) = &c.oauth {
                oauth.validate()?;
                super::oauth::validate_endpoint(&url)?;
                if headers.contains_key(&reqwest::header::AUTHORIZATION) {
                    return Err(Error::bad(
                        "OAuth and an Authorization header cannot be combined",
                    ));
                }
            }
            c.url = Some(url.into());
        }
        McpTransport::Stdio => {
            if c.url.is_some() || !c.headers.is_empty() || c.oauth.is_some() {
                return Err(Error::bad("HTTP fields cannot be used with stdio"));
            }
            if c.command
                .as_ref()
                .is_none_or(|s| s.trim().is_empty() || s.contains('\0'))
                || c.args.iter().any(|s| s.contains('\0'))
                || c.cwd
                    .as_ref()
                    .is_some_and(|s| s.is_empty() || s.contains('\0'))
                || c.env
                    .iter()
                    .any(|(k, v)| k.is_empty() || k.contains(['\0', '=']) || v.contains('\0'))
            {
                return Err(Error::bad(
                    "invalid stdio command, arguments, environment, or cwd",
                ));
            }
        }
    }
    Ok(())
}
fn save(
    conn: &Connection,
    id: Option<McpServerId>,
    fields: Fields,
) -> Result<(McpServerInfo, bool, Option<McpServerId>)> {
    if fields.is_empty() {
        return Err(Error::bad("expected MCP configuration fields"));
    }
    let old = id.map(|id| load(conn, id)).transpose()?;
    let mut doc = json!({});
    if let Some(row) = &old {
        let c = &row.cfg;
        if fields
            .get("name")
            .is_some_and(|v| v.as_str() != Some(&c.name))
        {
            return Err(Error::bad("MCP server name is immutable"));
        }
        doc = json!(c);
        let switching = fields
            .get("transport")
            .is_some_and(|t| t != &doc["transport"]);
        if switching {
            for key in ["command", "args", "env", "cwd", "url", "headers", "oauth"] {
                doc.as_object_mut().unwrap().remove(key);
            }
        } else if let Some(new) = fields.get("url").and_then(Value::as_str)
            && let Some(current) = &c.url
            && Url::parse(new).map(|u| u.origin()) != Url::parse(current).map(|u| u.origin())
            && ((!c.headers.is_empty() && !fields.contains_key("headers"))
                || (c.oauth.is_some() && !fields.contains_key("oauth")))
        {
            return Err(Error::bad(
                "set credentials explicitly when changing the MCP origin",
            ));
        }
    }
    doc.as_object_mut().unwrap().extend(fields.0);
    let mut c: McpServerConfig = serde_json::from_value(doc)
        .map_err(|_| Error::bad("invalid MCP configuration fields or types"))?;
    validate(&mut c)?;
    let connection = connection_document(&c);
    let auth_changed = old.as_ref().is_some_and(|s| {
        s.cfg.transport != c.transport || connection_document(&s.cfg) != connection
    });
    let transport = match c.transport {
        McpTransport::StreamableHttp => "streamable-http",
        McpTransport::Stdio => "stdio",
    };

    if let Some(old_id) = id
        && old.as_ref().is_some_and(|s| s.cfg.shared && !c.shared)
    {
        crate::cluster::ledger::assert_no_conflict(conn, old_id.as_str())?;
        let old_oauth = if !auth_changed {
            conn.query_row(
                "SELECT generation,binding,credentials,rejected FROM mcp_oauth
                 WHERE server_id=? AND (credentials IS NOT NULL OR rejected=1)",
                [old_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()?
        } else {
            None
        };
        let new_id = McpServerId::new()?;
        conn.execute("DELETE FROM mcp_servers WHERE id=?", [old_id])?;
        conn.execute(
            "INSERT INTO mcp_servers(id,name,description,enabled,shared,transport,config_json,allowed_tools,timeout_ms,revision)
             VALUES(?,?,?,?,0,?,?,?,?,1)",
            params![
                new_id,
                c.name,
                c.description,
                c.enabled,
                transport,
                connection,
                c.allowed_tools.as_ref().map(|names| json!(names)),
                c.timeout_ms
            ],
        )
        .map_err(|e| match e.sqlite_error().map(|code| code.extended_code) {
            Some(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE) => {
                Error::conflict("MCP server name already exists")
            }
            _ => e.into(),
        })?;
        if let Some((generation, binding, credentials, rejected)) = old_oauth {
            conn.execute(
                "INSERT INTO mcp_oauth(server_id,generation,binding,pending,credentials,staged_credentials,rejected)
                 VALUES(?,?,?,NULL,?,NULL,?)",
                params![new_id, generation, binding, credentials, rejected],
            )?;
        }
        crate::cluster::ledger::record_mcp_deleted(conn, old_id)?;
        return Ok((by_id(conn, new_id)?, auth_changed, Some(old_id)));
    }

    let sql = "INSERT INTO mcp_servers(id,name,description,enabled,shared,transport,config_json,allowed_tools,timeout_ms,revision)
        VALUES(?,?,?,?,?,?,?,?,?,1) ON CONFLICT(id) DO UPDATE SET description=excluded.description,enabled=excluded.enabled,
        shared=excluded.shared,transport=excluded.transport,config_json=excluded.config_json,allowed_tools=excluded.allowed_tools,
        timeout_ms=excluded.timeout_ms,revision=mcp_servers.revision+1 RETURNING id";
    let id = match id {
        Some(id) => id,
        None => McpServerId::new()?,
    };
    let id = conn
        .query_row(
            sql,
            params![
                id,
                c.name,
                c.description,
                c.enabled,
                c.shared,
                transport,
                connection,
                c.allowed_tools.as_ref().map(|names| json!(names)),
                c.timeout_ms
            ],
            |r| r.get(0),
        )
        .map_err(|e| match e.sqlite_error().map(|code| code.extended_code) {
            Some(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE) => {
                Error::conflict("MCP server name already exists")
            }
            _ => e.into(),
        })?;
    if c.shared {
        crate::cluster::ledger::record_mcp_if_shared(conn, id)?;
    }
    Ok((by_id(conn, id)?, auth_changed, None))
}

async fn list_servers(State(app): State<App>) -> Result<Json<Vec<McpServerInfo>>> {
    app.db
        .call(|s| {
            Ok(Json(
                s.conn
                    .prepare_cached(&format!("{SELECT} ORDER BY id"))?
                    .query_map([], |row| stored(row).map(Stored::info))?
                    .collect::<rusqlite::Result<_>>()?,
            ))
        })
        .await
}

async fn get_server(
    State(app): State<App>,
    Path(id): Path<McpServerId>,
) -> Result<Json<McpServerInfo>> {
    app.db.call(move |s| by_id(&s.conn, id).map(Json)).await
}

async fn create_server(
    State(app): State<App>,
    fields: Fields,
) -> Result<(StatusCode, Json<McpServerInfo>)> {
    app.db
        .write(
            move |conn| save(conn, None, fields),
            |_, (value, _, _)| Ok((StatusCode::CREATED, Json(value))),
        )
        .await
}

async fn patch_server(
    State(app): State<App>,
    Path(id): Path<McpServerId>,
    fields: Fields,
) -> Result<Json<McpServerInfo>> {
    app.db
        .write(
            move |conn| save(conn, Some(id), fields),
            move |s, (value, auth_changed, detached_from)| {
                if let Some(old_id) = detached_from {
                    s.mcp.oauth().cancel_login(old_id);
                    s.mcp.forget(old_id);
                } else {
                    if auth_changed || !value.enabled {
                        s.mcp.oauth().cancel_login(id);
                    }
                    s.mcp.forget(id);
                }
                Ok(Json(value))
            },
        )
        .await
}

async fn delete_server(State(app): State<App>, Path(id): Path<McpServerId>) -> Result<StatusCode> {
    app.db
        .write(
            move |conn| {
                crate::cluster::ledger::assert_no_conflict(conn, id.as_str())?;
                if conn.execute("DELETE FROM mcp_servers WHERE id=?", [id])? == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                crate::cluster::ledger::record_mcp_deleted(conn, id)?;
                Ok(())
            },
            move |s, ()| {
                s.mcp.oauth().cancel_login(id);
                s.mcp.forget(id);
                Ok(StatusCode::NO_CONTENT)
            },
        )
        .await
}
