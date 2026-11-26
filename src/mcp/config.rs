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
        .prepare_cached(&format!("{SELECT} WHERE deleted=0 AND id=?"))?
        .query_row([id], stored)?)
}
fn by_id(conn: &Connection, id: McpServerId) -> Result<McpServerInfo> {
    Ok(load(conn, id)?.info())
}
pub(super) fn endpoint_by_id(conn: &Connection, id: McpServerId) -> Result<Endpoint> {
    load(conn, id)?.endpoint(true)
}
pub(super) fn endpoint(conn: &Connection, name: &str) -> Result<Endpoint> {
    let mut stmt = conn.prepare_cached(&format!(
        "{SELECT} WHERE deleted=0 AND enabled=1 AND (id=?1 OR name=?1) LIMIT 2"
    ))?;
    let mut rows = stmt
        .query_map([name], stored)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match rows.len() {
        0 => Err(Error::bad("MCP server is unknown or disabled")),
        1 => rows.pop().unwrap().endpoint(false),
        _ => Err(Error::conflict(
            "ambiguous MCP server name; specify server id or rename duplicate",
        )),
    }
}
pub(super) fn enabled_servers(conn: &Connection) -> Result<Value> {
    let mut stmt = conn.prepare_cached(
        "SELECT name,description FROM mcp_servers WHERE deleted=0 AND enabled=1 ORDER BY name",
    )?;
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
    update: Option<(McpServerId, &mut super::Connections)>,
    fields: Fields,
) -> Result<(McpServerInfo, bool, Option<McpServerId>)> {
    if fields.is_empty() {
        return Err(Error::bad("expected MCP configuration fields"));
    }
    let id = update.as_ref().map(|(id, _)| *id);
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
    let config_changed = old.as_ref().is_some_and(|s| {
        s.cfg.transport != c.transport || connection_document(&s.cfg) != connection
    });
    let oauth_invalidated = old.as_ref().is_some_and(|s| {
        s.cfg.transport != c.transport
            || s.cfg.url != c.url
            || serde_json::to_value(&s.cfg.oauth).ok() != serde_json::to_value(&c.oauth).ok()
    });
    let transport = match c.transport {
        McpTransport::StreamableHttp => "streamable-http",
        McpTransport::Stdio => "stdio",
    };

    if let Some((old_id, mcp)) = update
        && old.as_ref().is_some_and(|s| s.cfg.shared && !c.shared)
    {
        let _grant_guard = if oauth_invalidated {
            None
        } else {
            oauth::guard_grant_copy(conn, mcp, old_id)?
        };
        let old_oauth = if !oauth_invalidated {
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
        crate::cluster::ledger::tombstone_shared_mcp(conn, old_id)?;
        let new_id = McpServerId::new()?;
        let name_taken: bool = conn
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM mcp_servers WHERE name=?1 AND deleted=0 AND id!=?2)",
            )?
            .query_row(params![&c.name, new_id], |r| r.get(0))?;
        if name_taken {
            return Err(Error::conflict("MCP server name already exists"));
        }
        let ts = crate::cluster::ledger::tick_hlc(conn)?;
        conn.execute(
            "INSERT INTO mcp_servers(
                id,name,name_ts,description,description_ts,enabled,enabled_ts,shared,transport,
                allowed_tools,allowed_tools_ts,config_json,timeout_ms,timeout_ts,revision,
                config_ts,oauth_ts,deleted,sync_rev
             ) VALUES(?1,?2,?9,?3,?9,?4,?9,0,?5,?6,?9,?7,?8,?9,1,?9,?9,0,0)",
            params![
                new_id,
                c.name,
                c.description,
                c.enabled,
                transport,
                c.allowed_tools.as_ref().map(|names| json!(names)),
                connection,
                c.timeout_ms,
                ts,
            ],
        )?;
        if let Some((generation, binding, credentials, rejected)) = old_oauth {
            conn.execute(
                "INSERT INTO mcp_oauth(server_id,generation,binding,pending,credentials,pending_refresh,rejected)
                 VALUES(?,?,?,NULL,?,NULL,?)",
                params![new_id, generation, binding, credentials, rejected],
            )?;
        }
        return Ok((by_id(conn, new_id)?, config_changed, Some(old_id)));
    }

    let id = match id {
        Some(id) => id,
        None => McpServerId::new()?,
    };
    let name_taken: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM mcp_servers WHERE name=?1 AND deleted=0 AND id!=?2)",
        )?
        .query_row(params![&c.name, id], |r| r.get(0))?;
    if name_taken {
        return Err(Error::conflict("MCP server name already exists"));
    }
    let ts = crate::cluster::ledger::tick_hlc(conn)?;
    let sync_rev = if c.shared {
        crate::cluster::ledger::next_catalog_rev()
    } else {
        0
    };
    let sql = "INSERT INTO mcp_servers(
            id,name,name_ts,description,description_ts,enabled,enabled_ts,shared,transport,
            allowed_tools,allowed_tools_ts,config_json,timeout_ms,timeout_ts,revision,
            config_ts,oauth_ts,deleted,sync_rev
        ) VALUES(?1,?2,?10,?3,?10,?4,?10,?5,?6,?7,?10,?8,?9,?10,1,?10,?10,0,?11)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            name_ts=CASE WHEN mcp_servers.shared < excluded.shared OR mcp_servers.name != excluded.name THEN ?10 ELSE mcp_servers.name_ts END,
            description=excluded.description,
            description_ts=CASE WHEN mcp_servers.shared < excluded.shared OR mcp_servers.description != excluded.description THEN ?10 ELSE mcp_servers.description_ts END,
            enabled=excluded.enabled,
            enabled_ts=CASE WHEN mcp_servers.shared < excluded.shared OR mcp_servers.enabled != excluded.enabled THEN ?10 ELSE mcp_servers.enabled_ts END,
            shared=excluded.shared,
            transport=excluded.transport,
            allowed_tools=excluded.allowed_tools,
            allowed_tools_ts=CASE WHEN mcp_servers.shared < excluded.shared OR mcp_servers.allowed_tools IS NOT excluded.allowed_tools THEN ?10 ELSE mcp_servers.allowed_tools_ts END,
            config_json=excluded.config_json,
            timeout_ms=excluded.timeout_ms,
            timeout_ts=CASE WHEN mcp_servers.shared < excluded.shared OR mcp_servers.timeout_ms != excluded.timeout_ms THEN ?10 ELSE mcp_servers.timeout_ts END,
            revision=mcp_servers.revision+1,
            config_ts=CASE WHEN mcp_servers.shared < excluded.shared OR ?12 THEN ?10 ELSE mcp_servers.config_ts END,
            oauth_ts=CASE WHEN mcp_servers.shared < excluded.shared OR ?12 THEN ?10 ELSE mcp_servers.oauth_ts END,
            sync_rev=excluded.sync_rev
        WHERE mcp_servers.deleted=0
        RETURNING id";
    let id = conn.query_row(
        sql,
        params![
            id,
            c.name,
            c.description,
            c.enabled,
            c.shared,
            transport,
            c.allowed_tools.as_ref().map(|names| json!(names)),
            connection,
            c.timeout_ms,
            ts,
            sync_rev,
            config_changed,
        ],
        |r| r.get(0),
    )?;
    if oauth_invalidated {
        conn.execute("DELETE FROM mcp_oauth WHERE server_id=?", [id])?;
    }
    if c.shared {
        crate::cluster::ledger::stage_catalog();
    }
    Ok((by_id(conn, id)?, config_changed, None))
}

async fn list_servers(State(app): State<App>) -> Result<Json<Vec<McpServerInfo>>> {
    app.db
        .call(|s| {
            Ok(Json(
                s.conn
                    .prepare_cached(&format!("{SELECT} WHERE deleted=0 ORDER BY id"))?
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
        .call(move |s| {
            let tx = s.conn.transaction()?;
            let (value, auth_changed, detached_from) = save(&tx, Some((id, &mut s.mcp)), fields)?;
            tx.commit()?;
            if detached_from.is_some() || auth_changed || !value.enabled {
                s.mcp.oauth().cancel_login(id);
            }
            s.mcp.forget(id);
            Ok(Json(value))
        })
        .await
}

async fn delete_server(State(app): State<App>, Path(id): Path<McpServerId>) -> Result<StatusCode> {
    app.db
        .write(
            move |conn| {
                if !crate::cluster::ledger::tombstone_shared_mcp(conn, id)? {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
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
