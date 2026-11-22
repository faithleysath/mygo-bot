use super::{oauth, stdio};
use crate::{App, Error, Result};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use rusqlite::{Connection, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub fn routes() -> utoipa_axum::router::OpenApiRouter<App> {
    use utoipa_axum::{router::OpenApiRouter, routes};
    OpenApiRouter::new()
        .routes(routes!(list_servers, create_server))
        .routes(routes!(get_server, patch_server, delete_server))
        .merge(oauth::routes())
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, utoipa::ToSchema, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum McpTransport {
    #[default]
    StreamableHttp,
    Stdio,
}

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct McpServerConfig {
    /// 稳定的服务名，创建后不可改名。
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "enabled")]
    enabled: bool,
    #[serde(default)]
    transport: McpTransport,
    /// HTTP(S) 服务地址。查询参数只写。
    url: Option<String>,
    /// 静态 HTTP 请求头。PATCH 省略时保留，空对象清除。
    #[serde(default)]
    #[schema(write_only)]
    headers: BTreeMap<String, String>,
    /// OAuth 客户端设置。传对象整体替换，null 关闭。
    #[schema(write_only)]
    oauth: Option<oauth::Config>,
    /// stdio 可执行文件，直接按 argv 启动。
    command: Option<String>,
    #[serde(default)]
    #[schema(write_only)]
    args: Vec<String>,
    #[serde(default)]
    #[schema(write_only)]
    env: BTreeMap<String, String>,
    cwd: Option<String>,
    /// null 表示所有会话，空数组表示所有会话都不可访问。
    session_ids: Option<BTreeSet<i64>>,
    /// 精确工具原名，null 表示全部，空数组禁止工具调用。
    allowed_tools: Option<BTreeSet<String>>,
    #[serde(default = "timeout_ms")]
    #[schema(minimum = 1, maximum = 86400000, default = 60000)]
    timeout_ms: u32,
}
fn enabled() -> bool {
    true
}
fn timeout_ms() -> u32 {
    60_000
}
type McpServerPatch = crate::docs::Patch<McpServerConfig>;
type Fields = Json<Map<String, Value>>;

#[derive(Serialize, utoipa::ToSchema)]
struct McpServerInfo {
    id: i64,
    name: String,
    description: String,
    enabled: bool,
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
    session_ids: Option<BTreeSet<i64>>,
    allowed_tools: Option<BTreeSet<String>>,
    timeout_ms: u32,
    revision: i64,
}
pub(super) struct Endpoint {
    pub id: i64,
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
    id: i64,
    revision: i64,
    cfg: McpServerConfig,
}
const SELECT: &str = "SELECT id,name,description,enabled,transport,config_json,session_ids,allowed_tools,timeout_ms,revision FROM mcp_servers";
// Resolve the caller's session in the host, never from the script's arguments.
const VISIBLE: &str = "enabled AND (session_ids IS NULL OR EXISTS (SELECT 1 FROM json_each(session_ids) WHERE value=?1))";
fn stored(r: &Row) -> rusqlite::Result<Stored> {
    let mut value: Value = r.get(5)?;
    let obj = value.as_object_mut().ok_or(rusqlite::Error::InvalidQuery)?;
    obj.insert("name".into(), json!(r.get::<_, String>(1)?));
    obj.insert("description".into(), json!(r.get::<_, String>(2)?));
    obj.insert("enabled".into(), json!(r.get::<_, bool>(3)?));
    obj.insert("transport".into(), json!(r.get::<_, String>(4)?));
    obj.insert(
        "session_ids".into(),
        r.get::<_, Option<Value>>(6)?.unwrap_or(Value::Null),
    );
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
            transport: c.transport,
            url,
            has_url_query,
            has_headers: !c.headers.is_empty(),
            has_oauth: c.oauth.is_some(),
            command: c.command,
            arg_count: c.args.len(),
            has_env: !c.env.is_empty(),
            cwd: c.cwd,
            session_ids: c.session_ids,
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
fn load(conn: &Connection, id: i64) -> Result<Stored> {
    Ok(conn
        .prepare_cached(&format!("{SELECT} WHERE id=?"))?
        .query_row([id], stored)?)
}
fn by_id(conn: &Connection, id: i64) -> Result<McpServerInfo> {
    Ok(load(conn, id)?.info())
}
pub(super) fn endpoint_by_id(conn: &Connection, id: i64) -> Result<Endpoint> {
    load(conn, id)?.endpoint(true)
}
pub(super) fn endpoint(conn: &Connection, sid: i64, name: &str) -> Result<Endpoint> {
    conn.prepare_cached(&format!("{SELECT} WHERE {VISIBLE} AND name=?2"))?
        .query_row((sid, name), stored)
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                Error::bad("MCP server is unknown or disabled for this session")
            }
            other => other.into(),
        })?
        .endpoint(false)
}
pub(super) fn enabled_servers(conn: &Connection, sid: i64) -> Result<Value> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT name,description FROM mcp_servers WHERE {VISIBLE} ORDER BY name"
    ))?;
    let mut rows = stmt.query([sid])?;
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
fn validate(conn: &Connection, c: &mut McpServerConfig) -> Result<()> {
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
    if let Some(ids) = &c.session_ids {
        let sql = "SELECT EXISTS(SELECT 1 FROM json_each(?1) scope
                   WHERE NOT EXISTS(SELECT 1 FROM sessions WHERE sessions.id=scope.value))";
        let missing: bool = conn.query_row(sql, [json!(ids)], |row| row.get(0))?;
        if missing {
            return Err(Error::bad("unknown session in session_ids"));
        }
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
fn save(conn: &Connection, id: Option<i64>, fields: Fields) -> Result<(McpServerInfo, bool)> {
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
    validate(conn, &mut c)?;
    let connection = connection_document(&c);
    let auth_changed = old.as_ref().is_some_and(|s| {
        s.cfg.transport != c.transport || connection_document(&s.cfg) != connection
    });
    let sql = "INSERT INTO mcp_servers(id,name,description,enabled,transport,config_json,session_ids,allowed_tools,timeout_ms,revision)
        VALUES(?,?,?,?,?,?,?,?,?,1) ON CONFLICT(id) DO UPDATE SET description=excluded.description,enabled=excluded.enabled,
        transport=excluded.transport,config_json=excluded.config_json,session_ids=excluded.session_ids,allowed_tools=excluded.allowed_tools,
        timeout_ms=excluded.timeout_ms,revision=mcp_servers.revision+1 RETURNING id";
    let transport = match c.transport {
        McpTransport::StreamableHttp => "streamable-http",
        McpTransport::Stdio => "stdio",
    };
    let id = conn
        .query_row(
            sql,
            params![
                id,
                c.name,
                c.description,
                c.enabled,
                transport,
                connection,
                c.session_ids.as_ref().map(|ids| json!(ids)),
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
    Ok((by_id(conn, id)?, auth_changed))
}

/// 列出 MCP 服务配置。只显示是否设置了请求头。
#[utoipa::path(get, path="/mcp/servers", tag="MCP", responses((status=200,body=Vec<McpServerInfo>), crate::docs::ServiceErrors))]
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
/// 读取 MCP 服务配置。
#[utoipa::path(get, path="/mcp/servers/{id}", tag="MCP", params(("id"=i64, Path)), responses((status=200,body=McpServerInfo),(status=404,body=crate::ErrorBody),crate::docs::ServiceErrors))]
async fn get_server(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<McpServerInfo>> {
    app.db.call(move |s| by_id(&s.conn, id).map(Json)).await
}
/// 创建 Streamable HTTP 或 stdio 服务。默认启用，超时 60 秒。
#[utoipa::path(post, path="/mcp/servers", tag="MCP", request_body=McpServerConfig, responses((status=201,body=McpServerInfo),(status=400,body=crate::ErrorBody),(status=409,body=crate::ErrorBody),crate::docs::ServiceErrors,crate::docs::JsonBodyErrors))]
async fn create_server(
    State(app): State<App>,
    fields: Fields,
) -> Result<(StatusCode, Json<McpServerInfo>)> {
    app.db
        .write(
            move |conn| save(conn, None, fields),
            |_, (value, _)| Ok((StatusCode::CREATED, Json(value))),
        )
        .await
}
/// 更新服务。已发出的调用可收尾，后续调用使用新配置。
#[utoipa::path(patch, path="/mcp/servers/{id}", tag="MCP", params(("id"=i64, Path)),request_body=McpServerPatch, responses((status=200,body=McpServerInfo),(status=400,body=crate::ErrorBody),(status=404,body=crate::ErrorBody),crate::docs::ServiceErrors,crate::docs::JsonBodyErrors))]
async fn patch_server(
    State(app): State<App>,
    Path(id): Path<i64>,
    fields: Fields,
) -> Result<Json<McpServerInfo>> {
    app.db
        .write(
            move |conn| save(conn, Some(id), fields),
            move |s, (value, auth_changed)| {
                if auth_changed || !value.enabled {
                    s.mcp.oauth().cancel_login(id);
                }
                s.mcp.forget(id);
                Ok(Json(value))
            },
        )
        .await
}
/// 删除服务及其配置。
#[utoipa::path(delete, path="/mcp/servers/{id}", tag="MCP", params(("id"=i64, Path)), responses((status=204,description="已删除"),(status=404,body=crate::ErrorBody),crate::docs::ServiceErrors))]
async fn delete_server(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    app.db
        .write(
            move |conn| {
                if conn.execute("DELETE FROM mcp_servers WHERE id=?", [id])? == 0 {
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
