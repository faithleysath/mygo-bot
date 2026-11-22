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
use std::collections::{BTreeMap, HashMap};

pub fn routes() -> utoipa_axum::router::OpenApiRouter<App> {
    use utoipa_axum::{router::OpenApiRouter, routes};
    OpenApiRouter::new()
        .routes(routes!(list_servers, create_server))
        .routes(routes!(get_server, patch_server, delete_server))
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, utoipa::ToSchema)]
enum McpTransport {
    #[default]
    #[serde(rename = "streamable-http")]
    StreamableHttp,
}

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct McpServerConfig {
    /// 稳定的工具服务名。创建后不可改名。
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "enabled")]
    enabled: bool,
    #[serde(default)]
    transport: McpTransport,
    url: String,
    /// 静态请求头。PATCH 省略时保留，传空对象清除。
    #[serde(default)]
    #[schema(write_only)]
    headers: BTreeMap<String, String>,
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
    /// 查询参数可能包含凭据，读取时省略。
    url: String,
    has_url_query: bool,
    has_headers: bool,
    timeout_ms: u32,
    revision: i64,
}

pub(super) struct Endpoint {
    pub id: i64,
    pub url: String,
    pub headers: HashMap<HeaderName, HeaderValue>,
    pub timeout_ms: u32,
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
        let mut value =
            HeaderValue::from_str(value).map_err(|_| Error::bad("invalid MCP header value"))?;
        value.set_sensitive(true);
        if headers.insert(name, value).is_some() {
            return Err(Error::bad("duplicate MCP header name"));
        }
    }
    Ok(headers)
}

const SELECT: &str = "SELECT id,name,description,enabled,json_extract(config_json,'$.url'),
    json_extract(config_json,'$.headers')!='{}',timeout_ms,revision FROM mcp_servers";
fn info(row: &Row) -> rusqlite::Result<McpServerInfo> {
    let url: String = row.get(4)?;
    let (url, query) = url
        .split_once('?')
        .map_or((url.as_str(), false), |(path, _)| (path, true));
    Ok(McpServerInfo {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        enabled: row.get(3)?,
        transport: McpTransport::StreamableHttp,
        url: url.to_owned(),
        has_url_query: query,
        has_headers: row.get(5)?,
        timeout_ms: row.get(6)?,
        revision: row.get(7)?,
    })
}
fn by_id(conn: &Connection, id: i64) -> Result<McpServerInfo> {
    Ok(conn
        .prepare_cached(&format!("{SELECT} WHERE id=?"))?
        .query_row([id], info)?)
}

pub(super) fn endpoint(conn: &Connection, name: &str) -> Result<Endpoint> {
    let sql = "SELECT id,json_extract(config_json,'$.url'),json_extract(config_json,'$.headers'),timeout_ms
               FROM mcp_servers WHERE name=? AND enabled";
    let (id, url, raw, timeout_ms): (_, _, String, _) = conn
        .prepare_cached(sql)?
        .query_row([name], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Error::bad("MCP server is unknown or disabled"),
            other => other.into(),
        })?;
    let headers =
        serde_json::from_str(&raw).map_err(|_| Error::internal("invalid stored MCP headers"))?;
    Ok(Endpoint {
        id,
        url,
        headers: parse_headers(&headers)?,
        timeout_ms,
    })
}

pub(super) fn enabled_servers(conn: &Connection) -> Result<Value> {
    let mut stmt = conn
        .prepare_cached("SELECT name,description FROM mcp_servers WHERE enabled ORDER BY name")?;
    let mut servers = Vec::new();
    let mut bytes = 0;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let server = json!({"name":row.get::<_,String>(0)?, "description":row.get::<_,String>(1)?});
        bytes += server.to_string().len() + 1;
        if bytes > super::BYTE_LIMIT {
            return Err(Error::bad("MCP server list exceeds byte limit"));
        }
        servers.push(server);
    }
    Ok(Value::Array(servers))
}

fn save(conn: &Connection, id: Option<i64>, fields: Fields) -> Result<McpServerInfo> {
    if fields.is_empty() {
        return Err(Error::bad("expected MCP configuration fields"));
    }
    let mut base = json!({});
    if let Some(id) = id {
        let old = by_id(conn, id)?;
        let config: Value = conn.query_row(
            "SELECT config_json FROM mcp_servers WHERE id=?",
            [id],
            |r| r.get(0),
        )?;
        if fields
            .get("name")
            .is_some_and(|v| v.as_str() != Some(&old.name))
        {
            return Err(Error::bad("MCP server name is immutable"));
        }
        if old.has_headers
            && !fields.contains_key("headers")
            && let Some(new) = fields.get("url").and_then(Value::as_str)
            && Url::parse(new).map(|u| u.origin()) != Url::parse(&old.url).map(|u| u.origin())
        {
            return Err(Error::bad(
                "set headers explicitly when changing the MCP origin",
            ));
        }
        base = json!({"name":old.name, "description":old.description, "enabled":old.enabled,
                      "url":config["url"], "headers":config["headers"], "timeout_ms":old.timeout_ms});
    }
    base.as_object_mut().unwrap().extend(fields.0);
    let cfg: McpServerConfig = serde_json::from_value(base)
        .map_err(|_| Error::bad("invalid MCP configuration fields or types"))?;
    let url = Url::parse(&cfg.url).map_err(|_| Error::bad("invalid MCP URL"))?;
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
    if cfg.name.trim().is_empty()
        || cfg.name.trim() != cfg.name
        || cfg.name.len() > 128
        || cfg.description.len() > 4096
        || !(1..=86_400_000).contains(&cfg.timeout_ms)
    {
        return Err(Error::bad(
            "invalid MCP name (1–128 bytes), description (up to 4096 bytes), or timeout (1–86400000 ms)",
        ));
    }
    parse_headers(&cfg.headers)?;
    let sql = "INSERT INTO mcp_servers(id,name,description,enabled,transport,config_json,timeout_ms,revision)
        VALUES(?,?,?,?,'streamable-http',?,?,1) ON CONFLICT(id) DO UPDATE SET description=excluded.description,
        enabled=excluded.enabled,config_json=excluded.config_json,timeout_ms=excluded.timeout_ms,revision=mcp_servers.revision+1 RETURNING id";
    let http = json!({"url":url.as_str(), "headers":cfg.headers});
    let id = conn
        .query_row(
            sql,
            params![
                id,
                cfg.name,
                cfg.description,
                cfg.enabled,
                http,
                cfg.timeout_ms
            ],
            |r| r.get(0),
        )
        .map_err(|e| match e.sqlite_error().map(|code| code.extended_code) {
            Some(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE) => {
                Error::conflict("MCP server name already exists")
            }
            _ => e.into(),
        })?;
    by_id(conn, id)
}

/// 列出 MCP 服务配置。只显示是否设置了请求头。
#[utoipa::path(get, path="/mcp/servers", tag="MCP", responses((status=200,body=Vec<McpServerInfo>), crate::docs::ServiceErrors))]
async fn list_servers(State(app): State<App>) -> Result<Json<Vec<McpServerInfo>>> {
    app.db
        .call(|s| {
            Ok(Json(
                s.conn
                    .prepare_cached(&format!("{SELECT} ORDER BY id"))?
                    .query_map([], info)?
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
/// 创建 Streamable HTTP 服务。默认启用，超时 60 秒。
#[utoipa::path(post, path="/mcp/servers", tag="MCP", request_body=McpServerConfig, responses((status=201,body=McpServerInfo),(status=400,body=crate::ErrorBody),(status=409,body=crate::ErrorBody),crate::docs::ServiceErrors,crate::docs::JsonBodyErrors))]
async fn create_server(
    State(app): State<App>,
    fields: Fields,
) -> Result<(StatusCode, Json<McpServerInfo>)> {
    app.db
        .write(
            move |conn| save(conn, None, fields),
            |_, value| Ok((StatusCode::CREATED, Json(value))),
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
            move |s, value| {
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
                s.mcp.forget(id);
                Ok(StatusCode::NO_CONTENT)
            },
        )
        .await
}
