mod flow;
mod revoke;
mod storage;
mod transport;
pub(super) use transport::HttpClient;

use crate::{App, Error, Result, store::Db};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use reqwest::Url;
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, AuthorizationMetadata, OAuthClientConfig,
};
use rusqlite::params;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
    sync::{Arc, Weak},
    time::Duration,
};
use storage::Account;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const LOGIN_TTL: u32 = 600;

#[derive(Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = McpOAuthConfig)]
pub(super) struct Config {
    /// 预注册的客户端 ID。省略时使用动态注册。
    pub client_id: Option<String>,
    #[schema(write_only)]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    /// 显式回调地址通过管理 API 提交。省略时自动监听随机本地端口。
    pub redirect_uri: Option<String>,
}

pub(super) fn validate_endpoint(url: &Url) -> Result<()> {
    let host = url.host_str().unwrap_or_default();
    let loopback = host == "localhost"
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
        || host.is_empty()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::bad(
            "OAuth requires HTTPS or HTTP loopback without user info or fragment",
        ));
    }
    Ok(())
}

impl Config {
    pub(super) fn validate(&self) -> Result<()> {
        if self
            .client_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty())
            || (self.client_secret.is_some() && self.client_id.is_none())
        {
            return Err(Error::bad(
                "expected an OAuth client ID when providing client credentials",
            ));
        }
        if self.scopes.iter().any(|scope| {
            scope.is_empty()
                || !scope
                    .bytes()
                    .all(|c| matches!(c, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        }) {
            return Err(Error::bad("invalid OAuth scope"));
        }
        if let Some(uri) = &self.redirect_uri {
            let url = parse(uri)?;
            validate_endpoint(&url)?;
            if url.query().is_some() {
                return Err(Error::bad("OAuth redirect URI cannot include a query"));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct Runtime {
    listeners: HashMap<i64, (String, CancellationToken)>,
    refresh: HashMap<String, Weak<Mutex<()>>>,
}
impl Runtime {
    pub(super) fn cancel_login(&mut self, id: i64) {
        if let Some((_, stop)) = self.listeners.remove(&id) {
            stop.cancel();
        }
    }
    fn refresh_lock(&mut self, generation: &str) -> Arc<Mutex<()>> {
        self.refresh.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = self.refresh.get(generation).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        self.refresh
            .insert(generation.to_owned(), Arc::downgrade(&gate));
        gate
    }
}

fn parse(value: &str) -> Result<Url> {
    Url::parse(value).map_err(|_| Error::bad("invalid OAuth URL"))
}
fn encode(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|_| Error::internal("failed to encode MCP OAuth data"))
}
fn decode<T: DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|_| Error::internal("invalid stored MCP OAuth data"))
}

// Restore the exact token endpoint, issuer, client and resource selected during authorization.
// Discovery never silently retargets a previously granted refresh token.
#[derive(Serialize, Deserialize)]
struct Binding {
    resource: String,
    metadata: AuthorizationMetadata,
    client_id: String,
    client_secret: Option<String>,
    redirect_uri: String,
    discovery_challenge: Option<String>,
}

impl Binding {
    async fn manager(&self, endpoint: &str, account: &Account) -> Result<AuthorizationManager> {
        let resource = parse(&self.resource)?;
        let canonical = resource.as_str() == self.resource;
        let base = if canonical {
            resource
        } else {
            flow::discovery_base(endpoint)?
        };
        let mut manager = AuthorizationManager::new(base)
            .await
            .map_err(|_| Error::internal("OAuth client initialization failed"))?;
        if !canonical {
            // RFC 8707 indicators are strings. A root URL without a trailing slash is common;
            // only the SDK's discovered-resource path preserves its exact spelling on token requests.
            let resolution = manager
                .resolve_metadata_from_challenge(self.discovery_challenge.as_deref())
                .await
                .map_err(|_| Error::bad("OAuth resource discovery failed"))?;
            if resolution.metadata.issuer != self.metadata.issuer {
                return Err(Error::bad("OAuth issuer changed; login required"));
            }
        }
        manager.set_metadata(self.metadata.clone());
        let mut cfg = OAuthClientConfig::new(&self.client_id, &self.redirect_uri);
        cfg.client_secret.clone_from(&self.client_secret);
        manager
            .configure_client(cfg)
            .map_err(|_| Error::bad("invalid OAuth client metadata"))?;
        if !canonical {
            let url = manager
                .get_authorization_url(&[])
                .await
                .map_err(|_| Error::bad("OAuth resource validation failed"))?;
            if flow::parameter(&parse(&url)?, "resource")? != self.resource {
                return Err(Error::bad("OAuth resource changed; login required"));
            }
        }
        manager.set_state_store(account.clone());
        manager.set_credential_store(account.clone());
        Ok(manager)
    }
}

pub(super) fn routes(documented: bool) -> crate::docs::RouteRegistry {
    crate::docs::RouteRegistry::new(documented)
        .register(crate::docs::register!(status, logout))
        .register(crate::docs::register!(login))
        .register(crate::docs::register!(callback))
}

#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = McpOAuthLogin)]
struct Login {
    authorization_url: String,
    redirect_uri: String,
    /// Unix 时间戳（秒）。
    expires_at: u64,
}
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = McpOAuthCallback)]
struct Callback {
    url: String,
}
#[derive(Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
enum OAuthStatus {
    Disabled,
    Required,
    Pending,
    Authorized,
}
#[derive(Serialize, utoipa::ToSchema)]
#[schema(as = McpOAuthStatus)]
struct Status {
    status: OAuthStatus,
}

/// 查询 OAuth 登录状态。凭据不会出现在响应中。
#[utoipa::path(get,path="/mcp/servers/{id}/oauth",tag="MCP",params(("id"=i64,Path)),responses((status=200,body=Status),(status=404,body=crate::ErrorBody),crate::docs::ServiceErrors))]
async fn status(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Status>> {
    app.db
        .call(move |s| {
            let sql = format!(
                "SELECT json_type(config_json,'$.oauth') IS 'object', {},
             json_extract(pending,'$.created_at')+?2>unixepoch()
             FROM mcp_servers LEFT JOIN mcp_oauth ON id=server_id WHERE id=?1",
                storage::AUTHORIZED_SQL
            );
            let status = s.conn.query_row(&sql, params![id, LOGIN_TTL], |row| {
                Ok(if !row.get::<_, bool>(0)? {
                    OAuthStatus::Disabled
                } else if row.get::<_, bool>(1)? {
                    OAuthStatus::Authorized
                } else if row.get::<_, Option<bool>>(2)?.unwrap_or(false) {
                    OAuthStatus::Pending
                } else {
                    OAuthStatus::Required
                })
            })?;
            Ok(Json(Status { status }))
        })
        .await
}

/// 获取授权地址。在浏览器中完成登录，默认使用 10 分钟有效的本地回调。
#[utoipa::path(post,path="/mcp/servers/{id}/oauth/login",tag="MCP",params(("id"=i64,Path)),responses((status=200,body=Login),(status=400,body=crate::ErrorBody),(status=404,body=crate::ErrorBody),(status=409,body=crate::ErrorBody),crate::docs::ServiceErrors))]
async fn login(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Login>> {
    flow::start(&app, id).await.map(Json)
}

/// 手动提交浏览器重定向后的完整 URL，适用于浏览器和 daemon 在不同机器上。
#[utoipa::path(post,path="/mcp/servers/{id}/oauth/callback",tag="MCP",params(("id"=i64,Path)),request_body=Callback,responses((status=204,description="已登录"),(status=400,body=crate::ErrorBody),(status=404,body=crate::ErrorBody),(status=409,body=crate::ErrorBody),crate::docs::ServiceErrors,crate::docs::JsonBodyErrors))]
async fn callback(
    State(app): State<App>,
    Path(id): Path<i64>,
    Json(body): Json<Callback>,
) -> Result<StatusCode> {
    flow::complete(&app.db, id, &body.url, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// 清除本地凭据、取消登录并关闭缓存连接。授权服务器声明撤销端点时，同时撤销远端令牌。
#[utoipa::path(delete,path="/mcp/servers/{id}/oauth",tag="MCP",params(("id"=i64,Path)),responses((status=204,description="已登出"),(status=404,body=crate::ErrorBody),(status=409,description="等待期间已开始新登录",body=crate::ErrorBody),(status=502,description="本地已登出，远端撤销未确认",body=crate::ErrorBody),crate::docs::ServiceErrors))]
async fn logout(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    // An accepted logout must finish revocation even if the HTTP caller disconnects.
    app.workers
        .clone()
        .spawn(async move { revoke::logout(&app, id).await })
        .await
        .map_err(|_| {
            Error::internal("OAuth logout interrupted; check the local login and remote grant")
        })??;
    Ok(StatusCode::NO_CONTENT)
}
