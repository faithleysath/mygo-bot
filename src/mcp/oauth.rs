mod client;
mod flow;
mod revoke;
mod storage;
mod transport;
pub(in crate::mcp) use client::header_has_bearer_challenge;
pub(super) use transport::HttpClient;

use crate::{App, Error, Result, store::Db};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use reqwest::Url;
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

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    pub redirect_uri: Option<String>,
}

pub(super) fn validate_endpoint(url: &Url) -> Result<()> {
    let host = url.host_str().unwrap_or_default();
    let loopback = host == "localhost"
        || host.ends_with(".localhost")
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
    metadata: client::AuthorizationMetadata,
    client_id: String,
    client_secret: Option<String>,
    redirect_uri: String,
}

pub(super) fn routes() -> Router<App> {
    Router::new()
        .route("/mcp/servers/{id}/oauth", get(status).delete(logout))
        .route("/mcp/servers/{id}/oauth/login", post(login))
        .route("/mcp/servers/{id}/oauth/callback", post(callback))
}

#[derive(Serialize)]
struct Login {
    authorization_url: String,
    redirect_uri: String,
    expires_at: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Callback {
    url: String,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum OAuthStatus {
    Disabled,
    Required,
    Pending,
    Authorized,
}
#[derive(Serialize)]
struct Status {
    status: OAuthStatus,
}

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

async fn login(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Login>> {
    flow::start(&app, id).await.map(Json)
}

async fn callback(
    State(app): State<App>,
    Path(id): Path<i64>,
    Json(body): Json<Callback>,
) -> Result<StatusCode> {
    flow::complete(&app.db, id, &body.url, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

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
