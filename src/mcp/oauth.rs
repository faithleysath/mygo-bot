mod client;
mod flow;
mod revoke;
mod storage;
mod transport;
pub(in crate::mcp) use client::header_has_bearer_challenge;
pub(super) use transport::HttpClient;

use crate::{App, Error, Result, id::McpServerId, store::Db};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use reqwest::Url;
use rusqlite::{OptionalExtension, params};
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct PendingRefreshState {
    pub coordinator: String,
    pub fp: String,
}

impl PendingRefreshState {
    pub(crate) fn clear_if_current(
        &self,
        conn: &rusqlite::Connection,
        id: McpServerId,
        generation: &str,
    ) -> Result<()> {
        conn.execute(
            "UPDATE mcp_oauth SET pending_refresh=NULL WHERE server_id=?1 AND generation=?2
             AND json_extract(pending_refresh,'$.coordinator')=?3 AND json_extract(pending_refresh,'$.fp')=?4",
            params![id, generation, self.coordinator, self.fp],
        )?;
        Ok(())
    }
}

pub(crate) fn credentials_fingerprint(raw_credentials: Option<&str>) -> Option<String> {
    let raw = raw_credentials?;
    let creds = decode::<client::StoredCredentials>(raw).ok()?;
    let tr = creds.token_response.as_ref()?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mygo-oauth-cred-fp-v1\0");
    let update_opt_str = |h: &mut blake3::Hasher, s: Option<&str>| match s {
        Some(v) => {
            h.update(&[1]);
            h.update(&(v.len() as u64).to_le_bytes());
            h.update(v.as_bytes());
        }
        None => {
            h.update(&[0]);
        }
    };
    update_opt_str(&mut hasher, tr.refresh_token.as_deref());
    update_opt_str(&mut hasher, Some(tr.access_token.as_str()));
    match tr.expires_in {
        Some(v) => {
            hasher.update(&[1]);
            hasher.update(&v.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    match creds.token_received_at {
        Some(v) => {
            hasher.update(&[1]);
            hasher.update(&v.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

pub(crate) fn credentials_need_refresh(raw_credentials: Option<&str>, rejected: bool) -> bool {
    let Some(raw) = raw_credentials else {
        return false;
    };
    let Ok(creds) = decode::<client::StoredCredentials>(raw) else {
        return false;
    };
    storage::needs_refresh(&creds, rejected)
}

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
    listeners: HashMap<McpServerId, (String, CancellationToken)>,
    refresh: HashMap<String, Weak<Mutex<()>>>,
}
impl Runtime {
    pub(super) fn cancel_login(&mut self, id: McpServerId) {
        if let Some((_, stop)) = self.listeners.remove(&id) {
            stop.cancel();
        }
    }
}

impl super::Connections {
    pub(crate) fn refresh_gate(&mut self, generation: &str) -> Arc<Mutex<()>> {
        let refresh = &mut self.oauth.refresh;
        refresh.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = refresh.get(generation).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        refresh.insert(generation.to_owned(), Arc::downgrade(&gate));
        gate
    }
}

// Called on the SQLite worker in the same operation that copies the grant.
// The generation gate also covers an issuer request whose response is still in flight.
pub(crate) fn guard_grant_copy(
    conn: &rusqlite::Connection,
    mcp: &mut super::Connections,
    id: McpServerId,
) -> Result<Option<tokio::sync::OwnedMutexGuard<()>>> {
    let row: Option<(String, bool)> = conn
        .prepare_cached(
            "SELECT generation,pending_refresh IS NOT NULL FROM mcp_oauth
             JOIN mcp_servers ON id=server_id WHERE server_id=? AND shared=1 AND deleted=0",
        )?
        .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let Some((generation, pending)) = row else {
        return Ok(None);
    };
    if pending {
        return Err(Error::conflict("OAuth refresh is awaiting confirmation; retry detaching later"));
    }
    mcp.refresh_gate(&generation)
        .try_lock_owned()
        .map(Some)
        .map_err(|_| Error::conflict("OAuth refresh in progress; retry detaching later"))
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

async fn status(State(app): State<App>, Path(id): Path<McpServerId>) -> Result<Json<Status>> {
    app.db
        .call(move |s| {
            let sql = format!(
                "SELECT json_type(config_json,'$.oauth') IS 'object', {},
             json_extract(pending,'$.created_at')+?2>unixepoch()
             FROM mcp_servers LEFT JOIN mcp_oauth ON id=server_id WHERE id=?1 AND deleted=0",
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

async fn login(State(app): State<App>, Path(id): Path<McpServerId>) -> Result<Json<Login>> {
    flow::start(&app, id).await.map(Json)
}

pub(crate) async fn complete_local(db: &Db, id: McpServerId, url: &str) -> Result<()> {
    flow::complete(db, id, url, None).await
}

async fn callback(
    State(app): State<App>,
    Path(id): Path<McpServerId>,
    Json(body): Json<Callback>,
) -> Result<StatusCode> {
    if let Some(initiator_node_id) = flow::callback_initiator_node(&body.url)? {
        let local_node_id = app.exec.presence().local_node_id();
        if initiator_node_id != local_node_id {
            let target = initiator_node_id.clone();
            let is_peer = app
                .db
                .call(move |s| {
                    Ok(crate::cluster::identity::list_peers(&s.conn)?
                        .into_iter()
                        .any(|p| p.node_id == target))
                })
                .await?;
            if is_peer {
                app.exec
                    .forward_oauth_callback(&initiator_node_id, id, body.url)
                    .await?;
                return Ok(StatusCode::NO_CONTENT);
            }
        }
    }
    flow::complete(&app.db, id, &body.url, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn logout(State(app): State<App>, Path(id): Path<McpServerId>) -> Result<StatusCode> {
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

pub(crate) struct CoordinatorRefreshOutcome {
    pub status: crate::cluster::sync::OAuthRefreshStatus,
    pub server: Option<crate::cluster::ledger::McpServerWire>,
    pub error: Option<String>,
}

pub(crate) async fn refresh_shared_as_coordinator(
    db: &Db,
    _client: &reqwest::Client,
    id: McpServerId,
    generation: &str,
    rejected_token_hash: Option<[u8; 32]>,
) -> Result<CoordinatorRefreshOutcome> {
    use crate::cluster::{ledger, sync::OAuthRefreshStatus};

    let gen_owned = generation.to_owned();
    let gate = db
        .call(move |s| Ok(s.mcp.refresh_gate(&gen_owned)))
        .await?;
    let _guard = gate.lock_owned().await;

    let gen_owned = generation.to_owned();
    enum Precheck {
        Early(CoordinatorRefreshOutcome),
        Proceed {
            revision: i64,
            auth_config_ts: String,
            auth_fingerprint: String,
            binding: Binding,
            endpoint: String,
            reject_token: Option<String>,
            needs_refresh: bool,
        },
    }

    let pre = db
        .call(move |s| {
            struct PreRow {
                revision: i64,
                transport: String,
                config_json: String,
                m_auth_ts: String,
                o_auth_ts: String,
                o_fp: String,
                binding: String,
                credentials: Option<String>,
                rejected: bool,
                authorized: bool,
                pending_refresh: bool,
            }
            let row = s
                .conn
                .query_row(
                    &format!(
                        "SELECT m.revision, m.transport, m.config_json, m.auth_config_ts,
                                o.auth_config_ts, o.auth_fingerprint, o.binding,
                                o.credentials, o.rejected, ({}), o.pending_refresh IS NOT NULL
                         FROM mcp_servers m JOIN mcp_oauth o ON o.server_id=m.id
                         WHERE m.id=?1 AND m.shared=1 AND m.deleted=0 AND o.generation=?2",
                        storage::AUTHORIZED_SQL
                    ),
                    params![id, gen_owned],
                    |r| {
                        Ok(PreRow {
                            revision: r.get(0)?,
                            transport: r.get(1)?,
                            config_json: r.get(2)?,
                            m_auth_ts: r.get(3)?,
                            o_auth_ts: r.get(4)?,
                            o_fp: r.get(5)?,
                            binding: r.get(6)?,
                            credentials: r.get(7)?,
                            rejected: r.get(8)?,
                            authorized: r.get(9)?,
                            pending_refresh: r.get(10)?,
                        })
                    },
                )
                .optional()?
                .and_then(|r| {
                    let live_fp = crate::cluster::ledger::auth_config_fingerprint(
                        &r.transport,
                        &r.config_json,
                    );
                    if r.m_auth_ts != r.o_auth_ts || r.o_fp != live_fp {
                        return None;
                    }
                    let endpoint = serde_json::from_str::<serde_json::Value>(&r.config_json)
                        .ok()
                        .and_then(|v| v.get("url").and_then(|u| u.as_str()).map(str::to_owned))?;
                    Some((r, endpoint))
                });

            let Some((row, endpoint)) = row
            else {
                return Ok(Precheck::Early(CoordinatorRefreshOutcome {
                    status: OAuthRefreshStatus::ReauthRequired,
                    server: ledger::load_mcp_wire(&s.conn, id)?,
                    error: Some("MCP OAuth login changed or missing".into()),
                }));
            };
            let PreRow {
                revision,
                m_auth_ts: auth_config_ts,
                o_fp: auth_fingerprint,
                binding: raw_binding,
                credentials: raw_creds,
                rejected,
                authorized,
                pending_refresh,
                ..
            } = row;
            if pending_refresh {
                return Err(Error::conflict("OAuth refresh is awaiting confirmation"));
            }
            let Some(raw_creds) = raw_creds else {
                return Ok(Precheck::Early(CoordinatorRefreshOutcome {
                    status: OAuthRefreshStatus::ReauthRequired,
                    server: ledger::load_mcp_wire(&s.conn, id)?,
                    error: Some("MCP OAuth credentials missing".into()),
                }));
            };
            let binding = decode::<Binding>(&raw_binding)?;
            let creds = decode::<client::StoredCredentials>(&raw_creds)?;
            let matching_rejected = rejected_token_hash.is_some_and(|rh| {
                creds
                    .token_response
                    .as_ref()
                    .is_some_and(|t| *blake3::hash(t.access_token.as_bytes()).as_bytes() == rh)
            });
            let reject_token = (matching_rejected && !rejected)
                .then(|| creds.token_response.as_ref().map(|t| t.access_token.clone()))
                .flatten();
            if !authorized && !matching_rejected {
                return Ok(Precheck::Early(CoordinatorRefreshOutcome {
                    status: OAuthRefreshStatus::ReauthRequired,
                    server: ledger::load_mcp_wire(&s.conn, id)?,
                    error: Some("MCP OAuth re-authentication required".into()),
                }));
            }
            let needs_refresh = storage::needs_refresh(&creds, rejected || matching_rejected);
            Ok(Precheck::Proceed {
                revision,
                auth_config_ts,
                auth_fingerprint,
                binding,
                endpoint,
                reject_token,
                needs_refresh,
            })
        })
        .await?;

    let status = match pre {
        Precheck::Early(out) => return Ok(out),
        Precheck::Proceed {
            revision,
            auth_config_ts,
            auth_fingerprint,
            binding,
            endpoint,
            reject_token,
            needs_refresh,
        } => {
            if !needs_refresh {
                OAuthRefreshStatus::UpToDate
            } else {
                let account = Account {
                    db: db.clone(),
                    id,
                    revision,
                    generation: generation.to_owned(),
                    auth_config_ts,
                    auth_fingerprint,
                    allow_disabled: true,
                };
                if let Some(tok) = reject_token {
                    account.reject(tok).await;
                }
                let base = flow::discovery_base(&endpoint)?;
                let oauth_http = client::OAuthHttp::new(&base).map_err(Error::internal)?;
                match storage::acquire_valid_token(&oauth_http, &binding, &account).await {
                    Ok(_) => OAuthRefreshStatus::Refreshed,
                    Err(true) => OAuthRefreshStatus::ReauthRequired,
                    Err(false) => {
                        return Err(Error::internal("MCP OAuth token refresh failed"));
                    }
                }
            }
        }
    };

    let server = db.call(move |s| ledger::load_mcp_wire(&s.conn, id)).await?;
    Ok(CoordinatorRefreshOutcome {
        status,
        server,
        error: None,
    })
}
