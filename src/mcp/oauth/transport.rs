use super::client::OAuthHttp;
use super::*;
use crate::cluster::Cluster;
use crate::mcp::client::McpError;

struct Authorization {
    endpoint: String,
    oauth_http: OAuthHttp,
    binding: Binding,
    account: Account,
    gate: Arc<tokio::sync::Mutex<()>>,
    cluster: Arc<Cluster>,
}

#[derive(Clone)]
pub(in crate::mcp) struct HttpClient {
    pub(in crate::mcp) http: reqwest::Client,
    authorization: Option<Arc<Authorization>>,
}

impl HttpClient {
    pub(in crate::mcp) async fn new(
        http: reqwest::Client,
        db: Db,
        cluster: Arc<Cluster>,
        id: McpServerId,
        revision: i64,
        oauth_enabled: bool,
        allow_disabled: bool,
    ) -> std::result::Result<Self, String> {
        let authorization = if oauth_enabled {
            let (generation, binding, endpoint, gate, auth_config_ts, auth_fingerprint) = db
                .call(move |s| {
                    let sql = format!(
                        "SELECT o.generation, o.binding, m.transport, m.config_json,
                                m.auth_config_ts, o.auth_config_ts, o.auth_fingerprint
                         FROM mcp_oauth o JOIN mcp_servers m ON m.id=o.server_id
                         WHERE m.id=? AND m.deleted=0 AND m.revision=? AND (m.enabled OR ?) AND {}",
                        storage::AUTHORIZED_SQL
                    );
                    let (generation, raw, transport, config_json, m_auth_ts, o_auth_ts, o_fp): (
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                    ) = s
                        .conn
                        .query_row(&sql, params![id, revision, allow_disabled], |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                            ))
                        })
                        .map_err(|e| match e {
                            rusqlite::Error::QueryReturnedNoRows => {
                                Error::bad("MCP OAuth login required")
                            }
                            other => other.into(),
                        })?;
                    let live_fp =
                        crate::cluster::ledger::auth_config_fingerprint(&transport, &config_json);
                    if m_auth_ts != o_auth_ts || o_fp != live_fp {
                        return Err(Error::bad("MCP OAuth login required"));
                    }
                    let endpoint = serde_json::from_str::<serde_json::Value>(&config_json)
                        .ok()
                        .and_then(|v| v.get("url").and_then(|u| u.as_str()).map(str::to_owned))
                        .ok_or_else(|| Error::bad("MCP OAuth login required"))?;
                    let gate = s.mcp.refresh_gate(&generation);
                    Ok((
                        generation,
                        decode::<Binding>(&raw)?,
                        endpoint,
                        gate,
                        m_auth_ts,
                        o_fp,
                    ))
                })
                .await
                .map_err(|e| e.to_string())?;
            let account = Account {
                db,
                id,
                revision,
                generation,
                auth_config_ts,
                auth_fingerprint,
                allow_disabled,
            };
            let base = flow::discovery_base(&endpoint).map_err(|e| e.to_string())?;
            let oauth_http = OAuthHttp::new(&base)?;
            Some(Arc::new(Authorization {
                endpoint,
                oauth_http,
                binding,
                account,
                gate,
                cluster,
            }))
        } else {
            None
        };
        Ok(Self {
            http,
            authorization,
        })
    }

    pub(in crate::mcp) async fn acquire_token(
        &self,
        uri: &str,
    ) -> std::result::Result<Option<String>, McpError> {
        let Some(auth) = &self.authorization else {
            return Ok(None);
        };
        if auth.endpoint != uri {
            return Err(McpError::AuthRequired);
        }
        if let Ok(Some((creds, rejected))) = auth.account.load_credentials().await
            && storage::needs_refresh(&creds, rejected)
        {
            let rejected_hash = rejected
                .then(|| creds.token_response.as_ref())
                .flatten()
                .map(|tr| *blake3::hash(tr.access_token.as_bytes()).as_bytes());
            match auth
                .cluster
                .coordinate_mcp_oauth_refresh(
                    auth.account.id,
                    &auth.account.generation,
                    rejected_hash,
                )
                .await
            {
                Ok(crate::cluster::sync::RefreshCoordination::Delegated {
                    reauth_required: true,
                })
                | Err(_) => return Err(McpError::AuthRequired),
                Ok(
                    crate::cluster::sync::RefreshCoordination::PendingConfirmation
                    | crate::cluster::sync::RefreshCoordination::Delegated {
                        reauth_required: false,
                    },
                ) => {
                    if let Ok(Some((creds, rejected))) = auth.account.load_credentials().await
                        && !storage::needs_refresh(&creds, rejected)
                        && let Some(tr) = creds.token_response
                    {
                        return Ok(Some(tr.access_token));
                    }
                    // Never fall back to local refresh after delegating unless the coordinator
                    // explicitly returned control via RefreshCoordination::Local (P1 #5).
                    return Err(McpError::TransportClosed);
                }
                Ok(crate::cluster::sync::RefreshCoordination::Local) => {}
            }
        }
        // Queued requests recheck expiry after the previous refresh commits. Once token
        // acquisition starts, finish a possible refresh even if this MCP operation times out:
        // an issuer can rotate its refresh token before the HTTP response reaches us.
        let guard = auth.gate.clone().lock_owned().await;
        let auth = auth.clone();
        let token = tokio::spawn(async move {
            let _guard = guard;
            tokio::time::timeout(
                Duration::from_secs(45),
                storage::acquire_valid_token(&auth.oauth_http, &auth.binding, &auth.account),
            )
            .await
            .map_err(|_| false)?
        })
        .await
        .map_err(|_| McpError::TransportClosed)?
        .map_err(|reauth| {
            if reauth {
                McpError::AuthRequired
            } else {
                McpError::TransportClosed
            }
        })?;
        Ok(Some(token))
    }

    pub(in crate::mcp) fn attach_bearer(
        headers: &mut reqwest::header::HeaderMap,
        token: &str,
    ) -> std::result::Result<(), McpError> {
        client::attach_bearer(headers, token).map_err(|()| McpError::InvalidResponse)
    }

    pub(in crate::mcp) async fn on_unauthorized(&self, token: Option<String>) {
        if let (Some(auth), Some(token)) = (&self.authorization, token) {
            auth.account.reject(token).await;
        }
    }
}
