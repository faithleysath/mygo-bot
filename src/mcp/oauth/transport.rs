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
            let (generation, binding, endpoint, gate) = db
                .call(move |s| {
                    let sql = format!(
                        "SELECT generation,binding,json_extract(config_json,'$.url')
                     FROM mcp_oauth JOIN mcp_servers ON id=server_id
                     WHERE id=? AND revision=? AND (enabled OR ?) AND {}",
                        storage::AUTHORIZED_SQL
                    );
                    let (generation, raw, endpoint): (String, String, String) = s
                        .conn
                        .query_row(&sql, params![id, revision, allow_disabled], |row| {
                            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                        })
                        .map_err(|e| match e {
                            rusqlite::Error::QueryReturnedNoRows => {
                                Error::bad("MCP OAuth login required")
                            }
                            other => other.into(),
                        })?;
                    let gate = s.mcp.oauth().refresh_lock(&generation);
                    Ok((generation, decode::<Binding>(&raw)?, endpoint, gate))
                })
                .await
                .map_err(|e| e.to_string())?;
            let account = Account {
                db,
                id,
                revision,
                generation,
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
            let _ = auth
                .cluster
                .coordinate_mcp_oauth_refresh(auth.account.id, &auth.account.generation)
                .await;
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
