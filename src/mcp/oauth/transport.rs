use super::*;
use futures_util::stream::BoxStream;
use reqwest::header::{HeaderName, HeaderValue};
use rmcp::{
    model::ClientJsonRpcMessage,
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
        StreamableHttpResponseLimits,
    },
};
use sse_stream::{Error as SseError, Sse};

type HttpResult<T> = std::result::Result<T, StreamableHttpError<reqwest::Error>>;
type Headers = HashMap<HeaderName, HeaderValue>;
type Events = BoxStream<'static, std::result::Result<Sse, SseError>>;

struct Authorization {
    endpoint: String,
    manager: AuthorizationManager,
    account: Account,
    gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
pub(in crate::mcp) struct HttpClient {
    http: reqwest::Client,
    authorization: Option<Arc<Authorization>>,
}

impl HttpClient {
    pub(in crate::mcp) async fn new(
        http: reqwest::Client,
        db: Db,
        id: i64,
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
            let manager = binding
                .manager(&endpoint, &account)
                .await
                .map_err(|e| e.to_string())?;
            Some(Arc::new(Authorization {
                endpoint,
                manager,
                account,
                gate,
            }))
        } else {
            None
        };
        Ok(Self {
            http,
            authorization,
        })
    }

    async fn token(&self, uri: &str, supplied: Option<String>) -> HttpResult<Option<String>> {
        let Some(auth) = &self.authorization else {
            return Ok(supplied);
        };
        if auth.endpoint != uri || supplied.is_some() {
            return Err(AuthError::AuthorizationRequired.into());
        }
        // Queued requests recheck expiry after the previous refresh commits. Once token
        // acquisition starts, finish a possible refresh even if this MCP operation times out:
        // an issuer can rotate its refresh token before the HTTP response reaches us.
        let guard = auth.gate.clone().lock_owned().await;
        let auth = auth.clone();
        let token = tokio::spawn(async move {
            let _guard = guard;
            // rmcp's HTTP timeout is 30s. Bound all SDK/store work as well, with
            // time left to commit a response received near that network deadline.
            tokio::time::timeout(Duration::from_secs(45), auth.manager.get_access_token())
                .await
                .map_err(|_| {
                    AuthError::TokenRefreshFailed("MCP OAuth token acquisition timed out".into())
                })?
        })
        .await??;
        Ok(Some(token))
    }

    async fn record<T>(&self, token: Option<String>, response: HttpResult<T>) -> HttpResult<T> {
        let rejected = match &response {
            Err(StreamableHttpError::AuthRequired(_)) => true,
            Err(StreamableHttpError::Client(e)) => e.status() == Some(StatusCode::UNAUTHORIZED),
            Err(StreamableHttpError::HttpStatus(e)) => e.status == 401,
            // rmcp 3.11 preserves an unchallenged POST status as this prefix.
            Err(StreamableHttpError::UnexpectedServerResponse(raw)) => raw.starts_with("HTTP 401 "),
            _ => false,
        };
        if rejected && let (Some(auth), Some(token)) = (&self.authorization, token) {
            auth.account.reject(token).await;
        }
        response
    }
}

// rmcp's AuthClient retries a rejected POST. Here a 401 only invalidates the bearer for the
// next request. Each method forwards a message exactly once and keeps the SDK's response limits.
impl StreamableHttpClient for HttpClient {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        token: Option<String>,
        headers: Headers,
    ) -> HttpResult<StreamableHttpPostResponse> {
        self.post_message_with_response_limits(
            uri,
            message,
            session,
            token,
            headers,
            Default::default(),
        )
        .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        token: Option<String>,
        headers: Headers,
        max: usize,
    ) -> HttpResult<StreamableHttpPostResponse> {
        let mut limits = StreamableHttpResponseLimits::default();
        limits.max_sse_event_size = max;
        self.post_message_with_response_limits(uri, message, session, token, headers, limits)
            .await
    }
    async fn post_message_with_response_limits(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        token: Option<String>,
        headers: Headers,
        limits: StreamableHttpResponseLimits,
    ) -> HttpResult<StreamableHttpPostResponse> {
        let token = self.token(&uri, token).await?;
        let response = self
            .http
            .post_message_with_response_limits(
                uri,
                message,
                session,
                token.clone(),
                headers,
                limits,
            )
            .await;
        self.record(token, response).await
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        token: Option<String>,
        headers: Headers,
    ) -> HttpResult<Events> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session,
            last_id,
            token,
            headers,
            StreamableHttpResponseLimits::default().max_sse_event_size,
        )
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        token: Option<String>,
        headers: Headers,
        max: usize,
    ) -> HttpResult<Events> {
        let token = self.token(&uri, token).await?;
        let response = self
            .http
            .get_stream_with_max_sse_event_size(uri, session, last_id, token.clone(), headers, max)
            .await;
        self.record(token, response).await
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        token: Option<String>,
        headers: Headers,
    ) -> HttpResult<()> {
        let token = self.token(&uri, token).await?;
        let response = self
            .http
            .delete_session(uri, session, token.clone(), headers)
            .await;
        self.record(token, response).await
    }
}
