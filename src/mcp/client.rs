use super::{WIRE_LIMIT, oauth, stdio::StdioTransport};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};

pub(super) const KNOWN_VERSIONS: &[&str] = &[
    "2026-07-28",
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
];

#[derive(Clone, Debug)]
pub(super) enum McpError {
    Protocol { code: i64, message: String },
    AuthRequired,
    SessionExpired,
    HttpStatus(u16),
    ResponseTooLarge(usize),
    UnexpectedContentType,
    ConnectFailed,
    TimedOut,
    NoCompatibleProtocolVersion,
    TransportClosed,
    InvalidResponse,
}

impl McpError {
    pub(super) fn is_auth_required(&self) -> bool {
        matches!(self, Self::AuthRequired)
    }

    pub(super) fn should_retire(&self) -> bool {
        matches!(
            self,
            Self::TransportClosed | Self::SessionExpired | Self::ConnectFailed
        )
    }
}

pub(super) struct ListCap {
    pub list_changed: bool,
}

pub(super) struct ServerCapabilities {
    pub tools: Option<ListCap>,
    pub resources: bool,
    pub prompts: Option<ListCap>,
}

impl ServerCapabilities {
    fn from_json(caps: Option<&Value>) -> Self {
        let list_cap = |key: &str| {
            caps.and_then(|c| c.get(key))
                .and_then(Value::as_object)
                .map(|obj| ListCap {
                    list_changed: obj
                        .get("listChanged")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
        };
        Self {
            tools: list_cap("tools"),
            resources: caps
                .and_then(|c| c.get("resources"))
                .is_some_and(Value::is_object),
            prompts: list_cap("prompts"),
        }
    }
}

pub(super) struct ServerInfo {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
}

enum Transport {
    Http(HttpTransport),
    Stdio(StdioTransport),
}

struct HttpTransport {
    http: oauth::HttpClient,
    url: String,
    headers: HashMap<HeaderName, HeaderValue>,
    session_id: RwLock<Option<String>>,
    protocol_version: RwLock<Option<String>>,
    closed: AtomicBool,
    cache_epoch: Arc<AtomicU64>,
}

#[derive(Default)]
struct ResponseCache {
    epoch: u64,
    order: VecDeque<(&'static str, Option<String>)>,
    entries: HashMap<(&'static str, Option<String>), (Instant, Value)>,
}

impl ResponseCache {
    fn get(&mut self, epoch: u64, key: &(&'static str, Option<String>)) -> Option<Value> {
        if self.epoch != epoch {
            self.epoch = epoch;
            self.order.clear();
            self.entries.clear();
            return None;
        }
        let (expires_at, val) = self.entries.get(key)?;
        if Instant::now() >= *expires_at {
            self.entries.remove(key);
            return None;
        }
        Some(val.clone())
    }

    fn insert(&mut self, epoch: u64, key: (&'static str, Option<String>), val: Value, ttl_ms: Option<u64>) {
        if self.epoch != epoch {
            self.epoch = epoch;
            self.order.clear();
            self.entries.clear();
        }
        let ttl = ttl_ms
            .map(|ms| Duration::from_millis(ms).min(Duration::from_secs(30)))
            .unwrap_or(Duration::from_secs(5));
        if ttl.is_zero() {
            return;
        }
        let expires_at = Instant::now() + ttl;
        if !self.entries.contains_key(&key) {
            while self.entries.len() >= 4 {
                if let Some(evict) = self.order.pop_front() {
                    if self.entries.remove(&evict).is_some() {
                        break;
                    }
                } else {
                    break;
                }
            }
            self.order.push_back(key.clone());
        }
        self.entries.insert(key, (expires_at, val));
    }
}

pub(super) struct Client {
    transport: Transport,
    info: ServerInfo,
    next_id: AtomicU64,
    cache: Option<Mutex<ResponseCache>>,
    cache_epoch: Arc<AtomicU64>,
}

pub(super) fn parse_rpc_message(msg: &Value) -> Result<Value, McpError> {
    if let Some(err) = msg.get("error").and_then(Value::as_object) {
        let code = err.get("code").and_then(Value::as_i64).unwrap_or(-32603);
        let message = err
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        return Err(McpError::Protocol { code, message });
    }
    msg.get("result")
        .cloned()
        .ok_or(McpError::InvalidResponse)
}

fn classify_reqwest_error(err: reqwest::Error) -> McpError {
    if let Some(status) = err.status() {
        return McpError::HttpStatus(status.as_u16());
    }
    if err.is_connect() {
        return McpError::ConnectFailed;
    }
    if err.is_timeout() {
        return McpError::TimedOut;
    }
    McpError::TransportClosed
}

fn content_type_is(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(';').next())
        .is_some_and(|essence| essence.trim().eq_ignore_ascii_case(expected))
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, McpError> {
    if response
        .content_length()
        .is_some_and(|len| len > WIRE_LIMIT as u64)
    {
        return Err(McpError::ResponseTooLarge(WIRE_LIMIT));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(classify_reqwest_error)? {
        if chunk.len() > WIRE_LIMIT - body.len() {
            return Err(McpError::ResponseTooLarge(WIRE_LIMIT));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Default)]
struct SseDecoder {
    line: Vec<u8>,
    event_data: Vec<u8>,
    has_data: bool,
    pending_cr: bool,
}

impl SseDecoder {
    fn handle_line(&mut self, out: &mut Vec<Vec<u8>>) -> Result<(), McpError> {
        if self.line.is_empty() {
            if self.has_data {
                self.has_data = false;
                out.push(std::mem::take(&mut self.event_data));
            }
            return Ok(());
        }
        if let Some(val) = self.line.strip_prefix(b"data:") {
            let val = val.strip_prefix(b" ").unwrap_or(val);
            let extra = val.len() + usize::from(self.has_data);
            if extra > WIRE_LIMIT - self.event_data.len() {
                return Err(McpError::ResponseTooLarge(WIRE_LIMIT));
            }
            if self.has_data {
                self.event_data.push(b'\n');
            }
            self.event_data.extend_from_slice(val);
            self.has_data = true;
        }
        self.line.clear();
        Ok(())
    }

    fn push(&mut self, mut chunk: &[u8]) -> Result<Vec<Vec<u8>>, McpError> {
        let mut out = Vec::new();
        if self.pending_cr {
            self.pending_cr = false;
            if let Some(rest) = chunk.strip_prefix(b"\n") {
                chunk = rest;
            }
        }
        while !chunk.is_empty() {
            if let Some(pos) = chunk.iter().position(|&b| b == b'\n' || b == b'\r') {
                let slice = &chunk[..pos];
                if slice.len() > WIRE_LIMIT - self.line.len() {
                    return Err(McpError::ResponseTooLarge(WIRE_LIMIT));
                }
                self.line.extend_from_slice(slice);
                let delim = chunk[pos];
                chunk = &chunk[pos + 1..];
                if delim == b'\r' {
                    if let Some(rest) = chunk.strip_prefix(b"\n") {
                        chunk = rest;
                    } else if chunk.is_empty() {
                        self.pending_cr = true;
                    }
                }
                self.handle_line(&mut out)?;
            } else {
                if chunk.len() > WIRE_LIMIT - self.line.len() {
                    return Err(McpError::ResponseTooLarge(WIRE_LIMIT));
                }
                self.line.extend_from_slice(chunk);
                break;
            }
        }
        Ok(out)
    }

    fn finish(mut self) -> Result<Option<Vec<u8>>, McpError> {
        let mut out = Vec::new();
        if !self.line.is_empty() {
            self.handle_line(&mut out)?;
        }
        if self.has_data {
            out.push(self.event_data);
        }
        Ok(out.into_iter().next())
    }
}

fn match_rpc_message(
    msg: &Value,
    expected_id: u64,
    cache_epoch: &AtomicU64,
) -> Option<Result<Value, McpError>> {
    if let Some(method) = msg.get("method").and_then(Value::as_str) {
        if matches!(
            method,
            "notifications/tools/list_changed"
                | "notifications/prompts/list_changed"
                | "notifications/resources/list_changed"
        ) {
            cache_epoch.fetch_add(1, Ordering::Release);
        }
        return None;
    }
    match msg.get("id").and_then(Value::as_u64) {
        Some(id) if id == expected_id => Some(parse_rpc_message(msg)),
        None if msg.get("error").is_some() => Some(parse_rpc_message(msg)),
        _ => None,
    }
}

impl HttpTransport {
    async fn post(&self, payload: &[u8], expected_id: Option<u64>) -> Result<Option<Value>, McpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(McpError::TransportClosed);
        }
        let token = self.http.acquire_token(&self.url).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("text/event-stream, application/json"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        for (name, value) in &self.headers {
            headers.insert(name, value.clone());
        }
        let session = self.session_id.read().await.clone();
        let session_was_attached = session.is_some();
        if let Some(sid) = session
            && let Ok(val) = HeaderValue::from_str(&sid)
        {
            headers.insert("mcp-session-id", val);
        }
        if let Some(ver) = self.protocol_version.read().await.as_deref()
            && let Ok(val) = HeaderValue::from_str(ver)
        {
            headers.insert("mcp-protocol-version", val);
        }
        if let Some(t) = token.as_deref() {
            super::oauth::HttpClient::attach_bearer(&mut headers, t)?;
        }

        let response = self
            .http
            .http
            .post(&self.url)
            .headers(headers)
            .body(payload.to_vec())
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            self.http.on_unauthorized(token).await;
            return Err(McpError::AuthRequired);
        }
        if status == reqwest::StatusCode::FORBIDDEN
            && super::oauth::header_has_bearer_challenge(response.headers())
        {
            return Err(McpError::AuthRequired);
        }
        if status == reqwest::StatusCode::NOT_FOUND && session_was_attached {
            return Err(McpError::SessionExpired);
        }
        if matches!(
            status,
            reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::NO_CONTENT
        ) {
            return if expected_id.is_none() {
                Ok(None)
            } else {
                Err(McpError::InvalidResponse)
            };
        }

        if let Some(sid) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
        {
            *self.session_id.write().await = Some(sid.to_owned());
        }

        let is_json = content_type_is(response.headers(), "application/json");
        let is_sse = content_type_is(response.headers(), "text/event-stream");

        if !status.is_success() {
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::FORBIDDEN
                || !is_json
            {
                return Err(McpError::HttpStatus(status.as_u16()));
            }
            let body = read_bounded(response).await?;
            if let Ok(msg) = serde_json::from_slice::<Value>(&body)
                && msg.get("error").is_some()
            {
                return Err(parse_rpc_message(&msg).unwrap_err());
            }
            return Err(McpError::HttpStatus(status.as_u16()));
        }

        let Some(expected_id) = expected_id else {
            return Ok(None);
        };

        if is_json {
            let body = read_bounded(response).await?;
            let msg =
                serde_json::from_slice::<Value>(&body).map_err(|_| McpError::InvalidResponse)?;
            return match_rpc_message(&msg, expected_id, &self.cache_epoch)
                .unwrap_or(Err(McpError::InvalidResponse))
                .map(Some);
        }

        if is_sse {
            let mut decoder = SseDecoder::default();
            let mut resp = response;
            while let Some(chunk) = resp.chunk().await.map_err(classify_reqwest_error)? {
                for event in decoder.push(&chunk)? {
                    if event.is_empty() {
                        continue;
                    }
                    if let Ok(msg) = serde_json::from_slice::<Value>(&event)
                        && let Some(res) = match_rpc_message(&msg, expected_id, &self.cache_epoch)
                    {
                        return res.map(Some);
                    }
                }
            }
            if let Some(event) = decoder.finish()?
                && !event.is_empty()
                && let Ok(msg) = serde_json::from_slice::<Value>(&event)
                && let Some(res) = match_rpc_message(&msg, expected_id, &self.cache_epoch)
            {
                return res.map(Some);
            }
            return Err(McpError::TransportClosed);
        }

        Err(McpError::UnexpectedContentType)
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let Some(sid) = self.session_id.read().await.clone() else {
            return;
        };
        let Ok(token) = self.http.acquire_token(&self.url).await else {
            return;
        };
        let mut headers = HeaderMap::new();
        for (name, value) in &self.headers {
            headers.insert(name, value.clone());
        }
        if let Ok(val) = HeaderValue::from_str(&sid) {
            headers.insert("mcp-session-id", val);
        }
        if let Some(ver) = self.protocol_version.read().await.as_deref()
            && let Ok(val) = HeaderValue::from_str(ver)
        {
            headers.insert("mcp-protocol-version", val);
        }
        if let Some(t) = token.as_deref()
            && super::oauth::HttpClient::attach_bearer(&mut headers, t).is_err()
        {
            return;
        }
        let _ = self.http.http.delete(&self.url).headers(headers).send().await;
    }
}

impl Client {
    pub(super) async fn connect_http(
        http: oauth::HttpClient,
        url: String,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<Self, McpError> {
        let cache_epoch = Arc::new(AtomicU64::new(0));
        let transport = Transport::Http(HttpTransport {
            http,
            url,
            headers,
            session_id: RwLock::new(None),
            protocol_version: RwLock::new(None),
            closed: AtomicBool::new(false),
            cache_epoch: cache_epoch.clone(),
        });
        Self::handshake(transport, cache_epoch).await
    }

    pub(super) async fn connect_stdio(
        spawned: super::stdio::SpawnedChild,
    ) -> Result<Self, McpError> {
        let cache_epoch = Arc::new(AtomicU64::new(0));
        let transport = Transport::Stdio(StdioTransport::start(spawned, cache_epoch.clone()));
        Self::handshake(transport, cache_epoch).await
    }

    async fn handshake(
        transport: Transport,
        cache_epoch: Arc<AtomicU64>,
    ) -> Result<Self, McpError> {
        let mut client = Self {
            transport,
            info: ServerInfo {
                protocol_version: String::new(),
                capabilities: ServerCapabilities {
                    tools: None,
                    resources: false,
                    prompts: None,
                },
            },
            next_id: AtomicU64::new(1),
            cache: None,
            cache_epoch,
        };
        let init_params = json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {
                "name": "mygo-bot",
                "version": env!("CARGO_PKG_VERSION"),
            }
        });
        let (info, has_initialize) = match client.raw_request("initialize", init_params).await {
            Ok(result) => {
                let ver = result
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("2025-11-25");
                if !KNOWN_VERSIONS.contains(&ver) {
                    return Err(McpError::NoCompatibleProtocolVersion);
                }
                let capabilities = ServerCapabilities::from_json(result.get("capabilities"));
                client.set_protocol_version(ver).await;
                let _ = client.raw_notify("notifications/initialized", None).await;
                (
                    ServerInfo {
                        protocol_version: ver.to_owned(),
                        capabilities,
                    },
                    ver != "2026-07-28",
                )
            }
            Err(McpError::Protocol { code: -32601, .. }) => {
                let disc = client.raw_request("server/discover", json!({})).await?;
                let versions = disc
                    .get("supportedVersions")
                    .or_else(|| disc.get("supportedProtocolVersions"))
                    .and_then(Value::as_array)
                    .ok_or(McpError::InvalidResponse)?;
                let ver = KNOWN_VERSIONS
                    .iter()
                    .copied()
                    .find(|known| versions.iter().any(|v| v.as_str() == Some(*known)))
                    .ok_or(McpError::NoCompatibleProtocolVersion)?;
                let capabilities = ServerCapabilities::from_json(disc.get("capabilities"));
                client.set_protocol_version(ver).await;
                (
                    ServerInfo {
                        protocol_version: ver.to_owned(),
                        capabilities,
                    },
                    false,
                )
            }
            Err(err) => return Err(err),
        };
        // Do not cache binary reads or dynamic catalogs without a change subscription.
        let volatile = info.capabilities.resources
            || (!has_initialize
                && (info
                    .capabilities
                    .tools
                    .as_ref()
                    .is_some_and(|t| t.list_changed)
                    || info
                        .capabilities
                        .prompts
                        .as_ref()
                        .is_some_and(|p| p.list_changed)));
        client.info = info;
        if !volatile {
            client.cache = Some(Mutex::new(ResponseCache::default()));
        }
        Ok(client)
    }

    async fn set_protocol_version(&self, ver: &str) {
        if let Transport::Http(http) = &self.transport {
            *http.protocol_version.write().await = Some(ver.to_owned());
        }
    }

    pub(super) fn peer_info(&self) -> &ServerInfo {
        &self.info
    }

    pub(super) fn is_closed(&self) -> bool {
        match &self.transport {
            Transport::Http(http) => http.closed.load(Ordering::Acquire),
            Transport::Stdio(stdio) => stdio.is_closed(),
        }
    }

    pub(super) fn cancel(&self) {
        match &self.transport {
            Transport::Http(http) => http.closed.store(true, Ordering::Release),
            Transport::Stdio(stdio) => stdio.cancel(),
        }
    }

    pub(super) async fn close(self, timeout: Duration) {
        match &self.transport {
            Transport::Http(http) => http.close().await,
            Transport::Stdio(stdio) => stdio.close(timeout).await,
        }
    }

    async fn raw_notify(&self, method: &str, params: Option<Value>) -> Result<(), McpError> {
        let mut msg = json!({"jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            msg["params"] = params;
        }
        let payload = serde_json::to_vec(&msg).map_err(|_| McpError::InvalidResponse)?;
        match &self.transport {
            Transport::Http(http) => {
                http.post(&payload, None).await?;
                Ok(())
            }
            Transport::Stdio(stdio) => stdio.notify(&payload).await,
        }
    }

    async fn raw_request(&self, method: &str, params: Value) -> Result<Value, McpError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let payload = serde_json::to_vec(&msg).map_err(|_| McpError::InvalidResponse)?;
        match &self.transport {
            Transport::Http(http) => http
                .post(&payload, Some(id))
                .await?
                .ok_or(McpError::InvalidResponse),
            Transport::Stdio(stdio) => stdio.request(id, &payload).await,
        }
    }

    async fn list_cached(
        &self,
        method: &'static str,
        cursor: Option<String>,
    ) -> Result<Value, McpError> {
        let key = (method, cursor.clone());
        let epoch = self.cache_epoch.load(Ordering::Acquire);
        if let Some(cache) = &self.cache
            && let Some(hit) = cache.lock().await.get(epoch, &key)
        {
            return Ok(hit);
        }
        let params = match cursor {
            Some(c) => json!({"cursor": c}),
            None => json!({}),
        };
        let result = self.raw_request(method, params).await?;
        let ttl_ms = result.get("ttlMs").and_then(Value::as_u64);
        let is_private = result
            .get("cacheScope")
            .and_then(Value::as_str)
            .is_some_and(|s| s == "private");
        if let Some(cache) = &self.cache
            && !is_private
        {
            cache
                .lock()
                .await
                .insert(self.cache_epoch.load(Ordering::Acquire), key, result.clone(), ttl_ms);
        }
        Ok(result)
    }

    pub(super) async fn list_tools(&self, cursor: Option<String>) -> Result<Value, McpError> {
        self.list_cached("tools/list", cursor).await
    }

    pub(super) async fn list_resources(&self, cursor: Option<String>) -> Result<Value, McpError> {
        self.list_cached("resources/list", cursor).await
    }

    pub(super) async fn list_resource_templates(
        &self,
        cursor: Option<String>,
    ) -> Result<Value, McpError> {
        self.list_cached("resources/templates/list", cursor).await
    }

    pub(super) async fn read_resource(&self, uri: &str) -> Result<Value, McpError> {
        self.raw_request("resources/read", json!({"uri": uri})).await
    }

    pub(super) async fn list_prompts(&self, cursor: Option<String>) -> Result<Value, McpError> {
        self.list_cached("prompts/list", cursor).await
    }

    pub(super) async fn get_prompt(
        &self,
        name: &str,
        args: Option<Map<String, Value>>,
    ) -> Result<Value, McpError> {
        let params = match args {
            Some(args) => json!({"name": name, "arguments": args}),
            None => json!({"name": name}),
        };
        self.raw_request("prompts/get", params).await
    }

    pub(super) async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
    ) -> Result<Value, McpError> {
        self.raw_request("tools/call", json!({"name": name, "arguments": args}))
            .await
    }
}

