use super::{WIRE_LIMIT, oauth, stdio::StdioTransport};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::Mutex,
    time::Instant,
};

pub(super) const KNOWN_VERSIONS: &[&str] = &[
    "2026-07-28",
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
];

const MODERN_PROTOCOL_VERSION: &str = "2026-07-28";

#[derive(Clone, Debug)]
pub(super) enum McpError {
    Protocol {
        code: i64,
        message: String,
        data: Option<Value>,
    },
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

type ToolParamHeaders = HashMap<String, Vec<(Vec<String>, String)>>;

struct HttpInner {
    http: oauth::HttpClient,
    url: String,
    headers: HashMap<HeaderName, HeaderValue>,
    session_id: std::sync::RwLock<Option<String>>,
    protocol_version: std::sync::RwLock<Option<String>>,
    tool_param_headers: std::sync::RwLock<ToolParamHeaders>,
    closed: AtomicBool,
    delete_sent: AtomicBool,
    cache_epoch: Arc<AtomicU64>,
}

struct HttpTransport {
    inner: Arc<HttpInner>,
    runtime: Handle,
}

impl Drop for HttpTransport {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        if self.inner.session_id.read().unwrap().is_some()
            && !self.inner.delete_sent.load(Ordering::Acquire)
        {
            self.runtime
                .spawn(delete_session(self.inner.clone(), Duration::from_secs(5)));
        }
    }
}

struct CacheEntry {
    key: (&'static str, Option<String>),
    expires_at: Instant,
    val: Value,
}

#[derive(Default)]
struct ResponseCache {
    epoch: u64,
    entries: Vec<CacheEntry>,
}

impl ResponseCache {
    fn sync_epoch(&mut self, epoch: u64) {
        if epoch > self.epoch {
            self.epoch = epoch;
            self.entries.clear();
        }
    }

    fn get(&mut self, epoch: u64, key: &(&'static str, Option<String>)) -> Option<Value> {
        self.sync_epoch(epoch);
        if epoch < self.epoch {
            return None;
        }
        let now = Instant::now();
        self.entries.retain(|entry| entry.expires_at > now);
        self.entries
            .iter()
            .find(|entry| &entry.key == key)
            .map(|entry| entry.val.clone())
    }

    fn insert(
        &mut self,
        req_epoch: u64,
        cur_epoch: u64,
        key: (&'static str, Option<String>),
        val: Value,
        ttl_ms: Option<u64>,
    ) {
        self.sync_epoch(cur_epoch);
        if req_epoch != cur_epoch || cur_epoch != self.epoch {
            return;
        }
        let ttl = ttl_ms
            .map(|ms| Duration::from_millis(ms).min(Duration::from_secs(30)))
            .unwrap_or(Duration::from_secs(5));
        if ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries.retain(|entry| entry.expires_at > now);
        let expires_at = now + ttl;
        if let Some(existing) = self.entries.iter_mut().find(|entry| entry.key == key) {
            existing.expires_at = expires_at;
            existing.val = val;
            return;
        }
        if self.entries.len() >= 4 {
            self.entries.remove(0);
        }
        self.entries.push(CacheEntry {
            key,
            expires_at,
            val,
        });
    }
}

pub(super) struct Client {
    transport: Transport,
    info: ServerInfo,
    next_id: AtomicU64,
    cache: Option<Mutex<ResponseCache>>,
    cache_epoch: Arc<AtomicU64>,
    send_meta: bool,
}

pub(super) fn parse_rpc_message(msg: &Value) -> Result<Value, McpError> {
    if let Some(err) = msg.get("error") {
        let Some(obj) = err.as_object() else {
            return Err(McpError::InvalidResponse);
        };
        let code = obj.get("code").and_then(Value::as_i64).unwrap_or(-32603);
        let message = obj
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let data = obj.get("data").cloned();
        return Err(McpError::Protocol {
            code,
            message,
            data,
        });
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

fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn encode_header_value(value: &str) -> String {
    let bytes = value.as_bytes();
    let needs_b64 = !value.is_empty()
        && (matches!(bytes.first(), Some(b' ' | b'\t'))
            || matches!(bytes.last(), Some(b' ' | b'\t'))
            || value
                .chars()
                .any(|c| (c as u32) < 0x20 || (c as u32) > 0x7E)
            || (value.starts_with("=?base64?") && value.ends_with("?=")));
    if needs_b64 {
        format!("=?base64?{}?=", BASE64_STANDARD.encode(value))
    } else {
        value.to_owned()
    }
}

fn extract_mcp_name<'a>(method: &str, params: Option<&'a Value>) -> Option<&'a str> {
    let params = params?;
    match method {
        "tools/call" | "prompts/get" => params.get("name")?.as_str(),
        "resources/read" | "resources/subscribe" | "resources/unsubscribe" => {
            params.get("uri")?.as_str()
        }
        _ => None,
    }
}

fn collect_prop_headers(
    properties: Option<&Value>,
    path: &mut Vec<String>,
    seen: &mut HashSet<String>,
    out: &mut Vec<(Vec<String>, String)>,
) {
    let Some(map) = properties.and_then(Value::as_object) else {
        return;
    };
    for (name, schema) in map {
        path.push(name.clone());
        if let Some(header) = schema.get("x-mcp-header").and_then(Value::as_str)
            && !header.is_empty()
            && header.bytes().all(is_tchar)
            && matches!(
                schema.get("type").and_then(Value::as_str),
                Some("string" | "integer" | "boolean")
            )
            && seen.insert(header.to_ascii_lowercase())
        {
            out.push((path.clone(), header.to_owned()));
        }
        collect_prop_headers(schema.get("properties"), path, seen, out);
        path.pop();
    }
}

fn collect_param_headers(schema: &Map<String, Value>) -> Vec<(Vec<String>, String)> {
    if schema.contains_key("x-mcp-header") {
        return Vec::new();
    }
    let mut out = Vec::new();
    collect_prop_headers(
        schema.get("properties"),
        &mut Vec::new(),
        &mut HashSet::new(),
        &mut out,
    );
    out
}

fn primitive_str(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
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

enum IncomingRpc {
    Response(Result<Value, McpError>),
    ServerRequest { id: Value, method: String },
    Ignore,
}

fn match_rpc_message(
    msg: &Value,
    expected_id: u64,
    cache_epoch: &AtomicU64,
) -> IncomingRpc {
    if let Some(method) = msg.get("method").and_then(Value::as_str) {
        if let Some(id) = msg.get("id")
            && !id.is_null()
        {
            return IncomingRpc::ServerRequest {
                id: id.clone(),
                method: method.to_owned(),
            };
        }
        if matches!(
            method,
            "notifications/tools/list_changed"
                | "notifications/prompts/list_changed"
                | "notifications/resources/list_changed"
        ) {
            cache_epoch.fetch_add(1, Ordering::Release);
        }
        return IncomingRpc::Ignore;
    }
    match msg.get("id").and_then(Value::as_u64) {
        Some(id) if id == expected_id => IncomingRpc::Response(parse_rpc_message(msg)),
        None if msg.get("error").is_some() => IncomingRpc::Response(parse_rpc_message(msg)),
        _ => IncomingRpc::Ignore,
    }
}

async fn delete_session(inner: Arc<HttpInner>, timeout: Duration) {
    inner.closed.store(true, Ordering::Release);
    if inner.delete_sent.swap(true, Ordering::AcqRel) {
        return;
    }
    let Some(sid) = inner.session_id.read().unwrap().clone() else {
        return;
    };
    let ver = inner.protocol_version.read().unwrap().clone();
    let _ = tokio::time::timeout(timeout, async {
        let Ok(token) = inner.http.acquire_token(&inner.url).await else {
            return;
        };
        let mut headers = HeaderMap::new();
        for (name, value) in &inner.headers {
            headers.insert(name, value.clone());
        }
        if let Ok(val) = HeaderValue::from_str(&sid) {
            headers.insert("mcp-session-id", val);
        }
        if let Some(ver) = ver.as_deref()
            && let Ok(val) = HeaderValue::from_str(ver)
        {
            headers.insert("mcp-protocol-version", val);
        }
        if let Some(t) = token.as_deref()
            && super::oauth::HttpClient::attach_bearer(&mut headers, t).is_err()
        {
            return;
        }
        let _ = inner
            .http
            .http
            .delete(&inner.url)
            .headers(headers)
            .send()
            .await;
    })
    .await;
}

impl HttpTransport {
    fn build_headers(
        &self,
        method: Option<&str>,
        params: Option<&Value>,
        override_version: Option<&str>,
    ) -> (HeaderMap, bool) {
        let mut headers = HeaderMap::new();
        for (name, value) in &self.inner.headers {
            headers.insert(name, value.clone());
        }
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("text/event-stream, application/json"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let session = self.inner.session_id.read().unwrap().clone();
        let session_was_attached = session.is_some();
        if let Some(sid) = session
            && let Ok(val) = HeaderValue::from_str(&sid)
        {
            headers.insert("mcp-session-id", val);
        }

        let version = override_version
            .map(str::to_owned)
            .or_else(|| self.inner.protocol_version.read().unwrap().clone());
        if let Some(ver) = version {
            if let Ok(val) = HeaderValue::from_str(&ver) {
                headers.insert("mcp-protocol-version", val);
            }
            if ver.as_str() >= MODERN_PROTOCOL_VERSION
                && let Some(method) = method
                && method != "initialize"
            {
                if let Ok(val) = HeaderValue::from_str(method) {
                    headers.insert("mcp-method", val);
                }
                if let Some(name) = extract_mcp_name(method, params)
                    && let Ok(val) = HeaderValue::from_str(&encode_header_value(name))
                {
                    headers.insert("mcp-name", val);
                }
                if method == "tools/call"
                    && let Some(tool_name) = params
                        .and_then(|p| p.get("name"))
                        .and_then(Value::as_str)
                    && let Some(arguments) = params.and_then(|p| p.get("arguments"))
                    && let Some(entries) = self
                        .inner
                        .tool_param_headers
                        .read()
                        .unwrap()
                        .get(tool_name)
                        .cloned()
                {
                    for (path, header) in entries {
                        let Some(arg) = path.iter().try_fold(arguments, |v, k| v.get(k)) else {
                            continue;
                        };
                        let Some(raw) = primitive_str(arg) else {
                            continue;
                        };
                        let full = format!("mcp-param-{header}");
                        if let (Ok(name), Ok(val)) = (
                            HeaderName::from_bytes(full.as_bytes()),
                            HeaderValue::from_str(&encode_header_value(&raw)),
                        ) {
                            headers.insert(name, val);
                        }
                    }
                }
            }
        }
        (headers, session_was_attached)
    }

    fn remember_tool_headers(&self, page: &Value) {
        let Some(tools) = page.get("tools").and_then(Value::as_array) else {
            return;
        };
        let mut cache = self.inner.tool_param_headers.write().unwrap();
        for tool in tools {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                continue;
            };
            let Some(schema) = tool.get("inputSchema").and_then(Value::as_object) else {
                continue;
            };
            let collected = collect_param_headers(schema);
            if collected.is_empty() {
                cache.remove(name);
            } else if cache.len() < 512 || cache.contains_key(name) {
                cache.insert(name.to_owned(), collected);
            }
        }
    }

    async fn post_reply(&self, payload: &[u8], override_version: Option<&str>) {
        if self.inner.closed.load(Ordering::Acquire) {
            return;
        }
        let Ok(token) = self.inner.http.acquire_token(&self.inner.url).await else {
            return;
        };
        let (mut headers, _) = self.build_headers(None, None, override_version);
        if let Some(t) = token.as_deref()
            && super::oauth::HttpClient::attach_bearer(&mut headers, t).is_err()
        {
            return;
        }
        let Ok(Ok(resp)) = tokio::time::timeout(
            Duration::from_secs(5),
            self.inner
                .http
                .http
                .post(&self.inner.url)
                .headers(headers)
                .body(payload.to_vec())
                .send(),
        )
        .await
        else {
            return;
        };
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.inner.http.on_unauthorized(token).await;
        }
    }

    async fn handle_sse_message(
        &self,
        msg: &Value,
        expected_id: u64,
        override_version: Option<&str>,
    ) -> Option<Result<Value, McpError>> {
        match match_rpc_message(msg, expected_id, &self.inner.cache_epoch) {
            IncomingRpc::Response(res) => Some(res),
            IncomingRpc::ServerRequest { id, method } => {
                let reply = if method == "ping" {
                    json!({"jsonrpc":"2.0","id":id,"result":{}})
                } else {
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
                };
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    self.post_reply(&bytes, override_version).await;
                }
                None
            }
            IncomingRpc::Ignore => None,
        }
    }

    async fn post(
        &self,
        payload: &[u8],
        expected_id: Option<u64>,
        method: Option<&str>,
        params: Option<&Value>,
        override_version: Option<&str>,
    ) -> Result<Option<Value>, McpError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(McpError::TransportClosed);
        }
        let token = self.inner.http.acquire_token(&self.inner.url).await?;
        let (mut headers, session_was_attached) =
            self.build_headers(method, params, override_version);
        if let Some(t) = token.as_deref() {
            super::oauth::HttpClient::attach_bearer(&mut headers, t)?;
        }

        let response = self
            .inner
            .http
            .http
            .post(&self.inner.url)
            .headers(headers)
            .body(payload.to_vec())
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            self.inner.http.on_unauthorized(token).await;
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
                return match parse_rpc_message(&msg) {
                    Err(err) => Err(err),
                    Ok(_) => Err(McpError::InvalidResponse),
                };
            }
            return Err(McpError::HttpStatus(status.as_u16()));
        }

        if let Some(sid) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
        {
            *self.inner.session_id.write().unwrap() = Some(sid.to_owned());
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

        let Some(expected_id) = expected_id else {
            return Ok(None);
        };

        if is_json {
            let body = read_bounded(response).await?;
            let msg =
                serde_json::from_slice::<Value>(&body).map_err(|_| McpError::InvalidResponse)?;
            return match match_rpc_message(&msg, expected_id, &self.inner.cache_epoch) {
                IncomingRpc::Response(res) => res.map(Some),
                IncomingRpc::ServerRequest { .. } | IncomingRpc::Ignore => {
                    Err(McpError::InvalidResponse)
                }
            };
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
                        && let Some(res) = self
                            .handle_sse_message(&msg, expected_id, override_version)
                            .await
                    {
                        return res.map(Some);
                    }
                }
            }
            if let Some(event) = decoder.finish()?
                && !event.is_empty()
                && let Ok(msg) = serde_json::from_slice::<Value>(&event)
                && let Some(res) = self
                    .handle_sse_message(&msg, expected_id, override_version)
                    .await
            {
                return res.map(Some);
            }
            return Err(McpError::TransportClosed);
        }

        Err(McpError::UnexpectedContentType)
    }
}

fn client_meta(protocol_version: &str) -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": protocol_version,
        "io.modelcontextprotocol/clientInfo": {
            "name": "mygo-bot",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}

enum DiscoverOutcome {
    Modern(ServerInfo),
    Legacy,
}

impl Client {
    pub(super) async fn connect_http(
        http: oauth::HttpClient,
        url: String,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<Self, McpError> {
        let cache_epoch = Arc::new(AtomicU64::new(0));
        let transport = Transport::Http(HttpTransport {
            inner: Arc::new(HttpInner {
                http,
                url,
                headers,
                session_id: std::sync::RwLock::new(None),
                protocol_version: std::sync::RwLock::new(None),
                tool_param_headers: std::sync::RwLock::new(HashMap::new()),
                closed: AtomicBool::new(false),
                delete_sent: AtomicBool::new(false),
                cache_epoch: cache_epoch.clone(),
            }),
            runtime: Handle::current(),
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

    async fn discover_startup(&self) -> Result<DiscoverOutcome, McpError> {
        let mut attempted: Vec<&str> = Vec::new();
        let mut candidate = KNOWN_VERSIONS[0];
        loop {
            attempted.push(candidate);
            let params = json!({ "_meta": client_meta(candidate) });
            match self
                .raw_request_with_version("server/discover", params, Some(candidate))
                .await
            {
                Ok(disc) => {
                    if let Some(kind) = disc.get("resultType").and_then(Value::as_str)
                        && kind != "complete"
                    {
                        return Err(McpError::InvalidResponse);
                    }
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
                    return Ok(DiscoverOutcome::Modern(ServerInfo {
                        protocol_version: ver.to_owned(),
                        capabilities,
                    }));
                }
                Err(McpError::Protocol {
                    code: -32022,
                    data,
                    ..
                }) => {
                    let supported: Vec<String> = data
                        .as_ref()
                        .and_then(|d| d.get("supported"))
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default();
                    let may_retry_current =
                        attempted.iter().filter(|&&v| v == candidate).count() == 1;
                    let next = KNOWN_VERSIONS.iter().copied().find(|known| {
                        supported.iter().any(|s| s == known)
                            && (!attempted.contains(known)
                                || (may_retry_current && *known == candidate))
                    });
                    let Some(next) = next else {
                        return Err(McpError::NoCompatibleProtocolVersion);
                    };
                    candidate = next;
                }
                Err(err @ McpError::Protocol {
                    code: -32021 | -32020,
                    ..
                }) => return Err(err),
                Err(McpError::Protocol { .. }) => return Ok(DiscoverOutcome::Legacy),
                Err(McpError::HttpStatus(s))
                    if (400..500).contains(&s) && !matches!(s, 401 | 403 | 429) =>
                {
                    return Ok(DiscoverOutcome::Legacy);
                }
                Err(err) => return Err(err),
            }
        }
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
            send_meta: false,
        };
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), client.discover_startup()).await;
        let (info, has_initialize, send_meta) = match outcome {
            Ok(Ok(DiscoverOutcome::Modern(info))) => {
                client.set_protocol_version(&info.protocol_version);
                (info, false, true)
            }
            Ok(Ok(DiscoverOutcome::Legacy)) | Err(_) => {
                client.reset_http_session();
                let init_params = json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "mygo-bot",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                });
                let result = client.raw_request("initialize", init_params).await?;
                let ver = result
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("2025-11-25");
                if !KNOWN_VERSIONS.contains(&ver) {
                    return Err(McpError::NoCompatibleProtocolVersion);
                }
                let capabilities = ServerCapabilities::from_json(result.get("capabilities"));
                client.set_protocol_version(ver);
                let _ = client.raw_notify("notifications/initialized", None).await;
                let has_initialize = ver < MODERN_PROTOCOL_VERSION;
                (
                    ServerInfo {
                        protocol_version: ver.to_owned(),
                        capabilities,
                    },
                    has_initialize,
                    !has_initialize,
                )
            }
            Ok(Err(err)) => return Err(err),
        };
        // Only stdio receives pushed list_changed notifications; HTTP has no GET SSE stream.
        let receives_notifications =
            matches!(client.transport, Transport::Stdio(_)) && has_initialize;
        let has_list_changed = info
            .capabilities
            .tools
            .as_ref()
            .is_some_and(|t| t.list_changed)
            || info
                .capabilities
                .prompts
                .as_ref()
                .is_some_and(|p| p.list_changed);
        let volatile =
            info.capabilities.resources || (!receives_notifications && has_list_changed);
        client.info = info;
        client.send_meta = send_meta;
        if !volatile {
            client.cache = Some(Mutex::new(ResponseCache::default()));
        }
        Ok(client)
    }

    fn reset_http_session(&self) {
        if let Transport::Http(http) = &self.transport {
            *http.inner.session_id.write().unwrap() = None;
            *http.inner.protocol_version.write().unwrap() = None;
        }
    }

    fn set_protocol_version(&self, ver: &str) {
        if let Transport::Http(http) = &self.transport {
            *http.inner.protocol_version.write().unwrap() = Some(ver.to_owned());
        }
    }

    pub(super) fn peer_info(&self) -> &ServerInfo {
        &self.info
    }

    pub(super) fn is_closed(&self) -> bool {
        match &self.transport {
            Transport::Http(http) => http.inner.closed.load(Ordering::Acquire),
            Transport::Stdio(stdio) => stdio.is_closed(),
        }
    }

    pub(super) fn cancel(&self) {
        match &self.transport {
            Transport::Http(http) => http.inner.closed.store(true, Ordering::Release),
            Transport::Stdio(stdio) => stdio.cancel(),
        }
    }

    pub(super) async fn close(self, timeout: Duration) {
        match &self.transport {
            Transport::Http(http) => delete_session(http.inner.clone(), timeout).await,
            Transport::Stdio(stdio) => stdio.close(timeout).await,
        }
    }

    async fn raw_notify(&self, method: &str, params: Option<Value>) -> Result<(), McpError> {
        let mut msg = json!({"jsonrpc": "2.0", "method": method});
        if let Some(ref p) = params {
            msg["params"] = p.clone();
        }
        let payload = serde_json::to_vec(&msg).map_err(|_| McpError::InvalidResponse)?;
        match &self.transport {
            Transport::Http(http) => {
                http.post(&payload, None, Some(method), params.as_ref(), None)
                    .await?;
                Ok(())
            }
            Transport::Stdio(stdio) => stdio.notify(&payload).await,
        }
    }

    async fn raw_request(&self, method: &str, mut params: Value) -> Result<Value, McpError> {
        if self.send_meta
            && let Some(obj) = params.as_object_mut()
            && !obj.contains_key("_meta")
        {
            obj.insert(
                "_meta".to_owned(),
                client_meta(&self.info.protocol_version),
            );
        }
        self.raw_request_with_version(method, params, None).await
    }

    async fn raw_request_with_version(
        &self,
        method: &str,
        params: Value,
        override_version: Option<&str>,
    ) -> Result<Value, McpError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": &params});
        let payload = serde_json::to_vec(&msg).map_err(|_| McpError::InvalidResponse)?;
        match &self.transport {
            Transport::Http(http) => http
                .post(&payload, Some(id), Some(method), Some(&params), override_version)
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
        let req_epoch = self.cache_epoch.load(Ordering::Acquire);
        if let Some(cache) = &self.cache
            && let Some(hit) = cache.lock().await.get(req_epoch, &key)
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
            let cur_epoch = self.cache_epoch.load(Ordering::Acquire);
            cache
                .lock()
                .await
                .insert(req_epoch, cur_epoch, key, result.clone(), ttl_ms);
        }
        Ok(result)
    }

    pub(super) async fn list_tools(&self, cursor: Option<String>) -> Result<Value, McpError> {
        let page = self.list_cached("tools/list", cursor).await?;
        if let Transport::Http(http) = &self.transport
            && self.info.protocol_version.as_str() >= MODERN_PROTOCOL_VERSION
        {
            http.remember_tool_headers(&page);
        }
        Ok(page)
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
