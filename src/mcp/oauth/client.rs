use super::{Binding, Error, Result, validate_endpoint};
use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use form_urlencoded::{Serializer, byte_serialize};
use reqwest::{
    Client, StatusCode, Url,
    dns::{Addrs, Name, Resolve, Resolving},
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue, LOCATION, WWW_AUTHENTICATE},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_REDIRECTS: usize = 10;
const CLOUD_METADATA_HOSTS: &[&str] = &[
    "metadata",
    "metadata.google.internal",
    "metadata.azure.internal",
];
const PROXY_ENV_VARS: &[&str] = &[
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
];

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(in crate::mcp) struct AuthorizationMetadata {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub registration_endpoint: Option<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub scopes_supported: Option<Vec<String>>,
    #[serde(default)]
    pub response_types_supported: Option<Vec<String>>,
    #[serde(default)]
    pub code_challenge_methods_supported: Option<Vec<String>>,
    #[serde(flatten)]
    pub additional_fields: HashMap<String, Value>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(in crate::mcp) struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(in crate::mcp) struct StoredCredentials {
    pub client_id: String,
    #[serde(default)]
    pub token_response: Option<TokenResponse>,
    #[serde(default)]
    pub granted_scopes: Vec<String>,
    #[serde(default)]
    pub token_received_at: Option<u64>,
    #[serde(default)]
    pub issuer: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(in crate::mcp) struct PendingState {
    pub pkce_verifier: String,
    pub csrf_token: String,
    #[serde(default)]
    pub expected_issuer: Option<String>,
    #[serde(default)]
    pub require_issuer: bool,
    pub created_at: u64,
    #[serde(default)]
    pub requested_scopes: Vec<String>,
}

pub(super) enum RefreshError {
    Rejected,
    Transient,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AddressClass {
    Public,
    Loopback,
    Private,
    Blocked,
}

impl AddressClass {
    fn embedded_ipv4(addr: Ipv6Addr) -> Option<Ipv4Addr> {
        let [a, b, c, d, e, f, g, h] = addr.segments();
        let low = Ipv4Addr::from((u32::from(g) << 16) | u32::from(h));
        match [a, b, c, d, e, f] {
            [0, 0, 0, 0, 0, 0xffff] | [0x64, 0xff9b, 0, 0, 0, 0] => Some(low),
            [0, 0, 0, 0, 0, 0] if !addr.is_loopback() && !addr.is_unspecified() => Some(low),
            [0x2002, ..] => Some(Ipv4Addr::from((u32::from(b) << 16) | u32::from(c))),
            _ => None,
        }
    }

    fn of(addr: IpAddr) -> Self {
        let addr = match addr {
            IpAddr::V6(v6) => Self::embedded_ipv4(v6).map_or(IpAddr::V6(v6), IpAddr::V4),
            v4 => v4,
        };
        match addr {
            IpAddr::V4(v4) => {
                let o = v4.octets();
                if v4.is_loopback() {
                    Self::Loopback
                } else if v4.is_private() || (o[0] == 100 && (64..=127).contains(&o[1])) {
                    Self::Private
                } else if v4.is_link_local()
                    || v4.is_broadcast()
                    || v4.is_unspecified()
                    || v4.is_multicast()
                    || o[0] == 0
                    || (o[0] == 198 && matches!(o[1], 18 | 19))
                {
                    Self::Blocked
                } else {
                    Self::Public
                }
            }
            IpAddr::V6(v6) => {
                let s = v6.segments();
                if v6.is_loopback() {
                    Self::Loopback
                } else if (s[0] & 0xfe00) == 0xfc00 {
                    Self::Private
                } else if v6.is_unspecified()
                    || v6.is_multicast()
                    || (s[0] & 0xffc0) == 0xfe80
                    || s[..2] == [0x2001, 0]
                    || s[..3] == [0x64, 0xff9b, 1]
                {
                    Self::Blocked
                } else {
                    Self::Public
                }
            }
        }
    }
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

fn literal_ip(url: &Url) -> Option<IpAddr> {
    url.host_str()?
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        .ok()
}

fn is_loopback_host(url: &Url) -> bool {
    match literal_ip(url) {
        Some(ip) => ip.is_loopback(),
        None => url.host_str().is_some_and(|h| {
            let h = normalize_host(h);
            h == "localhost" || h.ends_with(".localhost")
        }),
    }
}

fn is_disallowed_hostname(host: &str) -> bool {
    let h = normalize_host(host);
    h == "localhost" || h.ends_with(".localhost") || CLOUD_METADATA_HOSTS.contains(&h.as_str())
}

fn is_same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str()
            .zip(b.host_str())
            .is_some_and(|(x, y)| x.eq_ignore_ascii_case(y))
        && a.port_or_known_default() == b.port_or_known_default()
}

struct MetadataResolver {
    base_ip: Option<IpAddr>,
    base_classes: Vec<AddressClass>,
    unfiltered_hosts: Vec<String>,
}

impl MetadataResolver {
    fn new(base_url: &Url) -> Self {
        let base_ip = literal_ip(base_url);
        let base_classes = match base_ip {
            Some(ip) => vec![AddressClass::of(ip)],
            None if is_loopback_host(base_url) => vec![AddressClass::Loopback],
            None => Vec::new(),
        };
        let unfiltered_hosts = PROXY_ENV_VARS
            .iter()
            .filter_map(|var| std::env::var(var).ok())
            .filter_map(|proxy| {
                let raw = if proxy.contains("://") {
                    proxy
                } else {
                    format!("http://{proxy}")
                };
                Url::parse(&raw).ok()?.host_str().map(normalize_host)
            })
            .collect();
        Self {
            base_ip,
            base_classes,
            unfiltered_hosts,
        }
    }

    fn is_allowed(&self, class: AddressClass) -> bool {
        match class {
            AddressClass::Public => true,
            AddressClass::Loopback | AddressClass::Private => self.base_classes.contains(&class),
            AddressClass::Blocked => false,
        }
    }

    fn allows_url(&self, url: &Url) -> bool {
        match literal_ip(url) {
            Some(ip) => self.base_ip == Some(ip) || self.is_allowed(AddressClass::of(ip)),
            None => true,
        }
    }
}

impl Resolve for MetadataResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        let norm = normalize_host(&host);
        let bypass = self.unfiltered_hosts.contains(&norm);
        let base_classes = self.base_classes.clone();
        Box::pin(async move {
            let resolved: Vec<SocketAddr> =
                tokio::task::spawn_blocking(move || (host.as_str(), 0).to_socket_addrs())
                    .await??
                    .collect();
            if bypass {
                return Ok(Box::new(resolved.into_iter()) as Addrs);
            }
            let allowed: Vec<SocketAddr> = resolved
                .into_iter()
                .filter(|addr| match AddressClass::of(addr.ip()) {
                    AddressClass::Public => true,
                    c @ (AddressClass::Loopback | AddressClass::Private) => {
                        base_classes.contains(&c)
                    }
                    AddressClass::Blocked => false,
                })
                .collect();
            if allowed.is_empty() {
                return Err(format!("no allowed OAuth address for {norm}").into());
            }
            Ok(Box::new(allowed.into_iter()) as Addrs)
        })
    }
}

pub(in crate::mcp) struct OAuthHttp {
    http: Client,
    resolver: Arc<MetadataResolver>,
}

struct ResponseData {
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
}

impl OAuthHttp {
    pub(super) fn new(base_url: &Url) -> std::result::Result<Self, String> {
        let resolver = Arc::new(MetadataResolver::new(base_url));
        let builder = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(resolver.clone());
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        let builder = {
            let mut tls = native_tls::TlsConnector::builder();
            tls.request_alpns(&["http/1.1"]);
            builder.tls_backend_preconfigured(
                tls.build()
                    .map_err(|e| format!("OAuth TLS initialization failed: {e}"))?,
            )
        };
        let http = builder
            .build()
            .map_err(|e| format!("OAuth HTTP client initialization failed: {e}"))?;
        Ok(Self { http, resolver })
    }

    async fn send_checked(&self, request: reqwest::Request) -> std::result::Result<ResponseData, ()> {
        if !self.resolver.allows_url(request.url()) {
            return Err(());
        }
        let mut response = self.http.execute(request).await.map_err(|_| ())?;
        if response
            .content_length()
            .is_some_and(|len| len > MAX_BODY_BYTES as u64)
        {
            return Err(());
        }
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
            if chunk.len() > MAX_BODY_BYTES - body.len() {
                return Err(());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ResponseData {
            status,
            headers,
            body,
        })
    }

    async fn discovery_get(&self, url: &Url) -> std::result::Result<ResponseData, ()> {
        let mut current = url.clone();
        for _ in 0..MAX_REDIRECTS {
            let req = self
                .http
                .get(current.clone())
                .header("MCP-Protocol-Version", "2024-11-05")
                .build()
                .map_err(|_| ())?;
            let resp = self.send_checked(req).await?;
            let status = resp.status;
            if status.is_server_error()
                || matches!(
                    status,
                    StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
                )
            {
                return Err(());
            }
            if !status.is_redirection() {
                return Ok(resp);
            }
            let Some(loc) = resp.headers.get(LOCATION).and_then(|v| v.to_str().ok()) else {
                return Ok(resp);
            };
            let next = current.join(loc).map_err(|_| ())?;
            if matches!(next.scheme(), "http" | "https")
                && next.host_str().is_some()
                && is_same_origin(&current, &next)
            {
                current = next;
                continue;
            }
            return Err(());
        }
        Err(())
    }

    pub(super) async fn discover(
        &self,
        base_url: &Url,
        initial_challenge: Option<&str>,
    ) -> Result<DiscoveryResult> {
        let fail = || Error::bad("OAuth metadata discovery failed");
        let mut challenge_scopes: Option<Vec<String>> = None;
        let mut discovered_resource: Option<String> = None;
        let mut resource_scopes: Vec<String> = Vec::new();

        if let Some(hdr) = initial_challenge {
            let params = parse_www_authenticate(hdr, base_url);
            challenge_scopes = Some(params.scopes.clone());
            if let Some(prm_url) = params.resource_metadata_url
                && let Some(prm) = self
                    .fetch_prm(&prm_url, base_url, true)
                    .await
                    .map_err(|_| fail())?
            {
                if let Some(r) = &prm.resource {
                    discovered_resource = Some(r.clone());
                }
                resource_scopes = prm.scopes_supported.clone().unwrap_or_default();
                let meta = self
                    .resolve_as_from_prm(base_url, &prm_url, prm)
                    .await
                    .map_err(|_| fail())?;
                if let Some(metadata) = meta {
                    return Self::finish_discovery(
                        base_url,
                        metadata,
                        discovered_resource,
                        challenge_scopes,
                        resource_scopes,
                    );
                }
            }
        }

        if let Some((prm_url, prm)) = self
            .discover_prm(base_url, &mut challenge_scopes)
            .await
            .map_err(|_| fail())?
        {
            if let Some(r) = &prm.resource {
                discovered_resource = Some(r.clone());
            }
            resource_scopes = prm.scopes_supported.clone().unwrap_or_default();
            if let Some(metadata) = self
                .resolve_as_from_prm(base_url, &prm_url, prm)
                .await
                .map_err(|_| fail())?
            {
                return Self::finish_discovery(
                    base_url,
                    metadata,
                    discovered_resource,
                    challenge_scopes,
                    resource_scopes,
                );
            }
        }

        if let Some(metadata) = self
            .try_discover_as(base_url, None)
            .await
            .map_err(|_| fail())?
        {
            return Self::finish_discovery(
                base_url,
                metadata,
                discovered_resource,
                challenge_scopes,
                resource_scopes,
            );
        }

        Err(Error::bad("MCP server did not publish an OAuth issuer"))
    }

    fn finish_discovery(
        base_url: &Url,
        metadata: AuthorizationMetadata,
        discovered_resource: Option<String>,
        challenge_scopes: Option<Vec<String>>,
        resource_scopes: Vec<String>,
    ) -> Result<DiscoveryResult> {
        let issuer = metadata
            .issuer
            .as_deref()
            .ok_or_else(|| Error::bad("MCP server did not publish an OAuth issuer"))?;
        for raw in [
            Some(issuer),
            Some(metadata.authorization_endpoint.as_str()),
            Some(metadata.token_endpoint.as_str()),
            metadata.registration_endpoint.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            let url = Url::parse(raw).map_err(|_| Error::bad("invalid OAuth URL"))?;
            validate_endpoint(&url)?;
            if base_url.scheme() == "https" && url.scheme() != "https" {
                return Err(Error::bad("OAuth metadata attempts to downgrade HTTPS"));
            }
        }
        if let Some(types) = &metadata.response_types_supported
            && !types.iter().any(|t| t == "code")
        {
            return Err(Error::bad("OAuth server does not support authorization code flow"));
        }
        if let Some(methods) = &metadata.code_challenge_methods_supported
            && !methods.iter().any(|m| m == "S256")
        {
            return Err(Error::bad("OAuth server does not support PKCE S256"));
        }
        let resource = discovered_resource
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| base_url.to_string());
        let mut scopes = challenge_scopes
            .filter(|s| !s.is_empty())
            .unwrap_or(resource_scopes);
        scopes = dedup_scopes(scopes);
        if !scopes.is_empty()
            && !scopes.iter().any(|s| s == "offline_access")
            && metadata
                .scopes_supported
                .as_ref()
                .is_some_and(|s| s.iter().any(|x| x == "offline_access"))
        {
            scopes.push("offline_access".to_owned());
        }
        Ok(DiscoveryResult {
            metadata,
            resource,
            initial_scopes: scopes,
        })
    }

    async fn discover_prm(
        &self,
        base_url: &Url,
        challenge_scopes: &mut Option<Vec<String>>,
    ) -> std::result::Result<Option<(Url, ResourceMetadata)>, ()> {
        let mut requested = HashSet::new();
        let probe = self.discovery_get(base_url).await?;
        if probe.status == StatusCode::UNAUTHORIZED {
            let hdr_params = extract_challenge(&probe.headers, base_url);
            if challenge_scopes.is_none() {
                *challenge_scopes = Some(
                    hdr_params
                        .as_ref()
                        .map(|p| p.scopes.clone())
                        .unwrap_or_default(),
                );
            }
            if let Some(advertised) = hdr_params.and_then(|p| p.resource_metadata_url) {
                requested.insert(advertised.clone());
                if let Some(prm) = self.fetch_prm(&advertised, base_url, true).await? {
                    return Ok(Some((advertised, prm)));
                }
            }
        } else if challenge_scopes.is_none() {
            *challenge_scopes = Some(Vec::new());
        }

        for path in well_known_prm_paths(base_url.path()) {
            let mut url = base_url.clone();
            url.set_query(None);
            url.set_fragment(None);
            url.set_path(&path);
            if !requested.insert(url.clone()) {
                continue;
            }
            let resp = self.discovery_get(&url).await?;
            match resp.status {
                StatusCode::OK => {
                    if let Some(prm) = parse_prm(&resp.body, base_url, false)? {
                        return Ok(Some((url, prm)));
                    }
                }
                StatusCode::UNAUTHORIZED => {
                    let Some(pointer) =
                        extract_challenge(&resp.headers, base_url).and_then(|p| p.resource_metadata_url)
                    else {
                        continue;
                    };
                    if !requested.insert(pointer.clone()) {
                        continue;
                    }
                    if let Some(prm) = self.fetch_prm(&pointer, base_url, false).await? {
                        return Ok(Some((pointer, prm)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    async fn fetch_prm(
        &self,
        url: &Url,
        base_url: &Url,
        advertised: bool,
    ) -> std::result::Result<Option<ResourceMetadata>, ()> {
        let resp = self.discovery_get(url).await?;
        if resp.status != StatusCode::OK {
            return Ok(None);
        }
        parse_prm(&resp.body, base_url, advertised)
    }

    async fn resolve_as_from_prm(
        &self,
        base_url: &Url,
        prm_url: &Url,
        prm: ResourceMetadata,
    ) -> std::result::Result<Option<AuthorizationMetadata>, ()> {
        let mut candidates = Vec::new();
        let mut push = |raw: String| {
            let s = raw.trim();
            if !s.is_empty() && !candidates.iter().any(|c| c == s) {
                candidates.push(s.to_owned());
            }
        };
        if let Some(single) = prm.authorization_server {
            push(single);
        }
        for multi in prm.authorization_servers.unwrap_or_default() {
            push(multi);
        }
        if candidates.is_empty() {
            return Ok(None);
        }
        for candidate in candidates {
            let Ok(url) = Url::parse(&candidate).or_else(|_| prm_url.join(&candidate)) else {
                continue;
            };
            if !is_allowed_metadata_url(base_url, &url) {
                continue;
            }
            if url.path().contains("/.well-known/") {
                if let Some(meta) = self.fetch_as_metadata(&url, None).await? {
                    return Ok(Some(meta));
                }
                continue;
            }
            if let Some(meta) = self.try_discover_as(&url, Some(url.as_str())).await? {
                return Ok(Some(meta));
            }
        }
        Err(())
    }

    async fn try_discover_as(
        &self,
        base_url: &Url,
        expected_issuer: Option<&str>,
    ) -> std::result::Result<Option<AuthorizationMetadata>, ()> {
        for url in as_discovery_urls(base_url) {
            if let Some(meta) = self.fetch_as_metadata(&url, expected_issuer).await? {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    async fn fetch_as_metadata(
        &self,
        url: &Url,
        expected_issuer: Option<&str>,
    ) -> std::result::Result<Option<AuthorizationMetadata>, ()> {
        let resp = self.discovery_get(url).await?;
        if resp.status != StatusCode::OK {
            return Ok(None);
        }
        let Ok(meta) = serde_json::from_slice::<AuthorizationMetadata>(&resp.body) else {
            return Ok(None);
        };
        let expected = expected_issuer
            .map(str::to_owned)
            .or_else(|| expected_issuer_for_url(url));
        if let Some(expected) = expected {
            let Some(received) = meta.issuer.as_deref() else {
                return Err(());
            };
            if !issuers_match(received, &expected) {
                return Err(());
            }
        }
        Ok(Some(meta))
    }

    pub(super) async fn register_client(
        &self,
        metadata: &AuthorizationMetadata,
        redirect_uri: &str,
        scopes: &[&str],
    ) -> Result<(String, Option<String>)> {
        let fail = || {
            Error::bad(
                "OAuth client registration failed; configure a registered client ID if registration is unavailable",
            )
        };
        let reg_url = metadata.registration_endpoint.as_deref().ok_or_else(fail)?;
        let mut current = Url::parse(reg_url).map_err(|_| fail())?;
        let mut payload = serde_json::json!({
            "client_name": "MyGo-Bot",
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "token_endpoint_auth_method": "none",
            "response_types": ["code"],
            "application_type": "native",
        });
        if !scopes.is_empty() {
            payload["scope"] = Value::String(scopes.join(" "));
        }
        let body = serde_json::to_vec(&payload).map_err(|_| fail())?;
        let mut resp = None;
        for _ in 0..=MAX_REDIRECTS {
            let req = self
                .http
                .post(current.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .build()
                .map_err(|_| fail())?;
            let r = self.send_checked(req).await.map_err(|_| fail())?;
            if r.status.is_redirection()
                && matches!(
                    r.status,
                    StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
                )
                && let Some(loc) = r.headers.get(LOCATION).and_then(|v| v.to_str().ok())
                && let Ok(next) = current.join(loc)
                && self.resolver.allows_url(&next)
            {
                current = next;
                continue;
            }
            resp = Some(r);
            break;
        }
        let resp = resp.ok_or_else(fail)?;
        if !resp.status.is_success() {
            return Err(fail());
        }
        #[derive(Deserialize)]
        struct RegResponse {
            client_id: String,
            client_secret: Option<String>,
        }
        let reg: RegResponse = serde_json::from_slice(&resp.body).map_err(|_| fail())?;
        if reg.client_id.trim().is_empty() {
            return Err(fail());
        }
        Ok((reg.client_id, reg.client_secret.filter(|s| !s.is_empty())))
    }

    pub(super) async fn exchange_code(
        &self,
        binding: &Binding,
        code: &str,
        pkce_verifier: &str,
    ) -> Result<TokenResponse> {
        let req = build_token_request(&self.http, binding, &[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &binding.redirect_uri),
            ("code_verifier", pkce_verifier),
            ("resource", &binding.resource),
        ])
        .map_err(|_| Error::bad("OAuth callback rejected or token exchange failed"))?;
        let resp = self
            .send_checked(req)
            .await
            .map_err(|_| Error::bad("OAuth callback rejected or token exchange failed"))?;
        if !resp.status.is_success() {
            return Err(Error::bad("OAuth callback rejected or token exchange failed"));
        }
        parse_token_response(&resp.body)
            .ok_or_else(|| Error::bad("OAuth callback rejected or token exchange failed"))
    }

    pub(super) async fn refresh_token(
        &self,
        binding: &Binding,
        refresh_token: &str,
    ) -> std::result::Result<TokenResponse, RefreshError> {
        let req = build_token_request(&self.http, binding, &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("resource", &binding.resource),
        ])
        .map_err(|()| RefreshError::Transient)?;
        let resp = self
            .send_checked(req)
            .await
            .map_err(|()| RefreshError::Transient)?;
        if !resp.status.is_success() {
            #[derive(Deserialize)]
            struct ErrBody {
                error: Option<String>,
            }
            if let Ok(err) = serde_json::from_slice::<ErrBody>(&resp.body)
                && err.error.as_deref() == Some("invalid_grant")
            {
                return Err(RefreshError::Rejected);
            }
            return Err(RefreshError::Transient);
        }
        let mut token = parse_token_response(&resp.body).ok_or(RefreshError::Transient)?;
        if token.refresh_token.as_deref().is_none_or(str::is_empty) {
            token.refresh_token = Some(refresh_token.to_owned());
        }
        Ok(token)
    }
}

fn build_token_request(
    http: &Client,
    binding: &Binding,
    pairs: &[(&str, &str)],
) -> std::result::Result<reqwest::Request, ()> {
    let token_url = Url::parse(&binding.metadata.token_endpoint).map_err(|_| ())?;
    let mut req = http.post(token_url);
    let mut form = Serializer::new(String::new());
    for (k, v) in pairs {
        form.append_pair(k, v);
    }
    if let Some(secret) = binding.client_secret.as_deref() {
        let use_post = binding
            .metadata
            .additional_fields
            .get("token_endpoint_auth_methods_supported")
            .and_then(Value::as_array)
            .is_some_and(|arr| {
                let basic = arr.iter().any(|m| m.as_str() == Some("client_secret_basic"));
                let post = arr.iter().any(|m| m.as_str() == Some("client_secret_post"));
                post && !basic
            });
        if use_post {
            form.append_pair("client_id", &binding.client_id)
                .append_pair("client_secret", secret);
        } else {
            let enc = |s: &str| byte_serialize(s.as_bytes()).collect::<String>();
            req = req.basic_auth(enc(&binding.client_id), Some(enc(secret)));
        }
    } else {
        form.append_pair("client_id", &binding.client_id);
    }
    let body = form.finish();
    req.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(body)
        .build()
        .map_err(|_| ())
}

pub(super) struct DiscoveryResult {
    pub metadata: AuthorizationMetadata,
    pub resource: String,
    pub initial_scopes: Vec<String>,
}

#[derive(Deserialize)]
struct ResourceMetadata {
    resource: Option<String>,
    authorization_server: Option<String>,
    authorization_servers: Option<Vec<String>>,
    scopes_supported: Option<Vec<String>>,
}

#[derive(Default)]
struct WwwChallenge {
    resource_metadata_url: Option<Url>,
    scopes: Vec<String>,
}

fn parse_header_param(header: &str, key: &str) -> Vec<String> {
    let lower = header.to_ascii_lowercase();
    let needle = format!("{key}=");
    let mut out = Vec::new();
    let mut offset = 0;
    while let Some(pos) = lower[offset..].find(&needle) {
        let start = offset + pos + needle.len();
        let tail = header[start..].trim_start();
        let leading = header[start..].len() - tail.len();
        if let Some(rest) = tail.strip_prefix('"') {
            let mut escaped = false;
            let mut val = String::new();
            let mut closed = None;
            for (idx, ch) in rest.char_indices() {
                if escaped {
                    val.push(ch);
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    closed = Some(start + leading + idx + 2);
                    break;
                } else {
                    val.push(ch);
                }
            }
            let Some(next_off) = closed else { break };
            out.push(val);
            offset = next_off;
        } else {
            let end = tail
                .find(|c: char| c == ',' || c == ';' || c.is_whitespace())
                .unwrap_or(tail.len());
            out.push(tail[..end].to_owned());
            offset = start + leading + end;
        }
    }
    out
}

fn parse_www_authenticate(header: &str, base_url: &Url) -> WwwChallenge {
    let mut out = WwwChallenge::default();
    for raw in parse_header_param(header, "resource_metadata") {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(url) = Url::parse(trimmed).or_else(|_| base_url.join(trimmed)) else {
            continue;
        };
        if is_allowed_metadata_url(base_url, &url) {
            out.resource_metadata_url = Some(url);
            break;
        }
    }
    if let Some(scope) = parse_header_param(header, "scope").into_iter().next() {
        out.scopes = scope.split_whitespace().map(str::to_owned).collect();
    }
    out
}

fn extract_challenge(
    headers: &reqwest::header::HeaderMap,
    base_url: &Url,
) -> Option<WwwChallenge> {
    let mut scope_only = None;
    for hdr in headers.get_all(WWW_AUTHENTICATE) {
        let Ok(text) = hdr.to_str() else { continue };
        let parsed = parse_www_authenticate(text, base_url);
        if parsed.resource_metadata_url.is_some() {
            return Some(parsed);
        }
        if scope_only.is_none() && !parsed.scopes.is_empty() {
            scope_only = Some(parsed);
        }
    }
    scope_only
}

fn is_allowed_metadata_url(base_url: &Url, url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    if is_same_origin(base_url, url) {
        return true;
    }
    if base_url.scheme() == "https" && url.scheme() != "https" {
        return false;
    }
    if is_disallowed_hostname(host) {
        return is_loopback_host(base_url) && is_loopback_host(url);
    }
    if let Some(ip) = literal_ip(url) {
        return match AddressClass::of(ip) {
            AddressClass::Public => true,
            AddressClass::Loopback => is_loopback_host(base_url),
            AddressClass::Private => literal_ip(base_url)
                .is_some_and(|b| AddressClass::of(b) == AddressClass::Private),
            AddressClass::Blocked => false,
        };
    }
    true
}

fn well_known_prm_paths(base_path: &str) -> Vec<String> {
    let trimmed = base_path.trim_matches('/');
    let canonical = "/.well-known/oauth-protected-resource".to_owned();
    if trimmed.is_empty() {
        return vec![canonical];
    }
    vec![
        format!("{canonical}/{trimmed}"),
        format!("/{trimmed}/.well-known/oauth-protected-resource"),
        canonical,
    ]
}

fn as_discovery_urls(base_url: &Url) -> Vec<Url> {
    let trimmed = base_url.path().trim_matches('/');
    let paths = if trimmed.is_empty() {
        vec![
            "/.well-known/oauth-authorization-server".to_owned(),
            "/.well-known/openid-configuration".to_owned(),
        ]
    } else {
        vec![
            format!("/.well-known/oauth-authorization-server/{trimmed}"),
            format!("/.well-known/openid-configuration/{trimmed}"),
            format!("/{trimmed}/.well-known/openid-configuration"),
            "/.well-known/oauth-authorization-server".to_owned(),
        ]
    };
    paths
        .into_iter()
        .map(|p| {
            let mut u = base_url.clone();
            u.set_query(None);
            u.set_fragment(None);
            u.set_path(&p);
            u
        })
        .collect()
}

fn expected_issuer_for_url(discovery_url: &Url) -> Option<String> {
    let path = discovery_url.path();
    let oauth_prefix = "/.well-known/oauth-authorization-server";
    let oidc_prefix = "/.well-known/openid-configuration";
    let issuer_path = if path == oauth_prefix || path == oidc_prefix {
        ""
    } else if let Some(s) = path.strip_prefix(&format!("{oauth_prefix}/")) {
        s
    } else if let Some(s) = path.strip_prefix(&format!("{oidc_prefix}/")) {
        s
    } else {
        let p = path.strip_suffix(oidc_prefix)?;
        p.trim_start_matches('/')
    };
    let mut u = discovery_url.clone();
    u.set_query(None);
    u.set_fragment(None);
    if issuer_path.is_empty() {
        u.set_path("");
    } else {
        u.set_path(&format!("/{issuer_path}"));
    }
    Some(u.to_string())
}

fn trim_root_slash(s: &str) -> &str {
    s.strip_suffix('/')
        .filter(|t| Url::parse(t).is_ok_and(|u| u.path().is_empty() || u.path() == "/"))
        .unwrap_or(s)
}

fn issuers_match(received: &str, expected: &str) -> bool {
    received == expected || trim_root_slash(received) == trim_root_slash(expected)
}

fn is_resource_id_valid(expected: &Url, actual: &Url) -> bool {
    if expected == actual {
        return true;
    }
    if expected.scheme() != actual.scheme()
        || expected.host_str() != actual.host_str()
        || expected.port_or_known_default() != actual.port_or_known_default()
    {
        return false;
    }
    let exp = expected.path();
    let act = actual.path();
    exp == act
        || (exp.starts_with(act) && (act.ends_with('/') || exp.as_bytes().get(act.len()) == Some(&b'/')))
}

fn parse_prm(
    body: &[u8],
    base_url: &Url,
    advertised: bool,
) -> std::result::Result<Option<ResourceMetadata>, ()> {
    let Ok(prm) = serde_json::from_slice::<ResourceMetadata>(body) else {
        return Ok(None);
    };
    if prm.resource.is_none()
        && prm.authorization_server.is_none()
        && prm.authorization_servers.is_none()
    {
        return if advertised { Err(()) } else { Ok(None) };
    }
    let valid = prm
        .resource
        .as_deref()
        .and_then(|r| Url::parse(r).ok())
        .is_some_and(|u| u.fragment().is_none() && is_resource_id_valid(base_url, &u));
    if !valid {
        return if advertised { Err(()) } else { Ok(None) };
    }
    Ok(Some(prm))
}

fn parse_token_response(body: &[u8]) -> Option<TokenResponse> {
    let tok: TokenResponse = serde_json::from_slice(body).ok()?;
    if tok.access_token.is_empty() || !tok.token_type.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(tok)
}

fn dedup_scopes(scopes: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    scopes
        .into_iter()
        .filter(|s| seen.insert(s.clone()))
        .collect()
}

pub(super) fn build_authorization_url(
    binding: &Binding,
    scopes: &[&str],
) -> Result<(String, PendingState)> {
    let mut auth_url = Url::parse(&binding.metadata.authorization_endpoint)
        .map_err(|_| Error::bad("OAuth authorization request failed"))?;
    let csrf_token = random_token()?;
    let pkce_verifier = random_token()?;
    let challenge = BASE64_URL_SAFE_NO_PAD.encode(sha256(pkce_verifier.as_bytes()));
    {
        let mut query = auth_url.query_pairs_mut();
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &binding.client_id)
            .append_pair("redirect_uri", &binding.redirect_uri)
            .append_pair("state", &csrf_token)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("resource", &binding.resource);
        if !scopes.is_empty() {
            query.append_pair("scope", &scopes.join(" "));
        }
    }
    let require_issuer = binding
        .metadata
        .additional_fields
        .get("authorization_response_iss_parameter_supported")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let pending = PendingState {
        pkce_verifier,
        csrf_token,
        expected_issuer: binding.metadata.issuer.clone(),
        require_issuer,
        created_at: now_epoch_secs(),
        requested_scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
    };
    Ok((auth_url.into(), pending))
}

pub(super) fn find_bearer_challenge(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get_all(WWW_AUTHENTICATE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|value| value.to_ascii_lowercase().contains("bearer"))
        .map(str::to_owned)
}

pub(in crate::mcp) fn header_has_bearer_challenge(headers: &reqwest::header::HeaderMap) -> bool {
    headers.contains_key(WWW_AUTHENTICATE)
}

pub(super) fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn random_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| Error::internal("OAuth entropy unavailable"))?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn bearer_value(token: &str) -> std::result::Result<HeaderValue, ()> {
    HeaderValue::from_str(&format!("Bearer {token}"))
        .map(|mut v| {
            v.set_sensitive(true);
            v
        })
        .map_err(|_| ())
}

pub(super) fn attach_bearer(
    headers: &mut reqwest::header::HeaderMap,
    token: &str,
) -> std::result::Result<(), ()> {
    headers.insert(AUTHORIZATION, bearer_value(token)?);
    Ok(())
}

/// FIPS 180-4 SHA-256 over arbitrary bytes.
fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (input.len() as u64) * 8;
    let pad_len = (64 - (input.len() + 9) % 64) % 64;
    let mut buf = Vec::with_capacity(input.len() + 9 + pad_len);
    buf.extend_from_slice(input);
    buf.push(0x80);
    buf.resize(buf.len() + pad_len, 0);
    buf.extend_from_slice(&bit_len.to_be_bytes());

    for block in buf.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(block[4 * i..4 * i + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (slot, delta) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *slot = slot.wrapping_add(delta);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in h.into_iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

