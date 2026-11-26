use super::*;
use axum::{
    Router,
    http::{HeaderMap, Uri, header},
    routing::get,
};
use client::{OAuthHttp, PendingState, StoredCredentials, build_authorization_url, find_bearer_challenge, now_epoch_secs};
use tokio::net::TcpListener;

pub(super) fn discovery_base(endpoint: &str) -> Result<Url> {
    let mut url = parse(endpoint)?;
    validate_endpoint(&url)?;
    // Endpoint query parameters can be transport secrets. Keep them out of the browser URL.
    url.set_query(None);
    Ok(url)
}

pub(super) fn parameter(url: &Url, name: &str) -> Result<String> {
    let mut matches = url.query_pairs().filter(|(key, _)| key == name);
    let value = matches
        .next()
        .filter(|(_, v)| !v.is_empty())
        .ok_or_else(|| Error::bad("missing OAuth parameter"))?;
    if matches.next().is_some() {
        return Err(Error::bad("duplicate OAuth parameter"));
    }
    Ok(value.1.into_owned())
}

pub(super) fn callback_initiator_node(url: &str) -> Result<Option<String>> {
    let parsed = parse(url)?;
    validate_endpoint(&parsed)?;
    let state = parameter(&parsed, "state")?;
    Ok(state
        .split_once('.')
        .map(|(prefix, _)| prefix)
        .filter(|prefix| !prefix.is_empty() && prefix.bytes().all(|b| b.is_ascii_alphanumeric()))
        .map(str::to_owned))
}

#[derive(Deserialize)]
struct Server {
    url: String,
    oauth: Config,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

pub(super) async fn start(app: &App, id: McpServerId) -> Result<Login> {
    let mut nonce = [0_u8; 24];
    getrandom::fill(&mut nonce).map_err(|_| Error::internal("OAuth nonce unavailable"))?;
    let generation: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
    let key = generation.clone();
    let (revision, server, timeout, local_node_id, auth_config_ts, auth_fingerprint) = app
        .db
        .write(
            move |conn| {
                let (transport, raw, revision, timeout, oauth, auth_config_ts): (
                    String,
                    String,
                    i64,
                    u32,
                    bool,
                    String,
                ) = conn.query_row(
                    "SELECT transport,config_json,revision,timeout_ms,json_type(config_json,'$.oauth') IS 'object',auth_config_ts
                     FROM mcp_servers WHERE id=? AND deleted=0",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
                )?;
                if !oauth {
                    return Err(Error::bad("MCP server has no OAuth configuration"));
                }
                let server: Server = decode(&raw)?;
                server.oauth.validate()?;
                let auth_fp = crate::cluster::ledger::auth_config_fingerprint(&transport, &raw);
                let sql = format!(
                    "INSERT INTO mcp_oauth(server_id,generation,auth_config_ts,auth_fingerprint) VALUES(?,?,?,?)
                     ON CONFLICT(server_id)
                     DO UPDATE SET generation=excluded.generation,auth_config_ts=excluded.auth_config_ts,
                                   auth_fingerprint=excluded.auth_fingerprint,binding=NULL,pending=NULL,
                                   credentials=NULL,pending_refresh=NULL,rejected=0
                     WHERE NOT {}",
                    super::storage::AUTHORIZED_SQL
                );
                let updated = conn.execute(&sql, params![id, key, auth_config_ts, auth_fp])?;
                if updated == 0 {
                    return Err(Error::conflict(
                        "MCP OAuth already authorized; sign out before replacing the account",
                    ));
                }
                let local_node_id = crate::cluster::identity::load_local_node(conn)?.node_id;
                Ok((
                    revision,
                    server,
                    Duration::from_millis(timeout.into()),
                    local_node_id,
                    auth_config_ts,
                    auth_fp,
                ))
            },
            move |s, values| {
                s.mcp.oauth().cancel_login(id);
                s.mcp.forget(id);
                Ok(values)
            },
        )
        .await?;
    let account = Account {
        db: app.db.clone(),
        id,
        revision,
        generation,
        auth_config_ts,
        auth_fingerprint,
        allow_disabled: true,
    };
    tokio::time::timeout(
        timeout,
        prepare(
            app,
            &account,
            &local_node_id,
            server.url,
            server.oauth,
            server.headers,
        ),
    )
    .await
    .map_err(|_| Error::bad("OAuth discovery or registration timed out"))?
}

async fn prepare(
    app: &App,
    account: &Account,
    local_node_id: &str,
    endpoint: String,
    config: Config,
    headers: BTreeMap<String, String>,
) -> Result<Login> {
    let base = discovery_base(&endpoint)?;
    let oauth_http = OAuthHttp::new(&base).map_err(Error::internal)?;
    let challenge = if !headers.is_empty() || parse(&endpoint)?.query().is_some() {
        // Forward configured headers only to the explicitly configured MCP endpoint.
        let mut req = app
            .client
            .get(&endpoint)
            .header(header::ACCEPT, "application/json, text/event-stream");
        for (name, value) in &headers {
            req = req.header(name, value);
        }
        let response = req
            .send()
            .await
            .map_err(|_| Error::bad("OAuth resource probe failed"))?;
        find_bearer_challenge(response.headers())
    } else {
        None
    };
    let discovery = oauth_http.discover(&base, challenge.as_deref()).await?;
    let (redirect_uri, listener) = match config.redirect_uri {
        Some(uri) => (parse(&uri)?.to_string(), None),
        None => {
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
            (
                format!("http://{}/callback", listener.local_addr()?),
                Some(listener),
            )
        }
    };
    let scopes = if config.scopes.is_empty() {
        discovery.initial_scopes
    } else {
        config.scopes
    };
    let scope_refs: Vec<&str> = scopes.iter().map(String::as_str).collect();
    let (client_id, client_secret) = if let Some(id) = config.client_id {
        (id, config.client_secret)
    } else {
        oauth_http
            .register_client(&discovery.metadata, &redirect_uri, &scope_refs)
            .await?
    };
    let binding = Binding {
        resource: discovery.resource,
        metadata: discovery.metadata,
        client_id,
        client_secret,
        redirect_uri: redirect_uri.clone(),
    };
    let (authorization_url, pending) = build_authorization_url(&binding, &scope_refs, local_node_id)?;
    let expires_at = pending.created_at + u64::from(LOGIN_TTL);
    let stop = CancellationToken::new();
    let listener_stop = listener.is_some().then(|| stop.clone());
    storage::save_pending_and_binding(&app.db, account, &binding, &pending, listener_stop).await?;
    if let Some(listener) = listener {
        serve_callback(app, account, listener, stop)?;
    }
    Ok(Login {
        authorization_url,
        redirect_uri,
        expires_at,
    })
}

fn serve_callback(
    app: &App,
    account: &Account,
    listener: TcpListener,
    stop: CancellationToken,
) -> Result<()> {
    let host = listener.local_addr()?.to_string();
    let db = app.db.clone();
    let id = account.id;
    let generation = account.generation.clone();
    let routes = Router::new().route(
        "/callback",
        get(move |headers: HeaderMap, uri: Uri| {
            let (db, host, generation) = (db.clone(), host.clone(), generation.clone());
            async move {
                // Accept only origin-form requests for this socket's exact loopback authority.
                if headers.get(header::HOST).and_then(|h| h.to_str().ok()) != Some(host.as_str())
                    || uri.authority().is_some()
                {
                    return Err(Error::bad("invalid OAuth callback host"));
                }
                complete(&db, id, &format!("http://{host}{uri}"), Some(generation)).await?;
                Ok::<_, Error>((
                    [
                        (header::CACHE_CONTROL, "no-store"),
                        (header::REFERRER_POLICY, "no-referrer"),
                    ],
                    "Authorization complete. You can close this window.",
                ))
            }
        }),
    );
    let shutdown = app.stopping.clone();
    let db = app.db.clone();
    let generation = account.generation.clone();
    app.workers.spawn(async move {
        let _ = axum::serve(listener, routes)
            .with_graceful_shutdown(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => (),
                    _ = stop.cancelled() => (),
                    _ = tokio::time::sleep(Duration::from_secs(LOGIN_TTL.into())) => (),
                }
            })
            .await;
        let _ = db
            .call(move |s| {
                let runtime = s.mcp.oauth();
                if runtime
                    .listeners
                    .get(&id)
                    .is_some_and(|(current, _)| current == &generation)
                {
                    runtime.cancel_login(id);
                }
                Ok(())
            })
            .await;
    });
    Ok(())
}

pub(super) async fn complete(
    db: &Db,
    id: McpServerId,
    url: &str,
    expected_generation: Option<String>,
) -> Result<()> {
    let url = parse(url)?;
    validate_endpoint(&url)?;
    let state = parameter(&url, "state")?;
    let deny = url.query_pairs().any(|(key, _)| key == "error");
    let code = if deny {
        parameter(&url, "error")?;
        if url.query_pairs().any(|(k, _)| k == "code") {
            return Err(Error::bad("ambiguous OAuth callback"));
        }
        None
    } else {
        Some(parameter(&url, "code")?)
    };
    let issuer = if url.query_pairs().any(|(key, _)| key == "iss") {
        Some(parameter(&url, "iss")?)
    } else {
        None
    };
    let mut redirect = url.clone();
    redirect.set_query(None);
    let lookup_state = state.clone();
    let (binding, endpoint, revision, generation, timeout, pending, auth_config_ts, auth_fingerprint) = db
        .call(move |s| {
            let sql = "SELECT o.binding, m.transport, m.config_json, m.revision, o.generation,
                              m.timeout_ms, o.pending, m.auth_config_ts, o.auth_config_ts, o.auth_fingerprint
                FROM mcp_oauth o JOIN mcp_servers m ON m.id=o.server_id WHERE m.id=?1 AND m.deleted=0
                AND json_extract(o.pending,'$.csrf_token')=?2 AND json_extract(o.pending,'$.created_at')+?3>unixepoch()";
            let row = s
                .conn
                .query_row(sql, params![id, lookup_state, LOGIN_TTL], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, u32>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, String>(8)?,
                        r.get::<_, String>(9)?,
                    ))
                })
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Error::bad("unknown or expired OAuth login"),
                    other => other.into(),
                })?;
            let (raw, transport, cfg, revision, generation, ms, pending, m_auth_ts, o_auth_ts, o_fp) = row;
            if expected_generation
                .as_ref()
                .is_some_and(|expected| expected != &generation)
            {
                return Err(Error::bad("OAuth login superseded"));
            }
            let live_fp = crate::cluster::ledger::auth_config_fingerprint(&transport, &cfg);
            if m_auth_ts != o_auth_ts || o_fp != live_fp {
                return Err(Error::bad("OAuth login superseded"));
            }
            let endpoint = serde_json::from_str::<serde_json::Value>(&cfg)
                .ok()
                .and_then(|v| v.get("url").and_then(|u| u.as_str()).map(str::to_owned))
                .ok_or_else(|| Error::bad("invalid MCP OAuth endpoint"))?;
            Ok((
                decode::<Binding>(&raw)?,
                endpoint,
                revision,
                generation,
                Duration::from_millis(ms.into()),
                decode::<PendingState>(&pending)?,
                m_auth_ts,
                o_fp,
            ))
        })
        .await?;
    if redirect.as_str() != binding.redirect_uri {
        return Err(Error::bad("OAuth callback URI mismatch"));
    }
    // Apply RFC 9207 to denials as well as successful callbacks: an unrelated issuer cannot consume state.
    if issuer
        .as_ref()
        .is_some_and(|value| Some(value) != pending.expected_issuer.as_ref())
        || issuer.is_none() && pending.require_issuer
    {
        return Err(Error::bad("OAuth callback issuer mismatch"));
    }
    if deny {
        db.write(
            move |conn| {
                let sql = "DELETE FROM mcp_oauth WHERE server_id=? AND generation=?
                           AND json_extract(pending,'$.csrf_token')=?";
                if conn.execute(sql, params![id, generation, state])? == 0 {
                    return Err(Error::bad("OAuth login superseded"));
                }
                Ok(())
            },
            move |s, ()| {
                s.mcp.oauth().cancel_login(id);
                s.mcp.forget(id);
                Ok(())
            },
        )
        .await?;
        return Err(Error::bad("OAuth login denied"));
    }
    let code = code.ok_or_else(|| Error::bad("invalid OAuth callback"))?;
    let account = Account {
        db: db.clone(),
        id,
        revision,
        generation,
        auth_config_ts,
        auth_fingerprint,
        allow_disabled: true,
    };
    account.consume_pending(&state).await?;
    let base = discovery_base(&endpoint)?;
    let oauth_http = OAuthHttp::new(&base).map_err(Error::internal)?;
    tokio::time::timeout(timeout, async {
        let tokens = oauth_http
            .exchange_code(&binding, &code, &pending.pkce_verifier)
            .await?;
        let granted_scopes = tokens
            .scope
            .as_deref()
            .map(|s| s.split_whitespace().map(str::to_owned).collect())
            .unwrap_or(pending.requested_scopes);
        let stored = StoredCredentials {
            client_id: binding.client_id.clone(),
            token_response: Some(tokens),
            granted_scopes,
            token_received_at: Some(now_epoch_secs()),
            issuer: binding.metadata.issuer.clone(),
        };
        if !account.save_credentials(stored).await? {
            return Err(Error::conflict("OAuth login superseded"));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::bad("OAuth token exchange timed out; start a new login"))??;
    db.call(move |s| {
        let sql = "SELECT EXISTS(SELECT 1 FROM mcp_oauth WHERE server_id=?
                   AND generation=? AND credentials IS NOT NULL)";
        let current: bool = s
            .conn
            .query_row(sql, params![id, account.generation], |r| r.get(0))?;
        if !current {
            return Err(Error::conflict("OAuth login superseded"));
        }
        s.mcp.oauth().cancel_login(id);
        s.mcp.forget(id);
        Ok(())
    })
    .await
}
