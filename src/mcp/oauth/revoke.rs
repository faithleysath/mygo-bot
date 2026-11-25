use super::{Binding, client::StoredCredentials, decode, validate_endpoint};
use crate::{App, Error, Result, id::McpServerId};
use axum::http::StatusCode;
use form_urlencoded::{Serializer, byte_serialize};
use reqwest::{Client, Url, header::CONTENT_TYPE};
use rusqlite::Connection;
use std::time::Duration;

struct Grant {
    credentials: Option<String>,
    binding: Option<String>,
    timeout: Duration,
}

// Keep decoding and network errors outside the transaction so neither can undo logout.
fn take(conn: &Connection, id: McpServerId, expected: Option<&str>, timeout: Duration) -> Result<Grant> {
    crate::cluster::ledger::assert_no_conflict(conn, id.as_str())?;
    let (generation, grant) = conn.query_row(
        "SELECT generation,credentials,binding FROM mcp_servers
         LEFT JOIN mcp_oauth ON server_id=id WHERE id=?",
        [id],
        |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                Grant {
                    credentials: r.get(1)?,
                    binding: r.get(2)?,
                    timeout,
                },
            ))
        },
    )?;
    if generation.is_some() && generation.as_deref() != expected {
        return Err(Error::conflict(
            "OAuth login changed while signing out; check the current login",
        ));
    }
    conn.execute("DELETE FROM mcp_oauth WHERE server_id=?", [id])?;
    if !crate::cluster::ledger::has_conflict(conn, id.as_str())? {
        crate::cluster::ledger::record_mcp_if_shared(conn, id)?;
    }
    Ok(grant)
}

fn incomplete(reason: &str) -> Error {
    Error(
        StatusCode::BAD_GATEWAY,
        format!(
            "Local OAuth credentials cleared. {reason}. Revoke the grant at the authorization server."
        ),
    )
}

fn endpoint(binding: &Binding) -> Result<Option<Url>> {
    let meta = &binding.metadata;
    let Some(raw) = meta.additional_fields.get("revocation_endpoint") else {
        return Ok(None);
    };
    let invalid = || {
        incomplete("The stored revocation endpoint is invalid or outside the authorized origins")
    };
    let url = raw
        .as_str()
        .and_then(|s| Url::parse(s).ok())
        .ok_or_else(invalid)?;
    validate_endpoint(&url).map_err(|_| invalid())?;
    let trusted = [meta.issuer.as_deref(), Some(meta.token_endpoint.as_str())]
        .into_iter()
        .flatten()
        .filter_map(|s| Url::parse(s).ok())
        .any(|bound| bound.origin() == url.origin());
    if !trusted {
        return Err(invalid());
    }
    Ok(Some(url))
}

enum ClientAuth<'a> {
    Public,
    Basic(&'a str),
    Post(&'a str),
}

impl<'a> ClientAuth<'a> {
    fn select(binding: &'a Binding) -> Result<Self> {
        let fields = &binding.metadata.additional_fields;
        let methods = fields
            .get("revocation_endpoint_auth_methods_supported")
            .or_else(|| fields.get("token_endpoint_auth_methods_supported"));
        let supports = |name: &str| {
            methods.is_none_or(|v| {
                v.as_array()
                    .is_some_and(|list| list.iter().any(|m| m.as_str() == Some(name)))
            })
        };
        match binding.client_secret.as_deref() {
            None if supports("none") => Ok(Self::Public),
            Some(secret) if supports("client_secret_basic") => Ok(Self::Basic(secret)),
            Some(secret) if supports("client_secret_post") => Ok(Self::Post(secret)),
            _ => Err(incomplete(
                "The authorization server's advertised revocation authentication methods cannot be used with this client",
            )),
        }
    }
}

struct Revocation<'a> {
    http: &'a Client,
    endpoint: Url,
    client_id: &'a str,
    auth: ClientAuth<'a>,
    timeout: Duration,
}

impl Revocation<'_> {
    async fn send(&self, token: &str, hint: &str) -> Result<()> {
        let mut request = self.http.post(self.endpoint.clone()).timeout(self.timeout);
        // RFC 6749 requires form-encoding both components of the Basic credentials.
        let body = {
            let mut form = Serializer::new(String::new());
            form.append_pair("token", token)
                .append_pair("token_type_hint", hint);
            match self.auth {
                ClientAuth::Basic(secret) => {
                    let encoded = |s: &str| byte_serialize(s.as_bytes()).collect::<String>();
                    request = request.basic_auth(encoded(self.client_id), Some(encoded(secret)));
                }
                ClientAuth::Post(secret) => {
                    form.append_pair("client_id", self.client_id)
                        .append_pair("client_secret", secret);
                }
                ClientAuth::Public => {
                    form.append_pair("client_id", self.client_id);
                }
            }
            form.finish()
        };
        // The application HTTP client disables redirects and retries. Never consume or echo an IdP error body.
        let response = request
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|_| incomplete("Remote token revocation could not be confirmed"))?;
        if !matches!(response.status(), StatusCode::OK | StatusCode::NO_CONTENT) {
            return Err(incomplete(&format!(
                "Token revocation returned HTTP {}",
                response.status().as_u16()
            )));
        }
        Ok(())
    }
}

pub(super) async fn logout(app: &App, id: McpServerId) -> Result<()> {
    let (generation, gate, timeout) = app
        .db
        .call(move |s| {
            let (generation, ms): (Option<String>, u32) = s.conn.query_row(
                "SELECT generation,timeout_ms FROM mcp_servers LEFT JOIN mcp_oauth ON server_id=id WHERE id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let gate = generation.as_ref().map(|g| s.mcp.refresh_gate(g));
            Ok((generation, gate, Duration::from_millis(ms.min(5_000).into())))
        })
        .await?;
    // A refresh that already sent its request may rotate both tokens. Wait outside
    // SQLite for its save. On timeout the generation-guarded DELETE still wins.
    let guard = match gate {
        Some(gate) => tokio::time::timeout(timeout, gate.lock_owned()).await.ok(),
        None => None,
    };
    let refresh_in_flight = generation.is_some() && guard.is_none();
    let grant = app
        .db
        .write(
            move |conn| take(conn, id, generation.as_deref(), timeout),
            move |s, grant| {
                s.mcp.oauth().cancel_login(id);
                s.mcp.forget(id);
                Ok(grant)
            },
        )
        .await?;
    drop(guard);
    remote(&app.client, grant, refresh_in_flight).await
}

async fn remote(http: &Client, grant: Grant, refresh_in_flight: bool) -> Result<()> {
    let Some(raw) = grant.credentials else {
        return Ok(());
    };
    let invalid = || incomplete("The stored OAuth grant could not be verified");
    let creds: StoredCredentials = decode(&raw).map_err(|_| invalid())?;
    let Some(tokens) = creds.token_response else {
        return Ok(());
    };
    let binding: Binding =
        decode(grant.binding.as_deref().ok_or_else(invalid)?).map_err(|_| invalid())?;
    if binding.client_id != creds.client_id
        || creds
            .issuer
            .as_ref()
            .is_some_and(|i| binding.metadata.issuer.as_ref() != Some(i))
    {
        return Err(invalid());
    }
    if let Some(endpoint) = endpoint(&binding)? {
        let revocation = Revocation {
            http,
            endpoint,
            client_id: &binding.client_id,
            auth: ClientAuth::select(&binding)?,
            timeout: grant.timeout,
        };
        let access = revocation.send(&tokens.access_token, "access_token");
        if let Some(refresh) = tokens.refresh_token.as_deref() {
            // RFC 7009 accepts already-revoked tokens. Attempt both even if one fails.
            let (refresh, access) = tokio::join!(revocation.send(refresh, "refresh_token"), access);
            refresh?;
            access?;
        } else {
            access.await?;
        }
    }
    if refresh_in_flight && tokens.refresh_token.is_some() {
        return Err(incomplete(
            "A concurrent token request may have rotated the refresh token",
        ));
    }
    Ok(())
}
