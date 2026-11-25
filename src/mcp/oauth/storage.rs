use super::*;
use client::{OAuthHttp, PendingState, StoredCredentials, now_epoch_secs};
use rusqlite::OptionalExtension;

#[derive(Clone)]
pub(super) struct Account {
    pub db: Db,
    pub id: McpServerId,
    pub revision: i64,
    pub generation: String,
}

// Every credential/state mutation is an UPDATE tied to the login that issued it. Only
// the login endpoint may create a row. A late callback or refresh cannot recreate it.
const CURRENT: &str = "server_id=?1 AND generation=?2 AND EXISTS(SELECT 1 FROM mcp_servers
    WHERE id=?1 AND revision=?3)";

// A refreshable grant can survive an expired or peer-rejected access token. A public
// client is allowed to receive just an access token, in which case login must reopen.
pub(super) const AUTHORIZED_SQL: &str = "coalesce(credentials IS NOT NULL AND (
    json_extract(credentials,'$.token_response.refresh_token') IS NOT NULL OR
    (NOT rejected AND (json_extract(credentials,'$.token_response.expires_in') IS NULL OR
     json_extract(credentials,'$.token_received_at') + json_extract(credentials,'$.token_response.expires_in') > unixepoch()))),0)";

impl Account {
    pub async fn update(
        &self,
        set: &'static str,
        guard: &str,
        value: Option<String>,
        sync: bool,
    ) -> Result<()> {
        let id = self.id;
        let revision = self.revision;
        let generation = self.generation.clone();
        let sql = format!("UPDATE mcp_oauth SET {set} WHERE {CURRENT} {guard}");
        self.db
            .write(
                move |conn| {
                    if conn.execute(&sql, params![id, generation, revision, value])? != 1 {
                        return Err(Error::conflict(
                            "OAuth login expired or configuration changed",
                        ));
                    }
                    if sync {
                        crate::cluster::ledger::record_mcp_if_shared(conn, id)?;
                    }
                    Ok(())
                },
                |_, ()| Ok(()),
            )
            .await
    }

    pub async fn consume_pending(&self, state: &str) -> Result<()> {
        self.update(
            "pending=NULL",
            &format!(
                "AND json_extract(pending,'$.csrf_token')=?4 AND json_extract(pending,'$.created_at')+{LOGIN_TTL}>unixepoch()"
            ),
            Some(state.to_owned()),
            false,
        )
        .await
    }

    pub async fn save_credentials(&self, credentials: StoredCredentials) -> Result<bool> {
        let value = encode(&credentials)?;
        let id = self.id;
        let generation = self.generation.clone();
        let revision = self.revision;
        // A callback creates the grant and needs its original revision. A refresh
        // rotates an existing grant: an unrelated description/access edit retires
        // this caller, but the new connection still needs the issuer's rotated token.
        // Auth changes delete the generation, so this UPDATE can never restore it.
        self.db
            .write(
                move |conn| {
                    let sql = "UPDATE mcp_oauth SET credentials=?4,rejected=0
                        WHERE server_id=?1 AND generation=?2 AND pending IS NULL AND
                        (credentials IS NOT NULL OR EXISTS(SELECT 1 FROM mcp_servers WHERE id=?1 AND revision=?3))
                        RETURNING (SELECT revision=?3 FROM mcp_servers WHERE id=?1)";
                    let current = conn
                        .query_row(sql, params![id, generation, revision, value], |row| {
                            row.get::<_, bool>(0)
                        })?;
                    crate::cluster::ledger::record_mcp_if_shared(conn, id)?;
                    Ok(current)
                },
                |_, current| Ok(current),
            )
            .await
    }

    pub async fn load_credentials(&self) -> Result<Option<(StoredCredentials, bool)>> {
        let id = self.id;
        let revision = self.revision;
        let generation = self.generation.clone();
        self.db
            .call(move |s| {
                let sql = format!(
                    "SELECT credentials,rejected FROM mcp_oauth WHERE {CURRENT} AND {AUTHORIZED_SQL}"
                );
                let row: Option<(Option<String>, bool)> = s
                    .conn
                    .query_row(&sql, params![id, generation, revision], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .optional()?;
                let Some((Some(raw), rejected)) = row else {
                    return Ok(None);
                };
                Ok(Some((decode(&raw)?, rejected)))
            })
            .await
    }

    pub async fn reject(&self, token: String) {
        // Only mark the exact bearer the peer rejected. Other requests may already have refreshed it.
        let _ = self
            .update(
                "rejected=1,credentials=CASE WHEN json_extract(credentials,'$.token_response.refresh_token') IS NULL
                 THEN NULL ELSE credentials END",
                "AND json_extract(credentials,'$.token_response.access_token')=?4",
                Some(token),
                true,
            )
            .await;
    }

    pub async fn on_refresh_token_rejected(&self, token: &str) -> Result<()> {
        let token = token.to_owned();
        let id = self.id;
        let generation = self.generation.clone();
        self.db
            .write(
                move |conn| {
                    conn.execute(
                        "UPDATE mcp_oauth SET credentials=NULL,rejected=0 WHERE server_id=? AND generation=?
                        AND json_extract(credentials,'$.token_response.refresh_token')=?",
                        params![id, generation, token],
                    )?;
                    crate::cluster::ledger::record_mcp_if_shared(conn, id)?;
                    Ok(())
                },
                |_, ()| Ok(()),
            )
            .await
    }
}

pub(super) async fn save_pending_and_binding(
    db: &Db,
    account: &Account,
    binding: &Binding,
    pending: &PendingState,
    register_listener: Option<CancellationToken>,
) -> Result<()> {
    let binding = encode(binding)?;
    let pending = encode(pending)?;
    let id = account.id;
    let revision = account.revision;
    let generation = account.generation.clone();
    let listener_generation = generation.clone();
    db.write(
        move |conn| {
            if conn.execute(
                "UPDATE mcp_oauth SET binding=?1,pending=?2 WHERE server_id=?3 AND generation=?4
                 AND EXISTS(SELECT 1 FROM mcp_servers WHERE id=?3 AND revision=?5)",
                params![binding, pending, id, generation, revision],
            )? != 1
            {
                return Err(Error::conflict("OAuth login superseded or configuration changed"));
            }
            Ok(())
        },
        move |s, ()| {
            if let Some(stop) = register_listener {
                s.mcp.oauth().listeners.insert(id, (listener_generation, stop));
            }
            Ok(())
        },
    )
    .await
}

pub(super) fn needs_refresh(creds: &StoredCredentials, rejected: bool) -> bool {
    let Some(response) = &creds.token_response else {
        return false;
    };
    if response.refresh_token.is_none() {
        return false;
    }
    if rejected {
        return true;
    }
    if let (Some(expires_in), Some(received_at)) = (response.expires_in, creds.token_received_at) {
        let elapsed = now_epoch_secs().saturating_sub(received_at);
        let remaining = expires_in.saturating_sub(elapsed);
        let buffer = 30u64.min(expires_in / 2).max(1);
        return remaining < buffer;
    }
    false
}

pub(super) async fn acquire_valid_token(
    oauth_http: &OAuthHttp,
    binding: &Binding,
    account: &Account,
) -> std::result::Result<String, bool> {
    let Some((creds, rejected)) = account.load_credentials().await.map_err(|_| false)? else {
        return Err(true);
    };
    let Some(token_resp) = &creds.token_response else {
        return Err(true);
    };
    if creds.client_id != binding.client_id {
        return Err(true);
    }
    if !needs_refresh(&creds, rejected) {
        return Ok(token_resp.access_token.clone());
    }
    let Some(refresh) = token_resp.refresh_token.as_deref() else {
        return Err(true);
    };
    let next_token = match oauth_http.refresh_token(binding, refresh).await {
        Ok(tok) => tok,
        Err(client::RefreshError::Rejected) => {
            let _ = account.on_refresh_token_rejected(refresh).await;
            return Err(true);
        }
        Err(client::RefreshError::Transient) => return Err(false),
    };
    let access_token = next_token.access_token.clone();
    let granted_scopes = next_token
        .scope
        .as_deref()
        .map(|s| s.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_else(|| creds.granted_scopes.clone());
    let next_creds = StoredCredentials {
        client_id: binding.client_id.clone(),
        token_response: Some(next_token),
        granted_scopes,
        token_received_at: Some(now_epoch_secs()),
        issuer: binding.metadata.issuer.clone(),
    };
    match account.save_credentials(next_creds).await {
        Ok(true) => Ok(access_token),
        Ok(false) | Err(_) => Err(true),
    }
}
