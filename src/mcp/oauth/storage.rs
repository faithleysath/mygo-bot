use super::*;
use async_trait::async_trait;
use oauth2::TokenResponse;
use rmcp::transport::auth::{
    CredentialStore, StateStore, StoredAuthorizationState, StoredCredentials,
};
use rusqlite::OptionalExtension;

#[derive(Clone)]
pub(super) struct Account {
    pub db: Db,
    pub id: i64,
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

fn store_error(_: Error) -> AuthError {
    AuthError::CredentialStoreError("MCP OAuth storage unavailable or configuration changed".into())
}

impl Account {
    pub async fn update(
        &self,
        set: &'static str,
        guard: &str,
        value: Option<String>,
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
                    Ok(())
                },
                |_, ()| Ok(()),
            )
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
            )
            .await;
    }
}

#[async_trait]
impl CredentialStore for Account {
    async fn load(&self) -> std::result::Result<Option<StoredCredentials>, AuthError> {
        let id = self.id;
        let revision = self.revision;
        let generation = self.generation.clone();
        self.db
            .call(move |s| {
                let sql = format!("SELECT credentials,rejected FROM mcp_oauth WHERE {CURRENT} AND {AUTHORIZED_SQL}");
                let row: Option<(Option<String>, bool)> = s
                    .conn
                    .query_row(&sql, params![id, generation, revision], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .optional()?;
                let Some((Some(raw), rejected)) = row else {
                    return Ok(None);
                };
                let mut stored: StoredCredentials = decode(&raw)?;
                if let Some(response) = &mut stored.token_response {
                    if response.refresh_token().is_none() {
                        // SQL checks real expiry for an access-only grant. Avoid rmcp's early
                        // refresh window, which would discard its last usable seconds.
                        response.set_expires_in(None);
                    } else if rejected {
                        response.set_expires_in(Some(&Duration::ZERO));
                        stored.token_received_at = Some(0);
                    }
                }
                Ok(Some(stored))
            })
            .await
            .map_err(store_error)
    }

    async fn save(&self, credentials: StoredCredentials) -> std::result::Result<(), AuthError> {
        let value = encode(&credentials).map_err(store_error)?;
        let id = self.id;
        let generation = self.generation.clone();
        let revision = self.revision;
        // A callback creates the grant and needs its original revision. A refresh
        // rotates an existing grant: an unrelated description/access edit retires
        // this caller, but the new connection still needs the issuer's rotated token.
        // Auth changes delete the generation, so this UPDATE can never restore it.
        let current = self.db.write(move |conn| {
            let sql = "UPDATE mcp_oauth SET credentials=?4,rejected=0
                WHERE server_id=?1 AND generation=?2 AND pending IS NULL AND
                (credentials IS NOT NULL OR EXISTS(SELECT 1 FROM mcp_servers WHERE id=?1 AND revision=?3))
                RETURNING (SELECT revision=?3 FROM mcp_servers WHERE id=?1)";
            conn.query_row(sql, params![id,generation,revision,value], |row| row.get::<_,bool>(0))
                .map_err(Into::into)
        }, |_, current| Ok(current)).await.map_err(store_error)?;
        // Report retirement *after* committing. This old caller must not send a POST.
        if current {
            Ok(())
        } else {
            Err(AuthError::AuthorizationRequired)
        }
    }

    async fn clear(&self) -> std::result::Result<(), AuthError> {
        self.update("credentials=?4,rejected=0", "", None)
            .await
            .map_err(store_error)
    }

    async fn on_refresh_token_rejected(
        &self,
        used: &StoredCredentials,
    ) -> std::result::Result<(), AuthError> {
        let Some(token) = used
            .token_response
            .as_ref()
            .and_then(TokenResponse::refresh_token)
        else {
            return Ok(());
        };
        let token = token.secret().to_owned();
        let id = self.id;
        let generation = self.generation.clone();
        self.db.write(move |conn| {
            conn.execute("UPDATE mcp_oauth SET credentials=NULL,rejected=0 WHERE server_id=? AND generation=?
                AND json_extract(credentials,'$.token_response.refresh_token')=?", params![id,generation,token])?;
            Ok(())
        }, |_, ()| Ok(())).await.map_err(store_error)
    }
}

#[async_trait]
impl StateStore for Account {
    async fn save(
        &self,
        _: &str,
        state: StoredAuthorizationState,
    ) -> std::result::Result<(), AuthError> {
        self.update(
            "pending=?4",
            "AND credentials IS NULL",
            Some(encode(&state).map_err(store_error)?),
        )
        .await
        .map_err(store_error)
    }

    async fn load(
        &self,
        token: &str,
    ) -> std::result::Result<Option<StoredAuthorizationState>, AuthError> {
        let token = token.to_owned();
        let id = self.id;
        let revision = self.revision;
        let generation = self.generation.clone();
        self.db.call(move |s| {
            let sql = format!("SELECT pending FROM mcp_oauth WHERE {CURRENT}
                AND json_extract(pending,'$.csrf_token')=?4 AND json_extract(pending,'$.created_at')+?5>unixepoch()");
            let raw: Option<String> = s.conn.query_row(&sql,params![id,generation,revision,token,LOGIN_TTL],|r| r.get(0)).optional()?;
            raw.as_deref().map(decode).transpose()
        }).await.map_err(store_error)
    }

    async fn delete(&self, token: &str) -> std::result::Result<(), AuthError> {
        // rmcp validates the issuer first. Only one matching callback may consume the verifier.
        self.update(
            "pending=NULL", &format!("AND json_extract(pending,'$.csrf_token')=?4 AND json_extract(pending,'$.created_at')+{LOGIN_TTL}>unixepoch()"),
            Some(token.to_owned()),
        ).await.map_err(store_error)
    }
}
