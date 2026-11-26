use crate::{
    App, Error, Result,
    id::{Id, ModelId, Prefix, ProviderId},
    media, pricing,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use rusqlite::{Connection, OptionalExtension, Row, ffi, params, types::Type};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use std::collections::HashSet;

type Document = Json<Map<String, Value>>;
pub fn routes() -> Router<App> {
    Router::new()
        .route("/providers", get(list_providers).post(create_provider))
        .route(
            "/providers/{id}",
            get(get_provider)
                .patch(patch_provider)
                .delete(archive_provider),
        )
        .route("/providers/{id}/restore", post(restore_provider))
        .route("/models", get(list_models).post(create_model))
        .route(
            "/models/{id}",
            get(get_model).patch(patch_model).delete(archive_model),
        )
        .route("/models/{id}/restore", post(restore_model))
}

fn document<T: DeserializeOwned + Serialize>(old: Option<&T>, fields: Document) -> Result<T> {
    if fields.is_empty() {
        return Err(Error::bad("expected catalog fields"));
    }
    let mut value = match old {
        Some(previous) => json!(previous),
        None => json!({}),
    };
    value.as_object_mut().unwrap().extend(fields.0);
    serde_json::from_value(value).map_err(|e| Error::bad(e.to_string()))
}

fn record<T, K: Prefix>(
    conn: &Connection,
    select: &str,
    id: Id<K>,
    read: fn(&Row) -> rusqlite::Result<T>,
) -> Result<T> {
    let mut stmt = conn.prepare_cached(&format!("{select} WHERE id=?"))?;
    Ok(stmt.query_row([id], read)?)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListOptions {
    #[serde(default)]
    include_archived: bool,
}

fn list<T>(
    conn: &Connection,
    select: &str,
    filter: &str,
    read: fn(&Row) -> rusqlite::Result<T>,
) -> Result<Json<Vec<T>>> {
    let mut stmt = conn.prepare_cached(&format!("{select} {filter} ORDER BY id"))?;
    let values = stmt.query_map([], read)?.collect::<rusqlite::Result<_>>()?;
    Ok(Json(values))
}

fn catalog_error(err: rusqlite::Error) -> Error {
    match err.sqlite_error().map(|e| e.extended_code) {
        Some(ffi::SQLITE_CONSTRAINT_UNIQUE) => {
            Error::conflict("name is already in use by an active catalog entry")
        }
        Some(ffi::SQLITE_CONSTRAINT_FOREIGNKEY) => Error::bad("unknown provider_id"),
        _ => err.into(),
    }
}

// The database worker commits this write atomically.
async fn set_archived<K: Prefix>(
    app: App,
    table: &'static str,
    id: Id<K>,
    archive: bool,
) -> Result<StatusCode> {
    app.db
        .write(
            move |conn| {
                let timestamp = archive.then(|| chrono::Utc::now().timestamp_millis());
                let ts = crate::cluster::ledger::tick_hlc(conn)?;
                let rev = crate::cluster::ledger::next_catalog_rev();
                if table == "llm_providers" {
                    if !archive {
                        let clash: bool = conn
                            .prepare_cached(
                                "SELECT EXISTS(
                                    SELECT 1 FROM llm_providers a JOIN llm_providers b ON b.id=?1
                                    WHERE a.id != b.id AND a.archived_at IS NULL AND a.name = b.name
                                )",
                            )?
                            .query_row([id], |r| r.get(0))?;
                        if clash {
                            return Err(Error::conflict(
                                "name is already in use by an active catalog entry",
                            ));
                        }
                    }
                    let val_sql = if archive {
                        "coalesce(archived_at, ?1)"
                    } else {
                        "?1"
                    };
                    let sql = format!(
                        "UPDATE llm_providers SET archived_at={val_sql}, archived_ts=?2, sync_rev=?3 WHERE id=?4"
                    );
                    if conn.execute(&sql, params![timestamp, ts, rev, id]).map_err(catalog_error)? == 0 {
                        return Err(rusqlite::Error::QueryReturnedNoRows.into());
                    }
                } else if table == "llm_models" {
                    if !archive {
                        let clash: bool = conn
                            .prepare_cached(
                                "SELECT EXISTS(
                                    SELECT 1 FROM llm_models a JOIN llm_models b ON b.id=?1
                                    WHERE a.id != b.id AND a.archived_at IS NULL
                                      AND a.provider_id = b.provider_id AND a.model_key = b.model_key
                                )",
                            )?
                            .query_row([id], |r| r.get(0))?;
                        if clash {
                            return Err(Error::conflict(
                                "name is already in use by an active catalog entry",
                            ));
                        }
                    }
                    let val_sql = if archive {
                        "coalesce(archived_at, ?1)"
                    } else {
                        "?1"
                    };
                    let sql = format!(
                        "UPDATE llm_models SET archived_at={val_sql}, archived_ts=?2, sync_rev=?3 WHERE id=?4"
                    );
                    if conn.execute(&sql, params![timestamp, ts, rev, id]).map_err(catalog_error)? == 0 {
                        return Err(rusqlite::Error::QueryReturnedNoRows.into());
                    }
                }
                crate::cluster::ledger::stage_catalog();
                Ok(())
            },
            |_, ()| Ok(StatusCode::NO_CONTENT),
        )
        .await
}

async fn archive_provider(
    State(app): State<App>,
    Path(id): Path<ProviderId>,
) -> Result<StatusCode> {
    set_archived(app, "llm_providers", id, true).await
}

async fn restore_provider(
    State(app): State<App>,
    Path(id): Path<ProviderId>,
) -> Result<StatusCode> {
    set_archived(app, "llm_providers", id, false).await
}

async fn archive_model(State(app): State<App>, Path(id): Path<ModelId>) -> Result<StatusCode> {
    set_archived(app, "llm_models", id, true).await
}

async fn restore_model(State(app): State<App>, Path(id): Path<ModelId>) -> Result<StatusCode> {
    set_archived(app, "llm_models", id, false).await
}

pub fn available_capabilities(conn: &Connection, id: ModelId) -> Result<(Value, Value)> {
    let (available, modalities, efforts) = conn
        .prepare_cached(
            "SELECT available,input_modalities,reasoning_efforts FROM model_catalog WHERE id=?",
        )?
        .query_row([id], |r| Ok((r.get::<_, bool>(0)?, r.get(1)?, r.get(2)?)))?;
    if !available {
        return Err(Error::conflict(format!(
            "model {id} or its provider is archived; select another model"
        )));
    }
    Ok((modalities, efforts))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    name: String,
    base_url: String,
    api_key: Option<String>,
}

const PROVIDER: &str =
    "SELECT id,name,base_url,coalesce(api_key,'')<>'',archived_at FROM llm_providers";

#[derive(Serialize)]
struct ProviderInfo {
    id: ProviderId,
    name: String,
    base_url: String,
    has_api_key: bool,
    archived_at: Option<i64>,
}

fn public_provider(row: &Row) -> rusqlite::Result<ProviderInfo> {
    Ok(ProviderInfo {
        id: row.get(0)?,
        name: row.get(1)?,
        base_url: row.get(2)?,
        has_api_key: row.get(3)?,
        archived_at: row.get(4)?,
    })
}

async fn get_provider(
    State(app): State<App>,
    Path(id): Path<ProviderId>,
) -> Result<Json<ProviderInfo>> {
    app.db
        .call(move |s| Ok(Json(record(&s.conn, PROVIDER, id, public_provider)?)))
        .await
}

async fn list_providers(
    State(app): State<App>,
    Query(opts): Query<ListOptions>,
) -> Result<Json<Vec<ProviderInfo>>> {
    let filter = if opts.include_archived {
        ""
    } else {
        "WHERE archived_at IS NULL"
    };
    app.db
        .call(move |s| list(&s.conn, PROVIDER, filter, public_provider))
        .await
}

async fn save<T: Send + 'static, K: Prefix>(
    app: App,
    id: Option<Id<K>>,
    fields: Document,
    apply: fn(&Connection, Option<Id<K>>, Document) -> Result<T>,
) -> Result<Json<T>> {
    app.db
        .write(
            move |conn| apply(conn, id, fields),
            |_, value| Ok(Json(value)),
        )
        .await
}

fn save_provider(
    conn: &Connection,
    id: Option<ProviderId>,
    fields: Document,
) -> Result<ProviderInfo> {
    let old = id
        .map(|id| {
            conn.query_row(
                "SELECT name,base_url,api_key FROM llm_providers WHERE id=?",
                [id],
                |r| {
                    Ok(Provider {
                        name: r.get(0)?,
                        base_url: r.get(1)?,
                        api_key: r.get(2)?,
                    })
                },
            )
        })
        .transpose()?;
    let has_name = fields.contains_key("name");
    let has_base_url = fields.contains_key("base_url");
    let has_api_key = fields.contains_key("api_key");
    let provider = document(old.as_ref(), fields)?;
    let url =
        reqwest::Url::parse(&provider.base_url).map_err(|_| Error::bad("invalid base_url"))?;
    if provider.name.trim().is_empty() {
        return Err(Error::bad("expected provider name"));
    }
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::bad(
            "expected HTTP(S) base_url without credentials, query or fragment",
        ));
    }
    let id = match id {
        Some(id) => id,
        None => ProviderId::new()?,
    };
    let clash: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM llm_providers WHERE name=?1 AND archived_at IS NULL AND id!=?2)",
        )?
        .query_row(params![&provider.name, id], |r| r.get(0))?;
    if clash {
        return Err(Error::conflict(
            "name is already in use by an active catalog entry",
        ));
    }
    let ts = crate::cluster::ledger::tick_hlc(conn)?;
    let rev = crate::cluster::ledger::next_catalog_rev();
    let api_key = provider.api_key.filter(|k| !k.is_empty());
    let name_ts = (old.is_none() || has_name).then(|| ts.as_str());
    let base_url_ts = (old.is_none() || has_base_url).then(|| ts.as_str());
    let api_key_ts = (old.is_none() || has_api_key).then(|| ts.as_str());
    let id: ProviderId = conn
        .query_row(
            "INSERT INTO llm_providers(id,name,name_ts,base_url,base_url_ts,api_key,api_key_ts,sync_rev)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(id) DO UPDATE SET
               name=excluded.name,
               name_ts=coalesce(?9, llm_providers.name_ts),
               base_url=excluded.base_url,
               base_url_ts=coalesce(?10, llm_providers.base_url_ts),
               api_key=excluded.api_key,
               api_key_ts=coalesce(?11, llm_providers.api_key_ts),
               sync_rev=excluded.sync_rev
             RETURNING id",
            params![
                id,
                provider.name,
                ts,
                provider.base_url,
                ts,
                api_key,
                ts,
                rev,
                name_ts,
                base_url_ts,
                api_key_ts
            ],
            |r| r.get(0),
        )
        .map_err(catalog_error)?;
    crate::cluster::ledger::stage_catalog();
    record(conn, PROVIDER, id, public_provider)
}

async fn create_provider(
    State(app): State<App>,
    fields: Document,
) -> Result<(StatusCode, Json<ProviderInfo>)> {
    Ok((
        StatusCode::CREATED,
        save(app, None, fields, save_provider).await?,
    ))
}

async fn patch_provider(
    State(app): State<App>,
    Path(id): Path<ProviderId>,
    fields: Document,
) -> Result<Json<ProviderInfo>> {
    save(app, Some(id), fields, save_provider).await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    provider_id: ProviderId,
    model_key: String,
    context_window_tokens: Option<i64>,
    input_modalities: Vec<String>,
    #[serde(default)]
    reasoning_efforts: Vec<String>,
    pricing: Option<Value>,
}

const MODEL: &str = "SELECT id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing,archived_at,provider_archived_at FROM model_catalog";

fn string_array(row: &Row, index: usize) -> rusqlite::Result<Vec<String>> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(e)))
}

impl Model {
    fn read(row: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            provider_id: row.get(1)?,
            model_key: row.get(2)?,
            context_window_tokens: row.get(3)?,
            input_modalities: string_array(row, 4)?,
            reasoning_efforts: string_array(row, 5)?,
            pricing: row.get(6)?,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.model_key.trim().is_empty() {
            return Err(Error::bad("expected model_key"));
        }
        if self.context_window_tokens.is_some_and(|n| n <= 0) {
            return Err(Error::bad(
                "expected positive context_window_tokens or null",
            ));
        }
        let mut seen = HashSet::new();
        if self.input_modalities.is_empty()
            || self.input_modalities.iter().any(|m| {
                !matches!(m.as_str(), "text" | "image" | "audio" | "video") || !seen.insert(m)
            })
        {
            return Err(Error::bad(
                "expected distinct input_modalities from text, image, audio, video",
            ));
        }
        seen.clear();
        if self
            .reasoning_efforts
            .iter()
            .any(|e| e.is_empty() || e.trim() != e || !seen.insert(e))
        {
            return Err(Error::bad("expected distinct nonblank reasoning_efforts"));
        }
        pricing::validate(self.pricing.as_ref().unwrap_or(&Value::Null))
    }

    fn check_update(&self, conn: &Connection, id: ModelId, old: &Self) -> Result<()> {
        if (self.provider_id != old.provider_id || self.model_key != old.model_key)
            && conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM llm_calls WHERE model_id=?)",
                [id],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Err(Error::conflict(
                "provider_id and model_key are fixed after the first call",
            ));
        }
        let fewer_efforts = old
            .reasoning_efforts
            .iter()
            .any(|e| !self.reasoning_efforts.contains(e));
        let fewer_modalities = old
            .input_modalities
            .iter()
            .any(|m| !self.input_modalities.contains(m));
        if !fewer_efforts && !fewer_modalities {
            return Ok(());
        }
        let modalities = json!(self.input_modalities);
        let mut sessions = conn.prepare_cached(
            "SELECT id,reasoning_effort FROM sessions WHERE model_id=? AND deleted=0 ORDER BY id",
        )?;
        let mut rows = sessions.query([id])?;
        while let Some(row) = rows.next()? {
            let sid: crate::id::SessionId = row.get(0)?;
            if fewer_efforts
                && let Some(e) = row.get::<_, Option<String>>(1)?
                && !self.reasoning_efforts.contains(&e)
            {
                return Err(Error::conflict(format!(
                    "reasoning_effort {e} is in use by session {sid}"
                )));
            }
            if fewer_modalities {
                if !crate::cluster::ledger::session_branch_complete(conn, sid)? {
                    return Err(Error::conflict(format!(
                        "session {sid} history is incomplete locally; sync before removing model input modalities"
                    )));
                }
                media::check_session(conn, sid, &modalities)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ModelInfo {
    id: ModelId,
    provider_id: ProviderId,
    model_key: String,
    context_window_tokens: Option<i64>,
    input_modalities: Vec<String>,
    reasoning_efforts: Vec<String>,
    pricing: Option<Value>,
    archived_at: Option<i64>,
    provider_archived_at: Option<i64>,
}

fn public_model(row: &Row) -> rusqlite::Result<ModelInfo> {
    let model = Model::read(row)?;
    Ok(ModelInfo {
        id: row.get(0)?,
        provider_id: model.provider_id,
        model_key: model.model_key,
        context_window_tokens: model.context_window_tokens,
        input_modalities: model.input_modalities,
        reasoning_efforts: model.reasoning_efforts,
        pricing: model.pricing,
        archived_at: row.get(7)?,
        provider_archived_at: row.get(8)?,
    })
}

async fn get_model(State(app): State<App>, Path(id): Path<ModelId>) -> Result<Json<ModelInfo>> {
    app.db
        .call(move |s| Ok(Json(record(&s.conn, MODEL, id, public_model)?)))
        .await
}

async fn list_models(
    State(app): State<App>,
    Query(opts): Query<ListOptions>,
) -> Result<Json<Vec<ModelInfo>>> {
    let filter = if opts.include_archived {
        ""
    } else {
        "WHERE available"
    };
    app.db
        .call(move |s| list(&s.conn, MODEL, filter, public_model))
        .await
}

fn save_model(conn: &Connection, id: Option<ModelId>, fields: Document) -> Result<ModelInfo> {
    let old = id
        .map(|id| record(conn, MODEL, id, Model::read))
        .transpose()?;
    let has_identity = fields.contains_key("provider_id") || fields.contains_key("model_key");
    let has_cw = fields.contains_key("context_window_tokens");
    let has_modalities = fields.contains_key("input_modalities");
    let has_efforts = fields.contains_key("reasoning_efforts");
    let has_pricing = fields.contains_key("pricing");
    let model = document(old.as_ref(), fields)?;
    model.validate()?;
    if old
        .as_ref()
        .is_none_or(|previous| previous.provider_id != model.provider_id)
    {
        let archived: Option<bool> = conn
            .query_row(
                "SELECT archived_at IS NOT NULL FROM llm_providers WHERE id=?",
                [model.provider_id],
                |r| r.get(0),
            )
            .optional()?;
        match archived {
            Some(false) => (),
            Some(true) => return Err(Error::conflict("cannot add models to an archived provider")),
            None => return Err(Error::bad("unknown provider_id")),
        }
    }
    if let (Some(id), Some(old)) = (id, old.as_ref()) {
        model.check_update(conn, id, old)?;
    }
    let id = match id {
        Some(id) => id,
        None => ModelId::new()?,
    };
    let clash: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM llm_models WHERE provider_id=?1 AND model_key=?2 AND archived_at IS NULL AND id!=?3)",
        )?
        .query_row(params![model.provider_id, &model.model_key, id], |r| r.get(0))?;
    if clash {
        return Err(Error::conflict(
            "name is already in use by an active catalog entry",
        ));
    }
    let ts = crate::cluster::ledger::tick_hlc(conn)?;
    let rev = crate::cluster::ledger::next_catalog_rev();
    let is_new = old.is_none();
    let id_ts = (is_new || has_identity).then(|| ts.as_str());
    let cw_ts = (is_new || has_cw).then(|| ts.as_str());
    let mod_ts = (is_new || has_modalities).then(|| ts.as_str());
    let eff_ts = (is_new || has_efforts).then(|| ts.as_str());
    let price_ts = (is_new || has_pricing).then(|| ts.as_str());
    let id: ModelId = conn
        .query_row(
            "INSERT INTO llm_models(
                id,provider_id,model_key,identity_ts,
                context_window_tokens,context_window_ts,
                input_modalities,input_modalities_ts,
                reasoning_efforts,reasoning_efforts_ts,
                pricing,pricing_ts,sync_rev
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
             ON CONFLICT(id) DO UPDATE SET
               provider_id=excluded.provider_id,
               model_key=excluded.model_key,
               identity_ts=coalesce(?14, llm_models.identity_ts),
               context_window_tokens=excluded.context_window_tokens,
               context_window_ts=coalesce(?15, llm_models.context_window_ts),
               input_modalities=excluded.input_modalities,
               input_modalities_ts=coalesce(?16, llm_models.input_modalities_ts),
               reasoning_efforts=excluded.reasoning_efforts,
               reasoning_efforts_ts=coalesce(?17, llm_models.reasoning_efforts_ts),
               pricing=excluded.pricing,
               pricing_ts=coalesce(?18, llm_models.pricing_ts),
               sync_rev=excluded.sync_rev
             RETURNING id",
            params![
                id,
                model.provider_id,
                model.model_key,
                ts,
                model.context_window_tokens,
                ts,
                json!(model.input_modalities),
                ts,
                json!(model.reasoning_efforts),
                ts,
                model.pricing,
                ts,
                rev,
                id_ts,
                cw_ts,
                mod_ts,
                eff_ts,
                price_ts
            ],
            |r| r.get(0),
        )
        .map_err(catalog_error)?;
    crate::cluster::ledger::stage_catalog();
    record(conn, MODEL, id, public_model)
}

async fn create_model(
    State(app): State<App>,
    fields: Document,
) -> Result<(StatusCode, Json<ModelInfo>)> {
    Ok((
        StatusCode::CREATED,
        save(app, None, fields, save_model).await?,
    ))
}

async fn patch_model(
    State(app): State<App>,
    Path(id): Path<ModelId>,
    fields: Document,
) -> Result<Json<ModelInfo>> {
    save(app, Some(id), fields, save_model).await
}
