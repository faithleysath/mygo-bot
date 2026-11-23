use crate::{App, Error, Result, media, pricing};
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

fn record<T>(
    conn: &Connection,
    select: &str,
    id: i64,
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

// The database worker commits this write (including any default change) atomically.
async fn set_archived(app: App, table: &'static str, id: i64, archive: bool) -> Result<StatusCode> {
    app.db
        .write(
            move |conn| {
                let timestamp = archive.then(|| chrono::Utc::now().timestamp_millis());
                let value = if archive {
                    "coalesce(archived_at, ?1)"
                } else {
                    "?1"
                };
                let sql = format!("UPDATE {table} SET archived_at={value} WHERE id=?2");
                if conn.execute(&sql, (timestamp, id)).map_err(catalog_error)? == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                if archive {
                    conn.execute(
                        "UPDATE config SET default_model_id=NULL, default_reasoning_effort=NULL
                         WHERE EXISTS(SELECT 1 FROM model_catalog
                                      WHERE id=config.default_model_id AND NOT available)",
                        [],
                    )?;
                }
                Ok(())
            },
            |_, ()| Ok(StatusCode::NO_CONTENT),
        )
        .await
}

async fn archive_provider(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    set_archived(app, "llm_providers", id, true).await
}

async fn restore_provider(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    set_archived(app, "llm_providers", id, false).await
}

async fn archive_model(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    set_archived(app, "llm_models", id, true).await
}

async fn restore_model(State(app): State<App>, Path(id): Path<i64>) -> Result<StatusCode> {
    set_archived(app, "llm_models", id, false).await
}

pub fn available_capabilities(conn: &Connection, id: i64) -> Result<(Value, Value)> {
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
    id: i64,
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

async fn get_provider(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<ProviderInfo>> {
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

async fn save<T: Send + 'static>(
    app: App,
    id: Option<i64>,
    fields: Document,
    apply: fn(&Connection, Option<i64>, Document) -> Result<T>,
) -> Result<Json<T>> {
    app.db
        .write(
            move |conn| apply(conn, id, fields),
            |_, value| Ok(Json(value)),
        )
        .await
}

fn save_provider(conn: &Connection, id: Option<i64>, fields: Document) -> Result<ProviderInfo> {
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
    let id = conn.query_row(
        "INSERT INTO llm_providers(id,name,base_url,api_key) VALUES(?,?,?,?)
         ON CONFLICT(id) DO UPDATE SET name=excluded.name,base_url=excluded.base_url,api_key=excluded.api_key
         RETURNING id",
        params![id, provider.name, provider.base_url, provider.api_key.filter(|k| !k.is_empty())],
        |r| r.get(0),
    ).map_err(catalog_error)?;
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
    Path(id): Path<i64>,
    fields: Document,
) -> Result<Json<ProviderInfo>> {
    save(app, Some(id), fields, save_provider).await
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    provider_id: i64,
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

    fn check_update(&self, conn: &Connection, id: i64, old: &Self) -> Result<()> {
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
        if fewer_efforts {
            let default: Option<String> = conn
                .query_row(
                    "SELECT default_reasoning_effort FROM config WHERE default_model_id=?",
                    [id],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            if let Some(e) = default
                && !self.reasoning_efforts.contains(&e)
            {
                return Err(Error::conflict(format!(
                    "default_reasoning_effort {e} is in use"
                )));
            }
        }
        let modalities = json!(self.input_modalities);
        let mut sessions = conn.prepare_cached(
            "SELECT id,reasoning_effort FROM sessions WHERE model_id=? ORDER BY id",
        )?;
        let mut rows = sessions.query([id])?;
        while let Some(row) = rows.next()? {
            let sid: i64 = row.get(0)?;
            if fewer_efforts
                && let Some(e) = row.get::<_, Option<String>>(1)?
                && !self.reasoning_efforts.contains(&e)
            {
                return Err(Error::conflict(format!(
                    "reasoning_effort {e} is in use by session {sid}"
                )));
            }
            if fewer_modalities {
                media::check_session(conn, sid, &modalities)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ModelInfo {
    id: i64,
    provider_id: i64,
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

async fn get_model(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<ModelInfo>> {
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

fn save_model(conn: &Connection, id: Option<i64>, fields: Document) -> Result<ModelInfo> {
    let old = id
        .map(|id| record(conn, MODEL, id, Model::read))
        .transpose()?;
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
    if let (Some(id), Some(old)) = (id, old) {
        model.check_update(conn, id, &old)?;
    }
    let id = conn.query_row(
        "INSERT INTO llm_models(id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing)
         VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET provider_id=excluded.provider_id,
         model_key=excluded.model_key,context_window_tokens=excluded.context_window_tokens,
         input_modalities=excluded.input_modalities,reasoning_efforts=excluded.reasoning_efforts,pricing=excluded.pricing
         RETURNING id",
        params![
            id, model.provider_id, model.model_key, model.context_window_tokens,
            json!(model.input_modalities), json!(model.reasoning_efforts), model.pricing
        ],
        |r| r.get(0),
    ).map_err(catalog_error)?;
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
    Path(id): Path<i64>,
    fields: Document,
) -> Result<Json<ModelInfo>> {
    save(app, Some(id), fields, save_model).await
}
