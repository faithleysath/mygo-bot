use crate::{
    App, Error, Result, agent, events,
    store::{self, session},
};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub async fn authorize_tcp(
    State(token): State<Arc<str>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    let mut headers = request.headers().get_all(header::AUTHORIZATION).iter();
    let authorized = headers
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .is_some_and(|(scheme, credential)| {
            scheme.eq_ignore_ascii_case("Bearer")
                && bool::from(
                    token
                        .as_bytes()
                        .ct_eq(credential.trim_start_matches(' ').as_bytes()),
                )
        });
    if authorized && headers.next().is_none() {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(json!({"error":"unauthorized"})),
        )
            .into_response()
    }
}

pub fn routes(app: App) -> Router {
    Router::new()
        .route("/config", get(get_config).patch(patch_config))
        .route("/sessions", post(create_session).get(list_sessions))
        .route("/sessions/{sid}", get(get_session).patch(patch_session))
        .route("/sessions/{sid}/messages", post(enqueue).get(messages))
        .route("/sessions/{sid}/messages/{mid}", delete(withdraw))
        .route(
            "/sessions/{sid}/run",
            put(agent::start).delete(agent::pause),
        )
        .route("/sessions/{sid}/history", get(history))
        .route("/sessions/{sid}/context", get(context))
        .route("/sessions/{sid}/events", get(events::subscribe))
        .with_state(app)
}

type Document = Json<Map<String, Value>>;
type Data = Result<Json<Value>>;
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub instructions: String,
    pub default_reasoning_effort: Option<String>,
}
impl Config {
    pub fn load(conn: &Connection) -> Result<Option<Self>> {
        let sql = "SELECT base_url,api_key,model,instructions,default_reasoning_effort
                   FROM config WHERE id=1";
        Ok(conn
            .query_row(sql, [], |r| {
                Ok(Self {
                    base_url: r.get(0)?,
                    api_key: r.get(1)?,
                    model: r.get(2)?,
                    instructions: r.get(3)?,
                    default_reasoning_effort: r.get(4)?,
                })
            })
            .optional()?)
    }
    fn public(self) -> Value {
        json!({"configured":true,"base_url":self.base_url,"model":self.model,"instructions":self.instructions,"has_api_key":!self.api_key.is_empty(),"default_reasoning_effort":self.default_reasoning_effort})
    }
}
fn validate_effort(e: Option<&str>) -> Result<()> {
    if matches!(e, None | Some("low" | "high" | "max")) {
        Ok(())
    } else {
        Err(Error::bad("expected low, high, max or null"))
    }
}
async fn get_config(State(app): State<App>) -> Data {
    app.db
        .call(|s| {
            Ok(Json(
                Config::load(&s.conn)?
                    .map(Config::public)
                    .unwrap_or(json!({"configured":false})),
            ))
        })
        .await
}
async fn patch_config(State(app): State<App>, Json(fields): Document) -> Data {
    if fields.is_empty() {
        return Err(Error::bad("expected configuration fields"));
    }
    app.db
        .call(move |s| {
            let mut value = json!(Config::load(&s.conn)?.unwrap_or_default());
            value.as_object_mut().unwrap().extend(fields);
            let cfg: Config =
                serde_json::from_value(value).map_err(|e| Error::bad(e.to_string()))?;
            let url =
                reqwest::Url::parse(&cfg.base_url).map_err(|_| Error::bad("invalid base_url"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || cfg.model.trim().is_empty()
            {
                return Err(Error::bad("expected model name and HTTP(S) base_url"));
            }
            validate_effort(cfg.default_reasoning_effort.as_deref())?;
            s.conn.execute(
                "INSERT OR REPLACE INTO config VALUES(1,?,?,?,?,?)",
                params![
                    cfg.base_url,
                    cfg.api_key,
                    cfg.model,
                    cfg.instructions,
                    cfg.default_reasoning_effort
                ],
            )?;
            Ok(Json(cfg.public()))
        })
        .await
}

fn effort_field(mut fields: Map<String, Value>) -> Result<Option<Option<String>>> {
    let effort = fields
        .remove("reasoning_effort")
        .map(serde_json::from_value::<Option<String>>)
        .transpose()
        .map_err(|e| Error::bad(e.to_string()))?;
    if !fields.is_empty() {
        return Err(Error::bad("unknown session setting"));
    }
    validate_effort(effort.as_ref().and_then(|x| x.as_deref()))?;
    Ok(effort)
}
async fn create_session(
    State(app): State<App>,
    body: Option<Document>,
) -> Result<(StatusCode, Json<Value>)> {
    let effort = effort_field(body.map(|x| x.0).unwrap_or_default())?;
    let created = app
        .db
        .write(
            move |conn| {
                let effort = match effort {
                    Some(e) => e,
                    None => conn
                        .query_row(
                            "SELECT default_reasoning_effort FROM config WHERE id=1",
                            [],
                            |r| r.get(0),
                        )
                        .optional()?
                        .flatten(),
                };
                conn.prepare_cached("INSERT INTO sessions(reasoning_effort) VALUES(?)")?
                    .execute([effort])?;
                session(conn, conn.last_insert_rowid())
            },
            |_, created| Ok(created),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(created)))
}
async fn get_session(State(app): State<App>, Path(sid): Path<i64>) -> Data {
    app.db.call(move |s| Ok(Json(session(&s.conn, sid)?))).await
}
async fn patch_session(State(app): State<App>, Path(sid): Path<i64>, Json(body): Document) -> Data {
    let effort = effort_field(body)?.ok_or_else(|| Error::bad("expected reasoning_effort"))?;
    app.db
        .call(move |s| {
            s.conn.execute(
                "UPDATE sessions SET reasoning_effort=? WHERE id=?",
                (effort, sid),
            )?;
            Ok(Json(session(&s.conn, sid)?))
        })
        .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    #[serde(default)]
    after: i64,
    #[serde(default = "default_limit")]
    limit: i64,
}
fn default_limit() -> i64 {
    50
}
impl Page {
    fn check(&self) -> Result<()> {
        if self.after < 0 || !(1..=200).contains(&self.limit) {
            Err(Error::bad("expected after >= 0 and limit 1..200"))
        } else {
            Ok(())
        }
    }
}
async fn list_sessions(State(app): State<App>, Query(page): Query<Page>) -> Data {
    page.check()?;
    app.db
        .call(move |s| {
            let sql = "SELECT id,status,reasoning_effort,last_error FROM sessions
                   WHERE id>? ORDER BY id LIMIT ?";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let rows = stmt
                .query_map((page.after, page.limit), store::session_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(Value::Array(rows)))
        })
        .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    text: String,
}
async fn enqueue(
    State(app): State<App>,
    Path(sid): Path<i64>,
    headers: HeaderMap,
    Json(message): Json<Message>,
) -> Result<(StatusCode, Json<Value>)> {
    if message.text.trim().is_empty() {
        return Err(Error::bad("text is required"));
    }
    let mut keys = headers.get_all("idempotency-key").iter();
    let key = keys
        .next()
        .map(|v| {
            v.to_str()
                .map(str::trim)
                .map(String::from)
                .map_err(|_| Error::bad("invalid idempotency key"))
        })
        .transpose()?;
    if keys.next().is_some() || key.as_ref().is_some_and(|k| k.is_empty() || k.len() > 256) {
        return Err(Error::bad("invalid idempotency key"));
    }
    let (status, id) = match app.db.enqueue(sid, message.text, key).await? {
        store::Enqueued::Created(id) => (StatusCode::CREATED, id),
        store::Enqueued::Existing(id) => (StatusCode::OK, id),
    };
    Ok((status, Json(json!({"id": id}))))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Filter {
    id: Option<i64>,
    status: Option<String>,
    #[serde(default)]
    after: i64,
    #[serde(default = "default_limit")]
    limit: i64,
}
async fn messages(State(app): State<App>, Path(sid): Path<i64>, Query(q): Query<Filter>) -> Data {
    Page {
        after: q.after,
        limit: q.limit,
    }
    .check()?;
    if !matches!(
        q.status.as_deref(),
        None | Some("queued" | "included" | "withdrawn")
    ) {
        return Err(Error::bad("unknown message status"));
    }
    app.db
        .call(move |s| {
            session(&s.conn, sid)?;
            let sql = "SELECT i.id, json_extract(i.payload,'$.content'),
                     CASE WHEN w.id IS NOT NULL THEN 'withdrawn'
                          WHEN c.item_id IS NOT NULL THEN 'included' ELSE 'queued' END AS status
                   FROM items i
                   LEFT JOIN items w ON w.target_id=i.id AND w.kind='withdrawal'
                   LEFT JOIN context_items c ON c.item_id=i.id
                   WHERE i.session_id=?1 AND i.kind='user_message'
                     AND i.id BETWEEN ?2 AND ?3 AND i.id>?4
                     AND (?5 IS NULL OR status=?5)
                   ORDER BY i.id LIMIT ?6";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let args = params![
                sid,
                q.id.unwrap_or(1),
                q.id.unwrap_or(i64::MAX),
                q.after,
                q.status,
                q.limit
            ];
            let data = stmt
                .query_map(args, |r| {
                    Ok(json!({
                        "id": r.get::<_, i64>(0)?,
                        "text": r.get::<_, String>(1)?,
                        "status": r.get::<_, String>(2)?
                    }))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(Value::Array(data)))
        })
        .await
}
async fn withdraw(
    State(app): State<App>,
    Path((sid, mid)): Path<(i64, i64)>,
) -> Result<StatusCode> {
    app.db
        .call(move |s| {
            let sql = "SELECT EXISTS(SELECT 1 FROM items WHERE kind='withdrawal' AND target_id=?2),
                          EXISTS(SELECT 1 FROM context_items WHERE item_id=?2)
                   FROM items WHERE session_id=?1 AND id=?2 AND kind='user_message'";
            let (withdrawn, included) = s.conn.query_row(sql, (sid, mid), |r| {
                Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?))
            })?;
            if included {
                return Err(Error::conflict("message already included"));
            }
            if !withdrawn {
                let id = store::add_item(&s.conn, sid, "withdrawal", &json!({}), Some(mid))?;
                s.item_saved(sid, id)?;
            }
            Ok(StatusCode::NO_CONTENT)
        })
        .await
}
async fn record_page(app: App, sid: i64, page: Page, rows: store::JsonRows) -> Result<Response> {
    page.check()?;
    let end = app
        .db
        .call(move |s| {
            session(&s.conn, sid)?;
            rows.page_end(&s.conn, sid, page.after, page.limit)
        })
        .await?;
    let body = rows
        .body(app.db, sid, page.after, end, vec![b'['], b"]")
        .await?;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        axum::body::Body::new(body),
    )
        .into_response())
}

async fn history(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(page): Query<Page>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::History).await
}

async fn context(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(page): Query<Page>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::Context).await
}
