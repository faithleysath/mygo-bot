use crate::{
    App, Error, ErrorBody, Result, agent, codemode, events,
    events::Notice,
    store::{self, Session, SessionStatus, session},
};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use rusqlite::{Connection, params};
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
            Json(ErrorBody {
                error: "unauthorized".into(),
            }),
        )
            .into_response()
    }
}

pub fn routes(app: App) -> Router {
    Router::new()
        .route("/config", get(get_config).patch(patch_config))
        .route("/events", get(events::subscribe_global))
        .route("/sessions", get(list_sessions).post(create_session))
        .route(
            "/sessions/{sid}",
            get(get_session).patch(patch_session).delete(delete_session),
        )
        .route("/sessions/{sid}/messages", post(enqueue).get(messages))
        .route("/sessions/{sid}/messages/{mid}", delete(withdraw))
        .route("/sessions/{sid}/run", put(agent::start).delete(agent::pause))
        .route("/sessions/{sid}/history", get(history))
        .route("/sessions/{sid}/context", get(context))
        .route("/sessions/{sid}/events", get(events::subscribe))
        .route("/sessions/{sid}/tasks", get(list_tasks))
        .route("/sessions/{sid}/tasks/{tid}", delete(kill_task))
        .merge(crate::catalog::routes())
        .merge(crate::mcp::routes())
        .merge(crate::mcp::diagnostics::routes())
        .merge(crate::media::routes())
        .merge(crate::calls::routes())
        .with_state(app)
}

pub fn internal_routes(app: App) -> Router {
    routes(app).merge(crate::docs::openapi_route())
}

type Document = Json<Map<String, Value>>;
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    default_model_id: Option<i64>,
    default_reasoning_effort: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    model_id: Option<i64>,
    reasoning_effort: Option<String>,
}

impl Selection {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            model_id: row.get(0)?,
            reasoning_effort: row.get(1)?,
        })
    }

    fn defaults(conn: &Connection) -> Result<Self> {
        Ok(conn.query_row(
            "SELECT default_model_id,default_reasoning_effort FROM config WHERE id=1",
            [],
            Self::read,
        )?)
    }

    fn apply(self, fields: Map<String, Value>) -> Result<Self> {
        let mut data = json!(self);
        if fields.get("model_id").is_some_and(Value::is_null) {
            data["reasoning_effort"] = Value::Null;
        }
        data.as_object_mut().unwrap().extend(fields);
        serde_json::from_value(data).map_err(|e| Error::bad(e.to_string()))
    }

    fn validate(&self, conn: &Connection) -> Result<Option<Value>> {
        let Some(id) = self.model_id else {
            if self.reasoning_effort.is_some() {
                return Err(Error::bad(
                    "select a model before specifying reasoning effort",
                ));
            }
            return Ok(None);
        };
        let (modalities, efforts) = crate::catalog::available_capabilities(conn, id)?;
        if let Some(effort) = &self.reasoning_effort
            && !efforts
                .as_array()
                .is_some_and(|opts| opts.iter().any(|v| v.as_str() == Some(effort)))
        {
            return Err(Error::bad(format!(
                "model {id} does not support effort {effort}; available: {efforts}"
            )));
        }
        Ok(Some(modalities))
    }

    fn config(self) -> Config {
        Config {
            default_model_id: self.model_id,
            default_reasoning_effort: self.reasoning_effort,
        }
    }
}

async fn get_config(State(app): State<App>) -> Result<Json<Config>> {
    app.db
        .call(|s| Ok(Json(Selection::defaults(&s.conn)?.config())))
        .await
}

async fn patch_config(State(app): State<App>, Json(fields): Document) -> Result<Json<Config>> {
    let mut selection = Map::new();
    for (key, value) in fields {
        let field = match key.as_str() {
            "default_model_id" => "model_id",
            "default_reasoning_effort" => "reasoning_effort",
            _ => return Err(Error::bad("unknown configuration field")),
        };
        selection.insert(field.into(), value);
    }
    if selection.is_empty() {
        return Err(Error::bad("expected configuration fields"));
    }
    app.db
        .call(move |s| {
            let next = Selection::defaults(&s.conn)?.apply(selection)?;
            next.validate(&s.conn)?;
            s.conn.execute(
                "UPDATE config SET default_model_id=?, default_reasoning_effort=? WHERE id=1",
                params![next.model_id, next.reasoning_effort],
            )?;
            Ok(Json(next.config()))
        })
        .await
}

fn extract_title(fields: &mut Map<String, Value>) -> Result<Option<String>> {
    let Some(value) = fields.remove("title") else {
        return Ok(None);
    };
    let title = value
        .as_str()
        .ok_or_else(|| Error::bad("title must be a string"))?;
    if title.len() > 256 || title.contains('\0') {
        return Err(Error::bad("title must be at most 256 bytes without NUL"));
    }
    Ok(Some(title.to_owned()))
}

async fn create_session(
    State(app): State<App>,
    body: Option<Document>,
) -> Result<(StatusCode, Json<Session>)> {
    let mut fields = body.map(|x| x.0).unwrap_or_default();
    let title = extract_title(&mut fields)?.unwrap_or_default();
    let created = app
        .db
        .write(
            move |conn| {
                let next = Selection::defaults(conn)?.apply(fields)?;
                next.validate(conn)?;
                let now = chrono::Utc::now().timestamp_millis();
                conn.execute(
                    "INSERT INTO sessions(title,created_at,updated_at,model_id,reasoning_effort) VALUES(?,?,?,?,?)",
                    params![title, now, now, next.model_id, next.reasoning_effort],
                )?;
                session(conn, conn.last_insert_rowid())
            },
            |s, created| {
                s.publish_global("session.created", json!(created));
                Ok(created)
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_session(State(app): State<App>, Path(sid): Path<i64>) -> Result<Json<Session>> {
    app.db.call(move |s| Ok(Json(session(&s.conn, sid)?))).await
}

async fn patch_session(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Json(mut fields): Document,
) -> Result<Json<Session>> {
    if fields.is_empty() {
        return Err(Error::bad("expected session settings"));
    }
    let title = extract_title(&mut fields)?;
    app.db
        .write(
            move |conn| {
                let current = conn.query_row(
                    "SELECT model_id,reasoning_effort FROM sessions WHERE id=?",
                    [sid],
                    Selection::read,
                )?;
                let old_id = current.model_id;
                let next = current.apply(fields)?;
                let modalities = next.validate(conn)?;
                if next.model_id != old_id {
                    if let Some(modalities) = modalities {
                        crate::media::check_session(conn, sid, &modalities)?;
                    } else {
                        let has_context: bool = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM context_items WHERE session_id=?1
                     UNION ALL SELECT 1 FROM pending WHERE session_id=?1
                     UNION ALL SELECT 1 FROM items WHERE session_id=?1 AND kind='function_call')",
                            [sid],
                            |r| r.get(0),
                        )?;
                        if has_context {
                            return Err(Error::conflict("only an empty session may have no model"));
                        }
                    }
                }
                let now = chrono::Utc::now().timestamp_millis();
                conn.execute(
                    "UPDATE sessions SET model_id=?,reasoning_effort=?,title=coalesce(?,title),updated_at=? WHERE id=?",
                    params![next.model_id, next.reasoning_effort, title, now, sid],
                )?;
                session(conn, sid)
            },
            move |s, updated| {
                let data = json!(updated);
                if let Some(channel) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                    Notice::publish(&channel, "session.updated", data.clone());
                }
                s.publish_global("session.updated", data);
                Ok(Json(updated))
            },
        )
        .await
}

async fn delete_session(State(app): State<App>, Path(sid): Path<i64>) -> Result<StatusCode> {
    app.db
        .call(move |s| {
            let current = session(&s.conn, sid)?;
            if s.active.contains_key(&sid)
                || codemode::has_session_tasks(s, sid)
                || matches!(current.status, SessionStatus::Running | SessionStatus::Pausing)
            {
                return Err(Error::conflict(
                    "pause the session and stop active tasks before deleting",
                ));
            }
            let tx = s.conn.transaction()?;
            tx.execute("DELETE FROM context_items WHERE session_id=?", [sid])?;
            tx.execute(
                "DELETE FROM invalidated_warnings WHERE item_id IN (SELECT id FROM items WHERE session_id=?)",
                [sid],
            )?;
            tx.execute(
                "DELETE FROM items WHERE session_id=? AND (target_id IS NOT NULL OR codemode_call_item_id IS NOT NULL)",
                [sid],
            )?;
            tx.execute("DELETE FROM items WHERE session_id=?", [sid])?;
            tx.execute("UPDATE llm_calls SET session_id=NULL WHERE session_id=?", [sid])?;
            tx.execute("DELETE FROM sessions WHERE id=?", [sid])?;
            tx.commit()?;
            s.mcp.forget_session(sid);
            s.channels.remove(&sid);
            s.wake_sequences.remove(&sid);
            s.publish_global("session.deleted", json!({"id": sid}));
            Ok(())
        })
        .await?;
    app.streams.remove_session(sid);
    Ok(StatusCode::NO_CONTENT)
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

async fn list_sessions(
    State(app): State<App>,
    Query(page): Query<Page>,
) -> Result<Json<Vec<Session>>> {
    page.check()?;
    app.db
        .call(move |s| {
            let sql = "SELECT id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id FROM sessions
                   WHERE id>? ORDER BY id LIMIT ?";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let rows = stmt
                .query_map((page.after, page.limit), store::session_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(rows))
        })
        .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    content: Value,
}
#[derive(Serialize)]
struct MessageId {
    id: i64,
}

#[derive(Serialize)]
struct QueuedMessage {
    id: i64,
    content: Value,
    status: String,
}

async fn enqueue(
    State(app): State<App>,
    Path(sid): Path<i64>,
    headers: HeaderMap,
    Json(message): Json<Message>,
) -> Result<(StatusCode, Json<MessageId>)> {
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
    let (status, id) = match app.db.enqueue(sid, message.content, key).await? {
        store::Enqueued::Created(id) => (StatusCode::CREATED, id),
        store::Enqueued::Existing(id) => (StatusCode::OK, id),
    };
    Ok((status, Json(MessageId { id })))
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

async fn messages(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(q): Query<Filter>,
) -> Result<Json<Vec<QueuedMessage>>> {
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
                    Ok(QueuedMessage {
                        id: r.get(0)?,
                        content: r.get(1)?,
                        status: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(data))
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
                let id = store::add_item(&s.conn, sid, "withdrawal", &json!({}), Some(mid), None)?;
                s.item_saved(sid, id)?;
            }
            Ok(StatusCode::NO_CONTENT)
        })
        .await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordPage {
    after: Option<i64>,
    before: Option<i64>,
    #[serde(default)]
    tail: bool,
    #[serde(default = "default_limit")]
    limit: i64,
}

impl RecordPage {
    fn check(&self) -> Result<()> {
        let modes = usize::from(self.after.is_some())
            + usize::from(self.before.is_some())
            + usize::from(self.tail);
        if modes > 1 {
            return Err(Error::bad("specify at most one of after, before, or tail"));
        }
        if self.after.is_some_and(|v| v < 0)
            || self.before.is_some_and(|v| v <= 0)
            || !(1..=200).contains(&self.limit)
        {
            return Err(Error::bad(
                "expected after >= 0, before > 0, and limit 1..200",
            ));
        }
        Ok(())
    }
}

async fn record_page(
    app: App,
    sid: i64,
    page: RecordPage,
    rows: store::JsonRows,
) -> Result<Response> {
    page.check()?;
    let (start, end) = app
        .db
        .call(move |s| {
            session(&s.conn, sid)?;
            rows.page_range(&s.conn, sid, page.after, page.before, page.tail, page.limit)
        })
        .await?;
    let body = rows
        .body(app.db, sid, start, end, vec![b'['], b"]")
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
    Query(page): Query<RecordPage>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::History).await
}

async fn context(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(page): Query<RecordPage>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::Context).await
}

async fn list_tasks(State(app): State<App>, Path(sid): Path<i64>) -> Result<Json<Value>> {
    app.db
        .call(move |s| {
            session(&s.conn, sid)?;
            Ok(Json(codemode::session_tasks_snapshot(s, sid)))
        })
        .await
}

async fn kill_task(
    State(app): State<App>,
    Path((sid, tid)): Path<(i64, String)>,
) -> Result<Json<Value>> {
    let id = crate::tools::task_id(&tid).map_err(Error::bad)?;
    app.db
        .call(move |s| {
            session(&s.conn, sid)?;
            codemode::stop_task(s, sid, id)
                .map(Json)
                .ok_or_else(|| Error(StatusCode::NOT_FOUND, "task is not active".into()))
        })
        .await
}
