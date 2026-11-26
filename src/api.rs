use crate::{
    App, Error, ErrorBody, Result, agent,
    cluster::exec::{DeleteGateGuard, WriterTarget},
    codemode, events,
    events::Notice,
    id::{ItemId, ModelId, SessionId},
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
        .merge(crate::cluster::api::routes())
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    model_id: Option<ModelId>,
    reasoning_effort: Option<String>,
}

impl Selection {
    fn apply(self, fields: Map<String, Value>) -> Result<Self> {
        if fields.get("model_id").is_some_and(Value::is_null) {
            return Err(Error::bad("model_id cannot be null"));
        }
        let mut data = json!(self);
        data.as_object_mut().unwrap().extend(fields);
        serde_json::from_value(data).map_err(|e| Error::bad(e.to_string()))
    }

    fn validate(&self, conn: &Connection) -> Result<Value> {
        let Some(id) = self.model_id else {
            return Err(Error::bad("model_id is required"));
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
        Ok(modalities)
    }
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
                let next = Selection {
                    model_id: None,
                    reasoning_effort: None,
                }
                .apply(fields)?;
                next.validate(conn)?;
                let model_id = next
                    .model_id
                    .ok_or_else(|| Error::bad("model_id is required"))?;
                let id = SessionId::new()?;
                let now = chrono::Utc::now().timestamp_millis();
                let ts = crate::cluster::ledger::tick_hlc(conn)?;
                let rev = crate::cluster::ledger::next_catalog_rev();
                conn.execute(
                    "INSERT INTO sessions(id,title,title_ts,created_at,updated_at,status_ts,model_id,reasoning_effort,selection_ts,sync_rev)
                     VALUES(?,?,?,?,?,?,?,?,?,?)",
                    params![id, title, ts, now, now, ts, model_id, next.reasoning_effort, ts, rev],
                )?;
                crate::cluster::ledger::mark_session_watched(conn, id)?;
                crate::cluster::ledger::stage_catalog();
                session(conn, id)
            },
            |s, created| {
                let view = s.session_view(created);
                if s.global_events.receiver_count() > 0 {
                    Notice::publish_serializable(&s.global_events, "session.created", &view);
                }
                Ok(view)
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_session(State(app): State<App>, Path(sid): Path<SessionId>) -> Result<Json<Session>> {
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    app.db
        .call(move |s| Ok(Json(s.session_view(session(&s.conn, sid)?))))
        .await
}

async fn patch_session(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    Json(fields): Document,
) -> Result<Json<Session>> {
    if fields.is_empty() {
        return Err(Error::bad("expected session settings"));
    }
    match app.exec.resolve_session_writer(sid).await? {
        WriterTarget::Remote(peer_id) => app
            .exec
            .patch_session_remote(&peer_id, sid, fields)
            .await
            .map(Json),
        WriterTarget::Local => patch_session_local(&app, sid, fields).await.map(Json),
    }
}

pub(crate) async fn patch_session_local(
    app: &App,
    sid: SessionId,
    mut fields: Map<String, Value>,
) -> Result<Session> {
    if fields.is_empty() {
        return Err(Error::bad("expected session settings"));
    }
    let has_selection =
        fields.contains_key("model_id") || fields.contains_key("reasoning_effort");
    let title = extract_title(&mut fields)?;
    let active_head_req: Option<crate::id::ContextNodeId> = match fields.remove("active_head") {
        None => None,
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| Error::bad("active_head must be a ctx_ ID string"))?;
            Some(
                s.parse()
                    .map_err(|e: String| Error::bad(format!("invalid active_head: {e}")))?,
            )
        }
    };
    app.db
        .write(
            move |conn| {
                let cur_sess = session(conn, sid)?;
                if active_head_req.is_some()
                    && matches!(
                        cur_sess.status,
                        SessionStatus::Running | SessionStatus::Pausing
                    )
                {
                    return Err(Error::conflict(
                        "pause the session before switching active branch",
                    ));
                }
                let mut switched_branch = false;
                if let Some(target_head) = active_head_req {
                    if !crate::cluster::ledger::node_branch_complete(conn, sid, target_head)? {
                        return Err(Error::conflict(
                            "target branch head does not exist in this session or is incomplete",
                        ));
                    }
                    if cur_sess.active_head != Some(target_head) {
                        let ts = crate::cluster::ledger::tick_hlc(conn)?;
                        conn.execute(
                            "UPDATE sessions SET active_head=?2, active_head_ts=?3 WHERE id=?1",
                            params![sid, target_head, ts],
                        )?;
                        switched_branch = matches!(
                            crate::cluster::ledger::reconcile_active_context(conn, sid)?,
                            crate::cluster::ledger::ActiveContextChange::Switched
                                | crate::cluster::ledger::ActiveContextChange::Cleared
                        );
                    }
                }
                let current = Selection {
                    model_id: Some(cur_sess.model_id),
                    reasoning_effort: cur_sess.reasoning_effort,
                };
                let old_id = current.model_id;
                let next = current.apply(fields)?;
                let modalities = next.validate(conn)?;
                if next.model_id != old_id || switched_branch {
                    crate::media::check_session(conn, sid, &modalities)?;
                }
                let model_id = next
                    .model_id
                    .ok_or_else(|| Error::bad("model_id is required"))?;
                let now = chrono::Utc::now().timestamp_millis();
                let ts = crate::cluster::ledger::tick_hlc(conn)?;
                let rev = crate::cluster::ledger::next_catalog_rev();
                let title_ts = title.as_ref().map(|_| ts.as_str());
                let sel_ts = has_selection.then(|| ts.as_str());
                conn.execute(
                    "UPDATE sessions
                     SET model_id=?2,
                         reasoning_effort=?3,
                         selection_ts=coalesce(?4, selection_ts),
                         title=coalesce(?5, title),
                         title_ts=coalesce(?6, title_ts),
                         updated_at=max(updated_at, ?7),
                         sync_rev=?8
                     WHERE id=?1 AND deleted=0",
                    params![
                        sid,
                        model_id,
                        next.reasoning_effort,
                        sel_ts,
                        title,
                        title_ts,
                        now,
                        rev
                    ],
                )?;
                crate::cluster::ledger::stage_catalog();
                Ok((session(conn, sid)?, switched_branch))
            },
            move |s, (updated, switched_branch)| {
                if switched_branch {
                    s.bump_reset_gen(sid);
                    if let Some(channel) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                        Notice::publish(&channel, "session.reset", json!({}));
                    }
                }
                let view = s.session_view(updated);
                let data = json!(view);
                if let Some(channel) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                    Notice::publish(&channel, "session.updated", data.clone());
                }
                s.publish_global("session.updated", data);
                Ok(view)
            },
        )
        .await
}

async fn delete_session(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
) -> Result<StatusCode> {
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    let delete_gate = app.exec.acquire_delete_gate(sid).await?;
    // The SQLite job can outlive its HTTP caller. Keep its grant through commit or rollback.
    app.workers
        .clone()
        .spawn(finish_delete(app, sid, delete_gate))
        .await
        .map_err(|_| Error::internal("session delete interrupted; check session state"))?
}

async fn finish_delete(app: App, sid: SessionId, delete_gate: DeleteGateGuard) -> Result<StatusCode> {
    let delete_lease = delete_gate.lease();
    let res = app
        .db
        .call(move |s| {
            let current = session(&s.conn, sid)?;
            if s.active.contains_key(&sid)
                || s.pending_wakes.contains(&sid)
                || codemode::has_session_tasks(s, sid)
                || matches!(current.status, SessionStatus::Running | SessionStatus::Pausing)
                || s.presence.remote_owner(sid).is_some()
                || s.presence.has_committed_remote_lease(sid)
            {
                return Err(Error::conflict(
                    "pause the session and stop active tasks before deleting",
                ));
            }
            let tx = s.conn.transaction()?;
            s.presence.check_delete_lease(sid, delete_lease.as_deref())?;
            if s.presence.remote_owner(sid).is_some() || s.presence.has_committed_remote_lease(sid)
            {
                return Err(Error::conflict(
                    "pause the session and stop active tasks before deleting",
                ));
            }
            let now = chrono::Utc::now().timestamp_millis();
            crate::cluster::ledger::tombstone_session(&tx, sid, now)?;
            s.presence.check_delete_lease(sid, delete_lease.as_deref())?;
            tx.commit()?;
            s.mcp.forget_session(sid);
            s.channels.remove(&sid);
            s.wake_sequences.remove(&sid);
            s.bump_run_gen(sid);
            s.bump_reset_gen(sid);
            s.publish_global("session.deleted", json!({"id": sid}));
            Ok(())
        })
        .await;
    delete_gate.release_now().await;
    res?;
    app.streams.remove_session(sid);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    after: Option<SessionId>,
    #[serde(default = "default_limit")]
    limit: i64,
}
fn default_limit() -> i64 {
    50
}
impl Page {
    fn check(&self) -> Result<()> {
        if !(1..=200).contains(&self.limit) {
            Err(Error::bad("expected limit 1..200"))
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
            let sql = "SELECT id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id,active_head
                       FROM sessions WHERE deleted=0 AND id>? ORDER BY id LIMIT ?";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let after = page.after.as_ref().map_or("", SessionId::as_str);
            let rows = stmt
                .query_map((after, page.limit), store::session_row)?
                .map(|r| r.map(|row| s.session_view(row)))
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
    id: ItemId,
}

#[derive(Serialize)]
struct QueuedMessage {
    id: ItemId,
    content: Value,
    status: String,
}

async fn enqueue(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
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
    let (status, id) = if app.exec.presence().known_peer_count() == 0 {
        match app.db.enqueue(sid, message.content, key).await? {
            store::Enqueued::Created(id) => (StatusCode::CREATED, id),
            store::Enqueued::Existing(id) => (StatusCode::OK, id),
        }
    } else {
        match app.exec.resolve_session_writer(sid).await? {
            WriterTarget::Remote(peer_id) => {
                match app
                    .exec
                    .enqueue_remote(&peer_id, sid, message.content, key)
                    .await?
                {
                    (true, id) => (StatusCode::CREATED, id),
                    (false, id) => (StatusCode::OK, id),
                }
            }
            WriterTarget::Local => match app.db.enqueue(sid, message.content, key).await? {
                store::Enqueued::Created(id) => (StatusCode::CREATED, id),
                store::Enqueued::Existing(id) => (StatusCode::OK, id),
            },
        }
    };
    Ok((status, Json(MessageId { id })))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Filter {
    id: Option<ItemId>,
    status: Option<String>,
    after: Option<ItemId>,
    #[serde(default = "default_limit")]
    limit: i64,
}

macro_rules! messages_sql {
    ($filter:literal) => {
        concat!(
            "SELECT i.id, json_extract(i.payload,'$.content'),
                    CASE WHEN EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=i.session_id AND c.item_id=i.id) THEN 'included'
                         WHEN EXISTS(SELECT 1 FROM items w WHERE w.session_id=i.session_id AND w.kind='withdrawal' AND w.target_id=i.id) THEN 'withdrawn'
                         ELSE 'queued' END AS status
             FROM items i ",
            $filter
        )
    };
}

fn query_messages(
    conn: &Connection,
    sql: &str,
    args: &[&dyn rusqlite::ToSql],
) -> rusqlite::Result<Vec<QueuedMessage>> {
    let mut stmt = conn.prepare_cached(sql)?;
    stmt.query_map(args, |r| {
        Ok(QueuedMessage {
            id: r.get(0)?,
            content: r.get(1)?,
            status: r.get(2)?,
        })
    })?
    .collect()
}

async fn messages(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    Query(q): Query<Filter>,
) -> Result<Json<Vec<QueuedMessage>>> {
    Page {
        after: None,
        limit: q.limit,
    }
    .check()?;
    if !matches!(
        q.status.as_deref(),
        None | Some("queued" | "included" | "withdrawn")
    ) {
        return Err(Error::bad("unknown message status"));
    }
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    app.db
        .call(move |s| {
            session(&s.conn, sid)?;
            let after = q.after.as_ref().map_or("", ItemId::as_str);
            let (sql, args): (&str, &[&dyn rusqlite::ToSql]) = if let Some(ref id) = q.id {
                (
                    messages_sql!(
                        "WHERE i.id=?1 AND i.session_id=?2 AND i.kind='user_message'
                           AND i.id>?3 AND (?4 IS NULL OR status=?4)"
                    ),
                    &[id, &sid, &after, &q.status],
                )
            } else {
                (
                    messages_sql!(
                        "WHERE i.session_id=?1 AND i.kind='user_message'
                           AND i.id>?2 AND (?3 IS NULL OR status=?3)
                         ORDER BY i.id LIMIT ?4"
                    ),
                    &[&sid, &after, &q.status, &q.limit],
                )
            };
            let data = query_messages(&s.conn, sql, args)?;
            Ok(Json(data))
        })
        .await
}
async fn withdraw(
    State(app): State<App>,
    Path((sid, mid)): Path<(SessionId, ItemId)>,
) -> Result<StatusCode> {
    match app.exec.resolve_session_writer(sid).await? {
        WriterTarget::Remote(peer_id) => app.exec.withdraw_remote(&peer_id, sid, mid).await?,
        WriterTarget::Local => withdraw_local(&app, sid, mid).await?,
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn withdraw_local(app: &App, sid: SessionId, mid: ItemId) -> Result<()> {
    app.db
        .write(
            move |conn| {
                let sql = "SELECT EXISTS(SELECT 1 FROM items WHERE session_id=?1 AND kind='withdrawal' AND target_id=?2),
                              EXISTS(SELECT 1 FROM context_nodes WHERE session_id=?1 AND item_id=?2)
                       FROM items WHERE session_id=?1 AND id=?2 AND kind='user_message'";
                let (withdrawn, included) = conn.query_row(sql, (sid, mid), |r| {
                    Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?))
                })?;
                if included {
                    return Err(Error::conflict("message already included"));
                }
                if !withdrawn {
                    let id = store::add_item(conn, sid, "withdrawal", &json!({}), Some(mid), None)?;
                    Ok(Some(id))
                } else {
                    Ok(None)
                }
            },
            move |s, created| {
                if let Some(id) = created {
                    s.item_saved(sid, id)?;
                }
                Ok(())
            },
        )
        .await
}

fn check_page_mode(has_after: bool, has_before: bool, tail: bool, limit: i64) -> Result<()> {
    let modes = usize::from(has_after) + usize::from(has_before) + usize::from(tail);
    if modes > 1 {
        return Err(Error::bad("specify at most one of after, before, or tail"));
    }
    if !(1..=200).contains(&limit) {
        return Err(Error::bad("expected limit 1..200"));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryPage {
    after: Option<ItemId>,
    before: Option<ItemId>,
    #[serde(default)]
    tail: bool,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextPage {
    after: Option<i64>,
    before: Option<i64>,
    #[serde(default)]
    tail: bool,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn json_response(body: reqwest::Body) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        axum::body::Body::new(body),
    )
        .into_response()
}

async fn history(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    Query(page): Query<HistoryPage>,
) -> Result<Response> {
    check_page_mode(
        page.after.is_some(),
        page.before.is_some(),
        page.tail,
        page.limit,
    )?;
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    let range = app
        .db
        .call(move |s| {
            session(&s.conn, sid)?;
            store::JsonRows::history_range(
                &s.conn,
                sid,
                page.after,
                page.before,
                page.tail,
                page.limit,
            )
        })
        .await?;
    let body = store::JsonRows::History
        .history_body(app.db, sid, range)
        .await?;
    Ok(json_response(body))
}

async fn context(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    Query(page): Query<ContextPage>,
) -> Result<Response> {
    check_page_mode(
        page.after.is_some(),
        page.before.is_some(),
        page.tail,
        page.limit,
    )?;
    if page.after.is_some_and(|v| v < 0) || page.before.is_some_and(|v| v <= 0) {
        return Err(Error::bad(
            "expected after >= 0, before > 0, and limit 1..200",
        ));
    }
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    let range = app
        .db
        .call(move |s| {
            session(&s.conn, sid)?;
            store::JsonRows::context_range(
                &s.conn,
                sid,
                page.after,
                page.before,
                page.tail,
                page.limit,
            )
        })
        .await?;
    let body = store::JsonRows::Context
        .context_body(app.db, sid, range, vec![b'['], b"]")
        .await?;
    Ok(json_response(body))
}

async fn list_tasks(State(app): State<App>, Path(sid): Path<SessionId>) -> Result<Json<Value>> {
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    app.exec.list_tasks(sid).await.map(Json)
}

async fn kill_task(
    State(app): State<App>,
    Path((sid, tid)): Path<(SessionId, String)>,
) -> Result<Json<Value>> {
    if app.exec.presence().known_peer_count() > 0 {
        app.cluster.ensure_session_watched(sid).await?;
    }
    app.exec.kill_task(sid, tid).await.map(Json)
}
