use crate::{
    App, Error, Result, calls,
    cluster::{exec::ActiveLease, ledger::StreamHead},
    events::Notice,
    id::{ItemId, SessionId},
    model::{self, Failure},
    store::{self, Session, SessionStatus, Store, session},
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub struct Control {
    pub soft: CancellationToken,
    pub hard: CancellationToken,
    pub epoch: i64,
    pub run_gen: u64,
    pub lease: Option<Arc<ActiveLease>>,
}

impl Control {
    fn lease_valid(&self) -> bool {
        self.lease.as_ref().is_none_or(|l| l.is_valid())
    }
}

enum StartEffect {
    None,
    Idle(i64),
    Worker(Vec<ItemId>, i64, i64),
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct StartRunBody {
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    force_offline: bool,
}

/// 启动或恢复会话
///
/// 会话需要可用模型；没有待处理工作时返回当前会话。
pub async fn start(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    body: Bytes,
) -> Result<Json<Session>> {
    let req: StartRunBody = if body.is_empty() {
        StartRunBody::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| Error::bad(e.to_string()))?
    };
    if req.node.is_none() && app.exec.presence().known_peer_count() == 0 {
        return app.start_local_checked(sid, None, None).await.map(Json);
    }
    app.exec
        .start_run(&app, sid, req.node, req.force_offline)
        .await
        .map(Json)
}

impl App {
    pub fn wake(&self, sid: SessionId) {
        let app = self.clone();
        self.workers.spawn(async move {
            let result = if app.exec.presence().known_peer_count() > 0 {
                app.exec.start_wake_run(&app, sid).await
            } else {
                app.start_worker_inner(sid, true, None, None).await
            };
            if let Err(e) = result
                && !app.stopping.is_cancelled()
            {
                tracing::error!(%sid, error=%e, "could not start session for a task event");
                // Preserve a visible recovery path if the selected model became unavailable.
                // Never emit a synced delta if we are partitioned/unleased so majority quorum never forks.
                let _ = app
                    .db
                    .call(move |s| {
                        s.pending_wakes.remove(&sid);
                        s.refresh_local_presence(sid);
                        let can_sync_pause = s.presence.known_peer_count() == 0
                            || (s.is_local_lease_valid(sid)
                                && !matches!(
                                    e.0,
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE
                                        | axum::http::StatusCode::GATEWAY_TIMEOUT
                                        | axum::http::StatusCode::CONFLICT
                                ));
                        if !s.closing
                            && session(&s.conn, sid)?.status == SessionStatus::Idle
                            && store::has_pending(&s.conn, sid)?
                        {
                            let msg = e.to_string();
                            if can_sync_pause {
                                s.status(sid, "paused", &msg)?;
                            } else if s.presence.remote_owner(sid).is_none() {
                                let now = chrono::Utc::now().timestamp_millis();
                                let _ = s.conn.execute(
                                    "UPDATE sessions SET status='paused', last_error=?2, updated_at=?3 WHERE id=?1",
                                    (sid, &msg, now),
                                );
                                s.refresh_local_presence_with(
                                    sid,
                                    SessionStatus::Paused,
                                    &msg,
                                    now,
                                );
                                s.notify_status(sid, "paused", &msg, now);
                            }
                        }
                        Ok(())
                    })
                    .await;
            } else {
                let _ = app
                    .db
                    .call(move |s| {
                        if s.pending_wakes.remove(&sid) {
                            s.refresh_local_presence(sid);
                        }
                        Ok(())
                    })
                    .await;
            }
        });
    }

    pub async fn start_local_checked(
        &self,
        sid: SessionId,
        expected_head: Option<StreamHead>,
        lease: Option<Arc<ActiveLease>>,
    ) -> Result<Session> {
        self.start_worker_inner(sid, false, expected_head, lease)
            .await
    }

    pub(crate) async fn start_wake_checked(
        &self,
        sid: SessionId,
        expected_head: Option<StreamHead>,
        lease: Option<Arc<ActiveLease>>,
    ) -> Result<Session> {
        self.start_worker_inner(sid, true, expected_head, lease)
            .await
    }

    async fn start_worker_inner(
        &self,
        sid: SessionId,
        automatic: bool,
        expected_head: Option<StreamHead>,
        lease: Option<Arc<ActiveLease>>,
    ) -> Result<Session> {
        if automatic && self.exec.presence().remote_owner(sid).is_some() {
            let _ = self
                .db
                .call(move |s| {
                    s.pending_wakes.remove(&sid);
                    s.refresh_local_presence(sid);
                    Ok(())
                })
                .await;
            return self
                .db
                .call(move |s| Ok(s.session_view(session(&s.conn, sid)?)))
                .await;
        }
        let worker = self.clone();
        self.db
            .start_write(
                sid,
                move |conn, active| {
                    let current = session(conn, sid)?;
                    if automatic
                        && (active
                            || current.status != SessionStatus::Idle
                            || !store::has_pending(conn, sid)?)
                    {
                        return Ok((current, StartEffect::None));
                    }
                    let model_id = current.model_id;
                    crate::catalog::available_capabilities(conn, model_id)?;
                    if current.status == SessionStatus::Pausing {
                        return Err(Error::conflict("session is pausing"));
                    }
                    if active {
                        return Ok((current, StartEffect::None));
                    }
                    crate::cluster::ledger::assert_no_conflict(conn, sid.as_str())?;
                    if !crate::cluster::ledger::is_session_writable_local(conn, sid)? {
                        return Err(Error::conflict(
                            "session history is incomplete or read-only locally; sync before running",
                        ));
                    }
                    if let Some(ref exp) = expected_head {
                        let cur = crate::cluster::ledger::stream_head(conn, sid.as_str())?;
                        let matches = cur.as_ref().is_some_and(|h| {
                            h.epoch == exp.epoch
                                && h.head_seq == exp.head_seq
                                && h.head_hash == exp.head_hash
                        });
                        if !matches {
                            return Err(Error::conflict(
                                "session history changed concurrently before run start",
                            ));
                        }
                    }
                    let run_epoch = crate::cluster::ledger::stream_head(conn, sid.as_str())?
                        .map_or(1, |h| h.epoch);
                    let unfinished: bool = conn
                        .prepare_cached("SELECT needs_model FROM sessions WHERE id=?")?
                        .query_row([sid], |r| r.get(0))?;
                    let now = chrono::Utc::now().timestamp_millis();
                    if !unfinished && !store::has_pending(conn, sid)? {
                        if current.status == SessionStatus::Paused {
                            conn.prepare_cached(
                                "UPDATE sessions SET status='idle',last_error='',updated_at=?2 WHERE id=?1",
                            )?
                            .execute((sid, now))?;
                            crate::cluster::ledger::record_session_delta_at_epoch(
                                conn,
                                sid,
                                "session_delta",
                                &[],
                                Vec::new(),
                                Vec::new(),
                                Some(run_epoch),
                            )?;
                            return Ok((session(conn, sid)?, StartEffect::Idle(now)));
                        }
                        return Ok((current, StartEffect::None));
                    }
                    let recovered = if unfinished {
                        store::publish_tools(conn, sid, true)?
                    } else {
                        Vec::new()
                    };
                    store::take_pending(conn, sid)?;
                    conn.prepare_cached(
                        "UPDATE sessions SET status='running',last_error='',updated_at=?2 WHERE id=?1",
                    )?
                    .execute((sid, now))?;
                    Ok((
                        session(conn, sid)?,
                        StartEffect::Worker(recovered, now, run_epoch),
                    ))
                },
                move |s, (current, effect)| {
                    s.pending_wakes.remove(&sid);
                    match effect {
                        StartEffect::None => {
                            s.refresh_local_presence_with(
                                sid,
                                current.status.clone(),
                                &current.last_error,
                                current.updated_at,
                            );
                        }
                        StartEffect::Idle(updated_at) => {
                            s.refresh_local_presence_with(sid, SessionStatus::Idle, "", updated_at);
                            s.notify_status(sid, "idle", "", updated_at);
                        }
                        StartEffect::Worker(recovered, updated_at, run_epoch) => {
                            let notices = recovered
                                .into_iter()
                                .try_for_each(|id| s.item_saved(sid, id));
                            let channel = s.channel(sid);
                            let run_gen = s.bump_run_gen(sid);
                            let control = Control {
                                soft: CancellationToken::new(),
                                hard: CancellationToken::new(),
                                epoch: run_epoch,
                                run_gen,
                                lease,
                            };
                            s.active.insert(sid, control.clone());
                            s.refresh_local_presence_with(
                                sid,
                                SessionStatus::Running,
                                "",
                                updated_at,
                            );
                            s.notify_status(sid, "running", "", updated_at);
                            let tracker = worker.workers.clone();
                            // A committed start owns a worker even if the client left or a notice failed.
                            tracker.spawn(worker.run(sid, control, channel));
                            notices?;
                        }
                    }
                    Ok(s.session_view(current))
                },
            )
            .await
    }

    pub async fn fence_session_local(
        &self,
        sid: SessionId,
        fenced_term: Option<u64>,
        reason: &'static str,
    ) {
        let _ = self
            .db
            .call(move |s| {
                let lease_matches = match fenced_term {
                    Some(term) => {
                        s.active
                            .get(&sid)
                            .and_then(|c| c.lease.as_ref())
                            .is_some_and(|l| l.term == term)
                            || s.presence
                                .active_local_lease(sid)
                                .is_some_and(|l| l.term == term)
                    }
                    None => {
                        (s.active.contains_key(&sid)
                            || s.pending_wakes.contains(&sid)
                            || crate::codemode::has_session_tasks(s, sid))
                            && s.presence.active_local_lease_kind(sid).is_none()
                    }
                };
                if !lease_matches {
                    return Ok(());
                }
                if let Some(ctrl) = s.active.remove(&sid) {
                    ctrl.soft.cancel();
                    ctrl.hard.cancel();
                    if let Some(l) = &ctrl.lease {
                        l.revoked.cancel();
                    }
                }
                s.pending_wakes.remove(&sid);
                crate::codemode::stop_session(s, sid);
                let now = chrono::Utc::now().timestamp_millis();
                // Self-fencing must not append a synced commit so surviving majority quorum never forks.
                let _ = s.conn.execute(
                    "UPDATE sessions SET status='paused',last_error=?2,updated_at=?3 WHERE id=?1",
                    (sid, reason, now),
                );
                s.presence.set_local(sid, None);
                s.notify_status(sid, "paused", reason, now);
                Ok(())
            })
            .await;
    }
}
#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum PauseMode {
    #[default]
    Soft,
    Hard,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pause {
    #[serde(default)]
    mode: PauseMode,
}
pub async fn pause(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
    Query(q): Query<Pause>,
) -> Result<Json<Session>> {
    app.exec
        .pause_run(&app, sid, matches!(q.mode, PauseMode::Hard))
        .await
        .map(Json)
}

impl App {
    pub async fn pause_local(&self, sid: SessionId, hard: bool) -> Result<Session> {
        self.db
            .call(move |s| {
                let current = session(&s.conn, sid)?;
                if current.status != SessionStatus::Paused {
                    request_pause(s, sid, hard, "paused by user")?;
                } else if hard {
                    crate::codemode::stop_session(s, sid);
                    s.refresh_local_presence(sid);
                }
                Ok(s.session_view(session(&s.conn, sid)?))
            })
            .await
    }
}
fn request_pause(s: &mut Store, sid: SessionId, hard: bool, reason: &str) -> Result<()> {
    if let Some(control) = s.active.get(&sid).cloned() {
        if !control.soft.is_cancelled() {
            s.status(sid, "pausing", reason)?;
            control.soft.cancel();
        }
        if hard {
            control.hard.cancel();
            crate::codemode::stop_session(s, sid);
        }
    } else {
        s.status(sid, "paused", reason)?;
        if hard {
            crate::codemode::stop_session(s, sid);
        }
    }
    s.wake_waiters(sid);
    Ok(())
}

impl App {
    pub async fn shutdown(&self) -> Result<()> {
        self.db
            .call(|s| {
                s.closing = true;
                crate::codemode::shutdown_tasks(s, false)?;
                for sid in s.active.keys().copied().collect::<Vec<_>>() {
                    request_pause(s, sid, false, "daemon shutting down")?;
                }
                Ok(())
            })
            .await?;
        self.stopping.cancel();
        self.workers.close();
        if tokio::time::timeout(Duration::from_secs(5), self.workers.wait())
            .await
            .is_err()
        {
            self.db
                .call(|s| {
                    crate::codemode::shutdown_tasks(s, true)?;
                    for c in s.active.values() {
                        c.hard.cancel();
                    }
                    Ok(())
                })
                .await?;
            if tokio::time::timeout(Duration::from_secs(5), self.workers.wait())
                .await
                .is_err()
            {
                tracing::warn!("shutdown deadline reached with unfinished work");
            }
        }
        crate::mcp::close(&self.db).await
    }

    async fn run(self, sid: SessionId, control: Control, channel: broadcast::Sender<Notice>) {
        let outcome = self.run_loop(sid, &control, &channel).await;
        if let Err(problem) = outcome {
            let lease_fenced =
                !control.lease_valid() || !self.exec.presence().is_local_lease_valid(sid);
            let settled = self
                .db
                .call(move |s| {
                    if s.run_gen(sid) != control.run_gen {
                        return Ok(());
                    }
                    if s.active
                        .get(&sid)
                        .is_some_and(|c| c.run_gen == control.run_gen)
                    {
                        s.active.remove(&sid);
                    }
                    s.refresh_local_presence(sid);
                    if lease_fenced || !s.is_local_lease_valid(sid) {
                        crate::codemode::stop_session(s, sid);
                        if let Ok(current) = session(&s.conn, sid)
                            && matches!(
                                current.status,
                                SessionStatus::Running | SessionStatus::Pausing
                            )
                        {
                            let now = chrono::Utc::now().timestamp_millis();
                            let reason = problem.1.as_str();
                            // Local-only fallback pause if lease lapsed before fence_session_local ran.
                            let _ = s.conn.execute(
                                "UPDATE sessions SET status='paused',last_error=?2,updated_at=?3 WHERE id=?1",
                                (sid, reason, now),
                            );
                            s.presence.set_local(sid, None);
                            s.notify_status(sid, "paused", reason, now);
                        }
                        return Ok(());
                    }
                    let stream = crate::cluster::ledger::stream_head(&s.conn, sid.as_str())?;
                    let conflicted = crate::cluster::ledger::has_conflict(&s.conn, sid.as_str())?;
                    let epoch_ok = stream.as_ref().is_none_or(|st| {
                        st.epoch == control.epoch && !st.deleted && !st.retired
                    });
                    let writable = crate::cluster::ledger::is_session_writable_local(&s.conn, sid)?;
                    if conflicted || !epoch_ok || !writable {
                        return Ok(());
                    }
                    let Ok(current) = session(&s.conn, sid) else {
                        return Ok(());
                    };
                    if current.status == SessionStatus::Paused {
                        s.notify_status(sid, "paused", &current.last_error, current.updated_at);
                        return Ok(());
                    }
                    let reason = if control.soft.is_cancelled() {
                        current.last_error
                    } else {
                        problem.1
                    };
                    s.status(sid, "paused", &reason)?;
                    Ok(())
                })
                .await;
            if let Err(err) = settled {
                tracing::error!(%sid, error = %err, "cannot pause session");
            }
        }
    }

    async fn run_loop(
        &self,
        sid: SessionId,
        control: &Control,
        channel: &broadcast::Sender<Notice>,
    ) -> Result<()> {
        let mut retries = 0;
        let mut attempt = 0u64;
        let run_epoch = control.epoch;
        let run_gen = control.run_gen;
        loop {
            if !control.lease_valid() || !self.exec.presence().is_local_lease_valid(sid) {
                return Err(Error::conflict("execution lease lost"));
            }
            enum BeginOutcome {
                Input(model::Input),
                Idle(i64),
            }
            let soft = control.soft.clone();
            let input = self
                .db
                .write_for_run(
                    sid,
                    run_gen,
                    move |conn| {
                        if soft.is_cancelled() {
                            return Err(Error::conflict("paused"));
                        }
                        if crate::cluster::ledger::has_conflict(conn, sid.as_str())?
                            || crate::cluster::ledger::stream_head(conn, sid.as_str())?
                                .is_some_and(|h| h.epoch != run_epoch || h.deleted || h.retired)
                        {
                            return Err(Error::conflict("session epoch superseded or conflicted"));
                        }
                        store::take_pending(conn, sid)?;
                        let needed: bool = conn
                            .prepare_cached("SELECT needs_model FROM sessions WHERE id=?")?
                            .query_row([sid], |r| r.get(0))?;
                        // A queued message can be withdrawn before this worker takes it.
                        if !needed {
                            if !crate::cluster::ledger::is_session_writable_local(conn, sid)? {
                                return Err(Error::conflict(
                                    "session history is incomplete or deleted; sync before modifying",
                                ));
                            }
                            let now = chrono::Utc::now().timestamp_millis();
                            conn.prepare_cached(
                                "UPDATE sessions SET status='idle',last_error='',updated_at=?2 WHERE id=?1",
                            )?
                            .execute((sid, now))?;
                            crate::cluster::ledger::record_session_delta_at_epoch(
                                conn,
                                sid,
                                "session_delta",
                                &[],
                                Vec::new(),
                                Vec::new(),
                                Some(run_epoch),
                            )?;
                            return Ok(BeginOutcome::Idle(now));
                        }
                        let end = store::max_context_position(conn, sid)?;
                        let input = calls::begin(conn, sid, end)?;
                        Ok(BeginOutcome::Input(input))
                    },
                    move |s, outcome| match outcome {
                        BeginOutcome::Input(input) => Ok(Some(input)),
                        BeginOutcome::Idle(now) => {
                            if s.active.get(&sid).is_some_and(|c| c.run_gen == run_gen) {
                                s.active.remove(&sid);
                            }
                            s.refresh_local_presence_with(sid, SessionStatus::Idle, "", now);
                            s.notify_status(sid, "idle", "", now);
                            Ok(None)
                        }
                    },
                )
                .await?;
            let Some(input) = input else {
                return Ok(());
            };
            attempt += 1;
            let call_id = input.call_id;
            self.streams.start_call(sid, call_id);
            let mut output = model::Output::default();
            let has_null_att = self.exec.presence().has_null_attachments();
            let multi_node = self.exec.presence().known_peer_count() > 0;
            let streamed = if control.lease.is_none() && !has_null_att && !multi_node {
                tokio::select! {
                    biased;
                    _ = control.hard.cancelled() => Err(Failure::Cancelled),
                    result = self.stream(sid, input, &mut output, &control.hard, (run_epoch, run_gen)) => result,
                }
            } else {
                tokio::select! {
                    biased;
                    _ = control.hard.cancelled() => Err(Failure::Cancelled),
                    _ = async {
                        match &control.lease {
                            Some(lease) => lease.revoked.cancelled().await,
                            None => std::future::pending::<()>().await,
                        }
                    } => Err(Failure::Cancelled),
                    result = async {
                        if has_null_att || multi_node {
                            self.cluster
                                .ensure_session_attachments(sid)
                                .await
                                .map_err(Failure::Fatal)?;
                        }
                        self.stream(sid, input, &mut output, &control.hard, (run_epoch, run_gen))
                            .await
                    } => result,
                }
            };
            let finished_at = chrono::Utc::now().timestamp_millis();
            let report = std::mem::take(&mut output.report);
            // The buffered terminal reply and its billing record can commit together.
            let final_report = if output.final_item.is_some() {
                Some(report)
            } else {
                let (status, reason) = match &streamed {
                    Ok(()) => ("succeeded", None),
                    Err(Failure::Cancelled) => ("cancelled", Some("request cancelled locally")),
                    Err(Failure::Transient(_)) => (
                        "failed",
                        Some("upstream connection or temporary response error"),
                    ),
                    Err(Failure::Fatal(_)) => ("failed", Some("model request failed")),
                };
                self.db
                    .call(move |s| {
                        if s.run_gen(sid) != run_gen {
                            return Ok(());
                        }
                        report.finish(&s.conn, call_id, status, reason, finished_at)
                    })
                    .await?;
                None
            };
            // Tool writes outlive a transient upstream disconnect. Publish only once all finish.
            while let Some(result) = output.tasks.join_next().await {
                if let Err(err) = result.unwrap_or_else(|e| Err(Error::internal(e.to_string()))) {
                    self.streams.clear_call(sid, call_id, true);
                    output.tasks.shutdown().await;
                    return Err(err);
                }
            }
            let interrupted = control.hard.is_cancelled() && output.final_item.is_none();
            if streamed.is_err() || interrupted {
                self.streams.clear_call(sid, call_id, true);
            }
            // A call may have reached the store just as the stream was cancelled, before spawn.
            if (interrupted || (output.tools && streamed.is_err()))
                && control.lease_valid()
                && self.exec.presence().is_local_lease_valid(sid)
            {
                self.db
                    .call(move |s| {
                        if s.run_gen(sid) != run_gen || !s.is_local_lease_valid(sid) {
                            return Ok(());
                        }
                        let tx = s.conn.transaction()?;
                        if crate::cluster::ledger::has_conflict(&tx, sid.as_str())?
                            || crate::cluster::ledger::stream_head(&tx, sid.as_str())?
                                .is_some_and(|h| h.epoch != run_epoch || h.deleted || h.retired)
                        {
                            return Ok(());
                        }
                        let missing = store::publish_tools(&tx, sid, interrupted)?;
                        tx.commit()?;
                        for id in missing {
                            s.item_saved(sid, id)?;
                        }
                        Ok(())
                    })
                    .await?;
            }
            if interrupted {
                return Err(Error::conflict("paused"));
            }
            if !control.lease_valid() || !self.exec.presence().is_local_lease_valid(sid) {
                return Err(Error::conflict("execution lease lost"));
            }
            match streamed {
                Err(Failure::Transient(reason)) if retries < 3 => {
                    retries += 1;
                    Notice::publish(
                        channel,
                        "attempt.retry",
                        json!({"attempt":attempt,"reason":reason}),
                    );
                    tokio::select! {_=control.soft.cancelled()=>return Err(Error::conflict("paused")),_=tokio::time::sleep(Duration::from_millis(250<<(retries-1)))=>()}
                    continue;
                }
                Err(Failure::Transient(reason)) => {
                    return Err(Error::internal(format!("retry limit reached: {reason}")));
                }
                Err(Failure::Cancelled) => return Err(Error::conflict("paused")),
                Err(Failure::Fatal(err)) => return Err(err),
                Ok(()) if !output.answer && !output.tools => {
                    self.streams.clear_call(sid, call_id, true);
                    return Err(Error::internal(
                        "model completed without an answer or tool call",
                    ));
                }
                Ok(()) => (),
            }
            retries = 0;
            let tools = output.tools;
            let final_output_index = output.final_output_index;
            let soft = control.soft.clone();
            let stopped = self
                .db
                .write_for_run(
                    sid,
                    run_gen,
                    move |conn| {
                        if crate::cluster::ledger::stream_head(conn, sid.as_str())?
                            .is_some_and(|h| h.epoch != run_epoch || h.deleted || h.retired)
                        {
                            return Err(Error::conflict("session epoch superseded"));
                        }
                        if tools {
                            store::publish_tools(conn, sid, false)?;
                        }
                        if let Some(report) = final_report {
                            report.finish(conn, call_id, "succeeded", None, finished_at)?;
                        }
                        let mut added_items = Vec::new();
                        let mut added_ctx = Vec::new();
                        let saved = if let Some(item) = output.final_item {
                            let (id, pos) = store::add_context_item_raw(
                                conn,
                                sid,
                                "assistant_message",
                                &item,
                                call_id,
                            )?;
                            added_items.push(id);
                            added_ctx.push(crate::cluster::ledger::ContextPos {
                                position: pos,
                                item_id: id,
                            });
                            Some(id)
                        } else {
                            None
                        };
                        let state = if soft.is_cancelled() {
                            "paused"
                        } else if tools || store::has_pending(conn, sid)? {
                            "running"
                        } else {
                            "idle"
                        };
                        let now = chrono::Utc::now().timestamp_millis();
                        conn.prepare_cached(
                            "UPDATE sessions SET needs_model=?2,status=?3,updated_at=?4 WHERE id=?1",
                        )?
                        .execute((sid, tools, state, now))?;
                        crate::cluster::ledger::record_session_delta_at_epoch(
                            conn,
                            sid,
                            "session_delta",
                            &added_items,
                            added_ctx,
                            Vec::new(),
                            Some(run_epoch),
                        )?;
                        Ok((saved, state, now))
                    },
                    move |s, (saved, state, now)| {
                        if let Some(id) = saved {
                            s.item_saved(sid, id)?;
                        }
                        if state != "running" {
                            if s.active.get(&sid).is_some_and(|c| c.run_gen == run_gen) {
                                s.active.remove(&sid);
                            }
                            s.refresh_local_presence(sid);
                            if state == "idle" {
                                s.notify_status(sid, "idle", "", now);
                            } else {
                                let info = session(&s.conn, sid)?;
                                s.notify_status(sid, state, &info.last_error, now);
                            }
                        }
                        Ok(state != "running")
                    },
                )
                .await?;
            if let Some(idx) = final_output_index {
                self.streams.finish_item(sid, call_id, idx);
            }
            self.streams.clear_call(sid, call_id, true);
            if stopped {
                return Ok(());
            }
        }
    }
}
