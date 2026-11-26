use crate::{
    App, Error, Result, calls,
    cluster::exec::{ActiveLease, RunAuth},
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
    Worker(Vec<ItemId>, i64),
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
        match app.start_local_checked(sid, RunAuth::Standalone).await {
            Err(err)
                if err.0 == axum::http::StatusCode::CONFLICT
                    && err.1 == "cluster membership changed while starting session" => {}
            other => return other.map(Json),
        }
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
                match app.start_worker_inner(sid, true, RunAuth::Standalone).await {
                    Err(err)
                        if err.0 == axum::http::StatusCode::CONFLICT
                            && err.1 == "cluster membership changed while starting session" =>
                    {
                        app.exec.start_wake_run(&app, sid).await
                    }
                    other => other,
                }
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
                                    "UPDATE sessions SET local_status='paused', local_error=?2, updated_at=max(updated_at,?3) WHERE id=?1",
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
        auth: crate::cluster::exec::RunAuth,
    ) -> Result<Session> {
        self.start_worker_inner(sid, false, auth).await
    }

    pub(crate) async fn start_wake_checked(
        &self,
        sid: SessionId,
        auth: crate::cluster::exec::RunAuth,
    ) -> Result<Session> {
        self.start_worker_inner(sid, true, auth).await
    }

    async fn start_worker_inner(
        &self,
        sid: SessionId,
        automatic: bool,
        auth: crate::cluster::exec::RunAuth,
    ) -> Result<Session> {
        use crate::cluster::exec::RunAuth;

        let lease = match &auth {
            RunAuth::Quorum(l) => Some(l.clone()),
            RunAuth::Standalone | RunAuth::ForcedOffline => None,
        };
        let verify_standalone = matches!(auth, RunAuth::Standalone);
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
                    if verify_standalone {
                        let has_peers: bool =
                            conn.query_row("SELECT EXISTS(SELECT 1 FROM cluster_peers)", [], |r| r.get(0))?;
                        if has_peers {
                            return Err(Error::conflict(
                                "cluster membership changed while starting session",
                            ));
                        }
                    }
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
                    if !crate::cluster::ledger::session_branch_complete(conn, sid)? {
                        return Err(Error::conflict(
                            "session history is incomplete locally; sync before running",
                        ));
                    }
                    let unfinished = store::needs_model(conn, sid)?;
                    let now = chrono::Utc::now().timestamp_millis();
                    if !unfinished && !store::has_pending(conn, sid)? {
                        if current.status == SessionStatus::Paused {
                            let status_ts = crate::cluster::ledger::tick_hlc(conn)?;
                            let rev = crate::cluster::ledger::next_catalog_rev(conn)?;
                            conn.prepare_cached(
                                "UPDATE sessions SET status='idle',last_error='',local_status=NULL,local_error='',updated_at=max(updated_at,?2),status_ts=?3,sync_rev=?4 WHERE id=?1",
                            )?
                            .execute((sid, now, status_ts, rev))?;
                            crate::cluster::ledger::stage_catalog();
                            return Ok((session(conn, sid)?, StartEffect::Idle(now)));
                        }
                        return Ok((current, StartEffect::None));
                    }
                    let recovered = if unfinished {
                        store::publish_tools(conn, sid, true, None, Some(current.active_head))?.0
                    } else {
                        Vec::new()
                    };
                    store::take_pending(conn, sid)?;
                    conn.prepare_cached(
                        "UPDATE sessions SET local_status='running',local_error='',updated_at=max(updated_at,?2) WHERE id=?1",
                    )?
                    .execute((sid, now))?;
                    Ok((session(conn, sid)?, StartEffect::Worker(recovered, now)))
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
                        StartEffect::Worker(recovered, updated_at) => {
                            let notices = recovered
                                .into_iter()
                                .try_for_each(|id| s.item_saved(sid, id));
                            let channel = s.channel(sid);
                            let run_gen = s.bump_run_gen(sid);
                            let control = Control {
                                soft: CancellationToken::new(),
                                hard: CancellationToken::new(),
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
                            && !matches!(
                                s.presence.active_local_lease_kind(sid),
                                Some(crate::cluster::exec::LocalLease::Quorum(_))
                            )
                    }
                };
                if !lease_matches {
                    return Ok(());
                }
                fence_local_run(s, sid, reason)
            })
            .await;
    }
}

fn fence_local_run(s: &mut Store, sid: SessionId, reason: &str) -> Result<()> {
    if let Some(ctrl) = s.active.remove(&sid) {
        ctrl.soft.cancel();
        ctrl.hard.cancel();
    }
    if let Some(lease) = s.presence.active_local_lease(sid) {
        lease.revoked.cancel();
    }
    s.pending_wakes.remove(&sid);
    crate::codemode::stop_session(s, sid);
    s.bump_reset_gen(sid);
    let now = chrono::Utc::now().timestamp_millis();
    // A fenced runner can only record its pause locally.
    let update = s.conn.execute(
        "UPDATE sessions SET local_status='paused',local_error=?2,updated_at=max(updated_at,?3) WHERE id=?1",
        (sid, reason, now),
    );
    s.refresh_local_presence(sid);
    update?;
    s.notify_status(sid, "paused", reason, now);
    Ok(())
}

fn pause_failed_run(s: &mut Store, sid: SessionId, control: &Control, problem: Error) -> Result<()> {
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
    let mut fenced = false;
    let result = s.status_checked(sid, "paused", &reason, |s| {
        if control.lease_valid() && s.is_local_lease_valid(sid) {
            return Ok(());
        }
        fenced = true;
        Err(Error::conflict("execution lease lost"))
    });
    if fenced {
        crate::cluster::ledger::clear_staged();
        return fence_local_run(s, sid, "execution lease lost");
    }
    result
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
            let settled = self
                .db
                .call(move |s| {
                    if s.active.get(&sid).is_none_or(|run| run.run_gen != control.run_gen) {
                        return Ok(());
                    }
                    if !control.lease_valid() || !s.is_local_lease_valid(sid) {
                        return fence_local_run(s, sid, "execution lease lost");
                    }
                    s.active.remove(&sid);
                    // Keep the lease until the terminal write has committed or failed.
                    let result = pause_failed_run(s, sid, &control, problem);
                    s.refresh_local_presence(sid);
                    result
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
                        if conn
                            .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                            .query_row([sid], |r| r.get::<_, bool>(0))?
                        {
                            return Err(Error::conflict("session deleted"));
                        }
                        store::take_pending(conn, sid)?;
                        let needed = store::needs_model(conn, sid)?;
                        // A queued message can be withdrawn before this worker takes it.
                        if !needed {
                            let now = chrono::Utc::now().timestamp_millis();
                            let status_ts = crate::cluster::ledger::tick_hlc(conn)?;
                            let rev = crate::cluster::ledger::next_catalog_rev(conn)?;
                            conn.prepare_cached(
                                "UPDATE sessions SET status='idle',last_error='',local_status=NULL,local_error='',updated_at=max(updated_at,?2),status_ts=?3,sync_rev=?4 WHERE id=?1",
                            )?
                            .execute((sid, now, status_ts, rev))?;
                            crate::cluster::ledger::stage_catalog();
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
            output.current_head = input.head;
            let has_null_att = self.exec.presence().has_null_attachments();
            let multi_node = self.exec.presence().known_peer_count() > 0;
            let streamed = if control.lease.is_none() && !has_null_att && !multi_node {
                tokio::select! {
                    biased;
                    _ = control.hard.cancelled() => Err(Failure::Cancelled),
                    result = self.stream(sid, input, &mut output, &control.hard, run_gen) => result,
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
                        self.stream(sid, input, &mut output, &control.hard, run_gen)
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
            let mut step_head = output.current_head;
            // A call may have reached the store just as the stream was cancelled, before spawn.
            if (interrupted || (output.tools && streamed.is_err()))
                && control.lease_valid()
                && self.exec.presence().is_local_lease_valid(sid)
            {
                self.db
                    .call(move |s| {
                        if s.active.get(&sid).is_none_or(|run| run.run_gen != run_gen)
                            || !s.is_local_lease_valid(sid)
                        {
                            return Ok(());
                        }
                        let tx = s.conn.transaction()?;
                        if tx
                            .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                            .query_row([sid], |r| r.get::<_, bool>(0))?
                        {
                            return Ok(());
                        }
                        let (missing, _) = store::publish_tools(
                            &tx,
                            sid,
                            interrupted,
                            Some(call_id),
                            Some(step_head),
                        )?;
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
                        if conn
                            .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                            .query_row([sid], |r| r.get::<_, bool>(0))?
                        {
                            return Err(Error::conflict("session deleted"));
                        }
                        if tools {
                            let (_, next_head) = store::publish_tools(
                                conn,
                                sid,
                                false,
                                Some(call_id),
                                Some(step_head),
                            )?;
                            step_head = next_head;
                        }
                        if let Some(report) = final_report {
                            report.finish(conn, call_id, "succeeded", None, finished_at)?;
                        }
                        let saved = if let Some(item) = output.final_item {
                            let (id, next_head) = store::add_context_item_at(
                                conn,
                                sid,
                                "assistant_message",
                                &item,
                                call_id,
                                Some(step_head),
                            )?;
                            step_head = next_head;
                            Some(id)
                        } else {
                            None
                        };
                        if !tools
                            && let Some(h) = step_head
                        {
                            crate::cluster::ledger::mark_turn_settled_node(conn, sid, h)?;
                        }
                        let state = if soft.is_cancelled() {
                            "paused"
                        } else if tools || store::has_pending(conn, sid)? {
                            "running"
                        } else {
                            "idle"
                        };
                        let now = chrono::Utc::now().timestamp_millis();
                        if matches!(state, "idle" | "paused") {
                            let status_ts = crate::cluster::ledger::tick_hlc(conn)?;
                            let rev = crate::cluster::ledger::next_catalog_rev(conn)?;
                            conn.prepare_cached(
                                "UPDATE sessions
                                 SET status=?2,
                                     last_error=CASE WHEN ?2='paused' THEN coalesce(nullif(local_error,''), last_error) ELSE '' END,
                                     local_status=NULL,
                                     local_error='',
                                     updated_at=max(updated_at,?3),
                                     status_ts=?4,
                                     sync_rev=?5
                                 WHERE id=?1",
                            )?
                            .execute((sid, state, now, status_ts, rev))?;
                            crate::cluster::ledger::stage_catalog();
                        } else {
                            conn.prepare_cached(
                                "UPDATE sessions SET local_status='running',local_error='',updated_at=max(updated_at,?2) WHERE id=?1",
                            )?
                            .execute((sid, now))?;
                        }
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
