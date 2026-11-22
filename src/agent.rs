use crate::{
    App, Error, Result, calls,
    events::Notice,
    model::{self, Failure},
    store::{self, Session, SessionStatus, Store, session},
};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub struct Control {
    pub soft: CancellationToken,
    pub hard: CancellationToken,
}

enum StartEffect {
    None,
    Idle,
    Worker(Vec<i64>),
}

/// 启动或恢复会话
///
/// 会话需要可用模型；没有待处理工作时返回当前会话。
#[utoipa::path(
    put,
    path = "/sessions/{sid}/run",
    tag = "运行",
    params(("sid" = i64, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "启动或恢复会话", body = Session),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        (status = 503, description = "服务正在关闭", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
pub async fn start(State(app): State<App>, Path(sid): Path<i64>) -> Result<Json<Session>> {
    app.start_worker(sid, false).await
}

impl App {
    pub fn wake(&self, sid: i64) {
        let app = self.clone();
        self.workers.spawn(async move {
            if let Err(e) = app.start_worker(sid, true).await
                && !app.stopping.is_cancelled()
            {
                tracing::error!(sid,error=%e,"could not start session for a task event");
                // Preserve a visible recovery path if the selected model became unavailable.
                let _ = app
                    .db
                    .call(move |s| {
                        if !s.closing
                            && session(&s.conn, sid)?.status == SessionStatus::Idle
                            && store::has_pending(&s.conn, sid)?
                        {
                            s.status(sid, "paused", &e.to_string())?;
                        }
                        Ok(())
                    })
                    .await;
            }
        });
    }

    async fn start_worker(&self, sid: i64, automatic: bool) -> Result<Json<Session>> {
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
                    let model_id = current
                        .model_id
                        .ok_or_else(|| Error::conflict("select a session model before running"))?;
                    crate::catalog::available_capabilities(conn, model_id)?;
                    if current.status == SessionStatus::Pausing {
                        return Err(Error::conflict("session is pausing"));
                    }
                    if active {
                        return Ok((current, StartEffect::None));
                    }
                    let unfinished: bool = conn.query_row(
                        "SELECT needs_model FROM sessions WHERE id=?",
                        [sid],
                        |r| r.get(0),
                    )?;
                    if !unfinished && !store::has_pending(conn, sid)? {
                        if current.status == SessionStatus::Paused {
                            conn.execute(
                                "UPDATE sessions SET status='idle',last_error='' WHERE id=?",
                                [sid],
                            )?;
                            return Ok((session(conn, sid)?, StartEffect::Idle));
                        }
                        return Ok((current, StartEffect::None));
                    }
                    let recovered = if unfinished {
                        store::publish_tools(conn, sid, true)?
                    } else {
                        Vec::new()
                    };
                    store::take_pending(conn, sid)?;
                    conn.execute(
                        "UPDATE sessions SET status='running',last_error='' WHERE id=?",
                        [sid],
                    )?;
                    Ok((session(conn, sid)?, StartEffect::Worker(recovered)))
                },
                move |s, (current, effect)| {
                    match effect {
                        StartEffect::None => (),
                        StartEffect::Idle => s.notify_status(sid, "idle", ""),
                        StartEffect::Worker(recovered) => {
                            let notices = recovered
                                .into_iter()
                                .try_for_each(|id| s.item_saved(sid, id));
                            let channel = s.channel(sid);
                            Notice::publish(
                                &channel,
                                "session.status",
                                json!({"status":"running","reason":""}),
                            );
                            let control = Control::default();
                            s.active.insert(sid, control.clone());
                            let tracker = worker.workers.clone();
                            // A committed start owns a worker even if the client left or a notice failed.
                            tracker.spawn(worker.run(sid, control, channel));
                            notices?;
                        }
                    }
                    Ok(Json(current))
                },
            )
            .await
    }
}
#[derive(Deserialize, Default, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
enum PauseMode {
    #[default]
    Soft,
    Hard,
}
#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct Pause {
    #[serde(default)]
    /// soft 等待当前工作结束，hard 取消当前工作。默认 soft。
    #[param(inline)]
    mode: PauseMode,
}
/// 暂停会话
#[utoipa::path(
    delete,
    path = "/sessions/{sid}/run",
    tag = "运行",
    params(("sid" = i64, Path, description = "会话 ID"), Pause),
    responses(
        (status = 200, description = "暂停会话", body = Session),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
pub async fn pause(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(q): Query<Pause>,
) -> Result<Json<Session>> {
    app.db
        .call(move |s| {
            let current = session(&s.conn, sid)?;
            if matches!(q.mode, PauseMode::Hard) {
                crate::codemode::stop_session(s, sid);
            }
            if current.status != SessionStatus::Paused {
                request_pause(s, sid, matches!(q.mode, PauseMode::Hard), "paused by user")?;
            }
            Ok(Json(session(&s.conn, sid)?))
        })
        .await
}
fn request_pause(s: &mut Store, sid: i64, hard: bool, reason: &str) -> Result<()> {
    if let Some(control) = s.active.get(&sid).cloned() {
        if !control.soft.is_cancelled() {
            s.status(sid, "pausing", reason)?;
            control.soft.cancel();
        }
        if hard {
            control.hard.cancel();
        }
    } else {
        s.status(sid, "paused", reason)?;
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

    async fn run(self, sid: i64, control: Control, channel: broadcast::Sender<Notice>) {
        let outcome = self.run_loop(sid, &control, &channel).await;
        if let Err(problem) = outcome {
            let settled = self
                .db
                .call(move |s| {
                    let reason = if control.soft.is_cancelled() {
                        s.conn.query_row(
                            "SELECT last_error FROM sessions WHERE id=?",
                            [sid],
                            |r| r.get(0),
                        )?
                    } else {
                        problem.1
                    };
                    s.active.remove(&sid);
                    s.status(sid, "paused", &reason)?;
                    Ok(())
                })
                .await;
            if let Err(err) = settled {
                tracing::error!(sid, error = %err, "cannot pause session");
            }
        }
    }

    async fn run_loop(
        &self,
        sid: i64,
        control: &Control,
        channel: &broadcast::Sender<Notice>,
    ) -> Result<()> {
        let mut retries = 0;
        let mut attempt = 0u64;
        loop {
            let input = self
                .db
                .call(move |s| {
                    if s.active[&sid].soft.is_cancelled() {
                        return Err(Error::conflict("paused"));
                    }
                    let tx = s.conn.transaction()?;
                    store::take_pending(&tx, sid)?;
                    let needed: bool =
                        tx.query_row("SELECT needs_model FROM sessions WHERE id=?", [sid], |r| {
                            r.get(0)
                        })?;
                    // A queued message can be withdrawn before this worker takes it.
                    if !needed {
                        tx.commit()?;
                        s.status(sid, "idle", "")?;
                        s.active.remove(&sid);
                        return Ok(None);
                    }
                    let end = tx.query_row(
                        "SELECT coalesce(max(position),0) FROM context_items WHERE session_id=?",
                        [sid],
                        |r| r.get(0),
                    )?;
                    let input = calls::begin(&tx, sid, end)?;
                    tx.commit()?;
                    Ok(Some(input))
                })
                .await?;
            let Some(input) = input else {
                return Ok(());
            };
            attempt += 1;
            let call_id = input.call_id;
            let mut output = model::Output::default();
            let streamed = tokio::select! {
                biased;
                _ = control.hard.cancelled() => Err(Failure::Cancelled),
                result = self.stream(sid, input, &mut output, &control.hard) => result,
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
                    .call(move |s| report.finish(&s.conn, call_id, status, reason, finished_at))
                    .await?;
                None
            };
            // Tool writes outlive a transient upstream disconnect. Publish only once all finish.
            while let Some(result) = output.tasks.join_next().await {
                if let Err(err) = result.unwrap_or_else(|e| Err(Error::internal(e.to_string()))) {
                    output.tasks.shutdown().await;
                    return Err(err);
                }
            }
            let interrupted = control.hard.is_cancelled() && output.final_item.is_none();
            // A call may have reached the store just as the stream was cancelled, before spawn.
            if interrupted || (output.tools && streamed.is_err()) {
                self.db
                    .call(move |s| {
                        let tx = s.conn.transaction()?;
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
                    return Err(Error::internal(
                        "model completed without an answer or tool call",
                    ));
                }
                Ok(()) => (),
            }
            retries = 0;
            let tools = output.tools;
            let soft = control.soft.clone();
            let stopped = self
                .db
                .write(
                    move |conn| {
                        if tools {
                            store::publish_tools(conn, sid, false)?;
                        }
                        if let Some(report) = final_report {
                            report.finish(conn, call_id, "succeeded", None, finished_at)?;
                        }
                        let saved = if let Some(item) = output.final_item {
                            Some(store::add_context_item(
                                conn,
                                sid,
                                "assistant_message",
                                &item,
                                call_id,
                            )?)
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
                        conn.execute(
                            "UPDATE sessions SET needs_model=?2,status=?3 WHERE id=?1",
                            (sid, tools, state),
                        )?;
                        Ok((saved, state))
                    },
                    move |s, (saved, state)| {
                        if let Some(id) = saved {
                            s.item_saved(sid, id)?;
                        }
                        if state != "running" {
                            s.active.remove(&sid);
                            let info = session(&s.conn, sid)?;
                            Notice::publish(
                                &s.channel(sid),
                                "session.status",
                                json!({"status":state,"reason":info.last_error}),
                            );
                        }
                        Ok(state != "running")
                    },
                )
                .await?;
            if stopped {
                return Ok(());
            }
        }
    }
}
