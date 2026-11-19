use crate::{
    App, Error, Result,
    api::Config,
    events::Notice,
    model::{self, Failure, Input},
    store::{self, Store, session},
};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub struct Control {
    soft: CancellationToken,
    pub hard: CancellationToken,
}

enum StartEffect {
    None,
    Idle,
    Worker(Vec<i64>),
}

pub async fn start(State(app): State<App>, Path(sid): Path<i64>) -> Result<Json<Value>> {
    let worker = app.clone();
    app.db
        .start_write(
            sid,
            move |conn, active| {
                let current = session(conn, sid)?;
                if current["status"] == "pausing" {
                    return Err(Error::conflict("session is pausing"));
                }
                if active {
                    return Ok((current, StartEffect::None));
                }
                let unfinished: bool =
                    conn.query_row("SELECT needs_model FROM sessions WHERE id=?", [sid], |r| {
                        r.get(0)
                    })?;
                if !unfinished && !store::has_pending(conn, sid)? {
                    if current["status"] == "paused" {
                        conn.execute(
                            "UPDATE sessions SET status='idle',last_error='' WHERE id=?",
                            [sid],
                        )?;
                        return Ok((session(conn, sid)?, StartEffect::Idle));
                    }
                    return Ok((current, StartEffect::None));
                }
                if Config::load(conn)?.is_none() {
                    return Err(Error::conflict("configure the model first"));
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
    Path(sid): Path<i64>,
    Query(q): Query<Pause>,
) -> Result<Json<Value>> {
    app.db
        .call(move |s| {
            let current = session(&s.conn, sid)?;
            if current["status"] != "paused" {
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
    Ok(())
}

impl App {
    pub async fn shutdown(&self) -> Result<()> {
        self.db
            .call(|s| {
                s.closing = true;
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
                    for c in s.active.values() {
                        c.hard.cancel();
                    }
                    Ok(())
                })
                .await?;
            self.workers.wait().await;
        }
        Ok(())
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
                    let config = Config::load(&tx)?
                        .ok_or_else(|| Error::conflict("model configuration missing"))?;
                    let (effort, needed) = tx.query_row(
                        "SELECT reasoning_effort, needs_model FROM sessions WHERE id=?",
                        [sid],
                        |r| Ok((r.get(0)?, r.get::<_, bool>(1)?)),
                    )?;
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
                    tx.commit()?;
                    Ok(Some(Input {
                        config,
                        effort,
                        end,
                    }))
                })
                .await?;
            let Some(input) = input else {
                return Ok(());
            };
            attempt += 1;
            let mut output = model::Output::default();
            let streamed = tokio::select! {
                biased;
                _ = control.hard.cancelled() => Err(Failure::Fatal(Error::conflict("paused"))),
                result = self.stream(sid, input, &mut output, &control.hard) => result,
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
                        let saved = if let Some(item) = output.final_item {
                            Some(store::add_context_item(
                                conn,
                                sid,
                                "assistant_message",
                                &item,
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
                                json!({"status":state,"reason":info["last_error"]}),
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
