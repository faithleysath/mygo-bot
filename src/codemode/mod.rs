mod files;
mod io;
pub mod ledger;
mod live;
mod output;
mod process;
mod streams;
mod vm;
#[cfg(windows)]
mod windows;

use crate::{
    App, Error, Result,
    store::{SessionStatus, Store, has_pending, session},
    tools::Script,
};
use chrono::{DateTime, Utc};
use live::LiveState;
pub use output::{Blocks, Part};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{Notify, oneshot, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

const FRONT_WAIT: Duration = Duration::from_secs(90);
const LEASE: Duration = Duration::from_secs(7200);
const WARNINGS: [Duration; 2] = [Duration::from_secs(3600), Duration::from_secs(900)];
const KILL_WAIT: Duration = Duration::from_secs(3);
const MAX_TASKS: usize = 256;
const MAX_SESSION_TASKS: usize = 32;

pub struct Engine {
    pool: vm::Pool,
    client: reqwest::Client,
}
impl Engine {
    pub fn new(threads: usize) -> Result<Self> {
        let client = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(Self {
            pool: vm::Pool::new(threads)?,
            client,
        })
    }
}

#[derive(Debug)]
pub struct HostError {
    message: String,
    code: Option<&'static str>,
}
impl HostError {
    pub(super) fn context(mut self, context: String) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
}
impl From<String> for HostError {
    fn from(message: String) -> Self {
        Self {
            message,
            code: None,
        }
    }
}
impl From<&str> for HostError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<std::io::Error> for HostError {
    fn from(error: std::io::Error) -> Self {
        use std::io::ErrorKind::*;
        let code = match error.kind() {
            NotFound => "ENOENT",
            PermissionDenied => "EACCES",
            AlreadyExists => "EEXIST",
            NotADirectory => "ENOTDIR",
            IsADirectory => "EISDIR",
            DirectoryNotEmpty => "ENOTEMPTY",
            CrossesDevices => "EXDEV",
            ReadOnlyFilesystem => "EROFS",
            StorageFull => "ENOSPC",
            InvalidInput => "EINVAL",
            InvalidData => "EILSEQ",
            Unsupported => "ENOTSUP",
            FileTooLarge => "EFBIG",
            Interrupted => "EINTR",
            ResourceBusy => "EBUSY",
            QuotaExceeded => "EDQUOT",
            _ => "EIO",
        };
        Self {
            message: error.to_string(),
            code: Some(code),
        }
    }
}

pub struct Reply {
    pub value: Value,
    pub data: Option<Vec<u8>>,
}
impl Reply {
    fn value(value: Value) -> Self {
        Self { value, data: None }
    }
}
pub struct Outcome {
    pub error: Option<String>,
    pub output: Blocks,
}
pub struct Job {
    pub code: String,
    pub cancel: CancellationToken,
    pub host: Host,
    pub done: oneshot::Sender<Outcome>,
}

#[derive(Clone)]
pub struct Host {
    live: Arc<Mutex<LiveState>>,
    app: App,
    sid: i64,
    origin: i64,
    deadline: watch::Receiver<Instant>,
    pub client: reqwest::Client,
    pub cancel: CancellationToken,
    pub executor: Handle,
    streams: Arc<streams::Streams>,
}
impl Host {
    pub async fn io(
        &self,
        op: String,
        args: Value,
        data: Option<Vec<u8>>,
    ) -> std::result::Result<Reply, HostError> {
        if self.cancel.is_cancelled() || Instant::now() >= *self.deadline.borrow() {
            return Err("task stopped or expired".into());
        }
        io::execute(self, &op, args, data).await
    }
    pub async fn emit(&self, blocks: Blocks) -> std::result::Result<i64, String> {
        let sid = self.sid;
        let origin = self.origin;
        let app = self.app.clone();
        self.app
            .db
            .call(move |s| {
                let task = s
                    .tasks
                    .get_mut(&origin)
                    .ok_or_else(|| Error::conflict("task no longer active"))?;
                if task.reason.is_some() || task.expired() || task.cancel.is_cancelled() {
                    return Err(Error::conflict("task stopped or expired"));
                }
                if blocks.parts.is_empty() {
                    return Err(Error::bad("empty event"));
                }
                let now = Instant::now();
                // Burst of eight events; regain one token every second.
                let tokens =
                    (task.tokens + now.duration_since(task.refilled).as_secs_f64()).min(8.0);
                if tokens < 1.0 || ledger::pending_messages(&s.conn, sid)? >= 128 {
                    return Err(Error::conflict(
                        "backpressure: event rate or session queue limit reached",
                    ));
                }
                let tx = s.conn.transaction()?;
                let content = ledger::materialize(&tx, sid, &blocks)?;
                let id = ledger::event(&tx, sid, origin, "message", &content, true)?;
                tx.commit()?;
                task.tokens = tokens - 1.0;
                task.refilled = now;
                event_saved(s, &app, sid, id, true);
                Ok(id)
            })
            .await
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
pub struct Call {
    pub sid: i64,
    pub id: i64,
    pub call: String,
}
impl Call {
    fn save(&self, conn: &Connection, output: &Value, background: bool) -> Result<i64> {
        ledger::tool(conn, self.sid, self.id, &self.call, output, background)
    }
    pub fn reply(&self, store: &mut Store, body: Value) -> Result<()> {
        let id = self.save(&store.conn, &Value::String(body.to_string()), false)?;
        notice(store, self.sid, id);
        Ok(())
    }
}
fn notice(s: &mut Store, sid: i64, id: i64) {
    s.wake_waiters(sid);
    if let Err(e) = s.item_saved(sid, id) {
        tracing::warn!(sid,id,error=%e,"item notification failed after commit");
    }
}
fn event_saved(s: &mut Store, app: &App, sid: i64, id: i64, notify: bool) {
    notice(s, sid, id);
    if notify && !s.closing && !s.active.contains_key(&sid) {
        app.wake(sid);
    }
}

struct KillWait {
    call: Call,
    committed: oneshot::Sender<()>,
    until: Instant,
}
pub struct Task {
    live: Arc<Mutex<LiveState>>,
    origin: Call,
    label: String,
    created: Instant,
    started_at: DateTime<Utc>,
    deadline: watch::Sender<Instant>,
    warning: usize,
    background: bool,
    reason: Option<&'static str>,
    step_hard: CancellationToken,
    cancel: CancellationToken,
    changed: Arc<Notify>,
    foreground: Option<oneshot::Sender<()>>,
    kill: Option<KillWait>,
    tokens: f64,
    refilled: Instant,
}
impl Task {
    fn info(&self) -> Value {
        let deadline = *self.deadline.borrow();
        let expiry = self.started_at + deadline.duration_since(self.created);
        json!({
            "id": format!("cm_{}", self.origin.id),
            "label": self.label,
            "state": if self.reason.is_some() { "stopping" } else { "running" },
            "delivery": if self.background { "background" } else { "foreground" },
            "started_at": self.started_at.to_rfc3339(),
            "expires_at": expiry.to_rfc3339(),
            "remaining_seconds": deadline.saturating_duration_since(Instant::now()).as_secs_f64().ceil() as u64
        })
    }
    fn expired(&self) -> bool {
        Instant::now() >= *self.deadline.borrow()
    }
    fn lease(&mut self, now: Instant, duration: Duration) {
        self.deadline.send_replace(now + duration);
        self.warning = WARNINGS.iter().take_while(|t| **t >= duration).count();
        self.changed.notify_one();
    }
    fn stop(&mut self, reason: &'static str) {
        if self.reason.is_none() {
            self.reason = Some(reason);
            self.cancel.cancel();
            self.changed.notify_one();
        }
    }
    fn receipt(&self) -> Value {
        let mut body = self.info();
        body["delivery"] = json!("background");
        body["status"] = json!("background");
        body["message"] = json!(
            "Do independent work while this task runs. Use codemode_task_wait when you need its events or result. Finish your turn once the work is complete."
        );
        Value::String(body.to_string())
    }
    fn delivered(&mut self) {
        if let Some(tx) = self.foreground.take() {
            let _ = tx.send(());
        }
    }
    fn next_timer(&self, closing: bool) -> Option<Instant> {
        let mut next = self.kill.as_ref().map(|k| k.until);
        if self.reason.is_none() {
            let lease =
                *self.deadline.borrow() - WARNINGS.get(self.warning).copied().unwrap_or_default();
            next = Some(next.map_or(lease, |t| t.min(lease)));
        }
        if self.can_detach(closing) {
            let due = self.created + FRONT_WAIT;
            next = Some(next.map_or(due, |t| t.min(due)));
        }
        next
    }
    fn can_detach(&self, closing: bool) -> bool {
        !closing && !self.background && !self.step_hard.is_cancelled()
    }
}

pub fn stop_session(s: &mut Store, sid: i64) {
    for t in s.tasks.values_mut().filter(|t| t.origin.sid == sid) {
        t.stop("killed");
    }
}
pub fn shutdown_tasks(s: &mut Store, hard: bool) -> Result<()> {
    let mut paused = std::collections::BTreeSet::new();
    for t in s.tasks.values_mut() {
        if t.background || hard {
            t.stop("interrupted");
            if t.background {
                paused.insert(t.origin.sid);
            }
        }
        t.changed.notify_one();
    }
    for sid in paused {
        if session(&s.conn, sid)?.status == SessionStatus::Idle {
            s.status(sid, "paused", "background task interrupted by shutdown")?;
        }
    }
    Ok(())
}

pub async fn list(app: App, call: Call) -> Result<()> {
    app.db
        .call(move |s| {
            let mut tasks: Vec<_> = s
                .tasks
                .values()
                .filter(|t| t.origin.sid == call.sid)
                .collect();
            tasks.sort_unstable_by_key(|t| t.origin.id);
            let tasks: Vec<_> = tasks.into_iter().map(Task::info).collect();
            call.reply(s, json!({"tasks":tasks}))
        })
        .await
}

fn owned(s: &Store, sid: i64, id: i64) -> Option<&Task> {
    s.tasks.get(&id).filter(|t| t.origin.sid == sid)
}
fn inactive(call: &Call, s: &mut Store, id: i64) -> Result<()> {
    call.reply(s, json!({"id":format!("cm_{id}"),"status":"not_active"}))
}

pub async fn status(app: App, call: Call, id: i64, hard: CancellationToken) -> Result<()> {
    app.db
        .call(move |s| {
            if hard.is_cancelled() {
                return Ok(());
            }
            let body = if let Some(task) = owned(s, call.sid, id) {
                let mut info = task.info();
                task.live.lock().unwrap().snapshot(&mut info);
                info
            } else {
                ledger::task_status(&s.conn, call.sid, id)?
            };
            call.reply(s, body)
        })
        .await
}

pub async fn wait(app: App, call: Call, seconds: u32, hard: CancellationToken) -> Result<()> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds.into());
    let mut first = true;
    loop {
        let call = call.clone();
        let hard_check = hard.clone();
        let waiting = app.db.call(move |s| {
            if hard_check.is_cancelled() {
                return Ok(None);
            }
            let sid = call.sid;
            let waited_ms = start.elapsed().as_millis();
            let Some(control) = s.active.get(&sid).filter(|c| !s.closing && !c.soft.is_cancelled()) else {
                call.reply(s, json!({"status":"interrupted", "reason":"session pausing", "waited_ms":waited_ms}))?;
                return Ok(None);
            };
            let reply = if has_pending(&s.conn, sid)? {
                json!({"status":"woke", "reason":"pending_input", "waited_ms":waited_ms})
            } else {
                let active = s.tasks.values().filter(|task| task.origin.sid == sid).count();
                if !first && Instant::now() >= deadline {
                    json!({"status":"timeout", "waited_ms":waited_ms, "active_tasks":active})
                } else if active > 0 || ledger::starting_sibling(&s.conn, sid, call.id)? {
                    // The queue check and subscription share the SQLite thread.
                    let soft = control.soft.clone();
                    return Ok(Some((s.subscribe_wait(sid), soft)));
                } else if first {
                    json!({"status":"error", "error":"no active codemode tasks in this session"})
                } else {
                    json!({"status":"woke", "reason":"no_active_tasks", "waited_ms":waited_ms})
                }
            };
            call.reply(s, reply)?;
            Ok(None)
        }).await?;
        let Some((mut rx, soft)) = waiting else {
            return Ok(());
        };
        tokio::select! {
            biased;
            _ = hard.cancelled() => return Ok(()),
            _ = soft.cancelled() => (),
            _ = tokio::time::sleep_until(deadline) => (),
            _ = rx.changed() => (),
        }
        first = false;
    }
}

pub async fn renew(
    app: App,
    call: Call,
    id: i64,
    seconds: u32,
    hard: CancellationToken,
) -> Result<()> {
    app.db
        .call(move |s| {
            if hard.is_cancelled() {
                return Ok(());
            }
            let Some(task) = owned(s, call.sid, id) else {
                return inactive(&call, s, id);
            };
            let now = Instant::now();
            if s.closing || task.reason.is_some() || now >= *task.deadline.borrow() {
                return call.reply(
                    s,
                    json!({"status":"error", "error":"task is stopping or expired"}),
                );
            }
            let duration = Duration::from_secs(seconds.into());
            let expires = task.started_at + (now + duration - task.created);
            let response = json!({
                "id": format!("cm_{id}"), "status":"renewed",
                "expires_at": expires.to_rfc3339(), "remaining_seconds": seconds
            });
            let tx = s.conn.transaction()?;
            ledger::invalidate(&tx, call.sid, id)?;
            let saved = call.save(&tx, &Value::String(response.to_string()), false)?;
            tx.commit()?;
            s.tasks.get_mut(&id).unwrap().lease(now, duration);
            notice(s, call.sid, saved);
            Ok(())
        })
        .await
}

pub async fn kill(app: App, call: Call, id: i64, hard: CancellationToken) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    app.db
        .call(move |s| {
            if hard.is_cancelled() {
                return Ok(());
            }
            let Some(task) = s.tasks.get_mut(&id).filter(|t| t.origin.sid == call.sid) else {
                return inactive(&call, s, id);
            };
            if task.reason.is_some() {
                return call.reply(s, json!({"status":"stopping","id":format!("cm_{id}")}));
            }
            task.stop(if task.expired() { "expired" } else { "killed" });
            task.kill = Some(KillWait {
                call,
                committed: tx,
                until: Instant::now() + KILL_WAIT,
            });
            Ok(())
        })
        .await?;
    let _ = rx.await;
    Ok(())
}

pub async fn run(app: App, call: Call, script: Script, hard: CancellationToken) -> Result<()> {
    let (ack, wait) = oneshot::channel();
    let control = hard.clone();
    let db = app.db.clone();
    db.call(move |s| {
        if control.is_cancelled() {
            return Ok(());
        }
        if s.closing
            || s.tasks.len() >= MAX_TASKS
            || s.tasks
                .values()
                .filter(|t| t.origin.sid == call.sid)
                .count()
                >= MAX_SESSION_TASKS
        {
            return call.reply(
                s,
                json!({"status":"error","error":"task capacity reached or daemon closing"}),
            );
        }
        let now = Instant::now();
        let (deadline, lease) = watch::channel(now + LEASE);
        let cancel = CancellationToken::new();
        let id = call.id;
        let sid = call.sid;
        let changed = Arc::new(Notify::new());
        let live = Arc::new(Mutex::new(LiveState::default()));
        let mut task = Task {
            live: live.clone(),
            origin: call,
            label: script.label,
            created: now,
            started_at: Utc::now(),
            deadline,
            warning: 0,
            background: script.background,
            reason: None,
            step_hard: control,
            cancel: cancel.clone(),
            changed: changed.clone(),
            foreground: Some(ack),
            kill: None,
            tokens: 8.0,
            refilled: now,
        };
        if task.background {
            let saved = task.origin.save(&s.conn, &task.receipt(), true)?;
            notice(s, sid, saved);
            task.delivered();
        }
        let (done, finish) = oneshot::channel();
        let host = Host {
            live,
            app: app.clone(),
            client: app.codemode.client.clone(),
            sid,
            origin: id,
            cancel: cancel.clone(),
            deadline: lease,
            executor: Handle::current(),
            streams: Arc::default(),
        };
        s.tasks.insert(id, task);
        s.wake_waiters(sid);
        let submitted = app.codemode.pool.submit(Job {
            code: script.code,
            cancel,
            host,
            done,
        });
        let tracker = app.workers.clone();
        tracker.spawn(supervise(app, id, changed, finish, submitted));
        Ok(())
    })
    .await?;
    // A hard pause may publish the existing "result unknown" while cleanup continues.
    tokio::select! {_=wait=>{},_=hard.cancelled()=>{}};
    Ok(())
}

fn tick(s: &mut Store, app: &App, id: i64) -> Result<Option<Instant>> {
    let task = s.tasks.get_mut(&id).expect("supervisor owns task");
    let now = Instant::now();
    let sid = task.origin.sid;
    if task.kill.as_ref().is_some_and(|k| now >= k.until) {
        let k = task.kill.as_ref().unwrap();
        let saved = k.call.save(
            &s.conn,
            &Value::String(json!({"status":"stopping","id":format!("cm_{id}")}).to_string()),
            false,
        )?;
        let _ = task.kill.take().unwrap().committed.send(());
        notice(s, sid, saved);
    }
    let task = s.tasks.get_mut(&id).unwrap();
    if task.can_detach(s.closing) && now >= task.created + FRONT_WAIT {
        let saved = task.origin.save(&s.conn, &task.receipt(), true)?;
        task.background = true;
        task.delivered();
        notice(s, sid, saved);
    }
    let task = s.tasks.get_mut(&id).unwrap();
    if task.expired() {
        task.stop("expired");
    }
    if task.reason.is_some() {
        return Ok(task.next_timer(s.closing));
    }
    let deadline = *task.deadline.borrow();
    let crossed = WARNINGS[task.warning..]
        .iter()
        .take_while(|t| now >= deadline - **t)
        .count();
    if crossed > 0 {
        let mut info = task.info();
        info["message"] = json!(
            "Lease expiring. Renew with codemode_task_renew or stop with codemode_task_kill."
        );
        info["threshold_seconds"] = json!(WARNINGS[task.warning + crossed - 1].as_secs());
        let content = json!([{"type":"text","text":info.to_string()}]);
        let saved = ledger::event(&s.conn, sid, id, "lease_warning", &content, true)?;
        task.warning += crossed;
        event_saved(s, app, sid, saved, true);
    }
    Ok(s.tasks[&id].next_timer(s.closing))
}

async fn supervise(
    app: App,
    id: i64,
    changed: Arc<Notify>,
    mut finish: oneshot::Receiver<Outcome>,
    submitted: std::result::Result<(), String>,
) {
    let outcome = match submitted {
        Err(error) => Outcome {
            error: Some(error),
            output: Blocks::default(),
        },
        Ok(()) => loop {
            let worker = app.clone();
            let timer = match app.db.call(move |s| tick(s, &worker, id)).await {
                Ok(timer) => timer,
                Err(e) => {
                    tracing::error!(id,error=%e,"task timer transaction failed");
                    Some(Instant::now() + Duration::from_secs(1))
                }
            };
            let alarm = async {
                match timer {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                outcome = &mut finish => break outcome.unwrap_or_else(|_| Outcome {
                    error: Some("JS worker exited before delivering outcome".into()),
                    output: Blocks::default(),
                }),
                _ = changed.notified() => (),
                _ = alarm => (),
            }
        },
    };
    let outcome = Arc::new(outcome);
    loop {
        let worker = app.clone();
        let result = outcome.clone();
        match app
            .db
            .call(move |s| complete(s, &worker, id, &result))
            .await
        {
            Ok(()) => break,
            Err(e) => {
                tracing::error!(id,error=%e,"task result transaction failed");
                if app.stopping.is_cancelled() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn complete(s: &mut Store, app: &App, id: i64, result: &Outcome) -> Result<()> {
    let task = &s.tasks[&id];
    let sid = task.origin.sid;
    let tx = s.conn.transaction()?;
    let status = task.reason.unwrap_or_else(|| {
        if task.expired() {
            "expired"
        } else if result.error.is_some() {
            "failed"
        } else {
            "succeeded"
        }
    });
    let mut summary = json!({"id":format!("cm_{id}"),"status":status});
    if let Some(e) = &result.error {
        summary["error"] = json!(e);
    }
    // A hard pause can settle the original call while the VM is still being reclaimed.
    let original_pending = !task.background && ledger::tool_id(&tx, sid, id)?.is_none();
    let materialized = if task.background || original_pending || task.kill.is_some() {
        ledger::materialize(&tx, sid, &result.output)
    } else {
        Ok(Value::Array(Vec::new()))
    };
    let mut parts = match materialized {
        Ok(Value::Array(parts)) => parts,
        Ok(_) => return Err(Error::internal("expected codemode result blocks")),
        Err(e) if e.0.is_client_error() => {
            if status == "succeeded" {
                summary["status"] = json!("failed");
            }
            summary["error"] = json!(e.1);
            Vec::new()
        }
        Err(e) => return Err(e),
    };
    let mut saved = Vec::with_capacity(2);
    let header = json!({"type":"text", "text":summary.to_string()});
    let mut text_output = None;
    if (original_pending || task.kill.is_some()) && parts.iter().all(|p| p["type"] == "text") {
        let text = parts
            .iter()
            .filter_map(|p| p["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        summary["output"] = Value::String(text);
        text_output = Some(Value::String(summary.to_string()));
    }
    parts.insert(0, header);
    let content = Value::Array(parts);
    let wire = text_output.as_ref().unwrap_or(&content);
    ledger::invalidate(&tx, sid, id)?;
    if task.background {
        let notify = task.kill.is_none();
        let completed = ledger::event(&tx, sid, id, "completed", &content, notify)?;
        saved.push((completed, notify));
    } else if original_pending {
        saved.push((task.origin.save(&tx, wire, false)?, false));
    }
    if let Some(k) = &task.kill {
        saved.push((k.call.save(&tx, wire, false)?, false));
    }
    tx.commit()?;
    let mut task = s.tasks.remove(&id).unwrap();
    s.wake_waiters(sid);
    task.delivered();
    if let Some(k) = task.kill {
        let _ = k.committed.send(());
    }
    for (item, wake) in saved {
        event_saved(s, app, sid, item, wake);
    }
    Ok(())
}
