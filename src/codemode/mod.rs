pub(crate) mod files;
pub(crate) mod io;
pub mod ledger;
mod live;
mod output;
mod process;
#[cfg(unix)]
pub(crate) use process::{observe_exit, reap_group};
mod streams;
mod vm;
#[cfg(windows)]
mod windows;

use crate::{
    App, Error, Result,
    id::{ItemId, SessionId},
    store::{SessionStatus, Store, pending_reason, session},
    tools::Script,
};
use chrono::{DateTime, Utc};
use live::{LiveState, ProgressFingerprint};
pub(crate) use live::{CommandGuard, CommandPreview, McpGuard, RemoteCommandProgress, Stream};
pub(crate) use output::TASK_BYTES;
pub use output::{Blocks, Part};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
pub(crate) use streams::{Resource as StreamResource, Reservation as StreamReservation};
use tokio::{
    runtime::Handle,
    sync::{Notify, oneshot, watch},
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::task_tracker::TaskTrackerToken};

const FRONT_WAIT: Duration = Duration::from_secs(90);
const WAIT_LIMIT: Duration = Duration::from_secs(3600);
const LEASE: Duration = Duration::from_secs(7200);
const WARNINGS: [Duration; 2] = [Duration::from_secs(3600), Duration::from_secs(900)];
const KILL_WAIT: Duration = Duration::from_secs(3);
const MAX_TASKS: usize = 256;
const MAX_SESSION_TASKS: usize = 32;

pub struct Engine {
    pool: vm::Pool,
    home_dir: Option<PathBuf>,
    vm_counts: Mutex<HashMap<SessionId, usize>>,
    client: reqwest::Client,
}
impl Engine {
    pub fn new(threads: usize, client: reqwest::Client) -> Result<Self> {
        Ok(Self {
            pool: vm::Pool::new(threads)?,
            home_dir: std::env::home_dir().filter(|path| path.is_absolute()),
            vm_counts: Mutex::default(),
            client,
        })
    }

    pub(crate) fn home_dir(&self) -> Option<&Path> {
        self.home_dir.as_deref()
    }

    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

#[derive(Debug)]
pub struct HostError {
    message: String,
    code: Option<Cow<'static, str>>,
}
impl HostError {
    pub(crate) fn coded(code: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some(code.into()),
        }
    }
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
    pub(crate) fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
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
            code: Some(Cow::Borrowed(code)),
        }
    }
}

pub struct Reply {
    pub value: Value,
    pub data: Option<Vec<u8>>,
}
impl Reply {
    pub(crate) fn value(value: Value) -> Self {
        Self { value, data: None }
    }
}
pub struct Outcome {
    pub error: Option<String>,
    pub output: Blocks,
}
impl Outcome {
    fn failed(error: String) -> Self {
        Self {
            error: Some(error),
            output: Blocks::default(),
        }
    }
}
// A trigger has no Task entry. Count the VM until the worker finishes cleaning up,
// even if a hard pause has already released the step that submitted it.
struct VmSlot {
    engine: Arc<Engine>,
    sid: SessionId,
    _worker: TaskTrackerToken,
}
impl VmSlot {
    fn reserve(app: &App, sid: SessionId) -> std::result::Result<Self, String> {
        let mut counts = app.codemode.vm_counts.lock().unwrap();
        if counts.values().sum::<usize>() >= MAX_TASKS
            || counts.get(&sid).copied().unwrap_or(0) >= MAX_SESSION_TASKS
        {
            return Err("task capacity reached".into());
        }
        *counts.entry(sid).or_default() += 1;
        Ok(Self {
            engine: app.codemode.clone(),
            sid,
            _worker: app.workers.token(),
        })
    }
}
impl Drop for VmSlot {
    fn drop(&mut self) {
        let mut counts = self.engine.vm_counts.lock().unwrap();
        let count = counts.get_mut(&self.sid).expect("reserved VM slot");
        *count -= 1;
        if *count == 0 {
            counts.remove(&self.sid);
        }
    }
}

pub struct Job {
    _slot: VmSlot,
    pub code: String,
    pub cancel: CancellationToken,
    pub host: Host,
    pub done: oneshot::Sender<Outcome>,
}

#[derive(Clone)]
pub struct Host {
    pub(crate) live: Arc<Mutex<LiveState>>,
    pub(crate) app: App,
    pub(crate) sid: SessionId,
    pub(crate) origin: ItemId,
    trigger: bool,
    last_task_query: Arc<tokio::sync::Mutex<Option<Instant>>>,
    deadline: watch::Receiver<Instant>,
    pub client: reqwest::Client,
    pub cancel: CancellationToken,
    pub executor: Handle,
    pub(crate) streams: Arc<streams::Streams>,
}
impl Host {
    fn new(
        app: &App,
        call: &Call,
        deadline: watch::Receiver<Instant>,
        cancel: CancellationToken,
        trigger: bool,
    ) -> Self {
        Self {
            live: Arc::default(),
            app: app.clone(),
            client: app.codemode.client.clone(),
            sid: call.sid,
            origin: call.id,
            trigger,
            last_task_query: Arc::default(),
            cancel,
            deadline,
            executor: Handle::current(),
            streams: Arc::default(),
        }
    }
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
    pub async fn emit(&self, blocks: Blocks) -> std::result::Result<ItemId, String> {
        if self.trigger {
            return Err("emitEvent is not available in trigger_code; return from trigger_code to finish waiting".into());
        }
        let sid = self.sid;
        let origin = self.origin;
        let app = self.app.clone();
        self.app
            .db
            .call(move |s| {
                let task = s
                    .tasks
                    .get(&origin)
                    .ok_or_else(|| Error::conflict("task no longer active"))?;
                if task.reason.is_some() || task.expired() || task.cancel.is_cancelled() {
                    return Err(Error::conflict("task stopped or expired"));
                }
                if task_superseded(&s.conn, s.reset_gen(sid), task)? || !s.is_local_lease_valid(sid) {
                    return Err(Error::conflict("session superseded or conflicted"));
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
                let task = s.tasks.get_mut(&origin).expect("task checked above");
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
    pub sid: SessionId,
    pub id: ItemId,
    pub call: String,
}
impl Call {
    fn save(&self, conn: &Connection, output: &Value, background: bool) -> Result<ItemId> {
        ledger::tool(conn, self.sid, self.id, &self.call, output, background)
    }
    pub fn reply(&self, store: &mut Store, body: Value) -> Result<()> {
        let id = self.save(&store.conn, &Value::String(body.to_string()), false)?;
        notice(store, self.sid, id);
        Ok(())
    }
}
fn notice(s: &mut Store, sid: SessionId, id: ItemId) {
    s.wake_waiters(sid);
    if let Err(e) = s.item_saved(sid, id) {
        tracing::warn!(%sid, %id, error=%e, "item notification failed after commit");
    }
}
fn event_saved(s: &mut Store, app: &App, sid: SessionId, id: ItemId, notify: bool) {
    notice(s, sid, id);
    if notify && !s.closing && !s.active.contains_key(&sid) && s.presence.remote_owner(sid).is_none() {
        s.pending_wakes.insert(sid);
        s.refresh_local_presence(sid);
        app.wake(sid);
    }
}

struct KillWait {
    call: Call,
    committed: oneshot::Sender<()>,
    until: Instant,
}
pub struct Task {
    last_status_fingerprint: Option<ProgressFingerprint>,
    live: Arc<Mutex<LiveState>>,
    origin: Call,
    session_reset_gen: u64,
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
            "id": self.origin.id.task_id(),
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
            "Do independent work while this task runs. Call codemode_wait with {} to wait for its events or result in this turn (do not poll in another codemode call), or end your turn to hand control back—future task events and completion automatically wake an idle session."
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

pub fn stop_session(s: &mut Store, sid: SessionId) {
    for t in s.tasks.values_mut().filter(|t| t.origin.sid == sid) {
        t.stop("killed");
    }
}

pub fn has_session_tasks(s: &Store, sid: SessionId) -> bool {
    s.task_counts.contains_key(&sid)
}

pub fn task_session_ids(s: &Store) -> impl Iterator<Item = SessionId> + '_ {
    s.task_counts.keys().copied()
}

pub fn count_session_tasks(s: &Store, sid: SessionId) -> usize {
    s.task_counts.get(&sid).copied().unwrap_or(0)
}

pub fn session_tasks_snapshot(s: &Store, sid: SessionId) -> Value {
    let mut tasks: Vec<_> = s.tasks.values().filter(|t| t.origin.sid == sid).collect();
    tasks.sort_unstable_by_key(|t| t.origin.id);
    Value::Array(
        tasks
            .into_iter()
            .map(|t| {
                let mut info = t.info();
                t.live.lock().unwrap().snapshot(&mut info, true);
                info
            })
            .collect(),
    )
}

pub fn stop_task(s: &mut Store, sid: SessionId, id: ItemId) -> Option<Value> {
    let task = s.tasks.get_mut(&id).filter(|t| t.origin.sid == sid)?;
    task.stop(if task.expired() { "expired" } else { "killed" });
    Some(json!({"id": id.task_id(), "status": "stopping"}))
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

fn task_list(s: &Store, sid: SessionId) -> Value {
    let mut tasks: Vec<_> = s.tasks.values().filter(|t| t.origin.sid == sid).collect();
    tasks.sort_unstable_by_key(|t| t.origin.id);
    json!({"tasks": tasks.into_iter().map(Task::info).collect::<Vec<_>>()})
}

pub async fn list(app: App, call: Call) -> Result<()> {
    app.db
        .call(move |s| call.reply(s, task_list(s, call.sid)))
        .await
}

fn full_status(s: &Store, sid: SessionId, id: ItemId) -> Result<Value> {
    if let Some(task) = owned(s, sid, id) {
        let mut info = task.info();
        task.live.lock().unwrap().snapshot(&mut info, true);
        Ok(info)
    } else {
        ledger::task_status(&s.conn, sid, id, false)
    }
}

fn owned(s: &Store, sid: SessionId, id: ItemId) -> Option<&Task> {
    s.tasks.get(&id).filter(|t| t.origin.sid == sid)
}
fn inactive(call: &Call, s: &mut Store, id: ItemId) -> Result<()> {
    call.reply(s, json!({"id":id.task_id(),"status":"not_active"}))
}

pub async fn status(app: App, call: Call, id: ItemId, hard: CancellationToken) -> Result<()> {
    app.db
        .call(move |s| {
            if hard.is_cancelled() {
                return Ok(());
            }
            let Some(task) = s.tasks.get(&id).filter(|t| t.origin.sid == call.sid) else {
                let body = ledger::task_status(&s.conn, call.sid, id, true)?;
                return call.reply(s, body);
            };
            let mut info = task.info();
            let mut live = task.live.lock().unwrap();
            let fingerprint = live.fingerprint(task.reason.is_some(), task.background, *task.deadline.borrow());
            let unchanged = task.last_status_fingerprint.as_ref() == Some(&fingerprint);
            live.snapshot(&mut info, !unchanged);
            drop(live);
            if unchanged {
                info["unchanged_since_last_status"] = json!(true);
                info["message"] = json!("No new output or state change since the last status check. Call codemode_wait ({} for completion/events, or with trigger_code for live output) instead of polling codemode_task_status.");
            }
            call.reply(s, info)?;
            s.tasks.get_mut(&id).unwrap().last_status_fingerprint = Some(fingerprint);
            Ok(())
        })
        .await
}

struct Wait {
    call: Call,
    start: Instant,
    has_trigger: bool,
    hard: CancellationToken,
}

enum WaitPoll {
    Done,
    Cancel(Value),
    Listen {
        wake: watch::Receiver<u64>,
        soft: CancellationToken,
        had_tasks: bool,
    },
}

impl Wait {
    // The queue check, subscription and any winning result all use the SQLite thread.
    fn poll(
        &self,
        s: &mut Store,
        had_tasks: Option<bool>,
        running: bool,
        outcome: Option<Outcome>,
    ) -> Result<WaitPoll> {
        if self.hard.is_cancelled() {
            return Ok(WaitPoll::Done);
        }
        let sid = self.call.sid;
        let waited_ms = self.start.elapsed().as_millis();
        let control = s
            .active
            .get(&sid)
            .filter(|c| !s.closing && !c.soft.is_cancelled());
        let reply = if let Some(control) = control {
            if let Some(reason) = pending_reason(&s.conn, sid)? {
                let message = match reason {
                    "task_event" => "Queued task event(s) will be delivered in the next step.",
                    "user_message" => "Queued user message(s) will be delivered in the next step.",
                    _ => "Queued user message(s) and task event(s) will be delivered in the next step.",
                };
                json!({
                    "status": "woke",
                    "reason": reason,
                    "waited_ms": if had_tasks.is_none() { 0 } else { waited_ms },
                    "message": message,
                })
            } else {
                let active = count_session_tasks(s, sid);
                let has_tasks = active > 0 || ledger::starting_sibling(&s.conn, sid, self.call.id)?;
                if !has_tasks && had_tasks == Some(true) {
                    json!({"status":"woke", "reason":"no_active_tasks", "waited_ms":waited_ms})
                } else if !has_tasks && !self.has_trigger {
                    json!({"status":"error", "error":"no active codemode tasks in this session"})
                } else if self.start.elapsed() >= WAIT_LIMIT {
                    json!({"status":"timeout", "waited_ms":waited_ms, "active_tasks":active})
                } else if let Some(outcome) = outcome {
                    self.save_trigger(s, outcome)?;
                    return Ok(WaitPoll::Done);
                } else {
                    let soft = control.soft.clone();
                    return Ok(WaitPoll::Listen {
                        wake: s.subscribe_wait(sid),
                        soft,
                        had_tasks: had_tasks.unwrap_or(has_tasks),
                    });
                }
            }
        } else {
            json!({"status":"interrupted", "reason":"session pausing", "waited_ms":waited_ms})
        };
        if running {
            return Ok(WaitPoll::Cancel(reply));
        }
        self.call.reply(s, reply)?;
        Ok(WaitPoll::Done)
    }

    fn save_trigger(&self, s: &mut Store, outcome: Outcome) -> Result<()> {
        let sid = self.call.sid;
        let tx = s.conn.transaction()?;
        let (parts, error) = match ledger::materialize(&tx, sid, &outcome.output) {
            Ok(Value::Array(parts)) => (parts, outcome.error),
            Ok(_) => return Err(Error::internal("expected codemode result blocks")),
            Err(e) if e.0.is_client_error() => (Vec::new(), Some(e.1)),
            Err(e) => return Err(e),
        };
        let waited_ms = self.start.elapsed().as_millis();
        let summary = match error {
            Some(error) => {
                json!({"status":"error", "reason":"trigger_failed", "waited_ms":waited_ms, "error":error})
            }
            None => json!({"status":"woke", "reason":"trigger", "waited_ms":waited_ms}),
        };
        let (content, text) = pack_output(summary, parts, true);
        let id = self
            .call
            .save(&tx, text.as_ref().unwrap_or(&content), false)?;
        tx.commit()?;
        notice(s, sid, id);
        Ok(())
    }

    fn submit_trigger(
        &self,
        app: &App,
        code: String,
        cancel: CancellationToken,
    ) -> std::result::Result<oneshot::Receiver<Outcome>, String> {
        let (_, lease) = watch::channel(self.start + WAIT_LIMIT);
        let slot = VmSlot::reserve(app, self.call.sid)?;
        let host = Host::new(app, &self.call, lease, cancel.clone(), true);
        let (done, finish) = oneshot::channel();
        app.codemode.pool.submit(Job {
            _slot: slot,
            code,
            host,
            cancel,
            done,
        })?;
        Ok(finish)
    }
}

pub async fn wait(
    app: App,
    call: Call,
    mut code: Option<String>,
    hard: CancellationToken,
) -> Result<()> {
    let wait = Arc::new(Wait {
        call,
        start: Instant::now(),
        has_trigger: code.is_some(),
        hard: hard.clone(),
    });
    let deadline = wait.start + WAIT_LIMIT;
    let cancel = hard.child_token();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let mut had_tasks = None;
    let mut finish = None;
    let mut outcome = None;
    loop {
        let check = wait.clone();
        let running = finish.is_some();
        let result = outcome.take();
        let poll = app
            .db
            .call(move |s| check.poll(s, had_tasks, running, result))
            .await?;
        let (mut wake, soft) = match poll {
            WaitPoll::Done => return Ok(()),
            WaitPoll::Cancel(reply) => {
                cancel.cancel();
                tokio::select! {
                    biased;
                    _ = hard.cancelled() => return Ok(()),
                    _ = finish.take().expect("trigger running") => (),
                }
                return app
                    .db
                    .call(move |s| {
                        if hard.is_cancelled() {
                            Ok(())
                        } else {
                            wait.call.reply(s, reply)
                        }
                    })
                    .await;
            }
            WaitPoll::Listen {
                wake,
                soft,
                had_tasks: initial,
            } => {
                had_tasks = Some(initial);
                (wake, soft)
            }
        };
        if let Some(code) = code.take() {
            match wait.submit_trigger(&app, code, cancel.clone()) {
                Ok(rx) => finish = Some(rx),
                Err(error) => {
                    outcome = Some(Outcome::failed(error));
                    continue;
                }
            }
        }
        tokio::select! {
            biased;
            _ = hard.cancelled() => return Ok(()),
            _ = soft.cancelled() => (),
            _ = tokio::time::sleep_until(deadline) => (),
            result = async {
                match &mut finish {
                    Some(rx) => rx.await,
                    None => std::future::pending().await,
                }
            } => {
                finish = None;
                outcome = Some(result.unwrap_or_else(|_| Outcome::failed("JS worker exited before delivering outcome".into())));
            }
            _ = wake.changed() => (),
        }
    }
}

pub async fn renew(
    app: App,
    call: Call,
    id: ItemId,
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
            if task_superseded(&s.conn, s.reset_gen(call.sid), task)? {
                return Err(Error::conflict("session superseded"));
            }
            let duration = Duration::from_secs(seconds.into());
            let expires = task.started_at + (now + duration - task.created);
            let response = json!({
                "id": id.task_id(), "status":"renewed",
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

pub async fn kill(app: App, call: Call, id: ItemId, hard: CancellationToken) -> Result<()> {
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
                return call.reply(s, json!({"status":"stopping","id":id.task_id()}));
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
            || count_session_tasks(s, call.sid) >= MAX_SESSION_TASKS
        {
            return call.reply(
                s,
                json!({"status":"error","error":"task capacity reached or daemon closing"}),
            );
        }
        let deleted: bool = s
            .conn
            .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 1)")?
            .query_row([call.sid], |r| r.get(0))?;
        if deleted {
            return call.reply(
                s,
                json!({"status":"error","error":"session deleted or missing"}),
            );
        }
        let session_reset_gen = s.reset_gen(call.sid);
        let now = Instant::now();
        let (deadline, lease) = watch::channel(now + LEASE);
        let cancel = CancellationToken::new();
        let id = call.id;
        let sid = call.sid;
        let changed = Arc::new(Notify::new());
        let slot = match VmSlot::reserve(&app, sid) {
            Ok(slot) => slot,
            Err(error) => return call.reply(s, json!({"status":"error","error":error})),
        };
        let host = Host::new(&app, &call, lease, cancel.clone(), false);
        let mut task = Task {
            last_status_fingerprint: None,
            live: host.live.clone(),
            origin: call,
            session_reset_gen,
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
        if s.tasks.insert(id, task).is_none() {
            *s.task_counts.entry(sid).or_default() += 1;
        }
        s.refresh_local_presence(sid);
        s.wake_waiters(sid);
        let submitted = app.codemode.pool.submit(Job {
            _slot: slot,
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

fn task_superseded(conn: &Connection, reset_gen: u64, task: &Task) -> Result<bool> {
    if reset_gen != task.session_reset_gen {
        return Ok(true);
    }
    let deleted: bool = conn
        .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 1)")?
        .query_row([task.origin.sid], |r| r.get(0))?;
    Ok(deleted)
}

fn tick(s: &mut Store, app: &App, id: ItemId) -> Result<Option<Instant>> {
    let sid = s.tasks[&id].origin.sid;
    let cur_gen = s.reset_gen(sid);
    if task_superseded(&s.conn, cur_gen, &s.tasks[&id])? || !s.is_local_lease_valid(sid) {
        s.tasks.get_mut(&id).unwrap().stop("superseded");
        return Ok(None);
    }
    let now = Instant::now();
    if s.tasks[&id].kill.as_ref().is_some_and(|k| now >= k.until) {
        let saved = {
            let k = s.tasks[&id].kill.as_ref().unwrap();
            k.call.save(
                &s.conn,
                &Value::String(json!({"status":"stopping","id":id.task_id()}).to_string()),
                false,
            )?
        };
        let task = s.tasks.get_mut(&id).unwrap();
        let _ = task.kill.take().unwrap().committed.send(());
        notice(s, sid, saved);
    }
    let receipt = {
        let task = &s.tasks[&id];
        (task.can_detach(s.closing) && now >= task.created + FRONT_WAIT).then(|| task.receipt())
    };
    if let Some(receipt) = receipt {
        let saved = s.tasks[&id].origin.save(&s.conn, &receipt, true)?;
        let task = s.tasks.get_mut(&id).unwrap();
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
        s.tasks.get_mut(&id).unwrap().warning += crossed;
        event_saved(s, app, sid, saved, true);
    }
    Ok(s.tasks[&id].next_timer(s.closing))
}

async fn supervise(
    app: App,
    id: ItemId,
    changed: Arc<Notify>,
    mut finish: oneshot::Receiver<Outcome>,
    submitted: std::result::Result<(), String>,
) {
    let outcome = match submitted {
        Err(error) => Outcome::failed(error),
        Ok(()) => loop {
            let worker = app.clone();
            let timer = match app.db.call(move |s| tick(s, &worker, id)).await {
                Ok(timer) => timer,
                Err(e) => {
                    tracing::error!(%id, error=%e, "task timer transaction failed");
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
                outcome = &mut finish => break outcome.unwrap_or_else(|_| Outcome::failed("JS worker exited before delivering outcome".into())),
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
                tracing::error!(%id, error=%e, "task result transaction failed");
                if app.stopping.is_cancelled() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn pack_output(
    mut summary: Value,
    mut parts: Vec<Value>,
    for_tool: bool,
) -> (Value, Option<Value>) {
    let header = json!({"type":"text", "text":summary.to_string()});
    let text = (for_tool && parts.iter().all(|p| p["type"] == "text")).then(|| {
        summary["output"] = Value::String(
            parts
                .iter()
                .filter_map(|p| p["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        );
        Value::String(summary.to_string())
    });
    parts.insert(0, header);
    (Value::Array(parts), text)
}

fn remove_task(s: &mut Store, id: ItemId) -> Task {
    let task = s.tasks.remove(&id).unwrap();
    let sid = task.origin.sid;
    let count = s.task_counts.get_mut(&sid).expect("registered task count");
    *count -= 1;
    if *count == 0 {
        s.task_counts.remove(&sid);
    }
    task
}

fn complete(s: &mut Store, app: &App, id: ItemId, result: &Outcome) -> Result<()> {
    let (sid, cur_gen) = {
        let task = &s.tasks[&id];
        (task.origin.sid, s.reset_gen(task.origin.sid))
    };
    if task_superseded(&s.conn, cur_gen, &s.tasks[&id])? || !s.is_local_lease_valid(sid) {
        let mut task = remove_task(s, id);
        s.refresh_local_presence(sid);
        if !s.active.contains_key(&sid)
            && !s.pending_wakes.contains(&sid)
            && !has_session_tasks(s, sid)
            && let Ok(info) = session(&s.conn, sid)
        {
            s.notify_status(sid, info.status.as_str(), &info.last_error, info.updated_at);
        }
        s.wake_waiters(sid);
        task.delivered();
        if let Some(k) = task.kill {
            let _ = k.committed.send(());
        }
        return Ok(());
    }
    let task = &s.tasks[&id];
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
    let duration_ms = task.created.elapsed().as_millis();
    let mut summary = json!({
        "id": id.task_id(),
        "status": status,
        "duration_ms": duration_ms,
    });
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
    let parts = match materialized {
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
    let (content, text_output) =
        pack_output(summary, parts, original_pending || task.kill.is_some());
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
    let mut task = remove_task(s, id);
    s.wake_waiters(sid);
    task.delivered();
    if let Some(k) = task.kill {
        let _ = k.committed.send(());
    }
    for (item, wake) in saved {
        event_saved(s, app, sid, item, wake);
    }
    s.refresh_local_presence(sid);
    if !s.active.contains_key(&sid)
        && !s.pending_wakes.contains(&sid)
        && !has_session_tasks(s, sid)
        && let Ok(info) = session(&s.conn, sid)
    {
        s.notify_status(sid, info.status.as_str(), &info.last_error, info.updated_at);
    }
    Ok(())
}
