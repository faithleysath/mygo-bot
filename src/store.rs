use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    path::Path,
    sync::Arc,
    thread,
};

use axum::body::Bytes;
use reqwest::Body;
use rusqlite::{
    Connection, OptionalExtension, ToSql, params,
    types::{FromSql, ValueRef},
};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{
    Error, Result,
    agent::Control,
    cluster::{
        ClusterSignal, SyncSignal,
        exec::{PresenceState, SessionOccupancy, merge_remote_attachment_refs},
        ledger::{
            AttachmentRef, ContextPos, ItemSnapshot, clear_staged, has_conflict, is_session_writable_local,
            record_call, record_session_delta, returning_item_snapshot, take_staged, with_savepoint,
        },
    },
    events::Notice,
    id::{CallId, ItemId, ModelId, SessionId},
};

// One owner serializes transactions and the matching in-memory worker transitions.
// A slow disk never blocks the HTTP executor. No job holds SQLite across network I/O.
type Acknowledge = Box<dyn FnOnce(&mut Store, &Result<()>)>;
type ApplyWrite = Box<dyn FnOnce(Result<&Connection>, bool) -> (bool, Acknowledge) + Send>;

struct Write {
    start_session: Option<SessionId>,
    expected_run_gen: Option<(SessionId, u64)>,
    apply: ApplyWrite,
}

impl Write {
    fn can_join(&self, batch: &[Write]) -> bool {
        let starts = batch[0].start_session.is_some();
        match self.start_session {
            None => !starts,
            // A repeated start must observe the worker published by the previous commit.
            Some(sid) => starts && !batch.iter().any(|write| write.start_session == Some(sid)),
        }
    }
}

enum Command {
    Call(Box<dyn FnOnce(&mut Store) -> bool + Send>),
    Write(Write),
}

struct Enqueue {
    sid: SessionId,
    content: Value,
    key: Option<String>,
}

pub enum Enqueued {
    Created(ItemId),
    Existing(ItemId),
}

#[derive(Clone)]
pub struct Db(mpsc::Sender<Command>);

pub struct Store {
    pub conn: Connection,
    pub local_node_id: String,
    pub presence: Arc<PresenceState>,
    pub pending_wakes: HashSet<SessionId>,
    pub active: HashMap<SessionId, Control>,
    pub tasks: HashMap<ItemId, crate::codemode::Task>,
    pub mcp: crate::mcp::Connections,
    pub channels: HashMap<SessionId, broadcast::WeakSender<Notice>>,
    pub wake_sequences: HashMap<SessionId, watch::Sender<u64>>,
    pub session_run_gen: HashMap<SessionId, u64>,
    pub session_reset_gen: HashMap<SessionId, u64>,
    pub global_events: broadcast::Sender<Notice>,
    pub sync_tx: Arc<ClusterSignal>,
    pub closing: bool,
}

impl Db {
    pub fn open(
        path: &Path,
        global_events: broadcast::Sender<Notice>,
        sync_tx: Arc<ClusterSignal>,
        presence: Arc<PresenceState>,
    ) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path)?;
        let mut conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;")?;
        let tx = conn.transaction()?;
        let initialized: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*')",
            [],
            |r| r.get(0),
        )?;
        if !initialized {
            tx.execute_batch(include_str!("../schema.sql"))?;
            crate::cluster::identity::ensure_local_node(&tx)?;
        } else {
            let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
            if version != 7 {
                return Err(Error::internal(
                    "incompatible database schema; select a new database directory",
                ));
            }
            crate::id::seed_from_db(&tx)?;
            crate::cluster::identity::ensure_local_node(&tx)?;
        }
        crate::cluster::ledger::init_verified_tips(&tx)?;
        let local_node_id = crate::cluster::identity::local_node_id(&tx)?.to_string();
        presence.set_local_node_id(local_node_id.clone());
        let peer_count: usize = usize::try_from(tx.query_row(
            "SELECT count(*) FROM cluster_peers",
            [],
            |r| r.get::<_, i64>(0),
        )?)
        .unwrap_or(0);
        presence.set_known_peer_count(peer_count);
        let has_null_att: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attachments WHERE data IS NULL)",
            [],
            |r| r.get(0),
        )?;
        presence.set_has_null_attachments(has_null_att);
        let lease_rows = {
            let mut stmt = tx.prepare(
                "SELECT session_id, highest_term, holder_node_id, claim_id, expires_at_ms FROM session_leases",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, SessionId>(0)?,
                    u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        presence.load_persisted_leases(lease_rows);
        let call_ids = {
            let mut stmt = tx.prepare(
                "SELECT id FROM llm_calls WHERE status='in_flight' AND origin_node_id=?1",
            )?;
            stmt.query_map([&local_node_id], |r| r.get::<_, CallId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for cid in call_ids {
            tx.execute(
                "UPDATE llm_calls SET status='interrupted',error_summary='daemon restarted' WHERE id=?",
                [cid],
            )?;
            if !has_conflict(&tx, cid.as_str())? {
                record_call(&tx, cid)?;
            }
        }
        let running_sids = {
            let mut stmt = tx.prepare("SELECT id FROM sessions WHERE status IN ('running','pausing')")?;
            stmt.query_map([], |r| r.get::<_, SessionId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let has_peers = peer_count > 0;
        let mut deferred_recovery: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM deferred_crash_sessions)",
            [],
            |r| r.get(0),
        )?;
        for sid in running_sids {
            if has_peers
                && !has_conflict(&tx, sid.as_str())?
                && is_session_writable_local(&tx, sid)?
            {
                crate::codemode::ledger::record_deferred_crash_session(
                    &tx,
                    sid,
                    "daemon restarted",
                )?;
                deferred_recovery = true;
            }
            tx.execute(
                "UPDATE sessions SET status='paused', last_error='daemon restarted' WHERE id=?",
                [sid],
            )?;
            if !has_peers
                && !has_conflict(&tx, sid.as_str())?
                && is_session_writable_local(&tx, sid)?
            {
                record_session_delta(&tx, sid, "session_delta", Vec::new(), Vec::new(), Vec::new())?;
            }
        }
        if crate::codemode::ledger::recover(&tx, !has_peers)? {
            deferred_recovery = true;
        }
        presence.set_deferred_recovery_pending(deferred_recovery);
        tx.commit()?;
        clear_staged();
        let (sender, mut receiver) = mpsc::channel::<Command>(64);
        let runtime = tokio::runtime::Handle::current();
        thread::Builder::new()
            .name("sqlite".into())
            .spawn(move || {
                let _entered = runtime.enter();
                let mut store = Store {
                    conn,
                    local_node_id,
                    presence,
                    pending_wakes: HashSet::new(),
                    active: HashMap::new(),
                    tasks: HashMap::new(),
                    mcp: Default::default(),
                    channels: HashMap::new(),
                    wake_sequences: HashMap::new(),
                    session_run_gen: HashMap::new(),
                    session_reset_gen: HashMap::new(),
                    global_events,
                    sync_tx,
                    closing: false,
                };
                crate::cluster::identity::set_writer_node_id(&store.conn, &store.local_node_id);
                let mut next = None;
                let mut batch = Vec::new();
                while let Some(command) = next.take().or_else(|| receiver.blocking_recv()) {
                    match command {
                        Command::Call(call) => {
                            clear_staged();
                            if call(&mut store) {
                                let staged = take_staged();
                                store.notify_streams(staged);
                            } else {
                                clear_staged();
                            }
                        }
                        Command::Write(first) => {
                            batch.push(first);
                            // Drain only compatible writes already waiting at the head of the queue.
                            while batch.len() < 32 {
                                match receiver.try_recv() {
                                    Ok(Command::Write(write)) if write.can_join(&batch) => {
                                        batch.push(write)
                                    }
                                    other => {
                                        next = other.ok();
                                        break;
                                    }
                                }
                            }
                            store.commit_batch(&mut batch);
                        }
                    }
                }
            })?;
        Ok(Self(sender))
    }

    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.submit(|reply| {
            Command::Call(Box::new(move |store| {
                let res = f(store);
                let ok = res.is_ok();
                let _ = reply.send(res);
                ok
            }))
        })
        .await
    }

    pub async fn enqueue(
        &self,
        sid: SessionId,
        content: Value,
        key: Option<String>,
    ) -> Result<Enqueued> {
        self.write(
            move |conn| Enqueue { sid, content, key }.insert(conn),
            move |store, saved| {
                if let Enqueued::Created(id) = saved {
                    store.wake_waiters(sid);
                    store.item_saved(sid, id)?;
                }
                Ok(saved)
            },
        )
        .await
    }

    pub async fn enqueue_with_attachments(
        &self,
        sid: SessionId,
        content: Value,
        key: Option<String>,
        attachments: Vec<AttachmentRef>,
    ) -> Result<Enqueued> {
        let has_refs = !attachments.is_empty();
        self.write(
            move |conn| {
                merge_remote_attachment_refs(conn, &attachments)?;
                Enqueue { sid, content, key }.insert(conn)
            },
            move |store, saved| {
                if has_refs {
                    store.presence.set_has_null_attachments(true);
                }
                if let Enqueued::Created(id) = saved {
                    store.wake_waiters(sid);
                    store.item_saved(sid, id)?;
                }
                Ok(saved)
            },
        )
        .await
    }

    // The database thread runs apply in FIFO order, then publish in that same order after COMMIT.
    pub async fn write<T: 'static, R: Send + 'static>(
        &self,
        apply: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.grouped_write(None, None, move |conn, _| apply(conn), publish)
            .await
    }

    pub async fn write_for_run<T: 'static, R: Send + 'static>(
        &self,
        sid: SessionId,
        expected_run_gen: u64,
        apply: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.grouped_write(
            None,
            Some((sid, expected_run_gen)),
            move |conn, _| apply(conn),
            publish,
        )
        .await
    }

    pub async fn start_write<T: 'static, R: Send + 'static>(
        &self,
        sid: SessionId,
        apply: impl FnOnce(&Connection, bool) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.grouped_write(Some(sid), None, apply, publish).await
    }

    async fn grouped_write<T: 'static, R: Send + 'static>(
        &self,
        start_session: Option<SessionId>,
        expected_run_gen: Option<(SessionId, u64)>,
        apply: impl FnOnce(&Connection, bool) -> Result<T> + Send + 'static,
        publish: impl FnOnce(&mut Store, T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        self.submit(|reply| {
            Command::Write(Write {
                start_session,
                expected_run_gen,
                apply: Box::new(move |conn, active| {
                    let result = conn.and_then(|conn| apply(conn, active));
                    (
                        result.is_ok(),
                        Box::new(move |store, committed| {
                            let result = match committed {
                                Ok(()) => result.and_then(|value| publish(store, value)),
                                Err(error) => Err(error.clone()),
                            };
                            let _ = reply.send(result);
                        }),
                    )
                }),
            })
        })
        .await
    }

    async fn submit<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
    ) -> Result<T> {
        let (send, recv) = oneshot::channel();
        self.0
            .send(command(send))
            .await
            .map_err(|_| Error::internal("database worker stopped"))?;
        recv.await
            .map_err(|_| Error::internal("database worker stopped"))?
    }
}

impl Enqueue {
    fn insert(&self, conn: &Connection) -> Result<Enqueued> {
        if let Some(key) = &self.key {
            let mut lookup = conn.prepare_cached("SELECT id,json_extract(payload,'$.content') FROM items WHERE session_id=? AND idempotency_key=?")?;
            if let Some((id, content)) = lookup
                .query_row((self.sid, key), |row| {
                    Ok((row.get(0)?, row.get::<_, Value>(1)?))
                })
                .optional()?
            {
                if content != self.content {
                    return Err(Error::conflict(
                        "idempotency key reused with different content",
                    ));
                }
                return Ok(Enqueued::Existing(id));
            }
        }
        let model_id: ModelId = conn
            .prepare_cached("SELECT model_id FROM sessions WHERE id=?")?
            .query_row([self.sid], |row| row.get(0))?;
        let (modalities, _) = crate::catalog::available_capabilities(conn, model_id)?;
        crate::media::validate_message(conn, &self.content, &modalities)?;
        let origin = crate::cluster::identity::local_node_id(conn)?;
        let id = ItemId::new()?;
        let item = conn.prepare_cached(returning_item_snapshot!(
            "INSERT INTO items(id,session_id,kind,payload,idempotency_key,origin_node_id)
             VALUES(?1,?2,'user_message',json_object('content',json(?3),'role','user'),?4,?5)"
        ))?.query_row(params![id, self.sid, self.content, self.key, origin], ItemSnapshot::from_row)?;
        let now = chrono::Utc::now().timestamp_millis();
        conn.prepare_cached("UPDATE sessions SET updated_at=?2 WHERE id=?1")?
            .execute((self.sid, now))?;
        record_session_delta(conn, self.sid, "session_delta", vec![item], Vec::new(), Vec::new())?;
        Ok(Enqueued::Created(id))
    }
}

impl Store {
    pub fn sync_signal(&self, signal: SyncSignal) {
        match signal {
            SyncSignal::SelfMetaChanged => self.sync_tx.notify_self_meta(),
        }
    }

    pub fn notify_streams(&self, ids: Vec<String>) {
        if self.presence.known_peer_count() > 0 {
            self.sync_tx.notify_streams(ids);
        }
    }

    pub fn bump_run_gen(&mut self, sid: SessionId) -> u64 {
        let next = self.session_run_gen.get(&sid).copied().unwrap_or(0) + 1;
        self.session_run_gen.insert(sid, next);
        next
    }

    pub fn run_gen(&self, sid: SessionId) -> u64 {
        self.session_run_gen.get(&sid).copied().unwrap_or(0)
    }

    pub fn bump_reset_gen(&mut self, sid: SessionId) -> u64 {
        let next = self.session_reset_gen.get(&sid).copied().unwrap_or(0) + 1;
        self.session_reset_gen.insert(sid, next);
        next
    }

    pub fn reset_gen(&self, sid: SessionId) -> u64 {
        self.session_reset_gen.get(&sid).copied().unwrap_or(0)
    }

    fn commit_batch(&mut self, batch: &mut Vec<Write>) {
        let starting = batch[0].start_session.is_some();
        let mut replies = Vec::with_capacity(batch.len());
        let mut pending = batch.drain(..);
        let mut staged = Vec::new();
        clear_staged();
        let committed = (|| -> Result<()> {
            if starting && self.closing {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "shutting down".into(),
                ));
            }
            let active_map = &self.active;
            let mut tx = self.conn.transaction()?;
            for write in pending.by_ref() {
                if let Some((sid, expected_gen)) = write.expected_run_gen
                    && (active_map.get(&sid).is_none_or(|run| run.run_gen != expected_gen)
                        || !self.presence.is_local_lease_valid(sid))
                {
                    let (_, reply) = (write.apply)(
                        Err(Error::conflict(
                            "session superseded by cluster replacement or expired execution lease",
                        )),
                        false,
                    );
                    replies.push(reply);
                    continue;
                }
                // A failed multi-statement operation must not leave part of its reply in the DB.
                clear_staged();
                let savepoint = tx.savepoint();
                let active = write
                    .start_session
                    .is_some_and(|id| active_map.contains_key(&id));
                let (ok, reply) = (write.apply)(
                    savepoint
                        .as_deref()
                        .map_err(|e| Error::internal(e.to_string())),
                    active,
                );
                replies.push(reply);
                let savepoint = savepoint?;
                if ok {
                    savepoint.commit()?;
                    staged.extend(take_staged());
                } else {
                    clear_staged();
                    savepoint.finish()?;
                }
            }
            tx.commit()?;
            Ok(())
        })();
        if let Err(error) = &committed {
            clear_staged();
            replies.extend(pending.map(|write| (write.apply)(Err(error.clone()), false).1));
        } else {
            self.notify_streams(staged);
        }
        for reply in replies {
            reply(self, &committed);
        }
    }

    pub fn wake_waiters(&mut self, sid: SessionId) {
        if let std::collections::hash_map::Entry::Occupied(entry) = self.wake_sequences.entry(sid) {
            if entry.get().receiver_count() > 0 {
                entry.get().send_modify(|sequence| *sequence += 1);
            } else {
                entry.remove();
            }
        }
    }

    pub fn subscribe_wait(&mut self, sid: SessionId) -> watch::Receiver<u64> {
        if let Some(sender) = self.wake_sequences.get(&sid) {
            let mut rx = sender.subscribe();
            rx.borrow_and_update();
            return rx;
        }
        self.wake_sequences.retain(|_, s| s.receiver_count() > 0);
        let (tx, mut rx) = watch::channel(0);
        rx.borrow_and_update();
        self.wake_sequences.insert(sid, tx);
        rx
    }

    pub fn channel(&mut self, sid: SessionId) -> broadcast::Sender<Notice> {
        if let Some(sender) = self.channels.get(&sid).and_then(|w| w.upgrade()) {
            return sender;
        }
        self.channels.retain(|_, w| w.strong_count() > 0);
        let (sender, _) = broadcast::channel(64);
        self.channels.insert(sid, sender.downgrade());
        sender
    }

    pub fn item_saved(&self, sid: SessionId, id: ItemId) -> Result<()> {
        if let Some(channel) = self.channels.get(&sid).and_then(|w| w.upgrade())
            && channel.receiver_count() > 0
        {
            let mut stmt = self.conn.prepare_cached(
                "SELECT id,kind,payload,target_id,llm_call_id FROM items WHERE id=?",
            )?;
            Notice::publish_serializable(
                &channel,
                "item.saved",
                &stmt.query_row([id], history_row)?,
            );
        }
        Ok(())
    }

    pub fn publish_global(&self, name: &'static str, data: Value) {
        Notice::publish(&self.global_events, name, data);
    }

    pub fn status(&mut self, sid: SessionId, state: &str, reason: &str) -> Result<()> {
        self.status_checked(sid, state, reason, |_| Ok(()))
    }

    pub fn status_checked(
        &mut self,
        sid: SessionId,
        state: &str,
        reason: &str,
        before_commit: impl FnOnce(&Self) -> Result<()>,
    ) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        with_savepoint(&self.conn, "store_status", || {
            has_conflict(&self.conn, sid.as_str()).and_then(|c| {
                if c {
                    Err(Error::conflict(format!(
                        "sync conflict on {sid}; resolve it before modifying"
                    )))
                } else {
                    Ok(())
                }
            })?;
            if !is_session_writable_local(&self.conn, sid)? {
                return Err(Error::conflict(
                    "session history is incomplete or deleted; sync before modifying",
                ));
            }
            self.conn.execute(
                "UPDATE sessions SET status=?2,last_error=?3,updated_at=?4 WHERE id=?1",
                (sid, state, reason, now),
            )?;
            if matches!(state, "idle" | "paused") {
                record_session_delta(&self.conn, sid, "session_delta", Vec::new(), Vec::new(), Vec::new())?;
            }
            before_commit(self)
        })?;
        let status_enum = match state {
            "running" => SessionStatus::Running,
            "pausing" => SessionStatus::Pausing,
            "paused" => SessionStatus::Paused,
            _ => SessionStatus::Idle,
        };
        self.refresh_local_presence_with(sid, status_enum, reason, now);
        self.notify_status(sid, state, reason, now);
        Ok(())
    }

    pub fn is_local_lease_valid(&self, sid: SessionId) -> bool {
        self.presence.is_local_lease_valid(sid)
    }

    pub fn effective_runner_node_id(&self, sid: SessionId) -> Option<Cow<'_, str>> {
        if self.active.contains_key(&sid)
            || self.pending_wakes.contains(&sid)
            || crate::codemode::has_session_tasks(self, sid)
        {
            return Some(Cow::Borrowed(&self.local_node_id));
        }
        if self.presence.known_peer_count() == 0 {
            return None;
        }
        self.presence.remote_owner(sid).map(|(id, _)| Cow::Owned(id))
    }

    pub fn session_view(&self, mut s: Session) -> Session {
        let local_busy = self.active.contains_key(&s.id)
            || self.pending_wakes.contains(&s.id)
            || crate::codemode::has_session_tasks(self, s.id);
        if local_busy {
            s.runner_node_id = Some(self.local_node_id.clone());
        } else if self.presence.known_peer_count() > 0
            && let Some((peer_id, occ)) = self.presence.remote_owner(s.id)
        {
            s.runner_node_id = Some(peer_id);
            if matches!(occ.status, SessionStatus::Running | SessionStatus::Pausing) {
                let is_pausing = occ.status == SessionStatus::Pausing;
                s.status = occ.status;
                s.updated_at = s.updated_at.max(occ.updated_at);
                if is_pausing {
                    s.last_error = occ.reason;
                } else {
                    s.last_error.clear();
                }
            }
        } else {
            s.runner_node_id = None;
        }
        s
    }

    pub fn refresh_local_presence(&self, sid: SessionId) {
        if self.presence.known_peer_count() == 0 && !self.presence.has_any_local_state() {
            return;
        }
        self.force_refresh_local_presence(sid);
    }

    pub fn force_refresh_local_presence(&self, sid: SessionId) {
        let active = self.active.contains_key(&sid) || self.pending_wakes.contains(&sid);
        let active_tasks = crate::codemode::count_session_tasks(self, sid);
        if !active && active_tasks == 0 {
            self.presence.set_local(sid, None);
            return;
        }
        let occ = session(&self.conn, sid).ok().map(|info| SessionOccupancy {
            session_id: sid,
            status: if active && !matches!(info.status, SessionStatus::Running | SessionStatus::Pausing) {
                SessionStatus::Running
            } else {
                info.status
            },
            reason: info.last_error,
            active_tasks,
            updated_at: info.updated_at,
        });
        self.presence.set_local(sid, occ);
    }

    pub fn refresh_local_presence_with(
        &self,
        sid: SessionId,
        status: SessionStatus,
        reason: &str,
        updated_at: i64,
    ) {
        if self.presence.known_peer_count() == 0 && !self.presence.has_any_local_state() {
            return;
        }
        let active = self.active.contains_key(&sid) || self.pending_wakes.contains(&sid);
        let active_tasks = crate::codemode::count_session_tasks(self, sid);
        if !active && active_tasks == 0 {
            self.presence.set_local(sid, None);
            return;
        }
        self.presence.set_local(
            sid,
            Some(SessionOccupancy {
                session_id: sid,
                status: if active && !matches!(status, SessionStatus::Running | SessionStatus::Pausing) {
                    SessionStatus::Running
                } else {
                    status
                },
                reason: reason.to_owned(),
                active_tasks,
                updated_at,
            }),
        );
    }

    pub fn notify_status(&self, sid: SessionId, state: &str, reason: &str, updated_at: i64) {
        let channel = self
            .channels
            .get(&sid)
            .and_then(|w| w.upgrade())
            .filter(|ch| ch.receiver_count() > 0);
        let has_global = self.global_events.receiver_count() > 0;
        if channel.is_none() && !has_global {
            return;
        }
        #[derive(serde::Serialize)]
        struct StatusNotice<'a> {
            status: &'a str,
            reason: &'a str,
            updated_at: i64,
            runner_node_id: Option<&'a str>,
        }
        #[derive(serde::Serialize)]
        struct GlobalStatusNotice<'a> {
            id: SessionId,
            status: &'a str,
            reason: &'a str,
            updated_at: i64,
            runner_node_id: Option<&'a str>,
        }
        let runner_node_id = self.effective_runner_node_id(sid);
        let runner_ref = runner_node_id.as_deref();
        if let Some(channel) = channel {
            Notice::publish_serializable(
                &channel,
                "session.status",
                &StatusNotice {
                    status: state,
                    reason,
                    updated_at,
                    runner_node_id: runner_ref,
                },
            );
        }
        if has_global {
            Notice::publish_serializable(
                &self.global_events,
                "session.status",
                &GlobalStatusNotice {
                    id: sid,
                    status: state,
                    reason,
                    updated_at,
                    runner_node_id: runner_ref,
                },
            );
        }
    }
}

#[derive(serde::Serialize)]
pub struct HistoryItem {
    id: ItemId,
    kind: String,
    payload: Value,
    target_id: Option<ItemId>,
    llm_call_id: Option<CallId>,
}

fn history_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryItem> {
    Ok(HistoryItem {
        id: row.get(0)?,
        kind: row.get(1)?,
        payload: row.get(2)?,
        target_id: row.get(3)?,
        llm_call_id: row.get(4)?,
    })
}

pub fn session(conn: &Connection, sid: SessionId) -> Result<Session> {
    let mut stmt = conn.prepare_cached(
        "SELECT id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id FROM sessions WHERE id=?",
    )?;
    Ok(stmt.query_row([sid], session_row)?)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub title: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: SessionStatus,
    pub reasoning_effort: Option<String>,
    pub last_error: String,
    pub model_id: ModelId,
    #[serde(default)]
    pub runner_node_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Idle,
    Running,
    Pausing,
    Paused,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
        }
    }
}

impl rusqlite::types::FromSql for SessionStatus {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use serde::{Deserialize, de::value::StrDeserializer};
        Self::deserialize(StrDeserializer::<serde::de::value::Error>::new(
            value.as_str()?,
        ))
        .map_err(|err| rusqlite::types::FromSqlError::Other(Box::new(err)))
    }
}

pub fn session_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Session> {
    Ok(Session {
        id: row.get(0)?,
        title: row.get(1)?,
        created_at: row.get(2)?,
        updated_at: row.get(3)?,
        status: row.get(4)?,
        reasoning_effort: row.get(5)?,
        last_error: row.get(6)?,
        model_id: row.get(7)?,
        runner_node_id: None,
    })
}

// This view is shared by queue inspection, consumption and the idle transition.
pub fn has_pending(conn: &Connection, sid: SessionId) -> Result<bool> {
    let ready: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pending WHERE session_id=?)",
        [sid],
        |r| r.get(0),
    )?;
    // The caller can go idle without calling take_pending when only discarded inputs remain.
    if !ready {
        with_savepoint(conn, "advance_pending", || {
            if advance_queue(conn, sid, false)? {
                record_session_delta(conn, sid, "session_delta", Vec::new(), Vec::new(), Vec::new())?;
            }
            Ok(())
        })?;
    }
    Ok(ready)
}

pub fn pending_reason(conn: &Connection, sid: SessionId) -> Result<Option<&'static str>> {
    let (user, event): (bool, bool) = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pending p JOIN items i ON i.id=p.id WHERE p.session_id=?1 AND i.kind='user_message'),
                EXISTS(SELECT 1 FROM pending p JOIN items i ON i.id=p.id WHERE p.session_id=?1 AND i.kind='codemode_event')",
        [sid],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    match (user, event) {
        (true, true) => Ok(Some("pending_input")),
        (true, false) => Ok(Some("user_message")),
        (false, true) => Ok(Some("task_event")),
        (false, false) => {
            with_savepoint(conn, "advance_pending_reason", || {
                if advance_queue(conn, sid, false)? {
                    record_session_delta(conn, sid, "session_delta", Vec::new(), Vec::new(), Vec::new())?;
                }
                Ok(())
            })?;
            Ok(None)
        }
    }
}

pub fn max_context_position(conn: &Connection, sid: SessionId) -> Result<i64> {
    Ok(conn
        .prepare_cached("SELECT coalesce(max(position),0) FROM context_items WHERE session_id=?")?
        .query_row([sid], |r| r.get(0))?)
}

pub fn take_pending(tx: &Connection, sid: SessionId) -> Result<()> {
    let base = max_context_position(tx, sid)?;
    let added = tx.execute(
        "INSERT INTO context_items(session_id,position,item_id)
         SELECT session_id, ?2 + row_number() OVER (ORDER BY id), id
         FROM pending WHERE session_id=?1 ORDER BY id",
        (sid, base),
    )?;
    let mut ctx_added = Vec::new();
    if added > 0 {
        let mut stmt = tx.prepare_cached(
            "SELECT position,item_id FROM context_items WHERE session_id=? AND position>? ORDER BY position",
        )?;
        ctx_added = stmt
            .query_map((sid, base), |r| {
                Ok(ContextPos {
                    position: r.get(0)?,
                    item_id: r.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
    }
    let advanced = advance_queue(tx, sid, added > 0)?;
    if !ctx_added.is_empty() || advanced {
        record_session_delta(tx, sid, "session_delta", Vec::new(), ctx_added, Vec::new())?;
    }
    Ok(())
}

fn advance_queue(conn: &Connection, sid: SessionId, added: bool) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sessions SET input_queue_cursor=last_id,needs_model=needs_model OR ?2
         FROM (SELECT max(id) AS last_id FROM items WHERE session_id=?1 AND kind IN ('user_message','codemode_event'))
         WHERE sessions.id=?1 AND input_queue_cursor<last_id",
        (sid, added),
    )?;
    if changed == 0 && added {
        let mut updated = 0;
        if added {
            updated = conn.execute(
                "UPDATE sessions SET needs_model=1 WHERE id=?1 AND needs_model=0",
                [sid],
            )?;
        }
        return Ok(updated > 0);
    }
    Ok(changed > 0)
}

pub fn add_item(
    conn: &Connection,
    sid: SessionId,
    kind: &str,
    payload: &Value,
    target: Option<ItemId>,
    llm_call_id: Option<CallId>,
) -> Result<ItemSnapshot> {
    let id = ItemId::new()?;
    let origin = crate::cluster::identity::local_node_id(conn)?;
    Ok(conn.prepare_cached(returning_item_snapshot!(
        "INSERT INTO items(id,session_id,kind,payload,target_id,llm_call_id,origin_node_id) VALUES(?,?,?,?,?,?,?)"
    ))?.query_row(params![id, sid, kind, payload, target, llm_call_id, origin], ItemSnapshot::from_row)?)
}

pub fn add_context_item_raw(
    tx: &Connection,
    sid: SessionId,
    kind: &str,
    item: &Value,
    call_id: CallId,
) -> Result<(ItemSnapshot, i64)> {
    let saved = add_item(tx, sid, kind, item, None, Some(call_id))?;
    let pos = max_context_position(tx, sid)? + 1;
    tx.execute(
        "INSERT INTO context_items(session_id,position,item_id) VALUES(?,?,?)",
        (sid, pos, saved.id),
    )?;
    Ok((saved, pos))
}

pub fn add_context_item(
    tx: &Connection,
    sid: SessionId,
    kind: &str,
    item: &Value,
    call_id: CallId,
    expected_epoch: Option<i64>,
) -> Result<ItemId> {
    let (saved, pos) = add_context_item_raw(tx, sid, kind, item, call_id)?;
    let id = saved.id;
    crate::cluster::ledger::record_session_delta_at_epoch(
        tx,
        sid,
        "session_delta",
        vec![saved],
        vec![ContextPos {
            position: pos,
            item_id: id,
        }],
        Vec::new(),
        expected_epoch,
    )?;
    Ok(id)
}

// Publish all completed tools in call order. A missing result after interruption
// is recorded explicitly: replaying an already executed tool could repeat its effect.
pub fn publish_tools(tx: &Connection, sid: SessionId, recovering: bool) -> Result<Vec<ItemId>> {
    let mut recovered = Vec::new();
    let mut ctx_added = Vec::new();
    let mut after: Option<ItemId> = None;
    let mut pos = max_context_position(tx, sid)? + 1;
    let mut pending = tx.prepare_cached(
        "SELECT c.id, json_extract(c.payload,'$.call_id'), r.id FROM items c
         LEFT JOIN items r ON r.kind='function_call_output' AND r.target_id=c.id
         WHERE c.session_id=?1 AND c.kind='function_call' AND c.id>?2
           AND NOT EXISTS(SELECT 1 FROM context_items WHERE item_id=c.id)
         ORDER BY c.id LIMIT 1",
    )?;
    while let Some((id, call, output)) = pending
        .query_row((sid, after.as_ref().map_or("", ItemId::as_str)), |r| {
            Ok((
                r.get::<_, ItemId>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<ItemId>>(2)?,
            ))
        })
        .optional()?
    {
        after = Some(id);
        let result = match output {
            Some(id) => id,
            None if recovering => {
                let payload = json!({"type":"function_call_output","call_id":call,"output":"Execution interrupted; result unknown."});
                let result = add_item(tx, sid, "function_call_output", &payload, Some(id), None)?;
                let result_id = result.id;
                recovered.push(result);
                result_id
            }
            None => return Err(Error::internal("missing tool result")),
        };
        tx.execute(
            "INSERT INTO context_items(session_id,position,item_id) VALUES(?1,?2,?3),(?1,?4,?5)",
            (sid, pos, id, pos + 1, result),
        )?;
        ctx_added.push(ContextPos {
            position: pos,
            item_id: id,
        });
        ctx_added.push(ContextPos {
            position: pos + 1,
            item_id: result,
        });
        pos += 2;
    }
    let recovered_ids = recovered.iter().map(|item| item.id).collect();
    if !recovered.is_empty() || !ctx_added.is_empty() {
        record_session_delta(
            tx,
            sid,
            "session_delta",
            recovered,
            ctx_added,
            Vec::new(),
        )?;
    }
    Ok(recovered_ids)
}

const BATCH_ITEMS: i64 = 256;
const BATCH_BYTES: usize = 64 * 1024;

trait Cursor: Copy + Eq + Ord + Send + ToSql + FromSql + 'static {}
impl Cursor for i64 {}
impl Cursor for ItemId {}

#[derive(Clone, Copy)]
pub enum JsonRows {
    History,
    Context,
    ModelInput,
}

fn page_bounds<C: Cursor>(
    conn: &Connection,
    table: &str,
    col: &str,
    sid: SessionId,
    after: impl ToSql,
    before: Option<C>,
    tail: bool,
    limit: i64,
) -> Result<Option<(C, C)>> {
    let pair = if tail {
        let sql = format!(
            "SELECT min(k), max(k) FROM (SELECT {col} AS k FROM {table} WHERE session_id=?1 ORDER BY {col} DESC LIMIT ?2)"
        );
        conn.prepare_cached(&sql)?.query_row((sid, limit), |r| {
            Ok((r.get::<_, Option<C>>(0)?, r.get::<_, Option<C>>(1)?))
        })?
    } else if let Some(before) = before {
        let sql = format!(
            "SELECT min(k), max(k) FROM (SELECT {col} AS k FROM {table} WHERE session_id=?1 AND {col}<?2 ORDER BY {col} DESC LIMIT ?3)"
        );
        conn.prepare_cached(&sql)?
            .query_row((sid, before, limit), |r| {
                Ok((r.get::<_, Option<C>>(0)?, r.get::<_, Option<C>>(1)?))
            })?
    } else {
        let sql = format!(
            "SELECT min(k), max(k) FROM (SELECT {col} AS k FROM {table} WHERE session_id=?1 AND {col}>?2 ORDER BY {col} LIMIT ?3)"
        );
        conn.prepare_cached(&sql)?
            .query_row(params![sid, after, limit], |r| {
                Ok((r.get::<_, Option<C>>(0)?, r.get::<_, Option<C>>(1)?))
            })?
    };
    Ok(pair.0.zip(pair.1))
}

impl JsonRows {
    // Both tables are append-only. Pin the cursor span of this page before sending
    // bytes so a concurrent append can't appear halfway through the response.
    pub fn history_range(
        conn: &Connection,
        sid: SessionId,
        after: Option<ItemId>,
        before: Option<ItemId>,
        tail: bool,
        limit: i64,
    ) -> Result<Option<(ItemId, ItemId)>> {
        page_bounds(
            conn,
            "items",
            "id",
            sid,
            after.as_ref().map_or("", ItemId::as_str),
            before,
            tail,
            limit,
        )
    }

    pub fn context_range(
        conn: &Connection,
        sid: SessionId,
        after: Option<i64>,
        before: Option<i64>,
        tail: bool,
        limit: i64,
    ) -> Result<Option<(i64, i64)>> {
        page_bounds(
            conn,
            "context_items",
            "position",
            sid,
            after.unwrap_or(0),
            before,
            tail,
            limit,
        )
    }

    fn batch<C: Cursor>(
        self,
        conn: &Connection,
        sid: SessionId,
        cursor: C,
        end: C,
        inclusive: bool,
        mut bytes: Vec<u8>,
        mut comma: bool,
    ) -> Result<(Vec<u8>, C)> {
        let cmp = if inclusive { ">=" } else { ">" };
        let sql = match self {
            Self::ModelInput => format!(
                "SELECT c.position,i.payload,i.kind FROM context_items c JOIN items i ON i.id=c.item_id
                 WHERE c.session_id=? AND c.position{cmp}? AND c.position<=? ORDER BY c.position LIMIT ?"
            ),
            Self::Context => format!(
                "SELECT c.position,json_object('position',c.position,'item_id',i.id,'llm_call_id',i.llm_call_id,'payload',json(i.payload))
                 FROM context_items c JOIN items i ON i.id=c.item_id
                 WHERE c.session_id=? AND c.position{cmp}? AND c.position<=? ORDER BY c.position LIMIT ?"
            ),
            Self::History => format!(
                "SELECT id,json_object('id',id,'kind',kind,'payload',json(payload),'target_id',target_id,'llm_call_id',llm_call_id)
                 FROM items WHERE session_id=? AND id{cmp}? AND id<=? ORDER BY id LIMIT ?"
            ),
        };
        let mut stmt = conn.prepare_cached(&sql)?;
        let mut rows = stmt.query((sid, cursor, end, BATCH_ITEMS))?;
        let mut position = None;
        while let Some(row) = rows.next()? {
            let encoded;
            let raw = if matches!(self, Self::ModelInput)
                && let Some(kind @ ("user_message" | "function_call_output" | "codemode_event")) =
                    row.get_ref(2)?.as_str().ok()
            {
                encoded = crate::media::wire_message(conn, kind, row.get(1)?)?.to_string();
                encoded.as_bytes()
            } else {
                row.get_ref(1)?
                    .as_bytes()
                    .map_err(|e| Error::internal(e.to_string()))?
            };
            if position.is_some() && bytes.len() + raw.len() + 1 > BATCH_BYTES {
                break;
            }
            if comma {
                bytes.push(b',');
            }
            bytes.extend_from_slice(raw);
            comma = true;
            position = Some(row.get::<_, C>(0)?);
            if bytes.len() >= BATCH_BYTES {
                break;
            }
        }
        let Some(position) = position else {
            return Err(Error::internal("missing row in saved history"));
        };
        Ok((bytes, position))
    }

    pub async fn history_body(
        self,
        db: Db,
        sid: SessionId,
        range: Option<(ItemId, ItemId)>,
        history_rev: i64,
    ) -> Result<Body> {
        self.body_with_range(db, sid, range, vec![b'['], b"]", Some(history_rev)).await
    }

    pub async fn context_body(
        self,
        db: Db,
        sid: SessionId,
        range: Option<(i64, i64)>,
        prefix: Vec<u8>,
        suffix: &'static [u8],
    ) -> Result<Body> {
        self.body_with_range(db, sid, range, prefix, suffix, None).await
    }

    async fn body_with_range<C: Cursor>(
        self,
        db: Db,
        sid: SessionId,
        range: Option<(C, C)>,
        mut prefix: Vec<u8>,
        suffix: &'static [u8],
        history_rev: Option<i64>,
    ) -> Result<Body> {
        let Some((start, end)) = range else {
            prefix.extend_from_slice(suffix);
            return Ok(Body::from(prefix));
        };
        let read_batch = move |conn: &Connection, cursor, inclusive, bytes, comma| {
            if let Some(expected) = history_rev
                && crate::cluster::ledger::session_history_rev(conn, sid)? != expected
            {
                return Err(Error::conflict("session history changed; retry this page"));
            }
            self.batch(conn, sid, cursor, end, inclusive, bytes, comma)
        };
        let (mut bytes, mut position) = db
            .call(move |s| read_batch(&s.conn, start, true, prefix, false))
            .await?;
        if position == end {
            bytes.extend_from_slice(suffix);
            return Ok(Body::from(bytes));
        }
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Error>(Bytes::from(bytes));
            while position < end {
                let result = db
                    .call(move |s| read_batch(&s.conn, position, false, Vec::new(), true))
                    .await;
                match result {
                    Ok((mut bytes, next)) => {
                        position = next;
                        if position == end {
                            bytes.extend_from_slice(suffix);
                        }
                        yield Ok(Bytes::from(bytes));
                    }
                    Err(err) => {
                        yield Err(err);
                        break;
                    }
                }
            }
        };
        Ok(Body::wrap_stream(stream))
    }
}
