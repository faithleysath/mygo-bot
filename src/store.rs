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
use rusqlite::{Connection, OptionalExtension, params, types::ValueRef};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{
    Error, Result,
    agent::Control,
    cluster::{
        ClusterSignal, SyncSignal,
        exec::{PresenceState, SessionOccupancy},
        ledger::{
            AttachmentRef, CALLS_SCOPE, append_context_items, append_context_items_at,
            clear_staged, list_session_leaves, mark_session_watched, merge_attachment_refs,
            next_catalog_rev, next_origin_seq, on_item_inserted, seed_hlc_from_db, session_scope,
            stage_catalog, stage_session, take_staged, tick_hlc_for, with_savepoint,
        },
    },
    events::Notice,
    id::{CallId, ContextNodeId, ItemId, ModelId, SessionId},
};
pub use crate::cluster::ledger::BranchHead;

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
pub struct Db {
    tx: mpsc::Sender<Command>,
}

pub struct Store {
    pub conn: Connection,
    pub local_node_id: String,
    pub instance_id: String,
    pub presence: Arc<PresenceState>,
    pub pending_wakes: HashSet<SessionId>,
    pub active: HashMap<SessionId, Control>,
    pub tasks: HashMap<ItemId, crate::codemode::Task>,
    pub task_counts: HashMap<SessionId, usize>,
    pub mcp: crate::mcp::Connections,
    pub channels: HashMap<SessionId, broadcast::WeakSender<Notice>>,
    pub wake_sequences: HashMap<SessionId, watch::Sender<u64>>,
    pub session_run_gen: HashMap<SessionId, u64>,
    pub session_reset_gen: HashMap<SessionId, u64>,
    pub session_view_rev: HashMap<SessionId, u64>,
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
            if version != 1 {
                return Err(Error::internal(
                    "incompatible database schema; select a new database directory",
                ));
            }
            crate::id::seed_from_db(&tx)?;
            crate::cluster::identity::ensure_local_node(&tx)?;
        }
        seed_hlc_from_db(&tx)?;
        crate::media::init_validation_cache(&tx)?;
        let mut instance_raw = [0u8; 8];
        getrandom::fill(&mut instance_raw)
            .map_err(|_| Error::internal("entropy unavailable"))?;
        let instance_id: String = instance_raw.iter().map(|b| format!("{b:02x}")).collect();
        let local = crate::cluster::identity::load_local_node(&tx)?;
        let local_node_id = local.node_id.clone();
        presence.set_local_node_id(local_node_id.clone());
        let peers = crate::cluster::identity::list_peers(&tx)?;
        presence.set_known_peer_count(peers.len());
        let has_null_att: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attachments WHERE data IS NULL)",
            [],
            |r| r.get(0),
        )?;
        presence.set_has_null_attachments(has_null_att);
        let call_ids = {
            let mut stmt = tx.prepare(
                "SELECT id FROM llm_calls WHERE status='in_flight' AND origin_node_id=?1",
            )?;
            stmt.query_map([&local_node_id], |r| r.get::<_, CallId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for cid in call_ids {
            let seq = next_origin_seq(&tx, CALLS_SCOPE, &local_node_id)?;
            tx.execute(
                "UPDATE llm_calls SET status='interrupted',error_summary='daemon restarted',origin_seq=?2 WHERE id=?1",
                params![cid, seq],
            )?;
            stage_catalog();
        }
        let running_sids = {
            let mut stmt = tx.prepare(
                "SELECT id FROM sessions WHERE deleted=0 AND local_status IN ('running','pausing')",
            )?;
            stmt.query_map([], |r| r.get::<_, SessionId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let now = chrono::Utc::now().timestamp_millis();
        for sid in running_sids {
            let status_ts = tick_hlc_for(&local_node_id)?;
            let rev = next_catalog_rev(&tx)?;
            tx.execute(
                "UPDATE sessions
                 SET status='paused', last_error='daemon restarted',
                     local_status=NULL, local_error='',
                     updated_at=max(updated_at,?2), status_ts=?3, sync_rev=?4
                 WHERE id=?1",
                params![sid, now, status_ts, rev],
            )?;
            stage_catalog();
        }
        crate::codemode::ledger::recover(&tx)?;
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
                    instance_id,
                    presence,
                    pending_wakes: HashSet::new(),
                    active: HashMap::new(),
                    tasks: HashMap::new(),
                    task_counts: HashMap::new(),
                    mcp: Default::default(),
                    channels: HashMap::new(),
                    wake_sequences: HashMap::new(),
                    session_run_gen: HashMap::new(),
                    session_reset_gen: HashMap::new(),
                    session_view_rev: HashMap::new(),
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
                                let (catalog, sessions) = take_staged();
                                store.notify_staged(catalog, sessions);
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
        Ok(Self { tx: sender })
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
        self.write(
            move |conn| {
                let has_null = if attachments.is_empty() {
                    false
                } else {
                    merge_attachment_refs(conn, &attachments)?
                };
                Ok((Enqueue { sid, content, key }.insert(conn)?, has_null))
            },
            move |store, (saved, has_null)| {
                if has_null {
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
        self.tx
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
            let mut lookup = conn.prepare_cached(
                "SELECT id,json_extract(payload,'$.content') FROM items
                 WHERE session_id=?1 AND idempotency_key=?2 ORDER BY id ASC LIMIT 1",
            )?;
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
            .prepare_cached("SELECT model_id FROM sessions WHERE id=? AND deleted=0")?
            .query_row([self.sid], |row| row.get(0))?;
        let (modalities, _) = crate::catalog::available_capabilities(conn, model_id)?;
        crate::media::validate_message(conn, &self.content, &modalities)?;
        let origin = crate::cluster::identity::local_node_id(conn)?;
        let id = ItemId::new()?;
        let seq = next_origin_seq(conn, &session_scope(self.sid), &origin)?;
        let payload = json!({"content": self.content, "role": "user"});
        conn.prepare_cached(
            "INSERT INTO items(id,session_id,kind,payload,idempotency_key,origin_node_id,origin_seq)
             VALUES(?1,?2,'user_message',?3,?4,?5,?6)",
        )?
        .execute(params![id, self.sid, payload, self.key, &*origin, seq])?;
        on_item_inserted(conn, self.sid, id, "user_message", &payload, None)?;
        let now = chrono::Utc::now().timestamp_millis();
        let rev = next_catalog_rev(conn)?;
        conn.prepare_cached(
            "UPDATE sessions SET updated_at=max(updated_at,?2), sync_rev=?3 WHERE id=?1",
        )?
        .execute(params![self.sid, now, rev])?;
        mark_session_watched(conn, self.sid)?;
        stage_session(self.sid);
        stage_catalog();
        Ok(Enqueued::Created(id))
    }
}

impl Store {
    pub fn sync_signal(&self, signal: SyncSignal) {
        match signal {
            SyncSignal::SelfMetaChanged => self.sync_tx.notify_self_meta(),
        }
    }

    pub fn notify_staged(&self, catalog: bool, sessions: Vec<SessionId>) {
        if self.presence.known_peer_count() > 0 && (catalog || !sessions.is_empty()) {
            self.sync_tx.notify_changes(catalog, sessions);
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

    pub fn view_token(&self, sid: SessionId) -> String {
        let rev = self.session_view_rev.get(&sid).copied().unwrap_or(1);
        format!("{}:{rev}", self.instance_id)
    }

    pub fn bump_view_rev(&mut self, sid: SessionId) -> String {
        let next = self.session_view_rev.get(&sid).copied().unwrap_or(1) + 1;
        self.session_view_rev.insert(sid, next);
        format!("{}:{next}", self.instance_id)
    }

    fn commit_batch(&mut self, batch: &mut Vec<Write>) {
        let starting = batch[0].start_session.is_some();
        let mut replies = Vec::with_capacity(batch.len());
        let mut pending = batch.drain(..);
        let mut staged_catalog = false;
        let mut staged_sessions: Vec<SessionId> = Vec::new();
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
                if let Some((sid, expected_gen)) = write.expected_run_gen {
                    let valid = active_map
                        .get(&sid)
                        .is_some_and(|run| run.run_gen == expected_gen);
                    if !valid {
                        let (_, reply) = (write.apply)(
                            Err(Error::conflict(
                                "session superseded by cluster update",
                            )),
                            false,
                        );
                        replies.push(reply);
                        continue;
                    }
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
                    let (cat, sess) = take_staged();
                    staged_catalog |= cat;
                    for s_id in sess {
                        if !staged_sessions.contains(&s_id) {
                            staged_sessions.push(s_id);
                        }
                    }
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
            self.notify_staged(staged_catalog, staged_sessions);
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
        let reason_bounded =
            crate::calls::truncate_utf8(reason, crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES);
        let reason = reason_bounded.as_str();
        let now = chrono::Utc::now().timestamp_millis();
        with_savepoint(&self.conn, "store_status", || {
            let durable = matches!(state, "idle" | "paused");
            if durable {
                let status_ts = tick_hlc_for(&self.local_node_id)?;
                let rev = next_catalog_rev(&self.conn)?;
                self.conn.execute(
                    "UPDATE sessions
                     SET status=?2,last_error=?3,local_status=NULL,local_error='',
                         updated_at=max(updated_at,?4),status_ts=?5,sync_rev=?6
                     WHERE id=?1 AND deleted=0",
                    params![sid, state, reason, now, status_ts, rev],
                )?;
                stage_catalog();
            } else {
                self.conn.execute(
                    "UPDATE sessions SET local_status=?2,local_error=?3,updated_at=max(updated_at,?4)
                     WHERE id=?1 AND deleted=0",
                    params![sid, state, reason, now],
                )?;
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
        if let Ok(branches) = list_session_leaves(&self.conn, s.id) {
            s.branches = branches;
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
        "SELECT id,title,created_at,updated_at,coalesce(local_status,status),reasoning_effort,
                CASE WHEN local_status IS NOT NULL THEN local_error ELSE last_error END,
                model_id,active_head
         FROM sessions WHERE id=? AND deleted=0",
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
    pub active_head: Option<ContextNodeId>,
    #[serde(default)]
    pub branches: Vec<BranchHead>,
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
        active_head: row.get(8)?,
        branches: Vec::new(),
        runner_node_id: None,
    })
}

pub fn needs_model(conn: &Connection, sid: SessionId) -> Result<bool> {
    let active_head: Option<ContextNodeId> = conn
        .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
        .query_row([sid], |r| r.get(0))
        .optional()?
        .flatten();
    let Some(head_id) = active_head else {
        return Ok(false);
    };
    let settled: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM turn_settled WHERE context_node_id=?)")?
        .query_row([head_id], |r| r.get(0))?;
    Ok(!settled)
}

pub fn has_pending(conn: &Connection, sid: SessionId) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM pending_inputs WHERE session_id=?)")?
        .query_row([sid], |r| r.get(0))?)
}

pub fn pending_reason(conn: &Connection, sid: SessionId) -> Result<Option<&'static str>> {
    let (user, event): (bool, bool) = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM pending_inputs WHERE session_id=?1 AND kind='user_message'),
                    EXISTS(SELECT 1 FROM pending_inputs WHERE session_id=?1 AND kind='codemode_event')",
        )?
        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?)))?;
    match (user, event) {
        (true, true) => Ok(Some("pending_input")),
        (true, false) => Ok(Some("user_message")),
        (false, true) => Ok(Some("task_event")),
        (false, false) => Ok(None),
    }
}

pub fn max_context_position(conn: &Connection, sid: SessionId) -> Result<i64> {
    Ok(conn
        .prepare_cached("SELECT coalesce(max(position),0) FROM active_context WHERE session_id=?")?
        .query_row([sid], |r| r.get(0))?)
}

pub fn take_pending(tx: &Connection, sid: SessionId) -> Result<()> {
    let ids = {
        let mut stmt = tx.prepare_cached(
            "SELECT item_id FROM pending_inputs WHERE session_id=?1 ORDER BY item_id",
        )?;
        stmt.query_map([sid], |r| r.get::<_, ItemId>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if !ids.is_empty() {
        append_context_items(tx, sid, &ids, None)?;
    }
    Ok(())
}

pub fn add_item(
    conn: &Connection,
    sid: SessionId,
    kind: &str,
    payload: &Value,
    target: Option<ItemId>,
    llm_call_id: Option<CallId>,
) -> Result<ItemId> {
    let id = ItemId::new()?;
    let origin = crate::cluster::identity::local_node_id(conn)?;
    let seq = next_origin_seq(conn, &session_scope(sid), &origin)?;
    conn.prepare_cached(
        "INSERT INTO items(id,session_id,kind,payload,target_id,llm_call_id,origin_node_id,origin_seq)
         VALUES(?,?,?,?,?,?,?,?)",
    )?
    .execute(params![id, sid, kind, payload, target, llm_call_id, &*origin, seq])?;
    on_item_inserted(conn, sid, id, kind, payload, target)?;
    stage_session(sid);
    Ok(id)
}

pub fn add_context_item_at(
    tx: &Connection,
    sid: SessionId,
    kind: &str,
    item: &Value,
    call_id: CallId,
    expected_head: Option<Option<ContextNodeId>>,
) -> Result<(ItemId, Option<ContextNodeId>)> {
    if let Some(expected) = expected_head {
        let cur: Option<ContextNodeId> = tx
            .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
            .query_row([sid], |r| r.get(0))?;
        if cur != expected {
            return Err(Error::conflict("session branch changed during execution"));
        }
    }
    let id = add_item(tx, sid, kind, item, None, Some(call_id))?;
    let created = append_context_items_at(tx, sid, &[id], expected_head, None)?;
    Ok((id, created.last().copied()))
}

// Publish completed tools for one call (active execution) or for the active branch
// (crash recovery). Never pulls unpublished tool calls from another DAG branch.
pub fn publish_tools(
    tx: &Connection,
    sid: SessionId,
    recovering: bool,
    filter_call_id: Option<CallId>,
    expected_head: Option<Option<ContextNodeId>>,
) -> Result<(Vec<ItemId>, Option<ContextNodeId>)> {
    let mut recovered = Vec::new();
    let mut to_append = Vec::new();
    let mut after: Option<ItemId> = None;
    let cur_head: Option<ContextNodeId> = match expected_head {
        Some(h) => {
            let actual: Option<ContextNodeId> = tx
                .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
                .query_row([sid], |r| r.get(0))?;
            if actual != h {
                return Err(Error::conflict("session branch changed during execution"));
            }
            h
        }
        None => tx
            .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
            .query_row([sid], |r| r.get(0))?,
    };
    let next_row = |after: Option<ItemId>| -> Result<Option<(ItemId, String, Option<ItemId>)>> {
        let after_str = after.as_ref().map_or("", ItemId::as_str);
        if let Some(cid) = filter_call_id {
            let mut stmt = tx.prepare_cached(
                "SELECT c.id, json_extract(c.payload,'$.call_id'),
                        (SELECT r.id FROM items r WHERE r.session_id=c.session_id AND r.kind='function_call_output' AND r.target_id=c.id ORDER BY r.id LIMIT 1)
                 FROM items c
                 WHERE c.session_id=?1 AND c.kind='function_call' AND c.id>?2 AND c.llm_call_id=?3
                   AND NOT EXISTS(SELECT 1 FROM context_nodes n WHERE n.session_id=?1 AND n.item_id=c.id)
                 ORDER BY c.id LIMIT 1",
            )?;
            Ok(stmt
                .query_row(params![sid, after_str, cid], |r| {
                    Ok((
                        r.get::<_, ItemId>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<ItemId>>(2)?,
                    ))
                })
                .optional()?)
        } else {
            let mut stmt = tx.prepare_cached(
                "SELECT c.id, json_extract(c.payload,'$.call_id'),
                        (SELECT r.id FROM items r WHERE r.session_id=c.session_id AND r.kind='function_call_output' AND r.target_id=c.id ORDER BY r.id LIMIT 1)
                 FROM items c
                 WHERE c.session_id=?1 AND c.kind='function_call' AND c.id>?2
                   AND NOT EXISTS(SELECT 1 FROM context_nodes n WHERE n.session_id=?1 AND n.item_id=c.id)
                   AND EXISTS(
                     SELECT 1 FROM llm_calls lc
                     WHERE lc.id = c.llm_call_id
                       AND (
                         lc.input_head IS ?3
                         OR (?3 IS NOT NULL AND EXISTS(
                             SELECT 1 FROM active_context ac
                             JOIN items ai ON ai.id = ac.item_id
                             WHERE ac.session_id = ?1 AND ac.node_id = ?3 AND ai.llm_call_id = lc.id
                         ))
                       )
                   )
                 ORDER BY c.id LIMIT 1",
            )?;
            Ok(stmt
                .query_row(params![sid, after_str, cur_head], |r| {
                    Ok((
                        r.get::<_, ItemId>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<ItemId>>(2)?,
                    ))
                })
                .optional()?)
        }
    };
    while let Some((id, call, output)) = next_row(after)? {
        after = Some(id);
        let result = match output {
            Some(id) => id,
            None if recovering => {
                let payload = json!({"type":"function_call_output","call_id":call,"output":"Execution interrupted; result unknown."});
                let result_id = add_item(tx, sid, "function_call_output", &payload, Some(id), None)?;
                recovered.push(result_id);
                result_id
            }
            None => return Err(Error::internal("missing tool result")),
        };
        to_append.push(id);
        to_append.push(result);
    }
    if to_append.is_empty() {
        return Ok((recovered, cur_head));
    }
    let created = append_context_items_at(tx, sid, &to_append, expected_head, None)?;
    Ok((recovered, created.last().copied().or(cur_head)))
}

const BATCH_BYTES: usize = 64 * 1024;
const HISTORY_BRANCH_FILTER: &str = " AND (EXISTS(SELECT 1 FROM active_context ac WHERE ac.session_id=items.session_id AND ac.item_id=items.id) \
      OR NOT EXISTS(SELECT 1 FROM context_nodes cn WHERE cn.session_id=items.session_id AND cn.item_id=items.id))";

fn has_inactive_branch(conn: &Connection, sid: SessionId) -> Result<bool> {
    Ok(conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM session_leaves l
             WHERE l.session_id=?1 AND l.head_id != coalesce((SELECT active_head FROM sessions WHERE id=?1), ''))",
        )?
        .query_row([sid], |r| r.get(0))?)
}

#[derive(Clone, Copy)]
pub enum JsonRows {
    History,
    Context,
}

impl JsonRows {
    // Capture an immutable snapshot list of <= 200 keys in one DB read so branch switches
    // cannot mix two branches or lose rows while streaming response chunks.
    pub fn history_snapshot(
        conn: &Connection,
        sid: SessionId,
        after: Option<ItemId>,
        before: Option<ItemId>,
        tail: bool,
        limit: i64,
    ) -> Result<Vec<ItemId>> {
        let extra = if has_inactive_branch(conn, sid)? {
            HISTORY_BRANCH_FILTER
        } else {
            ""
        };
        let mut ids: Vec<ItemId> = if tail {
            let sql = format!(
                "SELECT id FROM items WHERE session_id=?1{extra} ORDER BY id DESC LIMIT ?2"
            );
            conn.prepare_cached(&sql)?
                .query_map((sid, limit), |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
        } else if let Some(before) = before {
            let sql = format!(
                "SELECT id FROM items WHERE session_id=?1 AND id<?2{extra} ORDER BY id DESC LIMIT ?3"
            );
            conn.prepare_cached(&sql)?
                .query_map((sid, before, limit), |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
        } else {
            let sql = format!(
                "SELECT id FROM items WHERE session_id=?1 AND id>?2{extra} ORDER BY id ASC LIMIT ?3"
            );
            conn.prepare_cached(&sql)?
                .query_map(
                    params![sid, after.as_ref().map_or("", ItemId::as_str), limit],
                    |r| r.get(0),
                )?
                .collect::<rusqlite::Result<_>>()?
        };
        if tail || before.is_some() {
            ids.reverse();
        }
        Ok(ids)
    }

    pub fn context_snapshot(
        conn: &Connection,
        sid: SessionId,
        after: Option<i64>,
        before: Option<i64>,
        tail: bool,
        limit: i64,
    ) -> Result<Vec<(i64, ItemId)>> {
        let mut rows: Vec<(i64, ItemId)> = if tail {
            conn.prepare_cached(
                "SELECT position, item_id FROM active_context WHERE session_id=?1 ORDER BY position DESC LIMIT ?2",
            )?
            .query_map((sid, limit), |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?
        } else if let Some(before) = before {
            conn.prepare_cached(
                "SELECT position, item_id FROM active_context WHERE session_id=?1 AND position<?2 ORDER BY position DESC LIMIT ?3",
            )?
            .query_map((sid, before, limit), |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?
        } else {
            conn.prepare_cached(
                "SELECT position, item_id FROM active_context WHERE session_id=?1 AND position>?2 ORDER BY position ASC LIMIT ?3",
            )?
            .query_map((sid, after.unwrap_or(0), limit), |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?
        };
        if tail || before.is_some() {
            rows.reverse();
        }
        Ok(rows)
    }

    fn history_chunk(
        conn: &Connection,
        items: &[ItemId],
        offset: usize,
        mut bytes: Vec<u8>,
        mut comma: bool,
    ) -> Result<(Vec<u8>, usize)> {
        let mut stmt = conn.prepare_cached(
            "SELECT json_object('id',id,'kind',kind,'payload',json(payload),'target_id',target_id,'llm_call_id',llm_call_id)
             FROM items WHERE id=?1",
        )?;
        let mut next = offset;
        for &id in &items[offset..] {
            let raw_str: String = stmt
                .query_row([id], |r| r.get(0))
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => {
                        Error::internal("missing row in saved history")
                    }
                    other => other.into(),
                })?;
            let raw = raw_str.as_bytes();
            if next > offset && bytes.len() + raw.len() + 1 > BATCH_BYTES {
                break;
            }
            if comma {
                bytes.push(b',');
            }
            bytes.extend_from_slice(raw);
            comma = true;
            next += 1;
            if bytes.len() >= BATCH_BYTES {
                break;
            }
        }
        Ok((bytes, next))
    }

    fn context_chunk(
        conn: &Connection,
        items: &[(i64, ItemId)],
        offset: usize,
        mut bytes: Vec<u8>,
        mut comma: bool,
    ) -> Result<(Vec<u8>, usize)> {
        let mut stmt = conn.prepare_cached(
            "SELECT json_object('position',?1,'item_id',id,'llm_call_id',llm_call_id,'payload',json(payload))
             FROM items WHERE id=?2",
        )?;
        let mut next = offset;
        for &(pos, id) in &items[offset..] {
            let raw_str: String = stmt
                .query_row(params![pos, id], |r| r.get(0))
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => {
                        Error::internal("missing row in saved history")
                    }
                    other => other.into(),
                })?;
            let raw = raw_str.as_bytes();
            if next > offset && bytes.len() + raw.len() + 1 > BATCH_BYTES {
                break;
            }
            if comma {
                bytes.push(b',');
            }
            bytes.extend_from_slice(raw);
            comma = true;
            next += 1;
            if bytes.len() >= BATCH_BYTES {
                break;
            }
        }
        Ok((bytes, next))
    }

    pub async fn history_body(self, db: Db, items: Vec<ItemId>) -> Result<Body> {
        let mut prefix = vec![b'['];
        let total = items.len();
        if total == 0 {
            prefix.push(b']');
            return Ok(Body::from(prefix));
        }
        let items = Arc::new(items);
        let first_items = items.clone();
        let (mut bytes, mut next) = db
            .call(move |s| Self::history_chunk(&s.conn, &first_items, 0, prefix, false))
            .await?;
        if next >= total {
            bytes.push(b']');
            return Ok(Body::from(bytes));
        }
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Error>(Bytes::from(bytes));
            while next < total {
                let batch_items = items.clone();
                let res = db
                    .call(move |s| Self::history_chunk(&s.conn, &batch_items, next, Vec::new(), true))
                    .await;
                match res {
                    Ok((mut chunk, next_idx)) => {
                        next = next_idx;
                        if next >= total {
                            chunk.push(b']');
                        }
                        yield Ok(Bytes::from(chunk));
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

    pub async fn context_body(
        self,
        db: Db,
        items: Vec<(i64, ItemId)>,
        mut prefix: Vec<u8>,
        suffix: &'static [u8],
    ) -> Result<Body> {
        let total = items.len();
        if total == 0 {
            prefix.extend_from_slice(suffix);
            return Ok(Body::from(prefix));
        }
        let items = Arc::new(items);
        let first_items = items.clone();
        let (mut bytes, mut next) = db
            .call(move |s| Self::context_chunk(&s.conn, &first_items, 0, prefix, false))
            .await?;
        if next >= total {
            bytes.extend_from_slice(suffix);
            return Ok(Body::from(bytes));
        }
        let stream = async_stream::stream! {
            yield Ok::<Bytes, Error>(Bytes::from(bytes));
            while next < total {
                let batch_items = items.clone();
                let res = db
                    .call(move |s| Self::context_chunk(&s.conn, &batch_items, next, Vec::new(), true))
                    .await;
                match res {
                    Ok((mut chunk, next_idx)) => {
                        next = next_idx;
                        if next >= total {
                            chunk.extend_from_slice(suffix);
                        }
                        yield Ok(Bytes::from(chunk));
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
