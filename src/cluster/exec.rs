use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};

use iroh::endpoint::{Connection, RecvStream, SendStream};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    App, Error, Result,
    cluster::{
        identity::{PeerInfo, ResolvedNode, list_peers, resolve_node_target},
        ledger::{
            AttachmentRef, StreamHead, assert_no_conflict, has_conflict,
            is_session_writable_local, stream_head_wire,
        },
        net::ClusterNet,
        sync::SyncEngine,
    },
    codemode,
    events::{NextSessionNotice, SessionEventStream},
    id::{ItemId, SessionId},
    media,
    store::{self, Db, Session, SessionStatus, session},
};

const EXEC_REQ_HEADER_MAX: usize = 8 * 1024 * 1024;
const EXEC_REQ_BODY_MAX: usize = 1024 * 1024;
const EXEC_RESP_HEADER_MAX: usize = 8 * 1024 * 1024;
const EXEC_RESP_BODY_MAX: usize = 1024 * 1024;
const EXEC_BYTE_UNIT: usize = 64 * 1024;
const MAX_EXEC_BYTE_PERMITS: usize = 512; // 32 MiB byte budget
const MAX_GLOBAL_INBOUND_EXEC: usize = 128;
const MAX_PEER_INBOUND_EXEC: usize = 64;
const MAX_REMOTE_SSE_SUBS: usize = 64;

const EXEC_REQ_READ_TIMEOUT: Duration = Duration::from_secs(15);
const EXEC_SHORT_TIMEOUT: Duration = Duration::from_secs(10);
const EXEC_OP_TIMEOUT: Duration = Duration::from_secs(30);
const CLAIM_TTL: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOccupancy {
    pub session_id: SessionId,
    pub status: SessionStatus,
    #[serde(default)]
    pub reason: String,
    pub active_tasks: usize,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunClaim {
    pub claim_id: String,
    pub node_id: String,
    pub started_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalClaimPhase {
    Probing,
    Committing,
    Yielded(String),
}

struct LocalClaim {
    claim_id: String,
    phase: LocalClaimPhase,
    acquired_at: Instant,
}

struct PresenceInner {
    instance_id: u64,
    local_seq: u64,
    local: HashMap<SessionId, SessionOccupancy>,
    remote_seq: HashMap<String, (u64, u64)>,
    remote: HashMap<String, HashMap<SessionId, SessionOccupancy>>,
    local_claims: HashMap<SessionId, LocalClaim>,
    remote_claims: HashMap<SessionId, (RunClaim, Instant)>,
    session_watchers: HashMap<SessionId, watch::Sender<Option<String>>>,
}

pub struct PresenceState {
    local_node_id: Mutex<String>,
    inner: Mutex<PresenceInner>,
    local_changed: Notify,
}

impl PresenceState {
    pub fn new() -> Arc<Self> {
        let mut raw = [0u8; 8];
        let _ = getrandom::fill(&mut raw);
        let instance_id = u64::from_le_bytes(raw).max(1);
        Arc::new(Self {
            local_node_id: Mutex::new(String::new()),
            inner: Mutex::new(PresenceInner {
                instance_id,
                local_seq: 1,
                local: HashMap::new(),
                remote_seq: HashMap::new(),
                remote: HashMap::new(),
                local_claims: HashMap::new(),
                remote_claims: HashMap::new(),
                session_watchers: HashMap::new(),
            }),
            local_changed: Notify::new(),
        })
    }

    pub fn set_local_node_id(&self, node_id: String) {
        *self.local_node_id.lock().unwrap() = node_id;
    }

    pub fn local_node_id(&self) -> String {
        self.local_node_id.lock().unwrap().clone()
    }

    fn remote_owner_locked(inner: &PresenceInner, sid: SessionId) -> Option<(String, SessionOccupancy)> {
        let mut best: Option<(String, SessionOccupancy)> = None;
        for (peer_id, map) in &inner.remote {
            if let Some(occ) = map.get(&sid)
                && best
                    .as_ref()
                    .is_none_or(|(best_id, _)| peer_id.as_str() < best_id.as_str())
            {
                best = Some((peer_id.clone(), occ.clone()));
            }
        }
        best
    }

    fn notify_watcher_locked(inner: &mut PresenceInner, sid: SessionId) {
        let next = Self::remote_owner_locked(inner, sid).map(|(id, _)| id);
        if let Some(tx) = inner.session_watchers.get(&sid) {
            if tx.receiver_count() == 0 {
                inner.session_watchers.remove(&sid);
            } else {
                tx.send_if_modified(|cur| {
                    if *cur != next {
                        *cur = next;
                        true
                    } else {
                        false
                    }
                });
            }
        }
    }

    fn prune_expired_claims_locked(inner: &mut PresenceInner, now: Instant) {
        inner
            .local_claims
            .retain(|_, c| now.saturating_duration_since(c.acquired_at) < CLAIM_TTL);
        inner
            .remote_claims
            .retain(|_, (_, at)| now.saturating_duration_since(*at) < CLAIM_TTL);
    }

    pub fn set_local(&self, sid: SessionId, next: Option<SessionOccupancy>) -> bool {
        let changed = {
            let mut inner = self.inner.lock().unwrap();
            let cur = inner.local.get(&sid);
            if cur == next.as_ref() {
                false
            } else {
                match next {
                    Some(occ) => {
                        inner.local.insert(sid, occ);
                    }
                    None => {
                        inner.local.remove(&sid);
                    }
                }
                inner.local_seq = inner.local_seq.saturating_add(1);
                true
            }
        };
        if changed {
            self.local_changed.notify_one();
        }
        changed
    }

    pub fn local_owner(&self, sid: SessionId) -> Option<SessionOccupancy> {
        self.inner.lock().unwrap().local.get(&sid).cloned()
    }

    pub fn remote_owner(&self, sid: SessionId) -> Option<(String, SessionOccupancy)> {
        let inner = self.inner.lock().unwrap();
        Self::remote_owner_locked(&inner, sid)
    }

    pub fn remote_owner_excluding(
        &self,
        sid: SessionId,
        exclude_peer: &str,
    ) -> Option<(String, SessionOccupancy)> {
        let inner = self.inner.lock().unwrap();
        let mut best: Option<(String, SessionOccupancy)> = None;
        for (peer_id, map) in &inner.remote {
            if peer_id == exclude_peer {
                continue;
            }
            if let Some(occ) = map.get(&sid)
                && best
                    .as_ref()
                    .is_none_or(|(best_id, _)| peer_id.as_str() < best_id.as_str())
            {
                best = Some((peer_id.clone(), occ.clone()));
            }
        }
        best
    }

    pub fn active_claim_holder_excluding(&self, sid: SessionId, exclude_peer: &str) -> Option<String> {
        let now = Instant::now();
        let local_id = self.local_node_id();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if let Some(c) = inner.local_claims.get(&sid)
            && matches!(c.phase, LocalClaimPhase::Probing | LocalClaimPhase::Committing)
        {
            return Some(local_id);
        }
        if let Some((claim, _)) = inner.remote_claims.get(&sid)
            && claim.node_id != exclude_peer
        {
            return Some(claim.node_id.clone());
        }
        None
    }

    pub fn snapshot_local(&self) -> (u64, u64, Vec<SessionOccupancy>) {
        let inner = self.inner.lock().unwrap();
        let mut list: Vec<SessionOccupancy> = inner.local.values().cloned().collect();
        list.sort_by_key(|o| o.session_id);
        (inner.instance_id, inner.local_seq, list)
    }

    pub fn apply_remote(
        &self,
        peer_id: &str,
        instance_id: u64,
        seq: u64,
        sessions: Vec<SessionOccupancy>,
        clear_claim: Option<(SessionId, String)>,
    ) -> Vec<SessionId> {
        let mut inner = self.inner.lock().unwrap();
        if let Some((claim_sid, claim_id)) = clear_claim
            && inner
                .remote_claims
                .get(&claim_sid)
                .is_some_and(|(c, _)| c.node_id == peer_id && c.claim_id == claim_id)
        {
            inner.remote_claims.remove(&claim_sid);
        }
        if let Some(&(prev_inst, prev_seq)) = inner.remote_seq.get(peer_id)
            && instance_id != 0
            && prev_inst == instance_id
            && seq < prev_seq
        {
            return Vec::new();
        }
        inner
            .remote_seq
            .insert(peer_id.to_owned(), (instance_id, seq));
        let mut next_map = HashMap::with_capacity(sessions.len());
        for occ in sessions {
            next_map.insert(occ.session_id, occ);
        }
        let prev_map = inner.remote.remove(peer_id).unwrap_or_default();
        let mut affected = HashSet::new();
        for (sid, prev_occ) in &prev_map {
            if next_map.get(sid) != Some(prev_occ) {
                affected.insert(*sid);
            }
        }
        for (sid, next_occ) in &next_map {
            if prev_map.get(sid) != Some(next_occ) {
                affected.insert(*sid);
            }
        }
        if !next_map.is_empty() {
            inner.remote.insert(peer_id.to_owned(), next_map);
        }
        let changed: Vec<SessionId> = affected.into_iter().collect();
        for &sid in &changed {
            Self::notify_watcher_locked(&mut inner, sid);
        }
        changed
    }

    pub fn clear_peer(&self, peer_id: &str) -> Vec<SessionId> {
        let mut inner = self.inner.lock().unwrap();
        inner.remote_seq.remove(peer_id);
        inner
            .remote_claims
            .retain(|_, (claim, _)| claim.node_id != peer_id);
        let removed = inner.remote.remove(peer_id).unwrap_or_default();
        let changed: Vec<SessionId> = removed.into_keys().collect();
        for &sid in &changed {
            Self::notify_watcher_locked(&mut inner, sid);
        }
        changed
    }

    pub fn clear_all_remote(&self) -> Vec<SessionId> {
        let mut inner = self.inner.lock().unwrap();
        inner.remote_seq.clear();
        inner.remote_claims.clear();
        let mut affected = HashSet::new();
        for (_, map) in inner.remote.drain() {
            affected.extend(map.into_keys());
        }
        let changed: Vec<SessionId> = affected.into_iter().collect();
        for &sid in &changed {
            Self::notify_watcher_locked(&mut inner, sid);
        }
        changed
    }

    pub fn try_acquire_local_claim(&self, sid: SessionId) -> std::result::Result<String, String> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| "entropy unavailable".to_owned())?;
        let claim_id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if inner.local_claims.contains_key(&sid) {
            return Err("session run is already starting on this node".to_owned());
        }
        if let Some((claim, _)) = inner.remote_claims.get(&sid) {
            return Err(format!(
                "session run is already starting on node {}",
                claim.node_id
            ));
        }
        inner.local_claims.insert(
            sid,
            LocalClaim {
                claim_id: claim_id.clone(),
                phase: LocalClaimPhase::Probing,
                acquired_at: now,
            },
        );
        Ok(claim_id)
    }

    pub fn release_local_claim(&self, sid: SessionId, claim_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .local_claims
            .get(&sid)
            .is_some_and(|c| c.claim_id == claim_id)
        {
            inner.local_claims.remove(&sid);
        }
    }

    pub fn accept_remote_claim(
        &self,
        sid: SessionId,
        claim: RunClaim,
        local_occupied: bool,
    ) -> std::result::Result<(), Option<String>> {
        let now = Instant::now();
        let local_id = self.local_node_id();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if local_occupied {
            return Err(Some(local_id));
        }
        if let Some((cur, _)) = inner.remote_claims.get(&sid)
            && (cur.node_id != claim.node_id || cur.claim_id != claim.claim_id)
        {
            return Err(Some(cur.node_id.clone()));
        }
        match inner.local_claims.get_mut(&sid) {
            Some(local) => match &local.phase {
                LocalClaimPhase::Probing => {
                    if claim.node_id < local_id {
                        local.phase = LocalClaimPhase::Yielded(claim.node_id.clone());
                        inner.remote_claims.insert(sid, (claim, now));
                        Ok(())
                    } else {
                        Err(Some(local_id))
                    }
                }
                LocalClaimPhase::Committing => Err(Some(local_id)),
                LocalClaimPhase::Yielded(winner) => {
                    if claim.node_id == *winner {
                        inner.remote_claims.insert(sid, (claim, now));
                        Ok(())
                    } else {
                        Err(Some(winner.clone()))
                    }
                }
            },
            None => {
                inner.remote_claims.insert(sid, (claim, now));
                Ok(())
            }
        }
    }

    pub fn promote_local_claim_to_committing(
        &self,
        sid: SessionId,
        claim_id: &str,
    ) -> std::result::Result<(), String> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let remote_holder = inner
            .remote_claims
            .get(&sid)
            .map(|(rc, _)| rc.node_id.clone());
        let Some(local) = inner.local_claims.get_mut(&sid) else {
            return Err("session run claim expired; retry".to_owned());
        };
        if local.claim_id != claim_id {
            return Err("session run claim expired; retry".to_owned());
        }
        match &local.phase {
            LocalClaimPhase::Probing => {
                if let Some(holder) = remote_holder {
                    return Err(format!("session is currently occupied on node {holder}"));
                }
                local.phase = LocalClaimPhase::Committing;
                local.acquired_at = now;
                Ok(())
            }
            LocalClaimPhase::Yielded(winner) => Err(format!(
                "session is currently occupied on node {winner}"
            )),
            LocalClaimPhase::Committing => Ok(()),
        }
    }

    pub fn release_remote_claim(&self, sid: SessionId, peer_id: &str, claim_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .remote_claims
            .get(&sid)
            .is_some_and(|(c, _)| c.node_id == peer_id && c.claim_id == claim_id)
        {
            inner.remote_claims.remove(&sid);
        }
    }

    pub fn watch_remote_runner(&self, sid: SessionId) -> watch::Receiver<Option<String>> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .session_watchers
            .retain(|_, tx| tx.receiver_count() > 0);
        if let Some(tx) = inner.session_watchers.get(&sid) {
            let mut rx = tx.subscribe();
            rx.borrow_and_update();
            return rx;
        }
        let initial = Self::remote_owner_locked(&inner, sid).map(|(id, _)| id);
        let (tx, mut rx) = watch::channel(initial);
        rx.borrow_and_update();
        inner.session_watchers.insert(sid, tx);
        rx
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ExecReq {
    PresenceSync {
        instance_id: u64,
        seq: u64,
        sessions: Vec<SessionOccupancy>,
        #[serde(default)]
        want_reply: bool,
        #[serde(default)]
        clear_claim: Option<(SessionId, String)>,
    },
    CheckOccupancy {
        session_id: SessionId,
        #[serde(default)]
        claim: Option<RunClaim>,
    },
    ReleaseClaim {
        session_id: SessionId,
        claim_id: String,
    },
    StartRun {
        session_id: SessionId,
    },
    PauseRun {
        session_id: SessionId,
        hard: bool,
    },
    EnqueueMessage {
        session_id: SessionId,
        content: Value,
        idempotency_key: Option<String>,
        #[serde(default)]
        attachments: Vec<AttachmentRef>,
    },
    WithdrawMessage {
        session_id: SessionId,
        message_id: ItemId,
    },
    PatchSession {
        session_id: SessionId,
        fields: Map<String, Value>,
    },
    ListTasks {
        session_id: SessionId,
    },
    KillTask {
        session_id: SessionId,
        task_id: String,
    },
    SubscribeSessionEvents {
        session_id: SessionId,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ExecResp {
    Ack,
    Presence {
        instance_id: u64,
        seq: u64,
        sessions: Vec<SessionOccupancy>,
    },
    Occupancy {
        occupied: bool,
        runner_node_id: Option<String>,
        status: Option<SessionStatus>,
        active_tasks: usize,
        head: Option<StreamHead>,
        has_conflict: bool,
    },
    Session {
        session: Value,
    },
    MessageEnqueued {
        id: ItemId,
        created: bool,
    },
    Tasks {
        tasks: Value,
    },
    TaskKilled {
        result: Value,
    },
    SseEvent {
        event: String,
        data: String,
    },
    SseEnd,
    Error {
        status: u16,
        message: String,
    },
}

#[derive(Clone, Debug)]
pub enum WriterTarget {
    Local,
    Remote(String),
}

pub enum RelayedEvent {
    Event { event: String, data: String },
    Disconnected,
}

enum ReadExecErr {
    Protocol(String),
    Transport(String),
}

async fn write_exec_frame(
    send: &mut SendStream,
    header: &impl Serialize,
) -> std::result::Result<(), String> {
    let hdr_bytes = serde_json::to_vec(header).map_err(|e| format!("exec encode failed: {e}"))?;
    let h_len = u32::try_from(hdr_bytes.len()).map_err(|_| "exec header too large".to_owned())?;
    let mut buf = Vec::with_capacity(8 + hdr_bytes.len());
    buf.extend_from_slice(&h_len.to_be_bytes());
    buf.extend_from_slice(&hdr_bytes);
    buf.extend_from_slice(&0u32.to_be_bytes());
    send.write_all(&buf)
        .await
        .map_err(|e| format!("exec stream write failed: {e}"))?;
    Ok(())
}

async fn read_exec_frame<T: DeserializeOwned>(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
    byte_budget: &Arc<Semaphore>,
) -> std::result::Result<T, ReadExecErr> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| ReadExecErr::Transport(format!("exec read header length failed: {e}")))?;
    let h_len = u32::from_be_bytes(len_buf) as usize;
    if h_len > max_header {
        return Err(ReadExecErr::Protocol(
            "exec frame header exceeds limit".into(),
        ));
    }
    let h_permits = u32::try_from(h_len.div_ceil(EXEC_BYTE_UNIT).max(1))
        .map_err(|_| ReadExecErr::Protocol("exec header permit overflow".into()))?;
    let _hdr_permit: OwnedSemaphorePermit = byte_budget
        .clone()
        .acquire_many_owned(h_permits)
        .await
        .map_err(|_| ReadExecErr::Transport("exec byte budget closed".into()))?;
    let mut header = vec![0u8; h_len];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| ReadExecErr::Transport(format!("exec read header failed: {e}")))?;
    let parsed: T = serde_json::from_slice(&header)
        .map_err(|e| ReadExecErr::Protocol(format!("invalid exec frame header: {e}")))?;

    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| ReadExecErr::Transport(format!("exec read body length failed: {e}")))?;
    let b_len = u32::from_be_bytes(len_buf) as usize;
    if b_len > max_body {
        return Err(ReadExecErr::Protocol(
            "exec frame body exceeds limit".into(),
        ));
    }
    if b_len > 0 {
        let mut body = vec![0u8; b_len];
        recv.read_exact(&mut body)
            .await
            .map_err(|e| ReadExecErr::Transport(format!("exec read body failed: {e}")))?;
    }
    Ok(parsed)
}

fn wire_error(status: u16, message: String) -> Error {
    let code = reqwest::StatusCode::from_u16(status)
        .ok()
        .filter(|s| s.is_client_error() || s.is_server_error())
        .map(|s| axum::http::StatusCode::from_u16(s.as_u16()).unwrap())
        .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
    Error(code, message)
}

pub struct ExecPlane {
    db: Db,
    net: Arc<ClusterNet>,
    presence: Arc<PresenceState>,
    sync: Arc<SyncEngine>,
    stopping: CancellationToken,
    workers: TaskTracker,
    byte_budget: Arc<Semaphore>,
    start_locks: Mutex<HashMap<SessionId, Weak<AsyncMutex<()>>>>,
    peer_sync_wake: Arc<Notify>,
    pending_peer_sync: Mutex<HashSet<String>>,
    self_weak: OnceLock<Weak<ExecPlane>>,
}

impl ExecPlane {
    pub fn new(
        db: Db,
        net: Arc<ClusterNet>,
        presence: Arc<PresenceState>,
        sync: Arc<SyncEngine>,
        stopping: CancellationToken,
        workers: TaskTracker,
    ) -> Arc<Self> {
        let arc = Arc::new(Self {
            db,
            net,
            presence,
            sync,
            stopping,
            workers,
            byte_budget: Arc::new(Semaphore::new(MAX_EXEC_BYTE_PERMITS)),
            start_locks: Mutex::default(),
            peer_sync_wake: Arc::new(Notify::new()),
            pending_peer_sync: Mutex::default(),
            self_weak: OnceLock::new(),
        });
        let _ = arc.self_weak.set(Arc::downgrade(&arc));
        arc
    }

    pub fn spawn_loops(self: &Arc<Self>) {
        let plane_bcast = self.clone();
        self.workers.spawn(async move {
            plane_bcast.presence_broadcast_loop().await;
        });
        let plane_sync = self.clone();
        self.workers.spawn(async move {
            plane_sync.peer_presence_sync_loop().await;
        });
    }

    pub fn presence(&self) -> &Arc<PresenceState> {
        &self.presence
    }

    pub fn remote_runner_id(&self, sid: SessionId) -> Option<String> {
        self.presence.remote_owner(sid).map(|(id, _)| id)
    }

    fn start_lock(&self, sid: SessionId) -> Arc<AsyncMutex<()>> {
        let mut map = self.start_locks.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(lock) = map.get(&sid).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        map.insert(sid, Arc::downgrade(&lock));
        lock
    }

    pub fn on_peer_online(&self, peer_id: String) {
        {
            self.pending_peer_sync.lock().unwrap().insert(peer_id);
        }
        self.peer_sync_wake.notify_one();
    }

    pub fn on_peer_offline(&self, peer_id: &str) {
        self.pending_peer_sync.lock().unwrap().remove(peer_id);
        let changed = self.presence.clear_peer(peer_id);
        self.publish_presence_changes(changed);
    }

    pub fn on_membership_switched(&self) {
        self.pending_peer_sync.lock().unwrap().clear();
        let changed = self.presence.clear_all_remote();
        self.publish_presence_changes(changed);
    }

    fn publish_presence_changes(&self, changed: Vec<SessionId>) {
        if changed.is_empty() || self.stopping.is_cancelled() {
            return;
        }
        let db = self.db.clone();
        self.workers.spawn(async move {
            let _ = db
                .call(move |s| {
                    for sid in changed {
                        if let Ok(info) = session(&s.conn, sid) {
                            let view = s.session_view(info);
                            s.notify_status(
                                sid,
                                view.status.as_str(),
                                &view.last_error,
                                view.updated_at,
                            );
                        }
                    }
                    Ok(())
                })
                .await;
        });
    }

    fn apply_remote_presence(
        &self,
        peer_id: &str,
        instance_id: u64,
        seq: u64,
        sessions: Vec<SessionOccupancy>,
        clear_claim: Option<(SessionId, String)>,
    ) {
        let changed = self
            .presence
            .apply_remote(peer_id, instance_id, seq, sessions, clear_claim);
        self.publish_presence_changes(changed);
    }

    async fn presence_broadcast_loop(&self) {
        let mut last_sent = 0u64;
        loop {
            tokio::select! {
                biased;
                _ = self.stopping.cancelled() => return,
                _ = self.presence.local_changed.notified() => (),
            }
            if self.stopping.is_cancelled() {
                return;
            }
            let (instance_id, seq, sessions) = self.presence.snapshot_local();
            if seq == last_sent {
                continue;
            }
            last_sent = seq;
            let online = self.net.online_peer_ids();
            if online.is_empty() {
                continue;
            }
            let req = ExecReq::PresenceSync {
                instance_id,
                seq,
                sessions,
                want_reply: false,
                clear_claim: None,
            };
            futures_util::future::join_all(online.into_iter().map(|peer_id| {
                let req_ref = &req;
                async move {
                    let _ = self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await;
                }
            }))
            .await;
        }
    }

    async fn broadcast_presence_now(
        &self,
        extra_peers: &[String],
        clear_claim: Option<(SessionId, String)>,
    ) {
        let (instance_id, seq, sessions) = self.presence.snapshot_local();
        let mut targets = self.net.online_peer_ids();
        for peer_id in extra_peers {
            if self.net.is_peer_online(peer_id) && !targets.contains(peer_id) {
                targets.push(peer_id.clone());
            }
        }
        if targets.is_empty() {
            return;
        }
        let req = ExecReq::PresenceSync {
            instance_id,
            seq,
            sessions,
            want_reply: false,
            clear_claim,
        };
        futures_util::future::join_all(targets.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move {
                let _ = self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await;
            }
        }))
        .await;
    }

    async fn peer_presence_sync_loop(&self) {
        loop {
            let peers: Vec<String> = {
                self.pending_peer_sync
                    .lock()
                    .unwrap()
                    .drain()
                    .collect()
            };
            if peers.is_empty() {
                tokio::select! {
                    biased;
                    _ = self.stopping.cancelled() => return,
                    _ = self.peer_sync_wake.notified() => (),
                }
                continue;
            }
            let (instance_id, seq, sessions) = self.presence.snapshot_local();
            let req = ExecReq::PresenceSync {
                instance_id,
                seq,
                sessions,
                want_reply: true,
                clear_claim: None,
            };
            futures_util::future::join_all(peers.into_iter().map(|peer_id| {
                let req_ref = &req;
                async move {
                    if !self.net.is_peer_online(&peer_id) {
                        return;
                    }
                    if let Ok(ExecResp::Presence {
                        instance_id: r_inst,
                        seq: r_seq,
                        sessions: r_sessions,
                    }) = self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await
                    {
                        self.apply_remote_presence(&peer_id, r_inst, r_seq, r_sessions, None);
                    }
                }
            }))
            .await;
        }
    }

    async fn peer_info(&self, peer_id: &str) -> Result<PeerInfo> {
        let target = peer_id.to_owned();
        let peer = self
            .db
            .call(move |s| {
                let peers = list_peers(&s.conn)?;
                Ok(peers.into_iter().find(|p| p.node_id == target))
            })
            .await?
            .ok_or_else(|| Error::conflict(format!("unknown cluster node {peer_id}")))?;
        Ok(peer)
    }

    async fn open_exec_stream(
        &self,
        peer_id: &str,
        cancel: &CancellationToken,
    ) -> Result<(Connection, SendStream, RecvStream)> {
        let peer = self.peer_info(peer_id).await?;
        if !self.net.is_peer_online(&peer.node_id) {
            return Err(Error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("target node '{}' is offline", peer.name),
            ));
        }
        let (conn, peer_cancel) = self
            .net
            .get_or_dial_exec_connection(&peer, cancel)
            .await
            .map_err(|e| Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, e))?;
        let combined = cancel.child_token();
        if peer_cancel.is_cancelled() {
            return Err(Error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("connection to '{}' closed", peer.name),
            ));
        }
        let (send, recv) = tokio::select! {
            biased;
            _ = combined.cancelled() => {
                return Err(Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, "cancelled".into()));
            }
            _ = peer_cancel.cancelled() => {
                return Err(Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, "peer disconnected".into()));
            }
            r = conn.open_bi() => r.map_err(|e| {
                self.net.evict_exec_conn(&peer.node_id, &conn);
                Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, format!("exec stream open failed: {e}"))
            })?,
        };
        Ok((conn, send, recv))
    }

    async fn exec_rpc(&self, peer_id: &str, req: &ExecReq, timeout_dur: Duration) -> Result<ExecResp> {
        let cancel = self.stopping.child_token();
        let (conn, mut send, mut recv) = self.open_exec_stream(peer_id, &cancel).await?;
        let res = tokio::time::timeout(timeout_dur, async {
            if let Err(e) = write_exec_frame(&mut send, req).await {
                self.net.evict_exec_conn(peer_id, &conn);
                return Err(Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, e));
            }
            let _ = send.finish();
            match read_exec_frame::<ExecResp>(
                &mut recv,
                EXEC_RESP_HEADER_MAX,
                EXEC_RESP_BODY_MAX,
                &self.byte_budget,
            )
            .await
            {
                Ok(ExecResp::Error { status, message }) => Err(wire_error(status, message)),
                Ok(resp) => Ok(resp),
                Err(ReadExecErr::Protocol(msg)) => Err(Error::internal(msg)),
                Err(ReadExecErr::Transport(msg)) => {
                    self.net.evict_exec_conn(peer_id, &conn);
                    Err(Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, msg))
                }
            }
        })
        .await;
        match res {
            Ok(inner) => inner,
            Err(_) => Err(Error(
                axum::http::StatusCode::GATEWAY_TIMEOUT,
                format!("remote exec request to {peer_id} timed out"),
            )),
        }
    }

    async fn probe_online_occupancy(
        &self,
        sid: SessionId,
    ) -> Result<Option<(String, SessionOccupancy)>> {
        let online = self.net.online_peer_ids();
        if online.is_empty() {
            return Ok(None);
        }
        let req = ExecReq::CheckOccupancy {
            session_id: sid,
            claim: None,
        };
        let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await) }
        }))
        .await;
        let local_id = self.presence.local_node_id();
        let mut best: Option<(String, SessionOccupancy)> = None;
        for (peer_id, res) in replies {
            if let Ok(ExecResp::Occupancy {
                occupied: true,
                runner_node_id,
                status: Some(status),
                active_tasks,
                ..
            }) = res
            {
                let owner = runner_node_id.unwrap_or(peer_id);
                if owner == local_id {
                    continue;
                }
                if best
                    .as_ref()
                    .is_none_or(|(best_id, _)| owner.as_str() < best_id.as_str())
                {
                    best = Some((
                        owner,
                        SessionOccupancy {
                            session_id: sid,
                            status,
                            reason: String::new(),
                            active_tasks,
                            updated_at: 0,
                        },
                    ));
                }
            }
        }
        Ok(best)
    }

    pub async fn start_run(&self, app: &App, sid: SessionId, node: Option<String>) -> Result<Session> {
        if let Some(raw_target) = node {
            let trimmed = raw_target.trim();
            if trimmed.is_empty() {
                return Err(Error::bad("node must be a nonempty node id or name"));
            }
            let target = trimmed.to_owned();
            let resolved = self
                .db
                .call(move |s| Ok(resolve_node_target(&s.conn, &target)))
                .await?
                .map_err(|(code, msg)| match code {
                    "ENODENOTFOUND" => Error(axum::http::StatusCode::NOT_FOUND, msg),
                    "ENODEAMBIGUOUS" => Error::conflict(msg),
                    _ => Error::internal(msg),
                })?;
            match resolved {
                ResolvedNode::Local { .. } => self.start_run_local(app, sid).await,
                ResolvedNode::Remote(peer) => {
                    if !self.net.is_peer_online(&peer.node_id) {
                        return Err(Error(
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            format!("target node '{}' is offline", peer.name),
                        ));
                    }
                    match self
                        .exec_rpc(&peer.node_id, &ExecReq::StartRun { session_id: sid }, EXEC_OP_TIMEOUT)
                        .await?
                    {
                        ExecResp::Session { session } => {
                            let parsed: Session = serde_json::from_value(session)
                                .map_err(|e| Error::internal(format!("invalid remote session reply: {e}")))?;
                            Ok(parsed)
                        }
                        _ => Err(Error::internal("unexpected remote StartRun response")),
                    }
                }
            }
        } else {
            self.start_run_local(app, sid).await
        }
    }

    pub async fn start_run_local(&self, app: &App, sid: SessionId) -> Result<Session> {
        let lock = self.start_lock(sid);
        let _guard = lock.lock().await;

        // Fast idempotent path if this node ALREADY owns an active worker for this session.
        let fast_local = self
            .db
            .call(move |s| {
                if s.active.contains_key(&sid) {
                    let current = session(&s.conn, sid)?;
                    if current.status == SessionStatus::Pausing {
                        return Err(Error::conflict("session is pausing"));
                    }
                    return Ok(Some(s.session_view(current)));
                }
                Ok(None)
            })
            .await?;
        if let Some(view) = fast_local {
            return Ok(view);
        }

        // Single-node fast path: zero extra cluster RPCs when no peers exist or all are offline.
        if self.net.online_peer_ids().is_empty() {
            self.sync.sync_session_latest(sid).await?;
            self.sync.ensure_session_attachments(sid).await?;
            return app.start_local_checked(sid, None).await;
        }

        // 4-Step Execution Gate across online peers.
        for attempt in 0..2 {
            // Step 1: Cached cluster occupancy check (verified live against cached holder) + local claim reservation.
            if let Some((owner_id, _)) = self.presence.remote_owner(sid)
                && self.net.is_peer_online(&owner_id)
            {
                match self
                    .exec_rpc(
                        &owner_id,
                        &ExecReq::CheckOccupancy {
                            session_id: sid,
                            claim: None,
                        },
                        EXEC_SHORT_TIMEOUT,
                    )
                    .await
                {
                    Ok(ExecResp::Occupancy {
                        occupied: true,
                        runner_node_id,
                        ..
                    }) => {
                        let holder = runner_node_id.unwrap_or(owner_id);
                        return Err(Error::conflict(format!(
                            "session is currently occupied on node {holder}"
                        )));
                    }
                    Ok(ExecResp::Occupancy {
                        occupied: false, ..
                    }) => {}
                    Ok(_) => {
                        return Err(Error::internal("unexpected CheckOccupancy response"));
                    }
                    Err(err) => {
                        if self.net.is_peer_online(&owner_id) {
                            return Err(err);
                        }
                    }
                }
            }
            let claim_id = self
                .presence
                .try_acquire_local_claim(sid)
                .map_err(Error::conflict)?;
            let mut claim_guard = ClaimReleaseGuard::new(
                self.presence.clone(),
                self.self_weak.get().and_then(Weak::upgrade),
                sid,
                claim_id.clone(),
            );

            // Step 2: Watch and synchronize latest session history + validate preconditions.
            self.sync.sync_session_latest(sid).await?;
            let (expected_head, local_id) = self
                .db
                .call(move |s| {
                    let current = session(&s.conn, sid)?;
                    crate::catalog::available_capabilities(&s.conn, current.model_id)?;
                    if current.status == SessionStatus::Pausing {
                        return Err(Error::conflict("session is pausing"));
                    }
                    assert_no_conflict(&s.conn, sid.as_str())?;
                    if !is_session_writable_local(&s.conn, sid)? {
                        return Err(Error::conflict(
                            "session history is incomplete or read-only locally; sync before running",
                        ));
                    }
                    let head = stream_head_wire(&s.conn, sid.as_str())?;
                    Ok((head, s.local_node_id.clone()))
                })
                .await?;

            // Step 3: Ensure all referenced context and pending attachments exist locally before starting.
            self.sync.ensure_session_attachments(sid).await?;

            // Step 4: Broadcast pre-run claim & check occupancy + stream head freshness on online peers.
            let online = self.net.online_peer_ids();
            if online.is_empty() {
                self.presence
                    .promote_local_claim_to_committing(sid, &claim_id)
                    .map_err(Error::conflict)?;
                let res = app.start_local_checked(sid, expected_head).await?;
                claim_guard.release_local_only();
                return Ok(res);
            }
            let claim = RunClaim {
                claim_id: claim_id.clone(),
                node_id: local_id,
                started_ms: chrono::Utc::now().timestamp_millis(),
            };
            let req = ExecReq::CheckOccupancy {
                session_id: sid,
                claim: Some(claim),
            };
            let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
                let req_ref = &req;
                async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await) }
            }))
            .await;

            let mut need_resync = false;
            for (peer_id, reply) in replies {
                match reply {
                    Ok(ExecResp::Occupancy {
                        occupied,
                        runner_node_id,
                        head: remote_head,
                        has_conflict: remote_conflict,
                        ..
                    }) => {
                        if occupied {
                            let holder = runner_node_id.unwrap_or(peer_id);
                            return Err(Error::conflict(format!(
                                "session is currently occupied on node {holder}"
                            )));
                        }
                        claim_guard.track_peer(peer_id.clone());
                        if remote_conflict {
                            return Err(Error::conflict(format!(
                                "session has an unresolved sync conflict on node {peer_id}"
                            )));
                        }
                        if is_remote_head_newer_or_diverged(expected_head.as_ref(), remote_head.as_ref()) {
                            need_resync = true;
                        }
                    }
                    Ok(_) => {
                        return Err(Error::internal("unexpected CheckOccupancy response"));
                    }
                    Err(err) => {
                        // Optimistic availability: only fail if peer is still online.
                        if self.net.is_peer_online(&peer_id) {
                            return Err(err);
                        }
                    }
                }
            }
            if need_resync {
                drop(claim_guard);
                if attempt == 0 {
                    continue;
                }
                return Err(Error::conflict(
                    "cluster session head advanced or diverged while starting run; retry or resolve conflict",
                ));
            }
            self.presence
                .promote_local_claim_to_committing(sid, &claim_id)
                .map_err(Error::conflict)?;
            let started = app.start_local_checked(sid, expected_head).await?;
            self.broadcast_presence_now(&claim_guard.peers, Some((sid, claim_id.clone())))
                .await;
            claim_guard.release_local_only();
            return Ok(started);
        }

        Err(Error::conflict("failed to acquire cluster run gate"))
    }

    pub async fn pause_run(&self, app: &App, sid: SessionId, hard: bool) -> Result<Session> {
        self.sync.ensure_session_watched(sid).await?;
        if self.presence.local_owner(sid).is_some() {
            return app.pause_local(sid, hard).await;
        }
        let target = match self.presence.remote_owner(sid) {
            Some((peer_id, _)) => Some(peer_id),
            None => self.probe_online_occupancy(sid).await?.map(|(id, _)| id),
        };
        if let Some(peer_id) = target {
            match self
                .exec_rpc(
                    &peer_id,
                    &ExecReq::PauseRun {
                        session_id: sid,
                        hard,
                    },
                    EXEC_SHORT_TIMEOUT,
                )
                .await?
            {
                ExecResp::Session { session } => {
                    return serde_json::from_value(session)
                        .map_err(|e| Error::internal(format!("invalid remote pause reply: {e}")));
                }
                _ => return Err(Error::internal("unexpected remote PauseRun response")),
            }
        }
        app.pause_local(sid, hard).await
    }

    pub async fn resolve_session_writer(&self, sid: SessionId) -> Result<WriterTarget> {
        if self.presence.local_owner(sid).is_some() {
            return Ok(WriterTarget::Local);
        }
        if let Some((peer_id, _)) = self.presence.remote_owner(sid) {
            return Ok(WriterTarget::Remote(peer_id));
        }
        if !self.net.online_peer_ids().is_empty() {
            if let Some((peer_id, _)) = self.probe_online_occupancy(sid).await? {
                return Ok(WriterTarget::Remote(peer_id));
            }
            self.sync.sync_session_latest(sid).await?;
        } else {
            self.sync.ensure_session_watched(sid).await?;
        }
        Ok(WriterTarget::Local)
    }

    pub async fn enqueue_remote(
        &self,
        peer_id: &str,
        sid: SessionId,
        content: Value,
        key: Option<String>,
    ) -> Result<(bool, ItemId)> {
        let content_for_scan = content.clone();
        let attachments = self
            .db
            .call(move |s| media::collect_content_attachment_meta(&s.conn, &content_for_scan))
            .await?;
        match self
            .exec_rpc(
                peer_id,
                &ExecReq::EnqueueMessage {
                    session_id: sid,
                    content,
                    idempotency_key: key,
                    attachments,
                },
                EXEC_OP_TIMEOUT,
            )
            .await?
        {
            ExecResp::MessageEnqueued { id, created } => Ok((created, id)),
            _ => Err(Error::internal("unexpected remote EnqueueMessage response")),
        }
    }

    pub async fn withdraw_remote(&self, peer_id: &str, sid: SessionId, mid: ItemId) -> Result<()> {
        match self
            .exec_rpc(
                peer_id,
                &ExecReq::WithdrawMessage {
                    session_id: sid,
                    message_id: mid,
                },
                EXEC_SHORT_TIMEOUT,
            )
            .await?
        {
            ExecResp::Ack => Ok(()),
            _ => Err(Error::internal("unexpected remote WithdrawMessage response")),
        }
    }

    pub async fn patch_session_remote(
        &self,
        peer_id: &str,
        sid: SessionId,
        fields: Map<String, Value>,
    ) -> Result<Session> {
        match self
            .exec_rpc(
                peer_id,
                &ExecReq::PatchSession {
                    session_id: sid,
                    fields,
                },
                EXEC_SHORT_TIMEOUT,
            )
            .await?
        {
            ExecResp::Session { session } => serde_json::from_value(session)
                .map_err(|e| Error::internal(format!("invalid remote PatchSession reply: {e}"))),
            _ => Err(Error::internal("unexpected remote PatchSession response")),
        }
    }

    pub async fn assert_cluster_unoccupied(&self, sid: SessionId) -> Result<()> {
        if self.presence.local_owner(sid).is_some() || self.presence.remote_owner(sid).is_some() {
            return Err(Error::conflict(
                "pause the session and stop active tasks before deleting",
            ));
        }
        let online = self.net.online_peer_ids();
        if online.is_empty() {
            return Ok(());
        }
        let req = ExecReq::CheckOccupancy {
            session_id: sid,
            claim: None,
        };
        let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await) }
        }))
        .await;
        for (peer_id, res) in replies {
            match res {
                Ok(ExecResp::Occupancy { occupied: true, .. }) => {
                    return Err(Error::conflict(
                        "pause the session and stop active tasks before deleting",
                    ));
                }
                Ok(_) => {}
                Err(err) => {
                    if self.net.is_peer_online(&peer_id) {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn list_tasks(&self, sid: SessionId) -> Result<Value> {
        if let Some(peer_id) = self.remote_runner_id(sid) {
            match self
                .exec_rpc(&peer_id, &ExecReq::ListTasks { session_id: sid }, EXEC_SHORT_TIMEOUT)
                .await?
            {
                ExecResp::Tasks { tasks } => return Ok(tasks),
                _ => return Err(Error::internal("unexpected remote ListTasks response")),
            }
        }
        self.db
            .call(move |s| {
                session(&s.conn, sid)?;
                Ok(codemode::session_tasks_snapshot(s, sid))
            })
            .await
    }

    pub async fn kill_task(&self, sid: SessionId, tid: String) -> Result<Value> {
        let id = crate::tools::task_id(&tid).map_err(Error::bad)?;
        if let Some(peer_id) = self.remote_runner_id(sid) {
            match self
                .exec_rpc(
                    &peer_id,
                    &ExecReq::KillTask {
                        session_id: sid,
                        task_id: tid,
                    },
                    EXEC_SHORT_TIMEOUT,
                )
                .await?
            {
                ExecResp::TaskKilled { result } => return Ok(result),
                _ => return Err(Error::internal("unexpected remote KillTask response")),
            }
        }
        self.db
            .call(move |s| {
                session(&s.conn, sid)?;
                codemode::stop_task(s, sid, id)
                    .ok_or_else(|| Error(axum::http::StatusCode::NOT_FOUND, "task is not active".into()))
            })
            .await
    }

    pub fn spawn_event_relay(
        &self,
        peer_id: String,
        sid: SessionId,
        cancel: CancellationToken,
    ) -> mpsc::Receiver<RelayedEvent> {
        let (tx, rx) = mpsc::channel(128);
        let plane = self.self_weak.get().and_then(Weak::upgrade);
        self.workers.spawn(async move {
            let Some(plane) = plane else {
                let _ = tx.send(RelayedEvent::Disconnected).await;
                return;
            };
            let stream = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                r = plane.open_exec_stream(&peer_id, &cancel) => r,
            };
            let (conn, mut send, mut recv) = match stream {
                Ok(parts) => parts,
                Err(_) => {
                    let _ = tx.send(RelayedEvent::Disconnected).await;
                    return;
                }
            };
            let write_res = write_exec_frame(
                &mut send,
                &ExecReq::SubscribeSessionEvents { session_id: sid },
            )
            .await;
            let _ = send.finish();
            if write_res.is_err() {
                plane.net.evict_exec_conn(&peer_id, &conn);
                let _ = tx.send(RelayedEvent::Disconnected).await;
                return;
            }
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        let _ = recv.stop(0u32.into());
                        break;
                    }
                    f = read_exec_frame::<ExecResp>(
                        &mut recv,
                        EXEC_RESP_HEADER_MAX,
                        EXEC_RESP_BODY_MAX,
                        &plane.byte_budget,
                    ) => f,
                };
                match frame {
                    Ok(ExecResp::SseEvent { event, data }) => {
                        match tx.try_send(RelayedEvent::Event { event, data }) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                let _ = recv.stop(0u32.into());
                                let _ = tx.send(RelayedEvent::Disconnected).await;
                                break;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                let _ = recv.stop(0u32.into());
                                break;
                            }
                        }
                    }
                    Ok(ExecResp::SseEnd) => {
                        let _ = tx.send(RelayedEvent::Disconnected).await;
                        break;
                    }
                    Ok(ExecResp::Error { .. }) | Ok(_) | Err(ReadExecErr::Protocol(_)) => {
                        let _ = tx.send(RelayedEvent::Disconnected).await;
                        break;
                    }
                    Err(ReadExecErr::Transport(_)) => {
                        plane.net.evict_exec_conn(&peer_id, &conn);
                        let _ = tx.send(RelayedEvent::Disconnected).await;
                        break;
                    }
                }
            }
        });
        rx
    }
}

fn is_remote_head_newer_or_diverged(
    local: Option<&StreamHead>,
    remote: Option<&StreamHead>,
) -> bool {
    match (local, remote) {
        (None, Some(_)) => true,
        (Some(l), Some(r)) => {
            r.epoch > l.epoch
                || (r.epoch == l.epoch && r.head_seq > l.head_seq)
                || (r.epoch == l.epoch && r.head_seq == l.head_seq && r.head_hash != l.head_hash)
        }
        _ => false,
    }
}

struct ClaimReleaseGuard {
    presence: Arc<PresenceState>,
    plane: Option<Arc<ExecPlane>>,
    sid: SessionId,
    claim_id: String,
    peers: Vec<String>,
    armed: bool,
}

impl ClaimReleaseGuard {
    fn new(
        presence: Arc<PresenceState>,
        plane: Option<Arc<ExecPlane>>,
        sid: SessionId,
        claim_id: String,
    ) -> Self {
        Self {
            presence,
            plane,
            sid,
            claim_id,
            peers: Vec::new(),
            armed: true,
        }
    }

    fn track_peer(&mut self, peer_id: String) {
        if !self.peers.contains(&peer_id) {
            self.peers.push(peer_id);
        }
    }

    fn release_local_only(mut self) {
        self.presence.release_local_claim(self.sid, &self.claim_id);
        self.armed = false;
    }
}

impl Drop for ClaimReleaseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.presence.release_local_claim(self.sid, &self.claim_id);
        if self.peers.is_empty() {
            return;
        }
        let Some(plane) = self.plane.take() else {
            return;
        };
        let peers = std::mem::take(&mut self.peers);
        let sid = self.sid;
        let claim_id = std::mem::take(&mut self.claim_id);
        let workers = plane.workers.clone();
        workers.spawn(async move {
            let req = ExecReq::ReleaseClaim {
                session_id: sid,
                claim_id,
            };
            futures_util::future::join_all(peers.into_iter().map(|peer_id| {
                let req_ref = &req;
                let plane = plane.clone();
                async move {
                    let _ = plane.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await;
                }
            }))
            .await;
        });
    }
}

pub struct ExecServer {
    app: App,
    global_sem: Arc<Semaphore>,
    peer_sems: Mutex<HashMap<String, Weak<Semaphore>>>,
    sse_sem: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
}

impl ExecServer {
    pub fn new(app: App) -> Self {
        Self {
            app,
            global_sem: Arc::new(Semaphore::new(MAX_GLOBAL_INBOUND_EXEC)),
            peer_sems: Mutex::default(),
            sse_sem: Arc::new(Semaphore::new(MAX_REMOTE_SSE_SUBS)),
            byte_budget: Arc::new(Semaphore::new(MAX_EXEC_BYTE_PERMITS)),
        }
    }

    fn peer_sem(&self, node_id: &str) -> Arc<Semaphore> {
        let mut map = self.peer_sems.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(sem) = map.get(node_id).and_then(Weak::upgrade) {
            return sem;
        }
        let sem = Arc::new(Semaphore::new(MAX_PEER_INBOUND_EXEC));
        map.insert(node_id.to_owned(), Arc::downgrade(&sem));
        sem
    }

    async fn send_reply(send: &mut SendStream, resp: &ExecResp) {
        let _ = write_exec_frame(send, resp).await;
        let _ = send.finish();
    }

    fn to_wire_err(err: Error) -> ExecResp {
        ExecResp::Error {
            status: err.0.as_u16(),
            message: err.1,
        }
    }

    pub async fn handle_stream(
        &self,
        from_peer: String,
        mut send: SendStream,
        mut recv: RecvStream,
        peer_cancel: CancellationToken,
    ) {
        let Ok(_global) = self.global_sem.try_acquire() else {
            Self::send_reply(
                &mut send,
                &ExecResp::Error {
                    status: 429,
                    message: "remote exec global concurrency limit reached".into(),
                },
            )
            .await;
            return;
        };
        let peer_sem = self.peer_sem(&from_peer);
        let Ok(_peer) = peer_sem.try_acquire() else {
            Self::send_reply(
                &mut send,
                &ExecResp::Error {
                    status: 429,
                    message: "remote exec peer concurrency limit reached".into(),
                },
            )
            .await;
            return;
        };

        let req = match tokio::time::timeout(
            EXEC_REQ_READ_TIMEOUT,
            read_exec_frame::<ExecReq>(&mut recv, EXEC_REQ_HEADER_MAX, EXEC_REQ_BODY_MAX, &self.byte_budget),
        )
        .await
        {
            Ok(Ok(req)) => req,
            Ok(Err(ReadExecErr::Protocol(message))) => {
                Self::send_reply(
                    &mut send,
                    &ExecResp::Error {
                        status: 400,
                        message,
                    },
                )
                .await;
                return;
            }
            Ok(Err(ReadExecErr::Transport(_))) | Err(_) => return,
        };

        match req {
            ExecReq::PresenceSync {
                instance_id,
                seq,
                sessions,
                want_reply,
                clear_claim,
            } => {
                self.app.exec.apply_remote_presence(
                    &from_peer,
                    instance_id,
                    seq,
                    sessions,
                    clear_claim,
                );
                if want_reply {
                    let (my_inst, my_seq, my_sessions) = self.app.exec.presence().snapshot_local();
                    Self::send_reply(
                        &mut send,
                        &ExecResp::Presence {
                            instance_id: my_inst,
                            seq: my_seq,
                            sessions: my_sessions,
                        },
                    )
                    .await;
                } else {
                    Self::send_reply(&mut send, &ExecResp::Ack).await;
                }
            }
            ExecReq::CheckOccupancy { session_id, claim } => {
                let resp = self
                    .handle_check_occupancy(&from_peer, session_id, claim)
                    .await;
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::ReleaseClaim {
                session_id,
                claim_id,
            } => {
                self.app
                    .exec
                    .presence()
                    .release_remote_claim(session_id, &from_peer, &claim_id);
                Self::send_reply(&mut send, &ExecResp::Ack).await;
            }
            ExecReq::StartRun { session_id } => {
                let resp = match self.app.exec.start_run_local(&self.app, session_id).await {
                    Ok(session) => ExecResp::Session {
                        session: json!(session),
                    },
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::PauseRun { session_id, hard } => {
                let resp = match self.handle_pause_local(session_id, hard).await {
                    Ok(session) => ExecResp::Session {
                        session: json!(session),
                    },
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::EnqueueMessage {
                session_id,
                content,
                idempotency_key,
                attachments,
            } => {
                let resp = match self
                    .handle_enqueue_local(session_id, content, idempotency_key, attachments)
                    .await
                {
                    Ok((created, id)) => ExecResp::MessageEnqueued { id, created },
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::WithdrawMessage {
                session_id,
                message_id,
            } => {
                let resp = match self.handle_withdraw_local(session_id, message_id).await {
                    Ok(()) => ExecResp::Ack,
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::PatchSession { session_id, fields } => {
                let resp = match self.handle_patch_local(session_id, fields).await {
                    Ok(session) => ExecResp::Session {
                        session: json!(session),
                    },
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::ListTasks { session_id } => {
                let resp = match self
                    .app
                    .db
                    .call(move |s| {
                        session(&s.conn, session_id)?;
                        Ok(codemode::session_tasks_snapshot(s, session_id))
                    })
                    .await
                {
                    Ok(tasks) => ExecResp::Tasks { tasks },
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::KillTask {
                session_id,
                task_id,
            } => {
                let resp = match crate::tools::task_id(&task_id).map_err(Error::bad) {
                    Err(e) => Self::to_wire_err(e),
                    Ok(id) => match self
                        .app
                        .db
                        .call(move |s| {
                            session(&s.conn, session_id)?;
                            codemode::stop_task(s, session_id, id).ok_or_else(|| {
                                Error(
                                    axum::http::StatusCode::NOT_FOUND,
                                    "task is not active".into(),
                                )
                            })
                        })
                        .await
                    {
                        Ok(result) => ExecResp::TaskKilled { result },
                        Err(e) => Self::to_wire_err(e),
                    },
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::SubscribeSessionEvents { session_id } => {
                let Ok(_sse_permit) = self.sse_sem.try_acquire() else {
                    Self::send_reply(
                        &mut send,
                        &ExecResp::Error {
                            status: 429,
                            message: "remote SSE relay limit reached".into(),
                        },
                    )
                    .await;
                    return;
                };
                self.handle_subscribe_events(session_id, send, peer_cancel)
                    .await;
            }
        }
    }

    async fn handle_check_occupancy(
        &self,
        from_peer: &str,
        sid: SessionId,
        claim: Option<RunClaim>,
    ) -> ExecResp {
        let presence = self.app.exec.presence().clone();
        let from_peer_owned = from_peer.to_owned();
        let db_res = self
            .app
            .db
            .call(move |s| {
                let local_occ = presence.local_owner(sid);
                let active_worker = s.active.contains_key(&sid) || s.pending_wakes.contains(&sid);
                let active_tasks = codemode::count_session_tasks(s, sid);
                let local_occupied = active_worker || active_tasks > 0 || local_occ.is_some();

                let mut conflict_holder: Option<String> = None;
                if let Some(claim) = claim {
                    if let Err(holder) = presence.accept_remote_claim(sid, claim, local_occupied) {
                        conflict_holder = holder;
                    }
                } else if !local_occupied {
                    conflict_holder = presence.active_claim_holder_excluding(sid, &from_peer_owned);
                }
                let remote_occ = presence.remote_owner_excluding(sid, &from_peer_owned);
                let status = if let Some(occ) = &local_occ {
                    Some(occ.status.clone())
                } else if local_occupied {
                    session(&s.conn, sid).ok().map(|row| row.status)
                } else {
                    remote_occ.as_ref().map(|(_, o)| o.status.clone())
                };
                let head = stream_head_wire(&s.conn, sid.as_str())?;
                let conflicted = has_conflict(&s.conn, sid.as_str())?;
                let occupied = local_occupied || remote_occ.is_some() || conflict_holder.is_some();
                let runner_node_id = if local_occupied {
                    Some(s.local_node_id.clone())
                } else if let Some(h) = conflict_holder {
                    Some(h)
                } else {
                    remote_occ.map(|(id, _)| id)
                };
                Ok(ExecResp::Occupancy {
                    occupied,
                    runner_node_id,
                    status,
                    active_tasks: if local_occupied {
                        active_tasks
                    } else {
                        0
                    },
                    head,
                    has_conflict: conflicted,
                })
            })
            .await;
        match db_res {
            Ok(resp) => resp,
            Err(e) => Self::to_wire_err(e),
        }
    }

    async fn ensure_local_writer_allowed(&self, sid: SessionId) -> Result<()> {
        if let Some((owner_id, _)) = self.app.exec.presence().remote_owner(sid) {
            return Err(Error::conflict(format!(
                "session is currently occupied on node {owner_id}"
            )));
        }
        Ok(())
    }

    async fn handle_pause_local(&self, sid: SessionId, hard: bool) -> Result<Session> {
        self.ensure_local_writer_allowed(sid).await?;
        self.app.cluster.ensure_session_watched(sid).await?;
        self.app.pause_local(sid, hard).await
    }

    async fn handle_enqueue_local(
        &self,
        sid: SessionId,
        content: Value,
        key: Option<String>,
        attachments: Vec<AttachmentRef>,
    ) -> Result<(bool, ItemId)> {
        self.ensure_local_writer_allowed(sid).await?;
        self.app.cluster.ensure_session_watched(sid).await?;
        match self
            .app
            .db
            .enqueue_with_attachments(sid, content, key, attachments)
            .await?
        {
            store::Enqueued::Created(id) => Ok((true, id)),
            store::Enqueued::Existing(id) => Ok((false, id)),
        }
    }

    async fn handle_withdraw_local(&self, sid: SessionId, mid: ItemId) -> Result<()> {
        self.ensure_local_writer_allowed(sid).await?;
        self.app.cluster.ensure_session_watched(sid).await?;
        crate::api::withdraw_local(&self.app, sid, mid).await
    }

    async fn handle_patch_local(&self, sid: SessionId, fields: Map<String, Value>) -> Result<Session> {
        self.ensure_local_writer_allowed(sid).await?;
        self.app.cluster.ensure_session_watched(sid).await?;
        crate::api::patch_session_local(&self.app, sid, fields).await
    }

    async fn handle_subscribe_events(
        &self,
        sid: SessionId,
        mut send: SendStream,
        peer_cancel: CancellationToken,
    ) {
        let mut stream = match SessionEventStream::open(&self.app, sid).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                Self::send_reply(
                    &mut send,
                    &ExecResp::Error {
                        status: 404,
                        message: "not found".into(),
                    },
                )
                .await;
                return;
            }
            Err(e) => {
                Self::send_reply(&mut send, &Self::to_wire_err(e)).await;
                return;
            }
        };
        if let Some(snap) = stream.take_initial_snapshot()
            && write_exec_frame(
                &mut send,
                &ExecResp::SseEvent {
                    event: "stream.snapshot".into(),
                    data: snap,
                },
            )
            .await
            .is_err()
        {
            return;
        }
        loop {
            let next = tokio::select! {
                biased;
                _ = self.app.stopping.cancelled() => break,
                _ = peer_cancel.cancelled() => break,
                _ = send.stopped() => break,
                ev = stream.next() => ev,
            };
            match next {
                NextSessionNotice::Event { event, data } => {
                    if write_exec_frame(&mut send, &ExecResp::SseEvent { event, data })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                NextSessionNotice::Closed => {
                    let _ = write_exec_frame(&mut send, &ExecResp::SseEnd).await;
                    let _ = send.finish();
                    return;
                }
            }
        }
        let _ = write_exec_frame(&mut send, &ExecResp::SseEnd).await;
        let _ = send.finish();
    }
}

pub fn merge_remote_attachment_refs(conn: &rusqlite::Connection, refs: &[AttachmentRef]) -> Result<()> {
    if refs.is_empty() {
        return Ok(());
    }
    let mut insert = conn.prepare_cached(
        "INSERT OR IGNORE INTO attachments(id,mime_type,content_hash,byte_len,data) VALUES(?1,?2,?3,?4,NULL)",
    )?;
    for att in refs {
        if att.byte_len <= 0 || att.mime_type.trim().is_empty() || att.mime_type.contains('\0') {
            return Err(Error::bad("invalid forwarded attachment metadata"));
        }
        let existing_hash: Option<Vec<u8>> = conn
            .prepare_cached("SELECT content_hash FROM attachments WHERE id=?")?
            .query_row([att.id], |r| r.get(0))
            .optional()?;
        if let Some(hash) = existing_hash {
            if hash.as_slice() != att.content_hash {
                return Err(Error::conflict(format!(
                    "attachment {} metadata mismatch",
                    att.id
                )));
            }
            continue;
        }
        insert.execute(rusqlite::params![
            att.id,
            att.mime_type,
            att.content_hash.as_slice(),
            att.byte_len
        ])?;
    }
    Ok(())
}
