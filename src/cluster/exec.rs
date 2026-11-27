use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, Semaphore, mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    App, Error, Result,
    cluster::{
        identity::{MAX_CLUSTER_PEERS, ResolvedNode, list_peers, resolve_node_target},
        ledger::{AttachmentRef, read_vector, session_branch_complete, session_scope},
        net::{ClusterNet, FrameReadErr as ReadExecErr, read_json_frame, write_json_frame},
        sync::SyncEngine,
    },
    codemode,
    events::{NextSessionNotice, SessionEventStream},
    id::{ContextNodeId, ItemId, McpServerId, SessionId},
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
const MAX_PEER_REMOTE_SSE_SUBS: usize = 32;

const EXEC_REQ_READ_TIMEOUT: Duration = Duration::from_secs(15);
const EXEC_SHORT_TIMEOUT: Duration = Duration::from_secs(10);
const EXEC_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const EXEC_OP_TIMEOUT: Duration = Duration::from_secs(30);
const CLAIM_TTL: Duration = Duration::from_secs(5);

const MAX_GOSSIP_HOPS: usize = 6;
const MAX_GOSSIP_ENTRIES: usize = MAX_CLUSTER_PEERS + 1;
const MAX_PRESENCE_SESSIONS_PER_NODE: usize = 256;
const MAX_CHECK_HOPS: u8 = 4;

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
pub struct NodePresence {
    pub node_id: String,
    pub instance_id: u64,
    pub seq: u64,
    #[serde(default)]
    pub sessions: Vec<SessionOccupancy>,
    #[serde(default)]
    pub path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunClaim {
    pub claim_id: String,
    pub node_id: String,
    pub started_ms: i64,
}

struct LocalClaim {
    claim_id: String,
    committing: bool,
    expires_at: Instant,
}

struct RemoteClaim {
    claim_id: String,
    node_id: String,
    expires_at: Instant,
}

struct PresenceInner {
    local_node_id: String,
    instance_id: u64,
    local_seq: u64,
    gossip_seq: u64,
    local: HashMap<SessionId, SessionOccupancy>,
    remote_by_via: HashMap<String, HashMap<String, NodePresence>>,
    peer_sync_cursor: HashMap<String, (u64, u64)>,
    effective: HashMap<String, NodePresence>,
    local_claims: HashMap<SessionId, LocalClaim>,
    remote_claims: HashMap<SessionId, RemoteClaim>,
    session_watchers: HashMap<SessionId, watch::Sender<Option<String>>>,
}

pub struct PresenceState {
    known_peer_count: AtomicUsize,
    local_nonempty: AtomicBool,
    has_null_attachments: AtomicBool,
    gossip_seq: AtomicU64,
    cluster_active_tx: watch::Sender<bool>,
    inner: Mutex<PresenceInner>,
    local_changed: Notify,
}

pub fn refresh_known_peer_count(
    conn: &rusqlite::Connection,
    presence: &PresenceState,
) -> Result<usize> {
    let count = list_peers(conn)?.len();
    presence.set_known_peer_count(count);
    Ok(count)
}

impl PresenceState {
    pub fn new() -> Arc<Self> {
        let mut raw = [0u8; 8];
        let _ = getrandom::fill(&mut raw);
        let instance_id = u64::from_le_bytes(raw).max(1);
        let (cluster_active_tx, _) = watch::channel(false);
        Arc::new(Self {
            known_peer_count: AtomicUsize::new(0),
            local_nonempty: AtomicBool::new(false),
            has_null_attachments: AtomicBool::new(false),
            gossip_seq: AtomicU64::new(1),
            cluster_active_tx,
            inner: Mutex::new(PresenceInner {
                local_node_id: String::new(),
                instance_id,
                local_seq: 1,
                gossip_seq: 1,
                local: HashMap::new(),
                remote_by_via: HashMap::new(),
                peer_sync_cursor: HashMap::new(),
                effective: HashMap::new(),
                local_claims: HashMap::new(),
                remote_claims: HashMap::new(),
                session_watchers: HashMap::new(),
            }),
            local_changed: Notify::new(),
        })
    }

    pub fn set_local_node_id(&self, node_id: String) {
        self.inner.lock().unwrap().local_node_id = node_id;
    }

    pub fn local_node_id(&self) -> String {
        self.inner.lock().unwrap().local_node_id.clone()
    }

    pub fn set_known_peer_count(&self, count: usize) {
        let prev = self.known_peer_count.swap(count, Ordering::AcqRel);
        if (prev == 0) != (count == 0) {
            self.cluster_active_tx.send_replace(count > 0);
        }
    }

    pub fn known_peer_count(&self) -> usize {
        self.known_peer_count.load(Ordering::Acquire)
    }

    pub fn subscribe_cluster_active(&self) -> watch::Receiver<bool> {
        self.cluster_active_tx.subscribe()
    }

    pub fn has_any_local_state(&self) -> bool {
        self.local_nonempty.load(Ordering::Acquire)
    }

    fn update_local_nonempty_locked(&self, inner: &PresenceInner) {
        self.local_nonempty
            .store(!inner.local.is_empty(), Ordering::Release);
    }

    pub fn set_has_null_attachments(&self, v: bool) {
        self.has_null_attachments.store(v, Ordering::Release);
    }

    pub fn has_null_attachments(&self) -> bool {
        self.has_null_attachments.load(Ordering::Acquire)
    }

    pub fn gossip_seq(&self) -> u64 {
        self.gossip_seq.load(Ordering::Acquire)
    }

    fn bump_gossip_seq_locked(&self, inner: &mut PresenceInner) {
        inner.gossip_seq = inner.gossip_seq.saturating_add(1);
        self.gossip_seq.store(inner.gossip_seq, Ordering::Release);
    }

    fn remote_owner_locked(
        inner: &PresenceInner,
        sid: SessionId,
    ) -> Option<(String, SessionOccupancy)> {
        let mut best: Option<(String, SessionOccupancy)> = None;
        for (origin, np) in &inner.effective {
            if let Some(occ) = np.sessions.iter().find(|s| s.session_id == sid)
                && best
                    .as_ref()
                    .is_none_or(|(best_id, _)| origin.as_str() < best_id.as_str())
            {
                best = Some((origin.clone(), occ.clone()));
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
        inner.local_claims.retain(|_, c| now < c.expires_at);
        inner.remote_claims.retain(|_, c| now < c.expires_at);
    }

    fn better_presence(candidate: &NodePresence, current: &NodePresence) -> bool {
        let cand_ver = (candidate.instance_id, candidate.seq);
        let cur_ver = (current.instance_id, current.seq);
        if cand_ver != cur_ver {
            return cand_ver > cur_ver;
        }
        if candidate.path.len() != current.path.len() {
            return candidate.path.len() < current.path.len();
        }
        candidate.path < current.path
    }

    fn recompute_effective_locked(&self, inner: &mut PresenceInner) -> (Vec<SessionId>, bool) {
        let local_id = inner.local_node_id.clone();
        let mut next_effective: HashMap<String, NodePresence> = HashMap::new();
        for by_origin in inner.remote_by_via.values() {
            for (origin, entry) in by_origin {
                if origin == &local_id {
                    continue;
                }
                match next_effective.get_mut(origin) {
                    Some(cur) => {
                        if Self::better_presence(entry, cur) {
                            *cur = entry.clone();
                        }
                    }
                    None => {
                        next_effective.insert(origin.clone(), entry.clone());
                    }
                }
            }
        }
        if next_effective == inner.effective {
            return (Vec::new(), false);
        }

        let mut old_by_sid: HashMap<SessionId, (String, SessionOccupancy)> = HashMap::new();
        for (origin, np) in &inner.effective {
            for occ in &np.sessions {
                let replace = old_by_sid
                    .get(&occ.session_id)
                    .is_none_or(|(best_id, _)| origin.as_str() < best_id.as_str());
                if replace {
                    old_by_sid.insert(occ.session_id, (origin.clone(), occ.clone()));
                }
            }
        }

        let mut new_by_sid: HashMap<SessionId, (String, SessionOccupancy)> = HashMap::new();
        for (origin, np) in &next_effective {
            for occ in &np.sessions {
                let replace = new_by_sid
                    .get(&occ.session_id)
                    .is_none_or(|(best_id, _)| origin.as_str() < best_id.as_str());
                if replace {
                    new_by_sid.insert(occ.session_id, (origin.clone(), occ.clone()));
                }
            }
        }

        let mut affected: HashSet<SessionId> = HashSet::new();
        for (sid, prev) in &old_by_sid {
            if new_by_sid.get(sid) != Some(prev) {
                affected.insert(*sid);
            }
        }
        for (sid, next) in &new_by_sid {
            if old_by_sid.get(sid) != Some(next) {
                affected.insert(*sid);
            }
        }

        inner.effective = next_effective;
        self.bump_gossip_seq_locked(inner);
        let changed: Vec<SessionId> = affected.into_iter().collect();
        for &sid in &changed {
            Self::notify_watcher_locked(inner, sid);
        }
        (changed, true)
    }

    pub fn set_local(&self, sid: SessionId, next: Option<SessionOccupancy>) -> bool {
        let changed = {
            let mut inner = self.inner.lock().unwrap();
            let unchanged = inner.local.get(&sid) == next.as_ref();
            if !unchanged {
                match next {
                    Some(occ) => {
                        inner.local.insert(sid, occ);
                    }
                    None => {
                        inner.local.remove(&sid);
                    }
                }
                inner.local_seq = inner.local_seq.saturating_add(1);
                self.bump_gossip_seq_locked(&mut inner);
            }
            self.update_local_nonempty_locked(&inner);
            !unchanged
        };
        if changed && self.known_peer_count.load(Ordering::Acquire) > 0 {
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

    pub fn active_claim_holder_excluding(
        &self,
        sid: SessionId,
        exclude_node_id: &str,
    ) -> Option<(String, bool)> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let local_id = inner.local_node_id.clone();
        if local_id != exclude_node_id
            && let Some(c) = inner.local_claims.get(&sid)
        {
            return Some((local_id, !c.committing));
        }
        if let Some(rc) = inner.remote_claims.get(&sid)
            && rc.node_id != exclude_node_id
        {
            return Some((rc.node_id.clone(), true));
        }
        None
    }

    pub fn snapshot_for_peer(&self, target_peer: &str) -> (u64, u64, Vec<NodePresence>) {
        let inner = self.inner.lock().unwrap();
        let local_id = inner.local_node_id.clone();
        let mut local_sessions: Vec<SessionOccupancy> = inner.local.values().cloned().collect();
        local_sessions.sort_by_key(|o| o.session_id);
        local_sessions.truncate(MAX_PRESENCE_SESSIONS_PER_NODE);

        let mut entries = Vec::with_capacity(1 + inner.effective.len().min(MAX_GOSSIP_ENTRIES - 1));
        entries.push(NodePresence {
            node_id: local_id.clone(),
            instance_id: inner.instance_id,
            seq: inner.local_seq,
            sessions: local_sessions,
            path: vec![local_id.clone()],
        });

        let mut others: Vec<&NodePresence> = inner
            .effective
            .values()
            .filter(|entry| {
                entry.node_id != target_peer
                    && entry.node_id != local_id
                    && entry.path.len() < MAX_GOSSIP_HOPS
                    && !entry.path.iter().any(|p| p == target_peer)
            })
            .collect();
        others.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        for entry in others {
            if entries.len() >= MAX_GOSSIP_ENTRIES {
                break;
            }
            let mut forwarded = entry.clone();
            forwarded.path.push(local_id.clone());
            entries.push(forwarded);
        }
        (inner.instance_id, inner.gossip_seq, entries)
    }

    pub fn apply_remote_gossip(
        &self,
        from_peer: &str,
        sender_instance_id: u64,
        sender_gossip_seq: u64,
        entries: Vec<NodePresence>,
    ) -> Vec<SessionId> {
        let now = Instant::now();
        let (changed, effective_changed) = {
            let mut inner = self.inner.lock().unwrap();
            Self::prune_expired_claims_locked(&mut inner, now);
            if let Some(&(prev_inst, prev_seq)) = inner.peer_sync_cursor.get(from_peer)
                && sender_instance_id != 0
                && prev_inst == sender_instance_id
                && sender_gossip_seq < prev_seq
            {
                return Vec::new();
            }
            inner
                .peer_sync_cursor
                .insert(from_peer.to_owned(), (sender_instance_id, sender_gossip_seq));

            let local_id = inner.local_node_id.clone();
            let mut next_via: HashMap<String, NodePresence> = HashMap::new();
            for mut entry in entries.into_iter().take(MAX_GOSSIP_ENTRIES) {
                if entry.node_id.is_empty()
                    || entry.node_id == local_id
                    || entry.path.is_empty()
                    || entry.path.len() > MAX_GOSSIP_HOPS
                    || entry.path.first().map(String::as_str) != Some(entry.node_id.as_str())
                    || entry.path.last().map(String::as_str) != Some(from_peer)
                    || entry.path.iter().any(|p| p == &local_id)
                {
                    continue;
                }
                let unique_len = entry.path.iter().collect::<HashSet<_>>().len();
                if unique_len != entry.path.len() {
                    continue;
                }
                entry.sessions.sort_by_key(|o| o.session_id);
                entry.sessions.dedup_by_key(|o| o.session_id);
                entry.sessions.truncate(MAX_PRESENCE_SESSIONS_PER_NODE);

                match next_via.get_mut(&entry.node_id) {
                    Some(cur) => {
                        if Self::better_presence(&entry, cur) {
                            *cur = entry;
                        }
                    }
                    None => {
                        next_via.insert(entry.node_id.clone(), entry);
                    }
                }
            }
            if next_via.is_empty() {
                inner.remote_by_via.remove(from_peer);
            } else {
                inner.remote_by_via.insert(from_peer.to_owned(), next_via);
            }
            self.recompute_effective_locked(&mut inner)
        };
        if effective_changed && self.known_peer_count.load(Ordering::Acquire) > 0 {
            self.local_changed.notify_one();
        }
        changed
    }

    pub fn clear_peer(&self, peer_id: &str) -> Vec<SessionId> {
        let (changed, effective_changed) = {
            let mut inner = self.inner.lock().unwrap();
            inner.peer_sync_cursor.remove(peer_id);
            inner.remote_claims.retain(|_, rc| rc.node_id != peer_id);
            inner.remote_by_via.remove(peer_id);
            self.recompute_effective_locked(&mut inner)
        };
        if effective_changed && self.known_peer_count.load(Ordering::Acquire) > 0 {
            self.local_changed.notify_one();
        }
        changed
    }

    pub fn clear_all_remote(&self) -> Vec<SessionId> {
        let (changed, effective_changed) = {
            let mut inner = self.inner.lock().unwrap();
            inner.peer_sync_cursor.clear();
            inner.remote_claims.clear();
            inner.remote_by_via.clear();
            self.recompute_effective_locked(&mut inner)
        };
        if effective_changed && self.known_peer_count.load(Ordering::Acquire) > 0 {
            self.local_changed.notify_one();
        }
        changed
    }

    pub fn try_acquire_local_claim(
        &self,
        sid: SessionId,
    ) -> std::result::Result<(String, i64), (String, bool)> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| ("entropy unavailable".to_owned(), false))?;
        let claim_id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let started_ms = chrono::Utc::now().timestamp_millis();
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if inner.local.contains_key(&sid) {
            return Err(("session is already occupied on this node".to_owned(), false));
        }
        if inner.local_claims.contains_key(&sid) {
            return Err((
                "session run is already starting on this node".to_owned(),
                true,
            ));
        }
        if let Some(rc) = inner.remote_claims.get(&sid) {
            return Err((
                format!("session is currently occupied on node {}", rc.node_id),
                true,
            ));
        }
        inner.local_claims.insert(
            sid,
            LocalClaim {
                claim_id: claim_id.clone(),
                committing: false,
                expires_at: now + CLAIM_TTL,
            },
        );
        Ok((claim_id, started_ms))
    }

    pub fn promote_local_claim_to_committing(
        &self,
        sid: SessionId,
        claim_id: &str,
    ) -> std::result::Result<(), (String, bool)> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if let Some(rc) = inner.remote_claims.get(&sid) {
            return Err((
                format!("session is currently occupied on node {}", rc.node_id),
                true,
            ));
        }
        let Some(local) = inner.local_claims.get_mut(&sid) else {
            return Err(("session run claim expired; retry".to_owned(), true));
        };
        if local.claim_id != claim_id {
            return Err(("session run claim superseded; retry".to_owned(), true));
        }
        local.committing = true;
        local.expires_at = now + CLAIM_TTL;
        Ok(())
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

    pub fn check_or_register_remote_claim(
        &self,
        sid: SessionId,
        claim: &RunClaim,
    ) -> std::result::Result<(), (String, bool)> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let local_id = inner.local_node_id.clone();
        if inner.local.contains_key(&sid) {
            return Err((local_id, false));
        }
        if let Some(lc) = inner.local_claims.get(&sid) {
            if lc.committing || claim.node_id >= local_id {
                return Err((local_id, !lc.committing));
            }
            // Local probing claim yields to smaller node_id on simultaneous probe tie-break.
            inner.local_claims.remove(&sid);
        }
        if let Some(rc) = inner.remote_claims.get(&sid)
            && (rc.node_id != claim.node_id || rc.claim_id != claim.claim_id)
        {
            return Err((rc.node_id.clone(), true));
        }
        inner.remote_claims.insert(
            sid,
            RemoteClaim {
                claim_id: claim.claim_id.clone(),
                node_id: claim.node_id.clone(),
                expires_at: now + CLAIM_TTL,
            },
        );
        Ok(())
    }

    pub fn release_remote_claim(&self, sid: SessionId, holder_node_id: &str, claim_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .remote_claims
            .get(&sid)
            .is_some_and(|rc| rc.node_id == holder_node_id && rc.claim_id == claim_id)
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
        sender_instance_id: u64,
        sender_gossip_seq: u64,
        #[serde(default)]
        entries: Vec<NodePresence>,
        #[serde(default)]
        want_reply: bool,
    },
    CheckOccupancy {
        session_id: SessionId,
        #[serde(default)]
        claim: Option<RunClaim>,
        #[serde(default)]
        visited: Vec<String>,
        #[serde(default)]
        hops_left: u8,
    },
    ReleaseClaim {
        session_id: SessionId,
        holder_node_id: String,
        claim_id: String,
        #[serde(default)]
        visited: Vec<String>,
        #[serde(default)]
        hops_left: u8,
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
    OAuthCallback {
        server_id: McpServerId,
        url: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ExecResp {
    Ack,
    Presence {
        sender_instance_id: u64,
        sender_gossip_seq: u64,
        #[serde(default)]
        entries: Vec<NodePresence>,
    },
    Occupancy {
        occupied: bool,
        runner_node_id: Option<String>,
        status: Option<SessionStatus>,
        active_tasks: usize,
        #[serde(default)]
        occupied_by_claim: bool,
        #[serde(default)]
        session_deleted: bool,
        #[serde(default)]
        session_active_head: Option<ContextNodeId>,
        #[serde(default)]
        session_active_head_ts: String,
        #[serde(default)]
        session_vector: BTreeMap<String, i64>,
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

async fn write_exec_frame(
    send: &mut SendStream,
    header: &impl Serialize,
) -> std::result::Result<(), String> {
    write_json_frame(send, header, None, "exec").await
}

async fn read_exec_frame<T: DeserializeOwned>(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
    byte_budget: &Arc<Semaphore>,
) -> std::result::Result<T, ReadExecErr> {
    let (parsed, _) = read_json_frame(
        recv,
        max_header,
        max_body,
        byte_budget,
        EXEC_BYTE_UNIT,
        "exec",
    )
    .await?;
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

#[derive(Clone, Copy)]
enum GatePurpose {
    Run { require_model_caps: bool },
    Delete,
}

struct ConnectedGate {
    online_peers: Vec<String>,
    claim_guard: Option<ClaimReleaseGuard>,
}

pub struct DeleteGateGuard {
    guard: Option<ClaimReleaseGuard>,
    _lock: Option<OwnedMutexGuard<()>>,
}

impl DeleteGateGuard {
    pub async fn release_now(mut self) {
        if let Some(guard) = self.guard.take() {
            guard.release_now().await;
        }
    }
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
    acked_presence: Mutex<HashMap<String, u64>>,
    self_weak: OnceLock<Weak<ExecPlane>>,
}

pub(crate) fn is_membership_changed_err(err: &Error) -> bool {
    err.0 == axum::http::StatusCode::CONFLICT
        && err.1 == "cluster membership changed while starting session"
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
            acked_presence: Mutex::default(),
            self_weak: OnceLock::new(),
        });
        let _ = arc.self_weak.set(Arc::downgrade(&arc));
        arc
    }

    pub fn spawn_loops(self: &Arc<Self>, _app: App) {
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

    pub async fn on_peer_connected(&self, peer_id: String) {
        {
            self.pending_peer_sync.lock().unwrap().insert(peer_id);
        }
        self.peer_sync_wake.notify_one();
    }

    pub fn on_peer_offline(&self, peer_id: &str) {
        self.pending_peer_sync.lock().unwrap().remove(peer_id);
        self.acked_presence.lock().unwrap().remove(peer_id);
        let changed = self.presence.clear_peer(peer_id);
        self.publish_presence_changes(changed);
    }

    pub fn on_membership_switched(&self) {
        self.pending_peer_sync.lock().unwrap().clear();
        self.acked_presence.lock().unwrap().clear();
        let changed = self.presence.clear_all_remote();
        self.publish_presence_changes(changed);
    }

    fn mark_presence_acked(&self, peer_id: &str, seq: u64) {
        let mut acked = self.acked_presence.lock().unwrap();
        acked
            .entry(peer_id.to_owned())
            .and_modify(|s| *s = (*s).max(seq))
            .or_insert(seq);
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
        from_peer: &str,
        sender_instance_id: u64,
        sender_gossip_seq: u64,
        entries: Vec<NodePresence>,
    ) {
        let changed = self.presence.apply_remote_gossip(
            from_peer,
            sender_instance_id,
            sender_gossip_seq,
            entries,
        );
        self.publish_presence_changes(changed);
    }

    async fn presence_broadcast_loop(&self) {
        let mut backoff: HashMap<String, (Instant, Duration)> = HashMap::new();
        let mut last_seen_seq = 0u64;
        loop {
            if self.stopping.is_cancelled() {
                return;
            }
            let seq = self.presence.gossip_seq();
            if seq != last_seen_seq {
                backoff.clear();
                last_seen_seq = seq;
            }
            let online = self.net.online_peer_ids();
            let online_set: HashSet<&str> = online.iter().map(String::as_str).collect();
            backoff.retain(|k, _| online_set.contains(k.as_str()));

            let unacked: Vec<String> = {
                let acked = self.acked_presence.lock().unwrap();
                online
                    .into_iter()
                    .filter(|p| acked.get(p).copied().unwrap_or(0) < seq)
                    .collect()
            };

            let now = Instant::now();
            let mut due = Vec::new();
            let mut earliest: Option<Instant> = None;
            for peer_id in unacked {
                match backoff.get(&peer_id) {
                    Some(&(next_at, _)) if now < next_at => {
                        earliest = Some(earliest.map_or(next_at, |e| e.min(next_at)));
                    }
                    _ => due.push(peer_id),
                }
            }

            if !due.is_empty() {
                let results = futures_util::future::join_all(due.into_iter().map(|peer_id| {
                    let (sender_instance_id, sender_gossip_seq, entries) =
                        self.presence.snapshot_for_peer(&peer_id);
                    let req = ExecReq::PresenceSync {
                        sender_instance_id,
                        sender_gossip_seq,
                        entries,
                        want_reply: false,
                    };
                    async move {
                        let res = self.exec_rpc(&peer_id, &req, EXEC_SHORT_TIMEOUT).await;
                        (peer_id, sender_gossip_seq, res)
                    }
                }))
                .await;
                let retry_base = Instant::now();
                for (peer_id, sent_seq, res) in results {
                    if res.is_ok() {
                        self.mark_presence_acked(&peer_id, sent_seq);
                        backoff.remove(&peer_id);
                    } else if self.net.is_peer_online(&peer_id) {
                        let next_delay = backoff
                            .get(&peer_id)
                            .map(|(_, prev)| (*prev * 2).min(Duration::from_millis(3200)))
                            .unwrap_or_else(|| Duration::from_millis(200));
                        let next_at = retry_base + next_delay;
                        backoff.insert(peer_id, (next_at, next_delay));
                        earliest = Some(earliest.map_or(next_at, |e| e.min(next_at)));
                    }
                }
            }

            let wait_for = earliest.map(|at| at.saturating_duration_since(Instant::now()));
            match wait_for {
                Some(dur) => {
                    tokio::select! {
                        biased;
                        _ = self.stopping.cancelled() => return,
                        _ = self.presence.local_changed.notified() => {
                            backoff.clear();
                        }
                        _ = tokio::time::sleep(dur) => (),
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        _ = self.stopping.cancelled() => return,
                        _ = self.presence.local_changed.notified() => {
                            backoff.clear();
                        }
                    }
                }
            }
        }
    }

    async fn broadcast_presence_to(&self, peers: &[String]) {
        let mut targets = Vec::new();
        for peer_id in peers {
            if self.net.is_peer_online(peer_id) && !targets.contains(peer_id) {
                targets.push(peer_id.clone());
            }
        }
        if targets.is_empty() {
            return;
        }
        let results = futures_util::future::join_all(targets.into_iter().map(|peer_id| {
            let (sender_instance_id, sender_gossip_seq, entries) =
                self.presence.snapshot_for_peer(&peer_id);
            let req = ExecReq::PresenceSync {
                sender_instance_id,
                sender_gossip_seq,
                entries,
                want_reply: false,
            };
            async move {
                let res = self.exec_rpc(&peer_id, &req, EXEC_PROBE_TIMEOUT).await;
                (peer_id, sender_gossip_seq, res)
            }
        }))
        .await;
        for (peer_id, sent_seq, res) in results {
            if res.is_ok() {
                self.mark_presence_acked(&peer_id, sent_seq);
            }
        }
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
            futures_util::future::join_all(peers.into_iter().map(|peer_id| async move {
                if !self.net.is_peer_online(&peer_id) {
                    return;
                }
                let (sender_instance_id, sender_gossip_seq, entries) =
                    self.presence.snapshot_for_peer(&peer_id);
                let req = ExecReq::PresenceSync {
                    sender_instance_id,
                    sender_gossip_seq,
                    entries,
                    want_reply: true,
                };
                if let Ok(ExecResp::Presence {
                    sender_instance_id: r_inst,
                    sender_gossip_seq: r_seq,
                    entries: r_entries,
                }) = self.exec_rpc(&peer_id, &req, EXEC_SHORT_TIMEOUT).await
                {
                    self.mark_presence_acked(&peer_id, sender_gossip_seq);
                    self.apply_remote_presence(&peer_id, r_inst, r_seq, r_entries);
                }
            }))
            .await;
        }
    }

    pub async fn forward_oauth_callback(
        &self,
        peer_id: &str,
        server_id: McpServerId,
        url: String,
    ) -> Result<()> {
        match self
            .exec_rpc(
                peer_id,
                &ExecReq::OAuthCallback { server_id, url },
                EXEC_OP_TIMEOUT,
            )
            .await?
        {
            ExecResp::Ack => Ok(()),
            _ => Err(Error::internal("unexpected remote OAuthCallback response")),
        }
    }

    async fn peer_info(&self, peer_id: &str) -> Result<crate::cluster::identity::PeerInfo> {
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
        let active_conn: Mutex<Option<Connection>> = Mutex::new(None);
        let res = tokio::time::timeout(timeout_dur, async {
            let (conn, mut send, mut recv) = self.open_exec_stream(peer_id, &cancel).await?;
            *active_conn.lock().unwrap() = Some(conn.clone());
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
            Err(_) => {
                cancel.cancel();
                if let Some(conn) = active_conn.into_inner().unwrap() {
                    self.net.evict_exec_conn(peer_id, &conn);
                }
                Err(Error(
                    axum::http::StatusCode::GATEWAY_TIMEOUT,
                    format!("remote exec request to {peer_id} timed out"),
                ))
            }
        }
    }

    async fn probe_online_occupancy(&self, sid: SessionId) -> Result<Option<String>> {
        let local_id = self.presence.local_node_id();
        if let Some((holder, by_claim)) = self.presence.active_claim_holder_excluding(sid, &local_id)
        {
            if by_claim {
                return Err(Error::conflict(format!(
                    "session run is starting on node {holder}"
                )));
            }
            return Ok(Some(holder));
        }
        if let Some((owner, _)) = self.presence.remote_owner(sid) {
            return Ok(Some(owner));
        }
        let online = self.net.online_peer_ids();
        if online.is_empty() {
            return Ok(None);
        }
        let req = ExecReq::CheckOccupancy {
            session_id: sid,
            claim: None,
            visited: vec![local_id.clone()],
            hops_left: MAX_CHECK_HOPS,
        };
        let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT).await) }
        }))
        .await;
        let mut best: Option<String> = None;
        let mut starting_holder: Option<String> = None;
        let mut deleted_peers = Vec::new();
        for (peer_id, res) in replies {
            if let Ok(ExecResp::Occupancy {
                occupied,
                runner_node_id,
                status,
                occupied_by_claim,
                session_deleted,
                ..
            }) = res
            {
                if session_deleted {
                    deleted_peers.push(peer_id);
                    continue;
                }
                if occupied {
                    let owner = runner_node_id.unwrap_or(peer_id);
                    if owner == local_id {
                        continue;
                    }
                    if status.is_none() || occupied_by_claim {
                        starting_holder.get_or_insert(owner);
                        continue;
                    }
                    if best.as_ref().is_none_or(|b| owner.as_str() < b.as_str()) {
                        best = Some(owner);
                    }
                }
            }
        }
        if !deleted_peers.is_empty() {
            let _ = self.sync.sync_session_strict(sid, &deleted_peers).await;
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        if let Some(owner) = best {
            return Ok(Some(owner));
        }
        if let Some(holder) = starting_holder {
            return Err(Error::conflict(format!(
                "session run is starting on node {holder}"
            )));
        }
        Ok(None)
    }

    pub async fn start_run(
        &self,
        app: &App,
        sid: SessionId,
        node: Option<String>,
    ) -> Result<Session> {
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
                        .exec_rpc(
                            &peer.node_id,
                            &ExecReq::StartRun { session_id: sid },
                            EXEC_OP_TIMEOUT,
                        )
                        .await?
                    {
                        ExecResp::Session { session } => serde_json::from_value(session)
                            .map_err(|e| Error::internal(format!("invalid remote session reply: {e}"))),
                        _ => Err(Error::internal("unexpected remote StartRun response")),
                    }
                }
            }
        } else {
            self.start_run_local(app, sid).await
        }
    }

    pub async fn start_run_local(&self, app: &App, sid: SessionId) -> Result<Session> {
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            match app.start_local_checked(sid, true).await {
                Err(err) if is_membership_changed_err(&err) => {}
                other => return other,
            }
        }

        let lock = self.start_lock(sid);
        let _guard = lock.lock().await;

        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            match app.start_local_checked(sid, true).await {
                Err(err) if is_membership_changed_err(&err) => {}
                other => return other,
            }
        }

        // Fast idempotent path if this node already owns an active session worker.
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

        // Fast local path when this node already occupies the session (e.g., background tasks).
        if self.presence.local_owner(sid).is_some() {
            self.validate_local_gate_preconditions(
                sid,
                GatePurpose::Run {
                    require_model_caps: true,
                },
            )
            .await?;
            self.sync.ensure_session_attachments(sid).await?;
            let started = app.start_local_checked(sid, false).await?;
            let peers = self.net.online_peer_ids();
            if !peers.is_empty() {
                self.broadcast_presence_to(&peers).await;
            }
            return Ok(started);
        }

        let gate = self
            .acquire_connected_gate(
                sid,
                GatePurpose::Run {
                    require_model_caps: true,
                },
            )
            .await?;
        let started = app.start_local_checked(sid, false).await;
        self.finish_connected_start(sid, gate, started).await
    }

    pub async fn start_wake_run(&self, app: &App, sid: SessionId) -> Result<Session> {
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            match app.start_wake_checked(sid, true).await {
                Err(err) if is_membership_changed_err(&err) => {}
                other => return other,
            }
        }

        let lock = self.start_lock(sid);
        let _guard = lock.lock().await;

        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            match app.start_wake_checked(sid, true).await {
                Err(err) if is_membership_changed_err(&err) => {}
                other => return other,
            }
        }
        if self.presence.local_owner(sid).is_some() {
            return app.start_wake_checked(sid, false).await;
        }

        let gate = self
            .acquire_connected_gate(
                sid,
                GatePurpose::Run {
                    require_model_caps: true,
                },
            )
            .await?;
        let started = app.start_wake_checked(sid, false).await;
        self.finish_connected_start(sid, gate, started).await
    }

    async fn finish_connected_start(
        &self,
        sid: SessionId,
        mut gate: ConnectedGate,
        started: Result<Session>,
    ) -> Result<Session> {
        let Ok(session) = started else {
            if let Some(cg) = gate.claim_guard.take() {
                cg.release_now().await;
            }
            return started;
        };
        if !gate.online_peers.is_empty() && self.presence.local_owner(sid).is_some() {
            self.broadcast_presence_to(&gate.online_peers).await;
        }
        if let Some(cg) = gate.claim_guard.take() {
            cg.release_now().await;
        }
        Ok(session)
    }

    async fn validate_local_gate_preconditions(
        &self,
        sid: SessionId,
        purpose: GatePurpose,
    ) -> Result<(String, String, BTreeMap<String, i64>)> {
        let presence = self.presence.clone();
        self.db
            .call(move |s| {
                refresh_known_peer_count(&s.conn, &presence)?;
                let current = session(&s.conn, sid)?;
                match purpose {
                    GatePurpose::Delete => {
                        if s.active.contains_key(&sid)
                            || s.pending_wakes.contains(&sid)
                            || codemode::has_session_tasks(s, sid)
                            || matches!(
                                current.status,
                                SessionStatus::Running | SessionStatus::Pausing
                            )
                            || presence.local_owner(sid).is_some()
                            || presence.remote_owner(sid).is_some()
                        {
                            return Err(Error::conflict(
                                "pause the session and stop active tasks before deleting",
                            ));
                        }
                    }
                    GatePurpose::Run { require_model_caps } => {
                        if require_model_caps {
                            crate::catalog::available_capabilities(&s.conn, current.model_id)?;
                            if current.status == SessionStatus::Pausing {
                                return Err(Error::conflict("session is pausing"));
                            }
                        }
                        if !session_branch_complete(&s.conn, sid)? {
                            return Err(Error::conflict(
                                "session branch history is incomplete locally; sync before running",
                            ));
                        }
                    }
                }
                let head_ts: String = s
                    .conn
                    .prepare_cached("SELECT active_head_ts FROM sessions WHERE id=? AND deleted=0")?
                    .query_row([sid], |r| r.get(0))?;
                let vector = read_vector(&s.conn, &session_scope(sid))?;
                Ok((s.local_node_id.clone(), head_ts, vector))
            })
            .await
    }

    async fn acquire_connected_gate(
        &self,
        sid: SessionId,
        purpose: GatePurpose,
    ) -> Result<ConnectedGate> {
        let require_model_caps =
            matches!(purpose, GatePurpose::Run { require_model_caps: true });
        let is_delete = matches!(purpose, GatePurpose::Delete);
        let max_attempts = if is_delete { 4 } else { 6 };

        for attempt in 0..max_attempts {
            let known_pre = self
                .db
                .call({
                    let presence = self.presence.clone();
                    move |s| refresh_known_peer_count(&s.conn, &presence)
                })
                .await?;
            let online = self.net.online_peer_ids();

            if online.is_empty() {
                if known_pre > 0 {
                    // Isolated partition: allow local start/delete without remote consensus.
                    self.validate_local_gate_preconditions(sid, purpose).await?;
                    if require_model_caps {
                        self.sync.ensure_session_attachments(sid).await?;
                    }
                } else {
                    self.validate_local_gate_preconditions(sid, purpose).await?;
                }
                if self.net.online_peer_ids().is_empty() {
                    return Ok(ConnectedGate {
                        online_peers: Vec::new(),
                        claim_guard: None,
                    });
                }
                continue;
            }

            if self.presence.local_owner(sid).is_some()
                || self.presence.remote_owner(sid).is_some()
            {
                let err_msg = if is_delete {
                    "pause the session and stop active tasks before deleting".to_owned()
                } else if let Some((owner_id, _)) = self.presence.remote_owner(sid) {
                    format!("session is currently occupied on node {owner_id}")
                } else {
                    "session is already occupied on this node".to_owned()
                };
                return Err(Error::conflict(err_msg));
            }

            let local_id_pre = self.presence.local_node_id();
            let (claim_id, started_ms) = match self.presence.try_acquire_local_claim(sid) {
                Ok(pair) => pair,
                Err((_msg, true)) if !is_delete && attempt + 1 < max_attempts => {
                    sleep_jitter(true).await;
                    continue;
                }
                Err((msg, _)) => return Err(Error::conflict(msg)),
            };
            let mut claim_guard = ClaimReleaseGuard::new(
                self.presence.clone(),
                self.self_weak.get().and_then(Weak::upgrade),
                sid,
                local_id_pre,
                claim_id.clone(),
            );

            if let Err(err) = self.sync.sync_session_best_effort(sid).await {
                claim_guard.release_now().await;
                return Err(err);
            }
            let (local_id, local_head_ts, local_vector) =
                match self.validate_local_gate_preconditions(sid, purpose).await {
                    Ok(v) => v,
                    Err(err) => {
                        claim_guard.release_now().await;
                        return Err(err);
                    }
                };
            if require_model_caps
                && let Err(err) = self.sync.ensure_session_attachments(sid).await
            {
                claim_guard.release_now().await;
                return Err(err);
            }

            let online = self.net.online_peer_ids();
            if online.is_empty() {
                claim_guard.release_now().await;
                return Ok(ConnectedGate {
                    online_peers: Vec::new(),
                    claim_guard: None,
                });
            }

            claim_guard.track_peers(&online);
            let claim = RunClaim {
                claim_id: claim_id.clone(),
                node_id: local_id.clone(),
                started_ms,
            };
            let req = ExecReq::CheckOccupancy {
                session_id: sid,
                claim: Some(claim),
                visited: vec![local_id.clone()],
                hops_left: MAX_CHECK_HOPS,
            };
            let replies = futures_util::future::join_all(online.iter().cloned().map(|peer_id| {
                let req_ref = &req;
                async move {
                    (
                        peer_id.clone(),
                        self.exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT).await,
                    )
                }
            }))
            .await;

            let idle_confirmers: HashSet<String> = replies
                .iter()
                .filter_map(|(peer_id, r)| match r {
                    Ok(ExecResp::Occupancy {
                        occupied: false, ..
                    }) => Some(peer_id.clone()),
                    _ => None,
                })
                .collect();

            let mut deleted_peers = Vec::new();
            let mut ahead_peers = Vec::new();
            let mut retry_by_claim: Option<String> = None;
            let mut hard_occupied: Option<String> = None;

            for (peer_id, reply) in replies {
                match reply {
                    Ok(ExecResp::Occupancy {
                        occupied,
                        runner_node_id,
                        occupied_by_claim,
                        session_deleted,
                        session_active_head_ts,
                        session_vector,
                        ..
                    }) => {
                        if session_deleted {
                            deleted_peers.push(peer_id);
                            continue;
                        }
                        if peer_ahead_of_local(
                            false,
                            &session_active_head_ts,
                            &session_vector,
                            &local_head_ts,
                            &local_vector,
                        ) {
                            ahead_peers.push(peer_id.clone());
                        }
                        if occupied {
                            let holder = runner_node_id.unwrap_or(peer_id);
                            if !is_delete
                                && (occupied_by_claim || idle_confirmers.contains(&holder))
                            {
                                retry_by_claim.get_or_insert(holder);
                            } else {
                                hard_occupied.get_or_insert(holder);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(%sid, %peer_id, error = %err, "peer unreachable during connected-component occupancy check");
                    }
                }
            }

            if !deleted_peers.is_empty() {
                claim_guard.release_now().await;
                let _ = self.sync.sync_session_strict(sid, &deleted_peers).await;
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            if let Some(holder) = hard_occupied {
                claim_guard.release_now().await;
                return Err(Error::conflict(if is_delete {
                    "pause the session and stop active tasks before deleting".to_owned()
                } else {
                    format!("session is currently occupied on node {holder}")
                }));
            }
            if !ahead_peers.is_empty() {
                claim_guard.release_now().await;
                let _ = self.sync.sync_session_strict(sid, &ahead_peers).await;
                if attempt + 1 < max_attempts {
                    sleep_jitter(false).await;
                    continue;
                }
                return Err(Error::conflict(if is_delete {
                    "session state advanced on cluster peers during delete gate; retry"
                } else {
                    "session state advanced on cluster peers while starting; retry"
                }));
            }
            if let Some(holder) = retry_by_claim {
                claim_guard.release_now().await;
                if attempt + 1 < max_attempts {
                    let prefer_early = local_id < holder;
                    sleep_jitter(prefer_early).await;
                    continue;
                }
                return Err(Error::conflict("concurrent cluster start detected; retry"));
            }

            match self.presence.promote_local_claim_to_committing(sid, &claim_id) {
                Ok(()) => {}
                Err((msg, retryable)) => {
                    claim_guard.release_now().await;
                    if retryable && attempt + 1 < max_attempts {
                        sleep_jitter(false).await;
                        continue;
                    }
                    return Err(Error::conflict(msg));
                }
            }

            return Ok(ConnectedGate {
                online_peers: online,
                claim_guard: Some(claim_guard),
            });
        }

        Err(Error::conflict(if is_delete {
            "failed to acquire delete gate"
        } else {
            "concurrent cluster start conflict; retry"
        }))
    }

    pub async fn pause_run(&self, app: &App, sid: SessionId, hard: bool) -> Result<Session> {
        if self.presence.known_peer_count() == 0 {
            return app.pause_local(sid, hard).await;
        }
        self.sync.ensure_session_watched(sid).await?;
        if self.presence.local_owner(sid).is_some() {
            return app.pause_local(sid, hard).await;
        }
        let target = match self.presence.remote_owner(sid) {
            Some((peer_id, _)) => Some(peer_id),
            None => self.probe_online_occupancy(sid).await?,
        };
        if let Some(peer_id) = target {
            if !self.net.is_peer_online(&peer_id) {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("session is running on node {peer_id}, which is not directly reachable from this node"),
                ));
            }
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
        if self.presence.known_peer_count() == 0 {
            return Ok(WriterTarget::Local);
        }
        if self.presence.local_owner(sid).is_some() {
            return Ok(WriterTarget::Local);
        }
        if let Some((peer_id, _)) = self.presence.remote_owner(sid) {
            if self.net.is_peer_online(&peer_id) {
                return Ok(WriterTarget::Remote(peer_id));
            }
            return Err(Error::conflict(format!(
                "session is currently occupied on node {peer_id} (not directly reachable)"
            )));
        }
        if !self.net.online_peer_ids().is_empty() {
            if let Some(peer_id) = self.probe_online_occupancy(sid).await? {
                if self.net.is_peer_online(&peer_id) {
                    return Ok(WriterTarget::Remote(peer_id));
                }
                return Err(Error::conflict(format!(
                    "session is currently occupied on node {peer_id} (not directly reachable)"
                )));
            }
            self.sync.sync_session_best_effort(sid).await?;
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

    pub async fn acquire_delete_gate(&self, sid: SessionId) -> Result<DeleteGateGuard> {
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return Ok(DeleteGateGuard {
                guard: None,
                _lock: None,
            });
        }
        let lock_guard = self.start_lock(sid).lock_owned().await;
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return Ok(DeleteGateGuard {
                guard: None,
                _lock: Some(lock_guard),
            });
        }
        let mut gate = self
            .acquire_connected_gate(sid, GatePurpose::Delete)
            .await?;
        Ok(DeleteGateGuard {
            guard: gate.claim_guard.take(),
            _lock: Some(lock_guard),
        })
    }

    pub async fn list_tasks(&self, sid: SessionId) -> Result<Value> {
        if self.presence.local_owner(sid).is_none()
            && let Some(peer_id) = self.remote_runner_id(sid)
        {
            if !self.net.is_peer_online(&peer_id) {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("session is active on node {peer_id}, which is not directly reachable from this node"),
                ));
            }
            match self
                .exec_rpc(
                    &peer_id,
                    &ExecReq::ListTasks { session_id: sid },
                    EXEC_SHORT_TIMEOUT,
                )
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
        if self.presence.local_owner(sid).is_none()
            && let Some(peer_id) = self.remote_runner_id(sid)
        {
            if !self.net.is_peer_online(&peer_id) {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("session is active on node {peer_id}, which is not directly reachable from this node"),
                ));
            }
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
                codemode::stop_task(s, sid, id).ok_or_else(|| {
                    Error(axum::http::StatusCode::NOT_FOUND, "task is not active".into())
                })
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

async fn sleep_jitter(prefer_early: bool) {
    let mut raw = [0u8; 1];
    let _ = getrandom::fill(&mut raw);
    let ms = if prefer_early {
        15 + u64::from(raw[0] % 16)
    } else {
        65 + u64::from(raw[0] % 46)
    };
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

fn peer_ahead_of_local(
    peer_deleted: bool,
    peer_head_ts: &str,
    peer_vec: &BTreeMap<String, i64>,
    local_head_ts: &str,
    local_vec: &BTreeMap<String, i64>,
) -> bool {
    if peer_deleted {
        return false;
    }
    peer_head_ts > local_head_ts
        || peer_vec
            .iter()
            .any(|(node, &seq)| seq > local_vec.get(node).copied().unwrap_or(0))
}

struct ClaimReleaseGuard {
    presence: Arc<PresenceState>,
    plane: Option<Arc<ExecPlane>>,
    sid: SessionId,
    holder_node_id: String,
    claim_id: String,
    peers: Vec<String>,
    armed: bool,
}

impl ClaimReleaseGuard {
    fn new(
        presence: Arc<PresenceState>,
        plane: Option<Arc<ExecPlane>>,
        sid: SessionId,
        holder_node_id: String,
        claim_id: String,
    ) -> Self {
        Self {
            presence,
            plane,
            sid,
            holder_node_id,
            claim_id,
            peers: Vec::new(),
            armed: true,
        }
    }

    fn track_peers(&mut self, peers: &[String]) {
        for peer_id in peers {
            if !self.peers.contains(peer_id) {
                self.peers.push(peer_id.clone());
            }
        }
    }

    async fn release_now(mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.presence.release_local_claim(self.sid, &self.claim_id);
        let peers = std::mem::take(&mut self.peers);
        if peers.is_empty() {
            return;
        }
        let Some(plane) = self.plane.take() else {
            return;
        };
        let holder = std::mem::take(&mut self.holder_node_id);
        let req = ExecReq::ReleaseClaim {
            session_id: self.sid,
            visited: vec![holder.clone()],
            holder_node_id: holder,
            claim_id: std::mem::take(&mut self.claim_id),
            hops_left: MAX_CHECK_HOPS,
        };
        futures_util::future::join_all(peers.into_iter().map(|peer_id| {
            let req_ref = &req;
            let plane = plane.clone();
            async move {
                let _ = plane.exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT).await;
            }
        }))
        .await;
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
        let holder = std::mem::take(&mut self.holder_node_id);
        let claim_id = std::mem::take(&mut self.claim_id);
        let workers = plane.workers.clone();
        workers.spawn(async move {
            let req = ExecReq::ReleaseClaim {
                session_id: sid,
                visited: vec![holder.clone()],
                holder_node_id: holder,
                claim_id,
                hops_left: MAX_CHECK_HOPS,
            };
            futures_util::future::join_all(peers.into_iter().map(|peer_id| {
                let req_ref = &req;
                let plane = plane.clone();
                async move {
                    let _ = plane.exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT).await;
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
    peer_sse_sems: Mutex<HashMap<String, Weak<Semaphore>>>,
    byte_budget: Arc<Semaphore>,
}

impl ExecServer {
    pub fn new(app: App) -> Self {
        Self {
            app,
            global_sem: Arc::new(Semaphore::new(MAX_GLOBAL_INBOUND_EXEC)),
            peer_sems: Mutex::default(),
            sse_sem: Arc::new(Semaphore::new(MAX_REMOTE_SSE_SUBS)),
            peer_sse_sems: Mutex::default(),
            byte_budget: Arc::new(Semaphore::new(MAX_EXEC_BYTE_PERMITS)),
        }
    }

    fn peer_sem(&self, node_id: &str) -> Arc<Semaphore> {
        Self::get_or_init_peer_sem(&self.peer_sems, node_id, MAX_PEER_INBOUND_EXEC)
    }

    fn peer_sse_sem(&self, node_id: &str) -> Arc<Semaphore> {
        Self::get_or_init_peer_sem(&self.peer_sse_sems, node_id, MAX_PEER_REMOTE_SSE_SUBS)
    }

    fn get_or_init_peer_sem(
        map: &Mutex<HashMap<String, Weak<Semaphore>>>,
        node_id: &str,
        limit: usize,
    ) -> Arc<Semaphore> {
        let mut map = map.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(sem) = map.get(node_id).and_then(Weak::upgrade) {
            return sem;
        }
        let sem = Arc::new(Semaphore::new(limit));
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
        let Ok(global_permit) = self.global_sem.try_acquire() else {
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
        let Ok(peer_permit) = peer_sem.try_acquire() else {
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
            read_exec_frame::<ExecReq>(
                &mut recv,
                EXEC_REQ_HEADER_MAX,
                EXEC_REQ_BODY_MAX,
                &self.byte_budget,
            ),
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
                sender_instance_id,
                sender_gossip_seq,
                entries,
                want_reply,
            } => {
                self.app.exec.apply_remote_presence(
                    &from_peer,
                    sender_instance_id,
                    sender_gossip_seq,
                    entries,
                );
                if want_reply {
                    let (my_inst, my_seq, my_entries) =
                        self.app.exec.presence().snapshot_for_peer(&from_peer);
                    Self::send_reply(
                        &mut send,
                        &ExecResp::Presence {
                            sender_instance_id: my_inst,
                            sender_gossip_seq: my_seq,
                            entries: my_entries,
                        },
                    )
                    .await;
                } else {
                    Self::send_reply(&mut send, &ExecResp::Ack).await;
                }
            }
            ExecReq::CheckOccupancy {
                session_id,
                claim,
                visited,
                hops_left,
            } => {
                let resp = self
                    .handle_check_occupancy(&from_peer, session_id, claim, visited, hops_left)
                    .await;
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::ReleaseClaim {
                session_id,
                holder_node_id,
                claim_id,
                visited,
                hops_left,
            } => {
                self.handle_release_claim(
                    &from_peer,
                    session_id,
                    holder_node_id,
                    claim_id,
                    visited,
                    hops_left,
                )
                .await;
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
                let peer_sse_sem = self.peer_sse_sem(&from_peer);
                let Ok(_peer_sse_permit) = peer_sse_sem.try_acquire() else {
                    Self::send_reply(
                        &mut send,
                        &ExecResp::Error {
                            status: 429,
                            message: "remote SSE relay per-peer limit reached".into(),
                        },
                    )
                    .await;
                    return;
                };
                // Long-lived SSE streams use their own semaphores and must not hold short-RPC permits.
                drop(peer_permit);
                drop(global_permit);
                self.handle_subscribe_events(session_id, send, peer_cancel)
                    .await;
            }
            ExecReq::OAuthCallback { server_id, url } => {
                let resp = match crate::mcp::oauth::complete_local(&self.app.db, server_id, &url)
                    .await
                {
                    Ok(()) => ExecResp::Ack,
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
        }
    }

    async fn handle_release_claim(
        &self,
        from_peer: &str,
        sid: SessionId,
        holder_node_id: String,
        claim_id: String,
        mut visited: Vec<String>,
        hops_left: u8,
    ) {
        self.app
            .exec
            .presence()
            .release_remote_claim(sid, &holder_node_id, &claim_id);
        let hops = hops_left.min(MAX_CHECK_HOPS);
        if hops == 0 {
            return;
        }
        visited.truncate(MAX_GOSSIP_ENTRIES);
        let local_id = self.app.exec.presence().local_node_id();
        let visited_set: HashSet<&str> = visited.iter().map(String::as_str).collect();
        let downstream: Vec<String> = self
            .app
            .cluster
            .net
            .online_peer_ids()
            .into_iter()
            .filter(|p| {
                p != from_peer
                    && p != &holder_node_id
                    && p != &local_id
                    && !visited_set.contains(p.as_str())
            })
            .collect();
        if downstream.is_empty() {
            return;
        }
        if !visited.contains(&local_id) {
            visited.push(local_id);
        }
        let req = ExecReq::ReleaseClaim {
            session_id: sid,
            holder_node_id,
            claim_id,
            visited,
            hops_left: hops - 1,
        };
        futures_util::future::join_all(downstream.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move {
                let _ = self.app.exec.exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT).await;
            }
        }))
        .await;
    }

    async fn handle_check_occupancy(
        &self,
        from_peer: &str,
        sid: SessionId,
        claim: Option<RunClaim>,
        mut visited: Vec<String>,
        hops_left: u8,
    ) -> ExecResp {
        if let Some(ref c) = claim
            && c.node_id.parse::<EndpointId>().is_err()
        {
            return ExecResp::Error {
                status: 400,
                message: "invalid claim node_id".into(),
            };
        }
        let presence = self.app.exec.presence().clone();
        let local_id = presence.local_node_id();
        let claim_owner = claim
            .as_ref()
            .map(|c| c.node_id.clone())
            .unwrap_or_else(|| from_peer.to_owned());

        visited.truncate(MAX_GOSSIP_ENTRIES);
        let hops = hops_left.min(MAX_CHECK_HOPS);

        let db_res = self
            .app
            .db
            .call({
                let presence = presence.clone();
                let claim_owner = claim_owner.clone();
                move |s| {
                    let local_occ = presence.local_owner(sid);
                    let active_worker =
                        s.active.contains_key(&sid) || s.pending_wakes.contains(&sid);
                    let local_active_tasks = codemode::count_session_tasks(s, sid);
                    let local_occupied =
                        active_worker || local_active_tasks > 0 || local_occ.is_some();
                    let claim_holder = presence.active_claim_holder_excluding(sid, &claim_owner);
                    let local_status = if let Some(occ) = &local_occ {
                        Some(occ.status.clone())
                    } else if local_occupied {
                        session(&s.conn, sid).ok().map(|row| row.status)
                    } else {
                        None
                    };
                    let sess_row: Option<(bool, Option<ContextNodeId>, String)> = s
                        .conn
                        .prepare_cached(
                            "SELECT deleted, active_head, active_head_ts FROM sessions WHERE id=?",
                        )?
                        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                        .optional()?;
                    let (session_deleted, session_active_head, session_active_head_ts) =
                        match sess_row {
                            Some((true, _, _)) => (true, None, String::new()),
                            Some((false, head, ts)) => (false, head, ts),
                            None => (false, None, String::new()),
                        };
                    let session_vector = if session_deleted {
                        BTreeMap::new()
                    } else {
                        read_vector(&s.conn, &session_scope(sid))?
                    };
                    Ok((
                        local_occupied,
                        local_status,
                        local_active_tasks,
                        claim_holder,
                        session_deleted,
                        session_active_head,
                        session_active_head_ts,
                        session_vector,
                    ))
                }
            })
            .await;

        let (
            local_occupied,
            local_status,
            local_active_tasks,
            claim_holder,
            session_deleted,
            session_active_head,
            session_active_head_ts,
            session_vector,
        ) = match db_res {
            Ok(v) => v,
            Err(e) => return Self::to_wire_err(e),
        };

        if session_deleted {
            return ExecResp::Occupancy {
                occupied: false,
                runner_node_id: None,
                status: None,
                active_tasks: 0,
                occupied_by_claim: false,
                session_deleted: true,
                session_active_head: None,
                session_active_head_ts: String::new(),
                session_vector: BTreeMap::new(),
            };
        }

        if local_occupied {
            return ExecResp::Occupancy {
                occupied: true,
                runner_node_id: Some(local_id),
                status: local_status,
                active_tasks: local_active_tasks,
                occupied_by_claim: false,
                session_deleted: false,
                session_active_head,
                session_active_head_ts,
                session_vector,
            };
        }

        if claim.is_none()
            && let Some((holder, by_claim)) = claim_holder
        {
            return ExecResp::Occupancy {
                occupied: true,
                runner_node_id: Some(holder),
                status: None,
                active_tasks: 0,
                occupied_by_claim: by_claim,
                session_deleted: false,
                session_active_head,
                session_active_head_ts,
                session_vector,
            };
        }

        // Forward across online downstream peers in the connected component.
        let visited_set: HashSet<String> = visited.iter().cloned().collect();
        let downstream: Vec<String> = if hops > 0 {
            self.app
                .cluster
                .net
                .online_peer_ids()
                .into_iter()
                .filter(|p| {
                    p != from_peer
                        && p != &claim_owner
                        && p != &local_id
                        && !visited_set.contains(p)
                })
                .collect()
        } else {
            Vec::new()
        };

        let mut confirmed_idle: HashSet<String> = HashSet::new();
        if !downstream.is_empty() {
            let mut next_visited = visited.clone();
            if !next_visited.contains(&local_id) && next_visited.len() < MAX_GOSSIP_ENTRIES {
                next_visited.push(local_id.clone());
            }
            let fwd_req = ExecReq::CheckOccupancy {
                session_id: sid,
                claim: claim.clone(),
                visited: next_visited,
                hops_left: hops - 1,
            };
            let fwd_replies =
                futures_util::future::join_all(downstream.into_iter().map(|peer_id| {
                    let req_ref = &fwd_req;
                    async move {
                        (
                            peer_id.clone(),
                            self.app
                                .exec
                                .exec_rpc(&peer_id, req_ref, EXEC_PROBE_TIMEOUT)
                                .await,
                        )
                    }
                }))
                .await;

            let mut deleted_peers = Vec::new();
            let mut ahead_peers = Vec::new();
            let mut fwd_occupied: Option<(String, Option<SessionStatus>, usize, bool)> = None;

            for (peer_id, reply) in fwd_replies {
                if let Ok(ExecResp::Occupancy {
                    occupied,
                    runner_node_id,
                    status,
                    active_tasks,
                    occupied_by_claim,
                    session_deleted: d_del,
                    session_active_head_ts: d_ts,
                    session_vector: d_vec,
                    ..
                }) = reply
                {
                    if d_del {
                        deleted_peers.push(peer_id);
                        continue;
                    }
                    if peer_ahead_of_local(false, &d_ts, &d_vec, &session_active_head_ts, &session_vector) {
                        ahead_peers.push(peer_id.clone());
                    }
                    if occupied {
                        let holder = runner_node_id.unwrap_or(peer_id);
                        if holder != claim_owner && fwd_occupied.is_none() {
                            fwd_occupied = Some((holder, status, active_tasks, occupied_by_claim));
                        }
                    } else {
                        confirmed_idle.insert(peer_id);
                    }
                }
            }

            if !deleted_peers.is_empty() {
                let _ = self.app.cluster.sync.sync_session_strict(sid, &deleted_peers).await;
                return ExecResp::Occupancy {
                    occupied: false,
                    runner_node_id: None,
                    status: None,
                    active_tasks: 0,
                    occupied_by_claim: false,
                    session_deleted: true,
                    session_active_head: None,
                    session_active_head_ts: String::new(),
                    session_vector: BTreeMap::new(),
                };
            }

            if let Some((holder, status, active_tasks, by_claim)) = fwd_occupied {
                return ExecResp::Occupancy {
                    occupied: true,
                    runner_node_id: Some(holder),
                    status,
                    active_tasks,
                    occupied_by_claim: by_claim,
                    session_deleted: false,
                    session_active_head,
                    session_active_head_ts,
                    session_vector,
                };
            }

            if !ahead_peers.is_empty() {
                let _ = self.app.cluster.sync.sync_session_strict(sid, &ahead_peers).await;
            }
        }

        // Check Gossip table for any node not equal to requester/visited/live-confirmed idle.
        if let Some((owner_id, occ)) = presence.remote_owner(sid)
            && owner_id != claim_owner
            && owner_id != from_peer
            && owner_id != local_id
            && !visited_set.contains(&owner_id)
            && !confirmed_idle.contains(&owner_id)
        {
            return ExecResp::Occupancy {
                occupied: true,
                runner_node_id: Some(owner_id),
                status: Some(occ.status),
                active_tasks: occ.active_tasks,
                occupied_by_claim: false,
                session_deleted: false,
                session_active_head,
                session_active_head_ts,
                session_vector,
            };
        }

        // Final local re-check and atomic remote claim registration.
        let final_res = self
            .app
            .db
            .call(move |s| {
                let local_occ = presence.local_owner(sid);
                let active_worker =
                    s.active.contains_key(&sid) || s.pending_wakes.contains(&sid);
                let local_active_tasks = codemode::count_session_tasks(s, sid);
                let local_occupied_now =
                    active_worker || local_active_tasks > 0 || local_occ.is_some();

                let sess_row: Option<(bool, Option<ContextNodeId>, String)> = s
                    .conn
                    .prepare_cached(
                        "SELECT deleted, active_head, active_head_ts FROM sessions WHERE id=?",
                    )?
                    .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .optional()?;
                let (deleted_now, head_now, ts_now) = match sess_row {
                    Some((true, _, _)) => (true, None, String::new()),
                    Some((false, head, ts)) => (false, head, ts),
                    None => (false, None, String::new()),
                };
                if deleted_now {
                    return Ok(ExecResp::Occupancy {
                        occupied: false,
                        runner_node_id: None,
                        status: None,
                        active_tasks: 0,
                        occupied_by_claim: false,
                        session_deleted: true,
                        session_active_head: None,
                        session_active_head_ts: String::new(),
                        session_vector: BTreeMap::new(),
                    });
                }
                let vec_now = read_vector(&s.conn, &session_scope(sid))?;

                if local_occupied_now {
                    let st = local_occ
                        .map(|o| o.status)
                        .or_else(|| session(&s.conn, sid).ok().map(|r| r.status));
                    return Ok(ExecResp::Occupancy {
                        occupied: true,
                        runner_node_id: Some(s.local_node_id.clone()),
                        status: st,
                        active_tasks: local_active_tasks,
                        occupied_by_claim: false,
                        session_deleted: false,
                        session_active_head: head_now,
                        session_active_head_ts: ts_now,
                        session_vector: vec_now,
                    });
                }

                if let Some(ref c) = claim {
                    if let Err((holder, by_claim)) = presence.check_or_register_remote_claim(sid, c)
                    {
                        return Ok(ExecResp::Occupancy {
                            occupied: true,
                            runner_node_id: Some(holder),
                            status: None,
                            active_tasks: 0,
                            occupied_by_claim: by_claim,
                            session_deleted: false,
                            session_active_head: head_now,
                            session_active_head_ts: ts_now,
                            session_vector: vec_now,
                        });
                    }
                } else if let Some((holder, by_claim)) =
                    presence.active_claim_holder_excluding(sid, &claim_owner)
                {
                    return Ok(ExecResp::Occupancy {
                        occupied: true,
                        runner_node_id: Some(holder),
                        status: None,
                        active_tasks: 0,
                        occupied_by_claim: by_claim,
                        session_deleted: false,
                        session_active_head: head_now,
                        session_active_head_ts: ts_now,
                        session_vector: vec_now,
                    });
                }

                Ok(ExecResp::Occupancy {
                    occupied: false,
                    runner_node_id: None,
                    status: None,
                    active_tasks: 0,
                    occupied_by_claim: false,
                    session_deleted: false,
                    session_active_head: head_now,
                    session_active_head_ts: ts_now,
                    session_vector: vec_now,
                })
            })
            .await;

        match final_res {
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

    async fn handle_patch_local(
        &self,
        sid: SessionId,
        fields: Map<String, Value>,
    ) -> Result<Session> {
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
                    data: snap.to_string(),
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
                ev = stream.next(&self.app) => ev,
            };
            match next {
                NextSessionNotice::Event { event, data } => {
                    if write_exec_frame(
                        &mut send,
                        &ExecResp::SseEvent {
                            event: event.into_owned(),
                            data: data.to_string(),
                        },
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                }
                NextSessionNotice::ClusterActive => continue,
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
