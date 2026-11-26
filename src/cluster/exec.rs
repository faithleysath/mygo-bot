use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use iroh::endpoint::{Connection, RecvStream, SendStream};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::sync::{
    Mutex as AsyncMutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, mpsc, watch,
};
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
    id::{ItemId, McpServerId, SessionId},
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
const EXEC_LEASE_TIMEOUT: Duration = Duration::from_secs(3);
const EXEC_OP_TIMEOUT: Duration = Duration::from_secs(30);
const CLAIM_TTL: Duration = Duration::from_secs(15);
const LEASE_TTL: Duration = Duration::from_secs(10);
const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(3);
const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(1);

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
    #[serde(default)]
    pub term: u64,
    #[serde(default)]
    pub ttl_ms: u64,
}

pub struct ActiveLease {
    pub term: u64,
    pub claim_id: String,
    deadline: Mutex<Instant>,
    pub revoked: CancellationToken,
    pub stopped: CancellationToken,
}

impl ActiveLease {
    pub fn new(term: u64, claim_id: String, initial_deadline: Instant) -> Arc<Self> {
        Arc::new(Self {
            term,
            claim_id,
            deadline: Mutex::new(initial_deadline),
            revoked: CancellationToken::new(),
            stopped: CancellationToken::new(),
        })
    }

    pub fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap()
    }

    pub fn extend(&self, next_deadline: Instant) {
        let mut dl = self.deadline.lock().unwrap();
        if next_deadline > *dl {
            *dl = next_deadline;
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.revoked.is_cancelled() && Instant::now() < *self.deadline.lock().unwrap()
    }
}

#[derive(Clone)]
pub enum LocalLease {
    Quorum(Arc<ActiveLease>),
    ForcedOffline,
}

impl LocalLease {
    fn active_term(&self) -> Option<u64> {
        match self {
            Self::Quorum(l) if l.is_valid() => Some(l.term),
            Self::ForcedOffline => Some(0),
            Self::Quorum(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalClaimPhase {
    Probing,
    Committing,
    Yielded(String),
}

struct LocalClaim {
    claim_id: String,
    term: u64,
    phase: LocalClaimPhase,
    acquired_at: Instant,
}

#[derive(Clone, Debug)]
struct GrantedLease {
    term: u64,
    holder_node_id: String,
    claim_id: String,
    expires_at: Instant,
    committed: bool,
}

struct PresenceInner {
    instance_id: u64,
    local_seq: u64,
    local: HashMap<SessionId, SessionOccupancy>,
    remote_seq: HashMap<String, (u64, u64)>,
    remote: HashMap<String, HashMap<SessionId, SessionOccupancy>>,
    local_claims: HashMap<SessionId, LocalClaim>,
    granted_leases: HashMap<SessionId, GrantedLease>,
    highest_terms: HashMap<SessionId, u64>,
    active_local_leases: HashMap<SessionId, LocalLease>,
    session_watchers: HashMap<SessionId, watch::Sender<Option<String>>>,
}

pub struct PresenceState {
    local_node_id: Mutex<String>,
    known_peer_count: AtomicUsize,
    local_nonempty: AtomicBool,
    has_null_attachments: AtomicBool,
    deferred_recovery_pending: AtomicBool,
    recovery_notify: Notify,
    membership_grew_notify: Notify,
    cluster_active_tx: watch::Sender<bool>,
    inner: Mutex<PresenceInner>,
    local_changed: Notify,
}

pub fn refresh_known_peer_count(
    conn: &rusqlite::Connection,
    presence: &PresenceState,
) -> Result<usize> {
    let count: i64 = conn.query_row("SELECT count(*) FROM cluster_peers", [], |r| r.get(0))?;
    let count_usize = usize::try_from(count).unwrap_or(0);
    presence.set_known_peer_count(count_usize);
    Ok(count_usize)
}

pub(crate) enum GrantOutcome {
    Granted {
        highest_term: u64,
        expires_at_ms: i64,
    },
    Occupied {
        holder: String,
        by_claim: bool,
        highest_term: u64,
    },
    StaleTerm {
        highest_term: u64,
    },
}

impl PresenceState {
    pub fn new() -> Arc<Self> {
        let mut raw = [0u8; 8];
        let _ = getrandom::fill(&mut raw);
        let instance_id = u64::from_le_bytes(raw).max(1);
        let (cluster_active_tx, _) = watch::channel(false);
        Arc::new(Self {
            local_node_id: Mutex::new(String::new()),
            known_peer_count: AtomicUsize::new(0),
            local_nonempty: AtomicBool::new(false),
            has_null_attachments: AtomicBool::new(false),
            deferred_recovery_pending: AtomicBool::new(false),
            recovery_notify: Notify::new(),
            membership_grew_notify: Notify::new(),
            cluster_active_tx,
            inner: Mutex::new(PresenceInner {
                instance_id,
                local_seq: 1,
                local: HashMap::new(),
                remote_seq: HashMap::new(),
                remote: HashMap::new(),
                local_claims: HashMap::new(),
                granted_leases: HashMap::new(),
                highest_terms: HashMap::new(),
                active_local_leases: HashMap::new(),
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

    pub fn set_known_peer_count(&self, count: usize) {
        let prev = self.known_peer_count.swap(count, Ordering::AcqRel);
        if (prev == 0) != (count == 0) {
            let _ = self.cluster_active_tx.send(count > 0);
        }
        if count > prev {
            self.membership_grew_notify.notify_one();
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
        self.local_nonempty.store(
            !inner.local.is_empty() || !inner.active_local_leases.is_empty(),
            Ordering::Release,
        );
    }

    pub fn set_has_null_attachments(&self, v: bool) {
        self.has_null_attachments.store(v, Ordering::Release);
    }

    pub fn has_null_attachments(&self) -> bool {
        self.has_null_attachments.load(Ordering::Acquire)
    }

    pub fn set_deferred_recovery_pending(&self, pending: bool) {
        self.deferred_recovery_pending
            .store(pending, Ordering::Release);
        if pending {
            self.recovery_notify.notify_one();
        }
    }

    pub fn is_deferred_recovery_pending(&self) -> bool {
        self.deferred_recovery_pending.load(Ordering::Acquire)
    }

    pub fn notify_recovery(&self) {
        self.recovery_notify.notify_one();
    }

    pub fn load_persisted_leases(&self, rows: Vec<(SessionId, u64, String, String, i64)>) {
        let now = Instant::now();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut inner = self.inner.lock().unwrap();
        for (sid, term, holder, claim_id, expires_at_ms) in rows {
            if term > 0 {
                let entry = inner.highest_terms.entry(sid).or_insert(0);
                *entry = (*entry).max(term);
            }
            if !holder.is_empty() && !claim_id.is_empty() && expires_at_ms > now_ms {
                let rem_ms = u64::try_from(expires_at_ms - now_ms)
                    .unwrap_or(0)
                    .min(LEASE_TTL.as_millis() as u64);
                if rem_ms > 0 {
                    inner.granted_leases.insert(
                        sid,
                        GrantedLease {
                            term,
                            holder_node_id: holder,
                            claim_id,
                            expires_at: now + Duration::from_millis(rem_ms),
                            committed: true,
                        },
                    );
                }
            }
        }
    }

    pub fn set_active_local_lease(&self, sid: SessionId, lease: Arc<ActiveLease>) {
        let prev = {
            let mut inner = self.inner.lock().unwrap();
            let cur = inner.highest_terms.entry(sid).or_insert(0);
            *cur = (*cur).max(lease.term);
            let old = inner
                .active_local_leases
                .insert(sid, LocalLease::Quorum(lease));
            self.update_local_nonempty_locked(&inner);
            old
        };
        if let Some(LocalLease::Quorum(old_l)) = prev {
            old_l.stopped.cancel();
        }
    }

    pub fn set_active_local_forced_offline(&self, sid: SessionId) {
        let prev = {
            let mut inner = self.inner.lock().unwrap();
            let old = inner
                .active_local_leases
                .insert(sid, LocalLease::ForcedOffline);
            self.update_local_nonempty_locked(&inner);
            old
        };
        if let Some(LocalLease::Quorum(old_l)) = prev {
            old_l.stopped.cancel();
        }
    }

    pub fn clear_active_local_lease(&self, sid: SessionId, claim_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(LocalLease::Quorum(cur)) = inner.active_local_leases.get(&sid)
            && cur.claim_id == claim_id
            && let Some(LocalLease::Quorum(lease)) = inner.active_local_leases.remove(&sid)
        {
            self.update_local_nonempty_locked(&inner);
            drop(inner);
            lease.stopped.cancel();
        }
    }

    pub fn active_local_lease(&self, sid: SessionId) -> Option<Arc<ActiveLease>> {
        let inner = self.inner.lock().unwrap();
        match inner.active_local_leases.get(&sid) {
            Some(LocalLease::Quorum(l)) => Some(l.clone()),
            _ => None,
        }
    }

    pub fn active_local_lease_kind(&self, sid: SessionId) -> Option<LocalLease> {
        self.inner.lock().unwrap().active_local_leases.get(&sid).cloned()
    }

    pub fn is_local_lease_valid(&self, sid: SessionId) -> bool {
        if self.known_peer_count.load(Ordering::Acquire) == 0 {
            return true;
        }
        let inner = self.inner.lock().unwrap();
        match inner.active_local_leases.get(&sid) {
            Some(LocalLease::Quorum(l)) => l.is_valid(),
            Some(LocalLease::ForcedOffline) => true,
            None => false,
        }
    }

    pub fn observe_remote_term(&self, sid: SessionId, term: u64) {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.highest_terms.entry(sid).or_insert(0);
        *entry = (*entry).max(term);
    }

    pub fn has_committed_remote_lease(&self, sid: SessionId) -> bool {
        let now = Instant::now();
        let local_id = self.local_node_id();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        inner
            .granted_leases
            .get(&sid)
            .is_some_and(|gl| gl.committed && gl.holder_node_id != local_id)
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
            .granted_leases
            .retain(|_, gl| now < gl.expires_at);
    }

    pub fn set_local(&self, sid: SessionId, next: Option<SessionOccupancy>) -> bool {
        let (changed, cleared_lease) = {
            let mut inner = self.inner.lock().unwrap();
            let unchanged = inner.local.get(&sid) == next.as_ref();
            let cleared = if next.is_none() {
                match inner.active_local_leases.get(&sid) {
                    Some(LocalLease::Quorum(l)) if !l.revoked.is_cancelled() => {
                        inner.active_local_leases.remove(&sid)
                    }
                    Some(LocalLease::ForcedOffline) => inner.active_local_leases.remove(&sid),
                    _ => None,
                }
            } else {
                None
            };
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
            }
            self.update_local_nonempty_locked(&inner);
            (!unchanged, cleared)
        };
        if let Some(LocalLease::Quorum(lease)) = cleared_lease {
            lease.stopped.cancel();
        }
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

    pub fn active_claim_holder_excluding(
        &self,
        sid: SessionId,
        exclude_peer: &str,
    ) -> Option<(String, bool)> {
        let now = Instant::now();
        let local_id = self.local_node_id();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if let Some(c) = inner.local_claims.get(&sid) {
            match c.phase {
                LocalClaimPhase::Probing => return Some((local_id, true)),
                LocalClaimPhase::Committing => return Some((local_id, false)),
                LocalClaimPhase::Yielded(_) => {}
            }
        }
        if let Some(lease) = inner.active_local_leases.get(&sid)
            && lease.active_term().is_some()
        {
            return Some((local_id, false));
        }
        if let Some(gl) = inner.granted_leases.get(&sid)
            && gl.holder_node_id != exclude_peer
        {
            return Some((gl.holder_node_id.clone(), !gl.committed));
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
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if let Some((claim_sid, claim_id)) = clear_claim
            && inner
                .granted_leases
                .get(&claim_sid)
                .is_some_and(|gl| gl.holder_node_id == peer_id && gl.claim_id == claim_id)
        {
            inner.granted_leases.remove(&claim_sid);
        }
        for occ in &sessions {
            if let Some(gl) = inner.granted_leases.get_mut(&occ.session_id)
                && gl.holder_node_id == peer_id
            {
                gl.committed = true;
            }
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
        inner.granted_leases.retain(|sid, gl| {
            !(gl.holder_node_id == peer_id && gl.committed && !next_map.contains_key(sid))
        });
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
        drop(inner);
        if !changed.is_empty() && self.is_deferred_recovery_pending() {
            self.recovery_notify.notify_one();
        }
        changed
    }

    pub fn clear_peer(&self, peer_id: &str) -> Vec<SessionId> {
        let mut inner = self.inner.lock().unwrap();
        inner.remote_seq.remove(peer_id);
        inner
            .granted_leases
            .retain(|_, gl| gl.holder_node_id != peer_id || gl.committed);
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
        inner.granted_leases.clear();
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

    pub fn try_acquire_local_claim(
        &self,
        sid: SessionId,
    ) -> std::result::Result<(String, u64), (String, bool)> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| ("entropy unavailable".to_owned(), false))?;
        let claim_id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        if inner.local_claims.contains_key(&sid) {
            return Err((
                "session run is already starting on this node".to_owned(),
                true,
            ));
        }
        if let Some(lease) = inner.active_local_leases.get(&sid)
            && lease.active_term().is_some()
        {
            return Err(("session is already running on this node".to_owned(), false));
        }
        if let Some(gl) = inner.granted_leases.get(&sid) {
            return Err((
                format!("session is held by node {}", gl.holder_node_id),
                !gl.committed,
            ));
        }
        let term = inner.highest_terms.get(&sid).copied().unwrap_or(0).saturating_add(1);
        inner.highest_terms.insert(sid, term);
        inner.local_claims.insert(
            sid,
            LocalClaim {
                claim_id: claim_id.clone(),
                term,
                phase: LocalClaimPhase::Probing,
                acquired_at: now,
            },
        );
        Ok((claim_id, term))
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

    pub(crate) fn accept_remote_claim(
        &self,
        sid: SessionId,
        claim: &RunClaim,
        local_occupied: bool,
    ) -> GrantOutcome {
        let now = Instant::now();
        let local_id = self.local_node_id();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let cur_term = inner.highest_terms.get(&sid).copied().unwrap_or(0);
        if local_occupied {
            return GrantOutcome::Occupied {
                holder: local_id,
                by_claim: false,
                highest_term: cur_term,
            };
        }
        if let Some(term) = inner
            .active_local_leases
            .get(&sid)
            .and_then(LocalLease::active_term)
        {
            return GrantOutcome::Occupied {
                holder: local_id,
                by_claim: false,
                highest_term: cur_term.max(term),
            };
        }
        let same_holder = inner
            .granted_leases
            .get(&sid)
            .is_some_and(|gl| gl.holder_node_id == claim.node_id && gl.claim_id == claim.claim_id);
        if let Some(gl) = inner.granted_leases.get(&sid)
            && (gl.holder_node_id != claim.node_id || gl.claim_id != claim.claim_id)
        {
            return GrantOutcome::Occupied {
                holder: gl.holder_node_id.clone(),
                by_claim: !gl.committed,
                highest_term: cur_term.max(gl.term),
            };
        }
        if claim.term == 0 || (!same_holder && claim.term <= cur_term) {
            return GrantOutcome::StaleTerm {
                highest_term: cur_term,
            };
        }
        if let Some(local) = inner.local_claims.get_mut(&sid) {
            match &local.phase {
                LocalClaimPhase::Probing => {
                    let incoming_wins = claim.term > local.term
                        || (claim.term == local.term && claim.node_id < local_id);
                    if !incoming_wins {
                        return GrantOutcome::Occupied {
                            holder: local_id,
                            by_claim: true,
                            highest_term: cur_term.max(local.term),
                        };
                    }
                    local.phase = LocalClaimPhase::Yielded(claim.node_id.clone());
                }
                LocalClaimPhase::Committing => {
                    return GrantOutcome::Occupied {
                        holder: local_id,
                        by_claim: false,
                        highest_term: cur_term.max(local.term),
                    };
                }
                LocalClaimPhase::Yielded(winner) => {
                    if winner != &claim.node_id {
                        return GrantOutcome::Occupied {
                            holder: winner.clone(),
                            by_claim: true,
                            highest_term: cur_term,
                        };
                    }
                }
            }
        }
        let new_term = cur_term.max(claim.term);
        inner.highest_terms.insert(sid, new_term);
        let ttl_ms = claim.ttl_ms.clamp(2_000, 30_000);
        let expires_at = now + Duration::from_millis(ttl_ms);
        let was_committed = inner
            .granted_leases
            .get(&sid)
            .is_some_and(|gl| gl.holder_node_id == claim.node_id && gl.claim_id == claim.claim_id && gl.committed);
        inner.granted_leases.insert(
            sid,
            GrantedLease {
                term: new_term,
                holder_node_id: claim.node_id.clone(),
                claim_id: claim.claim_id.clone(),
                expires_at,
                committed: was_committed,
            },
        );
        let expires_at_ms = chrono::Utc::now().timestamp_millis() + i64::try_from(ttl_ms).unwrap_or(10_000);
        GrantOutcome::Granted {
            highest_term: new_term,
            expires_at_ms,
        }
    }

    pub fn renew_remote_lease(
        &self,
        sid: SessionId,
        peer_id: &str,
        claim_id: &str,
        term: u64,
        ttl_ms: u64,
        local_occupied: bool,
        remote_occupied: bool,
    ) -> std::result::Result<i64, u64> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let cur_term = inner.highest_terms.get(&sid).copied().unwrap_or(0);
        let clamped_ms = ttl_ms.clamp(2_000, 30_000);
        let expires_at = now + Duration::from_millis(clamped_ms);
        let expires_at_ms =
            chrono::Utc::now().timestamp_millis() + i64::try_from(clamped_ms).unwrap_or(10_000);

        if let Some(gl) = inner.granted_leases.get_mut(&sid) {
            if term < cur_term
                || gl.holder_node_id != peer_id
                || gl.claim_id != claim_id
                || gl.term != term
            {
                return Err(cur_term.max(gl.term));
            }
            gl.committed = true;
            gl.expires_at = expires_at;
            return Ok(expires_at_ms);
        }

        // Newly joined peer can adopt an active remote runner's live term when locally free and higher than cur_term.
        if term == 0
            || term <= cur_term
            || claim_id.is_empty()
            || local_occupied
            || remote_occupied
            || inner.local_claims.contains_key(&sid)
            || inner
                .active_local_leases
                .get(&sid)
                .and_then(LocalLease::active_term)
                .is_some()
        {
            return Err(cur_term);
        }
        inner.highest_terms.insert(sid, term);
        inner.granted_leases.insert(
            sid,
            GrantedLease {
                term,
                holder_node_id: peer_id.to_owned(),
                claim_id: claim_id.to_owned(),
                expires_at,
                committed: true,
            },
        );
        Ok(expires_at_ms)
    }

    pub fn promote_local_claim_to_committing(
        &self,
        sid: SessionId,
        claim_id: &str,
    ) -> std::result::Result<(), (String, bool)> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        Self::prune_expired_claims_locked(&mut inner, now);
        let remote_holder = inner
            .granted_leases
            .get(&sid)
            .map(|gl| (gl.holder_node_id.clone(), !gl.committed));
        let Some(local) = inner.local_claims.get_mut(&sid) else {
            return Err(("session run claim expired; retry".to_owned(), true));
        };
        if local.claim_id != claim_id {
            return Err(("session run claim expired; retry".to_owned(), true));
        }
        match &local.phase {
            LocalClaimPhase::Probing => {
                if let Some((holder, by_claim)) = remote_holder {
                    return Err((
                        format!("session is currently occupied on node {holder}"),
                        by_claim,
                    ));
                }
                local.phase = LocalClaimPhase::Committing;
                local.acquired_at = now;
                Ok(())
            }
            LocalClaimPhase::Yielded(winner) => Err((
                format!("session is currently occupied on node {winner}"),
                true,
            )),
            LocalClaimPhase::Committing => Ok(()),
        }
    }

    pub fn release_remote_claim(&self, sid: SessionId, peer_id: &str, claim_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .granted_leases
            .get(&sid)
            .is_some_and(|gl| gl.holder_node_id == peer_id && gl.claim_id == claim_id)
        {
            inner.granted_leases.remove(&sid);
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

fn persist_lease_term(conn: &rusqlite::Connection, sid: SessionId, term: u64) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO session_leases(session_id, highest_term, holder_node_id, claim_id, expires_at_ms)
         VALUES(?1, ?2, '', '', 0)
         ON CONFLICT(session_id) DO UPDATE SET
           highest_term = max(session_leases.highest_term, excluded.highest_term)",
    )?
    .execute(params![sid, i64::try_from(term).unwrap_or(i64::MAX)])?;
    Ok(())
}

fn persist_lease_grant(
    conn: &rusqlite::Connection,
    sid: SessionId,
    term: u64,
    holder: &str,
    claim_id: &str,
    expires_at_ms: i64,
) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO session_leases(session_id, highest_term, holder_node_id, claim_id, expires_at_ms)
         VALUES(?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(session_id) DO UPDATE SET
           highest_term = max(session_leases.highest_term, excluded.highest_term),
           holder_node_id = excluded.holder_node_id,
           claim_id = excluded.claim_id,
           expires_at_ms = excluded.expires_at_ms",
    )?
    .execute(params![
        sid,
        i64::try_from(term).unwrap_or(i64::MAX),
        holder,
        claim_id,
        expires_at_ms
    ])?;
    Ok(())
}

fn clear_persisted_lease(
    conn: &rusqlite::Connection,
    sid: SessionId,
    holder: &str,
    claim_id: &str,
) -> Result<()> {
    conn.prepare_cached(
        "UPDATE session_leases SET holder_node_id='', claim_id='', expires_at_ms=0
         WHERE session_id=?1 AND holder_node_id=?2 AND claim_id=?3",
    )?
    .execute(params![sid, holder, claim_id])?;
    Ok(())
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
    RenewLease {
        session_id: SessionId,
        claim_id: String,
        term: u64,
        ttl_ms: u64,
    },
    StartRun {
        session_id: SessionId,
        #[serde(default)]
        force_offline: bool,
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
        #[serde(default)]
        lease_granted: bool,
        #[serde(default)]
        occupied_by_claim: bool,
        #[serde(default)]
        highest_term: u64,
    },
    LeaseRenewed {
        granted: bool,
        highest_term: u64,
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
    let hdr_permit: OwnedSemaphorePermit = byte_budget
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
    drop(header);
    drop(hdr_permit);

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
        let b_permits = u32::try_from(b_len.div_ceil(EXEC_BYTE_UNIT).max(1))
            .map_err(|_| ReadExecErr::Protocol("exec body permit overflow".into()))?;
        let _body_permit: OwnedSemaphorePermit = byte_budget
            .clone()
            .acquire_many_owned(b_permits)
            .await
            .map_err(|_| ReadExecErr::Transport("exec byte budget closed".into()))?;
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

struct AcquiredLeaseGate {
    expected_head: Option<StreamHead>,
    lease: Option<Arc<ActiveLease>>,
    granted_peers: Vec<String>,
    claim_guard: Option<ClaimReleaseGuard>,
}

pub struct DeleteGateGuard {
    expected_head: Option<StreamHead>,
    lease: Option<Arc<ActiveLease>>,
    guard: Option<ClaimReleaseGuard>,
    _lock: Option<OwnedMutexGuard<()>>,
}

impl DeleteGateGuard {
    pub fn expected_head(&self) -> Option<StreamHead> {
        self.expected_head.clone()
    }

    pub fn lease(&self) -> Option<Arc<ActiveLease>> {
        self.lease.clone()
    }

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
    app_slot: Mutex<Option<App>>,
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
            acked_presence: Mutex::default(),
            app_slot: Mutex::default(),
            self_weak: OnceLock::new(),
        });
        let _ = arc.self_weak.set(Arc::downgrade(&arc));
        arc
    }

    pub fn spawn_loops(self: &Arc<Self>, app: App) {
        *self.app_slot.lock().unwrap() = Some(app.clone());
        let plane_bcast = self.clone();
        self.workers.spawn(async move {
            plane_bcast.presence_broadcast_loop().await;
        });
        let plane_sync = self.clone();
        self.workers.spawn(async move {
            plane_sync.peer_presence_sync_loop().await;
        });
        let plane_rec = self.clone();
        self.workers.spawn(async move {
            plane_rec.deferred_recovery_loop().await;
        });
        let plane_mem = self.clone();
        self.workers.spawn(async move {
            plane_mem.membership_reconcile_loop(app).await;
            *plane_mem.app_slot.lock().unwrap() = None;
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

    pub async fn on_peer_connected(&self, peer_id: String, peer_count: usize) {
        self.presence.set_known_peer_count(peer_count);
        let app = self.app_slot.lock().unwrap().clone();
        if let Some(app) = app {
            self.reconcile_standalone_leases(&app).await;
        }
        {
            self.pending_peer_sync.lock().unwrap().insert(peer_id);
        }
        self.peer_sync_wake.notify_one();
    }

    async fn membership_reconcile_loop(&self, app: App) {
        loop {
            if self.presence.known_peer_count() > 0 {
                self.reconcile_standalone_leases(&app).await;
            }
            tokio::select! {
                biased;
                _ = self.stopping.cancelled() => return,
                _ = self.presence.membership_grew_notify.notified() => (),
            }
        }
    }

    async fn reconcile_standalone_leases(&self, app: &App) {
        let presence = self.presence.clone();
        let Ok((known_peers, candidates)) = self
            .db
            .call(move |s| {
                let peers = refresh_known_peer_count(&s.conn, &presence)?;
                if peers == 0 {
                    return Ok((0usize, Vec::new()));
                }
                let mut seen = HashSet::new();
                seen.extend(s.active.keys().copied());
                seen.extend(s.pending_wakes.iter().copied());
                seen.extend(codemode::task_session_ids(s));
                let candidates: Vec<SessionId> = seen
                    .into_iter()
                    .filter(|&sid| s.presence.active_local_lease_kind(sid).is_none())
                    .collect();
                Ok((peers, candidates))
            })
            .await
        else {
            return;
        };
        if known_peers == 0 || candidates.is_empty() {
            return;
        }

        for sid in candidates {
            for retry in 0..8 {
                if self.stopping.is_cancelled() {
                    return;
                }
                let lock = self.start_lock(sid);
                let guard = lock.lock().await;
                let presence = self.presence.clone();
                let still_unleased = self
                    .db
                    .call(move |s| {
                        let peers = refresh_known_peer_count(&s.conn, &presence)?;
                        if peers == 0 {
                            return Ok(false);
                        }
                        let occupied = s.active.contains_key(&sid)
                            || s.pending_wakes.contains(&sid)
                            || codemode::has_session_tasks(s, sid);
                        Ok(occupied && s.presence.active_local_lease_kind(sid).is_none())
                    })
                    .await
                    .unwrap_or(false);
                if !still_unleased {
                    break;
                }

                match self.acquire_quorum_lease(sid, false, false).await {
                    Ok(mut gate) => {
                        let Some(lease) = gate.lease.take() else {
                            break;
                        };
                        self.presence.set_active_local_lease(sid, lease.clone());
                        let lease_for_db = lease.clone();
                        let still_occupied = self
                            .db
                            .call(move |s| {
                                let occupied = s.active.contains_key(&sid)
                                    || s.pending_wakes.contains(&sid)
                                    || codemode::has_session_tasks(s, sid);
                                if !occupied {
                                    return Ok(false);
                                }
                                if let Some(ctrl) = s.active.get_mut(&sid) {
                                    ctrl.lease = Some(lease_for_db);
                                }
                                s.force_refresh_local_presence(sid);
                                Ok(true)
                            })
                            .await
                            .unwrap_or(false);
                        if still_occupied {
                            if let Some(cg) = gate.claim_guard.take() {
                                cg.release_local_only();
                            }
                            self.spawn_lease_renew_loop(app.clone(), sid, lease);
                            if !gate.granted_peers.is_empty() {
                                self.broadcast_presence_to(&gate.granted_peers, None).await;
                            }
                        } else {
                            self.presence.clear_active_local_lease(sid, &lease.claim_id);
                            if let Some(cg) = gate.claim_guard.take() {
                                cg.release_now().await;
                            }
                        }
                        break;
                    }
                    Err(err)
                        if err.0 == axum::http::StatusCode::SERVICE_UNAVAILABLE
                            && retry < 7
                            && !self.stopping.is_cancelled() =>
                    {
                        drop(guard);
                        tokio::select! {
                            biased;
                            _ = self.stopping.cancelled() => return,
                            _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                        }
                    }
                    Err(_) => {
                        app.fence_session_local(
                            sid,
                            None,
                            "cluster quorum unavailable after joining; session paused",
                        )
                        .await;
                        break;
                    }
                }
            }
        }
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
        let mut backoff: HashMap<String, (Instant, Duration)> = HashMap::new();
        let mut last_seen_seq = 0u64;
        loop {
            if self.stopping.is_cancelled() {
                return;
            }
            let (instance_id, seq, sessions) = self.presence.snapshot_local();
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
                let req = ExecReq::PresenceSync {
                    instance_id,
                    seq,
                    sessions,
                    want_reply: false,
                    clear_claim: None,
                };
                let results = futures_util::future::join_all(due.into_iter().map(|peer_id| {
                    let req_ref = &req;
                    async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_SHORT_TIMEOUT).await) }
                }))
                .await;
                let retry_base = Instant::now();
                for (peer_id, res) in results {
                    if res.is_ok() {
                        self.mark_presence_acked(&peer_id, seq);
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

    async fn broadcast_presence_to(
        &self,
        peers: &[String],
        clear_claim: Option<(SessionId, String)>,
    ) {
        let (instance_id, seq, sessions) = self.presence.snapshot_local();
        let mut targets = Vec::new();
        for peer_id in peers {
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
        let results = futures_util::future::join_all(targets.into_iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await) }
        }))
        .await;
        for (peer_id, res) in results {
            if res.is_ok() {
                self.mark_presence_acked(&peer_id, seq);
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
                        self.mark_presence_acked(&peer_id, seq);
                        self.apply_remote_presence(&peer_id, r_inst, r_seq, r_sessions, None);
                    }
                }
            }))
            .await;
        }
    }

    async fn deferred_recovery_loop(&self) {
        loop {
            if self.stopping.is_cancelled() {
                return;
            }
            if !self.presence.is_deferred_recovery_pending() {
                tokio::select! {
                    biased;
                    _ = self.stopping.cancelled() => return,
                    _ = self.presence.recovery_notify.notified() => (),
                }
                continue;
            }
            let Ok(entries) = self
                .db
                .call(|s| codemode::ledger::list_deferred_crash_sessions(&s.conn))
                .await
            else {
                tokio::select! {
                    biased;
                    _ = self.stopping.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => (),
                }
                continue;
            };
            if entries.is_empty() {
                self.presence.set_deferred_recovery_pending(false);
                continue;
            }

            let mut all_done = true;
            for entry in entries {
                if self.stopping.is_cancelled() {
                    return;
                }
                let sid = entry.session_id;
                let lock = self.start_lock(sid);
                let _lock_guard = lock.lock().await;
                match self.acquire_quorum_lease(sid, false, false).await {
                    Ok(mut gate) => {
                        let exp = gate.expected_head.clone();
                        let res = self
                            .db
                            .call(move |s| {
                                let Some(expected_head) = exp.as_ref() else {
                                    s.conn.execute(
                                        "DELETE FROM deferred_crash_sessions WHERE session_id=?",
                                        [sid],
                                    )?;
                                    return Ok(false);
                                };
                                let committed = codemode::ledger::apply_deferred_crash_recovery(
                                    &s.conn,
                                    &entry,
                                    expected_head,
                                )?;
                                if committed
                                    && let Ok(info) = session(&s.conn, sid)
                                {
                                    s.notify_status(
                                        sid,
                                        info.status.as_str(),
                                        &info.last_error,
                                        info.updated_at,
                                    );
                                }
                                Ok(true)
                            })
                            .await;
                        if let Some(cg) = gate.claim_guard.take() {
                            cg.release_now().await;
                        }
                        if res.is_err() {
                            all_done = false;
                        }
                    }
                    Err(e) if e.0 == axum::http::StatusCode::NOT_FOUND => {
                        let _ = self
                            .db
                            .call(move |s| {
                                s.conn.execute(
                                    "DELETE FROM deferred_crash_sessions WHERE session_id=?",
                                    [sid],
                                )?;
                                Ok(())
                            })
                            .await;
                    }
                    Err(_) => {
                        all_done = false;
                    }
                }
            }

            if all_done
                && self
                    .db
                    .call(|s| Ok(codemode::ledger::list_deferred_crash_sessions(&s.conn)?.is_empty()))
                    .await
                    .unwrap_or(false)
            {
                self.presence.set_deferred_recovery_pending(false);
            } else {
                tokio::select! {
                    biased;
                    _ = self.stopping.cancelled() => return,
                    _ = self.presence.recovery_notify.notified() => (),
                    _ = tokio::time::sleep(Duration::from_secs(2)) => (),
                }
            }
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
        if let Some((holder, by_claim)) = self.presence.active_claim_holder_excluding(sid, &local_id) {
            if by_claim {
                return Err(Error::conflict(format!(
                    "session run is starting on node {holder}"
                )));
            }
            if holder != local_id {
                return Ok(Some(holder));
            }
        }
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
            async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await) }
        }))
        .await;
        let mut best: Option<String> = None;
        let mut starting_holder: Option<String> = None;
        let mut unoccupied_acks = 0usize;
        let mut online_err: Option<Error> = None;
        for (peer_id, res) in replies {
            match res {
                Ok(ExecResp::Occupancy {
                    occupied: true,
                    runner_node_id,
                    status,
                    occupied_by_claim,
                    ..
                }) => {
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
                Ok(ExecResp::Occupancy {
                    occupied: false, ..
                }) => {
                    unoccupied_acks += 1;
                }
                Ok(_) => {
                    online_err.get_or_insert_with(|| {
                        Error::internal("unexpected CheckOccupancy response")
                    });
                }
                Err(err) => {
                    if self.net.is_peer_online(&peer_id) {
                        online_err.get_or_insert(err);
                    }
                }
            }
        }
        if let Some(owner) = best {
            return Ok(Some(owner));
        }
        if let Some(holder) = starting_holder {
            return Err(Error::conflict(format!(
                "session run is starting on node {holder}"
            )));
        }
        let total = 1usize.saturating_add(self.presence.known_peer_count());
        let quorum = total / 2 + 1;
        if 1 + unoccupied_acks >= quorum {
            return Ok(None);
        }
        if let Some(err) = online_err {
            return Err(err);
        }
        Ok(None)
    }

    pub async fn start_run(
        &self,
        app: &App,
        sid: SessionId,
        node: Option<String>,
        force_offline: bool,
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
                ResolvedNode::Local { .. } => self.start_run_local(app, sid, force_offline).await,
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
                            &ExecReq::StartRun {
                                session_id: sid,
                                force_offline,
                            },
                            EXEC_OP_TIMEOUT,
                        )
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
            self.start_run_local(app, sid, force_offline).await
        }
    }

    pub async fn start_run_local(
        &self,
        app: &App,
        sid: SessionId,
        force_offline: bool,
    ) -> Result<Session> {
        // Single-node fast path: zero start_lock, net mutex, or redundant DB pre-checks.
        if self.presence.known_peer_count() == 0 {
            return app.start_local_checked(sid, None, None).await;
        }

        let lock = self.start_lock(sid);
        let _guard = lock.lock().await;

        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return app.start_local_checked(sid, None, None).await;
        }

        // Fast idempotent path if this node ALREADY owns an active worker with a valid lease.
        let fast_local = self
            .db
            .call(move |s| {
                if s.active.contains_key(&sid) && s.presence.is_local_lease_valid(sid) {
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

        // Idle lease holder with active background tasks already holds a valid LocalLease.
        match self.presence.active_local_lease_kind(sid) {
            Some(LocalLease::Quorum(existing)) => {
                if !existing.is_valid() {
                    return Err(Error::conflict("execution lease lost"));
                }
                let (expected_head, _, _) = self
                    .validate_local_gate_preconditions(sid, true, Some(existing.term))
                    .await?;
                self.sync.ensure_session_attachments(sid).await?;
                let started = app
                    .start_local_checked(sid, expected_head, Some(existing))
                    .await?;
                let peers = self.net.online_peer_ids();
                if !peers.is_empty() {
                    self.broadcast_presence_to(&peers, None).await;
                }
                return Ok(started);
            }
            Some(LocalLease::ForcedOffline) if force_offline => {
                let (expected_head, _, _) = self
                    .validate_local_gate_preconditions(sid, true, None)
                    .await?;
                self.sync.ensure_session_attachments(sid).await?;
                let started = app.start_local_checked(sid, expected_head, None).await?;
                let peers = self.net.online_peer_ids();
                if !peers.is_empty() {
                    self.broadcast_presence_to(&peers, None).await;
                }
                return Ok(started);
            }
            Some(LocalLease::ForcedOffline) | None => {}
        }

        let gate = self.acquire_quorum_lease(sid, force_offline, true).await?;
        let forced_offline_bypass = gate.lease.is_none() && self.presence.known_peer_count() > 0;
        if let Some(ref lease) = gate.lease {
            self.presence.set_active_local_lease(sid, lease.clone());
        } else if forced_offline_bypass {
            self.presence.set_active_local_forced_offline(sid);
        }
        let started = app
            .start_local_checked(sid, gate.expected_head.clone(), gate.lease.clone())
            .await;
        self.finish_leased_start(app, sid, gate, started).await
    }

    pub async fn start_wake_run(&self, app: &App, sid: SessionId) -> Result<Session> {
        if self.presence.known_peer_count() == 0 {
            return app.start_wake_checked(sid, None, None).await;
        }

        let lock = self.start_lock(sid);
        let _guard = lock.lock().await;

        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return app.start_wake_checked(sid, None, None).await;
        }
        match self.presence.active_local_lease_kind(sid) {
            Some(LocalLease::Quorum(existing)) => {
                if existing.is_valid() {
                    return app.start_wake_checked(sid, None, Some(existing)).await;
                }
                return Err(Error::conflict("execution lease lost"));
            }
            Some(LocalLease::ForcedOffline) => {
                return app.start_wake_checked(sid, None, None).await;
            }
            None => {}
        }

        let gate = self.acquire_quorum_lease(sid, false, true).await?;
        if let Some(ref lease) = gate.lease {
            self.presence.set_active_local_lease(sid, lease.clone());
        }
        let started = app
            .start_wake_checked(sid, gate.expected_head.clone(), gate.lease.clone())
            .await;
        self.finish_leased_start(app, sid, gate, started).await
    }

    async fn finish_leased_start(
        &self,
        app: &App,
        sid: SessionId,
        mut gate: AcquiredLeaseGate,
        started: Result<Session>,
    ) -> Result<Session> {
        match started {
            Err(e) => {
                if let Some(ref lease) = gate.lease {
                    self.presence.clear_active_local_lease(sid, &lease.claim_id);
                } else if self.presence.known_peer_count() > 0
                    && self.presence.local_owner(sid).is_none()
                    && matches!(
                        self.presence.active_local_lease_kind(sid),
                        Some(LocalLease::ForcedOffline)
                    )
                {
                    self.presence.set_local(sid, None);
                }
                if let Some(cg) = gate.claim_guard.take() {
                    cg.release_now().await;
                }
                Err(e)
            }
            Ok(session) => {
                let lease_still_active = gate.lease.as_ref().is_some_and(|l| {
                    self.presence
                        .active_local_lease(sid)
                        .is_some_and(|cur| cur.claim_id == l.claim_id)
                }) || (gate.lease.is_none() && self.presence.local_owner(sid).is_some());

                if lease_still_active {
                    if let Some(cg) = gate.claim_guard.take() {
                        cg.release_local_only();
                    }
                    if let Some(lease) = gate.lease.take() {
                        self.spawn_lease_renew_loop(app.clone(), sid, lease);
                    }
                    if !gate.granted_peers.is_empty() {
                        self.broadcast_presence_to(&gate.granted_peers, None).await;
                    }
                } else {
                    if let Some(ref lease) = gate.lease {
                        self.presence.clear_active_local_lease(sid, &lease.claim_id);
                    } else if self.presence.known_peer_count() > 0
                        && self.presence.local_owner(sid).is_none()
                        && matches!(
                            self.presence.active_local_lease_kind(sid),
                            Some(LocalLease::ForcedOffline)
                        )
                    {
                        self.presence.set_local(sid, None);
                    }
                    if let Some(cg) = gate.claim_guard.take() {
                        cg.release_now().await;
                    }
                }
                Ok(session)
            }
        }
    }

    async fn validate_local_gate_preconditions(
        &self,
        sid: SessionId,
        require_model_caps: bool,
        persist_term: Option<u64>,
    ) -> Result<(Option<StreamHead>, String, usize)> {
        let presence = self.presence.clone();
        self.db
            .call(move |s| {
                let known_peers = refresh_known_peer_count(&s.conn, &presence)?;
                let current = session(&s.conn, sid)?;
                if require_model_caps {
                    crate::catalog::available_capabilities(&s.conn, current.model_id)?;
                    if current.status == SessionStatus::Pausing {
                        return Err(Error::conflict("session is pausing"));
                    }
                }
                assert_no_conflict(&s.conn, sid.as_str())?;
                if !is_session_writable_local(&s.conn, sid)? {
                    return Err(Error::conflict(
                        "session history is incomplete or read-only locally; sync before running",
                    ));
                }
                if let Some(term) = persist_term {
                    persist_lease_term(&s.conn, sid, term)?;
                }
                let head = stream_head_wire(&s.conn, sid.as_str())?;
                Ok((head, s.local_node_id.clone(), known_peers))
            })
            .await
    }

    async fn acquire_quorum_lease(
        &self,
        sid: SessionId,
        force_offline: bool,
        require_model_caps: bool,
    ) -> Result<AcquiredLeaseGate> {
        for attempt in 0..4 {
            let known_peers = self
                .db
                .call({
                    let presence = self.presence.clone();
                    move |s| refresh_known_peer_count(&s.conn, &presence)
                })
                .await?;
            let total = 1usize.saturating_add(known_peers);
            let quorum = total / 2 + 1;
            let online = self.net.online_peer_ids();

            if 1 + online.len() < quorum {
                if !force_offline {
                    return Err(Error(
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        format!(
                            "cluster quorum unavailable ({}/{} nodes online, need {}); reconnect peers or pass force_offline=true",
                            1 + online.len(),
                            total,
                            quorum
                        ),
                    ));
                }
                if !online.is_empty() {
                    if let Some(owner) = self.probe_online_occupancy(sid).await? {
                        return Err(Error::conflict(format!(
                            "session is currently occupied on node {owner}"
                        )));
                    }
                    self.sync.sync_session_latest(sid).await?;
                }
                let (expected_head, _, _) = self
                    .validate_local_gate_preconditions(sid, require_model_caps, None)
                    .await?;
                if require_model_caps {
                    self.sync.ensure_session_attachments(sid).await?;
                }
                return Ok(AcquiredLeaseGate {
                    expected_head,
                    lease: None,
                    granted_peers: Vec::new(),
                    claim_guard: None,
                });
            }

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
                        EXEC_LEASE_TIMEOUT,
                    )
                    .await
                {
                    Ok(ExecResp::Occupancy {
                        occupied: true,
                        runner_node_id,
                        occupied_by_claim,
                        ..
                    }) if !occupied_by_claim => {
                        let holder = runner_node_id.unwrap_or(owner_id);
                        return Err(Error::conflict(format!(
                            "session is currently occupied on node {holder}"
                        )));
                    }
                    Ok(ExecResp::Occupancy { .. }) => {}
                    Ok(_) => return Err(Error::internal("unexpected CheckOccupancy response")),
                    Err(_) => {}
                }
            }

            let (claim_id, term) = match self.presence.try_acquire_local_claim(sid) {
                Ok(pair) => pair,
                Err((_msg, true)) if attempt < 3 => {
                    sleep_jitter(true).await;
                    continue;
                }
                Err((msg, _)) => return Err(Error::conflict(msg)),
            };
            let mut claim_guard = ClaimReleaseGuard::new(
                self.presence.clone(),
                self.self_weak.get().and_then(Weak::upgrade),
                sid,
                claim_id.clone(),
            );

            if let Err(err) = self.sync.sync_session_latest(sid).await {
                claim_guard.release_now().await;
                return Err(err);
            }
            let (expected_head, local_id, known_peers_now) = match self
                .validate_local_gate_preconditions(sid, require_model_caps, Some(term))
                .await
            {
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

            let total_now = 1usize.saturating_add(known_peers_now);
            let quorum_now = total_now / 2 + 1;
            let online = self.net.online_peer_ids();
            if 1 + online.len() < quorum_now {
                claim_guard.release_now().await;
                if force_offline {
                    return Ok(AcquiredLeaseGate {
                        expected_head,
                        lease: None,
                        granted_peers: Vec::new(),
                        claim_guard: None,
                    });
                }
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "cluster quorum unavailable ({}/{} nodes online, need {}); reconnect peers or pass force_offline=true",
                        1 + online.len(),
                        total_now,
                        quorum_now
                    ),
                ));
            }
            if total_now == 1 {
                claim_guard.release_local_only();
                return Ok(AcquiredLeaseGate {
                    expected_head,
                    lease: None,
                    granted_peers: Vec::new(),
                    claim_guard: None,
                });
            }

            // Track every queried peer BEFORE sending CheckOccupancy so aborted/partial rounds always release synchronously.
            claim_guard.track_peers(&online);
            let grant_start = Instant::now();
            let claim = RunClaim {
                claim_id: claim_id.clone(),
                node_id: local_id.clone(),
                started_ms: chrono::Utc::now().timestamp_millis(),
                term,
                ttl_ms: u64::try_from(LEASE_TTL.as_millis()).unwrap_or(10_000),
            };
            let req = ExecReq::CheckOccupancy {
                session_id: sid,
                claim: Some(claim),
            };
            let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
                let req_ref = &req;
                async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await) }
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

            let mut granted_peers = Vec::new();
            let mut max_seen_term = term;
            let mut need_resync = false;
            let mut retry_by_claim: Option<String> = None;
            let mut retry_stale_term = false;
            let mut hard_occupied: Option<String> = None;
            let mut hard_conflict: Option<String> = None;
            let mut online_err: Option<Error> = None;

            for (peer_id, reply) in replies {
                match reply {
                    Ok(ExecResp::Occupancy {
                        occupied,
                        runner_node_id,
                        head: remote_head,
                        has_conflict: remote_conflict,
                        lease_granted,
                        occupied_by_claim,
                        highest_term,
                        ..
                    }) => {
                        max_seen_term = max_seen_term.max(highest_term);
                        if remote_conflict {
                            hard_conflict.get_or_insert(peer_id);
                        } else if occupied {
                            let holder = runner_node_id.unwrap_or_else(|| peer_id.clone());
                            if occupied_by_claim || idle_confirmers.contains(&holder) {
                                retry_by_claim.get_or_insert(holder);
                            } else {
                                hard_occupied.get_or_insert(holder);
                            }
                        } else if lease_granted {
                            granted_peers.push(peer_id);
                            if is_remote_head_newer_or_diverged(
                                expected_head.as_ref(),
                                remote_head.as_ref(),
                            ) {
                                need_resync = true;
                            }
                        } else {
                            retry_stale_term = true;
                        }
                    }
                    Ok(_) => {
                        online_err.get_or_insert_with(|| {
                            Error::internal("unexpected CheckOccupancy response")
                        });
                    }
                    Err(err) => {
                        if self.net.is_peer_online(&peer_id) {
                            online_err.get_or_insert(err);
                        }
                    }
                }
            }

            if max_seen_term > term {
                self.presence.observe_remote_term(sid, max_seen_term);
            }
            if let Some(peer_id) = hard_conflict {
                claim_guard.release_now().await;
                return Err(Error::conflict(format!(
                    "session has an unresolved sync conflict on node {peer_id}"
                )));
            }
            if let Some(holder) = hard_occupied {
                claim_guard.release_now().await;
                return Err(Error::conflict(format!(
                    "session is currently occupied on node {holder}"
                )));
            }
            if need_resync || retry_by_claim.is_some() || retry_stale_term {
                claim_guard.release_now().await;
                if attempt < 3 {
                    let prefer_early = retry_by_claim
                        .as_ref()
                        .is_some_and(|holder| local_id < *holder);
                    sleep_jitter(prefer_early).await;
                    continue;
                }
                return Err(Error::conflict(if need_resync {
                    "cluster session head advanced or diverged while starting run; retry or resolve conflict"
                } else if retry_by_claim.is_some() {
                    "concurrent cluster start detected; retry"
                } else {
                    "cluster execution term advanced; retry"
                }));
            }
            if 1 + granted_peers.len() < quorum_now {
                claim_guard.release_now().await;
                if force_offline {
                    return Ok(AcquiredLeaseGate {
                        expected_head,
                        lease: None,
                        granted_peers: Vec::new(),
                        claim_guard: None,
                    });
                }
                return Err(online_err.unwrap_or_else(|| {
                    Error(
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        "cluster quorum not reached for execution lease".into(),
                    )
                }));
            }

            match self.presence.promote_local_claim_to_committing(sid, &claim_id) {
                Ok(()) => {}
                Err((msg, retryable)) => {
                    claim_guard.release_now().await;
                    if retryable && attempt < 3 {
                        sleep_jitter(false).await;
                        continue;
                    }
                    return Err(Error::conflict(msg));
                }
            }
            let lease_ttl = LEASE_TTL.saturating_sub(LEASE_SAFETY_MARGIN);
            let lease = ActiveLease::new(term, claim_id, grant_start + lease_ttl);
            return Ok(AcquiredLeaseGate {
                expected_head,
                lease: Some(lease),
                granted_peers,
                claim_guard: Some(claim_guard),
            });
        }

        Err(Error::conflict("failed to acquire cluster execution lease"))
    }

    fn spawn_lease_renew_loop(&self, app: App, sid: SessionId, lease: Arc<ActiveLease>) {
        let Some(plane) = self.self_weak.get().and_then(Weak::upgrade) else {
            return;
        };
        let mut fast_retry = false;
        self.workers.spawn(async move {
            loop {
                let now = Instant::now();
                let deadline = lease.deadline();
                if now >= deadline {
                    lease.revoked.cancel();
                    app.fence_session_local(sid, Some(lease.term), "execution lease lost")
                        .await;
                    return;
                }
                let rem = deadline.saturating_duration_since(now);
                let wait = if fast_retry {
                    Duration::from_secs(1).min(rem)
                } else {
                    LEASE_RENEW_INTERVAL.min(rem)
                };
                tokio::select! {
                    biased;
                    _ = plane.stopping.cancelled() => return,
                    _ = lease.revoked.cancelled() => return,
                    _ = lease.stopped.cancelled() => {
                        let online = plane.net.online_peer_ids();
                        if !online.is_empty() {
                            let req = ExecReq::ReleaseClaim {
                                session_id: sid,
                                claim_id: lease.claim_id.clone(),
                            };
                            futures_util::future::join_all(online.into_iter().map(|peer_id| {
                                let req_ref = &req;
                                let plane = plane.clone();
                                async move {
                                    let _ = plane.exec_rpc(&peer_id, req_ref, Duration::from_secs(2)).await;
                                }
                            }))
                            .await;
                        }
                        return;
                    }
                    _ = tokio::time::sleep(wait) => {}
                }
                if lease.revoked.is_cancelled()
                    || lease.stopped.is_cancelled()
                    || plane.stopping.is_cancelled()
                {
                    continue;
                }
                let round_start = Instant::now();
                let total = 1usize.saturating_add(plane.presence.known_peer_count());
                let quorum = total / 2 + 1;
                let online = plane.net.online_peer_ids();
                let ttl_ms = u64::try_from(LEASE_TTL.as_millis()).unwrap_or(10_000);
                let req = ExecReq::RenewLease {
                    session_id: sid,
                    claim_id: lease.claim_id.clone(),
                    term: lease.term,
                    ttl_ms,
                };
                let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
                    let req_ref = &req;
                    let plane = plane.clone();
                    async move { plane.exec_rpc(&peer_id, req_ref, Duration::from_secs(2)).await }
                }))
                .await;

                let mut acks = 1usize;
                let mut max_term = lease.term;
                for r in replies {
                    if let Ok(ExecResp::LeaseRenewed {
                        granted,
                        highest_term,
                    }) = r
                    {
                        max_term = max_term.max(highest_term);
                        if granted {
                            acks += 1;
                        }
                    }
                }
                if max_term > lease.term {
                    lease.revoked.cancel();
                    plane.presence.observe_remote_term(sid, max_term);
                    app.fence_session_local(sid, Some(lease.term), "execution lease lost")
                        .await;
                    return;
                }
                if acks >= quorum {
                    lease.extend(round_start + LEASE_TTL.saturating_sub(LEASE_SAFETY_MARGIN));
                    fast_retry = false;
                } else if Instant::now() >= lease.deadline() {
                    lease.revoked.cancel();
                    app.fence_session_local(sid, Some(lease.term), "execution lease lost")
                        .await;
                    return;
                } else {
                    fast_retry = true;
                }
            }
        });
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
        if let Some((peer_id, _)) = self.presence.remote_owner(sid)
            && self.net.is_peer_online(&peer_id)
        {
            return Ok(WriterTarget::Remote(peer_id));
        }
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return Ok(WriterTarget::Local);
        }
        if !self.net.online_peer_ids().is_empty() {
            if let Some(peer_id) = self.probe_online_occupancy(sid).await? {
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

    pub async fn acquire_delete_gate(&self, sid: SessionId) -> Result<DeleteGateGuard> {
        if self.presence.known_peer_count() == 0 {
            return Ok(DeleteGateGuard {
                expected_head: None,
                lease: None,
                guard: None,
                _lock: None,
            });
        }
        let lock_guard = self.start_lock(sid).lock_owned().await;
        if self.presence.local_owner(sid).is_some()
            || self.presence.remote_owner(sid).is_some()
            || self.presence.has_committed_remote_lease(sid)
        {
            return Err(Error::conflict(
                "pause the session and stop active tasks before deleting",
            ));
        }
        if self.presence.known_peer_count() == 0 && self.net.online_peer_ids().is_empty() {
            return Ok(DeleteGateGuard {
                expected_head: None,
                lease: None,
                guard: None,
                _lock: Some(lock_guard),
            });
        }
        for attempt in 0..2 {
            self.sync.sync_session_latest(sid).await?;
            let (expected_head, local_id, known_peers) = self
                .db
                .call({
                    let presence = self.presence.clone();
                    move |s| {
                        let peers = refresh_known_peer_count(&s.conn, &presence)?;
                        assert_no_conflict(&s.conn, sid.as_str())?;
                        Ok((
                            stream_head_wire(&s.conn, sid.as_str())?,
                            s.local_node_id.clone(),
                            peers,
                        ))
                    }
                })
                .await?;
            let total = 1usize.saturating_add(known_peers);
            let quorum = total / 2 + 1;
            let online = self.net.online_peer_ids();
            if 1 + online.len() < quorum {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "cluster quorum unavailable ({}/{} nodes online, need {}); cannot safely delete session while partitioned",
                        1 + online.len(),
                        total,
                        quorum
                    ),
                ));
            }
            if total == 1 {
                return Ok(DeleteGateGuard {
                    expected_head,
                    lease: None,
                    guard: None,
                    _lock: Some(lock_guard),
                });
            }
            let (claim_id, term) = self
                .presence
                .try_acquire_local_claim(sid)
                .map_err(|(msg, _)| Error::conflict(msg))?;
            let mut claim_guard = ClaimReleaseGuard::new(
                self.presence.clone(),
                self.self_weak.get().and_then(Weak::upgrade),
                sid,
                claim_id.clone(),
            );
            claim_guard.track_peers(&online);
            let grant_start = Instant::now();
            let req = ExecReq::CheckOccupancy {
                session_id: sid,
                claim: Some(RunClaim {
                    claim_id: claim_id.clone(),
                    node_id: local_id,
                    started_ms: chrono::Utc::now().timestamp_millis(),
                    term,
                    ttl_ms: u64::try_from(LEASE_TTL.as_millis()).unwrap_or(10_000),
                }),
            };
            let replies = futures_util::future::join_all(online.into_iter().map(|peer_id| {
                let req_ref = &req;
                async move { (peer_id.clone(), self.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await) }
            }))
            .await;

            let mut granted = 0usize;
            let mut need_retry = false;
            let mut max_term = term;
            let mut online_err = None;
            for (peer_id, res) in replies {
                match res {
                    Ok(ExecResp::Occupancy {
                        occupied,
                        has_conflict: remote_conflict,
                        head: remote_head,
                        lease_granted,
                        highest_term,
                        ..
                    }) => {
                        max_term = max_term.max(highest_term);
                        if occupied {
                            claim_guard.release_now().await;
                            return Err(Error::conflict(
                                "pause the session and stop active tasks before deleting",
                            ));
                        }
                        if remote_conflict {
                            claim_guard.release_now().await;
                            return Err(Error::conflict(format!(
                                "session has an unresolved sync conflict on node {peer_id}"
                            )));
                        }
                        if lease_granted {
                            granted += 1;
                            if is_remote_head_newer_or_diverged(
                                expected_head.as_ref(),
                                remote_head.as_ref(),
                            ) {
                                need_retry = true;
                            }
                        } else {
                            need_retry = true;
                        }
                    }
                    Ok(_) => {
                        online_err.get_or_insert_with(|| {
                            Error::internal("unexpected CheckOccupancy response")
                        });
                    }
                    Err(err) => {
                        if self.net.is_peer_online(&peer_id) {
                            online_err.get_or_insert(err);
                        }
                    }
                }
            }
            if max_term > term {
                self.presence.observe_remote_term(sid, max_term);
            }
            if need_retry {
                claim_guard.release_now().await;
                if attempt == 0 {
                    sleep_jitter(false).await;
                    continue;
                }
                return Err(Error::conflict(
                    "session history changed on cluster before delete; retry",
                ));
            }
            if 1 + granted < quorum {
                claim_guard.release_now().await;
                return Err(online_err.unwrap_or_else(|| {
                    Error(
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        "cluster quorum not reached for session delete".into(),
                    )
                }));
            }
            if let Err((msg, _)) = self
                .presence
                .promote_local_claim_to_committing(sid, &claim_id)
            {
                claim_guard.release_now().await;
                return Err(Error::conflict(msg));
            }
            let lease_ttl = LEASE_TTL.saturating_sub(LEASE_SAFETY_MARGIN);
            let lease = ActiveLease::new(term, claim_id, grant_start + lease_ttl);
            return Ok(DeleteGateGuard {
                expected_head,
                lease: Some(lease),
                guard: Some(claim_guard),
                _lock: Some(lock_guard),
            });
        }
        Err(Error::conflict("failed to acquire cluster delete gate"))
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

    fn track_peers(&mut self, peers: &[String]) {
        for peer_id in peers {
            if !self.peers.contains(peer_id) {
                self.peers.push(peer_id.clone());
            }
        }
    }

    fn release_local_only(mut self) {
        self.presence.release_local_claim(self.sid, &self.claim_id);
        self.armed = false;
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
        let req = ExecReq::ReleaseClaim {
            session_id: self.sid,
            claim_id: std::mem::take(&mut self.claim_id),
        };
        futures_util::future::join_all(peers.into_iter().map(|peer_id| {
            let req_ref = &req;
            let plane = plane.clone();
            async move {
                let _ = plane.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await;
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
                    let _ = plane.exec_rpc(&peer_id, req_ref, EXEC_LEASE_TIMEOUT).await;
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
                    clear_claim.clone(),
                );
                if let Some((csid, cid)) = clear_claim {
                    let holder = from_peer.clone();
                    let _ = self
                        .app
                        .db
                        .call(move |s| clear_persisted_lease(&s.conn, csid, &holder, &cid))
                        .await;
                }
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
                let holder = from_peer.clone();
                let _ = self
                    .app
                    .db
                    .call(move |s| clear_persisted_lease(&s.conn, session_id, &holder, &claim_id))
                    .await;
                Self::send_reply(&mut send, &ExecResp::Ack).await;
            }
            ExecReq::RenewLease {
                session_id,
                claim_id,
                term,
                ttl_ms,
            } => {
                let presence = self.app.exec.presence().clone();
                let holder = from_peer.clone();
                let resp = match self
                    .app
                    .db
                    .call(move |s| {
                        let local_occupied = s.active.contains_key(&session_id)
                            || s.pending_wakes.contains(&session_id)
                            || codemode::count_session_tasks(s, session_id) > 0
                            || presence.local_owner(session_id).is_some();
                        let remote_occupied =
                            presence.remote_owner_excluding(session_id, &holder).is_some();
                        match presence.renew_remote_lease(
                            session_id,
                            &holder,
                            &claim_id,
                            term,
                            ttl_ms,
                            local_occupied,
                            remote_occupied,
                        ) {
                            Ok(expires_at_ms) => {
                                persist_lease_grant(
                                    &s.conn,
                                    session_id,
                                    term,
                                    &holder,
                                    &claim_id,
                                    expires_at_ms,
                                )?;
                                Ok(ExecResp::LeaseRenewed {
                                    granted: true,
                                    highest_term: term,
                                })
                            }
                            Err(highest_term) => Ok(ExecResp::LeaseRenewed {
                                granted: false,
                                highest_term,
                            }),
                        }
                    })
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => Self::to_wire_err(e),
                };
                Self::send_reply(&mut send, &resp).await;
            }
            ExecReq::StartRun {
                session_id,
                force_offline,
            } => {
                let resp = match self
                    .app
                    .exec
                    .start_run_local(&self.app, session_id, force_offline)
                    .await
                {
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

    async fn handle_check_occupancy(
        &self,
        from_peer: &str,
        sid: SessionId,
        claim: Option<RunClaim>,
    ) -> ExecResp {
        if let Some(ref c) = claim
            && c.node_id != from_peer
        {
            return ExecResp::Error {
                status: 400,
                message: "claim node_id mismatch".into(),
            };
        }
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
                let remote_occ = presence.remote_owner_excluding(sid, &from_peer_owned);
                let head = stream_head_wire(&s.conn, sid.as_str())?;
                let conflicted = has_conflict(&s.conn, sid.as_str())?;
                let local_status = if let Some(occ) = &local_occ {
                    Some(occ.status.clone())
                } else if local_occupied {
                    session(&s.conn, sid).ok().map(|row| row.status)
                } else {
                    remote_occ.as_ref().map(|(_, o)| o.status.clone())
                };

                if let Some(claim) = claim {
                    if local_occupied {
                        return Ok(ExecResp::Occupancy {
                            occupied: true,
                            runner_node_id: Some(s.local_node_id.clone()),
                            status: local_status,
                            active_tasks,
                            head,
                            has_conflict: conflicted,
                            lease_granted: false,
                            occupied_by_claim: false,
                            highest_term: presence
                                .active_local_lease(sid)
                                .map_or(0, |l| l.term),
                        });
                    }
                    if let Some((owner_id, occ)) = remote_occ {
                        return Ok(ExecResp::Occupancy {
                            occupied: true,
                            runner_node_id: Some(owner_id),
                            status: Some(occ.status),
                            active_tasks: 0,
                            head,
                            has_conflict: conflicted,
                            lease_granted: false,
                            occupied_by_claim: false,
                            highest_term: 0,
                        });
                    }
                    match presence.accept_remote_claim(sid, &claim, false) {
                        GrantOutcome::Granted {
                            highest_term,
                            expires_at_ms,
                        } => {
                            persist_lease_grant(
                                &s.conn,
                                sid,
                                highest_term,
                                &claim.node_id,
                                &claim.claim_id,
                                expires_at_ms,
                            )?;
                            Ok(ExecResp::Occupancy {
                                occupied: false,
                                runner_node_id: None,
                                status: None,
                                active_tasks: 0,
                                head,
                                has_conflict: conflicted,
                                lease_granted: true,
                                occupied_by_claim: false,
                                highest_term,
                            })
                        }
                        GrantOutcome::Occupied {
                            holder,
                            by_claim,
                            highest_term,
                        } => Ok(ExecResp::Occupancy {
                            occupied: true,
                            runner_node_id: Some(holder),
                            status: None,
                            active_tasks: 0,
                            head,
                            has_conflict: conflicted,
                            lease_granted: false,
                            occupied_by_claim: by_claim,
                            highest_term,
                        }),
                        GrantOutcome::StaleTerm { highest_term } => Ok(ExecResp::Occupancy {
                            occupied: false,
                            runner_node_id: None,
                            status: None,
                            active_tasks: 0,
                            head,
                            has_conflict: conflicted,
                            lease_granted: false,
                            occupied_by_claim: false,
                            highest_term,
                        }),
                    }
                } else {
                    let claim_holder = if !local_occupied && remote_occ.is_none() {
                        presence.active_claim_holder_excluding(sid, &from_peer_owned)
                    } else {
                        None
                    };
                    let occupied_by_claim = claim_holder.as_ref().is_some_and(|(_, by)| *by);
                    let occupied =
                        local_occupied || remote_occ.is_some() || claim_holder.is_some();
                    let runner_node_id = if local_occupied {
                        Some(s.local_node_id.clone())
                    } else if let Some((h, _)) = claim_holder {
                        Some(h)
                    } else {
                        remote_occ.map(|(id, _)| id)
                    };
                    Ok(ExecResp::Occupancy {
                        occupied,
                        runner_node_id,
                        status: local_status,
                        active_tasks: if local_occupied { active_tasks } else { 0 },
                        head,
                        has_conflict: conflicted,
                        lease_granted: false,
                        occupied_by_claim,
                        highest_term: 0,
                    })
                }
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
