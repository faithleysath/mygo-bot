use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use iroh::endpoint::Connection;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    cluster::{
        ClusterSignal,
        identity::{
            MAX_PEX_PEERS, PeerInfo, assert_membership_gen, list_peers, load_local_node,
            self_peer_info, upsert_peer_if_gen,
        },
        ledger::{
            self, AncestorProof, ApplyOutcome, COMMIT_CHUNK_BYTES, CommitMeta, CommitWire,
            ConflictRecord, MAX_COMMIT_PAYLOAD_BYTES, ResolutionReceipt, StreamHead, StreamKind,
            apply_counterparty_convergence, apply_remote_commits_page, apply_resolution_receipt,
            apply_terminal_mcp_delete, apply_terminal_session_delete,
            apply_unwatched_session_head, clear_staging_for_stream, commit_hash_at,
            commit_staged_epoch, get_conflict, get_conflict_opt, list_conflict_probes_page,
            local_manifest_page, mark_session_watched, pin_session_target_head,
            read_commit_payload_chunk, read_commits_page, read_delete_commit,
            read_resolutions_and_converged, read_resolutions_for_streams, record_fork_conflict,
            resolve_conflict_local, session_history_rev, stage_remote_commits_page, stream_head,
            stream_head_wire, verify_local_prefix,
        },
        net::{
            ClusterNet, RPC_BODY_MAX, RPC_READ_DEADLINE, RPC_REQ_HEADER_MAX, RPC_RESP_HEADER_MAX,
            RPC_WRITE_DEADLINE, read_frame_bounded, write_frame_timed,
        },
    },
    events::Hub,
    id::{AttachmentId, McpServerId, SessionId},
    store::{Db, Store, session},
};

// Keep fallback alias local if net defines RPC_CONTROL_HEADER_MAX; otherwise use RPC_REQ_HEADER_MAX.
const _: usize = RPC_REQ_HEADER_MAX;

const ATT_CHUNK_SIZE: usize = 256 * 1024;
const MAX_CHUNK_CONCURRENCY: usize = 4;
const MAX_ATT_BYTE_PERMITS: usize = 512; // 128 MiB global concurrent attachment budget (256 KiB units)
const MANIFEST_PAGE_LIMIT: usize = 256;
const COMMIT_BATCH_COUNT: usize = 64;
const COMMIT_BATCH_BYTES: usize = 2 * 1024 * 1024;
const MAX_INBOUND_QUEUE: usize = 4096;
const INBOUND_WORKERS: usize = 4;
const MAX_BLOCKING_CODEC: usize = 4;
const OFFLOAD_BYTES_THRESHOLD: usize = 64 * 1024;
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(30);
const RPC_TIMEOUT_SHORT: Duration = Duration::from_secs(10);
const RPC_TIMEOUT_MANIFEST: Duration = Duration::from_secs(4);
const RPC_TIMEOUT_DATA: Duration = Duration::from_secs(20);
const RPC_TIMEOUT_OAUTH: Duration = Duration::from_secs(45);

fn default_manifest_limit() -> usize {
    MANIFEST_PAGE_LIMIT
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcReq {
    ExchangePeers {
        self_info: PeerInfo,
        peers: Vec<PeerInfo>,
    },
    GetManifest {
        #[serde(default)]
        after_stream_id: Option<String>,
        #[serde(default = "default_manifest_limit")]
        limit: usize,
    },
    GetHeads {
        stream_ids: Vec<String>,
    },
    NotifyHeads {
        heads: Vec<StreamHead>,
    },
    GetCommitHash {
        stream_id: String,
        epoch: i64,
        seq: i64,
    },
    GetDeleteCommit {
        stream_id: String,
        epoch: i64,
        seq: i64,
    },
    GetCommits {
        stream_id: String,
        epoch: i64,
        after_seq: i64,
        #[serde(default)]
        up_to_seq: Option<i64>,
        max_count: usize,
        max_bytes: usize,
    },
    GetCommitChunk {
        stream_id: String,
        epoch: i64,
        seq: i64,
        commit_hash: [u8; 32],
        offset: u64,
        len: u32,
    },
    HasAttachments {
        hashes: Vec<[u8; 32]>,
    },
    GetAttachmentChunk {
        content_hash: [u8; 32],
        offset: u64,
        len: u32,
    },
    OAuthRefresh {
        server_id: McpServerId,
        generation: String,
        #[serde(default)]
        expected_epoch: i64,
        #[serde(default)]
        expected_seq: i64,
        #[serde(default)]
        expected_hash: Option<[u8; 32]>,
        #[serde(default)]
        rejected_token_hash: Option<[u8; 32]>,
    },
    GetResolutions {
        stream_ids: Vec<String>,
    },
    NotifyConflictResolved(ResolutionReceipt),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthRefreshStatus {
    Refreshed,
    UpToDate,
    Conflicted,
    ReauthRequired,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcResp {
    Peers {
        self_info: PeerInfo,
        peers: Vec<PeerInfo>,
    },
    Manifest {
        heads: Vec<StreamHead>,
        #[serde(default)]
        next_cursor: Option<String>,
    },
    Ack,
    CommitHash {
        hash: Option<[u8; 32]>,
    },
    DeleteCommit {
        commit: Option<CommitWire>,
    },
    Commits {
        commits: Vec<CommitWire>,
        #[serde(default)]
        oversized_next: Option<CommitMeta>,
        has_more: bool,
    },
    CommitChunk,
    AttachmentsAvailable {
        hashes: Vec<[u8; 32]>,
    },
    AttachmentChunk,
    OAuthRefreshResult {
        status: OAuthRefreshStatus,
        head: Option<StreamHead>,
        commits: Vec<CommitWire>,
        #[serde(default)]
        has_more: bool,
        error: Option<String>,
    },
    Resolutions {
        receipts: Vec<ResolutionReceipt>,
        #[serde(default)]
        converged_heads: Vec<StreamHead>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize)]
pub struct SyncSummary {
    pub peers_queried: usize,
    pub streams_updated: usize,
    pub conflicts: usize,
}

pub enum RefreshCoordination {
    Local,
    Delegated { reauth_required: bool },
    PendingConfirmation,
}

#[derive(Default)]
struct LockTable<K> {
    map: Mutex<HashMap<K, Weak<AsyncMutex<()>>>>,
}

impl<K: Eq + std::hash::Hash + Clone> LockTable<K> {
    fn lock_for(&self, key: K) -> Arc<AsyncMutex<()>> {
        let mut map = self.map.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(lock) = map.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        map.insert(key, Arc::downgrade(&lock));
        lock
    }
}

type InboundItem = (i64, String, StreamHead);

#[derive(Default)]
struct InboundState {
    by_stream: HashMap<String, Vec<InboundItem>>,
    fifo: VecDeque<String>,
    full_sync_peers: HashSet<(i64, String)>,
    total_entries: usize,
}

struct RetryEntry {
    attempts: u32,
    due: Instant,
}

pub struct SyncEngine {
    db: Db,
    client: reqwest::Client,
    streams: Hub,
    net: Arc<ClusterNet>,
    signal: Arc<ClusterSignal>,
    presence: Arc<crate::cluster::exec::PresenceState>,
    stream_locks: LockTable<String>,
    att_locks: LockTable<[u8; 32]>,
    att_byte_budget: Arc<Semaphore>,
    blocking_sem: Arc<Semaphore>,
    inbound: Mutex<InboundState>,
    inbound_notify: Notify,
    outbound_retries: Mutex<HashMap<String, RetryEntry>>,
    sync_now_gate: AsyncMutex<()>,
}

impl SyncEngine {
    pub fn new(
        db: Db,
        client: reqwest::Client,
        streams: Hub,
        net: Arc<ClusterNet>,
        signal: Arc<ClusterSignal>,
        presence: Arc<crate::cluster::exec::PresenceState>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            client,
            streams,
            net,
            signal,
            presence,
            stream_locks: LockTable::default(),
            att_locks: LockTable::default(),
            att_byte_budget: Arc::new(Semaphore::new(MAX_ATT_BYTE_PERMITS)),
            blocking_sem: Arc::new(Semaphore::new(MAX_BLOCKING_CODEC)),
            inbound: Mutex::default(),
            inbound_notify: Notify::new(),
            outbound_retries: Mutex::default(),
            sync_now_gate: AsyncMutex::new(()),
        })
    }

    pub fn on_membership_switched(&self) {
        let mut st = self.inbound.lock().unwrap();
        st.by_stream.clear();
        st.fifo.clear();
        st.full_sync_peers.clear();
        st.total_entries = 0;
        drop(st);
        self.outbound_retries.lock().unwrap().clear();
    }

    async fn run_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let _permit = self
            .blocking_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::internal("blocking worker pool closed"))?;
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| Error::internal(format!("blocking task panicked: {e}")))
    }

    async fn encode_resp_owned(&self, resp: RpcResp) -> Result<Vec<u8>> {
        let needs_offload = match &resp {
            RpcResp::Commits { commits, .. } => {
                commits.len() > 8
                    || commits
                        .iter()
                        .map(|c| c.payload.len() + 192)
                        .sum::<usize>()
                        > OFFLOAD_BYTES_THRESHOLD
            }
            RpcResp::Manifest { heads, .. } => {
                heads.len() > 32
                    || heads
                        .iter()
                        .map(|h| h.head_snapshot.as_ref().map_or(0, String::len) + 192)
                        .sum::<usize>()
                        > OFFLOAD_BYTES_THRESHOLD
            }
            RpcResp::OAuthRefreshResult { commits, head, .. } => {
                commits.len() > 8
                    || commits
                        .iter()
                        .map(|c| c.payload.len() + 192)
                        .sum::<usize>()
                        + head
                            .as_ref()
                            .and_then(|h| h.head_snapshot.as_ref())
                            .map_or(0, String::len)
                        > OFFLOAD_BYTES_THRESHOLD
            }
            RpcResp::DeleteCommit { commit } => commit
                .as_ref()
                .is_some_and(|c| c.payload.len() > OFFLOAD_BYTES_THRESHOLD),
            RpcResp::Resolutions {
                receipts,
                converged_heads,
            } => receipts.len() > 16 || converged_heads.len() > 16,
            _ => false,
        };
        if needs_offload {
            self.run_blocking(move || {
                serde_json::to_vec(&resp).map_err(|e| Error::internal(e.to_string()))
            })
            .await?
        } else {
            serde_json::to_vec(&resp).map_err(|e| Error::internal(e.to_string()))
        }
    }

    async fn decode_json<T: DeserializeOwned + Send + 'static>(&self, bytes: Vec<u8>) -> Result<T> {
        if bytes.len() > 256 * 1024 {
            self.run_blocking(move || {
                serde_json::from_slice(&bytes)
                    .map_err(|e| Error::bad(format!("invalid RPC JSON: {e}")))
            })
            .await?
        } else {
            serde_json::from_slice(&bytes).map_err(|e| Error::bad(format!("invalid RPC JSON: {e}")))
        }
    }

    pub fn spawn_loops(self: &Arc<Self>, stopping: CancellationToken, workers: &TaskTracker) {
        // Outbound signal & backoff retry & periodic anti-entropy loop
        let out_eng = self.clone();
        let out_stop = stopping.clone();
        workers.spawn(async move {
            let mut next_anti_entropy = Instant::now() + ANTI_ENTROPY_INTERVAL;
            loop {
                let next_retry_due = out_eng.next_retry_deadline();
                let sleep_until = next_anti_entropy.min(next_retry_due.unwrap_or(next_anti_entropy));
                tokio::select! {
                    _ = out_stop.cancelled() => break,
                    _ = out_eng.signal.notified() => {
                        let (mut streams, self_meta, full_sync) = out_eng.signal.take();
                        if self_meta {
                            out_eng.broadcast_peers().await;
                        }
                        if full_sync {
                            let _ = out_eng.sync_with_peers(None, None).await;
                        } else if !streams.is_empty() {
                            streams.sort();
                            streams.dedup();
                            out_eng.broadcast_heads(&streams, None).await;
                        }
                    }
                    _ = out_eng.net.wait_peer_connected() => {
                        let peers = out_eng.net.take_connected_peers();
                        for peer_id in peers {
                            let eng = out_eng.clone();
                            let stop = out_stop.clone();
                            tokio::select! {
                                _ = stop.cancelled() => break,
                                _ = eng.on_peer_connected(&peer_id) => {}
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(sleep_until.into()) => {
                        let now = Instant::now();
                        let due = out_eng.pop_due_retries(now);
                        if !due.is_empty() {
                            out_eng.broadcast_heads(&due, None).await;
                        }
                        if now >= next_anti_entropy {
                            next_anti_entropy = now + ANTI_ENTROPY_INTERVAL;
                            if !out_eng.net.online_peer_ids().is_empty() {
                                let _ = out_eng.sync_with_peers(None, None).await;
                            }
                        }
                    }
                }
            }
        });

        // Bounded inbound worker pool (replaces unbounded spawn + global reconcile_gate!)
        for _ in 0..INBOUND_WORKERS {
            let eng = self.clone();
            let stop = stopping.clone();
            workers.spawn(async move {
                loop {
                    if stop.is_cancelled() {
                        break;
                    }
                    if let Some((mgen, peer_id)) = eng.pop_full_sync_peer() {
                        if eng.net.membership_gen() == mgen {
                            let _ = eng.sync_with_peers(Some(&peer_id), None).await;
                        }
                        continue;
                    }
                    if let Some((stream_id, candidates)) = eng.pop_inbound_stream() {
                        eng.process_inbound_stream(&stream_id, candidates).await;
                        continue;
                    }
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = eng.inbound_notify.notified() => (),
                    }
                }
            });
        }
    }

    fn next_retry_deadline(&self) -> Option<Instant> {
        self.outbound_retries
            .lock()
            .unwrap()
            .values()
            .map(|r| r.due)
            .min()
    }

    fn pop_due_retries(&self, now: Instant) -> Vec<String> {
        let mut map = self.outbound_retries.lock().unwrap();
        let due_ids: Vec<String> = map
            .iter()
            .filter_map(|(k, v)| (v.due <= now).then(|| k.clone()))
            .collect();
        for id in &due_ids {
            map.remove(id);
        }
        due_ids
    }

    fn schedule_retry_streams(&self, ids: &[String]) {
        let mut map = self.outbound_retries.lock().unwrap();
        let now = Instant::now();
        for id in ids {
            if map.len() >= MAX_INBOUND_QUEUE && !map.contains_key(id) {
                continue;
            }
            let entry = map.entry(id.clone()).or_insert(RetryEntry {
                attempts: 0,
                due: now,
            });
            entry.attempts = entry.attempts.saturating_add(1).min(5);
            let secs = match entry.attempts {
                1 => 1,
                2 => 2,
                3 => 4,
                _ => 15,
            };
            entry.due = now + Duration::from_secs(secs);
        }
    }

    fn enqueue_inbound_heads(&self, conn_gen: i64, from_peer: String, heads: Vec<StreamHead>) {
        if self.net.membership_gen() != conn_gen {
            return;
        }
        let mut st = self.inbound.lock().unwrap();
        if self.net.membership_gen() != conn_gen {
            return;
        }
        if st.total_entries >= MAX_INBOUND_QUEUE {
            st.full_sync_peers.insert((conn_gen, from_peer));
            drop(st);
            self.inbound_notify.notify_waiters();
            return;
        }
        for h in heads {
            if !h.stream_id.starts_with(h.kind.prefix()) {
                continue;
            }
            let sid = h.stream_id.clone();
            let is_new = !st.by_stream.contains_key(&sid);
            let exists_from_peer = st
                .by_stream
                .get_mut(&sid)
                .and_then(|vec| vec.iter_mut().find(|(g, p, _)| *g == conn_gen && p == &from_peer));
            if let Some((_, _, prev)) = exists_from_peer {
                if (h.epoch, h.head_seq, h.body_complete)
                    >= (prev.epoch, prev.head_seq, prev.body_complete)
                {
                    *prev = h;
                }
                continue;
            }
            if st.total_entries >= MAX_INBOUND_QUEUE {
                st.full_sync_peers.insert((conn_gen, from_peer));
                break;
            }
            st.by_stream
                .entry(sid.clone())
                .or_default()
                .push((conn_gen, from_peer.clone(), h));
            st.total_entries += 1;
            if is_new {
                st.fifo.push_back(sid);
            }
        }
        drop(st);
        self.inbound_notify.notify_waiters();
    }

    fn pop_full_sync_peer(&self) -> Option<(i64, String)> {
        let mut st = self.inbound.lock().unwrap();
        let next = st.full_sync_peers.iter().next().cloned();
        if let Some(ref key) = next {
            st.full_sync_peers.remove(key);
        }
        next
    }

    fn pop_inbound_stream(&self) -> Option<(String, Vec<InboundItem>)> {
        let mut st = self.inbound.lock().unwrap();
        while let Some(sid) = st.fifo.pop_front() {
            if let Some(cands) = st.by_stream.remove(&sid) {
                st.total_entries = st.total_entries.saturating_sub(cands.len());
                return Some((sid, cands));
            }
        }
        None
    }

    async fn process_inbound_stream(&self, _stream_id: &str, items: Vec<InboundItem>) {
        let cur_mgen = self.net.membership_gen();
        let filtered: Vec<(String, StreamHead)> = items
            .into_iter()
            .filter_map(|(g, p, h)| (g == cur_mgen).then_some((p, h)))
            .collect();
        let Some((from_peer, _)) = filtered.last().cloned() else {
            return;
        };
        let mut peer_heads: HashMap<String, Vec<StreamHead>> = HashMap::new();
        for (p, h) in filtered {
            peer_heads.entry(p).or_default().push(h);
        }
        let manifest_list: Vec<(String, Vec<StreamHead>)> = peer_heads.into_iter().collect();
        let Ok((updated_ids, still_blocked)) = self
            .reconcile_multi_manifests_with_ids(manifest_list, None, cur_mgen)
            .await
        else {
            if self.net.membership_gen() == cur_mgen {
                self.inbound
                    .lock()
                    .unwrap()
                    .full_sync_peers
                    .insert((cur_mgen, from_peer));
                self.inbound_notify.notify_waiters();
            }
            return;
        };
        if self.net.membership_gen() != cur_mgen {
            return;
        }
        if still_blocked {
            let _ = self.sync_with_peers(Some(&from_peer), None).await;
        }
        if !updated_ids.is_empty() && self.net.membership_gen() == cur_mgen {
            self.broadcast_heads(&updated_ids, Some(&from_peer)).await;
        }
    }

    async fn rpc(&self, peer_id: &str, req: &RpcReq, timeout: Duration) -> Result<(RpcResp, Vec<u8>)> {
        let Some(conn) = self.net.get_connection(peer_id) else {
            return Err(Error::conflict(format!("peer {peer_id} is offline")));
        };
        self.rpc_on_conn(&conn, req, timeout).await
    }

    async fn rpc_on_conn(
        &self,
        conn: &Connection,
        req: &RpcReq,
        timeout: Duration,
    ) -> Result<(RpcResp, Vec<u8>)> {
        let req_bytes = serde_json::to_vec(req).map_err(|e| Error::internal(e.to_string()))?;
        if req_bytes.len() > RPC_REQ_HEADER_MAX {
            return Err(Error::bad("RPC request header exceeds bound"));
        }
        let byte_budget = self.net.rpc_byte_budget().clone();
        let (hdr, body, _permits) = tokio::time::timeout(timeout, async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::internal(format!("open RPC stream failed: {e}")))?;
            write_frame_timed(&mut send, &req_bytes, None, RPC_WRITE_DEADLINE).await?;
            read_frame_bounded(
                &mut recv,
                RPC_RESP_HEADER_MAX,
                RPC_BODY_MAX,
                &byte_budget,
                timeout,
            )
            .await
        })
        .await
        .map_err(|_| Error::internal("cluster RPC timed out"))??;

        let resp: RpcResp = self.decode_json(hdr).await?;
        if let RpcResp::Error { message } = resp {
            return Err(Error::conflict(message));
        }
        Ok((resp, body))
    }

    pub async fn handle_rpc(
        self: Arc<Self>,
        from_peer: String,
        mut send: iroh::endpoint::SendStream,
        mut recv: iroh::endpoint::RecvStream,
        conn_gen: i64,
        gen_cancel: CancellationToken,
        net: Arc<ClusterNet>,
    ) {
        let Some(_global_permit) = tokio::time::timeout(
            Duration::from_secs(2),
            net.global_rpc_sem().clone().acquire_owned(),
        )
        .await
        .ok()
        .and_then(|r| r.ok())
        else {
            return;
        };
        let peer_sem = net.peer_rpc_sem(&from_peer);
        let Some(_peer_permit) = tokio::time::timeout(Duration::from_secs(2), peer_sem.acquire_owned())
            .await
            .ok()
            .and_then(|r| r.ok())
        else {
            return;
        };

        let res = tokio::select! {
            _ = gen_cancel.cancelled() => return,
            r = async {
                if net.membership_gen() != conn_gen {
                    return Err(Error::conflict("membership generation superseded"));
                }
                let (hdr, _body, permits) = read_frame_bounded(
                    &mut recv,
                    RPC_REQ_HEADER_MAX,
                    1024,
                    net.rpc_byte_budget(),
                    RPC_READ_DEADLINE,
                )
                .await?;
                let req: RpcReq = self.decode_json(hdr).await?;
                drop(permits);
                self.dispatch_rpc(&from_peer, req, conn_gen).await
            } => r,
        };

        if gen_cancel.is_cancelled() || net.membership_gen() != conn_gen {
            return;
        }
        match res {
            Ok((resp, bin)) => {
                if let Ok(hdr) = self.encode_resp_owned(resp).await {
                    let _ = write_frame_timed(&mut send, &hdr, bin.as_deref(), RPC_WRITE_DEADLINE).await;
                }
            }
            Err(err) => {
                tracing::warn!(peer = %from_peer, status = %err.0, error = %err.1, "cluster RPC failed");
                if let Ok(hdr) = serde_json::to_vec(&RpcResp::Error { message: err.1 }) {
                    let _ = write_frame_timed(&mut send, &hdr, None, RPC_WRITE_DEADLINE).await;
                }
            }
        }
    }

    async fn dispatch_rpc(
        self: &Arc<Self>,
        from_peer: &str,
        req: RpcReq,
        conn_gen: i64,
    ) -> Result<(RpcResp, Option<Vec<u8>>)> {
        if self.net.membership_gen() != conn_gen {
            return Err(Error::conflict("membership generation changed"));
        }
        match req {
            RpcReq::ExchangePeers { self_info, mut peers } => {
                if peers.len() > MAX_PEX_PEERS {
                    peers.truncate(MAX_PEX_PEERS);
                }
                let addr = self.net.endpoint_addr();
                let (my_info, my_peers, discovered) = self
                    .db
                    .call(move |s| {
                        let now = chrono::Utc::now().timestamp_millis();
                        let local = load_local_node(&s.conn)?;
                        if local.membership_gen != conn_gen {
                            return Err(Error::conflict("membership changed during PEX"));
                        }
                        // ExchangePeers sender `self_info` is observed over direct authenticated conn;
                        // forwarded `peers` array entries are indirect PEX hearsay (`direct = false`).
                        let mut changed = upsert_peer_if_gen(
                            &s.conn,
                            &local.node_id,
                            &self_info,
                            now,
                            true,
                            conn_gen,
                        )?;
                        for p in peers {
                            changed |= upsert_peer_if_gen(
                                &s.conn,
                                &local.node_id,
                                &p,
                                now,
                                false,
                                conn_gen,
                            )?;
                        }
                        let my_info = self_peer_info(&local, &addr);
                        let mut my_peers = list_peers(&s.conn)?;
                        s.presence.set_known_peer_count(my_peers.len());
                        my_peers.truncate(MAX_PEX_PEERS);
                        Ok((my_info, my_peers, changed))
                    })
                    .await?;
                if discovered {
                    self.net.wake_dialer();
                }
                Ok((
                    RpcResp::Peers {
                        self_info: my_info,
                        peers: my_peers,
                    },
                    None,
                ))
            }
            RpcReq::GetManifest {
                after_stream_id,
                limit,
            } => {
                let (heads, next_cursor) = self
                    .db
                    .call(move |s| {
                        local_manifest_page(&s.conn, after_stream_id.as_deref(), limit)
                    })
                    .await?;
                Ok((RpcResp::Manifest { heads, next_cursor }, None))
            }
            RpcReq::GetHeads { stream_ids } => {
                let limited: Vec<String> = stream_ids.into_iter().take(256).collect();
                let heads = self
                    .db
                    .call(move |s| {
                        let mut out = Vec::new();
                        for id in limited {
                            if let Some(h) = stream_head_wire(&s.conn, &id)? {
                                out.push(h);
                            }
                        }
                        Ok(out)
                    })
                    .await?;
                Ok((
                    RpcResp::Manifest {
                        heads,
                        next_cursor: None,
                    },
                    None,
                ))
            }
            RpcReq::NotifyHeads { heads } => {
                self.enqueue_inbound_heads(conn_gen, from_peer.to_owned(), heads);
                Ok((RpcResp::Ack, None))
            }
            RpcReq::GetCommitHash {
                stream_id,
                epoch,
                seq,
            } => {
                let hash = self
                    .db
                    .call(move |s| commit_hash_at(&s.conn, &stream_id, epoch, seq))
                    .await?;
                Ok((RpcResp::CommitHash { hash }, None))
            }
            RpcReq::GetDeleteCommit {
                stream_id,
                epoch,
                seq,
            } => {
                let commit = self
                    .db
                    .call(move |s| read_delete_commit(&s.conn, &stream_id, epoch, seq))
                    .await?;
                Ok((RpcResp::DeleteCommit { commit }, None))
            }
            RpcReq::GetCommits {
                stream_id,
                epoch,
                after_seq,
                up_to_seq,
                max_count,
                max_bytes,
            } => {
                let count = max_count.clamp(1, 256);
                let bytes = max_bytes.clamp(64 * 1024, COMMIT_BATCH_BYTES);
                let page = self
                    .db
                    .call(move |s| {
                        read_commits_page(
                            &s.conn,
                            &stream_id,
                            epoch,
                            after_seq,
                            up_to_seq,
                            count,
                            bytes,
                        )
                    })
                    .await?;
                Ok((
                    RpcResp::Commits {
                        commits: page.commits,
                        oversized_next: page.oversized_next,
                        has_more: page.has_more,
                    },
                    None,
                ))
            }
            RpcReq::GetCommitChunk {
                stream_id,
                epoch,
                seq,
                commit_hash,
                offset,
                len,
            } => {
                let req_len = usize::try_from(len).unwrap_or(0);
                if req_len == 0 || req_len > COMMIT_CHUNK_BYTES {
                    return Err(Error::bad("invalid commit chunk length"));
                }
                let chunk = self
                    .db
                    .call(move |s| {
                        read_commit_payload_chunk(
                            &s.conn,
                            &stream_id,
                            epoch,
                            seq,
                            &commit_hash,
                            offset,
                            req_len,
                        )
                    })
                    .await?;
                let Some((bytes, _eof)) = chunk else {
                    return Err(Error::conflict("commit chunk unavailable or superseded"));
                };
                Ok((RpcResp::CommitChunk, Some(bytes)))
            }
            RpcReq::HasAttachments { hashes } => {
                let limited: Vec<[u8; 32]> = hashes.into_iter().take(64).collect();
                let available = self
                    .db
                    .call(move |s| {
                        let mut stmt = s.conn.prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM attachments WHERE content_hash=? AND data IS NOT NULL)",
                        )?;
                        let mut found = Vec::new();
                        for hash in limited {
                            if stmt.query_row([hash.as_slice()], |r| r.get::<_, bool>(0))? {
                                found.push(hash);
                            }
                        }
                        Ok(found)
                    })
                    .await?;
                Ok((RpcResp::AttachmentsAvailable { hashes: available }, None))
            }
            RpcReq::GetAttachmentChunk {
                content_hash,
                offset,
                len,
            } => {
                let req_len = usize::try_from(len).unwrap_or(0);
                if req_len == 0 || req_len > RPC_BODY_MAX {
                    return Err(Error::bad("invalid attachment chunk length"));
                }
                // P1-14: Zero-copy range read via SQLite incremental BLOB I/O!
                let chunk = self
                    .db
                    .call(move |s| {
                        let (rowid, byte_len): (i64, i64) = s
                            .conn
                            .prepare_cached(
                                "SELECT rowid, byte_len FROM attachments WHERE content_hash=? AND data IS NOT NULL LIMIT 1",
                            )?
                            .query_row([content_hash.as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))?;
                        let total = usize::try_from(byte_len).unwrap_or(0);
                        let start = usize::try_from(offset)
                            .map_err(|_| Error::bad("invalid chunk offset"))?;
                        let end = start.saturating_add(req_len);
                        if start >= total || end > total {
                            return Err(Error::bad("attachment chunk out of range"));
                        }
                        let blob = s.conn.blob_open(
                            rusqlite::MAIN_DB,
                            "attachments",
                            "data",
                            rowid,
                            true,
                        )?;
                        let mut buf = vec![0u8; req_len];
                        blob.read_at_exact(&mut buf, start)?;
                        Ok(buf)
                    })
                    .await?;
                Ok((RpcResp::AttachmentChunk, Some(chunk)))
            }
            RpcReq::OAuthRefresh {
                server_id,
                generation,
                expected_epoch,
                expected_seq,
                expected_hash,
                rejected_token_hash,
            } => {
                let res = crate::mcp::oauth::refresh_shared_as_coordinator(
                    &self.db,
                    &self.client,
                    server_id,
                    &generation,
                    expected_epoch,
                    expected_seq,
                    expected_hash,
                    rejected_token_hash,
                )
                .await;
                match res {
                    Ok(coord) => {
                        if let Some(h) = &coord.head {
                            self.signal.notify_streams(vec![h.stream_id.clone()]);
                        }
                        Ok((
                            RpcResp::OAuthRefreshResult {
                                status: coord.status,
                                head: coord.head,
                                commits: coord.commits,
                                has_more: coord.has_more,
                                error: coord.error,
                            },
                            None,
                        ))
                    }
                    Err(err) => Ok((
                        RpcResp::Error { message: err.1 },
                        None,
                    )),
                }
            }
            RpcReq::GetResolutions { stream_ids } => {
                let limited: Vec<String> = stream_ids.into_iter().take(256).collect();
                let (receipts, converged_heads) = self
                    .db
                    .call(move |s| read_resolutions_and_converged(&s.conn, &limited))
                    .await?;
                Ok((
                    RpcResp::Resolutions {
                        receipts,
                        converged_heads,
                    },
                    None,
                ))
            }
            RpcReq::NotifyConflictResolved(receipt) => {
                let sid = receipt.stream_id.clone();
                let cleared = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, conn_gen)?;
                        let cleared = apply_resolution_receipt(&s.conn, &receipt)?;
                        if cleared {
                            s.publish_global(
                                "sync.conflict",
                                json!({ "stream_id": sid, "resolved": true }),
                            );
                        }
                        Ok(cleared)
                    })
                    .await?;
                let _ = cleared;
                Ok((RpcResp::Ack, None))
            }
        }
    }

    async fn on_peer_connected(&self, peer_id: &str) -> Result<()> {
        if let Err(e) = self.exchange_peers_with(peer_id).await {
            tracing::warn!(%peer_id, error = %e, "peer exchange failed");
        }
        if let Err(e) = self.sync_with_peers(Some(peer_id), None).await {
            tracing::warn!(%peer_id, error = %e, "initial peer sync failed");
        }
        Ok(())
    }

    async fn exchange_peers_with(&self, peer_id: &str) -> Result<()> {
        let mgen = self.net.membership_gen();
        let addr = self.net.endpoint_addr();
        let (self_info, peers) = self
            .db
            .call(move |s| {
                let local = load_local_node(&s.conn)?;
                if local.membership_gen != mgen {
                    return Err(Error::conflict("membership changed"));
                }
                let mut my_peers = list_peers(&s.conn)?;
                my_peers.truncate(MAX_PEX_PEERS);
                Ok((self_peer_info(&local, &addr), my_peers))
            })
            .await?;
        let (resp, _) = self
            .rpc(
                peer_id,
                &RpcReq::ExchangePeers { self_info, peers },
                RPC_TIMEOUT_SHORT,
            )
            .await?;
        if self.net.membership_gen() != mgen {
            return Ok(());
        }
        if let RpcResp::Peers { self_info, mut peers } = resp {
            if peers.len() > MAX_PEX_PEERS {
                peers.truncate(MAX_PEX_PEERS);
            }
            let discovered = self
                .db
                .call(move |s| {
                    let now = chrono::Utc::now().timestamp_millis();
                    let local = load_local_node(&s.conn)?;
                    if local.membership_gen != mgen {
                        return Ok(false);
                    }
                    let mut changed =
                        upsert_peer_if_gen(&s.conn, &local.node_id, &self_info, now, true, mgen)?;
                    for p in peers {
                        changed |= upsert_peer_if_gen(
                            &s.conn,
                            &local.node_id,
                            &p,
                            now,
                            false,
                            mgen,
                        )?;
                    }
                    if changed {
                        crate::cluster::exec::refresh_known_peer_count(&s.conn, &s.presence)?;
                    }
                    Ok(changed)
                })
                .await?;
            if discovered {
                self.net.wake_dialer();
            }
        }
        Ok(())
    }

    pub async fn broadcast_peers(&self) {
        let peers = self.net.online_peer_ids();
        for peer_id in peers {
            let _ = self.exchange_peers_with(&peer_id).await;
        }
    }

    pub async fn broadcast_heads(&self, stream_ids: &[String], exclude_peer: Option<&str>) {
        if stream_ids.is_empty() {
            return;
        }
        let ids = stream_ids.to_vec();
        let Ok(heads) = self
            .db
            .call(move |s| {
                let mut out = Vec::new();
                for id in ids {
                    if let Some(h) = stream_head_wire(&s.conn, &id)? {
                        out.push(h);
                    }
                }
                Ok(out)
            })
            .await
        else {
            return;
        };
        if heads.is_empty() {
            return;
        }
        let online = self.net.online_peer_ids();
        let targets: Vec<String> = online
            .into_iter()
            .filter(|peer_id| exclude_peer != Some(peer_id.as_str()))
            .collect();
        if targets.is_empty() {
            return;
        }

        let mut failed_any = false;
        for chunk in heads.chunks(128) {
            let req = RpcReq::NotifyHeads {
                heads: chunk.to_vec(),
            };
            let req_ref = &req;
            let results = futures_util::future::join_all(
                targets
                    .iter()
                    .map(|peer_id| async move { self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await }),
            )
            .await;
            if results.iter().any(Result::is_err) {
                failed_any = true;
            }
        }
        if failed_any {
            let retry_ids: Vec<String> = heads.into_iter().map(|h| h.stream_id).collect();
            self.schedule_retry_streams(&retry_ids);
        }
    }

    async fn fetch_full_manifest_from_peer(&self, peer_id: &str, mgen: i64) -> Result<Vec<StreamHead>> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..4096 {
            if self.net.membership_gen() != mgen {
                return Err(Error::conflict("membership generation superseded"));
            }
            let (resp, _) = self
                .rpc(
                    peer_id,
                    &RpcReq::GetManifest {
                        after_stream_id: cursor.clone(),
                        limit: MANIFEST_PAGE_LIMIT,
                    },
                    RPC_TIMEOUT_MANIFEST,
                )
                .await?;
            if self.net.membership_gen() != mgen {
                return Err(Error::conflict("membership generation superseded"));
            }
            match resp {
                RpcResp::Manifest { heads, next_cursor } => {
                    let exhausted = next_cursor.is_none() || heads.is_empty();
                    all.extend(heads);
                    if exhausted {
                        break;
                    }
                    cursor = next_cursor;
                }
                _ => break,
            }
        }
        Ok(all)
    }

    pub async fn sync_now(&self, session_id: Option<SessionId>) -> Result<SyncSummary> {
        let _guard = self.sync_now_gate.lock().await;
        if let Some(sid) = session_id {
            let online_empty = self.net.online_peer_ids().is_empty();
            // Check session existence cleanly if specified when offline/unknown (returns 404).
            self.db.call(move |s| {
                let exists: bool = s.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=? UNION ALL SELECT 1 FROM sync_streams WHERE stream_id=?)",
                    [sid.as_str(), sid.as_str()],
                    |r| r.get(0),
                )?;
                if !exists && online_empty {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                Ok(())
            }).await?;
        }
        let mut summary = self.sync_with_peers(None, session_id).await?;
        if let Some(sid) = session_id {
            self.ensure_session_watched(sid).await?;
        }
        let conflicts = self
            .db
            .call(|s| {
                let count = s.conn.query_row("SELECT count(*) FROM sync_conflicts", [], |r| {
                    r.get::<_, i64>(0)
                })?;
                Ok(usize::try_from(count).unwrap_or(0))
            })
            .await?;
        summary.conflicts = conflicts;
        Ok(summary)
    }

    async fn sync_with_peers(
        &self,
        only_peer: Option<&str>,
        watch_sid: Option<SessionId>,
    ) -> Result<SyncSummary> {
        let peer_ids = match only_peer {
            Some(p) if self.net.is_peer_online(p) => vec![p.to_owned()],
            Some(_) => {
                return Ok(SyncSummary {
                    peers_queried: 0,
                    streams_updated: 0,
                    conflicts: 0,
                });
            }
            None => self.net.online_peer_ids(),
        };
        if peer_ids.is_empty() {
            return Ok(SyncSummary {
                peers_queried: 0,
                streams_updated: 0,
                conflicts: 0,
            });
        }

        let mgen = self.net.membership_gen();
        self.reconcile_open_conflicts_with_peers(&peer_ids, mgen).await?;

        let manifest_results = futures_util::future::join_all(peer_ids.iter().map(|peer_id| async move {
            (
                peer_id.clone(),
                self.fetch_full_manifest_from_peer(peer_id, mgen).await,
            )
        }))
        .await;
        if self.net.membership_gen() != mgen {
            return Ok(SyncSummary {
                peers_queried: 0,
                streams_updated: 0,
                conflicts: 0,
            });
        }
        let mut peer_manifests: Vec<(String, Vec<StreamHead>)> = Vec::new();
        for (peer_id, res) in manifest_results {
            if let Ok(heads) = res {
                peer_manifests.push((peer_id, heads));
            }
        }
        if self.net.membership_gen() != mgen {
            return Ok(SyncSummary {
                peers_queried: 0,
                streams_updated: 0,
                conflicts: 0,
            });
        }
        let peers_queried = peer_manifests.len();
        let (updated_ids, _) = self
            .reconcile_multi_manifests_with_ids(peer_manifests, watch_sid, mgen)
            .await?;
        if self.net.membership_gen() == mgen {
            // P1-12: propagate updates discovered via manifest sync to other online peers!
            if !updated_ids.is_empty() && peer_ids.len() < self.net.online_peer_ids().len().max(1) {
                self.signal.notify_streams(updated_ids.clone());
            } else if !updated_ids.is_empty() && only_peer.is_some() {
                self.broadcast_heads(&updated_ids, only_peer).await;
            }
        }
        let conflicts = self
            .db
            .call(|s| {
                let count = s.conn.query_row("SELECT count(*) FROM sync_conflicts", [], |r| {
                    r.get::<_, i64>(0)
                })?;
                Ok(usize::try_from(count).unwrap_or(0))
            })
            .await?;
        if peers_queried > 0 && self.presence.is_deferred_recovery_pending() {
            self.presence.notify_recovery();
        }
        Ok(SyncSummary {
            peers_queried,
            streams_updated: updated_ids.len(),
            conflicts,
        })
    }

    async fn reconcile_open_conflicts_with_peers(&self, peer_ids: &[String], mgen: i64) -> Result<()> {
        let mut after: Option<String> = None;
        for _page in 0..4096 {
            if self.net.membership_gen() != mgen {
                break;
            }
            let probes = self
                .db
                .call({
                    let after_cur = after.clone();
                    move |s| list_conflict_probes_page(&s.conn, after_cur.as_deref(), 256)
                })
                .await?;
            if probes.is_empty() {
                break;
            }
            let page_len = probes.len();
            after = probes.last().map(|(id, _)| id.clone());
            let req = RpcReq::GetResolutions {
                stream_ids: probes.iter().map(|(id, _)| id.clone()).collect(),
            };
            for peer_id in peer_ids {
                if self.net.membership_gen() != mgen {
                    break;
                }
                if let Ok((
                    RpcResp::Resolutions {
                        receipts,
                        converged_heads,
                    },
                    _,
                )) = self.rpc(peer_id, &req, RPC_TIMEOUT_SHORT).await
                {
                    if !receipts.is_empty() {
                        self.db
                            .call(move |s| {
                                assert_membership_gen(&s.conn, mgen)?;
                                for receipt in receipts {
                                    let sid = receipt.stream_id.clone();
                                    if apply_resolution_receipt(&s.conn, &receipt)? {
                                        s.publish_global(
                                            "sync.conflict",
                                            json!({ "stream_id": sid, "resolved": true }),
                                        );
                                    }
                                }
                                Ok(())
                            })
                            .await?;
                    }
                    for head in converged_heads {
                        if self.net.membership_gen() != mgen {
                            break;
                        }
                        let Ok(index) = probes.binary_search_by(|(id, _)| id.cmp(&head.stream_id)) else {
                            continue;
                        };
                        let expected_remote_hash = probes[index].1;
                        let conflict = self
                            .db
                            .call({
                                let sid = head.stream_id.clone();
                                move |s| get_conflict_opt(&s.conn, &sid)
                            })
                            .await?;
                        let Some(c) = conflict else {
                            continue;
                        };
                        if c.remote_node_id != *peer_id
                            || c.remote_hash != expected_remote_hash
                            || head.epoch != c.local_epoch
                        {
                            continue;
                        }
                        let proof = if head.head_seq > c.local_seq {
                            match self
                                .query_commit_hash_from_holders(
                                    std::slice::from_ref(peer_id),
                                    &head.stream_id,
                                    c.local_epoch,
                                    c.local_seq,
                                )
                                .await
                            {
                                Some(h) if h == c.local_hash => AncestorProof::Verified {
                                    epoch: c.local_epoch,
                                    seq: c.local_seq,
                                    hash: c.local_hash,
                                },
                                Some(_) => AncestorProof::Diverged {
                                    epoch: c.local_epoch,
                                    seq: c.local_seq,
                                    hash: c.local_hash,
                                },
                                None => AncestorProof::None,
                            }
                        } else {
                            AncestorProof::None
                        };
                        let peer = peer_id.clone();
                        self.db
                            .call(move |s| {
                                assert_membership_gen(&s.conn, mgen)?;
                                let sid = head.stream_id.clone();
                                if apply_counterparty_convergence(
                                    &s.conn, &peer, &head, expected_remote_hash, proof,
                                )? {
                                    s.publish_global(
                                        "sync.conflict",
                                        json!({ "stream_id": sid, "resolved": true }),
                                    );
                                }
                                Ok(())
                            })
                            .await?;
                    }
                }
            }
            if page_len < 256 {
                break;
            }
        }
        Ok(())
    }

    async fn reconcile_multi_manifests_with_ids(
        &self,
        peer_manifests: Vec<(String, Vec<StreamHead>)>,
        watch_sid: Option<SessionId>,
        mgen: i64,
    ) -> Result<(Vec<String>, bool)> {
        if self.net.membership_gen() != mgen {
            return Ok((Vec::new(), false));
        }

        #[derive(Clone)]
        struct Candidate {
            head: StreamHead,
            holders: Vec<String>,
            full_holders: Vec<String>,
        }
        let mut by_stream: HashMap<String, Vec<Candidate>> = HashMap::new();
        for (peer_id, heads) in peer_manifests {
            for head in heads {
                let list = by_stream.entry(head.stream_id.clone()).or_default();
                if let Some(existing) = list.iter_mut().find(|c| {
                    c.head.epoch == head.epoch
                        && c.head.head_seq == head.head_seq
                        && c.head.head_hash == head.head_hash
                }) {
                    if !existing.holders.contains(&peer_id) {
                        existing.holders.push(peer_id.clone());
                    }
                    if head.body_complete && !existing.full_holders.contains(&peer_id) {
                        existing.full_holders.push(peer_id.clone());
                    }
                    if head.body_complete {
                        existing.head.body_complete = true;
                    }
                } else {
                    let mut full_holders = Vec::new();
                    if head.body_complete {
                        full_holders.push(peer_id.clone());
                    }
                    list.push(Candidate {
                        head,
                        holders: vec![peer_id.clone()],
                        full_holders,
                    });
                }
            }
        }

        // Strict 8-phase dependency order:
        // 0: Provider
        // 1: Model
        // 2: Mcp (upserts & retired deletes)
        // 3: SessionHeads (non-deleted lightweight directory heads & conflict checks)
        // 4: Calls attached to existing/active sessions
        // 5: Terminal Session Deletes (frees FK items -> unbinds calls -> deletes session)
        // 6: Remaining / Detached Calls
        // 7: Watched Session Bodies
        let mut updated_streams = Vec::new();
        let mut resolved_or_done: HashSet<String> = HashSet::new();
        let mut unblocked_to_repull: Vec<String> = Vec::new();
        let mut still_blocked = false;

        for _pass in 0..4 {
            if self.net.membership_gen() != mgen {
                break;
            }
            let mut progress = false;
            let mut blocked_in_pass: HashSet<String> = HashSet::new();
            let mut work: Vec<(u8, String, Vec<Candidate>)> = Vec::new();

            for (stream_id, cands) in &by_stream {
                if resolved_or_done.contains(stream_id) {
                    continue;
                }
                let Some(first) = cands.first() else {
                    continue;
                };
                match first.head.kind {
                    StreamKind::Provider => work.push((0, stream_id.clone(), cands.clone())),
                    StreamKind::Model => work.push((1, stream_id.clone(), cands.clone())),
                    StreamKind::Mcp => work.push((2, stream_id.clone(), cands.clone())),
                    StreamKind::Session => {
                        let any_deleted = cands.iter().any(|c| c.head.deleted);
                        if any_deleted {
                            work.push((5, stream_id.clone(), cands.clone()));
                        }
                        work.push((3, stream_id.clone(), cands.clone()));
                        work.push((7, stream_id.clone(), cands.clone()));
                    }
                    StreamKind::Call => {
                        work.push((4, stream_id.clone(), cands.clone()));
                        work.push((6, stream_id.clone(), cands.clone()));
                    }
                }
            }
            work.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

            for (phase, stream_id, mut cands) in work {
                if self.net.membership_gen() != mgen {
                    break;
                }
                if resolved_or_done.contains(&stream_id) || blocked_in_pass.contains(&stream_id) {
                    continue;
                }
                cands.sort_by(|a, b| {
                    b.head
                        .epoch
                        .cmp(&a.head.epoch)
                        .then_with(|| b.head.deleted.cmp(&a.head.deleted))
                        .then_with(|| (!b.full_holders.is_empty()).cmp(&!a.full_holders.is_empty()))
                        .then_with(|| b.head.head_seq.cmp(&a.head.head_seq))
                });

                let force_watch = watch_sid.is_some_and(|sid| sid.as_str() == stream_id);

                if phase == 3 {
                    // Check ALL max-epoch candidates for multi-peer divergence (P1-04).
                    let max_epoch = cands[0].head.epoch;
                    let best = &cands[0];
                    if best.head.deleted {
                        continue;
                    }
                    let mut diverged_peer_pair: Option<(&Candidate, String)> = None;
                    for alt in cands.iter().skip(1) {
                        if alt.head.epoch != max_epoch {
                            break;
                        }
                        if alt.head.head_seq == best.head.head_seq {
                            if alt.head.head_hash != best.head.head_hash {
                                diverged_peer_pair = Some((alt, alt.holders[0].clone()));
                                break;
                            }
                        } else if alt.head.head_seq > 0 {
                            let proof = self
                                .query_commit_hash_from_holders(
                                    &best.holders,
                                    &stream_id,
                                    max_epoch,
                                    alt.head.head_seq,
                                )
                                .await;
                            if let Some(h) = proof
                                && h != alt.head.head_hash
                            {
                                diverged_peer_pair = Some((alt, alt.holders[0].clone()));
                                break;
                            }
                        }
                    }

                    let lock = self.stream_locks.lock_for(stream_id.clone());
                    let _guard = lock.lock().await;
                    let local_state = self
                        .db
                        .call({
                            let sid = stream_id.clone();
                            move |s| stream_head(&s.conn, &sid)
                        })
                        .await?;
                    if local_state
                        .as_ref()
                        .is_some_and(|l| l.watched || l.synced_seq > 0)
                        || force_watch
                    {
                        continue;
                    }

                    // If two online peers themselves diverged, record conflict and skip directory write!
                    if let Some((alt_cand, alt_peer)) = diverged_peer_pair {
                        let conflict_recorded = self
                            .db
                            .call({
                                let best_h = best.head.clone();
                                let alt_h = alt_cand.head.clone();
                                let streams = self.streams.clone();
                                move |s| {
                                    assert_membership_gen(&s.conn, mgen)?;
                                    let pseudo_local = ledger::LocalStreamState {
                                        stream_id: best_h.stream_id.clone(),
                                        kind: StreamKind::Session,
                                        epoch: best_h.epoch,
                                        head_seq: best_h.head_seq,
                                        head_hash: best_h.head_hash,
                                        synced_epoch: 0,
                                        synced_seq: 0,
                                        synced_hash: [0u8; 32],
                                        checkpoint_seq: 0,
                                        watched: false,
                                        deleted: best_h.deleted,
                                        retired: best_h.retired,
                                        head_snapshot: best_h.head_snapshot.clone(),
                                        updated_at: best_h.updated_at,
                                    };
                                    let _ = upsert_pseudo_session_head(&s.conn, &best_h);
                                    let newly = record_fork_conflict(&s.conn, &pseudo_local, &alt_h, &alt_peer)?;
                                    publish_outcome(
                                        s,
                                        &streams,
                                        StreamKind::Session,
                                        &best_h.stream_id,
                                        ApplyOutcome::Conflict {
                                            newly_recorded: newly,
                                        },
                                    )?;
                                    Ok(())
                                }
                            })
                            .await;
                        let _ = conflict_recorded;
                        resolved_or_done.insert(stream_id);
                        continue;
                    }

                    // Check ancestry against local directory head if same epoch & remote longer!
                    let mut ancestor_proof = AncestorProof::None;
                    if let Some(ref l) = local_state
                        && l.epoch == best.head.epoch
                        && best.head.head_seq > l.head_seq
                        && l.head_seq > 0
                    {
                        let mut check_holders = best.full_holders.clone();
                        for h in &best.holders {
                            if !check_holders.contains(h) {
                                check_holders.push(h.clone());
                            }
                        }
                        ancestor_proof = self
                            .prove_local_ancestor(
                                &check_holders,
                                &stream_id,
                                l.epoch,
                                l.head_seq,
                                l.head_hash,
                            )
                            .await;
                    }

                    let head_clone = best.head.clone();
                    let peer_clone = best.holders[0].clone();
                    let sid = stream_id.clone();
                    let streams = self.streams.clone();
                    let applied = self
                        .db
                        .call(move |s| {
                            assert_membership_gen(&s.conn, mgen)?;
                            let outcome = apply_unwatched_session_head(
                                &s.conn,
                                &head_clone,
                                &peer_clone,
                                ancestor_proof,
                            )?;
                            let tag = match &outcome {
                                ApplyOutcome::BlockedOnDependency => 1u8,
                                ApplyOutcome::Applied { updated: true, .. } => 2u8,
                                ApplyOutcome::Conflict { .. } => 3u8,
                                _ => 0u8,
                            };
                            publish_outcome(s, &streams, StreamKind::Session, &sid, outcome)?;
                            Ok(tag)
                        })
                        .await?;
                    match applied {
                        1 => {
                            blocked_in_pass.insert(stream_id);
                        }
                        2 => {
                            updated_streams.push(stream_id.clone());
                            progress = true;
                            resolved_or_done.insert(stream_id);
                        }
                        3 | 0 | _ => {
                            resolved_or_done.insert(stream_id);
                        }
                    }
                    continue;
                }

                if phase == 5 {
                    // Terminal session deletions (deleted == true candidates).
                    let best = &cands[0];
                    if !best.head.deleted {
                        continue;
                    }
                    let res = self
                        .pull_and_apply_terminal_delete(&best.head, &best.holders, mgen)
                        .await?;
                    match res {
                        PullResult::Updated => {
                            updated_streams.push(stream_id.clone());
                            progress = true;
                            resolved_or_done.insert(stream_id);
                        }
                        PullResult::Conflict | PullResult::UpToDate | PullResult::Ignored => {
                            resolved_or_done.insert(stream_id);
                        }
                        PullResult::Blocked => {
                            blocked_in_pass.insert(stream_id);
                        }
                    }
                    continue;
                }

                if phase == 7 && !force_watch {
                    let sid = stream_id.clone();
                    let should_fetch = self
                        .db
                        .call(move |s| {
                            Ok(stream_head(&s.conn, &sid)?
                                .is_some_and(|l| l.watched || l.synced_seq > 0))
                        })
                        .await?;
                    if !should_fetch {
                        resolved_or_done.insert(stream_id);
                        continue;
                    }
                }

                // Phases 0, 1, 2, 4, 6, 7: pull commits per candidate cleanly.
                let mut stream_updated = false;
                let mut stream_blocked = false;
                for cand in &cands {
                    if cand.head.deleted {
                        if matches!(cand.head.kind, StreamKind::Mcp | StreamKind::Session) {
                            let res = self
                                .pull_and_apply_terminal_delete(&cand.head, &cand.holders, mgen)
                                .await?;
                            match res {
                                PullResult::Updated => {
                                    stream_updated = true;
                                    progress = true;
                                }
                                PullResult::Blocked => {
                                    stream_blocked = true;
                                    break;
                                }
                                PullResult::Conflict => {
                                    resolved_or_done.insert(stream_id.clone());
                                    break;
                                }
                                PullResult::UpToDate | PullResult::Ignored => {}
                            }
                            continue;
                        }
                    }
                    let holders = if cand.head.kind == StreamKind::Session {
                        if cand.full_holders.is_empty() {
                            continue;
                        }
                        &cand.full_holders
                    } else {
                        &cand.holders
                    };
                    let (res, unblocked) = self
                        .pull_and_apply_stream(&cand.head, holders, force_watch, mgen)
                        .await?;
                    unblocked_to_repull.extend(unblocked);
                    match res {
                        PullResult::Updated => {
                            stream_updated = true;
                            progress = true;
                        }
                        PullResult::UpToDate | PullResult::Ignored => {}
                        PullResult::Blocked => {
                            stream_blocked = true;
                            break;
                        }
                        PullResult::Conflict => {
                            resolved_or_done.insert(stream_id.clone());
                            break;
                        }
                    }
                }
                if stream_updated {
                    updated_streams.push(stream_id.clone());
                }
                if stream_blocked {
                    blocked_in_pass.insert(stream_id);
                } else {
                    resolved_or_done.insert(stream_id);
                }
            }
            still_blocked = !blocked_in_pass.is_empty();
            if !progress || !still_blocked {
                break;
            }
        }

        // P1-05: re-pull any streams previously blocked on a now-freed unique name!
        unblocked_to_repull.sort();
        unblocked_to_repull.dedup();
        if !unblocked_to_repull.is_empty() {
            let mut extra = self.repull_streams_by_id(&unblocked_to_repull, mgen).await?;
            updated_streams.append(&mut extra);
        }

        updated_streams.sort();
        updated_streams.dedup();
        Ok((updated_streams, still_blocked))
    }

    async fn repull_streams_by_id(&self, stream_ids: &[String], mgen: i64) -> Result<Vec<String>> {
        if stream_ids.is_empty() || self.net.membership_gen() != mgen {
            return Ok(Vec::new());
        }
        let online = self.net.online_peer_ids();
        let req = RpcReq::GetHeads {
            stream_ids: stream_ids.to_vec(),
        };
        let mut manifests = Vec::new();
        for peer_id in online {
            if let Ok((RpcResp::Manifest { heads, .. }, _)) =
                self.rpc(&peer_id, &req, RPC_TIMEOUT_SHORT).await
                && !heads.is_empty()
            {
                manifests.push((peer_id, heads));
            }
        }
        if manifests.is_empty() {
            return Ok(Vec::new());
        }
        let mut updated = Vec::new();
        for (_, heads) in manifests {
            if self.net.membership_gen() != mgen {
                break;
            }
            for head in heads {
                if let Some(holders_peer) = self
                    .find_holders_for_head(&head)
                    .await
                    .filter(|h| !h.is_empty())
                {
                    let (res, _) = self
                        .pull_and_apply_stream(&head, &holders_peer, false, mgen)
                        .await?;
                    if res == PullResult::Updated {
                        updated.push(head.stream_id);
                    }
                }
            }
        }
        updated.sort();
        updated.dedup();
        Ok(updated)
    }

    async fn find_holders_for_head(&self, target: &StreamHead) -> Option<Vec<String>> {
        let online = self.net.online_peer_ids();
        let req = RpcReq::GetHeads {
            stream_ids: vec![target.stream_id.clone()],
        };
        let mut holders = Vec::new();
        for peer_id in online {
            if let Ok((RpcResp::Manifest { heads, .. }, _)) =
                self.rpc(&peer_id, &req, RPC_TIMEOUT_SHORT).await
                && heads.iter().any(|h| {
                    h.stream_id == target.stream_id
                        && h.epoch == target.epoch
                        && h.head_seq == target.head_seq
                        && h.head_hash == target.head_hash
                        && (target.kind != StreamKind::Session || target.deleted || h.body_complete)
                })
            {
                holders.push(peer_id);
            }
        }
        (!holders.is_empty()).then_some(holders)
    }

    async fn query_commit_hash_from_holders(
        &self,
        holders: &[String],
        stream_id: &str,
        epoch: i64,
        seq: i64,
    ) -> Option<[u8; 32]> {
        for peer in holders {
            if let Ok((RpcResp::CommitHash { hash: Some(h) }, _)) = self
                .rpc(
                    peer,
                    &RpcReq::GetCommitHash {
                        stream_id: stream_id.to_owned(),
                        epoch,
                        seq,
                    },
                    RPC_TIMEOUT_SHORT,
                )
                .await
            {
                return Some(h);
            }
        }
        None
    }

    async fn prove_local_ancestor(
        &self,
        holders: &[String],
        stream_id: &str,
        epoch: i64,
        seq: i64,
        hash: [u8; 32],
    ) -> AncestorProof {
        if seq <= 0 {
            return AncestorProof::None;
        }
        match self
            .query_commit_hash_from_holders(holders, stream_id, epoch, seq)
            .await
        {
            Some(h) if h == hash => AncestorProof::Verified { epoch, seq, hash },
            Some(_) => AncestorProof::Diverged { epoch, seq, hash },
            None => AncestorProof::None,
        }
    }

    async fn pull_and_apply_terminal_delete(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        expected_mgen: i64,
    ) -> Result<PullResult> {
        let lock = self.stream_locks.lock_for(remote_head.stream_id.clone());
        let _guard = lock.lock().await;
        self.pull_and_apply_terminal_delete_locked(remote_head, holders, expected_mgen)
            .await
    }

    async fn pull_and_apply_terminal_delete_locked(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        expected_mgen: i64,
    ) -> Result<PullResult> {
        let mut cached_delete: Option<(String, CommitWire)> = None;
        for _attempt in 0..3 {
            if self.net.membership_gen() != expected_mgen {
                return Ok(PullResult::Ignored);
            }
            let local = self
                .db
                .call({
                    let sid = remote_head.stream_id.clone();
                    move |s| stream_head(&s.conn, &sid)
                })
                .await?;
            let mut ancestor_proof = AncestorProof::None;
            if let Some(ref l) = local {
                if remote_head.epoch < l.epoch {
                    return Ok(PullResult::Ignored);
                }
                if remote_head.epoch == l.epoch && l.deleted && l.head_hash == remote_head.head_hash
                {
                    return Ok(PullResult::UpToDate);
                }
                if remote_head.epoch == l.epoch
                    && l.head_seq > 0
                    && l.head_seq < remote_head.head_seq
                {
                    ancestor_proof = self
                        .prove_local_ancestor(
                            holders,
                            &remote_head.stream_id,
                            l.epoch,
                            l.head_seq,
                            l.head_hash,
                        )
                        .await;
                }
            }

            if cached_delete.is_none() {
                cached_delete = self.fetch_delete_commit(remote_head, holders).await;
            }
            let Some((used_peer, delete_commit)) = cached_delete.clone() else {
                return Ok(PullResult::Ignored);
            };
            if self.net.membership_gen() != expected_mgen {
                return Ok(PullResult::Ignored);
            }

            let head_clone = remote_head.clone();
            let streams = self.streams.clone();
            let outcome_res = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let outcome = match head_clone.kind {
                        StreamKind::Session => apply_terminal_session_delete(
                            &s.conn,
                            &head_clone,
                            &delete_commit,
                            ancestor_proof,
                            &used_peer,
                            None,
                        )?,
                        StreamKind::Mcp => apply_terminal_mcp_delete(
                            &s.conn,
                            &head_clone,
                            &delete_commit,
                            ancestor_proof,
                            &used_peer,
                            None,
                        )?,
                        _ => ApplyOutcome::Ignored,
                    };
                    if matches!(outcome, ApplyOutcome::StaleProof) {
                        return Ok(None);
                    }
                    let res = match &outcome {
                        ApplyOutcome::Applied { updated: true, .. } => PullResult::Updated,
                        ApplyOutcome::Applied { updated: false, .. } => PullResult::UpToDate,
                        ApplyOutcome::Conflict { .. } => PullResult::Conflict,
                        ApplyOutcome::BlockedOnDependency => PullResult::Blocked,
                        ApplyOutcome::Ignored | ApplyOutcome::StaleProof => PullResult::Ignored,
                    };
                    publish_outcome(s, &streams, head_clone.kind, &head_clone.stream_id, outcome)?;
                    Ok(Some(res))
                })
                .await?;
            if let Some(res) = outcome_res {
                return Ok(res);
            }
        }
        Ok(PullResult::Ignored)
    }

    pub async fn sync_session_latest(&self, sid: SessionId) -> Result<()> {
        if !self.net.online_peer_ids().is_empty() {
            let _ = self.sync_with_peers(None, Some(sid)).await?;
        }
        self.ensure_session_watched(sid).await?;
        self.db
            .call(move |s| crate::cluster::ledger::assert_no_conflict(&s.conn, sid.as_str()))
            .await
    }

    pub async fn ensure_session_watched(&self, sid: SessionId) -> Result<()> {
        let stream_id = sid.to_string();

        // P1-03: Only fast-path return if body is COMPLETE (`watched && synced == target`),
        // prefix is verified, and no unresolved conflict leaves it incomplete.
        let (fast_ok, fast_deleted) = self
            .db
            .call(move |s| {
                let Some(l) = stream_head(&s.conn, sid.as_str())? else {
                    return Ok((false, false));
                };
                if l.deleted {
                    return Ok((false, true));
                }
                Ok((l.is_body_complete(), false))
            })
            .await?;
        if fast_deleted {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        if fast_ok {
            return Ok(());
        }

        if !self.net.online_peer_ids().is_empty() {
            let _ = self.sync_with_peers(None, None).await;
        }

        let lock = self.stream_locks.lock_for(stream_id.clone());
        let _guard = lock.lock().await;
        let mgen = self.net.membership_gen();

        let local = self
            .db
            .call(move |s| stream_head(&s.conn, sid.as_str()))
            .await?;
        if let Some(ref l) = local {
            if l.deleted {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            if l.is_body_complete() {
                return Ok(());
            }
            if !l.watched
                && l.synced_epoch == l.epoch
                && l.synced_seq == l.head_seq
                && l.synced_hash == l.head_hash
            {
                let sid_owned = stream_id.clone();
                self.db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, mgen)?;
                        mark_session_watched(&s.conn, &sid_owned)?;
                        Ok(())
                    })
                    .await?;
                return Ok(());
            }
        }

        let online = self.net.online_peer_ids();
        let req = RpcReq::GetHeads {
            stream_ids: vec![stream_id.clone()],
        };
        let replies = futures_util::future::join_all(online.iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await) }
        }))
        .await;

        let mut candidates: Vec<(StreamHead, Vec<String>, Vec<String>)> = Vec::new();
        for (peer_id, reply) in replies {
            if let Ok((RpcResp::Manifest { heads, .. }, _)) = reply {
                for head in heads {
                    if head.stream_id != stream_id {
                        continue;
                    }
                    if let Some((h, holders, full_holders)) = candidates.iter_mut().find(|(h, _, _)| {
                        h.epoch == head.epoch
                            && h.head_seq == head.head_seq
                            && h.head_hash == head.head_hash
                    }) {
                        if !holders.contains(&peer_id) {
                            holders.push(peer_id.clone());
                        }
                        if head.body_complete && !full_holders.contains(&peer_id) {
                            full_holders.push(peer_id.clone());
                        }
                        if head.body_complete {
                            h.body_complete = true;
                        }
                    } else {
                        let full = if head.body_complete {
                            vec![peer_id.clone()]
                        } else {
                            Vec::new()
                        };
                        candidates.push((head, vec![peer_id.clone()], full));
                    }
                }
            }
        }

        candidates.sort_by(|a, b| {
            b.0.epoch
                .cmp(&a.0.epoch)
                .then_with(|| b.0.deleted.cmp(&a.0.deleted))
                .then_with(|| (!b.2.is_empty()).cmp(&!a.2.is_empty()))
                .then_with(|| b.0.head_seq.cmp(&a.0.head_seq))
        });

        for (head, holders, full_holders) in candidates {
            if head.deleted {
                let res = self
                    .pull_and_apply_terminal_delete_locked(&head, &holders, mgen)
                    .await?;
                if matches!(res, PullResult::Updated | PullResult::Conflict) {
                    break;
                }
                continue;
            }
            if full_holders.is_empty() {
                continue;
            }
            let (res, _) = self
                .pull_and_apply_stream_locked(&head, &full_holders, true, mgen)
                .await?;
            match res {
                PullResult::Blocked => {
                    return Err(Error::conflict(
                        "session dependencies not yet synced; retry Watch",
                    ));
                }
                PullResult::Conflict => break,
                PullResult::Updated | PullResult::UpToDate | PullResult::Ignored => {}
            }
        }

        let after = self
            .db
            .call(move |s| stream_head(&s.conn, sid.as_str()))
            .await?;
        match after {
            Some(l) if l.deleted => Err(rusqlite::Error::QueryReturnedNoRows.into()),
            Some(l) => {
                if !l.is_body_complete() {
                    return Err(Error::conflict(
                        "session history is currently unavailable on online peers",
                    ));
                }
                Ok(())
            }
            None => Err(rusqlite::Error::QueryReturnedNoRows.into()),
        }
    }

    async fn fetch_commits_page(
        &self,
        holders: &[String],
        batch_idx: usize,
        stream_id: &str,
        epoch: i64,
        after_seq: i64,
        up_to_seq: Option<i64>,
    ) -> Result<Option<(String, Vec<CommitWire>)>> {
        for attempt in 0..holders.len().max(1) {
            let Some(peer_id) = holders.get((batch_idx + attempt) % holders.len().max(1)) else {
                break;
            };
            let resp = self
                .rpc(
                    peer_id,
                    &RpcReq::GetCommits {
                        stream_id: stream_id.to_owned(),
                        epoch,
                        after_seq,
                        up_to_seq,
                        max_count: COMMIT_BATCH_COUNT,
                        max_bytes: COMMIT_BATCH_BYTES,
                    },
                    RPC_TIMEOUT_DATA,
                )
                .await;
            let Ok((
                RpcResp::Commits {
                    commits,
                    oversized_next,
                    ..
                },
                _,
            )) = resp
            else {
                continue;
            };
            if !commits.is_empty() {
                return Ok(Some((peer_id.clone(), commits)));
            }
            let Some(meta) = oversized_next else {
                continue;
            };
            if meta.stream_id != stream_id
                || meta.epoch != epoch
                || meta.seq != after_seq + 1
                || meta.payload_len < 0
                || meta.payload_len as usize > MAX_COMMIT_PAYLOAD_BYTES
            {
                continue;
            }
            let total_len = meta.payload_len as usize;
            let mut buf = Vec::with_capacity(total_len.min(1024 * 1024));
            let mut failed_chunk = false;
            while buf.len() < total_len {
                let remaining = total_len - buf.len();
                let req_len = u32::try_from(remaining.min(COMMIT_CHUNK_BYTES)).unwrap_or(1);
                let mut got_chunk = None;
                let mut ordered_peers = vec![peer_id];
                for p in holders {
                    if p != peer_id {
                        ordered_peers.push(p);
                    }
                }
                for p in ordered_peers {
                    if let Ok((RpcResp::CommitChunk, bytes)) = self
                        .rpc(
                            p,
                            &RpcReq::GetCommitChunk {
                                stream_id: stream_id.to_owned(),
                                epoch,
                                seq: meta.seq,
                                commit_hash: meta.commit_hash,
                                offset: buf.len() as u64,
                                len: req_len,
                            },
                            RPC_TIMEOUT_DATA,
                        )
                        .await
                        && !bytes.is_empty()
                        && buf.len() + bytes.len() <= total_len
                    {
                        got_chunk = Some(bytes);
                        break;
                    }
                }
                let Some(chunk) = got_chunk else {
                    failed_chunk = true;
                    break;
                };
                buf.extend_from_slice(&chunk);
            }
            if failed_chunk || buf.len() != total_len {
                continue;
            }
            let Ok(payload) = String::from_utf8(buf) else {
                continue;
            };
            let wire = CommitWire {
                stream_id: meta.stream_id,
                kind: meta.kind,
                epoch: meta.epoch,
                seq: meta.seq,
                parent_hash: meta.parent_hash,
                commit_hash: meta.commit_hash,
                author_node_id: meta.author_node_id,
                op_type: meta.op_type,
                payload,
                created_at: meta.created_at,
            };
            if ledger::verify_commit(&wire, wire.seq, &wire.parent_hash) {
                return Ok(Some((peer_id.clone(), vec![wire])));
            }
        }
        Ok(None)
    }

    async fn pull_and_apply_stream(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        force_watch: bool,
        expected_mgen: i64,
    ) -> Result<(PullResult, Vec<String>)> {
        let lock = self.stream_locks.lock_for(remote_head.stream_id.clone());
        let _guard = lock.lock().await;
        self.pull_and_apply_stream_locked(remote_head, holders, force_watch, expected_mgen)
            .await
    }

    async fn pull_and_apply_stream_locked(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        force_watch: bool,
        expected_mgen: i64,
    ) -> Result<(PullResult, Vec<String>)> {
        let mut unblocked_all = Vec::new();
        if self.net.membership_gen() != expected_mgen {
            return Ok((PullResult::Ignored, unblocked_all));
        }
        if remote_head.kind == StreamKind::Session && !remote_head.deleted && holders.is_empty() {
            return Ok((PullResult::Ignored, unblocked_all));
        }
        let remote_clone = remote_head.clone();
        let primary_peer = holders.first().cloned().unwrap_or_default();

        // Pin target head cleanly inside DB if this is same-epoch longer unwatched directory -> wait:
        // we verify ancestry FIRST before accepting higher same-epoch seqs!
        let local_opt = self
            .db
            .call({
                let sid = remote_head.stream_id.clone();
                move |s| stream_head(&s.conn, &sid)
            })
            .await?;

        let (mut cursor, target_epoch, overwrite_epoch) = match &local_opt {
            None => (0i64, remote_head.epoch, false),
            Some(l) => {
                if remote_head.epoch < l.epoch {
                    return Ok((PullResult::Ignored, unblocked_all));
                }
                if remote_head.epoch > l.epoch {
                    if l.retired && !remote_head.deleted {
                        return Ok((PullResult::Ignored, unblocked_all));
                    }
                    (0i64, remote_head.epoch, true)
                } else {
                    let sid_chk = remote_head.stream_id.clone();
                    if self
                        .db
                        .call(move |s| ledger::has_conflict(&s.conn, &sid_chk))
                        .await?
                    {
                        return Ok((PullResult::Conflict, unblocked_all));
                    }
                    if l.retired {
                        return Ok((PullResult::Ignored, unblocked_all));
                    }
                    if remote_head.head_seq == l.head_seq && remote_head.head_hash == l.head_hash {
                        if l.synced_epoch == l.epoch
                            && l.synced_seq == l.head_seq
                            && l.synced_hash == l.head_hash
                        {
                            if force_watch && !l.watched {
                                let sid_w = remote_head.stream_id.clone();
                                self.db
                                    .call(move |s| {
                                        assert_membership_gen(&s.conn, expected_mgen)?;
                                        mark_session_watched(&s.conn, &sid_w)?;
                                        Ok(())
                                    })
                                    .await?;
                            }
                            return Ok((PullResult::UpToDate, unblocked_all));
                        }
                        if force_watch || l.watched || l.synced_seq > 0 {
                            (l.synced_seq, l.epoch, false)
                        } else {
                            return Ok((PullResult::UpToDate, unblocked_all));
                        }
                    } else if remote_head.head_seq <= l.synced_seq {
                        let matched = self
                            .db
                            .call({
                                let sid = remote_head.stream_id.clone();
                                let e = l.epoch;
                                let seq = remote_head.head_seq;
                                let h = remote_head.head_hash;
                                move |s| ledger::has_commit_hash(&s.conn, &sid, e, seq, &h)
                            })
                            .await?;
                        if matched {
                            return Ok((PullResult::Ignored, unblocked_all));
                        }
                        self.db
                            .call({
                                let streams = self.streams.clone();
                                move |s| {
                                    assert_membership_gen(&s.conn, expected_mgen)?;
                                    let l_cur = stream_head(&s.conn, &remote_clone.stream_id)?;
                                    if let Some(ref lc) = l_cur {
                                        let newly = record_fork_conflict(
                                            &s.conn,
                                            lc,
                                            &remote_clone,
                                            &primary_peer,
                                        )?;
                                        publish_outcome(
                                            s,
                                            &streams,
                                            lc.kind,
                                            &lc.stream_id,
                                            ApplyOutcome::Conflict {
                                                newly_recorded: newly,
                                            },
                                        )?;
                                    }
                                    Ok(())
                                }
                            })
                            .await?;
                        return Ok((PullResult::Conflict, unblocked_all));
                    } else {
                        // remote_head.head_seq > l.synced_seq at same epoch.
                        if l.synced_seq < l.head_seq && l.head_seq > remote_head.head_seq {
                            // Never downgrade known head_seq to a shorter branch!
                            return Ok((PullResult::Ignored, unblocked_all));
                        }
                        if l.head_seq > 0 && remote_head.head_seq >= l.head_seq && l.synced_seq < l.head_seq {
                            let h_opt = self
                                .query_commit_hash_from_holders(
                                    holders,
                                    &remote_head.stream_id,
                                    l.epoch,
                                    l.head_seq,
                                )
                                .await;
                            match h_opt {
                                Some(h) if h == l.head_hash => {}
                                Some(_) => {
                                    self.db
                                        .call({
                                            let streams = self.streams.clone();
                                            move |s| {
                                                assert_membership_gen(&s.conn, expected_mgen)?;
                                                let l_cur = stream_head(&s.conn, &remote_clone.stream_id)?;
                                                if let Some(ref lc) = l_cur {
                                                    let newly = record_fork_conflict(
                                                        &s.conn,
                                                        lc,
                                                        &remote_clone,
                                                        &primary_peer,
                                                    )?;
                                                    publish_outcome(
                                                        s,
                                                        &streams,
                                                        lc.kind,
                                                        &lc.stream_id,
                                                        ApplyOutcome::Conflict {
                                                            newly_recorded: newly,
                                                        },
                                                    )?;
                                                }
                                                Ok(())
                                            }
                                        })
                                        .await?;
                                    return Ok((PullResult::Conflict, unblocked_all));
                                }
                                None => {
                                    if force_watch {
                                        return Err(Error::conflict(
                                            "unable to verify commit ancestry from online holders",
                                        ));
                                    }
                                    return Ok((PullResult::Blocked, unblocked_all));
                                }
                            }
                            // P1-03: pin higher target head cleanly while pulling!
                            if remote_head.kind == StreamKind::Session && remote_head.head_seq > l.head_seq {
                                self.db
                                    .call({
                                        let rc = remote_head.clone();
                                        let pp = primary_peer.clone();
                                        move |s| {
                                            assert_membership_gen(&s.conn, expected_mgen)?;
                                            let _ = pin_session_target_head(&s.conn, &rc, &pp)?;
                                            Ok(())
                                        }
                                    })
                                    .await?;
                            }
                        }
                        (l.synced_seq, remote_head.epoch, false)
                    }
                }
            }
        };

        if overwrite_epoch {
            // P1-13 & P1-21: Stream pages into sync_staging_commits, verify entire epoch, then
            // stop any running local worker and commit atomically!
            self.db
                .call({
                    let sid = remote_head.stream_id.clone();
                    move |s| {
                        assert_membership_gen(&s.conn, expected_mgen)?;
                        clear_staging_for_stream(&s.conn, &sid)
                    }
                })
                .await?;
        }

        let mut any_updated = false;
        let mut batch_idx = 0usize;
        let mut last_peer = primary_peer.clone();

        while cursor < remote_head.head_seq {
            if self.net.membership_gen() != expected_mgen {
                if overwrite_epoch {
                    let _ = self
                        .db
                        .call({
                            let sid = remote_head.stream_id.clone();
                            move |s| clear_staging_for_stream(&s.conn, &sid)
                        })
                        .await;
                }
                return Ok((PullResult::Ignored, unblocked_all));
            }
            let fetched = self
                .fetch_commits_page(
                    holders,
                    batch_idx,
                    &remote_head.stream_id,
                    target_epoch,
                    cursor,
                    Some(remote_head.head_seq),
                )
                .await?;
            let Some((used_peer, commits)) = fetched else {
                if overwrite_epoch {
                    let _ = self
                        .db
                        .call({
                            let sid = remote_head.stream_id.clone();
                            move |s| clear_staging_for_stream(&s.conn, &sid)
                        })
                        .await;
                }
                if force_watch {
                    return Err(Error::conflict(format!(
                        "could not fetch commits for {} from online peers",
                        remote_head.stream_id
                    )));
                }
                return Ok((
                    if any_updated {
                        PullResult::Updated
                    } else {
                        PullResult::Ignored
                    },
                    unblocked_all,
                ));
            };
            last_peer = used_peer.clone();
            let head_clone = remote_head.clone();
            if overwrite_epoch {
                match self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, expected_mgen)?;
                        stage_remote_commits_page(&s.conn, &head_clone, &commits)
                    })
                    .await
                {
                    Ok(next_cursor) => {
                        cursor = next_cursor;
                        batch_idx += 1;
                    }
                    Err(e) => {
                        let _ = self
                            .db
                            .call({
                                let sid = remote_head.stream_id.clone();
                                move |s| clear_staging_for_stream(&s.conn, &sid)
                            })
                            .await;
                        return Err(e);
                    }
                }
            } else {
                let next_cursor = commits.last().unwrap().seq;
                let streams = self.streams.clone();
                let (tag, unblocked) = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, expected_mgen)?;
                        let outcome = apply_remote_commits_page(
                            &s.conn,
                            &head_clone,
                            &commits,
                            &used_peer,
                            force_watch,
                        )?;
                        let unblocked = match &outcome {
                            ApplyOutcome::Applied {
                                unblocked_conflicts, ..
                            } => unblocked_conflicts.clone(),
                            _ => Vec::new(),
                        };
                        let tag = match &outcome {
                            ApplyOutcome::Applied { updated: true, .. } => PullResult::Updated,
                            ApplyOutcome::Applied { updated: false, .. } => PullResult::UpToDate,
                            ApplyOutcome::BlockedOnDependency => PullResult::Blocked,
                            ApplyOutcome::Conflict { .. } => PullResult::Conflict,
                            ApplyOutcome::Ignored | ApplyOutcome::StaleProof => PullResult::Ignored,
                        };
                        publish_outcome(
                            s,
                            &streams,
                            head_clone.kind,
                            &head_clone.stream_id,
                            outcome,
                        )?;
                        Ok((tag, unblocked))
                    })
                    .await?;
                unblocked_all.extend(unblocked);
                match tag {
                    PullResult::Updated => {
                        any_updated = true;
                        cursor = next_cursor;
                        batch_idx += 1;
                    }
                    PullResult::UpToDate => {
                        cursor = next_cursor;
                        batch_idx += 1;
                    }
                    other => return Ok((other, unblocked_all)),
                }
            }
        }

        if overwrite_epoch {
            let head_clone = remote_head.clone();
            let streams = self.streams.clone();
            let used_peer = last_peer;
            let (tag, unblocked) = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let outcome = commit_staged_epoch(
                        &s.conn,
                        &head_clone,
                        &used_peer,
                        force_watch,
                        None,
                    )?;
                    let unblocked = match &outcome {
                        ApplyOutcome::Applied {
                            unblocked_conflicts, ..
                        } => unblocked_conflicts.clone(),
                        _ => Vec::new(),
                    };
                    let tag = match &outcome {
                        ApplyOutcome::Applied { updated: true, .. } => PullResult::Updated,
                        ApplyOutcome::Applied { updated: false, .. } => PullResult::UpToDate,
                        ApplyOutcome::BlockedOnDependency => PullResult::Blocked,
                        ApplyOutcome::Conflict { .. } => PullResult::Conflict,
                        ApplyOutcome::Ignored | ApplyOutcome::StaleProof => PullResult::Ignored,
                    };
                    publish_outcome(
                        s,
                        &streams,
                        head_clone.kind,
                        &head_clone.stream_id,
                        outcome,
                    )?;
                    Ok((tag, unblocked))
                })
                .await?;
            let _ = self
                .db
                .call({
                    let sid = remote_head.stream_id.clone();
                    move |s| clear_staging_for_stream(&s.conn, &sid)
                })
                .await;
            unblocked_all.extend(unblocked);
            return Ok((tag, unblocked_all));
        }

        Ok((
            if any_updated {
                PullResult::Updated
            } else {
                PullResult::UpToDate
            },
            unblocked_all,
        ))
    }

    pub async fn ensure_attachment_data(self: &Arc<Self>, id: AttachmentId) -> Result<()> {
        let info: Option<(Vec<u8>, i64, bool)> = self
            .db
            .call(move |s| {
                s.conn
                    .prepare_cached(
                        "SELECT content_hash,byte_len,data IS NOT NULL FROM attachments WHERE id=?",
                    )?
                    .query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .optional()
                    .map_err(Into::into)
            })
            .await?;
        let Some((hash_vec, byte_len, has_data)) = info else {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        };
        if has_data {
            return Ok(());
        }
        let content_hash: [u8; 32] = hash_vec
            .try_into()
            .map_err(|_| Error::internal("invalid attachment content_hash"))?;
        let lock = self.att_locks.lock_for(content_hash);
        let _guard = lock.lock().await;

        let already = self
            .db
            .call(move |s| {
                let has: bool = s.conn.query_row(
                    "SELECT data IS NOT NULL FROM attachments WHERE id=?",
                    [id],
                    |r| r.get(0),
                )?;
                if has {
                    return Ok(true);
                }
                let local_copy: Option<Vec<u8>> = s
                    .conn
                    .prepare_cached(
                        "SELECT data FROM attachments WHERE content_hash=? AND data IS NOT NULL LIMIT 1",
                    )?
                    .query_row([content_hash.as_slice()], |r| r.get(0))
                    .optional()?;
                if let Some(bytes) = local_copy {
                    s.conn.execute(
                        "UPDATE attachments SET data=? WHERE content_hash=? AND data IS NULL",
                        params![bytes, content_hash.as_slice()],
                    )?;
                    return Ok(true);
                }
                Ok(false)
            })
            .await?;
        if already {
            return Ok(());
        }

        let online = self.net.online_peer_ids();
        let mut holders = Vec::new();
        for peer_id in online {
            if let Ok((RpcResp::AttachmentsAvailable { hashes }, _)) = self
                .rpc(
                    &peer_id,
                    &RpcReq::HasAttachments {
                        hashes: vec![content_hash],
                    },
                    RPC_TIMEOUT_SHORT,
                )
                .await
                && hashes.contains(&content_hash)
            {
                holders.push(peer_id);
            }
        }
        if holders.is_empty() {
            return Err(Error::conflict(format!(
                "attachment {id} bytes are unavailable on online peers"
            )));
        }

        let total_len = usize::try_from(byte_len)
            .map_err(|_| Error::internal("attachment byte_len overflow"))?;
        if total_len == 0 || total_len > crate::media::MAX_ATTACHMENT_BYTES {
            return Err(Error::bad("attachment exceeds allowed byte budget"));
        }
        let num_chunks = total_len.div_ceil(ATT_CHUNK_SIZE);
        let permits_needed = u32::try_from(num_chunks)
            .map_err(|_| Error::bad("attachment chunk count overflow"))?;
        let _att_budget_permit = tokio::time::timeout(
            RPC_TIMEOUT_DATA,
            self.att_byte_budget.clone().acquire_many_owned(permits_needed),
        )
        .await
        .map_err(|_| Error::conflict("attachment download memory budget saturated"))?
        .map_err(|_| Error::internal("attachment budget closed"))?;

        let next_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let holders = Arc::new(holders);
        let results: Arc<Mutex<Vec<Option<Vec<u8>>>>> =
            Arc::new(Mutex::new(vec![None; num_chunks]));

        let worker_count = MAX_CHUNK_CONCURRENCY.min(num_chunks);
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..worker_count {
            let eng = Arc::clone(self);
            let holders = holders.clone();
            let next_idx = next_idx.clone();
            let results = results.clone();
            set.spawn(async move {
                loop {
                    let idx = next_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if idx >= num_chunks {
                        return Ok::<(), Error>(());
                    }
                    let offset = idx * ATT_CHUNK_SIZE;
                    let len = (total_len - offset).min(ATT_CHUNK_SIZE);
                    let mut chunk = None;
                    for attempt in 0..holders.len() {
                        let peer_id = &holders[(idx + attempt) % holders.len()];
                        let res = eng
                            .rpc(
                                peer_id,
                                &RpcReq::GetAttachmentChunk {
                                    content_hash,
                                    offset: offset as u64,
                                    len: len as u32,
                                },
                                RPC_TIMEOUT_DATA,
                            )
                            .await;
                        if let Ok((RpcResp::AttachmentChunk, body)) = res
                            && body.len() == len
                        {
                            chunk = Some(body);
                            break;
                        }
                    }
                    let Some(bytes) = chunk else {
                        return Err(Error::conflict(format!(
                            "failed to download chunk {idx} of attachment {id}"
                        )));
                    };
                    results.lock().unwrap()[idx] = Some(bytes);
                }
            });
        }
        while let Some(res) = set.join_next().await {
            res.map_err(|e| Error::internal(e.to_string()))??;
        }

        // P1-14: Offload buffer assembly and BLAKE3 verification off the Tokio current-thread loop.
        let parts = std::mem::take(&mut *results.lock().unwrap());
        let assembled = self
            .run_blocking(move || -> Result<Vec<u8>> {
                let mut assembled = Vec::with_capacity(total_len);
                for part in parts {
                    let bytes =
                        part.ok_or_else(|| Error::internal("missing attachment chunk"))?;
                    assembled.extend_from_slice(&bytes);
                }
                if blake3::hash(&assembled).as_bytes() != &content_hash {
                    return Err(Error::bad(format!(
                        "attachment {id} failed BLAKE3 integrity verification"
                    )));
                }
                Ok(assembled)
            })
            .await??;

        self.db
            .call(move |s| {
                s.conn.execute(
                    "UPDATE attachments SET data=?1 WHERE content_hash=?2 AND data IS NULL",
                    params![assembled, content_hash.as_slice()],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn ensure_session_attachments(self: &Arc<Self>, sid: SessionId) -> Result<()> {
        let missing = self
            .db
            .call(move |s| crate::media::missing_session_context_attachments(&s.conn, sid))
            .await?;
        for att_id in missing {
            self.ensure_attachment_data(att_id).await?;
        }
        Ok(())
    }

    pub async fn resolve_conflict(&self, stream_id: &str, resolution: &str) -> Result<()> {
        let lock = self.stream_locks.lock_for(stream_id.to_owned());
        let _guard = lock.lock().await;
        match resolution {
            "local" => {
                let mgen = self.net.membership_gen();
                let conflict: ConflictRecord = self
                    .db
                    .call({
                        let sid = stream_id.to_owned();
                        move |s| get_conflict(&s.conn, &sid)
                    })
                    .await?;

                // P0-01: For a conflicted directory-only Session node resolving "local", first fetch
                // its conflicted local branch from any online holder with complete body!
                if conflict.kind == StreamKind::Session {
                    let has_local_body = self
                        .db
                        .call({
                            let sid = stream_id.to_owned();
                            let c = conflict.clone();
                            move |s| {
                                let st = stream_head(&s.conn, &sid)?;
                                Ok(st.is_some_and(|l| {
                                    l.is_body_complete()
                                        && l.epoch == c.local_epoch
                                        && l.head_seq == c.local_seq
                                        && l.head_hash == c.local_hash
                                        && verify_local_prefix(
                                            &s.conn,
                                            &sid,
                                            l.epoch,
                                            l.checkpoint_seq,
                                            l.head_seq,
                                            &l.head_hash,
                                        )
                                        .unwrap_or(false)
                                }))
                            }
                        })
                        .await?;
                    if !has_local_body {
                        self.fetch_conflicted_local_branch(&conflict, mgen).await?;
                    }
                }

                let sid_owned = stream_id.to_owned();
                let exp_c = conflict.clone();
                let streams = self.streams.clone();
                let (res, mcp_changes, has_receipt) = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, mgen)?;
                        let _grant_guard = if exp_c.detaches_mcp() {
                            let id = sid_owned.parse().map_err(|_| Error::bad("invalid MCP stream id"))?;
                            crate::mcp::oauth::guard_grant_copy(&s.conn, &mut s.mcp, id)?
                        } else {
                            None
                        };
                        let out = resolve_conflict_local(&s.conn, &sid_owned, &exp_c)?;
                        for ch in &out.mcp_changes {
                            s.mcp.apply_sync_change(*ch);
                        }
                        if exp_c.kind == StreamKind::Session
                            && let Ok(sid) = sid_owned.parse::<SessionId>()
                        {
                            abort_active_session_worker(s, &streams, sid);
                            s.wake_waiters(sid);
                            if let Ok(info) = session(&s.conn, sid) {
                                let view = s.session_view(info);
                                s.publish_global("session.updated", json!(view));
                            }
                        } else if matches!(exp_c.kind, StreamKind::Provider | StreamKind::Model) {
                            s.publish_global("catalog.updated", json!({ "id": sid_owned }));
                        } else if exp_c.kind == StreamKind::Mcp {
                            s.publish_global("mcp.updated", json!({ "id": sid_owned }));
                        }
                        s.publish_global(
                            "sync.conflict",
                            json!({ "stream_id": sid_owned, "resolved": true }),
                        );
                        Ok((out.unblocked_conflicts, out.mcp_changes, out.new_head.is_some()))
                    })
                    .await?;
                let _ = mcp_changes;
                // Notify counterparty peers using durable resolution receipt.
                if has_receipt {
                    self.notify_conflict_resolved_to_peers(stream_id).await;
                }
                if !res.is_empty() {
                    let _ = self.repull_streams_by_id(&res, mgen).await;
                }
                Ok(())
            }
            "cluster" => {
                let mgen = self.net.membership_gen();
                let conflict: ConflictRecord = self
                    .db
                    .call({
                        let sid = stream_id.to_owned();
                        move |s| get_conflict(&s.conn, &sid)
                    })
                    .await?;
                let online = self.net.online_peer_ids();
                let mut candidates: Vec<(StreamHead, Vec<String>)> = Vec::new();
                for peer_id in &online {
                    if let Ok((RpcResp::Manifest { heads, .. }, _)) = self
                        .rpc(
                            peer_id,
                            &RpcReq::GetHeads {
                                stream_ids: vec![stream_id.to_owned()],
                            },
                            RPC_TIMEOUT_SHORT,
                        )
                        .await
                    {
                        for head in heads {
                            if head.stream_id != stream_id {
                                continue;
                            }
                            if head.kind == StreamKind::Session && !head.deleted && !head.body_complete
                            {
                                continue;
                            }
                            if conflict.local_epoch > 0
                                && head.epoch == conflict.local_epoch
                                && head.head_seq == conflict.local_seq
                                && head.head_hash == conflict.local_hash
                            {
                                continue;
                            }
                            if let Some((_, holders)) = candidates.iter_mut().find(|(h, _)| {
                                h.epoch == head.epoch
                                    && h.head_seq == head.head_seq
                                    && h.head_hash == head.head_hash
                            }) {
                                if !holders.contains(peer_id) {
                                    holders.push(peer_id.clone());
                                }
                            } else {
                                candidates.push((head, vec![peer_id.clone()]));
                            }
                        }
                    }
                }
                // P1-08: Verify each candidate proves exact remote branch or its descendant!
                let mut qualified = Vec::new();
                for (head, holders) in candidates {
                    let descendant_proof = if head.epoch == conflict.remote_epoch
                        && head.head_seq > conflict.remote_seq
                    {
                        self.prove_local_ancestor(
                            &holders,
                            stream_id,
                            conflict.remote_epoch,
                            conflict.remote_seq,
                            conflict.remote_hash,
                        )
                        .await
                    } else {
                        AncestorProof::None
                    };
                    let is_valid = (head.epoch == conflict.remote_epoch
                        && head.head_seq == conflict.remote_seq
                        && head.head_hash == conflict.remote_hash)
                        || head.epoch > conflict.remote_epoch
                        || (head.epoch == conflict.remote_epoch
                            && head.head_seq > conflict.remote_seq
                            && matches!(descendant_proof, AncestorProof::Verified { .. }));
                    if is_valid {
                        qualified.push((head, holders, descendant_proof));
                    }
                }
                if qualified.is_empty() {
                    return Err(Error::conflict(
                        "conflicted remote branch source is currently offline or unavailable",
                    ));
                }
                qualified.sort_by(|a, b| {
                    let a_pref = i32::from(a.1.iter().any(|p| p == &conflict.remote_node_id));
                    let b_pref = i32::from(b.1.iter().any(|p| p == &conflict.remote_node_id));
                    b.0.epoch
                        .cmp(&a.0.epoch)
                        .then_with(|| b_pref.cmp(&a_pref))
                        .then_with(|| b.0.head_seq.cmp(&a.0.head_seq))
                });
                let (remote_head, holders, descendant_proof) = qualified.remove(0);

                // Special path for deleted session/MCP tombstone resolved as "cluster".
                if remote_head.deleted {
                    if let Some((used_peer, delete_commit)) =
                        self.fetch_delete_commit(&remote_head, &holders).await
                    {
                        let exp_c = conflict.clone();
                        let streams = self.streams.clone();
                        let sid_owned = stream_id.to_owned();
                        let unblocked = self
                            .db
                            .call(move |s| {
                                assert_membership_gen(&s.conn, mgen)?;
                                let cur = get_conflict(&s.conn, &sid_owned)?;
                                if cur != exp_c {
                                    return Err(Error::conflict("sync conflict state changed concurrently"));
                                }
                                let outcome = match remote_head.kind {
                                    StreamKind::Session => apply_terminal_session_delete(
                                        &s.conn,
                                        &remote_head,
                                        &delete_commit,
                                        descendant_proof,
                                        &used_peer,
                                        Some(&exp_c),
                                    )?,
                                    StreamKind::Mcp => apply_terminal_mcp_delete(
                                        &s.conn,
                                        &remote_head,
                                        &delete_commit,
                                        descendant_proof,
                                        &used_peer,
                                        Some(&exp_c),
                                    )?,
                                    _ => ApplyOutcome::Ignored,
                                };
                                let unblocked = match &outcome {
                                    ApplyOutcome::Applied {
                                        unblocked_conflicts, ..
                                    } => unblocked_conflicts.clone(),
                                    _ => return Err(Error::conflict("could not apply terminal remote state")),
                                };
                                publish_outcome(s, &streams, exp_c.kind, &sid_owned, outcome)?;
                                s.publish_global(
                                    "sync.conflict",
                                    json!({ "stream_id": sid_owned, "resolved": true }),
                                );
                                Ok(unblocked)
                            })
                            .await?;
                        self.notify_conflict_resolved_to_peers(stream_id).await;
                        if !unblocked.is_empty() {
                            let _ = self.repull_streams_by_id(&unblocked, mgen).await;
                        }
                        return Ok(());
                    }
                }

                // Stream full winning epoch page-by-page into SQLite sync_staging_commits (P1-13)!
                self.db
                    .call({
                        let sid = stream_id.to_owned();
                        move |s| {
                            assert_membership_gen(&s.conn, mgen)?;
                            clear_staging_for_stream(&s.conn, &sid)
                        }
                    })
                    .await?;
                let mut cursor = 0i64;
                let mut used_peer = holders[0].clone();
                let mut batch_idx = 0usize;
                while cursor < remote_head.head_seq {
                    let got = self
                        .fetch_commits_page(
                            &holders,
                            batch_idx,
                            stream_id,
                            remote_head.epoch,
                            cursor,
                            Some(remote_head.head_seq),
                        )
                        .await?;
                    let Some((peer_id, batch)) = got else {
                        let _ = self
                            .db
                            .call({
                                let sid = stream_id.to_owned();
                                move |s| clear_staging_for_stream(&s.conn, &sid)
                            })
                            .await;
                        return Err(Error::conflict(
                            "failed to fetch winning commits from online peers",
                        ));
                    };
                    used_peer = peer_id;
                    let target_clone = remote_head.clone();
                    match self
                        .db
                        .call(move |s| {
                            assert_membership_gen(&s.conn, mgen)?;
                            stage_remote_commits_page(&s.conn, &target_clone, &batch)
                        })
                        .await
                    {
                        Ok(next_seq) => {
                            cursor = next_seq;
                            batch_idx += 1;
                        }
                        Err(e) => {
                            let _ = self
                                .db
                                .call({
                                    let sid = stream_id.to_owned();
                                    move |s| clear_staging_for_stream(&s.conn, &sid)
                                })
                                .await;
                            return Err(e);
                        }
                    }
                }

                let sid_owned = stream_id.to_owned();
                let streams = self.streams.clone();
                let exp_c = conflict.clone();
                let res_db = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, mgen)?;
                        let outcome = commit_staged_epoch(
                            &s.conn,
                            &remote_head,
                            &used_peer,
                            true,
                            Some(&exp_c),
                        )?;
                        let unblocked = match &outcome {
                            ApplyOutcome::Applied {
                                unblocked_conflicts, ..
                            } => unblocked_conflicts.clone(),
                            _ => Vec::new(),
                        };
                        publish_outcome(s, &streams, exp_c.kind, &sid_owned, outcome)?;
                        s.publish_global(
                            "sync.conflict",
                            json!({ "stream_id": sid_owned, "resolved": true }),
                        );
                        Ok(unblocked)
                    })
                    .await;
                let _ = self
                    .db
                    .call({
                        let sid = stream_id.to_owned();
                        move |s| clear_staging_for_stream(&s.conn, &sid)
                    })
                    .await;
                let unblocked = res_db?;
                // P1-25: Notify peer holding our superseded conflict so both sides converge unlocked!
                self.notify_conflict_resolved_to_peers(stream_id).await;
                if !unblocked.is_empty() {
                    let _ = self.repull_streams_by_id(&unblocked, mgen).await;
                }
                Ok(())
            }
            _ => Err(Error::bad("resolution must be 'local' or 'cluster'")),
        }
    }

    async fn fetch_conflicted_local_branch(&self, c: &ConflictRecord, mgen: i64) -> Result<()> {
        let online = self.net.online_peer_ids();
        let req = RpcReq::GetHeads {
            stream_ids: vec![c.stream_id.clone()],
        };
        let mut target_head: Option<StreamHead> = None;
        let mut holders = Vec::new();
        for peer_id in online {
            if let Ok((RpcResp::Manifest { heads, .. }, _)) =
                self.rpc(&peer_id, &req, RPC_TIMEOUT_SHORT).await
            {
                for h in heads {
                    if h.stream_id == c.stream_id
                        && h.epoch == c.local_epoch
                        && h.head_seq == c.local_seq
                        && h.head_hash == c.local_hash
                        && h.body_complete
                    {
                        if target_head.is_none() {
                            target_head = Some(h);
                        }
                        holders.push(peer_id.clone());
                    }
                }
            }
        }
        let Some(head) = target_head else {
            return Err(Error::conflict(
                "local branch lacks complete history and no online holder can supply it",
            ));
        };
        self.db
            .call({
                let sid = c.stream_id.clone();
                move |s| {
                    assert_membership_gen(&s.conn, mgen)?;
                    clear_staging_for_stream(&s.conn, &sid)
                }
            })
            .await?;
        let mut cursor = 0i64;
        let mut batch_idx = 0usize;
        while cursor < head.head_seq {
            let got = self
                .fetch_commits_page(
                    &holders,
                    batch_idx,
                    &c.stream_id,
                    c.local_epoch,
                    cursor,
                    Some(c.local_seq),
                )
                .await?;
            let Some((_, batch)) = got else {
                let _ = self
                    .db
                    .call({
                        let sid = c.stream_id.clone();
                        move |s| clear_staging_for_stream(&s.conn, &sid)
                    })
                    .await;
                return Err(Error::conflict(
                    "failed to fetch complete local branch history from online holders",
                ));
            };
            let head_clone = head.clone();
            cursor = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, mgen)?;
                    stage_remote_commits_page(&s.conn, &head_clone, &batch)
                })
                .await?;
            batch_idx += 1;
        }
        // Apply staged local commits cleanly WITHOUT removing the conflict row.
        let sid_owned = c.stream_id.clone();
        self.db
            .call(move |s| {
                assert_membership_gen(&s.conn, mgen)?;
                ledger::commit_staged_branch_preserving_conflict(&s.conn, &head, &sid_owned)
            })
            .await?;
        let _ = self
            .db
            .call({
                let sid = c.stream_id.clone();
                move |s| clear_staging_for_stream(&s.conn, &sid)
            })
            .await;
        Ok(())
    }

    async fn fetch_delete_commit(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
    ) -> Option<(String, CommitWire)> {
        for peer in holders {
            if let Ok((RpcResp::DeleteCommit { commit: Some(c) }, _)) = self
                .rpc(
                    peer,
                    &RpcReq::GetDeleteCommit {
                        stream_id: remote_head.stream_id.clone(),
                        epoch: remote_head.epoch,
                        seq: remote_head.head_seq,
                    },
                    RPC_TIMEOUT_DATA,
                )
                .await
            {
                return Some((peer.clone(), c));
            }
        }
        None
    }

    async fn notify_conflict_resolved_to_peers(&self, stream_id: &str) {
        let sid = stream_id.to_owned();
        let Ok(mut receipts) = self
            .db
            .call(move |s| read_resolutions_for_streams(&s.conn, &[sid]))
            .await
        else {
            return;
        };
        let Some(receipt) = receipts.pop() else {
            return;
        };
        let req = RpcReq::NotifyConflictResolved(receipt);
        let online = self.net.online_peer_ids();
        let req_ref = &req;
        futures_util::future::join_all(
            online
                .iter()
                .map(|peer_id| async move { self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await }),
        )
        .await;
    }

    async fn query_mcp_participants(&self, stream_id: &str) -> Vec<String> {
        let online = self.net.online_peer_ids();
        let req = RpcReq::GetHeads {
            stream_ids: vec![stream_id.to_owned()],
        };
        let replies = futures_util::future::join_all(online.iter().map(|peer_id| {
            let req_ref = &req;
            async move { (peer_id.clone(), self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await) }
        }))
        .await;
        let mut holders = Vec::new();
        for (peer_id, reply) in replies {
            if let Ok((RpcResp::Manifest { heads, .. }, _)) = reply
                && heads
                    .iter()
                    .any(|h| h.stream_id == stream_id && !h.deleted && !h.retired)
            {
                holders.push(peer_id);
            }
        }
        holders
    }

    pub async fn coordinate_mcp_oauth_refresh(
        &self,
        server_id: McpServerId,
        generation: &str,
        rejected_token_hash: Option<[u8; 32]>,
    ) -> Result<RefreshCoordination> {
        use crate::mcp::oauth::{
            PendingRefreshState, credentials_fingerprint, credentials_need_refresh,
        };

        enum ReconciledOAuth {
            GoneOrReauth,
            Rotated,
            Unchanged(Option<ledger::LocalStreamState>),
        }

        fn evaluate_oauth_after_sync(
            conn: &rusqlite::Connection,
            mgen: i64,
            server_id: McpServerId,
            generation: &str,
            stream_id: &str,
            pending: &PendingRefreshState,
            clear_if_same_and_valid: bool,
        ) -> Result<ReconciledOAuth> {
            assert_membership_gen(conn, mgen)?;
            let row: Option<(Option<String>, bool)> = conn
                .prepare_cached(
                    "SELECT coalesce(staged_credentials,credentials), rejected
                     FROM mcp_oauth WHERE server_id=?1 AND generation=?2",
                )?
                .query_row(params![server_id, generation], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let Some((raw_creds, rejected)) = row else {
                return Ok(ReconciledOAuth::GoneOrReauth);
            };
            let Some(new_fp) = credentials_fingerprint(raw_creds.as_deref()) else {
                pending.clear_if_current(conn, server_id, generation)?;
                return Ok(ReconciledOAuth::GoneOrReauth);
            };
            if new_fp != pending.fp
                || (clear_if_same_and_valid
                    && !credentials_need_refresh(raw_creds.as_deref(), rejected))
            {
                pending.clear_if_current(conn, server_id, generation)?;
                return Ok(ReconciledOAuth::Rotated);
            }
            Ok(ReconciledOAuth::Unchanged(stream_head(conn, stream_id)?))
        }

        let mgen = self.net.membership_gen();
        let stream_id = server_id.to_string();
        let gen_owned = generation.to_owned();
        let (shared, conflicted, st, raw_creds, pending_raw) = self
            .db
            .call({
                let sid = stream_id.clone();
                let gen_q = gen_owned.clone();
                move |s| {
                    assert_membership_gen(&s.conn, mgen)?;
                    let shared: bool = s
                        .conn
                        .prepare_cached("SELECT shared FROM mcp_servers WHERE id=?")?
                        .query_row([server_id], |r| r.get(0))
                        .optional()?
                        .unwrap_or(false);
                    let conflicted = ledger::has_conflict(&s.conn, &sid)?;
                    let st = stream_head(&s.conn, &sid)?;
                    let oauth_row: Option<(Option<String>, Option<String>)> = s
                        .conn
                        .prepare_cached(
                            "SELECT coalesce(staged_credentials,credentials), pending_refresh
                             FROM mcp_oauth WHERE server_id=?1 AND generation=?2",
                        )?
                        .query_row(params![server_id, gen_q], |r| Ok((r.get(0)?, r.get(1)?)))
                        .optional()?;
                    let (raw_creds, pending_raw) = oauth_row.unwrap_or((None, None));
                    Ok((shared, conflicted, st, raw_creds, pending_raw))
                }
            })
            .await?;
        if conflicted {
            return Err(Error::conflict(
                "MCP stream has an unresolved sync conflict; resolve before refreshing OAuth",
            ));
        }
        if !shared {
            return Ok(RefreshCoordination::Local);
        }
        let Some(current_fp) = credentials_fingerprint(raw_creds.as_deref()) else {
            return Ok(RefreshCoordination::Delegated {
                reauth_required: true,
            });
        };
        let active_pending = pending_raw
            .as_deref()
            .and_then(|raw| serde_json::from_str::<PendingRefreshState>(raw).ok())
            .filter(|pr| pr.fp == current_fp);

        let (delegation, cur_st) = if let Some(pr) = active_pending {
            if self.net.is_peer_online(&pr.coordinator) {
                let _ = self.sync_with_peers(Some(&pr.coordinator), None).await;
            }
            let _ = self.sync_with_peers(None, None).await;
            let remote_holders = self.query_mcp_participants(&stream_id).await;
            let recheck = self
                .db
                .call({
                    let sid = stream_id.clone();
                    let gen_q = gen_owned.clone();
                    let pending = pr.clone();
                    move |s| {
                        evaluate_oauth_after_sync(
                            &s.conn,
                            mgen,
                            server_id,
                            &gen_q,
                            &sid,
                            &pending,
                            false,
                        )
                    }
                })
                .await?;
            match recheck {
                ReconciledOAuth::GoneOrReauth => {
                    return Ok(RefreshCoordination::Delegated {
                        reauth_required: true,
                    });
                }
                ReconciledOAuth::Rotated => {
                    return Ok(RefreshCoordination::Delegated {
                        reauth_required: false,
                    });
                }
                ReconciledOAuth::Unchanged(st_now) => {
                    if !remote_holders.iter().any(|p| p == &pr.coordinator) {
                        return Ok(RefreshCoordination::PendingConfirmation);
                    }
                    (pr, st_now)
                }
            }
        } else {
            let mut participants = self.query_mcp_participants(&stream_id).await;
            participants.push(self.net.local_node_id().to_owned());
            participants.sort();
            participants.dedup();
            let chosen = participants.first().cloned().unwrap_or_default();
            if chosen == self.net.local_node_id() || chosen.is_empty() {
                return Ok(RefreshCoordination::Local);
            }
            let pending = PendingRefreshState { coordinator: chosen, fp: current_fp };
            let pending_json = serde_json::to_string(&pending)
                .map_err(|e| Error::internal(e.to_string()))?;
            // Serialize the durable delegation claim with local and inbound issuer calls.
            let gate = self.db
                .call({
                    let generation = gen_owned.clone();
                    move |s| Ok(s.mcp.refresh_gate(&generation))
                })
                .await?;
            let _claim_guard = gate.lock_owned().await;
            let claim = self.db
                .write(
                    {
                        let gen_q = gen_owned.clone();
                        let fp = pending.fp.clone();
                        let sid = stream_id.clone();
                        move |conn| {
                            assert_membership_gen(conn, mgen)?;
                            let cur: Option<Option<String>> = conn
                                .prepare_cached(
                                    "SELECT coalesce(staged_credentials,credentials)
                                     FROM mcp_oauth WHERE server_id=?1 AND generation=?2",
                                )?
                                .query_row(params![server_id, gen_q], |r| r.get(0))
                                .optional()?;
                            let Some(raw) = cur else {
                                return Ok(Err(RefreshCoordination::Delegated { reauth_required: true }));
                            };
                            let latest_fp = credentials_fingerprint(raw.as_deref());
                            if latest_fp.as_deref() != Some(fp.as_str()) {
                                return Ok(Err(RefreshCoordination::Delegated {
                                    reauth_required: latest_fp.is_none(),
                                }));
                            }
                            ledger::assert_no_conflict(conn, &sid)?;
                            let changed = conn.execute(
                                "UPDATE mcp_oauth SET pending_refresh=?1 WHERE server_id=?2 AND generation=?3 AND pending_refresh IS ?4",
                                params![pending_json, server_id, gen_q, pending_raw],
                            )?;
                            if changed == 0 {
                                return Ok(Err(RefreshCoordination::PendingConfirmation));
                            }
                            Ok(Ok(stream_head(conn, &sid)?))
                        }
                    },
                    |_, res| Ok(res),
                )
                .await?;
            match claim {
                Ok(head) => (pending, head.or(st)),
                Err(outcome) => return Ok(outcome),
            }
        };

        let coordinator = &delegation.coordinator;
        let (expected_epoch, expected_seq, expected_hash) = match cur_st {
            Some(l) => (l.epoch, l.synced_seq, Some(l.synced_hash)),
            None => (0, 0, None),
        };

        let rpc_res = self
            .rpc(
                coordinator,
                &RpcReq::OAuthRefresh {
                    server_id,
                    generation: gen_owned.clone(),
                    expected_epoch,
                    expected_seq,
                    expected_hash,
                    rejected_token_hash,
                },
                RPC_TIMEOUT_OAUTH,
            )
            .await;

        match rpc_res {
            Ok((
                RpcResp::OAuthRefreshResult {
                    status,
                    head,
                    commits,
                    has_more,
                    error,
                },
                _,
            )) => {
                if status == OAuthRefreshStatus::Conflicted {
                    return Err(Error::conflict(
                        error.unwrap_or_else(|| "MCP stream conflicted on coordinator".into()),
                    ));
                }
                let Some(target_head) = head else {
                    if status == OAuthRefreshStatus::ReauthRequired {
                        self.db
                            .write(
                                {
                                    let gen_q = gen_owned.clone();
                                    move |conn| {
                                        assert_membership_gen(conn, mgen)?;
                                        delegation.clear_if_current(conn, server_id, &gen_q)?;
                                        Ok(())
                                    }
                                },
                                |_, ()| Ok(()),
                            )
                            .await?;
                        return Ok(RefreshCoordination::Delegated {
                            reauth_required: true,
                        });
                    }
                    // Coordinator lagged behind caller: push heads and sync, keeping pending_refresh
                    // rather than falling back to local refresh.
                    self.broadcast_heads(std::slice::from_ref(&stream_id), None)
                        .await;
                    let _ = self.sync_with_peers(Some(coordinator), None).await;
                    return Ok(RefreshCoordination::PendingConfirmation);
                };

                let lock = self.stream_locks.lock_for(stream_id.clone());
                let _guard = lock.lock().await;
                let coord_id = coordinator.clone();
                let streams = self.streams.clone();
                let need_full_pull = self
                    .db
                    .call({
                        let head_clone = target_head.clone();
                        let sid_clone = stream_id.clone();
                        let cc = coord_id.clone();
                        move |s| {
                            assert_membership_gen(&s.conn, mgen)?;
                            if let Some(l) = stream_head(&s.conn, &sid_clone)?
                                && l.epoch == head_clone.epoch
                                && l.synced_seq == head_clone.head_seq
                                && l.synced_hash == head_clone.head_hash
                            {
                                return Ok(false);
                            }
                            let contiguous = commits.first().is_some_and(|first| {
                                first.epoch == head_clone.epoch
                                    && stream_head(&s.conn, &sid_clone)
                                        .ok()
                                        .flatten()
                                        .is_some_and(|l| {
                                            l.epoch == head_clone.epoch
                                                && first.seq == l.synced_seq + 1
                                        })
                            });
                            if contiguous && !has_more && head_clone.epoch == expected_epoch {
                                let outcome = apply_remote_commits_page(
                                    &s.conn,
                                    &head_clone,
                                    &commits,
                                    &cc,
                                    true,
                                )?;
                                publish_outcome(
                                    s,
                                    &streams,
                                    StreamKind::Mcp,
                                    &sid_clone,
                                    outcome,
                                )?;
                                Ok(false)
                            } else {
                                Ok(true)
                            }
                        }
                    })
                    .await?;

                if need_full_pull {
                    let (pull_res, _) = self
                        .pull_and_apply_stream_locked(&target_head, &[coord_id], false, mgen)
                        .await?;
                    if pull_res == PullResult::Conflict {
                        return Err(Error::conflict(
                            "OAuth delegation encountered sync conflict",
                        ));
                    }
                }
                let sid_check = stream_id.clone();
                let converged = self
                    .db
                    .call(move |s| {
                        Ok(stream_head(&s.conn, &sid_check)?.is_some_and(|l| {
                            l.epoch == target_head.epoch
                                && l.synced_seq == target_head.head_seq
                                && l.synced_hash == target_head.head_hash
                        }))
                    })
                    .await?;
                if !converged {
                    return Err(Error::conflict("failed to converge delegated OAuth state"));
                }
                if status == OAuthRefreshStatus::ReauthRequired {
                    self.db
                        .write(
                            move |conn| {
                                assert_membership_gen(conn, mgen)?;
                                delegation.clear_if_current(conn, server_id, &gen_owned)?;
                                Ok(())
                            },
                            |_, ()| Ok(()),
                        )
                        .await?;
                    return Ok(RefreshCoordination::Delegated {
                        reauth_required: true,
                    });
                }
                let post = self
                    .db
                    .call(move |s| {
                        evaluate_oauth_after_sync(
                            &s.conn,
                            mgen,
                            server_id,
                            &gen_owned,
                            &stream_id,
                            &delegation,
                            true,
                        )
                    })
                    .await?;
                match post {
                    ReconciledOAuth::GoneOrReauth => Ok(RefreshCoordination::Delegated {
                        reauth_required: true,
                    }),
                    ReconciledOAuth::Rotated => Ok(RefreshCoordination::Delegated {
                        reauth_required: false,
                    }),
                    ReconciledOAuth::Unchanged(_) => Ok(RefreshCoordination::PendingConfirmation),
                }
            }
            _ => {
                // Coordinator disconnected/timed out: keep pending_refresh persisted unless credentials
                // actually rotated or authorization changed. Ordinary config commits never clear it.
                let _ = self.sync_with_peers(Some(coordinator), None).await;
                let _ = self.sync_with_peers(None, None).await;
                let post = self
                    .db
                    .call(move |s| {
                        evaluate_oauth_after_sync(
                            &s.conn,
                            mgen,
                            server_id,
                            &gen_owned,
                            &stream_id,
                            &delegation,
                            false,
                        )
                    })
                    .await?;
                match post {
                    ReconciledOAuth::GoneOrReauth => Ok(RefreshCoordination::Delegated {
                        reauth_required: true,
                    }),
                    ReconciledOAuth::Rotated => Ok(RefreshCoordination::Delegated {
                        reauth_required: false,
                    }),
                    ReconciledOAuth::Unchanged(_) => Ok(RefreshCoordination::PendingConfirmation),
                }
            }
        }
    }
}

fn upsert_pseudo_session_head(conn: &rusqlite::Connection, h: &StreamHead) -> Result<()> {
    let Some(raw_snap) = h.head_snapshot.as_deref() else {
        return Ok(());
    };
    let snap: ledger::SessionSnapshot = serde_json::from_str(raw_snap)
        .map_err(|e| Error::bad(format!("invalid session head snapshot: {e}")))?;
    let has_model: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_models WHERE id=?)")?
        .query_row([snap.model_id], |r| r.get(0))?;
    if !has_model {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO sessions(id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id,input_queue_cursor,needs_model)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO NOTHING",
        params![
            snap.id,
            snap.title,
            snap.created_at,
            snap.updated_at,
            snap.status,
            snap.reasoning_effort,
            snap.last_error,
            snap.model_id,
            snap.input_queue_cursor,
            snap.needs_model
        ],
    )?;
    conn.execute(
        "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
         VALUES(?1,'session',?2,?3,?4,0,0,?5,0,0,0,0,?6,?7) ON CONFLICT(stream_id) DO NOTHING",
        params![
            h.stream_id,
            h.epoch,
            h.head_seq,
            h.head_hash.as_slice(),
            [0u8; 32].as_slice(),
            raw_snap,
            h.updated_at
        ],
    )?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PullResult {
    Updated,
    UpToDate,
    Blocked,
    Conflict,
    Ignored,
}

fn abort_active_session_worker(s: &mut Store, streams: &Hub, sid: SessionId) {
    if let Some(ctrl) = s.active.remove(&sid) {
        ctrl.soft.cancel();
        ctrl.hard.cancel();
    }
    s.pending_wakes.remove(&sid);
    crate::codemode::stop_session(s, sid);
    s.refresh_local_presence(sid);
    s.bump_run_gen(sid);
    s.bump_reset_gen(sid);
    streams.remove_session(sid);
}

fn publish_outcome(
    s: &mut Store,
    streams: &Hub,
    kind: StreamKind,
    stream_id: &str,
    outcome: ApplyOutcome,
) -> Result<()> {
    match outcome {
        ApplyOutcome::Applied {
            updated,
            session_id,
            session_reset_epoch,
            deleted_session,
            new_items,
            mcp_changes,
            resolved_conflict,
            ..
        } => {
            for ch in mcp_changes {
                s.mcp.apply_sync_change(ch);
            }
            if resolved_conflict {
                s.publish_global(
                    "sync.conflict",
                    json!({ "stream_id": stream_id, "resolved": true }),
                );
            }
            if !updated {
                return Ok(());
            }
            if let Some(sid) = session_id {
                if deleted_session {
                    abort_active_session_worker(s, streams, sid);
                    s.mcp.forget_session(sid);
                    s.channels.remove(&sid);
                    s.wake_sequences.remove(&sid);
                    s.publish_global("session.deleted", json!({ "id": sid }));
                } else {
                    if session_reset_epoch.is_some() {
                        abort_active_session_worker(s, streams, sid);
                    }
                    s.wake_waiters(sid);
                    if let Some(epoch) = session_reset_epoch {
                        // P1-22 / P2 #1: notify WebUI with monotonic history_rev so it drops cursor and resets!
                        if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                            let history_rev = session_history_rev(&s.conn, sid).unwrap_or(0);
                            crate::events::Notice::publish(
                                &ch,
                                "session.reset",
                                json!({ "epoch": epoch, "history_rev": history_rev }),
                            );
                        }
                    } else {
                        for id in new_items {
                            let _ = s.item_saved(sid, id);
                        }
                    }
                    if let Ok(info) = session(&s.conn, sid) {
                        let view = s.session_view(info);
                        let data = json!(view);
                        if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                            crate::events::Notice::publish(&ch, "session.updated", data.clone());
                        }
                        s.publish_global("session.created", data.clone());
                        s.publish_global("session.updated", data);
                    }
                }
            } else if matches!(kind, StreamKind::Provider | StreamKind::Model) {
                s.publish_global("catalog.updated", json!({ "id": stream_id }));
            } else if kind == StreamKind::Mcp {
                s.publish_global("mcp.updated", json!({ "id": stream_id }));
            }
            Ok(())
        }
        ApplyOutcome::Conflict { newly_recorded } => {
            if newly_recorded {
                s.publish_global(
                    "sync.conflict",
                    json!({ "stream_id": stream_id, "resolved": false }),
                );
            }
            Ok(())
        }
        ApplyOutcome::BlockedOnDependency | ApplyOutcome::Ignored | ApplyOutcome::StaleProof => {
            Ok(())
        }
    }
}
