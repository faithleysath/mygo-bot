use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    io::Seek,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use axum::{body::Bytes, http::StatusCode};
use futures_util::{StreamExt, stream};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex as AsyncMutex, Notify, Semaphore},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    cluster::{
        ClusterSignal,
        identity::{
            MAX_PEX_PEERS, PeerInfo, assert_membership_gen, list_peers, load_local_node,
            sample_peers_for_pex, self_peer_info, upsert_peer_if_gen,
        },
        ledger::{
            self, CALLS_SCOPE, CatalogDelta, McpServerWire, SessionDelta, apply_catalog_delta,
            apply_session_delta, is_session_watched, list_watched_sessions, load_mcp_wire,
            mark_session_watched, merge_mcp_server_wire, read_catalog_delta,
            read_session_delta, read_vector, session_branch_complete, session_scope,
        },
        net::{
            ClusterNet, RPC_BODY_MAX, RPC_READ_DEADLINE, RPC_REQ_HEADER_MAX, RPC_RESP_HEADER_MAX,
            RPC_WRITE_DEADLINE, read_frame_bounded, write_frame_timed,
        },
    },
    events::Hub,
    id::{AttachmentId, ItemId, McpServerId, SessionId},
    store::{Db, Store, session},
};

const ATT_CHUNK_SIZE: usize = 256 * 1024;
const MAX_ITEM_PAYLOAD_BYTES: u64 = 32 * 1024 * 1024;
const MAX_CHUNK_CONCURRENCY: usize = 4;
const MAX_ATT_BYTE_PERMITS: usize = 512; // 128 MiB across temporary files (256 KiB units)
const ATTACHMENT_HASH_BATCH: usize = 64;
const MAX_ATTACHMENT_PROBES: usize = 8;
const MAX_INBOUND_QUEUE: usize = 4096;
const INBOUND_WORKERS: usize = 4;
const MAX_BLOCKING_CODEC: usize = 4;
const OFFLOAD_BYTES_THRESHOLD: usize = 64 * 1024;
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(30);
const RPC_TIMEOUT_SHORT: Duration = Duration::from_secs(10);
const RPC_TIMEOUT_DATA: Duration = Duration::from_secs(20);
const RPC_TIMEOUT_OAUTH: Duration = Duration::from_secs(45);

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcReq {
    ExchangePeers {
        self_info: PeerInfo,
        peers: Vec<PeerInfo>,
    },
    NotifyDirty {
        catalog: bool,
        sessions: Vec<SessionId>,
    },
    CatalogSync {
        #[serde(default)]
        peer_db_epoch: String,
        #[serde(default)]
        since_rev: i64,
        #[serde(default)]
        calls_vector: BTreeMap<String, i64>,
    },
    SessionSync {
        session_id: SessionId,
        #[serde(default)]
        vector: BTreeMap<String, i64>,
    },
    GetItemPayloadChunk {
        session_id: SessionId,
        item_id: ItemId,
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
    HasMcpServer {
        server_id: McpServerId,
    },
    OAuthRefresh {
        server_id: McpServerId,
        generation: String,
        #[serde(default)]
        caller_server: Option<McpServerWire>,
        #[serde(default)]
        rejected_token_hash: Option<[u8; 32]>,
        #[serde(default)]
        hops_left: u8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthRefreshStatus {
    Refreshed,
    UpToDate,
    ReauthRequired,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcResp {
    Peers {
        self_info: PeerInfo,
        peers: Vec<PeerInfo>,
    },
    Ack,
    CatalogDelta(CatalogDelta),
    SessionDelta(SessionDelta),
    ItemPayloadChunk,
    AttachmentsAvailable {
        hashes: Vec<[u8; 32]>,
    },
    AttachmentChunk,
    McpServerPresence {
        active: bool,
    },
    OAuthRefreshResult {
        status: OAuthRefreshStatus,
        server: Option<McpServerWire>,
        error: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize)]
pub struct SyncSummary {
    pub peers_queried: usize,
    pub catalog_rows_updated: usize,
    pub sessions_updated: usize,
}

pub enum RefreshCoordination {
    Local,
    Delegated { reauth_required: bool },
    PendingConfirmation,
}

struct LockTable<K> {
    map: Mutex<HashMap<K, Weak<AsyncMutex<()>>>>,
}

impl<K> Default for LockTable<K> {
    fn default() -> Self {
        Self {
            map: Mutex::default(),
        }
    }
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

enum InboundWork {
    Catalog { mgen: i64, peer_id: String },
    Session { mgen: i64, sid: SessionId, peer_id: String },
}

#[derive(Default)]
struct InboundState {
    catalog_peers: HashSet<(i64, String)>,
    catalog_fifo: VecDeque<(i64, String)>,
    sessions: HashMap<(i64, SessionId), Vec<String>>,
    session_fifo: VecDeque<(i64, SessionId)>,
    total_session_entries: usize,
}

#[derive(Default)]
struct RetryState {
    catalog_dirty: bool,
    sessions: HashSet<SessionId>,
    attempts: u32,
    due: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct MissingAttachment {
    id: AttachmentId,
    hash: [u8; 32],
    len: i64,
}

fn fill_attachment_aliases(
    conn: &rusqlite::Connection,
    att: &MissingAttachment,
    rowid: i64,
) -> Result<()> {
    conn.prepare_cached(
        "UPDATE attachments SET data=(SELECT data FROM attachments WHERE rowid=?1)
         WHERE content_hash=?2 AND byte_len=?3 AND data IS NULL",
    )?
    .execute(params![rowid, att.hash, att.len])?;
    Ok(())
}

fn missing_attachment(
    conn: &rusqlite::Connection,
    id: AttachmentId,
) -> Result<Option<MissingAttachment>> {
    let (hash, len, has_data) = conn
        .prepare_cached(
            "SELECT content_hash,byte_len,data IS NOT NULL FROM attachments WHERE id=?",
        )?
        .query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, bool>(2)?)))?;
    if has_data {
        return Ok(None);
    }
    if !(1..=crate::media::MAX_ATTACHMENT_BYTES as i64).contains(&len) {
        return Err(Error::bad("attachment exceeds allowed byte budget"));
    }
    let att = MissingAttachment { id, hash, len };
    let source = conn
        .prepare_cached(
            "SELECT rowid FROM attachments WHERE content_hash=?1 AND data IS NOT NULL AND length(data)=?2 LIMIT 1",
        )?
        .query_row(params![hash, len], |r| r.get(0))
        .optional()?;
    if let Some(rowid) = source {
        fill_attachment_aliases(conn, &att, rowid)?;
        return Ok(None);
    }
    Ok(Some(att))
}

pub struct SyncEngine {
    db: Db,
    client: reqwest::Client,
    streams: Hub,
    net: Arc<ClusterNet>,
    signal: Arc<ClusterSignal>,
    catalog_locks: LockTable<String>,
    session_locks: LockTable<(SessionId, String)>,
    mcp_locks: LockTable<McpServerId>,
    att_locks: LockTable<[u8; 32]>,
    att_byte_budget: Arc<Semaphore>,
    attachment_probes: Semaphore,
    blocking_sem: Arc<Semaphore>,
    inbound: Mutex<InboundState>,
    inbound_notify: Notify,
    outbound_retries: Mutex<RetryState>,
    sync_now_gate: AsyncMutex<()>,
}

impl SyncEngine {
    pub fn new(
        db: Db,
        client: reqwest::Client,
        streams: Hub,
        net: Arc<ClusterNet>,
        signal: Arc<ClusterSignal>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            client,
            streams,
            net,
            signal,
            catalog_locks: LockTable::default(),
            session_locks: LockTable::default(),
            mcp_locks: LockTable::default(),
            att_locks: LockTable::default(),
            att_byte_budget: Arc::new(Semaphore::new(MAX_ATT_BYTE_PERMITS)),
            attachment_probes: Semaphore::new(MAX_ATTACHMENT_PROBES),
            blocking_sem: Arc::new(Semaphore::new(MAX_BLOCKING_CODEC)),
            inbound: Mutex::default(),
            inbound_notify: Notify::new(),
            outbound_retries: Mutex::default(),
            sync_now_gate: AsyncMutex::new(()),
        })
    }

    pub fn on_membership_switched(&self) {
        let mut st = self.inbound.lock().unwrap();
        st.catalog_peers.clear();
        st.catalog_fifo.clear();
        st.sessions.clear();
        st.session_fifo.clear();
        st.total_session_entries = 0;
        drop(st);
        *self.outbound_retries.lock().unwrap() = RetryState::default();
    }

    async fn run_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let permit = self
            .blocking_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::internal("blocking worker pool closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        })
        .await
        .map_err(|e| Error::internal(format!("blocking task panicked: {e}")))
    }

    async fn encode_resp_owned(&self, resp: RpcResp) -> Result<Vec<u8>> {
        let needs_offload = match &resp {
            RpcResp::CatalogDelta(d) => {
                d.providers.len() + d.models.len() + d.mcp_servers.len() + d.sessions.len() + d.calls.len()
                    > 24
            }
            RpcResp::SessionDelta(d) => {
                d.items.len() + d.context_nodes.len() > 24
                    || d.items.iter().map(|i| i.payload.len()).sum::<usize>()
                        > OFFLOAD_BYTES_THRESHOLD
            }
            RpcResp::OAuthRefreshResult { server: Some(s), .. } => {
                s.config_json.len()
                    + s.oauth
                        .as_ref()
                        .and_then(|o| o.credentials.as_ref())
                        .map_or(0, String::len)
                    > OFFLOAD_BYTES_THRESHOLD
            }
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
        let out_eng = self.clone();
        let out_stop = stopping.clone();
        workers.spawn(async move {
            let mut next_anti_entropy = Instant::now() + ANTI_ENTROPY_INTERVAL;
            loop {
                let next_retry = out_eng.next_retry_deadline();
                let sleep_until = next_anti_entropy.min(next_retry.unwrap_or(next_anti_entropy));
                tokio::select! {
                    _ = out_stop.cancelled() => break,
                    _ = out_eng.signal.notified() => {
                        let batch = out_eng.signal.take();
                        if batch.self_meta {
                            out_eng.broadcast_peers().await;
                        }
                        if batch.full_sync {
                            let _ = out_eng.sync_with_peers(None, None).await;
                        } else if batch.catalog || !batch.sessions.is_empty() {
                            out_eng.broadcast_dirty(batch.catalog, batch.sessions, None).await;
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
                        if let Some((cat, sids)) = out_eng.pop_due_retry(now) {
                            out_eng.broadcast_dirty(cat, sids, None).await;
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

        for _ in 0..INBOUND_WORKERS {
            let eng = self.clone();
            let stop = stopping.clone();
            workers.spawn(async move {
                loop {
                    if stop.is_cancelled() {
                        break;
                    }
                    if let Some(work) = eng.pop_inbound_work() {
                        eng.process_inbound_work(work).await;
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
        self.outbound_retries.lock().unwrap().due
    }

    fn pop_due_retry(&self, now: Instant) -> Option<(bool, Vec<SessionId>)> {
        let mut st = self.outbound_retries.lock().unwrap();
        let due = st.due?;
        if due > now {
            return None;
        }
        st.due = None;
        let cat = std::mem::take(&mut st.catalog_dirty);
        let sids: Vec<SessionId> = st.sessions.drain().collect();
        if !cat && sids.is_empty() {
            return None;
        }
        Some((cat, sids))
    }

    fn schedule_retry_dirty(&self, catalog: bool, sessions: &[SessionId]) {
        if !catalog && sessions.is_empty() {
            return;
        }
        let mut st = self.outbound_retries.lock().unwrap();
        st.catalog_dirty |= catalog;
        for &sid in sessions {
            if st.sessions.len() < MAX_INBOUND_QUEUE {
                st.sessions.insert(sid);
            }
        }
        st.attempts = st.attempts.saturating_add(1).min(5);
        let delay = match st.attempts {
            1 => 1,
            2 => 2,
            3 => 4,
            _ => 15,
        };
        let due = Instant::now() + Duration::from_secs(delay);
        st.due = Some(st.due.map_or(due, |prev| prev.min(due)));
    }

    fn enqueue_inbound_dirty(
        &self,
        mgen: i64,
        from_peer: String,
        catalog: bool,
        sessions: Vec<SessionId>,
    ) {
        if self.net.membership_gen() != mgen {
            return;
        }
        let mut st = self.inbound.lock().unwrap();
        if self.net.membership_gen() != mgen {
            return;
        }
        if catalog && st.catalog_peers.insert((mgen, from_peer.clone())) {
            st.catalog_fifo.push_back((mgen, from_peer.clone()));
        }
        for sid in sessions {
            let key = (mgen, sid);
            if st.total_session_entries >= MAX_INBOUND_QUEUE && !st.sessions.contains_key(&key) {
                continue;
            }
            let is_new = !st.sessions.contains_key(&key);
            let list = st.sessions.entry(key).or_default();
            if !list.contains(&from_peer) {
                list.push(from_peer.clone());
                st.total_session_entries += 1;
            }
            if is_new {
                st.session_fifo.push_back(key);
            }
        }
        drop(st);
        self.inbound_notify.notify_waiters();
    }

    fn pop_inbound_work(&self) -> Option<InboundWork> {
        let mut st = self.inbound.lock().unwrap();
        if let Some((mgen, peer_id)) = st.catalog_fifo.pop_front() {
            st.catalog_peers.remove(&(mgen, peer_id.clone()));
            return Some(InboundWork::Catalog { mgen, peer_id });
        }
        while let Some((mgen, sid)) = st.session_fifo.pop_front() {
            if let Some(mut peers) = st.sessions.remove(&(mgen, sid))
                && let Some(peer_id) = peers.pop()
            {
                st.total_session_entries = st.total_session_entries.saturating_sub(1);
                if !peers.is_empty() {
                    st.sessions.insert((mgen, sid), peers);
                    st.session_fifo.push_back((mgen, sid));
                }
                return Some(InboundWork::Session { mgen, sid, peer_id });
            }
        }
        None
    }

    async fn process_inbound_work(&self, work: InboundWork) {
        let cur_mgen = self.net.membership_gen();
        match work {
            InboundWork::Catalog { mgen, peer_id } => {
                if mgen != cur_mgen {
                    return;
                }
                if let Ok((changed, _, _)) = self.pull_catalog_from_peer(&peer_id, mgen).await
                    && changed
                    && self.net.membership_gen() == cur_mgen
                {
                    self.broadcast_dirty(true, Vec::new(), Some(&peer_id)).await;
                }
            }
            InboundWork::Session { mgen, sid, peer_id } => {
                if mgen != cur_mgen {
                    return;
                }
                let watched = self
                    .db
                    .call(move |s| is_session_watched(&s.conn, sid))
                    .await
                    .unwrap_or(false);
                if !watched {
                    return;
                }
                if let Ok((true, _)) = self.pull_session_from_peer(sid, &peer_id, mgen).await
                    && self.net.membership_gen() == cur_mgen
                {
                    self.broadcast_dirty(false, vec![sid], Some(&peer_id)).await;
                }
            }
        }
    }

    async fn rpc(&self, peer_id: &str, req: &RpcReq, timeout: Duration) -> Result<(RpcResp, Vec<u8>)> {
        let bytes = serde_json::to_vec(req).map_err(|e| Error::internal(e.to_string()))?;
        self.rpc_encoded(peer_id, bytes.into(), timeout).await
    }

    async fn rpc_encoded(
        &self,
        peer_id: &str,
        req_bytes: Bytes,
        timeout: Duration,
    ) -> Result<(RpcResp, Vec<u8>)> {
        let Some(conn) = self.net.get_connection(peer_id) else {
            return Err(Error::conflict(format!("peer {peer_id} is offline")));
        };
        if req_bytes.len() > RPC_REQ_HEADER_MAX {
            return Err(Error::bad("RPC request header exceeds bound"));
        }
        let byte_budget = self.net.rpc_byte_budget().clone();
        let (hdr, body, _permits) = tokio::time::timeout(timeout, async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::internal(format!("open RPC stream failed: {e}")))?;
            write_frame_timed(&mut send, req_bytes, None, RPC_WRITE_DEADLINE).await?;
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
                    let _ = write_frame_timed(
                        &mut send,
                        hdr.into(),
                        bin.map(Bytes::from),
                        RPC_WRITE_DEADLINE,
                    )
                    .await;
                }
            }
            Err(err) => {
                tracing::warn!(peer = %from_peer, status = %err.0, error = %err.1, "cluster RPC failed");
                if let Ok(hdr) = serde_json::to_vec(&RpcResp::Error { message: err.1 }) {
                    let _ =
                        write_frame_timed(&mut send, hdr.into(), None, RPC_WRITE_DEADLINE).await;
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
            RpcReq::ExchangePeers {
                self_info,
                mut peers,
            } => {
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
                        let peers = list_peers(&s.conn)?;
                        s.presence.set_known_peer_count(peers.len());
                        let my_peers = sample_peers_for_pex(peers, MAX_PEX_PEERS);
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
            RpcReq::NotifyDirty { catalog, sessions } => {
                self.enqueue_inbound_dirty(
                    conn_gen,
                    from_peer.to_owned(),
                    catalog,
                    sessions.into_iter().take(256).collect(),
                );
                Ok((RpcResp::Ack, None))
            }
            RpcReq::CatalogSync {
                peer_db_epoch,
                since_rev,
                calls_vector,
            } => {
                let delta = self
                    .db
                    .call(move |s| {
                        let local = load_local_node(&s.conn)?;
                        if local.membership_gen != conn_gen {
                            return Err(Error::conflict("membership changed"));
                        }
                        let effective_rev = if peer_db_epoch == local.db_epoch {
                            since_rev.max(0)
                        } else {
                            0
                        };
                        read_catalog_delta(&s.conn, effective_rev, &calls_vector)
                    })
                    .await?;
                Ok((RpcResp::CatalogDelta(delta), None))
            }
            RpcReq::SessionSync { session_id, vector } => {
                let delta = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, conn_gen)?;
                        read_session_delta(&s.conn, session_id, &vector)
                    })
                    .await?;
                Ok((RpcResp::SessionDelta(delta), None))
            }
            RpcReq::GetItemPayloadChunk {
                session_id,
                item_id,
                offset,
                len,
            } => {
                let req_len = usize::try_from(len).unwrap_or(0);
                if req_len == 0 || req_len > ATT_CHUNK_SIZE || offset > MAX_ITEM_PAYLOAD_BYTES {
                    return Err(Error::bad("invalid item payload chunk request"));
                }
                let chunk = self
                    .db
                    .call(move |s| {
                        assert_membership_gen(&s.conn, conn_gen)?;
                        let deleted: bool = s
                            .conn
                            .prepare_cached(
                                "SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)",
                            )?
                            .query_row([session_id], |r| r.get(0))?;
                        if deleted {
                            return Err(rusqlite::Error::QueryReturnedNoRows.into());
                        }
                        let off_i64 = i64::try_from(offset)
                            .map_err(|_| Error::bad("invalid item payload chunk offset"))?;
                        let len_i64 = i64::try_from(req_len)
                            .map_err(|_| Error::bad("invalid item payload chunk length"))?;
                        let (total_len, chunk): (i64, Vec<u8>) = s
                            .conn
                            .prepare_cached(
                                "SELECT length(CAST(payload AS BLOB)),
                                        substr(CAST(payload AS BLOB), ?3, ?4)
                                 FROM items WHERE id=?1 AND session_id=?2",
                            )?
                            .query_row(
                                params![item_id, session_id, off_i64 + 1, len_i64],
                                |r| Ok((r.get(0)?, r.get(1)?)),
                            )?;
                        if off_i64 >= total_len || chunk.is_empty() {
                            return Err(Error::bad("item payload chunk out of range"));
                        }
                        Ok(chunk)
                    })
                    .await?;
                Ok((RpcResp::ItemPayloadChunk, Some(chunk)))
            }
            RpcReq::HasAttachments { hashes } => {
                let mut limited = hashes;
                limited.truncate(ATTACHMENT_HASH_BATCH);
                let available = self
                    .db
                    .call(move |s| {
                        if limited.is_empty() {
                            return Ok(Vec::new());
                        }
                        let values = std::iter::repeat_n("(?)", limited.len())
                            .collect::<Vec<_>>()
                            .join(",");
                        let sql = format!(
                            "WITH requested(hash) AS (VALUES {values}) SELECT hash FROM requested
                             WHERE EXISTS(SELECT 1 FROM attachments WHERE content_hash=hash AND data IS NOT NULL)"
                        );
                        let mut stmt = s.conn.prepare_cached(&sql)?;
                        let rows =
                            stmt.query_map(rusqlite::params_from_iter(limited), |r| r.get(0))?;
                        Ok(rows.collect::<rusqlite::Result<_>>()?)
                    })
                    .await?;
                Ok((
                    RpcResp::AttachmentsAvailable { hashes: available },
                    None,
                ))
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
            RpcReq::HasMcpServer { server_id } => {
                let active = self
                    .db
                    .call(move |s| {
                        Ok(s.conn
                            .prepare_cached(
                                "SELECT EXISTS(SELECT 1 FROM mcp_servers WHERE id=?1 AND shared=1 AND deleted=0)",
                            )?
                            .query_row([server_id], |r| r.get::<_, bool>(0))?)
                    })
                    .await?;
                Ok((RpcResp::McpServerPresence { active }, None))
            }
            RpcReq::OAuthRefresh {
                server_id,
                generation,
                caller_server,
                rejected_token_hash,
                hops_left,
            } => {
                let lock = self.mcp_locks.lock_for(server_id);
                let _guard = lock.lock().await;
                if let Some(caller_wire) = caller_server {
                    let changed = self
                        .db
                        .call(move |s| {
                            assert_membership_gen(&s.conn, conn_gen)?;
                            let (changed, ch) = merge_mcp_server_wire(&s.conn, &caller_wire)?;
                            if let Some(ch) = ch {
                                s.mcp.apply_sync_change(ch);
                            }
                            if changed {
                                s.publish_global("mcp.updated", json!({ "id": server_id }));
                            }
                            Ok(changed)
                        })
                        .await?;
                    if changed {
                        self.signal.notify_changes(true, Vec::new());
                    }
                }
                if hops_left > 0 {
                    let remote_holders = self.query_mcp_participants(server_id).await;
                    let next_coord = remote_holders
                        .into_iter()
                        .filter(|p| p.as_str() < self.net.local_node_id())
                        .min();
                    if let Some(next_coord) = next_coord {
                        let local_wire = self
                            .db
                            .call(move |s| {
                                assert_membership_gen(&s.conn, conn_gen)?;
                                load_mcp_wire(&s.conn, server_id)
                            })
                            .await?;
                        match self
                            .rpc(
                                &next_coord,
                                &RpcReq::OAuthRefresh {
                                    server_id,
                                    generation: generation.clone(),
                                    caller_server: local_wire,
                                    rejected_token_hash,
                                    hops_left: hops_left - 1,
                                },
                                RPC_TIMEOUT_OAUTH,
                            )
                            .await
                        {
                            Ok((
                                RpcResp::OAuthRefreshResult {
                                    status,
                                    server,
                                    error,
                                },
                                _,
                            )) => {
                                if let Some(ref w) = server {
                                    let w_clone = w.clone();
                                    let changed = self
                                        .db
                                        .call(move |s| {
                                            assert_membership_gen(&s.conn, conn_gen)?;
                                            let (changed, ch) =
                                                merge_mcp_server_wire(&s.conn, &w_clone)?;
                                            if let Some(ch) = ch {
                                                s.mcp.apply_sync_change(ch);
                                            }
                                            if changed {
                                                s.publish_global(
                                                    "mcp.updated",
                                                    json!({ "id": server_id }),
                                                );
                                            }
                                            Ok(changed)
                                        })
                                        .await?;
                                    if changed
                                        || status == OAuthRefreshStatus::Refreshed
                                        || status == OAuthRefreshStatus::ReauthRequired
                                    {
                                        self.signal.notify_changes(true, Vec::new());
                                    }
                                }
                                return Ok((
                                    RpcResp::OAuthRefreshResult {
                                        status,
                                        server,
                                        error,
                                    },
                                    None,
                                ));
                            }
                            Ok(_) | Err(_) => {
                                return Ok((
                                    RpcResp::Error {
                                        message: "upstream OAuth refresh coordinator unreachable"
                                            .into(),
                                    },
                                    None,
                                ));
                            }
                        }
                    }
                }
                let res = crate::mcp::oauth::refresh_shared_as_coordinator(
                    &self.db,
                    &self.client,
                    server_id,
                    &generation,
                    rejected_token_hash,
                )
                .await;
                match res {
                    Ok(coord) => {
                        if coord.status == OAuthRefreshStatus::Refreshed
                            || coord.status == OAuthRefreshStatus::ReauthRequired
                        {
                            self.signal.notify_changes(true, Vec::new());
                        }
                        Ok((
                            RpcResp::OAuthRefreshResult {
                                status: coord.status,
                                server: coord.server,
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
                let my_peers = sample_peers_for_pex(list_peers(&s.conn)?, MAX_PEX_PEERS);
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
        if let RpcResp::Peers {
            self_info,
            mut peers,
        } = resp
        {
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
                        changed |=
                            upsert_peer_if_gen(&s.conn, &local.node_id, &p, now, false, mgen)?;
                    }
                    let all = list_peers(&s.conn)?;
                    s.presence.set_known_peer_count(all.len());
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

    pub async fn broadcast_dirty(
        &self,
        catalog: bool,
        mut sessions: Vec<SessionId>,
        exclude_peer: Option<&str>,
    ) {
        if !catalog && sessions.is_empty() {
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
        sessions.sort();
        sessions.dedup();
        let req = RpcReq::NotifyDirty {
            catalog,
            sessions: sessions.clone(),
        };
        let Ok(bytes) = serde_json::to_vec(&req) else {
            self.schedule_retry_dirty(catalog, &sessions);
            return;
        };
        let bytes = Bytes::from(bytes);
        let results = futures_util::future::join_all(
            targets
                .iter()
                .map(|peer_id| self.rpc_encoded(peer_id, bytes.clone(), RPC_TIMEOUT_SHORT)),
        )
        .await;
        if results.iter().any(Result::is_err) {
            self.schedule_retry_dirty(catalog, &sessions);
        } else if !self.net.online_peer_ids().is_empty() {
            let mut st = self.outbound_retries.lock().unwrap();
            if st.sessions.is_empty() && !st.catalog_dirty {
                st.attempts = 0;
            }
        }
    }

    pub(crate) fn validate_session_delta_budget(delta: &SessionDelta) -> Result<u32> {
        let mut total_unpacked_bytes: u64 = 0;
        let mut total_oversized_units: u32 = 0;
        let mut oversized_items: usize = 0;
        for item in &delta.items {
            match (item.payload_len, item.payload_hash) {
                (Some(expected_len), Some(_)) => {
                    if expected_len == 0 || expected_len > MAX_ITEM_PAYLOAD_BYTES {
                        return Err(Error::bad("invalid oversized item payload length"));
                    }
                    if item.payload.is_empty() {
                        oversized_items += 1;
                    }
                    total_unpacked_bytes = total_unpacked_bytes.saturating_add(expected_len);
                    let units = u32::try_from(expected_len.div_ceil(ATT_CHUNK_SIZE as u64))
                        .map_err(|_| Error::bad("item payload budget overflow"))?
                        .max(1);
                    total_oversized_units = total_oversized_units.saturating_add(units);
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(Error::bad("incomplete item payload metadata"));
                }
                (None, None) => {
                    total_unpacked_bytes =
                        total_unpacked_bytes.saturating_add(item.payload.len() as u64);
                }
            }
        }
        let within_delta = total_unpacked_bytes <= ledger::MAX_DELTA_BYTES as u64;
        let single_oversized = oversized_items == 1
            && delta.items.len() == 1
            && delta.context_nodes.is_empty()
            && delta.turn_settled.is_empty()
            && delta.invalidated_warnings.is_empty()
            && delta.pruned_context_nodes.is_empty()
            && total_unpacked_bytes <= MAX_ITEM_PAYLOAD_BYTES;
        if !within_delta && !single_oversized {
            return Err(Error::bad("SessionDelta unpacked payload exceeds page budget"));
        }
        Ok(total_oversized_units)
    }

    async fn fetch_item_payload(
        &self,
        peer_id: &str,
        sid: SessionId,
        item_id: ItemId,
        expected_len: u64,
        expected_hash: [u8; 32],
        expected_mgen: i64,
    ) -> Result<String> {
        if expected_len == 0 || expected_len > MAX_ITEM_PAYLOAD_BYTES {
            return Err(Error::bad("invalid oversized item payload length"));
        }
        let total_len = usize::try_from(expected_len)
            .map_err(|_| Error::bad("oversized item payload length overflow"))?;

        let mut buf = Vec::with_capacity(total_len);
        let mut hasher = blake3::Hasher::new();
        while buf.len() < total_len {
            if self.net.membership_gen() != expected_mgen {
                return Err(Error::conflict(
                    "cluster membership changed during payload fetch",
                ));
            }
            let offset = buf.len() as u64;
            let want = (total_len - buf.len()).min(ATT_CHUNK_SIZE);
            let (resp, chunk) = self
                .rpc(
                    peer_id,
                    &RpcReq::GetItemPayloadChunk {
                        session_id: sid,
                        item_id,
                        offset,
                        len: want as u32,
                    },
                    RPC_TIMEOUT_DATA,
                )
                .await?;
            let RpcResp::ItemPayloadChunk = resp else {
                return Err(Error::bad("unexpected GetItemPayloadChunk response"));
            };
            if chunk.is_empty() || chunk.len() > want {
                return Err(Error::bad("invalid item payload chunk size"));
            }
            hasher.update(&chunk);
            buf.extend_from_slice(&chunk);
        }
        if buf.len() != total_len || hasher.finalize().as_bytes() != &expected_hash {
            return Err(Error::bad(
                "item payload failed BLAKE3 integrity verification",
            ));
        }
        String::from_utf8(buf).map_err(|_| Error::bad("item payload is not valid UTF-8"))
    }

    async fn pull_catalog_from_peer(
        &self,
        peer_id: &str,
        expected_mgen: i64,
    ) -> Result<(bool, usize, bool)> {
        let lock = self.catalog_locks.lock_for(peer_id.to_owned());
        let _guard = lock.lock().await;
        let mut any_changed = false;
        let mut total_rows = 0usize;
        let mut caught_up = false;
        let mut prev_cursor: Option<(String, i64)> = None;

        for _ in 0..1024 {
            if self.net.membership_gen() != expected_mgen {
                break;
            }
            let pid = peer_id.to_owned();
            let (stored_epoch, since_rev, calls_vector) = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let cur: Option<(String, i64)> = s
                        .conn
                        .prepare_cached(
                            "SELECT remote_db_epoch, last_remote_rev FROM peer_catalog_cursors WHERE peer_node_id=?",
                        )?
                        .query_row([&pid], |r| Ok((r.get(0)?, r.get(1)?)))
                        .optional()?;
                    let (epoch, rev) = cur.unwrap_or_default();
                    let cv = read_vector(&s.conn, CALLS_SCOPE)?;
                    Ok((epoch, rev, cv))
                })
                .await?;

            let (resp, _) = self
                .rpc(
                    peer_id,
                    &RpcReq::CatalogSync {
                        peer_db_epoch: stored_epoch,
                        since_rev,
                        calls_vector,
                    },
                    RPC_TIMEOUT_DATA,
                )
                .await?;
            let RpcResp::CatalogDelta(delta) = resp else {
                return Err(Error::bad("unexpected CatalogSync response"));
            };
            if self.net.membership_gen() != expected_mgen {
                break;
            }
            let has_more = delta.has_more;
            let cur_epoch = delta.db_epoch.clone();
            let next_epoch = delta.db_epoch.clone();
            let next_rev = delta.next_rev;
            let row_count = delta.providers.len()
                + delta.models.len()
                + delta.mcp_servers.len()
                + delta.sessions.len()
                + delta.calls.len();
            let pid_w = peer_id.to_owned();
            let streams = self.streams.clone();
            let outcome = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let outcome = apply_catalog_delta(&s.conn, &delta)?;
                    if !next_epoch.is_empty() {
                        s.conn.prepare_cached(
                            "INSERT INTO peer_catalog_cursors(peer_node_id, remote_db_epoch, last_remote_rev)
                             VALUES(?1, ?2, ?3)
                             ON CONFLICT(peer_node_id) DO UPDATE SET
                               remote_db_epoch=excluded.remote_db_epoch,
                               last_remote_rev=excluded.last_remote_rev",
                        )?
                        .execute(params![pid_w, next_epoch, next_rev])?;
                    }
                    publish_catalog_merge(s, &streams, &outcome)?;
                    Ok(outcome)
                })
                .await?;

            if outcome.changed {
                any_changed = true;
                total_rows += row_count;
            }
            if !has_more {
                caught_up = true;
                break;
            }
            let cur_key = (cur_epoch, next_rev);
            if row_count == 0 && prev_cursor.as_ref() == Some(&cur_key) {
                break;
            }
            prev_cursor = Some(cur_key);
        }

        Ok((any_changed, total_rows, caught_up))
    }

    async fn pull_session_from_peer(
        &self,
        sid: SessionId,
        peer_id: &str,
        expected_mgen: i64,
    ) -> Result<(bool, bool)> {
        let lock = self.session_locks.lock_for((sid, peer_id.to_owned()));
        let _guard = lock.lock().await;
        let mut any_changed = false;
        let mut caught_up = false;
        let mut prev_vec: Option<BTreeMap<String, i64>> = None;

        for _ in 0..1024 {
            if self.net.membership_gen() != expected_mgen {
                break;
            }
            let (deleted_local, vector) = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let deleted: bool = s
                        .conn
                        .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)")?
                        .query_row([sid], |r| r.get(0))?;
                    if deleted {
                        return Ok((true, BTreeMap::new()));
                    }
                    Ok((false, read_vector(&s.conn, &session_scope(sid))?))
                })
                .await?;
            if deleted_local {
                caught_up = true;
                break;
            }
            let (resp, _) = self
                .rpc(
                    peer_id,
                    &RpcReq::SessionSync {
                        session_id: sid,
                        vector,
                    },
                    RPC_TIMEOUT_DATA,
                )
                .await?;
            let RpcResp::SessionDelta(mut delta) = resp else {
                return Err(Error::bad("unexpected SessionSync response"));
            };
            if delta.session_id != sid {
                return Err(Error::bad("SessionDelta session_id mismatch"));
            }
            if self.net.membership_gen() != expected_mgen {
                break;
            }
            let total_oversized_units = Self::validate_session_delta_budget(&delta)?;
            let page_budget = if total_oversized_units > 0 {
                Some(
                    tokio::time::timeout(
                        RPC_TIMEOUT_DATA,
                        self.att_byte_budget
                            .clone()
                            .acquire_many_owned(total_oversized_units),
                    )
                    .await
                    .map_err(|_| Error::conflict("item payload staging budget saturated"))?
                    .map_err(|_| Error::internal("item payload budget closed"))?,
                )
            } else {
                None
            };
            for item in &mut delta.items {
                match (item.payload_len, item.payload_hash) {
                    (Some(expected_len), Some(expected_hash)) => {
                        if item.payload.is_empty() {
                            item.payload = self
                                .fetch_item_payload(
                                    peer_id,
                                    sid,
                                    item.id,
                                    expected_len,
                                    expected_hash,
                                    expected_mgen,
                                )
                                .await?;
                        } else {
                            let bytes = item.payload.as_bytes();
                            if bytes.len() as u64 != expected_len
                                || blake3::hash(bytes).as_bytes() != &expected_hash
                            {
                                return Err(Error::bad("inline item payload checksum mismatch"));
                            }
                        }
                        item.payload_len = None;
                        item.payload_hash = None;
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return Err(Error::bad("incomplete item payload metadata"));
                    }
                    (None, None) => {}
                }
            }
            let has_more = delta.has_more;
            let deleted = delta.deleted;
            let next_vec = delta.advanced_vector.clone();
            let streams = self.streams.clone();
            let outcome = self
                .db
                .call(move |s| {
                    assert_membership_gen(&s.conn, expected_mgen)?;
                    let outcome = apply_session_delta(&s.conn, &delta)?;
                    if outcome.has_null_attachments {
                        s.presence.set_has_null_attachments(true);
                    }
                    publish_session_merge(s, &streams, sid, &outcome)?;
                    Ok(outcome)
                })
                .await?;
            drop(page_budget);

            if outcome.changed {
                any_changed = true;
            }
            if deleted || !has_more {
                caught_up = true;
                break;
            }
            if prev_vec.as_ref() == Some(&next_vec) && next_vec.is_empty() {
                break;
            }
            prev_vec = Some(next_vec);
        }

        Ok((any_changed, caught_up))
    }

    async fn is_session_deleted_local(&self, sid: SessionId) -> Result<bool> {
        self.db
            .call(move |s| {
                s.conn
                    .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)")?
                    .query_row([sid], |r| r.get::<_, bool>(0))
                    .map_err(Into::into)
            })
            .await
    }

    async fn pull_catalog_peers(
        &self,
        peers: &[String],
        mgen: i64,
    ) -> (usize, usize, Option<Error>) {
        let mut ok_count = 0usize;
        let mut rows_updated = 0usize;
        let mut first_err: Option<Error> = None;
        let mut futs = stream::iter(peers.iter().cloned())
            .map(|peer_id| async move {
                if self.net.membership_gen() != mgen {
                    return Err(Error::conflict("membership generation changed"));
                }
                self.pull_catalog_from_peer(&peer_id, mgen).await.map_err(|err| {
                    Error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("catalog sync with peer {peer_id} failed: {}", err.1),
                    )
                })
            })
            .buffer_unordered(4);
        while let Some(res) = futs.next().await {
            match res {
                Ok((_, rows, true)) => {
                    ok_count += 1;
                    rows_updated += rows;
                }
                Ok((_, rows, false)) => {
                    rows_updated += rows;
                    if first_err.is_none() {
                        first_err = Some(Error(
                            StatusCode::GATEWAY_TIMEOUT,
                            "catalog sync truncated before catching up".into(),
                        ));
                    }
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        (ok_count, rows_updated, first_err)
    }

    async fn pull_session_peers(
        &self,
        sid: SessionId,
        peers: &[String],
        mgen: i64,
    ) -> (usize, bool, Option<Error>) {
        let mut ok_count = 0usize;
        let mut any_changed = false;
        let mut first_err: Option<Error> = None;
        let mut futs = stream::iter(peers.iter().cloned())
            .map(|peer_id| async move {
                if self.net.membership_gen() != mgen {
                    return Err(Error::conflict("membership generation changed"));
                }
                self.pull_session_from_peer(sid, &peer_id, mgen)
                    .await
                    .map_err(|err| {
                        Error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            format!("session sync with peer {peer_id} failed: {}", err.1),
                        )
                    })
            })
            .buffer_unordered(4);
        while let Some(res) = futs.next().await {
            match res {
                Ok((changed, true)) => {
                    ok_count += 1;
                    any_changed |= changed;
                }
                Ok((changed, false)) => {
                    any_changed |= changed;
                    if first_err.is_none() {
                        first_err = Some(Error(
                            StatusCode::GATEWAY_TIMEOUT,
                            "session sync truncated before catching up".into(),
                        ));
                    }
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        (ok_count, any_changed, first_err)
    }

    pub async fn sync_with_peers(
        &self,
        specific_peer: Option<&str>,
        target_session: Option<SessionId>,
    ) -> Result<SyncSummary> {
        let mgen = self.net.membership_gen();
        let peers = match specific_peer {
            Some(p) if self.net.is_peer_online(p) => vec![p.to_owned()],
            Some(_) => Vec::new(),
            None => self.net.online_peer_ids(),
        };
        if peers.is_empty() {
            return Ok(SyncSummary {
                peers_queried: 0,
                catalog_rows_updated: 0,
                sessions_updated: 0,
            });
        }

        let (_, catalog_rows_updated, _) = self.pull_catalog_peers(&peers, mgen).await;

        let sessions: Vec<SessionId> = match target_session {
            Some(sid) => vec![sid],
            None => self
                .db
                .call(|s| list_watched_sessions(&s.conn))
                .await
                .unwrap_or_default(),
        };

        let mut sessions_updated = 0usize;
        for sid in sessions {
            if self.net.membership_gen() != mgen {
                break;
            }
            let (_, updated, _) = self.pull_session_peers(sid, &peers, mgen).await;
            if updated {
                sessions_updated += 1;
            }
        }

        Ok(SyncSummary {
            peers_queried: peers.len(),
            catalog_rows_updated,
            sessions_updated,
        })
    }

    pub async fn sync_session_strict(&self, sid: SessionId, peers: &[String]) -> Result<()> {
        if self.is_session_deleted_local(sid).await? {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        let mgen = self.net.membership_gen();
        if !peers.is_empty() {
            let results: Vec<Result<()>> = stream::iter(peers.iter().cloned())
                .map(|peer_id| async move {
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    let (_, _, cat_caught_up) = self
                        .pull_catalog_from_peer(&peer_id, mgen)
                        .await
                        .map_err(|err| {
                            if self.net.membership_gen() != mgen {
                                Error::conflict("membership generation changed")
                            } else {
                                Error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    format!("catalog sync with peer {peer_id} failed: {}", err.1),
                                )
                            }
                        })?;
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    if !cat_caught_up {
                        return Err(Error(
                            StatusCode::GATEWAY_TIMEOUT,
                            format!(
                                "catalog sync with peer {peer_id} truncated before catching up"
                            ),
                        ));
                    }
                    let (_, sess_caught_up) = self
                        .pull_session_from_peer(sid, &peer_id, mgen)
                        .await
                        .map_err(|err| {
                            if self.net.membership_gen() != mgen {
                                Error::conflict("membership generation changed")
                            } else {
                                Error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    format!("session sync with peer {peer_id} failed: {}", err.1),
                                )
                            }
                        })?;
                    if !sess_caught_up {
                        return Err(Error(
                            StatusCode::GATEWAY_TIMEOUT,
                            format!(
                                "session sync with peer {peer_id} truncated before catching up"
                            ),
                        ));
                    }
                    Ok(())
                })
                .buffer_unordered(4)
                .collect()
                .await;
            for res in results {
                res?;
            }
        }
        self.db
            .call(move |s| {
                let row: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                if row != Some(false) {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                mark_session_watched(&s.conn, sid)?;
                Ok(())
            })
            .await
    }

    pub async fn sync_session_best_effort(&self, sid: SessionId) -> Result<()> {
        if self.is_session_deleted_local(sid).await? {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        let mgen = self.net.membership_gen();
        let peers = self.net.online_peer_ids();
        if !peers.is_empty() {
            let results: Vec<Result<()>> = stream::iter(peers.into_iter())
                .map(|peer_id| async move {
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    if let Err(err) = self.pull_catalog_from_peer(&peer_id, mgen).await {
                        if self.net.membership_gen() != mgen {
                            return Err(Error::conflict("membership generation changed"));
                        }
                        tracing::debug!(%sid, %peer_id, error = %err, "best-effort catalog pull skipped unreachable peer");
                        return Ok(());
                    }
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    if let Err(err) = self.pull_session_from_peer(sid, &peer_id, mgen).await {
                        if self.net.membership_gen() != mgen {
                            return Err(Error::conflict("membership generation changed"));
                        }
                        tracing::debug!(%sid, %peer_id, error = %err, "best-effort session pull skipped unreachable peer");
                    }
                    Ok(())
                })
                .buffer_unordered(4)
                .collect()
                .await;
            for res in results {
                res?;
            }
        }
        self.db
            .call(move |s| {
                let row: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                if row != Some(false) {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                mark_session_watched(&s.conn, sid)?;
                Ok(())
            })
            .await
    }

    pub async fn sync_session_from_peers_best_effort(
        &self,
        sid: SessionId,
        peers: &[String],
    ) -> Result<bool> {
        if self.is_session_deleted_local(sid).await? {
            return Ok(true);
        }
        let mgen = self.net.membership_gen();
        if !peers.is_empty() {
            let results: Vec<Result<()>> = stream::iter(peers.iter().cloned())
                .map(|peer_id| async move {
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    if let Err(err) = self.pull_catalog_from_peer(&peer_id, mgen).await {
                        if self.net.membership_gen() != mgen {
                            return Err(Error::conflict("membership generation changed"));
                        }
                        tracing::debug!(%sid, %peer_id, error = %err, "relay catalog pull skipped peer");
                        return Ok(());
                    }
                    if self.net.membership_gen() != mgen {
                        return Err(Error::conflict("membership generation changed"));
                    }
                    if let Err(err) = self.pull_session_from_peer(sid, &peer_id, mgen).await {
                        if self.net.membership_gen() != mgen {
                            return Err(Error::conflict("membership generation changed"));
                        }
                        tracing::debug!(%sid, %peer_id, error = %err, "relay session pull skipped peer");
                    }
                    Ok(())
                })
                .buffer_unordered(4)
                .collect()
                .await;
            for res in results {
                res?;
            }
        }
        self.db
            .call(move |s| {
                let row: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                match row {
                    Some(true) => Ok(true),
                    Some(false) => {
                        mark_session_watched(&s.conn, sid)?;
                        Ok(false)
                    }
                    None => Ok(false),
                }
            })
            .await
    }

    pub async fn sync_now(&self, session_id: Option<SessionId>) -> Result<SyncSummary> {
        let _gate = self.sync_now_gate.lock().await;
        let Some(sid) = session_id else {
            return self.sync_with_peers(None, None).await;
        };
        if self.is_session_deleted_local(sid).await? {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }

        let mgen = self.net.membership_gen();
        let peers = self.net.online_peer_ids();
        let (catalog_ok, catalog_rows_updated, first_cat_err) = if peers.is_empty() {
            (0, 0, None)
        } else {
            self.pull_catalog_peers(&peers, mgen).await
        };

        let state_after_cat: Option<bool> = self
            .db
            .call(move |s| {
                let row: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                if row == Some(false) {
                    mark_session_watched(&s.conn, sid)?;
                }
                Ok(row)
            })
            .await?;
        match state_after_cat {
            Some(true) => return Err(rusqlite::Error::QueryReturnedNoRows.into()),
            None => {
                if let Some(err) = first_cat_err {
                    return Err(err);
                }
                if !peers.is_empty() && catalog_ok < peers.len() {
                    return Err(Error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "unable to reach all cluster peers to verify session".into(),
                    ));
                }
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            Some(false) => {}
        }

        let mut session_updated = false;
        if !peers.is_empty() {
            let (session_ok, changed, first_sess_err) =
                self.pull_session_peers(sid, &peers, mgen).await;
            session_updated = changed;
            if session_ok == 0 {
                return Err(first_sess_err.unwrap_or_else(|| {
                    Error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "failed to sync session from peers".into(),
                    )
                }));
            }
            let exists_active: bool = self
                .db
                .call(move |s| {
                    let row: Option<bool> = s
                        .conn
                        .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                        .query_row([sid], |r| r.get(0))
                        .optional()?;
                    Ok(row == Some(false))
                })
                .await?;
            if !exists_active {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
        }

        let _ = self.ensure_session_attachments(sid).await;
        Ok(SyncSummary {
            peers_queried: peers.len(),
            catalog_rows_updated,
            sessions_updated: usize::from(session_updated),
        })
    }

    pub async fn ensure_session_watched(&self, sid: SessionId) -> Result<()> {
        let (deleted, fast_ok) = self
            .db
            .call(move |s| {
                let deleted: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                if deleted == Some(true) {
                    return Ok((true, false));
                }
                if deleted == Some(false) {
                    let already_watched = is_session_watched(&s.conn, sid)?;
                    mark_session_watched(&s.conn, sid)?;
                    let complete = already_watched && session_branch_complete(&s.conn, sid)?;
                    return Ok((false, complete));
                }
                Ok((false, false))
            })
            .await?;
        if deleted {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        if fast_ok {
            return Ok(());
        }
        if !self.net.online_peer_ids().is_empty() {
            let _ = self.sync_with_peers(None, Some(sid)).await;
        }
        let now_deleted = self
            .db
            .call(move |s| {
                let row: Option<bool> = s
                    .conn
                    .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
                    .query_row([sid], |r| r.get(0))
                    .optional()?;
                if row == Some(true) {
                    return Ok(true);
                }
                if row == Some(false) {
                    mark_session_watched(&s.conn, sid)?;
                }
                Ok(false)
            })
            .await?;
        if now_deleted {
            return Err(rusqlite::Error::QueryReturnedNoRows.into());
        }
        Ok(())
    }

    pub async fn ensure_attachment_data(&self, id: AttachmentId) -> Result<()> {
        self.ensure_attachment_batch(&[id]).await
    }

    async fn ensure_attachment_batch(&self, ids: &[AttachmentId]) -> Result<()> {
        let ids = ids.to_vec();
        let missing = self
            .db
            .call(move |s| {
                let mut missing = Vec::new();
                for id in ids {
                    if let Some(att) = missing_attachment(&s.conn, id)? {
                        missing.push(att);
                    }
                }
                Ok(missing)
            })
            .await?;
        if missing.is_empty() {
            return Ok(());
        }
        let mut hashes: Vec<_> = missing.iter().map(|att| att.hash).collect();
        hashes.sort_unstable();
        hashes.dedup();
        let req = serde_json::to_vec(&RpcReq::HasAttachments {
            hashes: hashes.clone(),
        })
        .map_err(|e| Error::internal(e.to_string()))?;
        let req = Bytes::from(req);
        let mut probes = stream::iter(self.net.online_peer_ids())
            .map(|peer| {
                let req = req.clone();
                async move {
                    let _permit = self.attachment_probes.acquire().await.ok()?;
                    match self.rpc_encoded(&peer, req, RPC_TIMEOUT_SHORT).await {
                        Ok((RpcResp::AttachmentsAvailable { hashes }, _)) => Some((peer, hashes)),
                        _ => None,
                    }
                }
            })
            .buffer_unordered(MAX_ATTACHMENT_PROBES);
        let mut holders = HashMap::<[u8; 32], Vec<String>>::new();
        while let Some(reply) = probes.next().await {
            if let Some((peer, mut available)) = reply {
                available.truncate(ATTACHMENT_HASH_BATCH);
                for hash in &hashes {
                    if available.contains(hash) {
                        holders.entry(*hash).or_default().push(peer.clone());
                    }
                }
            }
        }
        for att in missing {
            let lock = self.att_locks.lock_for(att.hash);
            let _guard = lock.lock().await;
            let Some(current) = self
                .db
                .call(move |s| missing_attachment(&s.conn, att.id))
                .await?
            else {
                continue;
            };
            if current != att {
                return Err(Error::conflict(
                    "attachment metadata changed during download",
                ));
            }
            let Some(peers) = holders.get(&att.hash) else {
                return Err(Error::conflict(format!(
                    "attachment {} bytes are unavailable on online peers",
                    att.id
                )));
            };
            self.download_attachment(att, peers).await?;
        }
        Ok(())
    }

    async fn download_attachment(&self, att: MissingAttachment, holders: &[String]) -> Result<()> {
        let total_len = att.len as usize;
        let num_chunks = total_len.div_ceil(ATT_CHUNK_SIZE);
        let budget = tokio::time::timeout(
            RPC_TIMEOUT_DATA,
            self.att_byte_budget
                .clone()
                .acquire_many_owned(num_chunks as u32),
        )
        .await
        .map_err(|_| Error::conflict("attachment staging budget saturated"))?
        .map_err(|_| Error::internal("attachment budget closed"))?;
        let file = self.run_blocking(tempfile::tempfile).await??;
        let mut file = tokio::fs::File::from_std(file);
        let mut chunks = stream::iter(0..num_chunks)
            .map(|idx| async move {
                let offset = idx * ATT_CHUNK_SIZE;
                let len = (total_len - offset).min(ATT_CHUNK_SIZE);
                let req = RpcReq::GetAttachmentChunk {
                    content_hash: att.hash,
                    offset: offset as u64,
                    len: len as u32,
                };
                for attempt in 0..holders.len() {
                    let peer = &holders[(idx + attempt) % holders.len()];
                    if let Ok((RpcResp::AttachmentChunk, bytes)) =
                        self.rpc(peer, &req, RPC_TIMEOUT_DATA).await
                        && bytes.len() == len
                    {
                        return Ok(bytes);
                    }
                }
                Err(Error::conflict(format!(
                    "failed to download chunk {idx} of attachment {}",
                    att.id
                )))
            })
            .buffered(MAX_CHUNK_CONCURRENCY);
        while let Some(chunk) = chunks.next().await {
            file.write_all(&chunk?).await?;
        }
        file.flush().await?;
        let mut file = file.into_std().await;
        let (mut file, budget) = self
            .run_blocking(move || -> Result<_> {
                file.rewind()?;
                let mut hash = blake3::Hasher::new();
                hash.update_reader(&mut file)?;
                if hash.count() != att.len as u64 || hash.finalize().as_bytes() != &att.hash {
                    return Err(Error::bad(format!(
                        "attachment {} failed BLAKE3 integrity verification",
                        att.id
                    )));
                }
                file.rewind()?;
                Ok((file, budget))
            })
            .await??;

        let cancelled = CancellationToken::new();
        let _guard = cancelled.clone().drop_guard();
        self.db
            .call(move |s| {
                let _budget = budget;
                let tx = s.conn.transaction()?;
                if cancelled.is_cancelled() {
                    return Err(Error::conflict("attachment download cancelled"));
                }
                let rowid: Option<i64> = tx
                    .query_row(
                        "UPDATE attachments SET data=zeroblob(?3)
                         WHERE id=?1 AND content_hash=?2 AND byte_len=?3 AND data IS NULL
                         RETURNING rowid",
                        params![att.id, att.hash, att.len],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(rowid) = rowid {
                    let mut blob =
                        tx.blob_open(rusqlite::MAIN_DB, "attachments", "data", rowid, false)?;
                    if std::io::copy(&mut file, &mut blob)? != att.len as u64 {
                        return Err(Error::internal("attachment staging file length changed"));
                    }
                    blob.close()?;
                    fill_attachment_aliases(&tx, &att, rowid)?;
                } else if missing_attachment(&tx, att.id)?.is_some() {
                    return Err(Error::conflict("attachment metadata changed during download"));
                }
                if cancelled.is_cancelled() {
                    return Err(Error::conflict("attachment download cancelled"));
                }
                Ok(tx.commit()?)
            })
            .await
    }

    pub async fn ensure_session_attachments(&self, sid: SessionId) -> Result<()> {
        let missing = self
            .db
            .call(move |s| crate::media::missing_session_context_attachments(&s.conn, sid))
            .await?;
        for ids in missing.chunks(ATTACHMENT_HASH_BATCH) {
            self.ensure_attachment_batch(ids).await?;
        }
        Ok(())
    }

    async fn query_mcp_participants(&self, server_id: McpServerId) -> Vec<String> {
        let online = self.net.online_peer_ids();
        let req = RpcReq::HasMcpServer { server_id };
        let replies = futures_util::future::join_all(online.iter().map(|peer_id| {
            let req_ref = &req;
            async move {
                (
                    peer_id.clone(),
                    self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await,
                )
            }
        }))
        .await;
        let mut holders = Vec::new();
        for (peer_id, reply) in replies {
            if let Ok((RpcResp::McpServerPresence { active: true }, _)) = reply {
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
            Unchanged,
        }

        impl ReconciledOAuth {
            fn into_coordination(self) -> RefreshCoordination {
                match self {
                    Self::GoneOrReauth => RefreshCoordination::Delegated {
                        reauth_required: true,
                    },
                    Self::Rotated => RefreshCoordination::Delegated {
                        reauth_required: false,
                    },
                    Self::Unchanged => RefreshCoordination::PendingConfirmation,
                }
            }
        }

        async fn sync_and_evaluate_oauth(
            sync: &SyncEngine,
            mgen: i64,
            server_id: McpServerId,
            generation: String,
            pending: PendingRefreshState,
            priority_peer: Option<&str>,
            clear_if_same_and_valid: bool,
        ) -> Result<ReconciledOAuth> {
            if let Some(peer) = priority_peer {
                let _ = sync.sync_with_peers(Some(peer), None).await;
            }
            let _ = sync.sync_with_peers(None, None).await;
            sync.db
                .call(move |s| {
                    evaluate_oauth_after_sync(
                        &s.conn,
                        mgen,
                        server_id,
                        &generation,
                        &pending,
                        clear_if_same_and_valid,
                    )
                })
                .await
        }

        fn evaluate_oauth_after_sync(
            conn: &rusqlite::Connection,
            mgen: i64,
            server_id: McpServerId,
            generation: &str,
            pending: &PendingRefreshState,
            clear_if_same_and_valid: bool,
        ) -> Result<ReconciledOAuth> {
            assert_membership_gen(conn, mgen)?;
            let row: Option<(Option<String>, bool)> = conn
                .prepare_cached(
                    "SELECT o.credentials, o.rejected
                     FROM mcp_oauth o JOIN mcp_servers m ON m.id=o.server_id
                     WHERE o.server_id=?1 AND o.generation=?2 AND m.deleted=0",
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
            Ok(ReconciledOAuth::Unchanged)
        }

        let mgen = self.net.membership_gen();
        let gen_owned = generation.to_owned();
        let (shared_active, raw_creds, pending_raw) = self
            .db
            .call({
                let gen_q = gen_owned.clone();
                move |s| {
                    assert_membership_gen(&s.conn, mgen)?;
                    let shared: Option<(bool, bool)> = s
                        .conn
                        .prepare_cached("SELECT shared, deleted FROM mcp_servers WHERE id=?")?
                        .query_row([server_id], |r| Ok((r.get(0)?, r.get(1)?)))
                        .optional()?;
                    let shared_active = matches!(shared, Some((true, false)));
                    let oauth_row: Option<(Option<String>, Option<String>)> = s
                        .conn
                        .prepare_cached(
                            "SELECT credentials, pending_refresh
                             FROM mcp_oauth WHERE server_id=?1 AND generation=?2",
                        )?
                        .query_row(params![server_id, gen_q], |r| Ok((r.get(0)?, r.get(1)?)))
                        .optional()?;
                    let (raw_creds, pending_raw) = oauth_row.unwrap_or((None, None));
                    Ok((shared_active, raw_creds, pending_raw))
                }
            })
            .await?;

        if !shared_active {
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

        let mut pending_raw_for_cas = pending_raw;
        let mut reused_delegation: Option<PendingRefreshState> = None;
        if let Some(pr) = active_pending {
            let coord = self
                .net
                .is_peer_online(&pr.coordinator)
                .then_some(pr.coordinator.as_str());
            let recheck = sync_and_evaluate_oauth(
                self,
                mgen,
                server_id,
                gen_owned.clone(),
                pr.clone(),
                coord,
                false,
            )
            .await?;
            match recheck {
                ReconciledOAuth::Unchanged => {
                    let remote_holders = self.query_mcp_participants(server_id).await;
                    if remote_holders.iter().any(|p| p == &pr.coordinator) {
                        reused_delegation = Some(pr);
                    } else {
                        let cleared = self
                            .db
                            .write(
                                {
                                    let gen_q = gen_owned.clone();
                                    let pr = pr.clone();
                                    move |conn| {
                                        assert_membership_gen(conn, mgen)?;
                                        let now_secs = chrono::Utc::now().timestamp();
                                        if pr.is_stale_or_orphaned(conn, now_secs)? {
                                            pr.clear_if_current(conn, server_id, &gen_q)?;
                                            Ok(true)
                                        } else {
                                            Ok(false)
                                        }
                                    }
                                },
                                |_, cleared| Ok(cleared),
                            )
                            .await?;
                        if !cleared {
                            return Ok(RefreshCoordination::PendingConfirmation);
                        }
                        tracing::warn!(
                            %server_id,
                            coordinator = %pr.coordinator,
                            "cleared stale or orphaned OAuth pending_refresh"
                        );
                        pending_raw_for_cas = None;
                    }
                }
                other => return Ok(other.into_coordination()),
            }
        }

        let delegation = if let Some(pr) = reused_delegation {
            pr
        } else {
            let mut participants = self.query_mcp_participants(server_id).await;
            participants.push(self.net.local_node_id().to_owned());
            participants.sort();
            participants.dedup();
            let chosen = participants.first().cloned().unwrap_or_default();
            if chosen == self.net.local_node_id() || chosen.is_empty() {
                return Ok(RefreshCoordination::Local);
            }
            let pending = PendingRefreshState {
                coordinator: chosen,
                fp: current_fp,
                created_at: chrono::Utc::now().timestamp(),
            };
            let pending_json =
                serde_json::to_string(&pending).map_err(|e| Error::internal(e.to_string()))?;
            let gate = self
                .db
                .call({
                    let generation = gen_owned.clone();
                    move |s| Ok(s.mcp.refresh_gate(&generation))
                })
                .await?;
            let _claim_guard = gate.lock_owned().await;
            let claim = self
                .db
                .write(
                    {
                        let gen_q = gen_owned.clone();
                        let fp = pending.fp.clone();
                        move |conn| {
                            assert_membership_gen(conn, mgen)?;
                            let cur: Option<Option<String>> = conn
                                .prepare_cached(
                                    "SELECT credentials FROM mcp_oauth WHERE server_id=?1 AND generation=?2",
                                )?
                                .query_row(params![server_id, gen_q], |r| r.get(0))
                                .optional()?;
                            let Some(raw) = cur else {
                                return Ok(Err(RefreshCoordination::Delegated {
                                    reauth_required: true,
                                }));
                            };
                            let latest_fp = credentials_fingerprint(raw.as_deref());
                            if latest_fp.as_deref() != Some(fp.as_str()) {
                                return Ok(Err(RefreshCoordination::Delegated {
                                    reauth_required: latest_fp.is_none(),
                                }));
                            }
                            let changed = conn.execute(
                                "UPDATE mcp_oauth SET pending_refresh=?1
                                 WHERE server_id=?2 AND generation=?3 AND pending_refresh IS ?4",
                                params![pending_json, server_id, gen_q, pending_raw_for_cas],
                            )?;
                            if changed == 0 {
                                return Ok(Err(RefreshCoordination::PendingConfirmation));
                            }
                            Ok(Ok(()))
                        }
                    },
                    |_, res| Ok(res),
                )
                .await?;
            if let Err(outcome) = claim {
                return Ok(outcome);
            }
            pending
        };

        let coordinator = &delegation.coordinator;
        let caller_server = self
            .db
            .call(move |s| {
                assert_membership_gen(&s.conn, mgen)?;
                load_mcp_wire(&s.conn, server_id)
            })
            .await?;

        let rpc_res = self
            .rpc(
                coordinator,
                &RpcReq::OAuthRefresh {
                    server_id,
                    generation: gen_owned.clone(),
                    caller_server,
                    rejected_token_hash,
                    hops_left: 2,
                },
                RPC_TIMEOUT_OAUTH,
            )
            .await;

        match rpc_res {
            Ok((RpcResp::OAuthRefreshResult { status, server, .. }, _)) => {
                if let Some(w) = server {
                    let lock = self.mcp_locks.lock_for(server_id);
                    let _guard = lock.lock().await;
                    self.db
                        .call(move |s| {
                            assert_membership_gen(&s.conn, mgen)?;
                            let (changed, ch) = merge_mcp_server_wire(&s.conn, &w)?;
                            if let Some(ch) = ch {
                                s.mcp.apply_sync_change(ch);
                            }
                            if changed {
                                s.publish_global("mcp.updated", json!({ "id": server_id }));
                            }
                            Ok(())
                        })
                        .await?;
                }
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
                let post = self
                    .db
                    .call(move |s| {
                        evaluate_oauth_after_sync(
                            &s.conn,
                            mgen,
                            server_id,
                            &gen_owned,
                            &delegation,
                            true,
                        )
                    })
                    .await?;
                Ok(post.into_coordination())
            }
            _ => {
                let post = sync_and_evaluate_oauth(
                    self,
                    mgen,
                    server_id,
                    gen_owned,
                    delegation.clone(),
                    Some(coordinator),
                    false,
                )
                .await?;
                Ok(post.into_coordination())
            }
        }
    }
}

fn abort_active_session_worker(s: &mut Store, streams: &Hub, sid: SessionId) {
    if let Some(ctrl) = s.active.remove(&sid) {
        ctrl.soft.cancel();
        ctrl.hard.cancel();
    }
    s.pending_wakes.remove(&sid);
    crate::codemode::stop_session(s, sid);
    let _ = s.conn.execute(
        "UPDATE sessions SET local_status=NULL, local_error='' WHERE id=?1 AND local_status IS NOT NULL",
        [sid],
    );
    s.refresh_local_presence(sid);
    s.bump_run_gen(sid);
    s.bump_reset_gen(sid);
    streams.remove_session(sid);
}

fn publish_catalog_merge(
    s: &mut Store,
    streams: &Hub,
    outcome: &ledger::CatalogMergeOutcome,
) -> Result<()> {
    for &ch in &outcome.mcp_changes {
        s.mcp.apply_sync_change(ch);
        s.publish_global("mcp.updated", json!({ "id": ch.id }));
    }
    if outcome.catalog_changed {
        s.publish_global("catalog.updated", json!({}));
    }
    for &sid in &outcome.deleted_sessions {
        abort_active_session_worker(s, streams, sid);
        s.mcp.forget_session(sid);
        s.channels.remove(&sid);
        s.wake_sequences.remove(&sid);
        s.publish_global("session.deleted", json!({ "id": sid }));
    }
    for &sid in &outcome.changed_sessions {
        s.wake_waiters(sid);
        if outcome.switched_sessions.contains(&sid) {
            abort_active_session_worker(s, streams, sid);
            let view = s.bump_view_rev(sid);
            if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                crate::events::Notice::publish(&ch, "session.reset", json!({ "view": view }));
            }
        } else if outcome.resync_sessions.contains(&sid) {
            let view = s.bump_view_rev(sid);
            if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                crate::events::Notice::publish(&ch, "resync", json!({ "view": view }));
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
    Ok(())
}

fn publish_session_merge(
    s: &mut Store,
    streams: &Hub,
    sid: SessionId,
    outcome: &ledger::SessionMergeOutcome,
) -> Result<()> {
    if !outcome.changed {
        return Ok(());
    }
    if outcome.deleted {
        abort_active_session_worker(s, streams, sid);
        s.mcp.forget_session(sid);
        s.channels.remove(&sid);
        s.wake_sequences.remove(&sid);
        s.publish_global("session.deleted", json!({ "id": sid }));
        return Ok(());
    }
    s.wake_waiters(sid);
    if outcome.reset_view {
        let view = s.bump_view_rev(sid);
        if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
            crate::events::Notice::publish(&ch, "session.reset", json!({ "view": view }));
        }
    } else {
        if outcome.out_of_order_view {
            let _ = s.bump_view_rev(sid);
        }
        for &id in &outcome.new_items {
            let _ = s.item_saved(sid, id);
        }
    }
    if let Ok(info) = session(&s.conn, sid) {
        let view = s.session_view(info);
        let data = json!(view);
        if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
            crate::events::Notice::publish(&ch, "session.updated", data.clone());
        }
        s.publish_global("session.updated", data);
    }
    Ok(())
}
