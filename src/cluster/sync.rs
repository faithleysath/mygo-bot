use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use iroh::endpoint::{Connection, RecvStream, SendStream};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    cluster::{
        SyncSignal,
        identity::{PeerInfo, list_peers, load_local_node, self_peer_info, upsert_peer},
        ledger::{
            self, ApplyOutcome, CommitWire, StreamHead, StreamKind, apply_remote_commits,
            apply_unwatched_session_head, has_commit_hash, local_manifest, read_commits_range,
            record_fork_conflict, resolve_conflict_cluster, stream_head,
        },
        net::{ClusterNet, PeerEvent, RPC_BODY_MAX_BYTES, RPC_HEADER_MAX_BYTES, read_frame, write_frame},
    },
    events::Hub,
    id::{AttachmentId, McpServerId, SessionId},
    store::{Db, Store, session},
};

const ATT_CHUNK_SIZE: usize = 256 * 1024;
const MAX_CHUNK_CONCURRENCY: usize = 4;
const COMMIT_BATCH_COUNT: usize = 64;
const COMMIT_BATCH_BYTES: usize = 4 * 1024 * 1024;
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
    GetManifest,
    GetHeads {
        stream_ids: Vec<String>,
    },
    NotifyHeads {
        heads: Vec<StreamHead>,
    },
    GetCommits {
        stream_id: String,
        epoch: i64,
        after_seq: i64,
        max_count: usize,
        max_bytes: usize,
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
        after_epoch: i64,
        #[serde(default)]
        after_seq: i64,
    },
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
    },
    Ack,
    Commits {
        commits: Vec<CommitWire>,
        has_more: bool,
    },
    AttachmentsAvailable {
        hashes: Vec<[u8; 32]>,
    },
    AttachmentChunk,
    OAuthRefreshResult {
        refreshed: bool,
        head: Option<StreamHead>,
        commits: Vec<CommitWire>,
        error: Option<String>,
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
    Delegated,
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

pub struct SyncEngine {
    db: Db,
    client: reqwest::Client,
    streams: Hub,
    net: Arc<ClusterNet>,
    signal_tx: mpsc::UnboundedSender<SyncSignal>,
    stream_locks: LockTable<String>,
    att_locks: LockTable<[u8; 32]>,
    reconcile_gate: AsyncMutex<()>,
}

impl SyncEngine {
    pub fn new(
        db: Db,
        client: reqwest::Client,
        streams: Hub,
        net: Arc<ClusterNet>,
        signal_tx: mpsc::UnboundedSender<SyncSignal>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            client,
            streams,
            net,
            signal_tx,
            stream_locks: LockTable::default(),
            att_locks: LockTable::default(),
            reconcile_gate: AsyncMutex::new(()),
        })
    }

    pub fn spawn_loops(
        self: &Arc<Self>,
        mut signal_rx: mpsc::UnboundedReceiver<SyncSignal>,
        mut peer_rx: mpsc::UnboundedReceiver<PeerEvent>,
        stopping: CancellationToken,
        workers: &TaskTracker,
    ) {
        let (bcast_tx, mut bcast_rx) = mpsc::unbounded_channel::<Vec<String>>();
        let bcast_eng = self.clone();
        let bcast_stop = stopping.clone();
        workers.spawn(async move {
            loop {
                let mut ids = tokio::select! {
                    _ = bcast_stop.cancelled() => break,
                    maybe = bcast_rx.recv() => match maybe {
                        Some(ids) => ids,
                        None => break,
                    },
                };
                while let Ok(more) = bcast_rx.try_recv() {
                    ids.extend(more);
                }
                ids.sort();
                ids.dedup();
                bcast_eng.broadcast_heads(&ids, None).await;
            }
        });

        let engine = self.clone();
        let stop = stopping.clone();
        workers.spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    Some(sig) = signal_rx.recv() => {
                        match sig {
                            SyncSignal::StreamsChanged(mut ids) => {
                                // Coalesce bursty local writes already queued.
                                while let Ok(next) = signal_rx.try_recv() {
                                    match next {
                                        SyncSignal::StreamsChanged(more) => ids.extend(more),
                                        SyncSignal::SelfMetaChanged => {
                                            let eng = engine.clone();
                                            tokio::spawn(async move { eng.broadcast_peers().await; });
                                        }
                                    }
                                }
                                let _ = bcast_tx.send(ids);
                            }
                            SyncSignal::SelfMetaChanged => {
                                let eng = engine.clone();
                                tokio::spawn(async move { eng.broadcast_peers().await; });
                            }
                        }
                    }
                    Some(ev) = peer_rx.recv() => {
                        match ev {
                            PeerEvent::Connected(peer_id) => {
                                let eng = engine.clone();
                                tokio::spawn(async move {
                                    let _ = eng.on_peer_connected(&peer_id).await;
                                });
                            }
                            PeerEvent::Disconnected(peer_id) => {
                                tracing::debug!(%peer_id, "cluster peer disconnected");
                            }
                        }
                    }
                }
            }
        });
    }

    async fn rpc(
        &self,
        peer_id: &str,
        req: &RpcReq,
        timeout: Duration,
    ) -> Result<(RpcResp, Vec<u8>)> {
        let Some(conn) = self.net.get_connection(peer_id) else {
            return Err(Error::conflict(format!("peer {peer_id} is offline")));
        };
        Self::rpc_on_conn(&conn, req, timeout).await
    }

    async fn rpc_on_conn(
        conn: &Connection,
        req: &RpcReq,
        timeout: Duration,
    ) -> Result<(RpcResp, Vec<u8>)> {
        let req_bytes = serde_json::to_vec(req).map_err(|e| Error::internal(e.to_string()))?;
        tokio::time::timeout(timeout, async {
            let (mut send, mut recv) = conn
                .open_bi()
                .await
                .map_err(|e| Error::internal(format!("open RPC stream failed: {e}")))?;
            write_frame(&mut send, &req_bytes, None).await?;
            let (hdr, body) =
                read_frame(&mut recv, RPC_HEADER_MAX_BYTES, RPC_BODY_MAX_BYTES).await?;
            let resp: RpcResp = serde_json::from_slice(&hdr)
                .map_err(|e| Error::bad(format!("invalid RPC reply: {e}")))?;
            if let RpcResp::Error { message } = resp {
                return Err(Error::conflict(message));
            }
            Ok((resp, body))
        })
        .await
        .map_err(|_| Error::internal("cluster RPC timed out"))?
    }

    pub async fn handle_rpc(
        self: Arc<Self>,
        from_peer: String,
        mut send: SendStream,
        mut recv: RecvStream,
    ) {
        let res = async {
            let (hdr, _body) = read_frame(&mut recv, RPC_HEADER_MAX_BYTES, 1024).await?;
            let req: RpcReq = serde_json::from_slice(&hdr)
                .map_err(|e| Error::bad(format!("invalid RPC request: {e}")))?;
            self.dispatch_rpc(&from_peer, req).await
        }
        .await;

        match res {
            Ok((resp, bin)) => {
                if let Ok(hdr) = serde_json::to_vec(&resp) {
                    let _ = write_frame(&mut send, &hdr, bin.as_deref()).await;
                }
            }
            Err(err) => {
                tracing::warn!(peer = %from_peer, status = %err.0, error = %err.1, "cluster RPC failed");
                if let Ok(hdr) = serde_json::to_vec(&RpcResp::Error { message: err.1 }) {
                    let _ = write_frame(&mut send, &hdr, None).await;
                }
            }
        }
    }

    async fn dispatch_rpc(
        self: &Arc<Self>,
        from_peer: &str,
        req: RpcReq,
    ) -> Result<(RpcResp, Option<Vec<u8>>)> {
        match req {
            RpcReq::ExchangePeers { self_info, peers } => {
                let addr = self.net.endpoint_addr();
                let (my_info, my_peers, discovered) = self
                    .db
                    .call(move |s| {
                        let now = chrono::Utc::now().timestamp_millis();
                        let local = load_local_node(&s.conn)?;
                        let mut changed = upsert_peer(&s.conn, &local.node_id, &self_info, now)?;
                        for p in peers {
                            changed |= upsert_peer(&s.conn, &local.node_id, &p, now)?;
                        }
                        let my_info = self_peer_info(&local, &addr);
                        let my_peers = list_peers(&s.conn)?;
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
            RpcReq::GetManifest => {
                let heads = self.db.call(|s| local_manifest(&s.conn)).await?;
                Ok((RpcResp::Manifest { heads }, None))
            }
            RpcReq::GetHeads { stream_ids } => {
                let heads = self
                    .db
                    .call(move |s| {
                        let mut out = Vec::new();
                        for id in stream_ids {
                            if let Some(st) = stream_head(&s.conn, &id)? {
                                out.push(StreamHead {
                                    stream_id: st.stream_id,
                                    kind: st.kind,
                                    epoch: st.epoch,
                                    head_seq: st.head_seq,
                                    head_hash: st.head_hash,
                                    deleted: st.deleted,
                                    head_snapshot: st.head_snapshot,
                                    updated_at: st.updated_at,
                                });
                            }
                        }
                        Ok(out)
                    })
                    .await?;
                Ok((RpcResp::Manifest { heads }, None))
            }
            RpcReq::NotifyHeads { heads } => {
                let eng = self.clone();
                let from = from_peer.to_owned();
                tokio::spawn(async move {
                    if let Err(e) = eng.reconcile_head_list(&from, heads).await {
                        tracing::warn!(peer = %from, error = %e, "reconcile pushed heads failed");
                    }
                });
                Ok((RpcResp::Ack, None))
            }
            RpcReq::GetCommits {
                stream_id,
                epoch,
                after_seq,
                max_count,
                max_bytes,
            } => {
                let count = max_count.clamp(1, 256);
                let bytes = max_bytes.clamp(64 * 1024, 64 * 1024 * 1024);
                let (commits, has_more) = self
                    .db
                    .call(move |s| read_commits_range(&s.conn, &stream_id, epoch, after_seq, count, bytes))
                    .await?;
                Ok((RpcResp::Commits { commits, has_more }, None))
            }
            RpcReq::HasAttachments { hashes } => {
                let available = self
                    .db
                    .call(move |s| {
                        let mut stmt = s.conn.prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM attachments WHERE content_hash=? AND data IS NOT NULL)",
                        )?;
                        let mut found = Vec::new();
                        for hash in hashes {
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
                if req_len == 0 || req_len > 2 * 1024 * 1024 {
                    return Err(Error::bad("invalid attachment chunk length"));
                }
                let chunk = self
                    .db
                    .call(move |s| {
                        let data: Vec<u8> = s
                            .conn
                            .prepare_cached(
                                "SELECT data FROM attachments WHERE content_hash=? AND data IS NOT NULL LIMIT 1",
                            )?
                            .query_row([content_hash.as_slice()], |r| r.get(0))?;
                        let start = usize::try_from(offset)
                            .map_err(|_| Error::bad("invalid chunk offset"))?;
                        let end = start.saturating_add(req_len);
                        if start >= data.len() || end > data.len() {
                            return Err(Error::bad("attachment chunk out of range"));
                        }
                        Ok(data[start..end].to_vec())
                    })
                    .await?;
                Ok((RpcResp::AttachmentChunk, Some(chunk)))
            }
            RpcReq::OAuthRefresh {
                server_id,
                generation,
                after_epoch,
                after_seq,
            } => {
                let result = crate::mcp::oauth::refresh_shared_as_coordinator(
                    &self.db,
                    &self.client,
                    server_id,
                    &generation,
                    after_epoch,
                    after_seq,
                )
                .await;
                match result {
                    Ok((head, commits)) => {
                        if let Some(h) = &head {
                            let _ = self.signal_tx.send(SyncSignal::StreamsChanged(vec![h.stream_id.clone()]));
                        }
                        Ok((
                            RpcResp::OAuthRefreshResult {
                                refreshed: true,
                                head,
                                commits,
                                error: None,
                            },
                            None,
                        ))
                    }
                    Err(err) => Ok((
                        RpcResp::OAuthRefreshResult {
                            refreshed: false,
                            head: None,
                            commits: Vec::new(),
                            error: Some(err.1),
                        },
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
        let addr = self.net.endpoint_addr();
        let (self_info, peers) = self
            .db
            .call(move |s| {
                let local = load_local_node(&s.conn)?;
                Ok((self_peer_info(&local, &addr), list_peers(&s.conn)?))
            })
            .await?;
        let (resp, _) = self
            .rpc(
                peer_id,
                &RpcReq::ExchangePeers { self_info, peers },
                RPC_TIMEOUT_SHORT,
            )
            .await?;
        if let RpcResp::Peers { self_info, peers } = resp {
            let discovered = self
                .db
                .call(move |s| {
                    let now = chrono::Utc::now().timestamp_millis();
                    let local = load_local_node(&s.conn)?;
                    let mut changed = upsert_peer(&s.conn, &local.node_id, &self_info, now)?;
                    for p in peers {
                        changed |= upsert_peer(&s.conn, &local.node_id, &p, now)?;
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
                    if let Some(st) = stream_head(&s.conn, &id)? {
                        out.push(StreamHead {
                            stream_id: st.stream_id,
                            kind: st.kind,
                            epoch: st.epoch,
                            head_seq: st.head_seq,
                            head_hash: st.head_hash,
                            deleted: st.deleted,
                            head_snapshot: st.head_snapshot,
                            updated_at: st.updated_at,
                        });
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
        let req = RpcReq::NotifyHeads { heads };
        let req_ref = &req;
        let targets: Vec<String> = online
            .into_iter()
            .filter(|peer_id| exclude_peer != Some(peer_id.as_str()))
            .collect();
        futures_util::future::join_all(
            targets
                .iter()
                .map(|peer_id| async move { self.rpc(peer_id, req_ref, RPC_TIMEOUT_SHORT).await }),
        )
        .await;
    }

    pub async fn sync_now(&self, session_id: Option<SessionId>) -> Result<SyncSummary> {
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
        let _guard = self.reconcile_gate.lock().await;
        let peer_ids = match only_peer {
            Some(p) if self.net.is_peer_online(p) => vec![p.to_owned()],
            Some(_) => return Ok(SyncSummary { peers_queried: 0, streams_updated: 0, conflicts: 0 }),
            None => self.net.online_peer_ids(),
        };
        if peer_ids.is_empty() {
            return Ok(SyncSummary {
                peers_queried: 0,
                streams_updated: 0,
                conflicts: 0,
            });
        }

        let mut peer_manifests: Vec<(String, Vec<StreamHead>)> = Vec::new();
        for peer_id in &peer_ids {
            if let Ok((RpcResp::Manifest { heads }, _)) =
                self.rpc(peer_id, &RpcReq::GetManifest, RPC_TIMEOUT_SHORT).await
            {
                peer_manifests.push((peer_id.clone(), heads));
            }
        }
        let peers_queried = peer_manifests.len();
        let updated = self
            .reconcile_multi_manifests(peer_manifests, watch_sid)
            .await?;
        let conflicts = self
            .db
            .call(|s| {
                let count = s.conn.query_row("SELECT count(*) FROM sync_conflicts", [], |r| {
                    r.get::<_, i64>(0)
                })?;
                Ok(usize::try_from(count).unwrap_or(0))
            })
            .await?;
        Ok(SyncSummary {
            peers_queried,
            streams_updated: updated,
            conflicts,
        })
    }

    async fn reconcile_head_list(&self, from_peer: &str, heads: Vec<StreamHead>) -> Result<usize> {
        let guard = self.reconcile_gate.lock().await;
        let (mut updated_ids, still_blocked) = self
            .reconcile_multi_manifests_with_ids(vec![(from_peer.to_owned(), heads)], None)
            .await?;
        if still_blocked
            && let Ok((RpcResp::Manifest { heads: full_heads }, _)) = self
                .rpc(from_peer, &RpcReq::GetManifest, RPC_TIMEOUT_SHORT)
                .await
        {
            let (more_ids, _) = self
                .reconcile_multi_manifests_with_ids(vec![(from_peer.to_owned(), full_heads)], None)
                .await?;
            updated_ids.extend(more_ids);
            updated_ids.sort();
            updated_ids.dedup();
        }
        drop(guard);
        let count = updated_ids.len();
        if !updated_ids.is_empty() {
            self.broadcast_heads(&updated_ids, Some(from_peer)).await;
        }
        Ok(count)
    }

    async fn reconcile_multi_manifests(
        &self,
        peer_manifests: Vec<(String, Vec<StreamHead>)>,
        watch_sid: Option<SessionId>,
    ) -> Result<usize> {
        let (updated_ids, _) = self
            .reconcile_multi_manifests_with_ids(peer_manifests, watch_sid)
            .await?;
        Ok(updated_ids.len())
    }

    async fn reconcile_multi_manifests_with_ids(
        &self,
        peer_manifests: Vec<(String, Vec<StreamHead>)>,
        watch_sid: Option<SessionId>,
    ) -> Result<(Vec<String>, bool)> {
        enum Phase3Outcome {
            SkipWatched,
            Done(bool),
            Blocked,
        }

        // Aggregate best heads per stream_id and track all peers holding that (epoch, seq, hash).
        #[derive(Clone)]
        struct Candidate {
            head: StreamHead,
            holders: Vec<String>,
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
                } else {
                    list.push(Candidate {
                        head,
                        holders: vec![peer_id.clone()],
                    });
                }
            }
        }

        // Order streams strictly: Provider -> Model -> Mcp -> SessionHeads -> Call -> SessionBodies/Deletes.
        // We run up to 3 passes if dependency-blocked items become unblocked.
        let mut updated_streams = Vec::new();
        let mut resolved_or_done: HashSet<String> = HashSet::new();
        let mut still_blocked = false;

        for _pass in 0..3 {
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
                        // Phase 3: non-deleted session directory snapshot (before Calls for FK);
                        // Phase 5: full commits for watched sessions & tombstones (after Calls for FK).
                        work.push((3, stream_id.clone(), cands.clone()));
                        work.push((5, stream_id.clone(), cands.clone()));
                    }
                    StreamKind::Call => work.push((4, stream_id.clone(), cands.clone())),
                }
            }
            work.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

            for (phase, stream_id, mut cands) in work {
                if resolved_or_done.contains(&stream_id) || blocked_in_pass.contains(&stream_id) {
                    continue;
                }
                // Sort candidates: highest epoch first, then highest seq.
                cands.sort_by(|a, b| {
                    b.head
                        .epoch
                        .cmp(&a.head.epoch)
                        .then_with(|| b.head.head_seq.cmp(&a.head.head_seq))
                });
                let best = &cands[0];
                let force_watch = watch_sid.is_some_and(|sid| sid.as_str() == stream_id);

                if phase == 3 {
                    // Only apply lightweight unwatched session head if NOT deleted and NOT already watched.
                    if best.head.deleted {
                        continue;
                    }
                    let head_clone = best.head.clone();
                    let peer_clone = best.holders[0].clone();
                    let sid = stream_id.clone();
                    let streams = self.streams.clone();
                    let applied = self
                        .db
                        .call(move |s| {
                            let local = stream_head(&s.conn, &sid)?;
                            let is_watched = force_watch
                                || local.as_ref().is_some_and(|l| l.watched || l.synced_seq > 0);
                            if is_watched {
                                return Ok(Phase3Outcome::SkipWatched);
                            }
                            let outcome =
                                apply_unwatched_session_head(&s.conn, &head_clone, &peer_clone)?;
                            match outcome {
                                ApplyOutcome::BlockedOnDependency => Ok(Phase3Outcome::Blocked),
                                ApplyOutcome::Applied { updated, .. } => {
                                    publish_outcome(
                                        s,
                                        &streams,
                                        StreamKind::Session,
                                        &sid,
                                        outcome,
                                    )?;
                                    Ok(Phase3Outcome::Done(updated))
                                }
                                other => {
                                    publish_outcome(
                                        s,
                                        &streams,
                                        StreamKind::Session,
                                        &sid,
                                        other,
                                    )?;
                                    Ok(Phase3Outcome::Done(false))
                                }
                            }
                        })
                        .await?;
                    match applied {
                        Phase3Outcome::SkipWatched => {}
                        Phase3Outcome::Blocked => {
                            blocked_in_pass.insert(stream_id);
                        }
                        Phase3Outcome::Done(changed) => {
                            if changed {
                                updated_streams.push(stream_id.clone());
                                progress = true;
                            }
                            resolved_or_done.insert(stream_id);
                        }
                    }
                    continue;
                }

                // If phase 5 for a session: skip if unwatched and not deleted and not force_watch.
                if phase == 5 && !best.head.deleted && !force_watch {
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

                // Check if multiple distinct remote branches at the same max epoch conflict.
                let mut stream_updated = false;
                let mut stream_blocked = false;
                for cand in &cands {
                    let res = self
                        .pull_and_apply_stream(&cand.head, &cand.holders, force_watch)
                        .await?;
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
        updated_streams.sort();
        updated_streams.dedup();
        Ok((updated_streams, still_blocked))
    }

    pub async fn ensure_session_watched(&self, sid: SessionId) -> Result<()> {
        let stream_id = sid.to_string();

        // Fast check without holding stream_locks.
        let local = self
            .db
            .call(move |s| stream_head(&s.conn, sid.as_str()))
            .await?;
        if let Some(ref l) = local {
            if l.deleted {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            if l.watched && l.synced_seq == l.head_seq {
                return Ok(());
            }
        }

        // Sync eager streams (Providers, Models, Calls) before acquiring stream_locks
        // so lock ordering is strictly reconcile_gate -> stream_locks.
        if !self.net.online_peer_ids().is_empty() {
            let _ = self.sync_with_peers(None, None).await;
        }

        let lock = self.stream_locks.lock_for(stream_id.clone());
        let mut guard = lock.lock().await;

        let local = self
            .db
            .call(move |s| stream_head(&s.conn, sid.as_str()))
            .await?;
        if let Some(ref l) = local {
            if l.deleted {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            if l.watched && l.synced_seq == l.head_seq {
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

        let mut candidates: Vec<(StreamHead, Vec<String>)> = Vec::new();
        for (peer_id, reply) in replies {
            if let Ok((RpcResp::Manifest { heads }, _)) = reply {
                for head in heads {
                    if head.stream_id != stream_id {
                        continue;
                    }
                    if let Some((_, holders)) = candidates.iter_mut().find(|(h, _)| {
                        h.epoch == head.epoch
                            && h.head_seq == head.head_seq
                            && h.head_hash == head.head_hash
                    }) {
                        holders.push(peer_id.clone());
                    } else {
                        candidates.push((head, vec![peer_id.clone()]));
                    }
                }
            }
        }

        if candidates.is_empty() {
            let Some(l) = local else {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            };
            if l.deleted {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            if l.synced_seq < l.head_seq {
                return Err(Error::conflict(
                    "session history is currently unavailable on online peers",
                ));
            }
            if !l.watched {
                self.db
                    .call(move |s| {
                        s.conn.execute(
                            "UPDATE sync_streams SET watched=1 WHERE stream_id=?",
                            [sid.as_str()],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
            return Ok(());
        }

        candidates.sort_by(|a, b| {
            b.0.epoch
                .cmp(&a.0.epoch)
                .then_with(|| b.0.head_seq.cmp(&a.0.head_seq))
        });
        for (head, holders) in candidates {
            match self
                .pull_and_apply_stream_locked(&head, &holders, true)
                .await?
            {
                PullResult::Blocked => {
                    drop(guard);
                    let _ = self.sync_with_peers(None, None).await;
                    guard = lock.lock().await;
                    let _ = self
                        .pull_and_apply_stream_locked(&head, &holders, true)
                        .await?;
                }
                PullResult::Conflict => break,
                PullResult::Updated | PullResult::UpToDate | PullResult::Ignored => {}
            }
        }
        drop(guard);

        let (after, conflicted) = self
            .db
            .call(move |s| {
                let st = stream_head(&s.conn, sid.as_str())?;
                let conflicted: bool = s
                    .conn
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_conflicts WHERE stream_id=?)")?
                    .query_row([sid.as_str()], |r| r.get(0))?;
                Ok((st, conflicted))
            })
            .await?;
        match after {
            Some(l) if l.deleted => Err(rusqlite::Error::QueryReturnedNoRows.into()),
            Some(l) => {
                if l.synced_seq < l.head_seq && !conflicted {
                    return Err(Error::conflict(
                        "session history is currently unavailable on online peers",
                    ));
                }
                if !l.watched {
                    self.db
                        .call(move |s| {
                            s.conn.execute(
                                "UPDATE sync_streams SET watched=1 WHERE stream_id=?",
                                [sid.as_str()],
                            )?;
                            Ok(())
                        })
                        .await?;
                }
                Ok(())
            }
            None => Err(rusqlite::Error::QueryReturnedNoRows.into()),
        }
    }

    async fn pull_and_apply_stream(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        force_watch: bool,
    ) -> Result<PullResult> {
        let lock = self.stream_locks.lock_for(remote_head.stream_id.clone());
        let _guard = lock.lock().await;
        self.pull_and_apply_stream_locked(remote_head, holders, force_watch)
            .await
    }

    async fn pull_and_apply_stream_locked(
        &self,
        remote_head: &StreamHead,
        holders: &[String],
        force_watch: bool,
    ) -> Result<PullResult> {
        let remote_clone = remote_head.clone();
        let primary_peer = holders.first().cloned().unwrap_or_default();
        let prep = self
            .db
            .call(move |s| {
                let local = stream_head(&s.conn, &remote_clone.stream_id)?;
                let Some(l) = local else {
                    return Ok(FetchPlan::Fetch {
                        after_seq: 0,
                        epoch: remote_clone.epoch,
                    });
                };
                if remote_clone.epoch < l.epoch {
                    return Ok(FetchPlan::Done(PullResult::Ignored));
                }
                if remote_clone.epoch > l.epoch {
                    return Ok(FetchPlan::Fetch {
                        after_seq: 0,
                        epoch: remote_clone.epoch,
                    });
                }
                if remote_clone.head_seq == l.head_seq && remote_clone.head_hash == l.head_hash {
                    if force_watch && l.synced_seq < l.head_seq {
                        return Ok(FetchPlan::Fetch {
                            after_seq: l.synced_seq,
                            epoch: l.epoch,
                        });
                    }
                    return Ok(FetchPlan::Done(PullResult::UpToDate));
                }
                if remote_clone.head_seq <= l.synced_seq {
                    if has_commit_hash(
                        &s.conn,
                        &remote_clone.stream_id,
                        l.epoch,
                        remote_clone.head_seq,
                        &remote_clone.head_hash,
                    )? {
                        return Ok(FetchPlan::Done(PullResult::Ignored));
                    }
                    record_fork_conflict(&s.conn, &l, &remote_clone, &primary_peer)?;
                    s.publish_global(
                        "sync.conflict",
                        json!({ "stream_id": remote_clone.stream_id }),
                    );
                    return Ok(FetchPlan::Done(PullResult::Conflict));
                }
                Ok(FetchPlan::Fetch {
                    after_seq: l.synced_seq,
                    epoch: remote_clone.epoch,
                })
            })
            .await?;

        let (mut cursor, target_epoch) = match prep {
            FetchPlan::Done(r) => return Ok(r),
            FetchPlan::Fetch { after_seq, epoch } => (after_seq, epoch),
        };

        let mut any_updated = false;
        let mut batch_idx = 0usize;
        while cursor < remote_head.head_seq {
            let mut fetched = None;
            for attempt in 0..holders.len().max(1) {
                let Some(peer_id) = holders.get((batch_idx + attempt) % holders.len()) else {
                    break;
                };
                let resp = self
                    .rpc(
                        peer_id,
                        &RpcReq::GetCommits {
                            stream_id: remote_head.stream_id.clone(),
                            epoch: target_epoch,
                            after_seq: cursor,
                            max_count: COMMIT_BATCH_COUNT,
                            max_bytes: COMMIT_BATCH_BYTES,
                        },
                        RPC_TIMEOUT_DATA,
                    )
                    .await;
                if let Ok((RpcResp::Commits { commits, .. }, _)) = resp
                    && !commits.is_empty()
                {
                    fetched = Some((peer_id.clone(), commits));
                    break;
                }
            }
            let Some((used_peer, commits)) = fetched else {
                if force_watch {
                    return Err(Error::conflict(format!(
                        "could not fetch commits for {} from online peers",
                        remote_head.stream_id
                    )));
                }
                return Ok(if any_updated {
                    PullResult::Updated
                } else {
                    PullResult::Ignored
                });
            };
            let next_cursor = commits.last().unwrap().seq;
            let head_clone = remote_head.clone();
            let streams = self.streams.clone();
            let outcome = self
                .db
                .call(move |s| {
                    let outcome = apply_remote_commits(
                        &s.conn,
                        &head_clone,
                        &commits,
                        &used_peer,
                        force_watch,
                    )?;
                    let tag = match &outcome {
                        ApplyOutcome::Applied { updated, .. } => {
                            if *updated {
                                PullResult::Updated
                            } else {
                                PullResult::UpToDate
                            }
                        }
                        ApplyOutcome::BlockedOnDependency => PullResult::Blocked,
                        ApplyOutcome::Conflict => PullResult::Conflict,
                        ApplyOutcome::Ignored => PullResult::Ignored,
                    };
                    publish_outcome(
                        s,
                        &streams,
                        head_clone.kind,
                        &head_clone.stream_id,
                        outcome,
                    )?;
                    Ok(tag)
                })
                .await?;
            match outcome {
                PullResult::Updated => {
                    any_updated = true;
                    cursor = next_cursor;
                    batch_idx += 1;
                }
                PullResult::UpToDate => {
                    cursor = next_cursor;
                    batch_idx += 1;
                }
                other => return Ok(other),
            }
        }
        Ok(if any_updated {
            PullResult::Updated
        } else {
            PullResult::UpToDate
        })
    }

    pub async fn ensure_attachment_data(&self, id: AttachmentId) -> Result<()> {
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

        // Re-check local DB (and local content-hash deduplication).
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
        let next_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let holders = Arc::new(holders);
        let results: Arc<Mutex<Vec<Option<Vec<u8>>>>> =
            Arc::new(Mutex::new(vec![None; num_chunks]));

        let worker_count = MAX_CHUNK_CONCURRENCY.min(num_chunks);
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..worker_count {
            let net = self.net.clone();
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
                        let Some(conn) = net.get_connection(peer_id) else {
                            continue;
                        };
                        let res = Self::rpc_on_conn(
                            &conn,
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

        let mut assembled = Vec::with_capacity(total_len);
        for part in results.lock().unwrap().iter_mut() {
            let bytes = part
                .take()
                .ok_or_else(|| Error::internal("missing attachment chunk"))?;
            assembled.extend_from_slice(&bytes);
        }
        if blake3::hash(&assembled).as_bytes() != &content_hash {
            return Err(Error::bad(format!(
                "attachment {id} failed BLAKE3 integrity verification"
            )));
        }
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

    pub async fn ensure_session_attachments(&self, sid: SessionId) -> Result<()> {
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
        match resolution {
            "local" => {
                let sid_owned = stream_id.to_owned();
                let stream_kind = self
                    .db
                    .call(move |s| {
                        let kind = ledger::get_conflict(&s.conn, &sid_owned)?.kind;
                        let mcp_changed = ledger::resolve_conflict_local(&s.conn, &sid_owned)?;
                        if let Some(mid) = mcp_changed {
                            s.mcp.reset_server(mid);
                        }
                        if kind == StreamKind::Session
                            && let Ok(sid) = sid_owned.parse::<SessionId>()
                        {
                            s.wake_waiters(sid);
                            if let Ok(info) = session(&s.conn, sid) {
                                s.publish_global("session.updated", json!(info));
                            }
                        }
                        Ok(kind)
                    })
                    .await?;
                let _ = stream_kind;
                self.broadcast_heads(&[stream_id.to_owned()], None).await;
                Ok(())
            }
            "cluster" => {
                let conflict = self
                    .db
                    .call({
                        let sid = stream_id.to_owned();
                        move |s| ledger::get_conflict(&s.conn, &sid)
                    })
                    .await?;
                let online = self.net.online_peer_ids();
                let mut candidates: Vec<(StreamHead, Vec<String>)> = Vec::new();
                for peer_id in &online {
                    if let Ok((RpcResp::Manifest { heads }, _)) = self
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
                            if let Some((_, holders)) = candidates.iter_mut().find(|(h, _)| {
                                h.epoch == head.epoch
                                    && h.head_seq == head.head_seq
                                    && h.head_hash == head.head_hash
                            }) {
                                holders.push(peer_id.clone());
                            } else {
                                candidates.push((head, vec![peer_id.clone()]));
                            }
                        }
                    }
                }
                if candidates.is_empty() {
                    return Err(Error::conflict(
                        "no online peer holds this stream to overwrite local conflict",
                    ));
                }
                candidates.sort_by(|a, b| {
                    let a_pref = i32::from(a.1.iter().any(|p| p == &conflict.remote_node_id));
                    let b_pref = i32::from(b.1.iter().any(|p| p == &conflict.remote_node_id));
                    b.0.epoch
                        .cmp(&a.0.epoch)
                        .then_with(|| b_pref.cmp(&a_pref))
                        .then_with(|| b.0.head_seq.cmp(&a.0.head_seq))
                });
                let (remote_head, holders) = candidates.remove(0);

                // Fetch entire chain for remote_head.epoch outside SQLite.
                let mut commits = Vec::new();
                let mut cursor = 0i64;
                let mut used_peer = holders[0].clone();
                while cursor < remote_head.head_seq {
                    let mut got = None;
                    for peer_id in &holders {
                        if let Ok((RpcResp::Commits { commits: batch, .. }, _)) = self
                            .rpc(
                                peer_id,
                                &RpcReq::GetCommits {
                                    stream_id: stream_id.to_owned(),
                                    epoch: remote_head.epoch,
                                    after_seq: cursor,
                                    max_count: COMMIT_BATCH_COUNT,
                                    max_bytes: COMMIT_BATCH_BYTES,
                                },
                                RPC_TIMEOUT_DATA,
                            )
                            .await
                            && !batch.is_empty()
                        {
                            got = Some((peer_id.clone(), batch));
                            break;
                        }
                    }
                    let Some((peer_id, batch)) = got else {
                        if commits.is_empty() && remote_head.deleted && remote_head.kind == StreamKind::Session {
                            break;
                        }
                        return Err(Error::conflict(
                            "failed to fetch winning commits from online peers",
                        ));
                    };
                    used_peer = peer_id;
                    cursor = batch.last().unwrap().seq;
                    commits.extend(batch);
                }
                if commits.len() as i64 != remote_head.head_seq
                    && !(commits.is_empty() && remote_head.deleted)
                {
                    return Err(Error::conflict(
                        "incomplete commit chain received for cluster resolution",
                    ));
                }

                let sid_owned = stream_id.to_owned();
                let streams = self.streams.clone();
                self.db
                    .call(move |s| {
                        let (session_id, deleted_session, mcp_changed) = resolve_conflict_cluster(
                            &s.conn,
                            &sid_owned,
                            &remote_head,
                            &commits,
                            &used_peer,
                        )?;
                        if let Some(mid) = mcp_changed {
                            s.mcp.reset_server(mid);
                        }
                        if let Some(sid) = session_id {
                            if deleted_session {
                                s.mcp.forget_session(sid);
                                streams.remove_session(sid);
                                s.channels.remove(&sid);
                                s.wake_sequences.remove(&sid);
                                s.publish_global("session.deleted", json!({ "id": sid }));
                            } else {
                                s.wake_waiters(sid);
                                if let Some(ch) = s.channels.get(&sid).and_then(|w| w.upgrade()) {
                                    crate::events::Notice::publish(&ch, "resync", json!({}));
                                }
                                if let Ok(info) = session(&s.conn, sid) {
                                    s.publish_global("session.updated", json!(info));
                                }
                            }
                        }
                        Ok(())
                    })
                    .await
            }
            _ => Err(Error::bad("resolution must be 'local' or 'cluster'")),
        }
    }

    pub async fn coordinate_mcp_oauth_refresh(
        &self,
        server_id: McpServerId,
        generation: &str,
    ) -> Result<RefreshCoordination> {
        let stream_id = server_id.to_string();
        let (shared, after_epoch, after_seq) = self
            .db
            .call({
                let sid = stream_id.clone();
                move |s| {
                    let shared: bool = s
                        .conn
                        .prepare_cached("SELECT shared FROM mcp_servers WHERE id=?")?
                        .query_row([server_id], |r| r.get(0))
                        .optional()?
                        .unwrap_or(false);
                    let st = stream_head(&s.conn, &sid)?;
                    let (e, seq) = st.map_or((0, 0), |l| (l.epoch, l.synced_seq));
                    Ok((shared, e, seq))
                }
            })
            .await?;
        if !shared {
            return Ok(RefreshCoordination::Local);
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
        let mut participants = vec![self.net.local_node_id().to_owned()];
        for (peer_id, reply) in replies {
            if let Ok((RpcResp::Manifest { heads }, _)) = reply
                && heads.iter().any(|h| !h.deleted)
            {
                participants.push(peer_id);
            }
        }
        participants.sort();
        participants.dedup();
        let coordinator = participants.first().map(String::as_str).unwrap_or("");
        if coordinator == self.net.local_node_id() || coordinator.is_empty() {
            return Ok(RefreshCoordination::Local);
        }

        match self
            .rpc(
                coordinator,
                &RpcReq::OAuthRefresh {
                    server_id,
                    generation: generation.to_owned(),
                    after_epoch,
                    after_seq,
                },
                RPC_TIMEOUT_OAUTH,
            )
            .await
        {
            Ok((
                RpcResp::OAuthRefreshResult {
                    refreshed: true,
                    head: Some(head),
                    commits,
                    ..
                },
                _,
            )) => {
                let lock = self.stream_locks.lock_for(stream_id.clone());
                let _guard = lock.lock().await;
                let coord_id = coordinator.to_owned();
                let need_pull = self
                    .db
                    .call({
                        let head_clone = head.clone();
                        let sid_clone = stream_id.clone();
                        let coord_clone = coord_id.clone();
                        let streams = self.streams.clone();
                        move |s| {
                            let local = stream_head(&s.conn, &sid_clone)?;
                            if let Some(ref l) = local
                                && (l.epoch > head_clone.epoch
                                    || (l.epoch == head_clone.epoch
                                        && l.synced_seq >= head_clone.head_seq))
                            {
                                return Ok(false);
                            }
                            let contiguous = commits.first().is_some_and(|first| match &local {
                                None => first.epoch == head_clone.epoch && first.seq == 1,
                                Some(l) if head_clone.epoch > l.epoch => {
                                    first.epoch == head_clone.epoch && first.seq == 1
                                }
                                Some(l) if head_clone.epoch == l.epoch => {
                                    first.epoch == l.epoch && first.seq == l.synced_seq + 1
                                }
                                _ => false,
                            });
                            if contiguous {
                                let outcome = apply_remote_commits(
                                    &s.conn,
                                    &head_clone,
                                    &commits,
                                    &coord_clone,
                                    true,
                                )?;
                                publish_outcome(
                                    s,
                                    &streams,
                                    StreamKind::Mcp,
                                    &head_clone.stream_id,
                                    outcome,
                                )?;
                                Ok(false)
                            } else {
                                Ok(true)
                            }
                        }
                    })
                    .await?;
                if need_pull {
                    let _ = self
                        .pull_and_apply_stream_locked(&head, &[coord_id], false)
                        .await?;
                }
                Ok(RefreshCoordination::Delegated)
            }
            _ => Ok(RefreshCoordination::Local),
        }
    }
}

enum FetchPlan {
    Done(PullResult),
    Fetch { after_seq: i64, epoch: i64 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PullResult {
    Updated,
    UpToDate,
    Blocked,
    Conflict,
    Ignored,
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
            deleted_session,
            new_items,
            mcp_changed,
        } => {
            if let Some(mid) = mcp_changed {
                s.mcp.reset_server(mid);
            }
            if !updated {
                return Ok(());
            }
            if let Some(sid) = session_id {
                if deleted_session {
                    s.mcp.forget_session(sid);
                    streams.remove_session(sid);
                    s.channels.remove(&sid);
                    s.wake_sequences.remove(&sid);
                    s.publish_global("session.deleted", json!({ "id": sid }));
                } else {
                    s.wake_waiters(sid);
                    for id in new_items {
                        let _ = s.item_saved(sid, id);
                    }
                    if let Ok(info) = session(&s.conn, sid) {
                        let data = json!(info);
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
        ApplyOutcome::Conflict => {
            s.publish_global("sync.conflict", json!({ "stream_id": stream_id }));
            Ok(())
        }
        ApplyOutcome::BlockedOnDependency | ApplyOutcome::Ignored => Ok(()),
    }
}
