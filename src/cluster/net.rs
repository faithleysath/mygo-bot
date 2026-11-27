use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::body::Bytes;
use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey,
    endpoint::{Connection, IdleTimeout, QuicTransportConfig, RecvStream, SendStream, presets},
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    cluster::{
        ClusterSignal,
        cap::CapServer,
        exec::{ExecPlane, ExecServer},
        identity::{
            LocalNode, PeerInfo, decode_endpoint_addr, encode_endpoint_addrs, list_peers,
            load_local_node, sanitize_addrs, self_peer_info, upsert_peer_if_gen,
        },
        sync::SyncEngine,
    },
    store::Db,
};

pub const CLUSTER_ALPN: &[u8] = b"mygo/cluster/1";
pub const CAP_ALPN: &[u8] = b"mygo/cap/1";
pub const EXEC_ALPN: &[u8] = b"mygo/exec/1";
const JOIN_ALPN: &[u8] = b"mygo/join/1";
const MAX_CAP_CONNS_PER_PEER: usize = 4;
const MAX_EXEC_CONNS_PER_PEER: usize = 2;
const HANDSHAKE_MAX_BYTES: usize = 64 * 1024;
pub const RPC_REQ_HEADER_MAX: usize = 2 * 1024 * 1024;
pub const RPC_RESP_HEADER_MAX: usize = 12 * 1024 * 1024;
pub const RPC_BODY_MAX: usize = 2 * 1024 * 1024;
pub const RPC_BYTE_UNIT: usize = 64 * 1024;
pub const MAX_RPC_BYTE_PERMITS: usize = 512; // 32 MiB global RPC frame budget
pub const MAX_GLOBAL_INFLIGHT_RPC: usize = 64;
pub const MAX_PEER_INFLIGHT_RPC: usize = 16;

const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(15);
pub const RPC_READ_DEADLINE: Duration = Duration::from_secs(15);
pub const RPC_WRITE_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AuthMsg {
    Init {
        nonce: [u8; 32],
        peer: PeerInfo,
    },
    Challenge {
        nonce: [u8; 32],
        mac: [u8; 32],
        peer: PeerInfo,
    },
    Finish {
        mac: [u8; 32],
    },
    Ok,
    Admission {
        ok: bool,
        #[serde(default)]
        reason: Option<String>,
    },
}

#[derive(Clone)]
struct PeerEntry {
    conn_id: u64,
    membership_gen: i64,
    conn: Connection,
    is_outbound: bool,
}

#[derive(Clone)]
struct CapPeerEntry {
    conn_id: u64,
    membership_gen: i64,
    conn: Connection,
    peer_cancel: CancellationToken,
}

type AuxConnMap = Mutex<HashMap<String, Vec<CapPeerEntry>>>;
type AuxLockMap = Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>;

struct NetAuthState {
    secret: [u8; 32],
    membership_gen: i64,
    gen_cancel: CancellationToken,
}

#[derive(Default)]
struct PeerConnectState {
    peers: HashSet<String>,
}

#[derive(Clone, Copy)]
enum AuxKind {
    Cap,
    Exec,
}

impl AuxKind {
    fn alpn(self) -> &'static [u8] {
        match self {
            Self::Cap => CAP_ALPN,
            Self::Exec => EXEC_ALPN,
        }
    }

    fn auth_labels(self) -> (&'static [u8], &'static [u8]) {
        match self {
            Self::Cap => (b"mygo-cap-srv", b"mygo-cap-cli"),
            Self::Exec => (b"mygo-exec-srv", b"mygo-exec-cli"),
        }
    }

    fn max_conns(self) -> usize {
        match self {
            Self::Cap => MAX_CAP_CONNS_PER_PEER,
            Self::Exec => MAX_EXEC_CONNS_PER_PEER,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Cap => "cap",
            Self::Exec => "exec",
        }
    }
}

pub struct ClusterNet {
    db: Db,
    // Shutdown stores None if it wins the race with the first bind.
    endpoint: OnceCell<Option<Endpoint>>,
    secret_key: SecretKey,
    config: crate::cluster::ClusterConfig,
    auth: Mutex<NetAuthState>,
    membership_gen_fast: AtomicI64,
    local_node_id: String,
    peers: Mutex<HashMap<String, PeerEntry>>,
    cap_peers: Mutex<HashMap<String, Vec<CapPeerEntry>>>,
    cap_dial_locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    exec_peers: Mutex<HashMap<String, Vec<CapPeerEntry>>>,
    exec_dial_locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    dialing: Mutex<HashSet<String>>,
    next_conn_id: AtomicU64,
    handler: Mutex<Option<Weak<SyncEngine>>>,
    cap_handler: Mutex<Option<Weak<CapServer>>>,
    exec_handler: Mutex<Option<Weak<ExecServer>>>,
    exec_plane: Mutex<Option<Weak<ExecPlane>>>,
    connected_peers: Mutex<PeerConnectState>,
    connect_notify: Notify,
    dial_wake: Notify,
    signal: Arc<ClusterSignal>,
    stopping: CancellationToken,
    tasks: TaskTracker,
    global_rpc_sem: Arc<Semaphore>,
    peer_rpc_sems: Mutex<HashMap<String, Weak<Semaphore>>>,
    rpc_byte_budget: Arc<Semaphore>,
}

fn auth_mac(
    cluster_secret: &[u8; 32],
    label: &[u8],
    first_id: &str,
    second_id: &str,
    first_nonce: &[u8; 32],
    second_nonce: &[u8; 32],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(cluster_secret);
    hasher.update(label);
    hasher.update(first_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(second_id.as_bytes());
    hasher.update(first_nonce);
    hasher.update(second_nonce);
    *hasher.finalize().as_bytes()
}

pub async fn write_frame_timed(
    send: &mut SendStream,
    header: Bytes,
    body: Option<Bytes>,
    deadline: Duration,
) -> Result<()> {
    let h_len = u32::try_from(header.len()).map_err(|_| Error::internal("frame header too large"))?;
    let body = body.unwrap_or_default();
    let b_len = u32::try_from(body.len()).map_err(|_| Error::internal("frame body too large"))?;
    let mut chunks = [
        Bytes::copy_from_slice(&h_len.to_be_bytes()),
        header,
        if b_len == 0 {
            Bytes::from_static(&[0; 4])
        } else {
            Bytes::copy_from_slice(&b_len.to_be_bytes())
        },
        body,
    ];
    tokio::time::timeout(deadline, send.write_all_chunks(&mut chunks))
        .await
        .map_err(|_| Error::internal("cluster stream write timed out"))?
        .map_err(|e| Error::internal(format!("cluster stream write failed: {e}")))?;
    send.finish()
        .map_err(|e| Error::internal(format!("cluster stream finish failed: {e}")))?;
    Ok(())
}

pub enum FrameReadErr {
    Protocol(String),
    Transport(String),
}

pub struct PermittedBytes {
    bytes: Vec<u8>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl PermittedBytes {
    pub fn into_parts(self) -> (Vec<u8>, Option<OwnedSemaphorePermit>) {
        (self.bytes, self._permit)
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::ops::Deref for PermittedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct FramePermits {
    _hdr: OwnedSemaphorePermit,
    _body: Option<OwnedSemaphorePermit>,
}

pub async fn write_json_frame(
    send: &mut SendStream,
    header: &impl Serialize,
    body: Option<&[u8]>,
    label: &str,
) -> std::result::Result<(), String> {
    let hdr_bytes = serde_json::to_vec(header).map_err(|e| format!("{label} encode failed: {e}"))?;
    let h_len = u32::try_from(hdr_bytes.len()).map_err(|_| format!("{label} header too large"))?;
    let b_slice = body.unwrap_or(&[]);
    let b_len = u32::try_from(b_slice.len()).map_err(|_| format!("{label} body too large"))?;
    let mut prefix = Vec::with_capacity(8 + hdr_bytes.len());
    prefix.extend_from_slice(&h_len.to_be_bytes());
    prefix.extend_from_slice(&hdr_bytes);
    prefix.extend_from_slice(&b_len.to_be_bytes());
    send.write_all(&prefix)
        .await
        .map_err(|e| format!("{label} stream write failed: {e}"))?;
    if !b_slice.is_empty() {
        send.write_all(b_slice)
            .await
            .map_err(|e| format!("{label} stream body write failed: {e}"))?;
    }
    Ok(())
}

async fn read_frame_section(
    recv: &mut RecvStream,
    max_len: usize,
    byte_budget: &Arc<Semaphore>,
    byte_unit: usize,
    min_one_permit: bool,
    label: &str,
    part: &str,
) -> std::result::Result<(Vec<u8>, Option<OwnedSemaphorePermit>), FrameReadErr> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| FrameReadErr::Transport(format!("{label} read {part} length failed: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(FrameReadErr::Protocol(format!(
            "{label} frame {part} exceeds limit"
        )));
    }
    if len == 0 && !min_one_permit {
        return Ok((Vec::new(), None));
    }
    let count = len.div_ceil(byte_unit);
    let permits = u32::try_from(if min_one_permit { count.max(1) } else { count })
        .map_err(|_| FrameReadErr::Protocol(format!("{label} {part} permit overflow")))?;
    let permit = byte_budget
        .clone()
        .acquire_many_owned(permits)
        .await
        .map_err(|_| FrameReadErr::Transport(format!("{label} byte budget closed")))?;
    let mut buf = vec![0u8; len];
    if len > 0 {
        recv.read_exact(&mut buf)
            .await
            .map_err(|e| FrameReadErr::Transport(format!("{label} read {part} failed: {e}")))?;
    }
    Ok((buf, Some(permit)))
}

pub async fn read_json_frame<T: serde::de::DeserializeOwned>(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
    byte_budget: &Arc<Semaphore>,
    byte_unit: usize,
    label: &str,
) -> std::result::Result<(T, PermittedBytes), FrameReadErr> {
    let (header, hdr_permit) = read_frame_section(
        recv, max_header, byte_budget, byte_unit, true, label, "header",
    )
    .await?;
    let parsed: T = serde_json::from_slice(&header)
        .map_err(|e| FrameReadErr::Protocol(format!("invalid {label} frame header: {e}")))?;
    drop(header);
    drop(hdr_permit);

    let (body, body_permit) = read_frame_section(
        recv, max_body, byte_budget, byte_unit, false, label, "body",
    )
    .await?;
    Ok((
        parsed,
        PermittedBytes {
            bytes: body,
            _permit: body_permit,
        },
    ))
}

pub async fn read_frame_bounded(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
    byte_budget: &Arc<Semaphore>,
    deadline: Duration,
) -> Result<(Vec<u8>, Vec<u8>, FramePermits)> {
    tokio::time::timeout(deadline, async {
        let (header, hdr_permit) = read_frame_section(
            recv, max_header, byte_budget, RPC_BYTE_UNIT, true, "cluster", "header",
        )
        .await
        .map_err(|e| match e {
            FrameReadErr::Protocol(m) => Error::bad(m),
            FrameReadErr::Transport(m) => Error::internal(m),
        })?;
        let (body, body_permit) = read_frame_section(
            recv, max_body, byte_budget, RPC_BYTE_UNIT, false, "cluster", "body",
        )
        .await
        .map_err(|e| match e {
            FrameReadErr::Protocol(m) => Error::bad(m),
            FrameReadErr::Transport(m) => Error::internal(m),
        })?;
        Ok((
            header,
            body,
            FramePermits {
                _hdr: hdr_permit.expect("header allocates >= 1 permit"),
                _body: body_permit,
            },
        ))
    })
    .await
    .map_err(|_| Error::internal("cluster frame read timed out"))?
}

async fn write_auth(send: &mut SendStream, msg: &AuthMsg) -> Result<()> {
    let bytes = serde_json::to_vec(msg).map_err(|e| Error::internal(e.to_string()))?;
    let len = u32::try_from(bytes.len()).map_err(|_| Error::internal("auth frame too large"))?;
    let mut buf = Vec::with_capacity(4 + bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&bytes);
    send.write_all(&buf)
        .await
        .map_err(|e| Error::internal(format!("auth write failed: {e}")))?;
    Ok(())
}

async fn read_auth(recv: &mut RecvStream) -> Result<AuthMsg> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::internal(format!("auth read failed: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > HANDSHAKE_MAX_BYTES {
        return Err(Error::bad("auth frame exceeds limit"));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| Error::internal(format!("auth body read failed: {e}")))?;
    serde_json::from_slice(&buf).map_err(|_| Error::bad("invalid auth message"))
}

struct DialGuard {
    net: Arc<ClusterNet>,
    peer_id: String,
}

impl Drop for DialGuard {
    fn drop(&mut self) {
        self.net.dialing.lock().unwrap().remove(&self.peer_id);
    }
}

impl ClusterNet {
    pub fn new(
        db: Db,
        local: LocalNode,
        config: crate::cluster::ClusterConfig,
        signal: Arc<ClusterSignal>,
        stopping: CancellationToken,
    ) -> Arc<Self> {
        let gen_cancel = stopping.child_token();
        Arc::new(Self {
            db,
            endpoint: OnceCell::new(),
            secret_key: SecretKey::from_bytes(&local.secret_key),
            config,
            membership_gen_fast: AtomicI64::new(local.membership_gen),
            auth: Mutex::new(NetAuthState {
                secret: local.cluster_secret,
                membership_gen: local.membership_gen,
                gen_cancel,
            }),
            local_node_id: local.node_id,
            peers: Mutex::default(),
            cap_peers: Mutex::default(),
            cap_dial_locks: Mutex::default(),
            exec_peers: Mutex::default(),
            exec_dial_locks: Mutex::default(),
            dialing: Mutex::default(),
            next_conn_id: AtomicU64::new(1),
            handler: Mutex::default(),
            cap_handler: Mutex::default(),
            exec_handler: Mutex::default(),
            exec_plane: Mutex::default(),
            connected_peers: Mutex::default(),
            connect_notify: Notify::new(),
            dial_wake: Notify::new(),
            signal,
            stopping,
            tasks: TaskTracker::new(),
            global_rpc_sem: Arc::new(Semaphore::new(MAX_GLOBAL_INFLIGHT_RPC)),
            peer_rpc_sems: Mutex::default(),
            rpc_byte_budget: Arc::new(Semaphore::new(MAX_RPC_BYTE_PERMITS)),
        })
    }

    async fn bind_endpoint(&self) -> Result<Endpoint> {
        let transport = QuicTransportConfig::builder()
            .keep_alive_interval(Duration::from_secs(2))
            .max_idle_timeout(Some(
                IdleTimeout::try_from(Duration::from_secs(6))
                    .map_err(|e| Error::internal(format!("invalid QUIC idle timeout: {e}")))?,
            ))
            .build();
        let mut builder = if self.config.local_only {
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
        } else {
            Endpoint::builder(presets::N0)
        }
        .secret_key(self.secret_key.clone())
        .transport_config(transport)
        .alpns(vec![
            CLUSTER_ALPN.to_vec(),
            CAP_ALPN.to_vec(),
            EXEC_ALPN.to_vec(),
            JOIN_ALPN.to_vec(),
        ]);
        if let Some(addr) = self.config.bind_addr {
            builder = builder
                .clear_ip_transports()
                .bind_addr(addr)
                .map_err(|e| Error::internal(format!("invalid cluster bind addr {addr}: {e}")))?;
        }
        builder
            .bind()
            .await
            .map_err(|e| Error::internal(format!("failed to bind iroh endpoint: {e}")))
    }

    pub(super) async fn ensure_endpoint(self: &Arc<Self>) -> Result<&Endpoint> {
        let mut starting_loops = None;
        let init = self.endpoint.get_or_try_init(|| async {
            let endpoint = self.bind_endpoint().await?;
            starting_loops = Some(self.tasks.token());
            Ok::<_, Error>(Some(endpoint))
        });
        let stopped = || Error::internal("cluster is shutting down");
        let endpoint = tokio::select! {
            biased;
            _ = self.stopping.cancelled() => return Err(stopped()),
            result = init => result?,
        }
        .as_ref()
        .ok_or_else(stopped)?;
        // Only the initializer starts the loops, after publication. Hold its tracker
        // token until both loops are registered so shutdown can await them.
        if starting_loops.is_some() {
            self.tasks.spawn(self.clone().accept_loop(endpoint.clone()));
            self.tasks.spawn(self.clone().dial_loop(endpoint.clone()));
            self.tasks
                .spawn(self.clone().addr_watch_loop(endpoint.clone()));
        }
        Ok(endpoint)
    }

    async fn addr_watch_loop(self: Arc<Self>, endpoint: Endpoint) {
        use iroh::Watcher as _;
        fn norm_addrs(addr: &EndpointAddr) -> Vec<String> {
            let mut addrs = sanitize_addrs(&encode_endpoint_addrs(addr));
            addrs.sort();
            addrs
        }
        let mut watcher = endpoint.watch_addr();
        let mut last_addrs = norm_addrs(&watcher.get());
        loop {
            let next_addr = tokio::select! {
                biased;
                _ = self.stopping.cancelled() => return,
                _ = endpoint.closed() => return,
                res = watcher.updated() => match res {
                    Ok(addr) => addr,
                    Err(_) => return,
                },
            };
            let mut latest = norm_addrs(&next_addr);
            if latest == last_addrs || latest.is_empty() {
                continue;
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                tokio::select! {
                    biased;
                    _ = self.stopping.cancelled() => return,
                    _ = endpoint.closed() => return,
                    _ = tokio::time::sleep_until(deadline) => break,
                    res = watcher.updated() => match res {
                        Ok(addr) => {
                            let cand = norm_addrs(&addr);
                            if !cand.is_empty() {
                                latest = cand;
                            }
                        }
                        Err(_) => return,
                    },
                }
            }
            if latest != last_addrs && !latest.is_empty() {
                last_addrs = latest;
                self.wake_dialer();
                if !self.online_peer_ids().is_empty() {
                    self.signal.notify_self_meta();
                }
            }
        }
    }

    fn endpoint(&self) -> Option<&Endpoint> {
        self.endpoint.get().and_then(Option::as_ref)
    }

    pub fn attach_handler(&self, handler: Weak<SyncEngine>) {
        *self.handler.lock().unwrap() = Some(handler);
    }

    pub fn attach_cap_handler(&self, handler: Weak<CapServer>) {
        *self.cap_handler.lock().unwrap() = Some(handler);
    }

    pub fn attach_exec_handler(&self, handler: Weak<ExecServer>) {
        *self.exec_handler.lock().unwrap() = Some(handler);
    }

    pub fn attach_exec_plane(&self, plane: Weak<ExecPlane>) {
        *self.exec_plane.lock().unwrap() = Some(plane);
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.endpoint()
            .map_or_else(|| EndpointAddr::new(self.secret_key.public()), Endpoint::addr)
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn is_local_only(&self) -> bool {
        self.config.local_only
    }

    pub fn membership_gen(&self) -> i64 {
        self.membership_gen_fast.load(Ordering::Acquire)
    }

    fn auth_context(&self) -> (i64, [u8; 32], CancellationToken) {
        let st = self.auth.lock().unwrap();
        (st.membership_gen, st.secret, st.gen_cancel.clone())
    }

    pub fn rpc_byte_budget(&self) -> &Arc<Semaphore> {
        &self.rpc_byte_budget
    }

    pub fn global_rpc_sem(&self) -> &Arc<Semaphore> {
        &self.global_rpc_sem
    }

    pub fn peer_rpc_sem(&self, node_id: &str) -> Arc<Semaphore> {
        let mut map = self.peer_rpc_sems.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(sem) = map.get(node_id).and_then(Weak::upgrade) {
            return sem;
        }
        let sem = Arc::new(Semaphore::new(MAX_PEER_INFLIGHT_RPC));
        map.insert(node_id.to_owned(), Arc::downgrade(&sem));
        sem
    }

    pub fn take_connected_peers(&self) -> Vec<String> {
        self.connected_peers.lock().unwrap().peers.drain().collect()
    }

    pub async fn wait_peer_connected(&self) {
        self.connect_notify.notified().await;
    }

    pub fn online_peer_ids(&self) -> Vec<String> {
        let current_gen = self.membership_gen();
        let mut stale = Vec::new();
        let ids: Vec<String> = {
            let mut peers = self.peers.lock().unwrap();
            peers.retain(|id, entry| {
                let keep =
                    entry.membership_gen == current_gen && entry.conn.close_reason().is_none();
                if !keep {
                    stale.push(id.clone());
                }
                keep
            });
            let mut ids: Vec<String> = peers.keys().cloned().collect();
            ids.sort();
            ids
        };
        for id in stale {
            self.notify_peer_offline(&id);
        }
        ids
    }

    pub fn is_peer_online(&self, node_id: &str) -> bool {
        let current_gen = self.membership_gen();
        let removed = {
            let mut peers = self.peers.lock().unwrap();
            if let Some(entry) = peers.get(node_id) {
                if entry.membership_gen == current_gen && entry.conn.close_reason().is_none() {
                    return true;
                }
                peers.remove(node_id).is_some()
            } else {
                false
            }
        };
        if removed {
            self.notify_peer_offline(node_id);
        }
        false
    }

    pub fn get_connection(&self, node_id: &str) -> Option<Connection> {
        let current_gen = self.membership_gen();
        let (conn, removed) = {
            let mut peers = self.peers.lock().unwrap();
            let entry = peers.get(node_id)?;
            if entry.membership_gen == current_gen && entry.conn.close_reason().is_none() {
                (Some(entry.conn.clone()), false)
            } else {
                peers.remove(node_id);
                (None, true)
            }
        };
        if removed {
            self.notify_peer_offline(node_id);
        }
        conn
    }

    fn notify_peer_offline(&self, node_id: &str) {
        self.clear_aux_for_peer(AuxKind::Cap, node_id, 3, b"peer offline");
        self.clear_aux_for_peer(AuxKind::Exec, node_id, 3, b"peer offline");
        self.connected_peers.lock().unwrap().peers.remove(node_id);
        self.peer_rpc_sems.lock().unwrap().remove(node_id);
        let plane = self.exec_plane.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if let Some(plane) = plane {
            plane.on_peer_offline(node_id);
        }
    }

    pub fn wake_dialer(&self) {
        if self.endpoint().is_some() {
            self.dial_wake.notify_one();
        }
    }

    pub fn switch_membership(&self, new_gen: i64, new_secret: [u8; 32]) {
        let old_cancel = {
            let mut st = self.auth.lock().unwrap();
            st.secret = new_secret;
            st.membership_gen = new_gen;
            self.membership_gen_fast.store(new_gen, Ordering::Release);
            std::mem::replace(&mut st.gen_cancel, self.stopping.child_token())
        };
        old_cancel.cancel();
        let entries: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, v)| v).collect();
        for entry in entries {
            entry.conn.close(2u32.into(), b"cluster changed");
        }
        self.drain_aux_all(AuxKind::Cap, 2, b"cluster changed");
        self.drain_aux_all(AuxKind::Exec, 2, b"cluster changed");
        self.dialing.lock().unwrap().clear();
        self.peer_rpc_sems.lock().unwrap().clear();
        self.connected_peers.lock().unwrap().peers.clear();
        let plane = self.exec_plane.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if let Some(plane) = plane {
            plane.on_membership_switched();
        }
        self.dial_wake.notify_one();
    }

    pub fn disconnect_peer(&self, node_id: &str) {
        if let Some(entry) = self.peers.lock().unwrap().remove(node_id) {
            entry.conn.close(3u32.into(), b"peer removed");
        }
        self.notify_peer_offline(node_id);
    }

    pub async fn shutdown(&self) {
        self.stopping.cancel();
        self.auth.lock().unwrap().gen_cancel.cancel();
        *self.handler.lock().unwrap() = None;
        *self.cap_handler.lock().unwrap() = None;
        *self.exec_handler.lock().unwrap() = None;
        *self.exec_plane.lock().unwrap() = None;
        let entries: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, v)| v).collect();
        for entry in entries {
            entry.conn.close(0u32.into(), b"shutdown");
        }
        self.drain_aux_all(AuxKind::Cap, 0, b"shutdown");
        self.drain_aux_all(AuxKind::Exec, 0, b"shutdown");
        if let Some(endpoint) = self.endpoint.get_or_init(|| async { None }).await {
            endpoint.close().await;
        }
        self.tasks.close();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.tasks.wait()).await;
    }

    async fn accept_loop(self: Arc<Self>, endpoint: Endpoint) {
        loop {
            let incoming = tokio::select! {
                _ = self.stopping.cancelled() => break,
                inc = endpoint.accept() => match inc {
                    Some(inc) => inc,
                    None => break,
                },
            };
            let net = self.clone();
            self.tasks.spawn(async move {
                let stop = net.stopping.clone();
                let res = tokio::select! {
                    _ = stop.cancelled() => return,
                    conn_res = async {
                        let connecting = incoming.accept().map_err(|_| ())?;
                        connecting.await.map_err(|_| ())
                    } => conn_res,
                };
                if let Ok(conn) = res {
                    net.handle_incoming_conn(conn).await;
                }
            });
        }
    }

    async fn dial_loop(self: Arc<Self>, endpoint: Endpoint) {
        loop {
            if self.stopping.is_cancelled() || endpoint.is_closed() {
                return;
            }
            let (mgen, _, gen_cancel) = self.auth_context();
            let peers = match self.db.call(|s| list_peers(&s.conn)).await {
                Ok(p) => p,
                Err(_) => break,
            };
            if gen_cancel.is_cancelled() || self.membership_gen() != mgen {
                continue;
            }
            for peer in peers {
                if self.stopping.is_cancelled() || endpoint.is_closed() {
                    return;
                }
                if gen_cancel.is_cancelled() || self.membership_gen() != mgen {
                    break;
                }
                if peer.node_id == self.local_node_id || self.is_peer_online(&peer.node_id) {
                    continue;
                }
                {
                    let mut dialing = self.dialing.lock().unwrap();
                    if !dialing.insert(peer.node_id.clone()) {
                        continue;
                    }
                }
                let guard = DialGuard {
                    net: self.clone(),
                    peer_id: peer.node_id.clone(),
                };
                let net = self.clone();
                let endpoint = endpoint.clone();
                self.tasks.spawn(async move {
                    let _guard = guard;
                    let _ = net.dial_peer(&endpoint, &peer).await;
                });
            }
            tokio::select! {
                _ = self.stopping.cancelled() => return,
                _ = endpoint.closed() => return,
                _ = self.dial_wake.notified() => (),
                _ = tokio::time::sleep(RECONNECT_INTERVAL) => (),
            }
        }
    }

    async fn dial_peer(self: &Arc<Self>, endpoint: &Endpoint, peer: &PeerInfo) -> Result<()> {
        if peer.node_id == self.local_node_id || self.is_peer_online(&peer.node_id) {
            return Ok(());
        }
        let (mgen, cluster_secret, gen_cancel) = self.auth_context();
        let addr = decode_endpoint_addr(&peer.node_id, &peer.addrs)?;
        let conn = tokio::select! {
            _ = gen_cancel.cancelled() => return Err(Error::conflict("membership changed during dial")),
            res = tokio::time::timeout(DIAL_TIMEOUT, endpoint.connect(addr, CLUSTER_ALPN)) => {
                res.map_err(|_| Error::internal("dial timed out"))?
                    .map_err(|e| Error::internal(format!("dial failed: {e}")))?
            }
        };
        let remote_id = conn.remote_id().to_string();
        if remote_id != peer.node_id {
            conn.close(1u32.into(), b"node id mismatch");
            return Err(Error::bad("peer node id mismatch"));
        }
        let res = tokio::select! {
            _ = gen_cancel.cancelled() => {
                conn.close(2u32.into(), b"cluster changed");
                return Err(Error::conflict("membership changed during handshake"));
            }
            r = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                self.authenticate_outbound(&conn, &cluster_secret, b"mygo-auth-srv", b"mygo-auth-cli"),
            ) => r,
        };
        let remote_peer = match res {
            Ok(Ok(info)) => info,
            Ok(Err(e)) => {
                conn.close(1u32.into(), b"auth failed");
                return Err(e);
            }
            Err(_) => {
                conn.close(1u32.into(), b"auth timeout");
                return Err(Error::internal("cluster auth timed out"));
            }
        };
        self.register_peer_conn(conn, remote_peer, true, mgen, gen_cancel)
            .await;
        Ok(())
    }

    pub(super) async fn request_admission(
        self: &Arc<Self>,
        secret: &[u8; 32],
        peer: &PeerInfo,
    ) -> Result<()> {
        let endpoint = self.ensure_endpoint().await?;
        let addr = decode_endpoint_addr(&peer.node_id, &peer.addrs)?;
        let conn = match tokio::time::timeout(
            DIAL_TIMEOUT,
            endpoint.connect(addr, JOIN_ALPN),
        )
        .await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("invite peer unreachable: {e}"),
                ));
            }
            Err(_) => {
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "timed out reaching invite peer".into(),
                ));
            }
        };
        let remote_id = conn.remote_id().to_string();
        if remote_id != peer.node_id {
            conn.close(1u32.into(), b"node id mismatch");
            return Err(Error::bad("peer node id mismatch"));
        }
        let auth = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            self.authenticate_outbound_stream(
                &conn,
                secret,
                b"mygo-join-srv",
                b"mygo-join-cli",
            ),
        )
        .await;
        let (_peer, _send, mut recv) = match auth {
            Ok(Ok(ok)) => ok,
            Ok(Err(e)) => {
                conn.close(1u32.into(), b"auth failed");
                return Err(if e.0 == axum::http::StatusCode::BAD_REQUEST {
                    Error::conflict(e.1)
                } else {
                    Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, e.1)
                });
            }
            Err(_) => {
                conn.close(1u32.into(), b"auth timeout");
                return Err(Error(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "cluster admission timed out".into(),
                ));
            }
        };
        let reply = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_auth(&mut recv)).await;
        conn.close(0u32.into(), b"admission complete");
        match reply {
            Ok(Ok(AuthMsg::Admission { ok: true, .. })) => Ok(()),
            Ok(Ok(AuthMsg::Admission { reason, .. })) => Err(Error::conflict(
                reason.unwrap_or_else(|| "invite peer cannot accept cluster joins".into()),
            )),
            Ok(Ok(_)) => Err(Error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "unexpected admission response".into(),
            )),
            Ok(Err(e)) => Err(if e.0 == axum::http::StatusCode::BAD_REQUEST {
                Error::conflict(e.1)
            } else {
                Error(axum::http::StatusCode::SERVICE_UNAVAILABLE, e.1)
            }),
            Err(_) => Err(Error(
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "cluster admission timed out".into(),
            )),
        }
    }

    async fn handle_incoming_conn(self: Arc<Self>, conn: Connection) {
        let alpn = conn.alpn();
        if alpn == JOIN_ALPN {
            self.admit_joining_peer(conn).await;
            return;
        }
        let (srv_label, cli_label, aux) = if alpn == CLUSTER_ALPN {
            (b"mygo-auth-srv".as_slice(), b"mygo-auth-cli".as_slice(), None)
        } else if alpn == CAP_ALPN {
            let (s, c) = AuxKind::Cap.auth_labels();
            (s, c, Some(AuxKind::Cap))
        } else if alpn == EXEC_ALPN {
            let (s, c) = AuxKind::Exec.auth_labels();
            (s, c, Some(AuxKind::Exec))
        } else {
            conn.close(1u32.into(), b"unsupported alpn");
            return;
        };
        let (mgen, cluster_secret, gen_cancel) = self.auth_context();
        let res = tokio::select! {
            _ = gen_cancel.cancelled() => {
                conn.close(2u32.into(), b"cluster changed");
                return;
            }
            r = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.authenticate_inbound(&conn, &cluster_secret, srv_label, cli_label)) => r,
        };
        let remote_peer = match res {
            Ok(Ok(info)) => info,
            _ => {
                conn.close(1u32.into(), b"unauthorized");
                return;
            }
        };
        if let Some(kind) = aux {
            if remote_peer.node_id == self.local_node_id {
                conn.close(1u32.into(), b"self connection");
                return;
            }
            let _ = self.register_aux_conn(kind, conn, remote_peer.node_id, mgen, gen_cancel);
        } else {
            self.register_peer_conn(conn, remote_peer, false, mgen, gen_cancel)
                .await;
        }
    }

    async fn authenticate_outbound(
        &self,
        conn: &Connection,
        cluster_secret: &[u8; 32],
        srv_label: &[u8],
        cli_label: &[u8],
    ) -> Result<PeerInfo> {
        let (peer, _, _) = self
            .authenticate_outbound_stream(conn, cluster_secret, srv_label, cli_label)
            .await?;
        Ok(peer)
    }

    async fn authenticate_outbound_stream(
        &self,
        conn: &Connection,
        cluster_secret: &[u8; 32],
        srv_label: &[u8],
        cli_label: &[u8],
    ) -> Result<(PeerInfo, SendStream, RecvStream)> {
        let local = self.db.call(|s| load_local_node(&s.conn)).await?;
        let self_info = self_peer_info(&local, &self.endpoint_addr());
        let mut client_nonce = [0u8; 32];
        getrandom::fill(&mut client_nonce).map_err(|_| Error::internal("entropy unavailable"))?;

        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| Error::internal(format!("auth stream open failed: {e}")))?;
        write_auth(
            &mut send,
            &AuthMsg::Init {
                nonce: client_nonce,
                peer: self_info,
            },
        )
        .await?;

        let (server_nonce, server_mac, remote_peer) = match read_auth(&mut recv).await? {
            AuthMsg::Challenge { nonce, mac, peer } => (nonce, mac, peer),
            _ => return Err(Error::bad("unexpected auth challenge")),
        };
        let remote_id = conn.remote_id().to_string();
        if remote_peer.node_id != remote_id {
            return Err(Error::bad("remote peer identity mismatch"));
        }
        let expected_server_mac = auth_mac(
            cluster_secret,
            srv_label,
            &remote_id,
            &local.node_id,
            &client_nonce,
            &server_nonce,
        );
        if !bool::from(server_mac.ct_eq(&expected_server_mac)) {
            return Err(Error::bad("cluster authentication failed"));
        }
        let client_mac = auth_mac(
            cluster_secret,
            cli_label,
            &local.node_id,
            &remote_id,
            &server_nonce,
            &client_nonce,
        );
        write_auth(&mut send, &AuthMsg::Finish { mac: client_mac }).await?;
        let _ = send.finish();
        match read_auth(&mut recv).await? {
            AuthMsg::Ok => Ok((remote_peer, send, recv)),
            _ => Err(Error::bad("cluster authentication rejected")),
        }
    }

    async fn authenticate_inbound(
        &self,
        conn: &Connection,
        cluster_secret: &[u8; 32],
        srv_label: &[u8],
        cli_label: &[u8],
    ) -> Result<PeerInfo> {
        let (remote_peer, mut send, _) = self
            .authenticate_inbound_stream(conn, cluster_secret, srv_label, cli_label)
            .await?;
        let _ = send.finish();
        Ok(remote_peer)
    }

    async fn authenticate_inbound_stream(
        &self,
        conn: &Connection,
        cluster_secret: &[u8; 32],
        srv_label: &[u8],
        cli_label: &[u8],
    ) -> Result<(PeerInfo, SendStream, RecvStream)> {
        let local = self.db.call(|s| load_local_node(&s.conn)).await?;
        let remote_id = conn.remote_id().to_string();
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(|e| Error::internal(format!("auth accept failed: {e}")))?;
        let (client_nonce, remote_peer) = match read_auth(&mut recv).await? {
            AuthMsg::Init { nonce, peer } => (nonce, peer),
            _ => return Err(Error::bad("expected auth init")),
        };
        if remote_peer.node_id != remote_id {
            return Err(Error::bad("remote peer identity mismatch"));
        }
        let mut server_nonce = [0u8; 32];
        getrandom::fill(&mut server_nonce).map_err(|_| Error::internal("entropy unavailable"))?;
        let server_mac = auth_mac(
            cluster_secret,
            srv_label,
            &local.node_id,
            &remote_id,
            &client_nonce,
            &server_nonce,
        );
        let self_info = self_peer_info(&local, &self.endpoint_addr());
        write_auth(
            &mut send,
            &AuthMsg::Challenge {
                nonce: server_nonce,
                mac: server_mac,
                peer: self_info,
            },
        )
        .await?;
        let client_mac = match read_auth(&mut recv).await? {
            AuthMsg::Finish { mac } => mac,
            _ => return Err(Error::bad("expected auth finish")),
        };
        let expected_client_mac = auth_mac(
            cluster_secret,
            cli_label,
            &remote_id,
            &local.node_id,
            &server_nonce,
            &client_nonce,
        );
        if !bool::from(client_mac.ct_eq(&expected_client_mac)) {
            return Err(Error::bad("cluster client authentication failed"));
        }
        write_auth(&mut send, &AuthMsg::Ok).await?;
        Ok((remote_peer, send, recv))
    }

    async fn admit_joining_peer(self: Arc<Self>, conn: Connection) {
        let (mgen, cluster_secret, gen_cancel) = self.auth_context();
        let res = tokio::select! {
            _ = gen_cancel.cancelled() => {
                conn.close(2u32.into(), b"cluster changed");
                return;
            }
            r = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                self.authenticate_inbound_stream(
                    &conn,
                    &cluster_secret,
                    b"mygo-join-srv",
                    b"mygo-join-cli",
                ),
            ) => r,
        };
        let (remote_peer, mut send, _recv) = match res {
            Ok(Ok(ok)) => ok,
            _ => {
                conn.close(1u32.into(), b"unauthorized");
                return;
            }
        };
        if remote_peer.node_id == self.local_node_id {
            conn.close(1u32.into(), b"self connection");
            return;
        }
        // Register before acknowledging so immediate reconnects recognize the joining peer.
        let admission = self
            .db
            .call(move |s| {
                let tx = s.conn.transaction()?;
                let local = load_local_node(&tx)?;
                if local.membership_gen != mgen {
                    return Err(Error::conflict("membership changed during admission"));
                }
                let now = chrono::Utc::now().timestamp_millis();
                upsert_peer_if_gen(&tx, &local.node_id, &remote_peer, now, true, mgen)?;
                let peers = list_peers(&tx)?;
                if !peers.iter().any(|p| p.node_id == remote_peer.node_id) {
                    return Err(Error::bad("invalid joining peer"));
                }
                tx.commit()?;
                s.presence.set_known_peer_count(peers.len());
                Ok(())
            })
            .await;
        let msg = match admission {
            Ok(()) => AuthMsg::Admission {
                ok: true,
                reason: None,
            },
            Err(err) => AuthMsg::Admission {
                ok: false,
                reason: Some(if err.0 == axum::http::StatusCode::CONFLICT {
                    err.1
                } else {
                    "invite source could not admit this peer".into()
                }),
            },
        };
        if write_auth(&mut send, &msg).await.is_ok() {
            let _ = send.finish();
            let _ = tokio::time::timeout(Duration::from_secs(1), send.stopped()).await;
        }
        conn.close(0u32.into(), b"admission complete");
    }

    fn aux_maps(&self, kind: AuxKind) -> (&AuxConnMap, &AuxLockMap) {
        match kind {
            AuxKind::Cap => (&self.cap_peers, &self.cap_dial_locks),
            AuxKind::Exec => (&self.exec_peers, &self.exec_dial_locks),
        }
    }

    fn clear_aux_for_peer(&self, kind: AuxKind, node_id: &str, code: u32, reason: &[u8]) {
        let (peers_map, locks_map) = self.aux_maps(kind);
        if let Some(entries) = peers_map.lock().unwrap().remove(node_id) {
            for entry in entries {
                entry.peer_cancel.cancel();
                entry.conn.close(code.into(), reason);
            }
        }
        locks_map.lock().unwrap().remove(node_id);
    }

    fn drain_aux_all(&self, kind: AuxKind, code: u32, reason: &[u8]) {
        let (peers_map, locks_map) = self.aux_maps(kind);
        let entries: Vec<CapPeerEntry> = peers_map
            .lock()
            .unwrap()
            .drain()
            .flat_map(|(_, v)| v)
            .collect();
        for entry in entries {
            entry.peer_cancel.cancel();
            entry.conn.close(code.into(), reason);
        }
        locks_map.lock().unwrap().clear();
    }

    fn aux_dial_lock(&self, kind: AuxKind, node_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let (_, locks_map) = self.aux_maps(kind);
        let mut map = locks_map.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(lock) = map.get(node_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        map.insert(node_id.to_owned(), Arc::downgrade(&lock));
        lock
    }

    fn pick_aux_conn(
        &self,
        kind: AuxKind,
        node_id: &str,
        mgen: i64,
    ) -> Option<(Connection, CancellationToken)> {
        let (peers_map, _) = self.aux_maps(kind);
        let mut peers = peers_map.lock().unwrap();
        let list = peers.get_mut(node_id)?;
        list.retain(|e| {
            e.membership_gen == mgen
                && !e.peer_cancel.is_cancelled()
                && e.conn.close_reason().is_none()
        });
        if list.is_empty() {
            peers.remove(node_id);
            return None;
        }
        let entry = list.last()?;
        Some((entry.conn.clone(), entry.peer_cancel.clone()))
    }

    fn evict_aux_conn(&self, kind: AuxKind, node_id: &str, conn: &Connection) {
        let (peers_map, _) = self.aux_maps(kind);
        let mut peers = peers_map.lock().unwrap();
        if let Some(list) = peers.get_mut(node_id) {
            let sid = conn.stable_id();
            list.retain(|e| {
                if e.conn.stable_id() == sid {
                    e.peer_cancel.cancel();
                    false
                } else {
                    true
                }
            });
            if list.is_empty() {
                peers.remove(node_id);
            }
        }
    }

    pub fn evict_cap_conn(&self, node_id: &str, conn: &Connection) {
        self.evict_aux_conn(AuxKind::Cap, node_id, conn);
    }

    pub fn evict_exec_conn(&self, node_id: &str, conn: &Connection) {
        self.evict_aux_conn(AuxKind::Exec, node_id, conn);
    }

    pub async fn get_or_dial_cap_connection(
        self: &Arc<Self>,
        peer: &PeerInfo,
        cancel: &CancellationToken,
    ) -> std::result::Result<(Connection, CancellationToken), String> {
        self.get_or_dial_aux_connection(AuxKind::Cap, peer, cancel)
            .await
    }

    pub async fn get_or_dial_exec_connection(
        self: &Arc<Self>,
        peer: &PeerInfo,
        cancel: &CancellationToken,
    ) -> std::result::Result<(Connection, CancellationToken), String> {
        self.get_or_dial_aux_connection(AuxKind::Exec, peer, cancel)
            .await
    }

    async fn get_or_dial_aux_connection(
        self: &Arc<Self>,
        kind: AuxKind,
        peer: &PeerInfo,
        cancel: &CancellationToken,
    ) -> std::result::Result<(Connection, CancellationToken), String> {
        let tag = kind.tag();
        if cancel.is_cancelled() || self.stopping.is_cancelled() {
            return Err("cancelled".into());
        }
        if !self.is_peer_online(&peer.node_id) {
            return Err(format!("peer {} is offline", peer.node_id));
        }
        let (mgen, _, gen_cancel) = self.auth_context();
        if let Some(found) = self.pick_aux_conn(kind, &peer.node_id, mgen) {
            return Ok(found);
        }
        let lock = self.aux_dial_lock(kind, &peer.node_id);
        let _guard = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("cancelled".into()),
            _ = gen_cancel.cancelled() => return Err(format!("membership changed during {tag} dial")),
            g = lock.lock() => g,
        };
        if !self.is_peer_online(&peer.node_id) {
            return Err(format!("peer {} is offline", peer.node_id));
        }
        let (mgen, cluster_secret, gen_cancel) = self.auth_context();
        if let Some(found) = self.pick_aux_conn(kind, &peer.node_id, mgen) {
            return Ok(found);
        }
        let endpoint = self.endpoint().ok_or("cluster endpoint is offline")?;
        let addr = decode_endpoint_addr(&peer.node_id, &peer.addrs).map_err(|e| e.to_string())?;
        let conn = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("cancelled".into()),
            _ = gen_cancel.cancelled() => return Err(format!("membership changed during {tag} dial")),
            res = tokio::time::timeout(DIAL_TIMEOUT, endpoint.connect(addr, kind.alpn())) => {
                res.map_err(|_| format!("{tag} dial to {} timed out", peer.node_id))?
                    .map_err(|e| format!("{tag} dial to {} failed: {e}", peer.node_id))?
            }
        };
        if conn.remote_id().to_string() != peer.node_id {
            conn.close(1u32.into(), b"node id mismatch");
            return Err("peer node id mismatch".into());
        }
        let (srv_label, cli_label) = kind.auth_labels();
        let auth = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                conn.close(1u32.into(), b"cancelled");
                return Err("cancelled".into());
            }
            _ = gen_cancel.cancelled() => {
                conn.close(2u32.into(), b"cluster changed");
                return Err(format!("membership changed during {tag} handshake"));
            }
            r = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                self.authenticate_outbound(&conn, &cluster_secret, srv_label, cli_label),
            ) => r,
        };
        match auth {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                conn.close(1u32.into(), b"auth failed");
                return Err(e.to_string());
            }
            Err(_) => {
                conn.close(1u32.into(), b"auth timeout");
                return Err(format!("{tag} auth timed out"));
            }
        }
        let peer_cancel = self
            .register_aux_conn(kind, conn.clone(), peer.node_id.clone(), mgen, gen_cancel)
            .ok_or_else(|| format!("membership changed during {tag} register"))?;
        Ok((conn, peer_cancel))
    }

    fn register_aux_conn(
        self: &Arc<Self>,
        kind: AuxKind,
        conn: Connection,
        remote_id: String,
        mgen: i64,
        gen_cancel: CancellationToken,
    ) -> Option<CancellationToken> {
        if gen_cancel.is_cancelled() || self.membership_gen() != mgen {
            conn.close(2u32.into(), b"cluster changed");
            return None;
        }
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::AcqRel);
        let peer_cancel = gen_cancel.child_token();
        let evicted: Vec<CapPeerEntry> = {
            if self.membership_gen() != mgen || gen_cancel.is_cancelled() {
                conn.close(2u32.into(), b"cluster changed");
                return None;
            }
            let (peers_map, _) = self.aux_maps(kind);
            let mut peers = peers_map.lock().unwrap();
            let list = peers.entry(remote_id.clone()).or_default();
            list.retain(|e| {
                e.membership_gen == mgen
                    && !e.peer_cancel.is_cancelled()
                    && e.conn.close_reason().is_none()
            });
            list.push(CapPeerEntry {
                conn_id,
                membership_gen: mgen,
                conn: conn.clone(),
                peer_cancel: peer_cancel.clone(),
            });
            let excess = list.len().saturating_sub(kind.max_conns());
            if excess > 0 {
                list.drain(..excess).collect()
            } else {
                Vec::new()
            }
        };
        for old in evicted {
            old.peer_cancel.cancel();
            old.conn.close(4u32.into(), b"connection pool limit");
        }
        let net = self.clone();
        let pc = peer_cancel.clone();
        self.tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = net.stopping.cancelled() => break,
                    _ = pc.cancelled() => break,
                    _ = conn.closed() => break,
                    bi = conn.accept_bi() => {
                        match bi {
                            Ok((send, recv)) => {
                                let from = remote_id.clone();
                                let stream_cancel = pc.clone();
                                match kind {
                                    AuxKind::Cap => {
                                        let handler = net.cap_handler.lock().unwrap().as_ref().and_then(Weak::upgrade);
                                        net.tasks.spawn(async move {
                                            if let Some(handler) = handler {
                                                handler.handle_stream(from, send, recv, stream_cancel).await;
                                            }
                                        });
                                    }
                                    AuxKind::Exec => {
                                        let handler = net.exec_handler.lock().unwrap().as_ref().and_then(Weak::upgrade);
                                        net.tasks.spawn(async move {
                                            if let Some(handler) = handler {
                                                handler.handle_stream(from, send, recv, stream_cancel).await;
                                            }
                                        });
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            pc.cancel();
            let (peers_map, _) = net.aux_maps(kind);
            let mut peers = peers_map.lock().unwrap();
            if let Some(list) = peers.get_mut(&remote_id) {
                list.retain(|e| e.conn_id != conn_id);
                if list.is_empty() {
                    peers.remove(&remote_id);
                }
            }
        });
        Some(peer_cancel)
    }

    async fn register_peer_conn(
        self: &Arc<Self>,
        conn: Connection,
        remote_peer: PeerInfo,
        is_outbound: bool,
        mgen: i64,
        gen_cancel: CancellationToken,
    ) {
        if gen_cancel.is_cancelled() || self.membership_gen() != mgen {
            conn.close(2u32.into(), b"cluster changed");
            return;
        }
        let remote_id = remote_peer.node_id.clone();
        let self_id = self.local_node_id.clone();
        let registered = self
            .db
            .call(move |s| {
                let local = load_local_node(&s.conn)?;
                if local.membership_gen != mgen {
                    return Err(Error::conflict("membership changed during register"));
                }
                let now = chrono::Utc::now().timestamp_millis();
                upsert_peer_if_gen(&s.conn, &self_id, &remote_peer, now, true, mgen)?;
                // Enable write notifications before the first manifest can be read.
                crate::cluster::exec::refresh_known_peer_count(&s.conn, &s.presence)
            })
            .await;
        if registered.is_err() {
            conn.close(2u32.into(), b"cluster changed");
            return;
        }
        if gen_cancel.is_cancelled() || self.membership_gen() != mgen {
            conn.close(2u32.into(), b"cluster changed");
            return;
        }

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::AcqRel);
        let old = {
            if self.membership_gen() != mgen || gen_cancel.is_cancelled() {
                conn.close(2u32.into(), b"cluster changed");
                return;
            }
            let mut peers = self.peers.lock().unwrap();
            if let Some(prev) = peers.get(&remote_id)
                && prev.membership_gen == mgen
                && prev.conn.close_reason().is_none()
            {
                if prev.is_outbound != is_outbound {
                    let keep_prev = (self.local_node_id < remote_id) == prev.is_outbound;
                    if keep_prev {
                        conn.close(4u32.into(), b"duplicate connection");
                        return;
                    }
                }
            }
            peers.insert(
                remote_id.clone(),
                PeerEntry {
                    conn_id,
                    membership_gen: mgen,
                    conn: conn.clone(),
                    is_outbound,
                },
            )
        };
        if let Some(prev) = old {
            prev.conn.close(4u32.into(), b"superseded connection");
        }
        {
            self.connected_peers
                .lock()
                .unwrap()
                .peers
                .insert(remote_id.clone());
        }
        self.connect_notify.notify_one();

        let net = self.clone();
        let reader_remote = remote_id.clone();
        self.tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = net.stopping.cancelled() => break,
                    _ = gen_cancel.cancelled() => break,
                    _ = conn.closed() => break,
                    bi = conn.accept_bi() => {
                        match bi {
                            Ok((send, recv)) => {
                                let handler = net.handler.lock().unwrap().as_ref().and_then(Weak::upgrade);
                                let from = reader_remote.clone();
                                let n = net.clone();
                                let gc = gen_cancel.clone();
                                net.tasks.spawn(async move {
                                    if let Some(handler) = handler {
                                        handler.handle_rpc(from, send, recv, mgen, gc, n).await;
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            let removed = {
                let mut peers = net.peers.lock().unwrap();
                if peers.get(&reader_remote).is_some_and(|e| e.conn_id == conn_id) {
                    peers.remove(&reader_remote).is_some()
                } else {
                    false
                }
            };
            if removed {
                net.notify_peer_offline(&reader_remote);
            }
        });

        let plane = self.exec_plane.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if let Some(plane) = plane {
            plane.on_peer_connected(remote_id).await;
        }
    }
}
