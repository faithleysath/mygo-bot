use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::sync::{Notify, mpsc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    cluster::{
        identity::{
            LocalNode, PeerInfo, decode_endpoint_addr, list_peers, load_local_node, self_peer_info,
            upsert_peer,
        },
        sync::SyncEngine,
    },
    store::Db,
};

pub const CLUSTER_ALPN: &[u8] = b"mygo/cluster/1";
const HANDSHAKE_MAX_BYTES: usize = 64 * 1024;
pub const RPC_HEADER_MAX_BYTES: usize = 80 * 1024 * 1024;
pub const RPC_BODY_MAX_BYTES: usize = 4 * 1024 * 1024;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub enum PeerEvent {
    Connected(String),
    Disconnected(String),
}

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
}

#[derive(Clone)]
struct PeerEntry {
    conn_id: u64,
    conn: Connection,
    is_outbound: bool,
    established_at: Instant,
}

pub struct ClusterNet {
    db: Db,
    endpoint: Endpoint,
    secret: Mutex<[u8; 32]>,
    local_node_id: String,
    peers: Mutex<HashMap<String, PeerEntry>>,
    next_conn_id: AtomicU64,
    handler: Mutex<Option<Arc<SyncEngine>>>,
    peer_events: mpsc::UnboundedSender<PeerEvent>,
    dial_wake: Notify,
    local_only: bool,
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

pub async fn write_frame(send: &mut SendStream, header: &[u8], body: Option<&[u8]>) -> Result<()> {
    let h_len = u32::try_from(header.len()).map_err(|_| Error::internal("frame header too large"))?;
    let b_slice = body.unwrap_or(&[]);
    let b_len = u32::try_from(b_slice.len()).map_err(|_| Error::internal("frame body too large"))?;
    let mut prefix = Vec::with_capacity(8 + header.len());
    prefix.extend_from_slice(&h_len.to_be_bytes());
    prefix.extend_from_slice(header);
    prefix.extend_from_slice(&b_len.to_be_bytes());
    send.write_all(&prefix)
        .await
        .map_err(|e| Error::internal(format!("cluster stream write failed: {e}")))?;
    if !b_slice.is_empty() {
        send.write_all(b_slice)
            .await
            .map_err(|e| Error::internal(format!("cluster stream body write failed: {e}")))?;
    }
    send.finish()
        .map_err(|e| Error::internal(format!("cluster stream finish failed: {e}")))?;
    Ok(())
}

pub async fn read_frame(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::internal(format!("cluster read header length failed: {e}")))?;
    let h_len = u32::from_be_bytes(len_buf) as usize;
    if h_len > max_header {
        return Err(Error::bad("cluster frame header exceeds limit"));
    }
    let mut header = vec![0u8; h_len];
    recv.read_exact(&mut header)
        .await
        .map_err(|e| Error::internal(format!("cluster read header failed: {e}")))?;
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| Error::internal(format!("cluster read body length failed: {e}")))?;
    let b_len = u32::from_be_bytes(len_buf) as usize;
    if b_len > max_body {
        return Err(Error::bad("cluster frame body exceeds limit"));
    }
    let mut body = vec![0u8; b_len];
    if b_len > 0 {
        recv.read_exact(&mut body)
            .await
            .map_err(|e| Error::internal(format!("cluster read body failed: {e}")))?;
    }
    Ok((header, body))
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

impl ClusterNet {
    pub async fn bind(
        db: Db,
        local: LocalNode,
        config: crate::cluster::ClusterConfig,
        peer_events: mpsc::UnboundedSender<PeerEvent>,
    ) -> Result<Arc<Self>> {
        let sk = SecretKey::from_bytes(&local.secret_key);
        let mut builder = if config.local_only {
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
        } else {
            Endpoint::builder(presets::N0)
        }
        .secret_key(sk)
        .alpns(vec![CLUSTER_ALPN.to_vec()]);
        if let Some(addr) = config.bind_addr {
            builder = builder
                .bind_addr(addr)
                .map_err(|e| Error::internal(format!("invalid cluster bind addr {addr}: {e}")))?;
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| Error::internal(format!("failed to bind iroh endpoint: {e}")))?;
        Ok(Arc::new(Self {
            db,
            endpoint,
            secret: Mutex::new(local.cluster_secret),
            local_node_id: local.node_id,
            peers: Mutex::default(),
            next_conn_id: AtomicU64::new(1),
            handler: Mutex::default(),
            peer_events,
            dial_wake: Notify::new(),
            local_only: config.local_only,
        }))
    }

    pub fn attach_handler(&self, handler: Arc<SyncEngine>) {
        *self.handler.lock().unwrap() = Some(handler);
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn is_local_only(&self) -> bool {
        self.local_only
    }

    pub fn online_peer_ids(&self) -> Vec<String> {
        let mut peers = self.peers.lock().unwrap();
        peers.retain(|_, entry| entry.conn.close_reason().is_none());
        let mut ids: Vec<String> = peers.keys().cloned().collect();
        ids.sort();
        ids
    }

    pub fn is_peer_online(&self, node_id: &str) -> bool {
        let mut peers = self.peers.lock().unwrap();
        if let Some(entry) = peers.get(node_id) {
            if entry.conn.close_reason().is_none() {
                return true;
            }
            peers.remove(node_id);
        }
        false
    }

    pub fn get_connection(&self, node_id: &str) -> Option<Connection> {
        let mut peers = self.peers.lock().unwrap();
        let entry = peers.get(node_id)?;
        if entry.conn.close_reason().is_none() {
            Some(entry.conn.clone())
        } else {
            peers.remove(node_id);
            None
        }
    }

    pub fn wake_dialer(&self) {
        self.dial_wake.notify_one();
    }

    pub fn reload_cluster_secret(&self, new_secret: [u8; 32]) {
        *self.secret.lock().unwrap() = new_secret;
        let entries: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, v)| v).collect();
        for entry in entries {
            entry.conn.close(2u32.into(), b"cluster changed");
        }
        self.dial_wake.notify_one();
    }

    pub fn disconnect_peer(&self, node_id: &str) {
        if let Some(entry) = self.peers.lock().unwrap().remove(node_id) {
            entry.conn.close(3u32.into(), b"peer removed");
        }
    }

    pub fn spawn_loops(self: &Arc<Self>, workers: &TaskTracker, stopping: CancellationToken) {
        let net_accept = self.clone();
        let stop_accept = stopping.clone();
        workers.spawn(async move {
            net_accept.accept_loop(stop_accept).await;
        });
        let net_dial = self.clone();
        workers.spawn(async move {
            net_dial.dial_loop(stopping).await;
        });
    }

    pub async fn shutdown(&self) {
        let entries: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, v)| v).collect();
        for entry in entries {
            entry.conn.close(0u32.into(), b"shutdown");
        }
        self.endpoint.close().await;
    }

    async fn accept_loop(self: Arc<Self>, stopping: CancellationToken) {
        loop {
            let incoming = tokio::select! {
                _ = stopping.cancelled() => break,
                inc = self.endpoint.accept() => match inc {
                    Some(inc) => inc,
                    None => break,
                },
            };
            let net = self.clone();
            tokio::spawn(async move {
                if let Ok(connecting) = incoming.accept()
                    && let Ok(conn) = connecting.await
                {
                    net.handle_incoming_conn(conn).await;
                }
            });
        }
    }

    async fn dial_loop(self: Arc<Self>, stopping: CancellationToken) {
        loop {
            if stopping.is_cancelled() || self.endpoint.is_closed() {
                return;
            }
            let peers = match self.db.call(|s| list_peers(&s.conn)).await {
                Ok(p) => p,
                Err(_) => break,
            };
            for peer in peers {
                if stopping.is_cancelled() || self.endpoint.is_closed() {
                    return;
                }
                if self.is_peer_online(&peer.node_id) {
                    continue;
                }
                let net = self.clone();
                tokio::spawn(async move {
                    let _ = net.dial_peer(&peer).await;
                });
            }
            tokio::select! {
                _ = stopping.cancelled() => return,
                _ = self.endpoint.closed() => return,
                _ = self.dial_wake.notified() => (),
                _ = tokio::time::sleep(RECONNECT_INTERVAL) => (),
            }
        }
    }

    pub async fn dial_peer(self: &Arc<Self>, peer: &PeerInfo) -> Result<()> {
        if peer.node_id == self.local_node_id || self.is_peer_online(&peer.node_id) {
            return Ok(());
        }
        let addr = decode_endpoint_addr(&peer.node_id, &peer.addrs)?;
        let conn = tokio::time::timeout(DIAL_TIMEOUT, self.endpoint.connect(addr, CLUSTER_ALPN))
            .await
            .map_err(|_| Error::internal("dial timed out"))?
            .map_err(|e| Error::internal(format!("dial failed: {e}")))?;
        let remote_id = conn.remote_id().to_string();
        if remote_id != peer.node_id {
            conn.close(1u32.into(), b"node id mismatch");
            return Err(Error::bad("peer node id mismatch"));
        }
        let res = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.authenticate_outbound(&conn)).await;
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
        self.register_peer_conn(conn, remote_peer, true).await;
        Ok(())
    }

    async fn handle_incoming_conn(self: Arc<Self>, conn: Connection) {
        if conn.alpn() != CLUSTER_ALPN {
            conn.close(1u32.into(), b"unsupported alpn");
            return;
        }
        let res = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.authenticate_inbound(&conn)).await;
        let remote_peer = match res {
            Ok(Ok(info)) => info,
            _ => {
                conn.close(1u32.into(), b"unauthorized");
                return;
            }
        };
        self.register_peer_conn(conn, remote_peer, false).await;
    }

    async fn authenticate_outbound(&self, conn: &Connection) -> Result<PeerInfo> {
        let local = self.db.call(|s| load_local_node(&s.conn)).await?;
        let cluster_secret = *self.secret.lock().unwrap();
        let self_info = self_peer_info(&local, &self.endpoint.addr());
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
            &cluster_secret,
            b"mygo-auth-srv",
            &remote_id,
            &local.node_id,
            &client_nonce,
            &server_nonce,
        );
        if !bool::from(server_mac.ct_eq(&expected_server_mac)) {
            return Err(Error::bad("cluster authentication failed"));
        }
        let client_mac = auth_mac(
            &cluster_secret,
            b"mygo-auth-cli",
            &local.node_id,
            &remote_id,
            &server_nonce,
            &client_nonce,
        );
        write_auth(&mut send, &AuthMsg::Finish { mac: client_mac }).await?;
        let _ = send.finish();
        match read_auth(&mut recv).await? {
            AuthMsg::Ok => Ok(remote_peer),
            _ => Err(Error::bad("cluster authentication rejected")),
        }
    }

    async fn authenticate_inbound(&self, conn: &Connection) -> Result<PeerInfo> {
        let local = self.db.call(|s| load_local_node(&s.conn)).await?;
        let cluster_secret = *self.secret.lock().unwrap();
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
            &cluster_secret,
            b"mygo-auth-srv",
            &local.node_id,
            &remote_id,
            &client_nonce,
            &server_nonce,
        );
        let self_info = self_peer_info(&local, &self.endpoint.addr());
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
            &cluster_secret,
            b"mygo-auth-cli",
            &remote_id,
            &local.node_id,
            &server_nonce,
            &client_nonce,
        );
        if !bool::from(client_mac.ct_eq(&expected_client_mac)) {
            return Err(Error::bad("cluster client authentication failed"));
        }
        write_auth(&mut send, &AuthMsg::Ok).await?;
        let _ = send.finish();
        Ok(remote_peer)
    }

    async fn register_peer_conn(
        self: &Arc<Self>,
        conn: Connection,
        remote_peer: PeerInfo,
        is_outbound: bool,
    ) {
        let remote_id = remote_peer.node_id.clone();
        let self_id = self.local_node_id.clone();
        let _ = self
            .db
            .call(move |s| {
                let now = chrono::Utc::now().timestamp_millis();
                upsert_peer(&s.conn, &self_id, &remote_peer, now)?;
                Ok(())
            })
            .await;

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::AcqRel);
        let old = {
            let mut peers = self.peers.lock().unwrap();
            if let Some(prev) = peers.get(&remote_id)
                && prev.conn.close_reason().is_none()
                && prev.is_outbound != is_outbound
                && prev.established_at.elapsed() < Duration::from_secs(5)
                && (self.local_node_id < remote_id) != is_outbound
            {
                // Simultaneous dial tie-breaker: deterministically keep outbound on lexicographically smaller node_id.
                conn.close(4u32.into(), b"duplicate connection");
                return;
            }
            peers.insert(
                remote_id.clone(),
                PeerEntry {
                    conn_id,
                    conn: conn.clone(),
                    is_outbound,
                    established_at: Instant::now(),
                },
            )
        };
        if let Some(prev) = old {
            prev.conn.close(4u32.into(), b"superseded connection");
        }
        let _ = self.peer_events.send(PeerEvent::Connected(remote_id.clone()));

        let net = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = conn.closed() => break,
                    bi = conn.accept_bi() => {
                        match bi {
                            Ok((send, recv)) => {
                                let handler = net.handler.lock().unwrap().clone();
                                let from = remote_id.clone();
                                tokio::spawn(async move {
                                    if let Some(handler) = handler {
                                        handler.handle_rpc(from, send, recv).await;
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
                if peers.get(&remote_id).is_some_and(|e| e.conn_id == conn_id) {
                    peers.remove(&remote_id);
                    true
                } else {
                    false
                }
            };
            if removed {
                let _ = net.peer_events.send(PeerEvent::Disconnected(remote_id));
            }
        });
    }
}
