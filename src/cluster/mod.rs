pub mod api;
pub mod cap;
pub mod exec;
pub mod identity;
pub mod ledger;
pub mod net;
pub mod sync;

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Error, Result,
    events::Hub,
    id::{AttachmentId, McpServerId, SessionId},
    store::Db,
};

const MAX_STAGED_SESSIONS: usize = 4096;

#[derive(Clone, Debug)]
pub enum SyncSignal {
    SelfMetaChanged,
}

#[derive(Default)]
struct SignalState {
    catalog: bool,
    sessions: HashSet<SessionId>,
    self_meta: bool,
    full_sync: bool,
}

#[derive(Default)]
pub struct DirtyBatch {
    pub catalog: bool,
    pub sessions: Vec<SessionId>,
    pub self_meta: bool,
    pub full_sync: bool,
}

pub struct ClusterSignal {
    state: Mutex<SignalState>,
    notify: Notify,
}

impl Default for ClusterSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterSignal {
    pub fn new() -> Self {
        Self {
            state: Mutex::default(),
            notify: Notify::new(),
        }
    }

    pub fn notify_changes(&self, catalog: bool, sessions: Vec<SessionId>) {
        if !catalog && sessions.is_empty() {
            return;
        }
        {
            let mut st = self.state.lock().unwrap();
            if catalog {
                st.catalog = true;
            }
            if !st.full_sync && !sessions.is_empty() {
                if st.sessions.len().saturating_add(sessions.len()) > MAX_STAGED_SESSIONS {
                    st.sessions.clear();
                    st.full_sync = true;
                } else {
                    st.sessions.extend(sessions);
                }
            }
        }
        self.notify.notify_one();
    }

    pub fn notify_self_meta(&self) {
        self.state.lock().unwrap().self_meta = true;
        self.notify.notify_one();
    }

    pub fn take(&self) -> DirtyBatch {
        let mut st = self.state.lock().unwrap();
        DirtyBatch {
            catalog: std::mem::take(&mut st.catalog),
            sessions: st.sessions.drain().collect(),
            self_meta: std::mem::take(&mut st.self_meta),
            full_sync: std::mem::take(&mut st.full_sync),
        }
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClusterConfig {
    pub local_only: bool,
    pub bind_addr: Option<SocketAddr>,
}

pub struct ClusterStart {
    pub db: Db,
    pub client: reqwest::Client,
    pub streams: Hub,
    pub signal: Arc<ClusterSignal>,
    pub presence: Arc<exec::PresenceState>,
    pub config: ClusterConfig,
    pub stopping: CancellationToken,
}

pub struct Cluster {
    db: Db,
    workers: TaskTracker,
    join_gate: Arc<AsyncMutex<()>>,
    presence: Arc<exec::PresenceState>,
    pub net: Arc<net::ClusterNet>,
    pub sync: Arc<sync::SyncEngine>,
}

impl Cluster {
    pub async fn start(start: ClusterStart, workers: &TaskTracker) -> Result<Arc<Self>> {
        let local = start
            .db
            .call(|s| identity::load_local_node(&s.conn))
            .await?;
        let bind_on_start =
            start.presence.known_peer_count() > 0 || start.config.bind_addr.is_some();
        let net = net::ClusterNet::new(
            start.db.clone(),
            local,
            start.config,
            start.stopping.clone(),
        );
        let sync = sync::SyncEngine::new(
            start.db.clone(),
            start.client,
            start.streams,
            net.clone(),
            start.signal.clone(),
        );
        net.attach_handler(Arc::downgrade(&sync));
        if bind_on_start {
            net.ensure_endpoint().await?;
        }
        sync.spawn_loops(start.stopping.clone(), workers);
        Ok(Arc::new(Self {
            db: start.db,
            workers: workers.clone(),
            join_gate: Arc::new(AsyncMutex::new(())),
            presence: start.presence,
            net,
            sync,
        }))
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
    }

    pub async fn join_cluster(&self, invite: String, force: bool) -> Result<identity::PeerInfo> {
        let (secret, peer) = identity::decode_invite(&invite)?;
        if self.net.is_local_only() && !identity::has_direct_ip(&peer.addrs) {
            return Err(Error::bad(
                "local-only cluster invite must include at least one direct IP address",
            ));
        }
        if peer.node_id == self.net.local_node_id() {
            return Err(Error::bad("cannot join own node invite"));
        }
        let db = self.db.clone();
        let net = self.net.clone();
        let sync = self.sync.clone();
        let join_gate = self.join_gate.clone();
        let presence = self.presence.clone();
        self.workers
            .spawn(async move {
                let _guard = join_gate.lock().await;
                let _joining = presence.begin_join();
                let precheck_secret = secret;
                db.call(move |s| {
                    if !s.active.is_empty() || !s.pending_wakes.is_empty() || !s.tasks.is_empty() {
                        return Err(Error::conflict(
                            "cannot join cluster while sessions or background tasks are active; pause or stop them first",
                        ));
                    }
                    let local = identity::load_local_node(&s.conn)?;
                    let has_peers: bool = s.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM cluster_peers)",
                        [],
                        |r| r.get(0),
                    )?;
                    if has_peers && local.cluster_secret != precheck_secret && !force {
                        return Err(Error::conflict(
                            "node already belongs to another cluster; pass force=true to switch clusters",
                        ));
                    }
                    Ok(())
                })
                .await?;
                let (epoch, voters) = net.request_admission(&secret, &peer).await?;
                let (secret_changed, new_gen, new_secret, peer, _) = db
                    .write(
                        move |conn| {
                            let prev_local = identity::load_local_node(conn)?;
                            let want_voter = prev_local.role.is_voter();
                            let (changed, next_gen, secret, peer) =
                                identity::apply_join(conn, secret, peer, force)?;
                            let (local, _) = identity::adopt_voter_config(conn, epoch, &voters)?;
                            let peers = identity::list_peers(conn)?;
                            Ok((changed, next_gen, secret, peer, peers, local, want_voter))
                        },
                        move |_, (changed, next_gen, secret, peer, peers, local, want_voter)| {
                            let count = peers.len();
                            if changed {
                                presence.set_want_voter_admission(want_voter && !local.role.is_voter());
                                presence.clear_all_remote();
                            }
                            presence.sync_local_node(&local);
                            presence.set_peer_roles(&peers);
                            presence.set_known_peer_count(count);
                            Ok((changed, next_gen, secret, peer, count))
                        },
                    )
                    .await?;
                if secret_changed {
                    net.switch_membership(new_gen, new_secret);
                    sync.on_membership_switched();
                } else {
                    net.wake_dialer();
                }
                Ok(peer)
            })
            .await
            .map_err(|e| Error::internal(format!("join task aborted: {e}")))?
    }

    pub async fn ensure_session_watched(&self, sid: SessionId) -> Result<()> {
        self.sync.ensure_session_watched(sid).await
    }

    pub async fn ensure_attachment_data(&self, id: AttachmentId) -> Result<()> {
        self.sync.ensure_attachment_data(id).await
    }

    pub async fn ensure_session_attachments(&self, sid: SessionId) -> Result<()> {
        self.sync.ensure_session_attachments(sid).await
    }

    pub async fn coordinate_mcp_oauth_refresh(
        &self,
        server_id: McpServerId,
        generation: &str,
        rejected_token_hash: Option<[u8; 32]>,
    ) -> Result<sync::RefreshCoordination> {
        self.sync
            .coordinate_mcp_oauth_refresh(server_id, generation, rejected_token_hash)
            .await
    }
}
