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

const MAX_STAGED_SIGNALS: usize = 8192;

#[derive(Clone, Debug)]
pub enum SyncSignal {
    SelfMetaChanged,
}

#[derive(Default)]
struct SignalState {
    streams: HashSet<String>,
    self_meta: bool,
    full_sync_needed: bool,
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

    pub fn notify_streams(&self, ids: Vec<String>) {
        if ids.is_empty() {
            return;
        }
        {
            let mut st = self.state.lock().unwrap();
            if !st.full_sync_needed {
                if st.streams.len().saturating_add(ids.len()) > MAX_STAGED_SIGNALS {
                    st.streams.clear();
                    st.full_sync_needed = true;
                } else {
                    st.streams.extend(ids);
                }
            }
        }
        self.notify.notify_one();
    }

    pub fn notify_self_meta(&self) {
        self.state.lock().unwrap().self_meta = true;
        self.notify.notify_one();
    }

    pub fn take(&self) -> (Vec<String>, bool, bool) {
        let mut st = self.state.lock().unwrap();
        let streams: Vec<String> = st.streams.drain().collect();
        let self_meta = std::mem::take(&mut st.self_meta);
        let full_sync_needed = std::mem::take(&mut st.full_sync_needed);
        (streams, self_meta, full_sync_needed)
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
        let net = net::ClusterNet::bind(
            start.db.clone(),
            local,
            start.config,
            start.stopping.clone(),
        )
        .await?;
        let sync = sync::SyncEngine::new(
            start.db.clone(),
            start.client,
            start.streams,
            net.clone(),
            start.signal.clone(),
            start.presence.clone(),
        );
        net.attach_handler(Arc::downgrade(&sync));
        sync.spawn_loops(start.stopping.clone(), workers);
        net.spawn_loops(workers);
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
        let db = self.db.clone();
        let net = self.net.clone();
        let sync = self.sync.clone();
        let join_gate = self.join_gate.clone();
        let presence = self.presence.clone();
        self.workers
            .spawn(async move {
                let _guard = join_gate.lock().await;
                let local_only = net.is_local_only();
                let presence_cb = presence.clone();
                let (secret_changed, new_gen, new_secret, peer, peer_count) = db
                    .write(
                        move |conn| {
                            let (changed, next_gen, secret, peer) =
                                identity::apply_join(conn, &invite, local_only, force)?;
                            let count = identity::list_peers(conn)?.len();
                            Ok((changed, next_gen, secret, peer, count))
                        },
                        move |_, res| {
                            presence_cb.set_known_peer_count(res.4);
                            Ok(res)
                        },
                    )
                    .await?;
                presence.set_known_peer_count(peer_count);
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
