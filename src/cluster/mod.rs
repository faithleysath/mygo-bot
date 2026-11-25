pub mod api;
pub mod identity;
pub mod ledger;
pub mod net;
pub mod sync;

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    Result,
    events::Hub,
    id::{AttachmentId, McpServerId, SessionId},
    store::Db,
};

#[derive(Clone, Debug)]
pub enum SyncSignal {
    StreamsChanged(Vec<String>),
    SelfMetaChanged,
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
    pub signal_tx: mpsc::UnboundedSender<SyncSignal>,
    pub signal_rx: mpsc::UnboundedReceiver<SyncSignal>,
    pub config: ClusterConfig,
    pub stopping: CancellationToken,
}

pub struct Cluster {
    pub net: Arc<net::ClusterNet>,
    pub sync: Arc<sync::SyncEngine>,
}

impl Cluster {
    pub async fn start(start: ClusterStart, workers: &TaskTracker) -> Result<Arc<Self>> {
        let local = start
            .db
            .call(|s| identity::load_local_node(&s.conn))
            .await?;
        let (peer_events_tx, peer_events_rx) = mpsc::unbounded_channel();
        let net =
            net::ClusterNet::bind(start.db.clone(), local, start.config, peer_events_tx).await?;
        let sync = sync::SyncEngine::new(
            start.db,
            start.client,
            start.streams,
            net.clone(),
            start.signal_tx,
        );
        net.attach_handler(sync.clone());
        sync.spawn_loops(
            start.signal_rx,
            peer_events_rx,
            start.stopping.clone(),
            workers,
        );
        net.spawn_loops(workers, start.stopping);
        Ok(Arc::new(Self { net, sync }))
    }

    pub async fn shutdown(&self) {
        self.net.shutdown().await;
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
    ) -> Result<sync::RefreshCoordination> {
        self.sync
            .coordinate_mcp_oauth_refresh(server_id, generation)
            .await
    }
}
