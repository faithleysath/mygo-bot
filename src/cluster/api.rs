use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    App, Error, Result,
    cluster::{
        SyncSignal,
        identity::{
            current_arch, current_os, delete_peer, encode_endpoint_addrs, encode_invite,
            list_peers, load_local_node, update_self_meta,
        },
        ledger::{ConflictInfo, list_conflicts},
        sync::SyncSummary,
    },
    id::SessionId,
};

pub fn routes() -> Router<App> {
    Router::new()
        .route("/cluster", get(status))
        .route("/cluster/self", patch(patch_self))
        .route("/cluster/invite", post(invite))
        .route("/cluster/join", post(join))
        .route("/cluster/peers/{node_id}", delete(remove_peer))
        .route("/cluster/sync", post(sync_now))
        .route("/cluster/conflicts", get(conflicts))
        .route(
            "/cluster/conflicts/{stream_id}/resolve",
            post(resolve_conflict),
        )
}

#[derive(Serialize)]
struct SelfNodeStatus {
    node_id: String,
    name: String,
    description: String,
    os: &'static str,
    arch: &'static str,
    meta_seq: i64,
    addrs: Vec<String>,
    local_only: bool,
}

#[derive(Serialize)]
struct ClusterPeerView {
    node_id: String,
    name: String,
    description: String,
    os: String,
    arch: String,
    meta_seq: i64,
    addrs: Vec<String>,
    last_seen_at: i64,
    online: bool,
}

#[derive(Serialize)]
struct ClusterStatus {
    #[serde(rename = "self")]
    self_node: SelfNodeStatus,
    peers: Vec<ClusterPeerView>,
    conflict_count: usize,
}

async fn status(State(app): State<App>) -> Result<Json<ClusterStatus>> {
    let addrs = encode_endpoint_addrs(&app.cluster.net.endpoint_addr());
    let online_set: std::collections::HashSet<String> =
        app.cluster.net.online_peer_ids().into_iter().collect();
    let local_only = app.cluster.net.is_local_only();
    let (local, peers, conflict_count) = app
        .db
        .call(|s| {
            let local = load_local_node(&s.conn)?;
            let peers = list_peers(&s.conn)?;
            let conflict_count = usize::try_from(
                s.conn
                    .query_row("SELECT count(*) FROM sync_conflicts", [], |r| r.get::<_, i64>(0))?,
            )
            .unwrap_or(0);
            Ok((local, peers, conflict_count))
        })
        .await?;
    let self_node = SelfNodeStatus {
        node_id: local.node_id,
        name: local.name,
        description: local.description,
        os: current_os(),
        arch: current_arch(),
        meta_seq: local.meta_seq,
        addrs,
        local_only,
    };
    let peers = peers
        .into_iter()
        .map(|p| {
            let online = online_set.contains(&p.node_id);
            ClusterPeerView {
                node_id: p.node_id,
                name: p.name,
                description: p.description,
                os: p.os,
                arch: p.arch,
                meta_seq: p.meta_seq,
                addrs: p.addrs,
                last_seen_at: p.last_seen_at,
                online,
            }
        })
        .collect();
    Ok(Json(ClusterStatus {
        self_node,
        peers,
        conflict_count,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchSelf {
    name: Option<String>,
    description: Option<String>,
}

async fn patch_self(
    State(app): State<App>,
    Json(body): Json<PatchSelf>,
) -> Result<Json<SelfNodeStatus>> {
    if body.name.is_none() && body.description.is_none() {
        return Err(Error::bad("expected name or description"));
    }
    let addrs = encode_endpoint_addrs(&app.cluster.net.endpoint_addr());
    let local_only = app.cluster.net.is_local_only();
    let local = app
        .db
        .write(
            move |conn| update_self_meta(conn, body.name, body.description),
            |s, updated| {
                s.sync_signal(SyncSignal::SelfMetaChanged);
                Ok(updated)
            },
        )
        .await?;
    Ok(Json(SelfNodeStatus {
        node_id: local.node_id,
        name: local.name,
        description: local.description,
        os: current_os(),
        arch: current_arch(),
        meta_seq: local.meta_seq,
        addrs,
        local_only,
    }))
}

#[derive(Serialize)]
struct InviteResponse {
    invite: String,
}

async fn invite(State(app): State<App>) -> Result<Json<InviteResponse>> {
    let addr = app.cluster.net.endpoint_addr();
    let invite = app
        .db
        .call(move |s| {
            let local = load_local_node(&s.conn)?;
            encode_invite(&local, &addr)
        })
        .await?;
    Ok(Json(InviteResponse { invite }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinBody {
    invite: String,
    #[serde(default)]
    force: bool,
}

async fn join(State(app): State<App>, Json(body): Json<JoinBody>) -> Result<Json<Value>> {
    let peer = app.cluster.join_cluster(body.invite, body.force).await?;
    Ok(Json(json!({
        "joined": true,
        "peer": {
            "node_id": peer.node_id,
            "name": peer.name,
            "description": peer.description,
            "os": peer.os,
            "arch": peer.arch,
        }
    })))
}

async fn remove_peer(
    State(app): State<App>,
    Path(node_id): Path<String>,
) -> Result<StatusCode> {
    let target = node_id.clone();
    let presence = app.exec.presence().clone();
    let removed = app
        .db
        .write(
            move |conn| {
                let removed = delete_peer(conn, &target)?;
                let count = if removed {
                    crate::cluster::identity::list_peers(conn)?.len()
                } else {
                    0
                };
                Ok((removed, count))
            },
            move |_, (removed, count)| {
                if removed {
                    presence.set_known_peer_count(count);
                }
                Ok(removed)
            },
        )
        .await?;
    if !removed {
        return Err(rusqlite::Error::QueryReturnedNoRows.into());
    }
    app.cluster.net.disconnect_peer(&node_id);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SyncBody {
    session_id: Option<SessionId>,
}

async fn sync_now(
    State(app): State<App>,
    body: Option<Json<SyncBody>>,
) -> Result<Json<SyncSummary>> {
    let sid = body.and_then(|b| b.0.session_id);
    let summary = app.cluster.sync.sync_now(sid).await?;
    Ok(Json(summary))
}

async fn conflicts(State(app): State<App>) -> Result<Json<Vec<ConflictInfo>>> {
    app.db.call(|s| Ok(Json(list_conflicts(&s.conn)?))).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveBody {
    resolution: String,
}

async fn resolve_conflict(
    State(app): State<App>,
    Path(stream_id): Path<String>,
    Json(body): Json<ResolveBody>,
) -> Result<StatusCode> {
    app.cluster
        .sync
        .resolve_conflict(&stream_id, &body.resolution)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
