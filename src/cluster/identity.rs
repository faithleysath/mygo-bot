use std::{fs, net::SocketAddr};

use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use iroh::{EndpointAddr, EndpointId, RelayUrl, SecretKey};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

const INVITE_PREFIX: &str = "mygo-cluster-v1:";

#[derive(Clone)]
pub struct LocalNode {
    pub secret_key: [u8; 32],
    pub node_id: String,
    pub cluster_secret: [u8; 32],
    pub name: String,
    pub description: String,
    pub meta_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub name: String,
    pub description: String,
    pub os: String,
    pub arch: String,
    pub meta_seq: i64,
    pub addrs: Vec<String>,
    #[serde(default)]
    pub last_seen_at: i64,
}

#[derive(Serialize, Deserialize)]
struct InvitePayload {
    cluster_secret: String,
    node: PeerInfo,
}

pub fn current_os() -> &'static str {
    std::env::consts::OS
}

pub fn current_arch() -> &'static str {
    std::env::consts::ARCH
}

fn default_node_name() -> String {
    let raw = std::env::var("MYGO_NODE_NAME")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| fs::read_to_string("/etc/hostname").ok())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|out| out.status.success())
                .and_then(|out| String::from_utf8(out.stdout).ok())
        });
    raw.map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty() && !s.contains('\0') && s.len() <= 128)
        .unwrap_or_else(|| "mygo-node".to_owned())
}

pub fn validate_node_name(name: &str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed != name || trimmed.len() > 128 || trimmed.contains('\0') {
        return Err(Error::bad(
            "node name must be 1..=128 bytes without leading/trailing spaces or NUL",
        ));
    }
    Ok(trimmed.to_owned())
}

pub fn validate_node_description(description: &str) -> Result<String> {
    if description.len() > 1024 || description.contains('\0') {
        return Err(Error::bad(
            "node description must be at most 1024 bytes without NUL",
        ));
    }
    Ok(description.to_owned())
}

pub fn ensure_local_node(conn: &Connection) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM local_node WHERE id=1)",
        [],
        |r| r.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let mut secret_key = [0u8; 32];
    getrandom::fill(&mut secret_key).map_err(|_| Error::internal("entropy unavailable"))?;
    let key = SecretKey::from_bytes(&secret_key);
    let node_id = key.public().to_string();
    let mut cluster_secret = [0u8; 32];
    getrandom::fill(&mut cluster_secret).map_err(|_| Error::internal("entropy unavailable"))?;
    let name = default_node_name();
    conn.execute(
        "INSERT INTO local_node(id,secret_key,node_id,cluster_secret,name,description,meta_seq)
         VALUES(1,?,?,?,?,?,1)",
        params![
            secret_key.as_slice(),
            node_id,
            cluster_secret.as_slice(),
            name,
            ""
        ],
    )?;
    Ok(())
}

pub fn load_local_node(conn: &Connection) -> Result<LocalNode> {
    conn.query_row(
        "SELECT secret_key,node_id,cluster_secret,name,description,meta_seq FROM local_node WHERE id=1",
        [],
        |r| {
            let sk: Vec<u8> = r.get(0)?;
            let cs: Vec<u8> = r.get(2)?;
            let secret_key: [u8; 32] = sk
                .try_into()
                .map_err(|_| rusqlite::Error::InvalidColumnType(0, "secret_key".into(), rusqlite::types::Type::Blob))?;
            let cluster_secret: [u8; 32] = cs
                .try_into()
                .map_err(|_| rusqlite::Error::InvalidColumnType(2, "cluster_secret".into(), rusqlite::types::Type::Blob))?;
            Ok(LocalNode {
                secret_key,
                node_id: r.get(1)?,
                cluster_secret,
                name: r.get(3)?,
                description: r.get(4)?,
                meta_seq: r.get(5)?,
            })
        },
    )
    .map_err(Into::into)
}

pub fn update_self_meta(
    conn: &Connection,
    name: Option<String>,
    description: Option<String>,
) -> Result<LocalNode> {
    let current = load_local_node(conn)?;
    let next_name = match name {
        Some(n) => validate_node_name(&n)?,
        None => current.name,
    };
    let next_desc = match description {
        Some(d) => validate_node_description(&d)?,
        None => current.description,
    };
    conn.execute(
        "UPDATE local_node SET name=?1, description=?2, meta_seq=meta_seq+1 WHERE id=1",
        params![next_name, next_desc],
    )?;
    load_local_node(conn)
}

pub fn encode_endpoint_addrs(addr: &EndpointAddr) -> Vec<String> {
    let mut out = Vec::new();
    for relay in addr.relay_urls() {
        out.push(format!("relay:{relay}"));
    }
    for ip in addr.ip_addrs() {
        out.push(format!("ip:{ip}"));
    }
    out
}

pub fn decode_endpoint_addr(node_id: &str, addrs: &[String]) -> Result<EndpointAddr> {
    let id: EndpointId = node_id
        .parse()
        .map_err(|_| Error::bad("invalid peer node_id"))?;
    let mut out = EndpointAddr::new(id);
    for entry in addrs {
        if let Some(raw) = entry.strip_prefix("relay:") {
            if let Ok(relay) = raw.parse::<RelayUrl>() {
                out = out.with_relay_url(relay);
            }
        } else if let Some(raw) = entry.strip_prefix("ip:")
            && let Ok(sock) = raw.parse::<SocketAddr>()
        {
            out = out.with_ip_addr(sock);
        }
    }
    Ok(out)
}

pub fn self_peer_info(local: &LocalNode, addr: &EndpointAddr) -> PeerInfo {
    PeerInfo {
        node_id: local.node_id.clone(),
        name: local.name.clone(),
        description: local.description.clone(),
        os: current_os().to_owned(),
        arch: current_arch().to_owned(),
        meta_seq: local.meta_seq,
        addrs: encode_endpoint_addrs(addr),
        last_seen_at: chrono::Utc::now().timestamp_millis(),
    }
}

pub fn encode_invite(local: &LocalNode, addr: &EndpointAddr) -> Result<String> {
    let payload = InvitePayload {
        cluster_secret: BASE64_URL_SAFE_NO_PAD.encode(local.cluster_secret),
        node: self_peer_info(local, addr),
    };
    let json = serde_json::to_vec(&payload)
        .map_err(|e| Error::internal(format!("failed to encode invite: {e}")))?;
    Ok(format!(
        "{INVITE_PREFIX}{}",
        BASE64_URL_SAFE_NO_PAD.encode(json)
    ))
}

pub fn decode_invite(invite: &str) -> Result<([u8; 32], PeerInfo)> {
    let encoded = invite
        .trim()
        .strip_prefix(INVITE_PREFIX)
        .ok_or_else(|| Error::bad("invalid cluster invite format"))?;
    if encoded.len() > 32 * 1024 {
        return Err(Error::bad("cluster invite is too large"));
    }
    let raw = BASE64_URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| Error::bad("invalid cluster invite encoding"))?;
    let payload: InvitePayload =
        serde_json::from_slice(&raw).map_err(|_| Error::bad("invalid cluster invite payload"))?;
    let secret_vec = BASE64_URL_SAFE_NO_PAD
        .decode(&payload.cluster_secret)
        .map_err(|_| Error::bad("invalid cluster secret in invite"))?;
    let secret: [u8; 32] = secret_vec
        .try_into()
        .map_err(|_| Error::bad("invalid cluster secret length"))?;
    payload
        .node
        .node_id
        .parse::<EndpointId>()
        .map_err(|_| Error::bad("invalid node_id in invite"))?;
    validate_node_name(&payload.node.name)?;
    validate_node_description(&payload.node.description)?;
    Ok((secret, payload.node))
}

pub fn apply_join(conn: &Connection, invite: &str) -> Result<(bool, [u8; 32], PeerInfo)> {
    let (new_secret, peer) = decode_invite(invite)?;
    let local = load_local_node(conn)?;
    if peer.node_id == local.node_id {
        return Err(Error::bad("cannot join own node invite"));
    }
    let secret_changed = local.cluster_secret != new_secret;
    if secret_changed {
        conn.execute(
            "UPDATE local_node SET cluster_secret=? WHERE id=1",
            [new_secret.as_slice()],
        )?;
        conn.execute("DELETE FROM cluster_peers", [])?;
    }
    let now = chrono::Utc::now().timestamp_millis();
    upsert_peer(conn, &local.node_id, &peer, now)?;
    Ok((secret_changed, new_secret, peer))
}

pub fn upsert_peer(
    conn: &Connection,
    self_node_id: &str,
    peer: &PeerInfo,
    now: i64,
) -> Result<bool> {
    if peer.node_id == self_node_id || peer.node_id.parse::<EndpointId>().is_err() {
        return Ok(false);
    }
    if validate_node_name(&peer.name).is_err() || validate_node_description(&peer.description).is_err() {
        return Ok(false);
    }
    let sanitized_addrs: Vec<String> = peer
        .addrs
        .iter()
        .filter(|a| {
            a.strip_prefix("relay:")
                .is_some_and(|r| r.parse::<RelayUrl>().is_ok())
                || a.strip_prefix("ip:")
                    .is_some_and(|ip| ip.parse::<SocketAddr>().is_ok())
        })
        .take(32)
        .cloned()
        .collect();
    let addrs_json = serde_json::to_string(&sanitized_addrs)
        .map_err(|e| Error::internal(format!("failed to encode peer addrs: {e}")))?;

    let existing: Option<(i64, String)> = conn
        .prepare_cached("SELECT meta_seq, addrs_json FROM cluster_peers WHERE node_id=?")?
        .query_row([&peer.node_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;

    let seen = peer.last_seen_at.max(now);
    match existing {
        None => {
            conn.execute(
                "INSERT INTO cluster_peers(node_id,name,description,os,arch,meta_seq,addrs_json,last_seen_at)
                 VALUES(?,?,?,?,?,?,?,?)",
                params![
                    peer.node_id,
                    peer.name,
                    peer.description,
                    peer.os,
                    peer.arch,
                    peer.meta_seq,
                    addrs_json,
                    seen
                ],
            )?;
            Ok(true)
        }
        Some((old_seq, old_addrs)) => {
            let next_addrs = if sanitized_addrs.is_empty() {
                &old_addrs
            } else {
                &addrs_json
            };
            if peer.meta_seq > old_seq {
                conn.execute(
                    "UPDATE cluster_peers SET name=?2, description=?3, os=?4, arch=?5, meta_seq=?6, addrs_json=?7, last_seen_at=max(last_seen_at, ?8)
                     WHERE node_id=?1",
                    params![
                        peer.node_id,
                        peer.name,
                        peer.description,
                        peer.os,
                        peer.arch,
                        peer.meta_seq,
                        next_addrs,
                        seen
                    ],
                )?;
                Ok(next_addrs != &old_addrs)
            } else {
                conn.execute(
                    "UPDATE cluster_peers SET addrs_json=?2, last_seen_at=max(last_seen_at, ?3)
                     WHERE node_id=?1",
                    params![peer.node_id, next_addrs, seen],
                )?;
                Ok(next_addrs != &old_addrs)
            }
        }
    }
}

pub fn list_peers(conn: &Connection) -> Result<Vec<PeerInfo>> {
    let mut stmt = conn.prepare_cached(
        "SELECT node_id,name,description,os,arch,meta_seq,addrs_json,last_seen_at
         FROM cluster_peers ORDER BY name, node_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let addrs_raw: String = r.get(6)?;
            let addrs: Vec<String> = serde_json::from_str(&addrs_raw).unwrap_or_default();
            Ok(PeerInfo {
                node_id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                os: r.get(3)?,
                arch: r.get(4)?,
                meta_seq: r.get(5)?,
                addrs,
                last_seen_at: r.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn delete_peer(conn: &Connection, node_id: &str) -> Result<bool> {
    let changed = conn.execute("DELETE FROM cluster_peers WHERE node_id=?", [node_id])?;
    Ok(changed > 0)
}
