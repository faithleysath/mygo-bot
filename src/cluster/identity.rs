use std::{collections::HashSet, fs, net::SocketAddr};

use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use iroh::{EndpointAddr, EndpointId, RelayUrl, SecretKey};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

const INVITE_PREFIX: &str = "mygo-cluster-v1:";
pub const MAX_CLUSTER_PEERS: usize = 256;
pub const MAX_PEX_PEERS: usize = 64;
const MAX_ADDRS_PER_PEER: usize = 16;

#[derive(Clone)]
pub struct LocalNode {
    pub secret_key: [u8; 32],
    pub node_id: String,
    pub cluster_secret: [u8; 32],
    pub name: String,
    pub description: String,
    pub meta_seq: i64,
    pub membership_gen: i64,
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
    pub addrs_ts: i64,
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

fn validate_peer_fields(peer: &PeerInfo) -> Result<()> {
    validate_node_name(&peer.name)?;
    validate_node_description(&peer.description)?;
    if peer.os.len() > 64
        || peer.os.contains('\0')
        || peer.arch.len() > 64
        || peer.arch.contains('\0')
    {
        return Err(Error::bad("invalid peer os/arch metadata"));
    }
    if peer.meta_seq < 0 || peer.addrs.len() > MAX_ADDRS_PER_PEER * 4 {
        return Err(Error::bad("invalid peer metadata sequence or address list"));
    }
    Ok(())
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
        "INSERT INTO local_node(id,secret_key,node_id,cluster_secret,name,description,meta_seq,membership_gen)
         VALUES(1,?,?,?,?,?,1,1)",
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
        "SELECT secret_key,node_id,cluster_secret,name,description,meta_seq,membership_gen
         FROM local_node WHERE id=1",
        [],
        |r| {
            let sk: Vec<u8> = r.get(0)?;
            let cs: Vec<u8> = r.get(2)?;
            let secret_key: [u8; 32] = sk.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(0, "secret_key".into(), rusqlite::types::Type::Blob)
            })?;
            let cluster_secret: [u8; 32] = cs.try_into().map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    2,
                    "cluster_secret".into(),
                    rusqlite::types::Type::Blob,
                )
            })?;
            Ok(LocalNode {
                secret_key,
                node_id: r.get(1)?,
                cluster_secret,
                name: r.get(3)?,
                description: r.get(4)?,
                meta_seq: r.get(5)?,
                membership_gen: r.get(6)?,
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

pub fn sanitize_addrs(addrs: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for a in addrs {
        if a.len() > 512 {
            continue;
        }
        let valid = a
            .strip_prefix("relay:")
            .is_some_and(|r| r.parse::<RelayUrl>().is_ok())
            || a.strip_prefix("ip:")
                .is_some_and(|ip| ip.parse::<SocketAddr>().is_ok());
        if valid && seen.insert(a.clone()) {
            out.push(a.clone());
            if out.len() >= MAX_ADDRS_PER_PEER {
                break;
            }
        }
    }
    out
}

pub fn has_direct_ip(addrs: &[String]) -> bool {
    addrs.iter().any(|a| {
        a.strip_prefix("ip:")
            .is_some_and(|ip| ip.parse::<SocketAddr>().is_ok())
    })
}

pub fn self_peer_info(local: &LocalNode, addr: &EndpointAddr) -> PeerInfo {
    let now = chrono::Utc::now().timestamp_millis();
    PeerInfo {
        node_id: local.node_id.clone(),
        name: local.name.clone(),
        description: local.description.clone(),
        os: current_os().to_owned(),
        arch: current_arch().to_owned(),
        meta_seq: local.meta_seq,
        addrs: sanitize_addrs(&encode_endpoint_addrs(addr)),
        addrs_ts: now,
        last_seen_at: now,
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
    let mut payload: InvitePayload =
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
    validate_peer_fields(&payload.node)?;
    payload.node.addrs = sanitize_addrs(&payload.node.addrs);
    if payload.node.addrs.is_empty() {
        return Err(Error::bad("cluster invite contains no valid endpoint address"));
    }
    Ok((secret, payload.node))
}

pub fn apply_join(
    conn: &Connection,
    invite: &str,
    local_only: bool,
    force: bool,
) -> Result<(bool, i64, [u8; 32], PeerInfo)> {
    let (new_secret, peer) = decode_invite(invite)?;
    if local_only && !has_direct_ip(&peer.addrs) {
        return Err(Error::bad(
            "local-only cluster invite must include at least one direct IP address",
        ));
    }
    let local = load_local_node(conn)?;
    if peer.node_id == local.node_id {
        return Err(Error::bad("cannot join own node invite"));
    }
    let secret_changed = local.cluster_secret != new_secret;
    let next_gen = if secret_changed {
        let has_peers: bool =
            conn.query_row("SELECT EXISTS(SELECT 1 FROM cluster_peers)", [], |r| r.get(0))?;
        if has_peers && !force {
            return Err(Error::conflict(
                "node already belongs to another cluster; pass force=true to switch clusters",
            ));
        }
        let next_gen = local.membership_gen + 1;
        conn.execute(
            "UPDATE local_node SET cluster_secret=?1, membership_gen=?2 WHERE id=1",
            params![new_secret.as_slice(), next_gen],
        )?;
        conn.execute("DELETE FROM cluster_peers", [])?;
        next_gen
    } else {
        local.membership_gen
    };
    let now = chrono::Utc::now().timestamp_millis();
    upsert_peer(conn, &local.node_id, &peer, now, true)?;
    Ok((secret_changed, next_gen, new_secret, peer))
}

pub fn upsert_peer_if_gen(
    conn: &Connection,
    self_node_id: &str,
    peer: &PeerInfo,
    now: i64,
    direct: bool,
    expected_gen: i64,
) -> Result<bool> {
    let current_gen: i64 =
        conn.query_row("SELECT membership_gen FROM local_node WHERE id=1", [], |r| r.get(0))?;
    if current_gen != expected_gen {
        return Ok(false);
    }
    upsert_peer(conn, self_node_id, peer, now, direct)
}

pub fn upsert_peer(
    conn: &Connection,
    self_node_id: &str,
    peer: &PeerInfo,
    now: i64,
    direct: bool,
) -> Result<bool> {
    if peer.node_id == self_node_id || peer.node_id.parse::<EndpointId>().is_err() {
        return Ok(false);
    }
    if validate_peer_fields(peer).is_err() {
        return Ok(false);
    }
    let sanitized_addrs = sanitize_addrs(&peer.addrs);
    let addrs_json = serde_json::to_string(&sanitized_addrs)
        .map_err(|e| Error::internal(format!("failed to encode peer addrs: {e}")))?;

    let existing: Option<(i64, String, i64)> = conn
        .prepare_cached("SELECT meta_seq, addrs_json, addrs_ts FROM cluster_peers WHERE node_id=?")?
        .query_row([&peer.node_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .optional()?;

    let incoming_ts = if direct {
        now
    } else {
        peer.addrs_ts.max(0).min(now)
    };
    let direct_seen = if direct { now } else { 0 };

    match existing {
        None => {
            if !direct {
                let peer_count: i64 =
                    conn.query_row("SELECT count(*) FROM cluster_peers", [], |r| r.get(0))?;
                if peer_count >= MAX_CLUSTER_PEERS as i64 {
                    return Ok(false);
                }
            }
            let ts = if sanitized_addrs.is_empty() {
                0
            } else {
                incoming_ts
            };
            conn.execute(
                "INSERT INTO cluster_peers(node_id,name,description,os,arch,meta_seq,addrs_json,addrs_ts,last_seen_at)
                 VALUES(?,?,?,?,?,?,?,?,?)",
                params![
                    peer.node_id,
                    peer.name,
                    peer.description,
                    peer.os,
                    peer.arch,
                    peer.meta_seq,
                    addrs_json,
                    ts,
                    direct_seen
                ],
            )?;
            Ok(true)
        }
        Some((old_seq, old_addrs, old_addrs_ts)) => {
            let take_addrs = !sanitized_addrs.is_empty()
                && (direct
                    || old_addrs == "[]"
                    || peer.meta_seq > old_seq
                    || (peer.meta_seq == old_seq && incoming_ts > old_addrs_ts));
            let (next_addrs, next_ts) = if take_addrs {
                (addrs_json.as_str(), incoming_ts.max(old_addrs_ts))
            } else {
                (old_addrs.as_str(), old_addrs_ts)
            };
            if peer.meta_seq > old_seq {
                conn.execute(
                    "UPDATE cluster_peers
                     SET name=?2, description=?3, os=?4, arch=?5, meta_seq=?6, addrs_json=?7, addrs_ts=?8,
                         last_seen_at=CASE WHEN ?9>0 THEN max(last_seen_at, ?9) ELSE last_seen_at END
                     WHERE node_id=?1",
                    params![
                        peer.node_id,
                        peer.name,
                        peer.description,
                        peer.os,
                        peer.arch,
                        peer.meta_seq,
                        next_addrs,
                        next_ts,
                        direct_seen
                    ],
                )?;
                Ok(next_addrs != old_addrs)
            } else if take_addrs || direct {
                conn.execute(
                    "UPDATE cluster_peers
                     SET addrs_json=?2, addrs_ts=?3,
                         last_seen_at=CASE WHEN ?4>0 THEN max(last_seen_at, ?4) ELSE last_seen_at END
                     WHERE node_id=?1",
                    params![peer.node_id, next_addrs, next_ts, direct_seen],
                )?;
                Ok(next_addrs != old_addrs)
            } else {
                Ok(false)
            }
        }
    }
}

pub fn list_peers(conn: &Connection) -> Result<Vec<PeerInfo>> {
    let mut stmt = conn.prepare_cached(
        "SELECT node_id,name,description,os,arch,meta_seq,addrs_json,addrs_ts,last_seen_at
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
                addrs_ts: r.get(7)?,
                last_seen_at: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn delete_peer(conn: &Connection, node_id: &str) -> Result<bool> {
    let changed = conn.execute("DELETE FROM cluster_peers WHERE node_id=?", [node_id])?;
    Ok(changed > 0)
}
