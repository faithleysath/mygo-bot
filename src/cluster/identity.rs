use std::{
    cell::RefCell, collections::HashSet, fmt, fs, net::SocketAddr, rc::Rc, str::FromStr,
};

use base64::prelude::{BASE64_URL_SAFE_NO_PAD, Engine as _};
use iroh::{EndpointAddr, EndpointId, RelayUrl, SecretKey};
use rusqlite::{
    Connection, OptionalExtension, ToSql, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

const INVITE_PREFIX: &str = "mygo-cluster-v1:";
pub const MAX_CLUSTER_PEERS: usize = 256;
pub const MAX_PEX_PEERS: usize = 64;
const MAX_ADDRS_PER_PEER: usize = 16;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    #[default]
    Voter,
    Edge,
}

impl NodeRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Voter => "voter",
            Self::Edge => "edge",
        }
    }

    pub fn is_voter(self) -> bool {
        matches!(self, Self::Voter)
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeRole {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "voter" => Ok(Self::Voter),
            "edge" => Ok(Self::Edge),
            _ => Err(format!("invalid cluster role '{s}'; expected 'voter' or 'edge'")),
        }
    }
}

impl ToSql for NodeRole {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.as_str().as_bytes())))
    }
}

impl FromSql for NodeRole {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()?
            .parse::<Self>()
            .map_err(|e| FromSqlError::Other(e.into()))
    }
}

#[derive(Clone)]
pub struct LocalNode {
    pub secret_key: [u8; 32],
    pub node_id: String,
    pub cluster_secret: [u8; 32],
    pub name: String,
    pub description: String,
    pub role: NodeRole,
    pub db_epoch: String,
    pub meta_seq: i64,
    pub membership_gen: i64,
    pub voter_epoch: u64,
    pub voter_set: Vec<String>,
    pub voter_ballot: String,
    pub pending_voter_epoch: u64,
    pub pending_voter_set: Vec<String>,
    pub pending_proposal_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub name: String,
    pub description: String,
    pub os: String,
    pub arch: String,
    #[serde(default)]
    pub role: NodeRole,
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

pub fn canonicalize_voter_set(set: &[String]) -> Result<Vec<String>> {
    if set.len() > MAX_CLUSTER_PEERS + 1 {
        return Err(Error::bad("voter_set exceeds maximum cluster size"));
    }
    let mut out = Vec::with_capacity(set.len());
    for entry in set {
        entry
            .parse::<EndpointId>()
            .map_err(|_| Error::bad("invalid node_id in voter_set"))?;
        out.push(entry.clone());
    }
    out.sort();
    out.dedup();
    if out.len() > MAX_CLUSTER_PEERS + 1 {
        return Err(Error::bad("voter_set exceeds maximum cluster size"));
    }
    Ok(out)
}

fn parse_voter_set_json(raw: &str) -> Vec<String> {
    let mut v: Vec<String> = serde_json::from_str(raw).unwrap_or_default();
    v.sort();
    v.dedup();
    v
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
    let mut epoch_bytes = [0u8; 16];
    getrandom::fill(&mut epoch_bytes).map_err(|_| Error::internal("entropy unavailable"))?;
    let mut db_epoch = String::with_capacity(32);
    for b in epoch_bytes {
        use std::fmt::Write;
        let _ = write!(db_epoch, "{b:02x}");
    }
    let name = default_node_name();
    let voter_set_json = serde_json::to_string(&[&node_id])
        .map_err(|e| Error::internal(format!("failed to encode voter_set: {e}")))?;
    conn.execute(
        "INSERT INTO local_node(
            id,secret_key,node_id,cluster_secret,name,description,role,db_epoch,
            meta_seq,membership_gen,catalog_rev,voter_epoch,voter_set_json,voter_ballot,
            pending_voter_epoch,pending_voter_set_json,pending_proposal_id
         ) VALUES(1,?,?,?,?,?,'voter',?,1,1,0,1,?,'',0,'[]','')",
        params![
            secret_key.as_slice(),
            node_id,
            cluster_secret.as_slice(),
            name,
            "",
            db_epoch,
            voter_set_json,
        ],
    )?;
    Ok(())
}

thread_local! {
    static WRITER_NODE: RefCell<Option<(*const Connection, Rc<str>)>> = const { RefCell::new(None) };
}

// Set once after the Store is in place on its dedicated thread. That thread exits with the Store.
pub fn set_writer_node_id(conn: &Connection, id: &str) {
    WRITER_NODE.set(Some((std::ptr::from_ref(conn), Rc::from(id))));
}

pub fn local_node_id(conn: &Connection) -> Result<Rc<str>> {
    WRITER_NODE.with_borrow(|writer| match writer {
        Some((owner, id)) if std::ptr::eq(*owner, conn) => Ok(Rc::clone(id)),
        // Startup recovery runs before the writer starts.
        _ => Ok(
            conn.query_row("SELECT node_id FROM local_node WHERE id=1", [], |r| {
                r.get(0)
            })?,
        ),
    })
}

pub fn load_local_node(conn: &Connection) -> Result<LocalNode> {
    conn.query_row(
        "SELECT secret_key,node_id,cluster_secret,name,description,role,db_epoch,meta_seq,membership_gen,
                voter_epoch,voter_set_json,voter_ballot,pending_voter_epoch,pending_voter_set_json,pending_proposal_id
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
            let voter_epoch_i64: i64 = r.get(9)?;
            let voter_set_raw: String = r.get(10)?;
            let voter_ballot: String = r.get(11)?;
            let pending_epoch_i64: i64 = r.get(12)?;
            let pending_set_raw: String = r.get(13)?;
            let pending_proposal_id: String = r.get(14)?;
            Ok(LocalNode {
                secret_key,
                node_id: r.get(1)?,
                cluster_secret,
                name: r.get(3)?,
                description: r.get(4)?,
                role: r.get(5)?,
                db_epoch: r.get(6)?,
                meta_seq: r.get(7)?,
                membership_gen: r.get(8)?,
                voter_epoch: u64::try_from(voter_epoch_i64).unwrap_or(0),
                voter_set: parse_voter_set_json(&voter_set_raw),
                voter_ballot,
                pending_voter_epoch: u64::try_from(pending_epoch_i64).unwrap_or(0),
                pending_voter_set: parse_voter_set_json(&pending_set_raw),
                pending_proposal_id,
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

pub fn set_local_role(conn: &Connection, role: NodeRole) -> Result<LocalNode> {
    let current = load_local_node(conn)?;
    let has_peers: bool =
        conn.query_row("SELECT EXISTS(SELECT 1 FROM cluster_peers)", [], |r| r.get(0))?;
    if has_peers {
        if current.role != role {
            return Err(Error::conflict(
                "cannot change cluster role offline while cluster peers exist",
            ));
        }
        return Ok(current);
    }
    let next_set = if role.is_voter() {
        vec![current.node_id.clone()]
    } else {
        Vec::new()
    };
    let next_json = serde_json::to_string(&next_set)
        .map_err(|e| Error::internal(format!("failed to encode voter_set: {e}")))?;
    let next_epoch = i64::try_from(current.voter_epoch.max(1)).unwrap_or(1);
    if current.role != role
        || current.voter_epoch == 0
        || current.voter_set != next_set
        || current.pending_voter_epoch != 0
    {
        conn.execute(
            "UPDATE local_node
             SET role=?1,
                 meta_seq=CASE WHEN role != ?1 THEN meta_seq+1 ELSE meta_seq END,
                 voter_epoch=?2,
                 voter_set_json=?3,
                 pending_voter_epoch=0,
                 pending_voter_set_json='[]',
                 pending_proposal_id=''
             WHERE id=1",
            params![role, next_epoch, next_json],
        )?;
    }
    load_local_node(conn)
}

pub fn probe_voter_config(
    conn: &Connection,
    expected_epoch: u64,
    next_epoch: u64,
    ballot: &str,
) -> Result<(bool, LocalNode)> {
    if ballot.is_empty() || next_epoch == 0 || next_epoch <= expected_epoch {
        return Err(Error::bad("invalid voter recovery ballot"));
    }
    crate::cluster::ledger::validate_hlc(ballot)?;
    crate::cluster::ledger::observe_hlc(ballot);
    let local = load_local_node(conn)?;
    if local.voter_epoch >= next_epoch {
        return Ok((false, local));
    }
    if local.voter_epoch != expected_epoch && local.voter_epoch != 0 {
        return Ok((false, local));
    }
    if ballot <= local.voter_ballot.as_str() {
        return Ok((false, local));
    }
    conn.execute(
        "UPDATE local_node SET voter_ballot=?1 WHERE id=1",
        [ballot],
    )?;
    Ok((true, load_local_node(conn)?))
}

pub fn prepare_voter_config(
    conn: &Connection,
    expected_epoch: u64,
    next_epoch: u64,
    proposal_id: &str,
    new_set: &[String],
) -> Result<LocalNode> {
    let canonical = canonicalize_voter_set(new_set)?;
    if canonical.is_empty() {
        return Err(Error::conflict("cluster must retain at least one voter"));
    }
    if proposal_id.is_empty() || next_epoch == 0 || next_epoch <= expected_epoch {
        return Err(Error::bad("invalid voter epoch transition"));
    }
    crate::cluster::ledger::validate_hlc(proposal_id)?;
    crate::cluster::ledger::observe_hlc(proposal_id);
    let local = load_local_node(conn)?;
    if local.voter_epoch >= next_epoch {
        return Err(Error::conflict("voter epoch already advanced"));
    }
    if local.voter_epoch != expected_epoch && local.voter_epoch != 0 {
        return Err(Error::conflict(format!(
            "voter_epoch mismatch: expected {expected_epoch}, local is {}",
            local.voter_epoch
        )));
    }
    if proposal_id < local.voter_ballot.as_str() {
        return Err(Error::conflict("stale voter configuration ballot"));
    }
    if local.pending_voter_epoch != 0 {
        if local.pending_voter_epoch != next_epoch {
            return Err(Error::conflict("voter reconfiguration already pending"));
        }
        if local.pending_proposal_id == proposal_id {
            if local.pending_voter_set == canonical {
                return Ok(local);
            }
            return Err(Error::conflict("conflicting voter proposal_id"));
        }
        if proposal_id < local.pending_proposal_id.as_str() {
            return Err(Error::conflict("stale voter configuration ballot"));
        }
    }
    let pending_json = serde_json::to_string(&canonical)
        .map_err(|e| Error::internal(format!("failed to encode voter_set: {e}")))?;
    let next_i64 = i64::try_from(next_epoch).map_err(|_| Error::bad("voter_epoch overflow"))?;
    conn.execute(
        "UPDATE local_node
         SET voter_ballot=CASE WHEN ?3 > voter_ballot THEN ?3 ELSE voter_ballot END,
             pending_voter_epoch=?1,
             pending_voter_set_json=?2,
             pending_proposal_id=?3
         WHERE id=1",
        params![next_i64, pending_json, proposal_id],
    )?;
    load_local_node(conn)
}

pub fn commit_voter_config(
    conn: &Connection,
    epoch: u64,
    new_set: &[String],
) -> Result<(LocalNode, bool)> {
    if epoch == 0 {
        return Err(Error::bad("committed voter_epoch must be positive"));
    }
    let canonical = canonicalize_voter_set(new_set)?;
    if canonical.is_empty() {
        return Err(Error::conflict("cluster must retain at least one voter"));
    }
    let local = load_local_node(conn)?;
    if local.voter_epoch > epoch {
        return Ok((local, false));
    }
    if local.voter_epoch == epoch {
        if local.voter_set != canonical {
            return Err(Error::conflict(
                "conflicting voter_set for already-committed voter_epoch",
            ));
        }
        if local.pending_voter_epoch == epoch {
            conn.execute(
                "UPDATE local_node
                 SET pending_voter_epoch=0, pending_voter_set_json='[]', pending_proposal_id=''
                 WHERE id=1",
                [],
            )?;
            return Ok((load_local_node(conn)?, false));
        }
        return Ok((local, false));
    }
    let should_be_voter = canonical.iter().any(|id| id == &local.node_id);
    let next_role = if should_be_voter {
        NodeRole::Voter
    } else {
        NodeRole::Edge
    };
    let role_changed = local.role != next_role;
    let json = serde_json::to_string(&canonical)
        .map_err(|e| Error::internal(format!("failed to encode voter_set: {e}")))?;
    let epoch_i64 = i64::try_from(epoch).map_err(|_| Error::bad("voter_epoch overflow"))?;
    conn.execute(
        "UPDATE local_node
         SET voter_epoch=?1,
             voter_set_json=?2,
             pending_voter_epoch=CASE WHEN pending_voter_epoch <= ?1 THEN 0 ELSE pending_voter_epoch END,
             pending_voter_set_json=CASE WHEN pending_voter_epoch <= ?1 THEN '[]' ELSE pending_voter_set_json END,
             pending_proposal_id=CASE WHEN pending_voter_epoch <= ?1 THEN '' ELSE pending_proposal_id END,
             role=?3,
             meta_seq=CASE WHEN role != ?3 THEN meta_seq+1 ELSE meta_seq END
         WHERE id=1",
        params![epoch_i64, json, next_role],
    )?;
    Ok((load_local_node(conn)?, role_changed))
}

pub fn abort_voter_config(
    conn: &Connection,
    next_epoch: u64,
    proposal_id: &str,
    fence_ballot: Option<&str>,
) -> Result<LocalNode> {
    if proposal_id.is_empty() {
        return Err(Error::bad("invalid voter proposal_id"));
    }
    if let Some(ballot) = fence_ballot {
        crate::cluster::ledger::validate_hlc(ballot)?;
        crate::cluster::ledger::observe_hlc(ballot);
    }
    let next_i64 = i64::try_from(next_epoch).map_err(|_| Error::bad("voter_epoch overflow"))?;
    if let Some(ballot) = fence_ballot {
        conn.execute(
            "UPDATE local_node
             SET voter_ballot=CASE WHEN ?3 > voter_ballot THEN ?3 ELSE voter_ballot END,
                 pending_voter_epoch=CASE WHEN pending_voter_epoch=?1 AND pending_proposal_id=?2 THEN 0 ELSE pending_voter_epoch END,
                 pending_voter_set_json=CASE WHEN pending_voter_epoch=?1 AND pending_proposal_id=?2 THEN '[]' ELSE pending_voter_set_json END,
                 pending_proposal_id=CASE WHEN pending_voter_epoch=?1 AND pending_proposal_id=?2 THEN '' ELSE pending_proposal_id END
             WHERE id=1",
            params![next_i64, proposal_id, ballot],
        )?;
    } else {
        conn.execute(
            "UPDATE local_node
             SET pending_voter_epoch=0, pending_voter_set_json='[]', pending_proposal_id=''
             WHERE id=1 AND pending_voter_epoch=?1 AND pending_proposal_id=?2",
            params![next_i64, proposal_id],
        )?;
    }
    load_local_node(conn)
}

pub fn adopt_voter_config(
    conn: &Connection,
    epoch: u64,
    set: &[String],
) -> Result<(LocalNode, bool)> {
    let local = load_local_node(conn)?;
    if epoch == 0 || set.is_empty() || epoch <= local.voter_epoch || epoch < local.pending_voter_epoch {
        return Ok((local, false));
    }
    commit_voter_config(conn, epoch, set)
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
        role: local.role,
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
    new_secret: [u8; 32],
    peer: PeerInfo,
    force: bool,
) -> Result<(bool, i64, [u8; 32], PeerInfo)> {
    let local = load_local_node(conn)?;
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
            "UPDATE local_node
             SET cluster_secret=?1, membership_gen=?2,
                 voter_epoch=0, voter_set_json='[]', voter_ballot='',
                 pending_voter_epoch=0, pending_voter_set_json='[]', pending_proposal_id=''
             WHERE id=1",
            params![new_secret.as_slice(), next_gen],
        )?;
        conn.execute("DELETE FROM cluster_peers", [])?;
        conn.execute("DELETE FROM peer_catalog_cursors", [])?;
        next_gen
    } else {
        local.membership_gen
    };
    let now = chrono::Utc::now().timestamp_millis();
    upsert_peer(conn, &local.node_id, &peer, now, true)?;
    Ok((secret_changed, next_gen, new_secret, peer))
}

pub fn assert_membership_gen(conn: &Connection, expected_gen: i64) -> Result<()> {
    let current_gen: i64 =
        conn.query_row("SELECT membership_gen FROM local_node WHERE id=1", [], |r| {
            r.get(0)
        })?;
    if current_gen != expected_gen {
        return Err(Error::conflict(
            "cluster membership changed during synchronization",
        ));
    }
    Ok(())
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

    let existing: Option<(i64, String, i64, NodeRole)> = conn
        .prepare_cached("SELECT meta_seq, addrs_json, addrs_ts, role FROM cluster_peers WHERE node_id=?")?
        .query_row([&peer.node_id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
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
                "INSERT INTO cluster_peers(node_id,name,description,os,arch,role,meta_seq,addrs_json,addrs_ts,last_seen_at)
                 VALUES(?,?,?,?,?,?,?,?,?,?)",
                params![
                    peer.node_id,
                    peer.name,
                    peer.description,
                    peer.os,
                    peer.arch,
                    peer.role,
                    peer.meta_seq,
                    addrs_json,
                    ts,
                    direct_seen
                ],
            )?;
            Ok(true)
        }
        Some((old_seq, old_addrs, old_addrs_ts, old_role)) => {
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
                     SET name=?2, description=?3, os=?4, arch=?5, role=?6, meta_seq=?7, addrs_json=?8, addrs_ts=?9,
                         last_seen_at=CASE WHEN ?10>0 THEN max(last_seen_at, ?10) ELSE last_seen_at END
                     WHERE node_id=?1",
                    params![
                        peer.node_id,
                        peer.name,
                        peer.description,
                        peer.os,
                        peer.arch,
                        peer.role,
                        peer.meta_seq,
                        next_addrs,
                        next_ts,
                        direct_seen
                    ],
                )?;
                Ok(next_addrs != old_addrs || peer.role != old_role)
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
        "SELECT node_id,name,description,os,arch,role,meta_seq,addrs_json,addrs_ts,last_seen_at
         FROM cluster_peers ORDER BY name, node_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let addrs_raw: String = r.get(7)?;
            let addrs: Vec<String> = serde_json::from_str(&addrs_raw).unwrap_or_default();
            Ok(PeerInfo {
                node_id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                os: r.get(3)?,
                arch: r.get(4)?,
                role: r.get(5)?,
                meta_seq: r.get(6)?,
                addrs,
                addrs_ts: r.get(8)?,
                last_seen_at: r.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub enum ResolvedNode {
    Local { name: String },
    Remote(PeerInfo),
}

pub fn resolve_node_target(
    conn: &Connection,
    target: &str,
) -> std::result::Result<ResolvedNode, (&'static str, String)> {
    let local = match load_local_node(conn) {
        Ok(n) => n,
        Err(e) => return Err(("EIO", e.to_string())),
    };
    let peers = match list_peers(conn) {
        Ok(p) => p,
        Err(e) => return Err(("EIO", e.to_string())),
    };
    if local.node_id == target {
        return Ok(ResolvedNode::Local { name: local.name });
    }
    if let Some(peer) = peers.iter().find(|p| p.node_id == target) {
        return Ok(ResolvedNode::Remote(peer.clone()));
    }
    let mut matches = Vec::new();
    if local.name == target {
        matches.push(ResolvedNode::Local { name: local.name });
    }
    for peer in peers {
        if peer.name == target {
            matches.push(ResolvedNode::Remote(peer));
        }
    }
    match matches.len() {
        0 => Err(("ENODENOTFOUND", format!("node not found: {target}"))),
        1 => Ok(matches.pop().unwrap()),
        _ => Err((
            "ENODEAMBIGUOUS",
            format!("ambiguous node name '{target}'; specify node id"),
        )),
    }
}

pub fn list_cluster_nodes(
    conn: &Connection,
    net: &crate::cluster::net::ClusterNet,
) -> Result<Vec<serde_json::Value>> {
    let local = load_local_node(conn)?;
    let peers = list_peers(conn)?;
    let mut nodes = Vec::with_capacity(1 + peers.len());
    nodes.push((
        local.name.clone(),
        local.node_id.clone(),
        serde_json::json!({
            "id": local.node_id,
            "name": local.name,
            "description": local.description,
            "os": current_os(),
            "arch": current_arch(),
            "role": local.role.as_str(),
            "online": true,
        }),
    ));
    for peer in peers {
        let online = net.is_peer_online(&peer.node_id);
        nodes.push((
            peer.name.clone(),
            peer.node_id.clone(),
            serde_json::json!({
                "id": peer.node_id,
                "name": peer.name,
                "description": peer.description,
                "os": peer.os,
                "arch": peer.arch,
                "role": peer.role.as_str(),
                "online": online,
            }),
        ));
    }
    nodes.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok(nodes.into_iter().map(|(_, _, v)| v).collect())
}

pub fn delete_peer(conn: &Connection, node_id: &str) -> Result<bool> {
    let changed = conn.execute("DELETE FROM cluster_peers WHERE node_id=?", [node_id])?;
    if changed > 0 {
        conn.execute(
            "DELETE FROM peer_catalog_cursors WHERE peer_node_id=?",
            [node_id],
        )?;
    }
    Ok(changed > 0)
}
