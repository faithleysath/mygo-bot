use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use reqwest::Url;
use rusqlite::{Connection, OptionalExtension, ffi, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Error, Result,
    cluster::identity::load_local_node,
    id::{self, AttachmentId, CallId, ItemId, McpServerId, ModelId, ProviderId, SessionId},
};

const MAX_SESSION_CHUNK_BYTES: usize = 900 * 1024;
const MAX_SESSION_CHUNK_ITEMS: usize = 200;
pub const MAX_INLINE_COMMIT_PAYLOAD_BYTES: usize = 1_500_000;
pub const COMMIT_CHUNK_BYTES: usize = 512 * 1024;
pub const MAX_COMMIT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_MCP_ANCESTOR_HASHES: usize = 8192;
const MAX_SYNC_RESOLUTIONS: i64 = 2048;

thread_local! {
    static SYNC_STAGED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

pub fn stage_stream(stream_id: &str) {
    SYNC_STAGED.with(|cell| {
        let mut list = cell.borrow_mut();
        if !list.iter().any(|s| s == stream_id) {
            list.push(stream_id.to_owned());
        }
    });
}

pub fn take_staged() -> Vec<String> {
    SYNC_STAGED.with(|cell| std::mem::take(&mut *cell.borrow_mut()))
}

pub fn clear_staged() {
    SYNC_STAGED.with(|cell| cell.borrow_mut().clear());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamKind {
    Provider,
    Model,
    Mcp,
    Session,
    Call,
}

impl StreamKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Model => "model",
            Self::Mcp => "mcp",
            Self::Session => "session",
            Self::Call => "call",
        }
    }

    pub fn from_db(s: &str) -> Result<Self> {
        match s {
            "provider" => Ok(Self::Provider),
            "model" => Ok(Self::Model),
            "mcp" => Ok(Self::Mcp),
            "session" => Ok(Self::Session),
            "call" => Ok(Self::Call),
            _ => Err(Error::internal(format!("unknown sync stream kind: {s}"))),
        }
    }

    pub fn prefix(self) -> &'static str {
        match self {
            Self::Provider => "prv_",
            Self::Model => "mdl_",
            Self::Mcp => "mcp_",
            Self::Session => "ses_",
            Self::Call => "cal_",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitWire {
    pub stream_id: String,
    pub kind: StreamKind,
    pub epoch: i64,
    pub seq: i64,
    pub parent_hash: [u8; 32],
    pub commit_hash: [u8; 32],
    pub author_node_id: String,
    pub op_type: String,
    pub payload: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitMeta {
    pub stream_id: String,
    pub kind: StreamKind,
    pub epoch: i64,
    pub seq: i64,
    pub parent_hash: [u8; 32],
    pub commit_hash: [u8; 32],
    pub author_node_id: String,
    pub op_type: String,
    pub payload_len: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct CommitsPage {
    pub commits: Vec<CommitWire>,
    pub oversized_next: Option<CommitMeta>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolutionReceipt {
    pub stream_id: String,
    pub kind: StreamKind,
    pub chosen: String,
    pub resolver_node_id: String,
    pub local_epoch: i64,
    pub local_seq: i64,
    pub local_hash: [u8; 32],
    pub remote_epoch: i64,
    pub remote_seq: i64,
    pub remote_hash: [u8; 32],
    pub winner_epoch: i64,
    pub winner_seq: i64,
    pub winner_hash: [u8; 32],
    pub resolved_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamHead {
    pub stream_id: String,
    pub kind: StreamKind,
    pub epoch: i64,
    pub head_seq: i64,
    pub head_hash: [u8; 32],
    #[serde(default)]
    pub body_complete: bool,
    #[serde(default)]
    pub checkpoint_seq: i64,
    pub deleted: bool,
    #[serde(default)]
    pub retired: bool,
    pub head_snapshot: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalStreamState {
    pub stream_id: String,
    pub kind: StreamKind,
    pub epoch: i64,
    pub head_seq: i64,
    pub head_hash: [u8; 32],
    pub synced_epoch: i64,
    pub synced_seq: i64,
    pub synced_hash: [u8; 32],
    pub checkpoint_seq: i64,
    pub watched: bool,
    pub deleted: bool,
    pub retired: bool,
    pub head_snapshot: Option<String>,
    pub updated_at: i64,
}

impl LocalStreamState {
    pub fn is_body_complete(&self) -> bool {
        self.watched
            && self.synced_epoch == self.epoch
            && self.synced_seq == self.head_seq
            && self.synced_hash == self.head_hash
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictInfo {
    pub stream_id: String,
    pub kind: StreamKind,
    pub local_epoch: i64,
    pub local_seq: i64,
    pub local_hash_hex: String,
    pub remote_epoch: i64,
    pub remote_seq: i64,
    pub remote_hash_hex: String,
    pub remote_node_id: String,
    pub reason: String,
    pub related_local_id: Option<String>,
    pub remote_summary: Option<Value>,
    pub detected_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictRecord {
    pub stream_id: String,
    pub kind: StreamKind,
    pub local_epoch: i64,
    pub local_seq: i64,
    pub local_hash: [u8; 32],
    pub remote_epoch: i64,
    pub remote_seq: i64,
    pub remote_hash: [u8; 32],
    pub remote_node_id: String,
    pub reason: String,
    pub related_local_id: Option<String>,
    pub remote_snapshot: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub id: AttachmentId,
    pub mime_type: String,
    pub content_hash: [u8; 32],
    pub byte_len: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ItemSnapshot {
    pub id: ItemId,
    pub session_id: SessionId,
    pub kind: String,
    pub payload: String,
    pub target_id: Option<ItemId>,
    pub llm_call_id: Option<CallId>,
    pub idempotency_key: Option<String>,
    pub codemode_call_item_id: Option<ItemId>,
    #[serde(default)]
    pub origin_node_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextPos {
    pub position: i64,
    pub item_id: ItemId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub title: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: String,
    pub reasoning_effort: Option<String>,
    pub last_error: String,
    pub model_id: ModelId,
    pub input_queue_cursor: String,
    pub needs_model: bool,
    #[serde(default)]
    pub required_modalities: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SessionDelta {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<ItemSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_items: Vec<ContextPos>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invalidated_warnings: Vec<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub detached_call_ids: Vec<CallId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderSnapshot {
    pub id: ProviderId,
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub archived_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelSnapshot {
    pub id: ModelId,
    pub provider_id: ProviderId,
    pub model_key: String,
    pub context_window_tokens: Option<i64>,
    pub input_modalities: String,
    pub reasoning_efforts: String,
    pub pricing: Option<String>,
    pub archived_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpServerSnapshot {
    pub id: McpServerId,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub shared: bool,
    pub transport: String,
    pub allowed_tools: Option<String>,
    pub config_json: String,
    pub timeout_ms: i64,
    pub revision: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpOAuthSnapshot {
    pub generation: String,
    pub binding: Option<String>,
    pub credentials: Option<String>,
    pub rejected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpSyncSnapshot {
    pub server: McpServerSnapshot,
    pub oauth: Option<McpOAuthSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallSnapshot {
    pub id: CallId,
    pub session_id: Option<SessionId>,
    pub model_id: ModelId,
    pub reasoning_effort: Option<String>,
    pub input_end_position: Option<i64>,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub status: String,
    pub upstream_response_id: Option<String>,
    pub http_status: Option<i64>,
    pub error_summary: Option<String>,
    pub raw_usage: Option<String>,
    pub input_uncached_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub pricing_snapshot: Option<String>,
    pub cost_nano_usd: Option<i64>,
    #[serde(default)]
    pub origin_node_id: String,
}

#[derive(Clone, Copy, Debug)]
pub struct McpChange {
    pub id: McpServerId,
    pub cancel_login: bool,
}

pub enum ApplyOutcome {
    Applied {
        updated: bool,
        session_id: Option<SessionId>,
        session_reset_epoch: Option<i64>,
        deleted_session: bool,
        new_items: Vec<ItemId>,
        mcp_changes: Vec<McpChange>,
        unblocked_conflicts: Vec<String>,
        resolved_conflict: bool,
    },
    Conflict {
        newly_recorded: bool,
    },
    BlockedOnDependency,
    Ignored,
}

pub struct CommitHashInput<'a> {
    pub stream_id: &'a str,
    pub kind: StreamKind,
    pub epoch: i64,
    pub seq: i64,
    pub parent_hash: &'a [u8; 32],
    pub author_node_id: &'a str,
    pub op_type: &'a str,
    pub payload: &'a str,
    pub created_at: i64,
}

pub fn compute_commit_hash(input: &CommitHashInput<'_>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mygo-commit-v1\0");
    let push_str = |h: &mut blake3::Hasher, s: &str| {
        let b = s.as_bytes();
        h.update(&(b.len() as u32).to_be_bytes());
        h.update(b);
    };
    push_str(&mut hasher, input.stream_id);
    push_str(&mut hasher, input.kind.as_str());
    hasher.update(&input.epoch.to_be_bytes());
    hasher.update(&input.seq.to_be_bytes());
    hasher.update(input.parent_hash);
    push_str(&mut hasher, input.author_node_id);
    push_str(&mut hasher, input.op_type);
    push_str(&mut hasher, input.payload);
    hasher.update(&input.created_at.to_be_bytes());
    *hasher.finalize().as_bytes()
}

pub fn verify_commit(c: &CommitWire, expected_seq: i64, expected_parent: &[u8; 32]) -> bool {
    if !c.stream_id.starts_with(c.kind.prefix())
        || c.seq != expected_seq
        || &c.parent_hash != expected_parent
        || c.epoch <= 0
    {
        return false;
    }
    let expected = compute_commit_hash(&CommitHashInput {
        stream_id: &c.stream_id,
        kind: c.kind,
        epoch: c.epoch,
        seq: c.seq,
        parent_hash: &c.parent_hash,
        author_node_id: &c.author_node_id,
        op_type: &c.op_type,
        payload: &c.payload,
        created_at: c.created_at,
    });
    expected == c.commit_hash
}

fn to_hash32(bytes: Vec<u8>) -> rusqlite::Result<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, "hash".into(), rusqlite::types::Type::Blob)
    })
}

pub fn hex_hash(hash: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub fn has_conflict(conn: &Connection, stream_id: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_conflicts WHERE stream_id=?)")?
        .query_row([stream_id], |r| r.get(0))?)
}

pub fn assert_no_conflict(conn: &Connection, stream_id: &str) -> Result<()> {
    if has_conflict(conn, stream_id)? {
        return Err(Error::conflict(format!(
            "sync conflict on {stream_id}; resolve it before modifying"
        )));
    }
    Ok(())
}

pub fn with_savepoint<T>(
    conn: &Connection,
    name: &str,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    conn.execute_batch(&format!("SAVEPOINT {name}"))?;
    match f() {
        Ok(val) => {
            conn.execute_batch(&format!("RELEASE {name}"))?;
            Ok(val)
        }
        Err(err) => {
            let _ = conn.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
            Err(err)
        }
    }
}

pub fn verify_local_prefix(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    checkpoint_seq: i64,
    target_seq: i64,
    target_hash: &[u8; 32],
) -> Result<bool> {
    if target_seq <= 0 || checkpoint_seq <= 0 || checkpoint_seq > target_seq {
        return Ok(false);
    }
    let st = stream_head(conn, stream_id)?;
    let Some(st) = st else {
        return Ok(false);
    };
    let kind = st.kind;
    let mut stmt = conn.prepare_cached(
        "SELECT seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
         FROM sync_commits
         WHERE stream_id=?1 AND epoch=?2 AND seq BETWEEN ?3 AND ?4
         ORDER BY seq ASC",
    )?;
    let mut rows = stmt.query(params![stream_id, epoch, checkpoint_seq, target_seq])?;

    let mut expected_seq = checkpoint_seq;
    let mut expected_parent = [0u8; 32];
    let mut last_hash = [0u8; 32];
    let mut count = 0i64;
    while let Some(r) = rows.next()? {
        let seq: i64 = r.get(0)?;
        let parent_hash = to_hash32(r.get::<_, Vec<u8>>(1)?)?;
        let commit_hash = to_hash32(r.get::<_, Vec<u8>>(2)?)?;
        if count == 0 && checkpoint_seq > 1 {
            // A terminal checkpoint may start cleanly above seq 1.
            expected_parent = parent_hash;
        }
        let wire = CommitWire {
            stream_id: stream_id.to_owned(),
            kind,
            epoch,
            seq,
            parent_hash,
            commit_hash,
            author_node_id: r.get(3)?,
            op_type: r.get(4)?,
            payload: r.get(5)?,
            created_at: r.get(6)?,
        };
        if !verify_commit(&wire, expected_seq, &expected_parent) {
            return Ok(false);
        }
        expected_parent = commit_hash;
        last_hash = commit_hash;
        expected_seq += 1;
        count += 1;
    }
    let expected_len = target_seq - checkpoint_seq + 1;
    Ok(count == expected_len && &last_hash == target_hash)
}

fn verify_local_tip_commit(conn: &Connection, st: &LocalStreamState) -> Result<bool> {
    if st.checkpoint_seq != 1 || !st.is_body_complete() {
        return Ok(false);
    }
    if st.head_seq == 0 {
        return Ok(st.head_hash == [0u8; 32]);
    }
    let has_base: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM sync_commits WHERE stream_id=?1 AND epoch=?2 AND seq=1)",
        )?
        .query_row(params![&st.stream_id, st.epoch], |r| r.get(0))?;
    if !has_base {
        return Ok(false);
    }
    let row = conn
        .prepare_cached(
            "SELECT parent_hash,commit_hash,author_node_id,op_type,payload,created_at
             FROM sync_commits WHERE stream_id=?1 AND epoch=?2 AND seq=?3",
        )?
        .query_row(params![&st.stream_id, st.epoch, st.head_seq], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .optional()?;
    let Some((pvec, cvec, author_node_id, op_type, payload, created_at)) = row else {
        return Ok(false);
    };
    let parent_hash = to_hash32(pvec)?;
    let commit_hash = to_hash32(cvec)?;
    if st.head_seq == 1 && parent_hash != [0u8; 32] {
        return Ok(false);
    }
    let wire = CommitWire {
        stream_id: st.stream_id.clone(),
        kind: st.kind,
        epoch: st.epoch,
        seq: st.head_seq,
        parent_hash,
        commit_hash,
        author_node_id,
        op_type,
        payload,
        created_at,
    };
    Ok(commit_hash == st.head_hash && verify_commit(&wire, st.head_seq, &parent_hash))
}

pub fn is_session_writable_local(conn: &Connection, sid: SessionId) -> Result<bool> {
    let Some(st) = stream_head(conn, sid.as_str())? else {
        return Ok(true);
    };
    if st.deleted || st.retired {
        return Ok(false);
    }
    verify_local_tip_commit(conn, &st)
}

pub fn record_commit_with_epoch(
    conn: &Connection,
    stream_id: &str,
    kind: StreamKind,
    op_type: &str,
    payload_val: &impl Serialize,
    deleted: bool,
    head_snapshot: Option<&str>,
    expected_epoch: Option<i64>,
) -> Result<()> {
    let payload = serde_json::to_string(payload_val)
        .map_err(|e| Error::internal(format!("commit encode failed: {e}")))?;
    with_savepoint(conn, "cluster_commit", || {
        assert_no_conflict(conn, stream_id)?;
        let author = load_local_node(conn)?.node_id;
        let now = chrono::Utc::now().timestamp_millis();
        let existing = stream_head(conn, stream_id)?;
        let (epoch, seq, parent_hash, checkpoint_seq) = match existing {
            Some(e) => {
                if e.deleted || e.retired {
                    return Err(Error::conflict(format!(
                        "stream {stream_id} has been deleted or retired"
                    )));
                }
                if expected_epoch.is_some_and(|exp| exp != e.epoch) {
                    return Err(Error::conflict(format!(
                        "stream {stream_id} epoch superseded; aborting stale write"
                    )));
                }
                if kind == StreamKind::Session && !verify_local_tip_commit(conn, &e)? {
                    return Err(Error::conflict(format!(
                        "session {stream_id} history is incomplete; sync full history before writing"
                    )));
                }
                (e.epoch, e.head_seq + 1, e.head_hash, e.checkpoint_seq)
            }
            None => {
                if let Some(exp) = expected_epoch
                    && exp != 1
                {
                    return Err(Error::conflict("unexpected session stream epoch"));
                }
                (1, 1, [0u8; 32], 1)
            }
        };
        let commit_hash = compute_commit_hash(&CommitHashInput {
            stream_id,
            kind,
            epoch,
            seq,
            parent_hash: &parent_hash,
            author_node_id: &author,
            op_type,
            payload: &payload,
            created_at: now,
        });
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
             VALUES(?1,?2,?3,?4,?5,?3,?4,?5,?6,1,?7,0,?8,?9)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,
               head_seq=excluded.head_seq,
               head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,
               synced_seq=excluded.synced_seq,
               synced_hash=excluded.synced_hash,
               checkpoint_seq=excluded.checkpoint_seq,
               watched=1,
               deleted=excluded.deleted,
               retired=0,
               head_snapshot=coalesce(excluded.head_snapshot, sync_streams.head_snapshot),
               updated_at=excluded.updated_at",
            params![
                stream_id,
                kind.as_str(),
                epoch,
                seq,
                commit_hash.as_slice(),
                checkpoint_seq,
                deleted,
                head_snapshot,
                now
            ],
        )?;
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![
                stream_id,
                epoch,
                seq,
                parent_hash.as_slice(),
                commit_hash.as_slice(),
                author,
                op_type,
                payload,
                now
            ],
        )?;
        stage_stream(stream_id);
        Ok(())
    })
}

pub fn record_commit(
    conn: &Connection,
    stream_id: &str,
    kind: StreamKind,
    op_type: &str,
    payload_val: &impl Serialize,
    deleted: bool,
    head_snapshot: Option<&str>,
) -> Result<()> {
    record_commit_with_epoch(
        conn,
        stream_id,
        kind,
        op_type,
        payload_val,
        deleted,
        head_snapshot,
        None,
    )
}

// --- Per-table Snapshot & Record Helpers ---

pub fn read_provider_snapshot(conn: &Connection, id: ProviderId) -> Result<ProviderSnapshot> {
    Ok(conn.query_row(
        "SELECT id,name,base_url,api_key,archived_at FROM llm_providers WHERE id=?",
        [id],
        |r| {
            Ok(ProviderSnapshot {
                id: r.get(0)?,
                name: r.get(1)?,
                base_url: r.get(2)?,
                api_key: r.get(3)?,
                archived_at: r.get(4)?,
            })
        },
    )?)
}

pub fn record_provider(conn: &Connection, id: ProviderId) -> Result<()> {
    let snap = read_provider_snapshot(conn, id)?;
    record_commit(
        conn,
        id.as_str(),
        StreamKind::Provider,
        "provider_upsert",
        &snap,
        false,
        None,
    )
}

pub fn read_model_snapshot(conn: &Connection, id: ModelId) -> Result<ModelSnapshot> {
    Ok(conn.query_row(
        "SELECT id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing,archived_at
         FROM llm_models WHERE id=?",
        [id],
        |r| {
            Ok(ModelSnapshot {
                id: r.get(0)?,
                provider_id: r.get(1)?,
                model_key: r.get(2)?,
                context_window_tokens: r.get(3)?,
                input_modalities: r.get(4)?,
                reasoning_efforts: r.get(5)?,
                pricing: r.get(6)?,
                archived_at: r.get(7)?,
            })
        },
    )?)
}

pub fn record_model(conn: &Connection, id: ModelId) -> Result<()> {
    let snap = read_model_snapshot(conn, id)?;
    record_commit(
        conn,
        id.as_str(),
        StreamKind::Model,
        "model_upsert",
        &snap,
        false,
        None,
    )
}

pub fn read_mcp_snapshot(conn: &Connection, id: McpServerId) -> Result<McpSyncSnapshot> {
    let server = conn.query_row(
        "SELECT id,name,description,enabled,shared,transport,allowed_tools,config_json,timeout_ms,revision
         FROM mcp_servers WHERE id=?",
        [id],
        |r| {
            Ok(McpServerSnapshot {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                enabled: r.get(3)?,
                shared: r.get(4)?,
                transport: r.get(5)?,
                allowed_tools: r.get(6)?,
                config_json: r.get(7)?,
                timeout_ms: r.get(8)?,
                revision: r.get(9)?,
            })
        },
    )?;
    // Never sync pending login state; only sync binding/credentials once authorized or rejected.
    let oauth = conn
        .query_row(
            "SELECT generation,binding,credentials,rejected FROM mcp_oauth
             WHERE server_id=? AND (credentials IS NOT NULL OR rejected=1)",
            [id],
            |r| {
                Ok(McpOAuthSnapshot {
                    generation: r.get(0)?,
                    binding: r.get(1)?,
                    credentials: r.get(2)?,
                    rejected: r.get(3)?,
                })
            },
        )
        .optional()?;
    Ok(McpSyncSnapshot { server, oauth })
}

pub fn record_mcp_if_shared(conn: &Connection, id: McpServerId) -> Result<()> {
    let shared: Option<bool> = conn
        .prepare_cached("SELECT shared FROM mcp_servers WHERE id=?")?
        .query_row([id], |r| r.get(0))
        .optional()?;
    if shared == Some(true) {
        let snap = read_mcp_snapshot(conn, id)?;
        record_commit(
            conn,
            id.as_str(),
            StreamKind::Mcp,
            "mcp_upsert",
            &snap,
            false,
            None,
        )?;
    }
    Ok(())
}

fn collect_ancestor_hashes_hex(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    up_to_seq: i64,
) -> Result<(i64, Vec<String>)> {
    if up_to_seq <= 0 {
        return Ok((0, Vec::new()));
    }
    let mut stmt = conn.prepare_cached(
        "SELECT seq, commit_hash FROM sync_commits
         WHERE stream_id=?1 AND epoch=?2 AND seq<=?3
         ORDER BY seq DESC LIMIT ?4",
    )?;
    let mut rows = stmt.query(params![
        stream_id,
        epoch,
        up_to_seq,
        MAX_MCP_ANCESTOR_HASHES as i64
    ])?;
    let mut desc = Vec::new();
    let mut expected = up_to_seq;
    while let Some(r) = rows.next()? {
        let seq: i64 = r.get(0)?;
        if seq != expected {
            break;
        }
        let h = to_hash32(r.get::<_, Vec<u8>>(1)?)?;
        desc.push(hex_hash(&h));
        expected -= 1;
    }
    desc.reverse();
    let start_seq = if desc.is_empty() {
        0
    } else {
        up_to_seq - i64::try_from(desc.len()).unwrap_or(0) + 1
    };
    Ok((start_seq, desc))
}

/// Write a higher-epoch terminal deletion checkpoint for an unshared/deleted MCP server and purge
/// all older history commits containing sensitive configuration or OAuth credentials (P1-09).
pub fn record_mcp_deleted(conn: &Connection, id: McpServerId) -> Result<()> {
    let Some(st) = stream_head(conn, id.as_str())? else {
        return Ok(());
    };
    with_savepoint(conn, "mcp_delete_checkpoint", || {
        assert_no_conflict(conn, id.as_str())?;
        if st.retired || st.deleted {
            return Ok(());
        }
        let (ancestor_start_seq, ancestor_hashes_hex) =
            collect_ancestor_hashes_hex(conn, id.as_str(), st.epoch, st.head_seq)?;
        let new_epoch = st.epoch + 1;
        let payload = json!({
            "id": id,
            "base_epoch": st.epoch,
            "base_seq": st.head_seq,
            "base_hash_hex": hex_hash(&st.head_hash),
            "ancestor_start_seq": ancestor_start_seq,
            "ancestor_hashes_hex": ancestor_hashes_hex,
        })
        .to_string();
        replace_stream_with_epoch_snapshot(
            conn,
            EpochSnapshot {
                stream_id: id.as_str(),
                kind: StreamKind::Mcp,
                new_epoch,
                op_type: "mcp_delete",
                payload,
                deleted: true,
                retired: true,
                head_snapshot: None,
            },
        )?;
        Ok(())
    })
}

pub fn read_call_snapshot(conn: &Connection, id: CallId) -> Result<CallSnapshot> {
    Ok(conn.query_row(
        "SELECT id,session_id,model_id,reasoning_effort,input_end_position,started_at,finished_at,status,
                upstream_response_id,http_status,error_summary,raw_usage,input_uncached_tokens,
                cache_read_tokens,cache_write_tokens,output_tokens,pricing_snapshot,cost_nano_usd,origin_node_id
         FROM llm_calls WHERE id=?",
        [id],
        |r| {
            Ok(CallSnapshot {
                id: r.get(0)?,
                session_id: r.get(1)?,
                model_id: r.get(2)?,
                reasoning_effort: r.get(3)?,
                input_end_position: r.get(4)?,
                started_at: r.get(5)?,
                finished_at: r.get(6)?,
                status: r.get(7)?,
                upstream_response_id: r.get(8)?,
                http_status: r.get(9)?,
                error_summary: r.get(10)?,
                raw_usage: r.get(11)?,
                input_uncached_tokens: r.get(12)?,
                cache_read_tokens: r.get(13)?,
                cache_write_tokens: r.get(14)?,
                output_tokens: r.get(15)?,
                pricing_snapshot: r.get(16)?,
                cost_nano_usd: r.get(17)?,
                origin_node_id: r.get(18)?,
            })
        },
    )?)
}

pub fn record_call(conn: &Connection, id: CallId) -> Result<()> {
    let snap = read_call_snapshot(conn, id)?;
    record_commit(
        conn,
        id.as_str(),
        StreamKind::Call,
        "call_upsert",
        &snap,
        false,
        None,
    )
}

fn read_session_snapshot_with_modalities(
    conn: &Connection,
    sid: SessionId,
    required_modalities: Vec<String>,
) -> Result<SessionSnapshot> {
    conn.prepare_cached(
        "SELECT id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id,input_queue_cursor,needs_model
         FROM sessions WHERE id=?",
    )?
    .query_row([sid], |r| {
        let raw_status: String = r.get(4)?;
        let status = if raw_status == "idle" {
            "idle".to_owned()
        } else {
            // Transient running/pausing states never replicate as durable states.
            "paused".to_owned()
        };
        Ok(SessionSnapshot {
            id: r.get(0)?,
            title: r.get(1)?,
            created_at: r.get(2)?,
            updated_at: r.get(3)?,
            status,
            reasoning_effort: r.get(5)?,
            last_error: r.get(6)?,
            model_id: r.get(7)?,
            input_queue_cursor: r.get(8)?,
            needs_model: r.get(9)?,
            required_modalities: required_modalities.clone(),
        })
    })
    .map_err(Into::into)
}

pub fn read_session_snapshot(conn: &Connection, sid: SessionId) -> Result<SessionSnapshot> {
    let required_modalities = crate::media::session_required_modalities(conn, sid)?;
    read_session_snapshot_with_modalities(conn, sid, required_modalities)
}

pub fn read_item_snapshot(conn: &Connection, id: ItemId) -> Result<ItemSnapshot> {
    conn.prepare_cached(
        "SELECT id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id,origin_node_id
         FROM items WHERE id=?",
    )?
    .query_row([id], |r| {
        Ok(ItemSnapshot {
            id: r.get(0)?,
            session_id: r.get(1)?,
            kind: r.get(2)?,
            payload: r.get(3)?,
            target_id: r.get(4)?,
            llm_call_id: r.get(5)?,
            idempotency_key: r.get(6)?,
            codemode_call_item_id: r.get(7)?,
            origin_node_id: r.get(8)?,
        })
    })
    .map_err(Into::into)
}

fn collect_attachment_ids_from_item(item: &ItemSnapshot, out: &mut HashSet<AttachmentId>) {
    let Ok(val) = serde_json::from_str::<Value>(&item.payload) else {
        return;
    };
    let parts = match item.kind.as_str() {
        "user_message" | "codemode_event" => val.get("content").and_then(Value::as_array),
        "function_call_output" => val.get("output").and_then(Value::as_array),
        _ => None,
    };
    if let Some(arr) = parts {
        for part in arr {
            if let Some(id_str) = part.get("attachment_id").and_then(Value::as_str)
                && let Ok(id) = id_str.parse::<AttachmentId>()
            {
                out.insert(id);
            }
        }
    }
}

pub fn load_attachment_refs(
    conn: &Connection,
    items: &[ItemSnapshot],
) -> Result<Vec<AttachmentRef>> {
    let mut ids = HashSet::new();
    for item in items {
        collect_attachment_ids_from_item(item, &mut ids);
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut sorted: Vec<AttachmentId> = ids.into_iter().collect();
    sorted.sort();
    let mut refs = Vec::with_capacity(sorted.len());
    let mut stmt = conn.prepare_cached(
        "SELECT id,mime_type,content_hash,byte_len FROM attachments WHERE id=?",
    )?;
    for id in sorted {
        if let Some(r) = stmt
            .query_row([id], |r| {
                let hash_vec: Vec<u8> = r.get(2)?;
                Ok(AttachmentRef {
                    id: r.get(0)?,
                    mime_type: r.get(1)?,
                    content_hash: to_hash32(hash_vec)?,
                    byte_len: r.get(3)?,
                })
            })
            .optional()?
        {
            refs.push(r);
        }
    }
    Ok(refs)
}

pub fn record_session_delta(
    conn: &Connection,
    sid: SessionId,
    op_type: &str,
    item_ids: &[ItemId],
    context_items: Vec<ContextPos>,
    invalidated_warnings: Vec<ItemId>,
) -> Result<()> {
    record_session_delta_at_epoch(
        conn,
        sid,
        op_type,
        item_ids,
        context_items,
        invalidated_warnings,
        None,
    )
}

pub fn record_session_delta_at_epoch(
    conn: &Connection,
    sid: SessionId,
    op_type: &str,
    item_ids: &[ItemId],
    context_items: Vec<ContextPos>,
    invalidated_warnings: Vec<ItemId>,
    expected_epoch: Option<i64>,
) -> Result<()> {
    let mut items = Vec::with_capacity(item_ids.len());
    for &id in item_ids {
        items.push(read_item_snapshot(conn, id)?);
    }
    items.sort_by_key(|item| item.id);
    let attachments = load_attachment_refs(conn, &items)?;
    let prev_snap: Option<SessionSnapshot> = if invalidated_warnings.is_empty()
        && items.iter().all(|it| it.kind != "withdrawal")
    {
        conn.prepare_cached("SELECT head_snapshot FROM sync_streams WHERE stream_id=?")?
            .query_row([sid.as_str()], |r| r.get::<_, Option<String>>(0))
            .optional()?
            .flatten()
            .and_then(|raw| serde_json::from_str::<SessionSnapshot>(&raw).ok())
    } else {
        None
    };
    let required_modalities = if let Some(prev) = prev_snap {
        let mut mods: std::collections::BTreeSet<String> =
            prev.required_modalities.into_iter().collect();
        for item in &items {
            if let Ok(payload) = serde_json::from_str::<Value>(&item.payload) {
                if item.kind == "codemode_event"
                    && payload.get("notify").and_then(Value::as_bool) != Some(true)
                    && !context_items.iter().any(|c| c.item_id == item.id)
                {
                    continue;
                }
                crate::media::item_required_modalities(conn, &item.kind, &payload, &mut mods)?;
            }
        }
        for ctx in &context_items {
            if items.iter().any(|it| it.id == ctx.item_id) {
                continue;
            }
            if let Some((kind, raw_payload)) = conn
                .prepare_cached("SELECT kind, payload FROM items WHERE id=?")?
                .query_row([ctx.item_id], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .optional()?
                && let Ok(payload) = serde_json::from_str::<Value>(&raw_payload)
            {
                crate::media::item_required_modalities(conn, &kind, &payload, &mut mods)?;
            }
        }
        mods.into_iter().collect()
    } else {
        crate::media::session_required_modalities(conn, sid)?
    };
    let snap = read_session_snapshot_with_modalities(conn, sid, required_modalities)?;
    let head_snap_json = serde_json::to_string(&snap)
        .map_err(|e| Error::internal(format!("session snapshot encode failed: {e}")))?;
    // Always include canonical normalized SessionSnapshot so author and all receivers produce the
    // exact same head_snapshot for the same commit hash (P1-18, P1-19).
    let delta = SessionDelta {
        attachments,
        items,
        context_items,
        invalidated_warnings,
        session: Some(snap),
        detached_call_ids: Vec::new(),
    };
    record_commit_with_epoch(
        conn,
        sid.as_str(),
        StreamKind::Session,
        op_type,
        &delta,
        false,
        Some(&head_snap_json),
        expected_epoch,
    )
}

pub fn record_session_deleted(
    conn: &Connection,
    sid: SessionId,
    detached_call_ids: &[CallId],
) -> Result<()> {
    record_commit(
        conn,
        sid.as_str(),
        StreamKind::Session,
        "session_delete",
        &json!({ "id": sid, "detached_call_ids": detached_call_ids }),
        true,
        None,
    )
}

fn load_full_session_rows(
    conn: &Connection,
    sid: SessionId,
) -> Result<(
    SessionSnapshot,
    Vec<ItemSnapshot>,
    Vec<ContextPos>,
    Vec<ItemId>,
    HashMap<AttachmentId, AttachmentRef>,
)> {
    let snap = read_session_snapshot(conn, sid)?;
    let mut item_stmt = conn.prepare_cached(
        "SELECT id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id,origin_node_id
         FROM items WHERE session_id=? ORDER BY id",
    )?;
    let items = item_stmt
        .query_map([sid], |r| {
            Ok(ItemSnapshot {
                id: r.get(0)?,
                session_id: r.get(1)?,
                kind: r.get(2)?,
                payload: r.get(3)?,
                target_id: r.get(4)?,
                llm_call_id: r.get(5)?,
                idempotency_key: r.get(6)?,
                codemode_call_item_id: r.get(7)?,
                origin_node_id: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut ctx_stmt = conn.prepare_cached(
        "SELECT position,item_id FROM context_items WHERE session_id=? ORDER BY position",
    )?;
    let context_items = ctx_stmt
        .query_map([sid], |r| {
            Ok(ContextPos {
                position: r.get(0)?,
                item_id: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut warn_stmt = conn.prepare_cached(
        "SELECT w.item_id FROM invalidated_warnings w JOIN items i ON i.id=w.item_id
         WHERE i.session_id=? ORDER BY w.item_id",
    )?;
    let invalidated_warnings = warn_stmt
        .query_map([sid], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<ItemId>>>()?;
    let refs: HashMap<AttachmentId, AttachmentRef> = load_attachment_refs(conn, &items)?
        .into_iter()
        .map(|a| (a.id, a))
        .collect();
    Ok((snap, items, context_items, invalidated_warnings, refs))
}

// --- Manifest, Commit Queries, and Staging ---

pub fn local_manifest_page(
    conn: &Connection,
    after_stream_id: Option<&str>,
    limit: usize,
) -> Result<(Vec<StreamHead>, Option<String>)> {
    let limit = limit.clamp(1, 512);
    let fetch = i64::try_from(limit + 1).unwrap_or(513);
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at
         FROM sync_streams
         WHERE (?1 IS NULL OR stream_id > ?1)
         ORDER BY stream_id ASC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![after_stream_id, fetch], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Vec<u8>>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, bool>(9)?,
                r.get::<_, bool>(10)?,
                r.get::<_, bool>(11)?,
                r.get::<_, Option<String>>(12)?,
                r.get::<_, i64>(13)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut heads: Vec<StreamHead> = Vec::with_capacity(limit.min(rows.len()));
    let mut next_cursor = None;
    for (idx, row) in rows.into_iter().enumerate() {
        if idx >= limit {
            if let Some(last) = heads.last() {
                next_cursor = Some(last.stream_id.clone());
            }
            break;
        }
        let (
            stream_id,
            kind_str,
            epoch,
            head_seq,
            hash_vec,
            synced_epoch,
            synced_seq,
            synced_vec,
            checkpoint_seq,
            watched,
            deleted,
            retired,
            head_snapshot,
            updated_at,
        ) = row;
        let head_hash = to_hash32(hash_vec)?;
        let synced_hash = to_hash32(synced_vec)?;
        let body_complete = watched
            && synced_epoch == epoch
            && synced_seq == head_seq
            && synced_hash == head_hash;
        heads.push(StreamHead {
            stream_id,
            kind: StreamKind::from_db(&kind_str)?,
            epoch,
            head_seq,
            head_hash,
            body_complete,
            checkpoint_seq,
            deleted,
            retired,
            head_snapshot,
            updated_at,
        });
    }
    Ok((heads, next_cursor))
}

pub fn stream_head(conn: &Connection, stream_id: &str) -> Result<Option<LocalStreamState>> {
    let row = conn
        .prepare_cached(
            "SELECT stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at
             FROM sync_streams WHERE stream_id=?",
        )?
        .query_row([stream_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Vec<u8>>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, bool>(9)?,
                r.get::<_, bool>(10)?,
                r.get::<_, bool>(11)?,
                r.get::<_, Option<String>>(12)?,
                r.get::<_, i64>(13)?,
            ))
        })
        .optional()?;
    let Some((
        stream_id,
        kind_str,
        epoch,
        head_seq,
        hash_vec,
        synced_epoch,
        synced_seq,
        synced_vec,
        checkpoint_seq,
        watched,
        deleted,
        retired,
        head_snapshot,
        updated_at,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(LocalStreamState {
        stream_id,
        kind: StreamKind::from_db(&kind_str)?,
        epoch,
        head_seq,
        head_hash: to_hash32(hash_vec)?,
        synced_epoch,
        synced_seq,
        synced_hash: to_hash32(synced_vec)?,
        checkpoint_seq,
        watched,
        deleted,
        retired,
        head_snapshot,
        updated_at,
    }))
}

pub fn stream_head_wire(conn: &Connection, stream_id: &str) -> Result<Option<StreamHead>> {
    Ok(stream_head(conn, stream_id)?.map(|st| {
        let body_complete = st.is_body_complete();
        StreamHead {
            stream_id: st.stream_id,
            kind: st.kind,
            epoch: st.epoch,
            head_seq: st.head_seq,
            head_hash: st.head_hash,
            body_complete,
            checkpoint_seq: st.checkpoint_seq,
            deleted: st.deleted,
            retired: st.retired,
            head_snapshot: st.head_snapshot,
            updated_at: st.updated_at,
        }
    }))
}

pub fn read_commits_range(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    after_seq: i64,
    up_to_seq: Option<i64>,
    max_count: usize,
    max_bytes: usize,
) -> Result<(Vec<CommitWire>, bool)> {
    let page = read_commits_page(conn, stream_id, epoch, after_seq, up_to_seq, max_count, max_bytes)?;
    if page.oversized_next.is_some() {
        return Err(Error::bad(
            "commit payload exceeds inline limit; use chunked commit fetch",
        ));
    }
    Ok((page.commits, page.has_more))
}

pub fn read_commits_page(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    after_seq: i64,
    up_to_seq: Option<i64>,
    max_count: usize,
    max_bytes: usize,
) -> Result<CommitsPage> {
    let stream_kind = conn
        .prepare_cached("SELECT kind FROM sync_streams WHERE stream_id=?")?
        .query_row([stream_id], |r| r.get::<_, String>(0))
        .optional()?;
    let Some(kind_str) = stream_kind else {
        return Ok(CommitsPage {
            commits: Vec::new(),
            oversized_next: None,
            has_more: false,
        });
    };
    let kind = StreamKind::from_db(&kind_str)?;
    let max_count = max_count.clamp(1, 256);
    let inline_single_limit = max_bytes.min(MAX_INLINE_COMMIT_PAYLOAD_BYTES);
    // Do NOT prefetch count+1 rows with .collect(): stream row-by-row and enforce serialized byte
    // budget incrementally so bounded RPC budgets are strictly respected (P1-03 / P1-13).
    let mut stmt = conn.prepare_cached(
        "SELECT seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
         FROM sync_commits
         WHERE stream_id=?1 AND epoch=?2 AND seq>?3 AND (?4 IS NULL OR seq<=?4)
         ORDER BY seq ASC",
    )?;
    let mut rows = stmt.query(params![stream_id, epoch, after_seq, up_to_seq])?;

    let mut out = Vec::new();
    let mut oversized_next = None;
    let mut total_bytes = 0usize;
    let mut has_more = false;
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        let parent_vec: Vec<u8> = row.get(1)?;
        let commit_vec: Vec<u8> = row.get(2)?;
        let author_node_id: String = row.get(3)?;
        let op_type: String = row.get(4)?;
        let payload: String = row.get(5)?;
        let created_at: i64 = row.get(6)?;
        let wire_estimate = if payload.len() > 256 * 1024 {
            serde_json::to_string(&payload)
                .map(|s| s.len() + 256)
                .unwrap_or_else(|_| payload.len().saturating_mul(2).saturating_add(256))
        } else {
            payload.len().saturating_mul(2).saturating_add(256)
        };
        if out.is_empty() && wire_estimate > inline_single_limit {
            oversized_next = Some(CommitMeta {
                stream_id: stream_id.to_owned(),
                kind,
                epoch,
                seq,
                parent_hash: to_hash32(parent_vec)?,
                commit_hash: to_hash32(commit_vec)?,
                author_node_id,
                op_type,
                payload_len: i64::try_from(payload.len())
                    .map_err(|_| Error::internal("commit payload size overflow"))?,
                created_at,
            });
            has_more = rows.next()?.is_some();
            break;
        }
        if !out.is_empty() && (out.len() >= max_count || total_bytes + wire_estimate > max_bytes) {
            has_more = true;
            break;
        }
        total_bytes += wire_estimate;
        out.push(CommitWire {
            stream_id: stream_id.to_owned(),
            kind,
            epoch,
            seq,
            parent_hash: to_hash32(parent_vec)?,
            commit_hash: to_hash32(commit_vec)?,
            author_node_id,
            op_type,
            payload,
            created_at,
        });
    }
    Ok(CommitsPage {
        commits: out,
        oversized_next,
        has_more,
    })
}

pub fn read_commit_payload_chunk(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    seq: i64,
    expected_commit_hash: &[u8; 32],
    offset: u64,
    max_bytes: usize,
) -> Result<Option<(Vec<u8>, bool)>> {
    let limit = i64::try_from(max_bytes.clamp(1, COMMIT_CHUNK_BYTES)).unwrap_or(512 * 1024);
    let offset_i64 = i64::try_from(offset).map_err(|_| Error::bad("chunk offset overflow"))?;
    let sql_offset = offset_i64
        .checked_add(1)
        .ok_or_else(|| Error::bad("chunk offset overflow"))?;
    let row: Option<(i64, Vec<u8>)> = conn
        .prepare_cached(
            "SELECT length(CAST(payload AS BLOB)), substr(CAST(payload AS BLOB), ?5, ?6)
             FROM sync_commits
             WHERE stream_id=?1 AND epoch=?2 AND seq=?3 AND commit_hash=?4",
        )?
        .query_row(
            params![
                stream_id,
                epoch,
                seq,
                expected_commit_hash.as_slice(),
                sql_offset,
                limit
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((total_len, chunk)) = row else {
        return Ok(None);
    };
    if offset_i64 < 0 || offset_i64 > total_len {
        return Err(Error::bad("commit chunk offset out of bounds"));
    }
    let end = offset_i64 + i64::try_from(chunk.len()).unwrap_or(i64::MAX);
    Ok(Some((chunk, end >= total_len)))
}

pub fn session_history_rev(conn: &Connection, sid: SessionId) -> Result<i64> {
    Ok(conn
        .prepare_cached("SELECT reset_gen FROM sync_streams WHERE stream_id=?")?
        .query_row([sid.as_str()], |r| r.get::<_, i64>(0))
        .optional()?
        .unwrap_or(0))
}

pub fn read_delete_commit(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    seq: i64,
) -> Result<Option<CommitWire>> {
    let Some(st) = stream_head(conn, stream_id)? else {
        return Ok(None);
    };
    if !st.deleted {
        return Ok(None);
    }
    let row = conn
        .prepare_cached(
            "SELECT parent_hash,commit_hash,author_node_id,op_type,payload,created_at
             FROM sync_commits
             WHERE stream_id=?1 AND epoch=?2 AND seq=?3 AND op_type IN ('session_delete','mcp_delete')",
        )?
        .query_row(params![stream_id, epoch, seq], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .optional()?;
    let Some((pvec, cvec, author_node_id, op_type, payload, created_at)) = row else {
        return Ok(None);
    };
    Ok(Some(CommitWire {
        stream_id: stream_id.to_owned(),
        kind: st.kind,
        epoch,
        seq,
        parent_hash: to_hash32(pvec)?,
        commit_hash: to_hash32(cvec)?,
        author_node_id,
        op_type,
        payload,
        created_at,
    }))
}

pub fn commit_hash_at(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    seq: i64,
) -> Result<Option<[u8; 32]>> {
    let raw: Option<Vec<u8>> = conn
        .prepare_cached(
            "SELECT commit_hash FROM sync_commits WHERE stream_id=?1 AND epoch=?2 AND seq=?3",
        )?
        .query_row(params![stream_id, epoch, seq], |r| r.get(0))
        .optional()?;
    raw.map(to_hash32).transpose().map_err(Into::into)
}

pub fn has_commit_hash(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    seq: i64,
    expected_hash: &[u8; 32],
) -> Result<bool> {
    Ok(commit_hash_at(conn, stream_id, epoch, seq)?.is_some_and(|h| &h == expected_hash))
}

fn delete_session_local_rows(conn: &Connection, sid: SessionId) -> Result<()> {
    conn.execute("DELETE FROM context_items WHERE session_id=?", [sid])?;
    conn.execute(
        "DELETE FROM invalidated_warnings WHERE item_id IN (SELECT id FROM items WHERE session_id=?)",
        [sid],
    )?;
    conn.execute(
        "DELETE FROM items WHERE session_id=? AND (target_id IS NOT NULL OR codemode_call_item_id IS NOT NULL)",
        [sid],
    )?;
    conn.execute("DELETE FROM items WHERE session_id=?", [sid])?;
    conn.execute("UPDATE llm_calls SET session_id=NULL WHERE session_id=?", [sid])?;
    conn.execute("DELETE FROM sessions WHERE id=?", [sid])?;
    Ok(())
}

fn clear_session_body_rows(conn: &Connection, sid: SessionId) -> Result<()> {
    conn.execute(
        "UPDATE sessions SET status='paused' WHERE id=? AND status IN ('running','pausing')",
        [sid],
    )?;
    conn.execute("DELETE FROM context_items WHERE session_id=?", [sid])?;
    conn.execute(
        "DELETE FROM invalidated_warnings WHERE item_id IN (SELECT id FROM items WHERE session_id=?)",
        [sid],
    )?;
    conn.execute(
        "DELETE FROM items WHERE session_id=? AND (target_id IS NOT NULL OR codemode_call_item_id IS NOT NULL)",
        [sid],
    )?;
    conn.execute("DELETE FROM items WHERE session_id=?", [sid])?;
    Ok(())
}

// --- Sanitized Conflict Persistence & Listing (P0-03) ---

fn sanitize_url_for_summary(raw: &str) -> String {
    match Url::parse(raw) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => "<invalid-url>".to_owned(),
    }
}

fn summarize_conflict_payload(kind: StreamKind, raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let val: Value = serde_json::from_str(raw).ok()?;
    let summary = match kind {
        StreamKind::Provider => {
            if let Ok(p) = serde_json::from_value::<ProviderSnapshot>(val.clone()) {
                json!({
                    "id": p.id,
                    "name": p.name,
                    "base_url": sanitize_url_for_summary(&p.base_url),
                    "has_api_key": p.api_key.as_deref().is_some_and(|k| !k.is_empty()),
                    "archived_at": p.archived_at,
                })
            } else {
                return None;
            }
        }
        StreamKind::Model => {
            if let Ok(m) = serde_json::from_value::<ModelSnapshot>(val.clone()) {
                json!({
                    "id": m.id,
                    "provider_id": m.provider_id,
                    "model_key": m.model_key,
                    "context_window_tokens": m.context_window_tokens,
                    "archived_at": m.archived_at,
                })
            } else {
                return None;
            }
        }
        StreamKind::Mcp => {
            if let Ok(m) = serde_json::from_value::<McpSyncSnapshot>(val.clone()) {
                // Strict whitelist: NEVER include config_json (headers/args/env/query) or oauth credentials!
                json!({
                    "id": m.server.id,
                    "name": m.server.name,
                    "description": m.server.description,
                    "enabled": m.server.enabled,
                    "shared": m.server.shared,
                    "transport": m.server.transport,
                    "revision": m.server.revision,
                    "has_oauth": m.oauth.is_some(),
                })
            } else if val.get("id").is_some() {
                json!({
                    "id": val["id"],
                    "deleted": true,
                })
            } else {
                return None;
            }
        }
        StreamKind::Session => {
            if let Ok(s) = serde_json::from_value::<SessionSnapshot>(val.clone()) {
                json!({
                    "id": s.id,
                    "title": s.title,
                    "status": s.status,
                    "model_id": s.model_id,
                    "updated_at": s.updated_at,
                    "required_modalities": s.required_modalities,
                })
            } else if val.get("id").is_some() {
                json!({
                    "id": val["id"],
                    "deleted": true,
                })
            } else {
                return None;
            }
        }
        StreamKind::Call => {
            if let Ok(c) = serde_json::from_value::<CallSnapshot>(val) {
                json!({
                    "id": c.id,
                    "session_id": c.session_id,
                    "model_id": c.model_id,
                    "status": c.status,
                    "started_at": c.started_at,
                    "finished_at": c.finished_at,
                    "cost_nano_usd": c.cost_nano_usd,
                })
            } else {
                return None;
            }
        }
    };
    serde_json::to_string(&summary).ok()
}

struct ConflictWrite<'a> {
    stream_id: &'a str,
    kind: StreamKind,
    local_epoch: i64,
    local_seq: i64,
    local_hash: &'a [u8; 32],
    remote: &'a StreamHead,
    remote_node_id: &'a str,
    reason: &'a str,
    related_local_id: Option<&'a str>,
    remote_snapshot: Option<&'a str>,
}

fn record_conflict_row(conn: &Connection, c: &ConflictWrite<'_>) -> Result<bool> {
    let existing = get_conflict_opt(conn, c.stream_id)?;
    let same = existing.as_ref().is_some_and(|e| {
        e.local_epoch == c.local_epoch
            && e.local_seq == c.local_seq
            && &e.local_hash == c.local_hash
            && e.remote_epoch == c.remote.epoch
            && e.remote_seq == c.remote.head_seq
            && e.remote_hash == c.remote.head_hash
            && e.reason == c.reason
            && e.related_local_id.as_deref() == c.related_local_id
    });
    let summary_json = summarize_conflict_payload(
        c.kind,
        c.remote_snapshot.or(c.remote.head_snapshot.as_deref()),
    );
    let now = chrono::Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO sync_conflicts(stream_id,kind,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,remote_node_id,reason,related_local_id,summary_json,remote_snapshot,detected_at)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(stream_id) DO UPDATE SET
           kind=excluded.kind,
           local_epoch=excluded.local_epoch,
           local_seq=excluded.local_seq,
           local_hash=excluded.local_hash,
           remote_epoch=excluded.remote_epoch,
           remote_seq=excluded.remote_seq,
           remote_hash=excluded.remote_hash,
           remote_node_id=excluded.remote_node_id,
           reason=excluded.reason,
           related_local_id=excluded.related_local_id,
           summary_json=coalesce(excluded.summary_json, sync_conflicts.summary_json),
           remote_snapshot=coalesce(excluded.remote_snapshot, sync_conflicts.remote_snapshot),
           detected_at=excluded.detected_at",
        params![
            c.stream_id,
            c.kind.as_str(),
            c.local_epoch,
            c.local_seq,
            c.local_hash.as_slice(),
            c.remote.epoch,
            c.remote.head_seq,
            c.remote.head_hash.as_slice(),
            c.remote_node_id,
            c.reason,
            c.related_local_id,
            summary_json,
            c.remote_snapshot,
            now
        ],
    )?;
    Ok(!same)
}

pub fn list_conflicts(conn: &Connection) -> Result<Vec<ConflictInfo>> {
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id,kind,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,
                remote_node_id,reason,related_local_id,summary_json,detected_at
         FROM sync_conflicts ORDER BY detected_at DESC, stream_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Vec<u8>>(7)?,
                r.get::<_, String>(8)?,
                r.get::<_, String>(9)?,
                r.get::<_, Option<String>>(10)?,
                r.get::<_, Option<String>>(11)?,
                r.get::<_, i64>(12)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::with_capacity(rows.len());
    for (
        stream_id,
        kind_str,
        local_epoch,
        local_seq,
        lh,
        remote_epoch,
        remote_seq,
        rh,
        remote_node_id,
        reason,
        related_local_id,
        raw_summary,
        detected_at,
    ) in rows
    {
        let remote_summary = raw_summary.and_then(|s| serde_json::from_str::<Value>(&s).ok());
        out.push(ConflictInfo {
            stream_id,
            kind: StreamKind::from_db(&kind_str)?,
            local_epoch,
            local_seq,
            local_hash_hex: hex_hash(&to_hash32(lh)?),
            remote_epoch,
            remote_seq,
            remote_hash_hex: hex_hash(&to_hash32(rh)?),
            remote_node_id,
            reason,
            related_local_id,
            remote_summary,
            detected_at,
        });
    }
    Ok(out)
}

pub fn get_conflict_opt(conn: &Connection, stream_id: &str) -> Result<Option<ConflictRecord>> {
    let row = conn
        .prepare_cached(
            "SELECT kind,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,
                    remote_node_id,reason,related_local_id,remote_snapshot
             FROM sync_conflicts WHERE stream_id=?",
        )?
        .query_row([stream_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, Vec<u8>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
                r.get::<_, Option<String>>(9)?,
                r.get::<_, Option<String>>(10)?,
            ))
        })
        .optional()?;
    let Some((
        kind_str,
        local_epoch,
        local_seq,
        lh,
        remote_epoch,
        remote_seq,
        rh,
        remote_node_id,
        reason,
        related_local_id,
        remote_snapshot,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(ConflictRecord {
        stream_id: stream_id.to_owned(),
        kind: StreamKind::from_db(&kind_str)?,
        local_epoch,
        local_seq,
        local_hash: to_hash32(lh)?,
        remote_epoch,
        remote_seq,
        remote_hash: to_hash32(rh)?,
        remote_node_id,
        reason,
        related_local_id,
        remote_snapshot,
    }))
}

pub fn get_conflict(conn: &Connection, stream_id: &str) -> Result<ConflictRecord> {
    get_conflict_opt(conn, stream_id)?
        .ok_or_else(|| Error::from(rusqlite::Error::QueryReturnedNoRows))
}

fn is_unique_constraint(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error().map(|e| e.extended_code),
        Some(ffi::SQLITE_CONSTRAINT_UNIQUE)
    )
}

fn upsert_attachment_meta(conn: &Connection, att: &AttachmentRef) -> Result<()> {
    id::observe_id(att.id);
    let existing: Option<bool> = conn
        .prepare_cached("SELECT data IS NOT NULL FROM attachments WHERE id=?")?
        .query_row([att.id], |r| r.get(0))
        .optional()?;
    if existing.is_some() {
        return Ok(());
    }
    let cached_data: Option<Vec<u8>> = conn
        .prepare_cached("SELECT data FROM attachments WHERE content_hash=? AND data IS NOT NULL LIMIT 1")?
        .query_row([att.content_hash.as_slice()], |r| r.get(0))
        .optional()?;
    conn.execute(
        "INSERT INTO attachments(id,mime_type,content_hash,byte_len,data) VALUES(?,?,?,?,?)
         ON CONFLICT(id) DO NOTHING",
        params![
            att.id,
            att.mime_type,
            att.content_hash.as_slice(),
            att.byte_len,
            cached_data
        ],
    )?;
    Ok(())
}

fn upsert_session_snapshot(conn: &Connection, snap: &SessionSnapshot) -> Result<bool> {
    id::observe_id(snap.id);
    if !snap.input_queue_cursor.is_empty() {
        id::observe_id_str(&snap.input_queue_cursor)?;
    }
    id::observe_id(snap.model_id);
    let exists: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_models WHERE id=?)")?
        .query_row([snap.model_id], |r| r.get(0))?;
    if !exists {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO sessions(id,title,created_at,updated_at,status,reasoning_effort,last_error,model_id,input_queue_cursor,needs_model)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
         ON CONFLICT(id) DO UPDATE SET
           title=excluded.title,
           created_at=excluded.created_at,
           updated_at=excluded.updated_at,
           status=CASE WHEN sessions.status IN ('running','pausing') THEN sessions.status ELSE excluded.status END,
           reasoning_effort=excluded.reasoning_effort,
           last_error=excluded.last_error,
           model_id=excluded.model_id,
           input_queue_cursor=excluded.input_queue_cursor,
           needs_model=excluded.needs_model",
        params![
            snap.id,
            snap.title,
            snap.created_at,
            snap.updated_at,
            snap.status,
            snap.reasoning_effort,
            snap.last_error,
            snap.model_id,
            snap.input_queue_cursor,
            snap.needs_model,
        ],
    )?;
    Ok(true)
}

// --- Remote Commit Replay & Directory Head Application ---

pub fn apply_unwatched_session_head(
    conn: &Connection,
    remote: &StreamHead,
    remote_node_id: &str,
    verified_ancestor_same_epoch: Option<bool>,
) -> Result<ApplyOutcome> {
    if remote.kind != StreamKind::Session {
        return Err(Error::internal("expected session stream head"));
    }
    let sid: SessionId = remote
        .stream_id
        .parse()
        .map_err(|_| Error::bad("invalid session stream_id"))?;
    with_savepoint(conn, "unwatched_session", || {
        let local = stream_head(conn, &remote.stream_id)?;
        if let Some(ref l) = local {
            if remote.epoch < l.epoch {
                return Ok(ApplyOutcome::Ignored);
            }
            if remote.epoch == l.epoch {
                if remote.head_seq == l.head_seq && remote.head_hash == l.head_hash {
                    return Ok(ApplyOutcome::Ignored);
                }
                if l.watched || l.synced_seq > 0 {
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote.head_seq < l.head_seq {
                    // Caller checks commit proof against holder when comparing multi-peer heads;
                    // a shorter peer head on the same stream without conflict is ignored here.
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote.head_seq == l.head_seq && remote.head_hash != l.head_hash {
                    let newly_recorded = record_conflict_row(
                        conn,
                        &ConflictWrite {
                            stream_id: &remote.stream_id,
                            kind: StreamKind::Session,
                            local_epoch: l.epoch,
                            local_seq: l.head_seq,
                            local_hash: &l.head_hash,
                            remote,
                            remote_node_id,
                            reason: "diverged",
                            related_local_id: None,
                            remote_snapshot: remote.head_snapshot.as_deref(),
                        },
                    )?;
                    return Ok(ApplyOutcome::Conflict { newly_recorded });
                }
                if l.head_seq > 0 {
                    match verified_ancestor_same_epoch {
                        Some(true) => {}
                        None => return Ok(ApplyOutcome::Ignored),
                        Some(false) => {
                            let newly_recorded = record_conflict_row(
                                conn,
                                &ConflictWrite {
                                    stream_id: &remote.stream_id,
                                    kind: StreamKind::Session,
                                    local_epoch: l.epoch,
                                    local_seq: l.head_seq,
                                    local_hash: &l.head_hash,
                                    remote,
                                    remote_node_id,
                                    reason: "diverged",
                                    related_local_id: None,
                                    remote_snapshot: remote.head_snapshot.as_deref(),
                                },
                            )?;
                            return Ok(ApplyOutcome::Conflict { newly_recorded });
                        }
                    }
                }
            } else {
                // Higher epoch supersedes unwatched session head. (Watched sessions go through
                // staged epoch replay in phase 7 so we never wipe a body prematurely!)
                if l.watched || l.synced_seq > 0 {
                    return Ok(ApplyOutcome::Ignored);
                }
                conn.execute(
                    "DELETE FROM sync_conflicts WHERE stream_id=?",
                    [&remote.stream_id],
                )?;
                conn.execute(
                    "DELETE FROM sync_commits WHERE stream_id=? AND epoch<?",
                    params![&remote.stream_id, remote.epoch],
                )?;
            }
        }
        if remote.deleted {
            // Terminal deletion is applied via apply_terminal_session_delete in phase 5.
            return Ok(ApplyOutcome::Ignored);
        }
        let Some(raw_snap) = remote.head_snapshot.as_deref() else {
            return Ok(ApplyOutcome::Ignored);
        };
        let snap: SessionSnapshot = serde_json::from_str(raw_snap)
            .map_err(|e| Error::bad(format!("invalid session head snapshot: {e}")))?;
        if snap.id != sid {
            return Err(Error::bad("session snapshot ID mismatch"));
        }
        if !upsert_session_snapshot(conn, &snap)? {
            return Ok(ApplyOutcome::BlockedOnDependency);
        }
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
             VALUES(?1,'session',?2,?3,?4,0,0,?5,0,0,0,0,?6,?7)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=0,synced_seq=0,synced_hash=excluded.synced_hash,checkpoint_seq=0,
               watched=0,deleted=0,retired=0,head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                remote.stream_id,
                remote.epoch,
                remote.head_seq,
                remote.head_hash.as_slice(),
                [0u8; 32].as_slice(),
                raw_snap,
                remote.updated_at
            ],
        )?;
        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id: Some(sid),
            session_reset_epoch: None,
            deleted_session: false,
            new_items: Vec::new(),
            mcp_changes: Vec::new(),
            unblocked_conflicts: Vec::new(),
            resolved_conflict: false,
        })
    })
}

fn mcp_local_is_ancestor_of_tombstone(
    local: &LocalStreamState,
    delete_payload: &str,
    ancestor_verified: bool,
) -> bool {
    if local.head_seq == 0 || ancestor_verified {
        return true;
    }
    let val: Value = match serde_json::from_str(delete_payload) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let base_epoch = val.get("base_epoch").and_then(Value::as_i64).unwrap_or(0);
    let base_seq = val.get("base_seq").and_then(Value::as_i64).unwrap_or(0);
    let base_hash_hex = val.get("base_hash_hex").and_then(Value::as_str).unwrap_or("");
    if local.epoch != base_epoch || base_seq <= 0 || local.head_seq > base_seq {
        return false;
    }
    let local_hex = hex_hash(&local.head_hash);
    if local.head_seq == base_seq {
        return local_hex == base_hash_hex;
    }
    let start_seq = val
        .get("ancestor_start_seq")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let Some(hashes) = val.get("ancestor_hashes_hex").and_then(Value::as_array) else {
        return false;
    };
    if start_seq <= 0 || local.head_seq < start_seq {
        return false;
    }
    let Ok(idx) = usize::try_from(local.head_seq - start_seq) else {
        return false;
    };
    hashes
        .get(idx)
        .and_then(Value::as_str)
        .is_some_and(|h| h == local_hex)
}

fn remote_matches_expected_conflict(
    remote_head: &StreamHead,
    exp_c: &ConflictRecord,
    ancestor_verified: bool,
) -> bool {
    (remote_head.epoch == exp_c.remote_epoch
        && remote_head.head_seq == exp_c.remote_seq
        && remote_head.head_hash == exp_c.remote_hash)
        || remote_head.epoch > exp_c.remote_epoch
        || (remote_head.epoch == exp_c.remote_epoch
            && remote_head.head_seq > exp_c.remote_seq
            && ancestor_verified)
}

pub fn apply_terminal_session_delete(
    conn: &Connection,
    remote_head: &StreamHead,
    delete_commit: &CommitWire,
    ancestor_verified: bool,
    remote_node_id: &str,
    expected_conflict: Option<&ConflictRecord>,
) -> Result<ApplyOutcome> {
    if remote_head.kind != StreamKind::Session
        || !remote_head.deleted
        || delete_commit.kind != StreamKind::Session
        || delete_commit.op_type != "session_delete"
        || delete_commit.stream_id != remote_head.stream_id
        || delete_commit.epoch != remote_head.epoch
        || delete_commit.seq != remote_head.head_seq
        || delete_commit.commit_hash != remote_head.head_hash
        || !verify_commit(
            delete_commit,
            remote_head.head_seq,
            &delete_commit.parent_hash,
        )
    {
        return Err(Error::bad("invalid terminal session_delete checkpoint"));
    }
    let sid: SessionId = remote_head
        .stream_id
        .parse()
        .map_err(|_| Error::bad("invalid session stream_id"))?;
    with_savepoint(conn, "terminal_ses_del", || {
        if let Some(exp_c) = expected_conflict {
            let current_c = get_conflict(conn, &remote_head.stream_id)?;
            if &current_c != exp_c {
                return Err(Error::conflict(
                    "sync conflict state changed concurrently; retry resolution",
                ));
            }
            if !remote_matches_expected_conflict(remote_head, exp_c, ancestor_verified) {
                return Err(Error::conflict(
                    "remote terminal delete does not match conflicted branch",
                ));
            }
        }
        let local = stream_head(conn, &remote_head.stream_id)?;
        let mut session_deleted_conflict = false;
        if let Some(ref l) = local {
            if expected_conflict.is_none() {
                if remote_head.epoch < l.epoch {
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote_head.epoch == l.epoch {
                    if l.deleted
                        && l.head_seq == remote_head.head_seq
                        && l.head_hash == remote_head.head_hash
                    {
                        return Ok(ApplyOutcome::Ignored);
                    }
                    if l.synced_seq == remote_head.head_seq
                        && l.synced_hash == remote_head.head_hash
                    {
                        return Ok(ApplyOutcome::Ignored);
                    }
                    // Check whether local branch is an ancestor of remote_head.
                    let is_ancestor = l.head_seq == 0
                        || (l.head_seq < remote_head.head_seq
                            && (ancestor_verified
                                || (l.head_seq + 1 == remote_head.head_seq
                                    && delete_commit.parent_hash == l.head_hash)));
                    if !is_ancestor {
                        let newly_recorded = record_conflict_row(
                            conn,
                            &ConflictWrite {
                                stream_id: &remote_head.stream_id,
                                kind: StreamKind::Session,
                                local_epoch: l.epoch,
                                local_seq: l.head_seq,
                                local_hash: &l.head_hash,
                                remote: remote_head,
                                remote_node_id,
                                reason: "diverged",
                                related_local_id: None,
                                remote_snapshot: None,
                            },
                        )?;
                        return Ok(ApplyOutcome::Conflict { newly_recorded });
                    }
                } else {
                    session_deleted_conflict = has_conflict(conn, &remote_head.stream_id)?;
                }
            } else {
                session_deleted_conflict = true;
            }
        }
        // Check if payload carries detached calls that might belong to us; deleting session
        // unconditionally cleans items first and sets llm_calls.session_id = NULL, avoiding FK error (P1-01).
        delete_session_local_rows(conn, sid)?;
        let deleted_conflict_rows = conn.execute(
            "DELETE FROM sync_conflicts WHERE stream_id=?",
            [&remote_head.stream_id],
        )?;
        let resolved_conflict = session_deleted_conflict || deleted_conflict_rows > 0;
        conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [&remote_head.stream_id])?;
        conn.execute("DELETE FROM sync_staging_commits WHERE stream_id=?", [&remote_head.stream_id])?;
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
             VALUES(?1,'session',?2,?3,?4,?2,?3,?4,?3,1,1,0,NULL,?5)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
               checkpoint_seq=excluded.checkpoint_seq,watched=1,deleted=1,retired=0,head_snapshot=NULL,updated_at=excluded.updated_at",
            params![
                remote_head.stream_id,
                remote_head.epoch,
                remote_head.head_seq,
                remote_head.head_hash.as_slice(),
                delete_commit.created_at
            ],
        )?;
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![
                delete_commit.stream_id,
                delete_commit.epoch,
                delete_commit.seq,
                delete_commit.parent_hash.as_slice(),
                delete_commit.commit_hash.as_slice(),
                delete_commit.author_node_id,
                delete_commit.op_type,
                delete_commit.payload,
                delete_commit.created_at
            ],
        )?;
        if let Some(exp_c) = expected_conflict {
            let resolver = load_local_node(conn)?.node_id;
            let now = chrono::Utc::now().timestamp_millis();
            record_resolution_receipt(
                conn,
                &ResolutionReceipt {
                    stream_id: remote_head.stream_id.clone(),
                    kind: StreamKind::Session,
                    chosen: "cluster".to_owned(),
                    resolver_node_id: resolver,
                    local_epoch: exp_c.local_epoch,
                    local_seq: exp_c.local_seq,
                    local_hash: exp_c.local_hash,
                    remote_epoch: exp_c.remote_epoch,
                    remote_seq: exp_c.remote_seq,
                    remote_hash: exp_c.remote_hash,
                    winner_epoch: remote_head.epoch,
                    winner_seq: remote_head.head_seq,
                    winner_hash: remote_head.head_hash,
                    resolved_at: now,
                },
            )?;
        }
        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id: Some(sid),
            session_reset_epoch: None,
            deleted_session: true,
            new_items: Vec::new(),
            mcp_changes: Vec::new(),
            unblocked_conflicts: Vec::new(),
            resolved_conflict,
        })
    })
}

pub fn apply_terminal_mcp_delete(
    conn: &Connection,
    remote_head: &StreamHead,
    delete_commit: &CommitWire,
    ancestor_verified: bool,
    remote_node_id: &str,
    expected_conflict: Option<&ConflictRecord>,
) -> Result<ApplyOutcome> {
    if remote_head.kind != StreamKind::Mcp
        || !remote_head.deleted
        || delete_commit.kind != StreamKind::Mcp
        || delete_commit.op_type != "mcp_delete"
        || delete_commit.stream_id != remote_head.stream_id
        || delete_commit.epoch != remote_head.epoch
        || delete_commit.seq != remote_head.head_seq
        || delete_commit.commit_hash != remote_head.head_hash
        || !verify_commit(
            delete_commit,
            remote_head.head_seq,
            &delete_commit.parent_hash,
        )
    {
        return Err(Error::bad("invalid terminal mcp_delete checkpoint"));
    }
    let mid: McpServerId = remote_head
        .stream_id
        .parse()
        .map_err(|_| Error::bad("invalid mcp stream_id"))?;
    with_savepoint(conn, "terminal_mcp_del", || {
        if let Some(exp_c) = expected_conflict {
            let current_c = get_conflict(conn, &remote_head.stream_id)?;
            if &current_c != exp_c {
                return Err(Error::conflict(
                    "sync conflict state changed concurrently; retry resolution",
                ));
            }
            if !remote_matches_expected_conflict(remote_head, exp_c, ancestor_verified) {
                return Err(Error::conflict(
                    "remote terminal MCP delete does not match conflicted branch",
                ));
            }
        }
        let local = stream_head(conn, &remote_head.stream_id)?;

        if expected_conflict.is_none()
            && let Some(ref l) = local
        {
            if remote_head.epoch < l.epoch {
                return Ok(ApplyOutcome::Ignored);
            }
            if l.retired && l.deleted {
                if l.epoch == remote_head.epoch
                    && l.head_seq == remote_head.head_seq
                    && l.head_hash == remote_head.head_hash
                {
                    return Ok(ApplyOutcome::Ignored);
                }
            } else if !mcp_local_is_ancestor_of_tombstone(
                l,
                &delete_commit.payload,
                ancestor_verified,
            ) {
                // Detect any offline edits (shorter, equal, or longer) without proven ancestry.
                let newly_recorded = record_conflict_row(
                    conn,
                    &ConflictWrite {
                        stream_id: &remote_head.stream_id,
                        kind: StreamKind::Mcp,
                        local_epoch: l.epoch,
                        local_seq: l.head_seq,
                        local_hash: &l.head_hash,
                        remote: remote_head,
                        remote_node_id,
                        reason: "retired_diverged",
                        related_local_id: None,
                        remote_snapshot: Some(&delete_commit.payload),
                    },
                )?;
                return Ok(ApplyOutcome::Conflict { newly_recorded });
            }
            if remote_head.epoch == l.epoch && l.head_seq >= remote_head.head_seq {
                if l.head_hash == remote_head.head_hash {
                    return Ok(ApplyOutcome::Ignored);
                }
                let newly_recorded = record_conflict_row(
                    conn,
                    &ConflictWrite {
                        stream_id: &remote_head.stream_id,
                        kind: StreamKind::Mcp,
                        local_epoch: l.epoch,
                        local_seq: l.head_seq,
                        local_hash: &l.head_hash,
                        remote: remote_head,
                        remote_node_id,
                        reason: "retired_diverged",
                        related_local_id: None,
                        remote_snapshot: Some(&delete_commit.payload),
                    },
                )?;
                return Ok(ApplyOutcome::Conflict { newly_recorded });
            }
        }
        conn.execute("DELETE FROM mcp_servers WHERE id=?", [mid])?;
        let resolved_conflict = conn.execute(
            "DELETE FROM sync_conflicts WHERE stream_id=?",
            [&remote_head.stream_id],
        )? > 0
            || expected_conflict.is_some();
        let mut unblocked = collect_unblocked_conflicts(conn, &remote_head.stream_id)?;
        conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [&remote_head.stream_id])?;
        conn.execute("DELETE FROM sync_staging_commits WHERE stream_id=?", [&remote_head.stream_id])?;
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
             VALUES(?1,'mcp',?2,?3,?4,?2,?3,?4,?3,1,1,1,NULL,?5)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
               checkpoint_seq=excluded.checkpoint_seq,watched=1,deleted=1,retired=1,head_snapshot=NULL,updated_at=excluded.updated_at",
            params![
                remote_head.stream_id,
                remote_head.epoch,
                remote_head.head_seq,
                remote_head.head_hash.as_slice(),
                delete_commit.created_at
            ],
        )?;
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![
                delete_commit.stream_id,
                delete_commit.epoch,
                delete_commit.seq,
                delete_commit.parent_hash.as_slice(),
                delete_commit.commit_hash.as_slice(),
                delete_commit.author_node_id,
                delete_commit.op_type,
                delete_commit.payload,
                delete_commit.created_at
            ],
        )?;
        if let Some(exp_c) = expected_conflict {
            let resolver = load_local_node(conn)?.node_id;
            let now = chrono::Utc::now().timestamp_millis();
            record_resolution_receipt(
                conn,
                &ResolutionReceipt {
                    stream_id: remote_head.stream_id.clone(),
                    kind: StreamKind::Mcp,
                    chosen: "cluster".to_owned(),
                    resolver_node_id: resolver,
                    local_epoch: exp_c.local_epoch,
                    local_seq: exp_c.local_seq,
                    local_hash: exp_c.local_hash,
                    remote_epoch: exp_c.remote_epoch,
                    remote_seq: exp_c.remote_seq,
                    remote_hash: exp_c.remote_hash,
                    winner_epoch: remote_head.epoch,
                    winner_seq: remote_head.head_seq,
                    winner_hash: remote_head.head_hash,
                    resolved_at: now,
                },
            )?;
        }
        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id: None,
            session_reset_epoch: None,
            deleted_session: false,
            new_items: Vec::new(),
            mcp_changes: vec![McpChange {
                id: mid,
                cancel_login: true,
            }],
            unblocked_conflicts: {
                unblocked.sort();
                unblocked.dedup();
                unblocked
            },
            resolved_conflict,
        })
    })
}

fn collect_unblocked_conflicts(conn: &Connection, local_id: &str) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare_cached("SELECT stream_id FROM sync_conflicts WHERE related_local_id=?")?;
    Ok(stmt
        .query_map([local_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

enum CommitEffect {
    Applied {
        deleted: bool,
        retired: bool,
        head_snapshot: Option<String>,
        session_id: Option<SessionId>,
        deleted_session: bool,
        new_items: Vec<ItemId>,
        mcp_change: Option<McpChange>,
    },
    NameConflict {
        related_local_id: Option<String>,
    },
    Blocked,
}

fn apply_one_commit(conn: &Connection, c: &CommitWire) -> Result<CommitEffect> {
    match c.kind {
        StreamKind::Provider => {
            let snap: ProviderSnapshot = serde_json::from_str(&c.payload)
                .map_err(|e| Error::bad(format!("invalid provider commit: {e}")))?;
            if snap.id.as_str() != c.stream_id {
                return Err(Error::bad("provider commit ID mismatch"));
            }
            id::observe_id(snap.id);
            let res = conn.execute(
                "INSERT INTO llm_providers(id,name,base_url,api_key,archived_at) VALUES(?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET name=excluded.name,base_url=excluded.base_url,
                   api_key=excluded.api_key,archived_at=excluded.archived_at",
                params![snap.id, snap.name, snap.base_url, snap.api_key, snap.archived_at],
            );
            if let Err(err) = res {
                if is_unique_constraint(&err) {
                    let existing_id: Option<String> = conn
                        .query_row(
                            "SELECT id FROM llm_providers WHERE name=? AND archived_at IS NULL AND id!=?",
                            params![snap.name, snap.id],
                            |r| r.get(0),
                        )
                        .optional()?;
                    return Ok(CommitEffect::NameConflict {
                        related_local_id: existing_id,
                    });
                }
                return Err(err.into());
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                retired: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_change: None,
            })
        }
        StreamKind::Model => {
            let snap: ModelSnapshot = serde_json::from_str(&c.payload)
                .map_err(|e| Error::bad(format!("invalid model commit: {e}")))?;
            if snap.id.as_str() != c.stream_id {
                return Err(Error::bad("model commit ID mismatch"));
            }
            id::observe_id(snap.id);
            id::observe_id(snap.provider_id);
            let has_provider: bool = conn
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_providers WHERE id=?)")?
                .query_row([snap.provider_id], |r| r.get(0))?;
            if !has_provider {
                return Ok(CommitEffect::Blocked);
            }
            let res = conn.execute(
                "INSERT INTO llm_models(id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing,archived_at)
                 VALUES(?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET provider_id=excluded.provider_id,model_key=excluded.model_key,
                   context_window_tokens=excluded.context_window_tokens,input_modalities=excluded.input_modalities,
                   reasoning_efforts=excluded.reasoning_efforts,pricing=excluded.pricing,archived_at=excluded.archived_at",
                params![
                    snap.id,
                    snap.provider_id,
                    snap.model_key,
                    snap.context_window_tokens,
                    snap.input_modalities,
                    snap.reasoning_efforts,
                    snap.pricing,
                    snap.archived_at
                ],
            );
            if let Err(err) = res {
                if is_unique_constraint(&err) {
                    let existing_id: Option<String> = conn
                        .query_row(
                            "SELECT id FROM llm_models WHERE provider_id=? AND model_key=? AND archived_at IS NULL AND id!=?",
                            params![snap.provider_id, snap.model_key, snap.id],
                            |r| r.get(0),
                        )
                        .optional()?;
                    return Ok(CommitEffect::NameConflict {
                        related_local_id: existing_id,
                    });
                }
                return Err(err.into());
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                retired: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_change: None,
            })
        }
        StreamKind::Mcp => {
            let id: McpServerId = c
                .stream_id
                .parse()
                .map_err(|_| Error::bad("invalid mcp stream_id"))?;
            id::observe_id(id);
            if c.op_type == "mcp_delete" {
                conn.execute("DELETE FROM mcp_servers WHERE id=?", [id])?;
                return Ok(CommitEffect::Applied {
                    deleted: true,
                    retired: true,
                    head_snapshot: None,
                    session_id: None,
                    deleted_session: false,
                    new_items: Vec::new(),
                    mcp_change: Some(McpChange {
                        id,
                        cancel_login: true,
                    }),
                });
            }
            let retired: bool = conn
                .prepare_cached("SELECT coalesce((SELECT retired FROM sync_streams WHERE stream_id=?), 0)")?
                .query_row([&c.stream_id], |r| r.get(0))?;
            if retired {
                return Err(Error::conflict("cannot reopen retired shared MCP stream"));
            }
            let old_server: Option<(String, String)> = conn
                .prepare_cached("SELECT transport,config_json FROM mcp_servers WHERE id=?")?
                .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let old_oauth_gen: Option<String> = conn
                .prepare_cached("SELECT generation FROM mcp_oauth WHERE server_id=?")?
                .query_row([id], |r| r.get(0))
                .optional()?;
            let snap: McpSyncSnapshot = serde_json::from_str(&c.payload)
                .map_err(|e| Error::bad(format!("invalid mcp commit: {e}")))?;
            if snap.server.id != id {
                return Err(Error::bad("mcp commit ID mismatch"));
            }
            let s = &snap.server;
            // Always upsert mcp_servers BEFORE mcp_oauth because invalidate_mcp_oauth trigger fires on UPDATE.
            let res = conn.execute(
                "INSERT INTO mcp_servers(id,name,description,enabled,shared,transport,allowed_tools,config_json,timeout_ms,revision)
                 VALUES(?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET
                   name=excluded.name,description=excluded.description,enabled=excluded.enabled,
                   shared=excluded.shared,transport=excluded.transport,allowed_tools=excluded.allowed_tools,
                   config_json=excluded.config_json,timeout_ms=excluded.timeout_ms,revision=excluded.revision",
                params![
                    s.id,
                    s.name,
                    s.description,
                    s.enabled,
                    s.shared,
                    s.transport,
                    s.allowed_tools,
                    s.config_json,
                    s.timeout_ms,
                    s.revision
                ],
            );
            if let Err(err) = res {
                if is_unique_constraint(&err) {
                    let existing_id: Option<String> = conn
                        .query_row(
                            "SELECT id FROM mcp_servers WHERE name=? AND id!=?",
                            params![s.name, s.id],
                            |r| r.get(0),
                        )
                        .optional()?;
                    return Ok(CommitEffect::NameConflict {
                        related_local_id: existing_id,
                    });
                }
                return Err(err.into());
            }
            let auth_changed = old_server
                .as_ref()
                .is_some_and(|(t, cfg)| t != &s.transport || cfg != &s.config_json);
            let mut oauth_cancel = auth_changed;
            if let Some(oauth) = &snap.oauth {
                if old_oauth_gen.as_deref() != Some(oauth.generation.as_str()) {
                    oauth_cancel = true;
                }
                conn.execute(
                    "INSERT INTO mcp_oauth(server_id,generation,binding,pending,credentials,staged_credentials,rejected)
                     VALUES(?1,?2,?3,NULL,?4,NULL,?5)
                     ON CONFLICT(server_id) DO UPDATE SET
                       generation=excluded.generation,binding=excluded.binding,pending=NULL,
                       credentials=excluded.credentials,staged_credentials=NULL,rejected=excluded.rejected",
                    params![
                        s.id,
                        oauth.generation,
                        oauth.binding,
                        oauth.credentials,
                        oauth.rejected
                    ],
                )?;
            } else {
                // Only remove local OAuth if there is no active pending login (P1-20).
                let deleted_oauth = conn.execute(
                    "DELETE FROM mcp_oauth WHERE server_id=? AND pending IS NULL",
                    [s.id],
                )?;
                if deleted_oauth > 0 {
                    oauth_cancel = true;
                }
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                retired: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_change: Some(McpChange {
                    id,
                    cancel_login: oauth_cancel,
                }),
            })
        }
        StreamKind::Call => {
            let snap: CallSnapshot = serde_json::from_str(&c.payload)
                .map_err(|e| Error::bad(format!("invalid call commit: {e}")))?;
            if snap.id.as_str() != c.stream_id {
                return Err(Error::bad("call commit ID mismatch"));
            }
            id::observe_id(snap.id);
            id::observe_id(snap.model_id);
            let has_model: bool = conn
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_models WHERE id=?)")?
                .query_row([snap.model_id], |r| r.get(0))?;
            if !has_model {
                return Ok(CommitEffect::Blocked);
            }
            let session_id = match snap.session_id {
                Some(sid) => {
                    id::observe_id(sid);
                    let tombstoned: bool = conn
                        .prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM sync_streams WHERE stream_id=? AND deleted=1)",
                        )?
                        .query_row([sid.as_str()], |r| r.get(0))?;
                    if tombstoned {
                        None
                    } else {
                        let exists: bool = conn
                            .prepare_cached("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)")?
                            .query_row([sid], |r| r.get(0))?;
                        if !exists {
                            return Ok(CommitEffect::Blocked);
                        }
                        Some(sid)
                    }
                }
                None => None,
            };
            // P1-01: if updating llm_calls changes or clears session_id while local items still hold
            // composite FK (session_id, llm_call_id), block instead of failing SQLite with 500!
            let prev_sid: Option<Option<SessionId>> = conn
                .prepare_cached("SELECT session_id FROM llm_calls WHERE id=?")?
                .query_row([snap.id], |r| r.get(0))
                .optional()?;
            if let Some(Some(old_sid)) = prev_sid
                && Some(old_sid) != session_id
            {
                let referenced: bool = conn
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM items WHERE session_id=?1 AND llm_call_id=?2)",
                    )?
                    .query_row(params![old_sid, snap.id], |r| r.get(0))?;
                if referenced {
                    return Ok(CommitEffect::Blocked);
                }
            }
            conn.execute(
                "INSERT INTO llm_calls(id,session_id,model_id,reasoning_effort,input_end_position,started_at,finished_at,status,
                   upstream_response_id,http_status,error_summary,raw_usage,input_uncached_tokens,
                   cache_read_tokens,cache_write_tokens,output_tokens,pricing_snapshot,cost_nano_usd,origin_node_id)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET
                   session_id=excluded.session_id,model_id=excluded.model_id,reasoning_effort=excluded.reasoning_effort,
                   input_end_position=excluded.input_end_position,started_at=excluded.started_at,
                   finished_at=excluded.finished_at,status=excluded.status,upstream_response_id=excluded.upstream_response_id,
                   http_status=excluded.http_status,error_summary=excluded.error_summary,raw_usage=excluded.raw_usage,
                   input_uncached_tokens=excluded.input_uncached_tokens,cache_read_tokens=excluded.cache_read_tokens,
                   cache_write_tokens=excluded.cache_write_tokens,output_tokens=excluded.output_tokens,
                   pricing_snapshot=excluded.pricing_snapshot,cost_nano_usd=excluded.cost_nano_usd,
                   origin_node_id=excluded.origin_node_id",
                params![
                    snap.id,
                    session_id,
                    snap.model_id,
                    snap.reasoning_effort,
                    snap.input_end_position,
                    snap.started_at,
                    snap.finished_at,
                    snap.status,
                    snap.upstream_response_id,
                    snap.http_status,
                    snap.error_summary,
                    snap.raw_usage,
                    snap.input_uncached_tokens,
                    snap.cache_read_tokens,
                    snap.cache_write_tokens,
                    snap.output_tokens,
                    snap.pricing_snapshot,
                    snap.cost_nano_usd,
                    if snap.origin_node_id.is_empty() {
                        &c.author_node_id
                    } else {
                        &snap.origin_node_id
                    }
                ],
            )?;
            Ok(CommitEffect::Applied {
                deleted: false,
                retired: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_change: None,
            })
        }
        StreamKind::Session => {
            let sid: SessionId = c
                .stream_id
                .parse()
                .map_err(|_| Error::bad("invalid session stream_id"))?;
            id::observe_id(sid);
            if c.op_type == "session_delete" {
                delete_session_local_rows(conn, sid)?;
                return Ok(CommitEffect::Applied {
                    deleted: true,
                    retired: false,
                    head_snapshot: None,
                    session_id: Some(sid),
                    deleted_session: true,
                    new_items: Vec::new(),
                    mcp_change: None,
                });
            }
            let delta: SessionDelta = serde_json::from_str(&c.payload)
                .map_err(|e| Error::bad(format!("invalid session delta commit: {e}")))?;
            if let Some(snap) = &delta.session {
                if snap.id != sid {
                    return Err(Error::bad("session delta ID mismatch"));
                }
                if !upsert_session_snapshot(conn, snap)? {
                    return Ok(CommitEffect::Blocked);
                }
            }
            for att in &delta.attachments {
                upsert_attachment_meta(conn, att)?;
            }
            let mut new_items = Vec::new();
            for item in &delta.items {
                if item.session_id != sid {
                    return Err(Error::bad("session item session_id mismatch"));
                }
                id::observe_id(item.id);
                if let Some(tid) = item.target_id {
                    id::observe_id(tid);
                }
                if let Some(cid) = item.codemode_call_item_id {
                    id::observe_id(cid);
                }
                if let Some(call_id) = item.llm_call_id {
                    id::observe_id(call_id);
                    let has_call: bool = conn
                        .prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM llm_calls WHERE session_id=? AND id=?)",
                        )?
                        .query_row(params![sid, call_id], |r| r.get(0))?;
                    if !has_call {
                        return Ok(CommitEffect::Blocked);
                    }
                }
                let origin = if item.origin_node_id.is_empty() {
                    &c.author_node_id
                } else {
                    &item.origin_node_id
                };
                let inserted = conn.execute(
                    "INSERT INTO items(id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id,origin_node_id)
                     VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
                    params![
                        item.id,
                        item.session_id,
                        item.kind,
                        item.payload,
                        item.target_id,
                        item.llm_call_id,
                        item.idempotency_key,
                        item.codemode_call_item_id,
                        origin
                    ],
                )?;
                if inserted > 0 {
                    new_items.push(item.id);
                }
            }
            for ctx in &delta.context_items {
                id::observe_id(ctx.item_id);
                conn.execute(
                    "INSERT INTO context_items(session_id,position,item_id) VALUES(?,?,?)
                     ON CONFLICT(session_id, position) DO UPDATE SET item_id=excluded.item_id",
                    params![sid, ctx.position, ctx.item_id],
                )?;
            }
            for warn_id in &delta.invalidated_warnings {
                id::observe_id(*warn_id);
                conn.execute(
                    "INSERT INTO invalidated_warnings(item_id) VALUES(?) ON CONFLICT(item_id) DO NOTHING",
                    [warn_id],
                )?;
            }
            let head_snapshot = delta
                .session
                .as_ref()
                .map(|snap| {
                    serde_json::to_string(snap).map_err(|e| {
                        Error::internal(format!("failed to encode session head_snapshot: {e}"))
                    })
                })
                .transpose()?;
            Ok(CommitEffect::Applied {
                deleted: false,
                retired: false,
                head_snapshot,
                session_id: Some(sid),
                deleted_session: false,
                new_items,
                mcp_change: None,
            })
        }
    }
}

pub fn apply_remote_commits_page(
    conn: &Connection,
    fixed_target: &StreamHead,
    commits: &[CommitWire],
    remote_node_id: &str,
    mark_watched: bool,
) -> Result<ApplyOutcome> {
    if commits.is_empty() {
        return Ok(ApplyOutcome::Ignored);
    }
    with_savepoint(conn, "apply_remote_page", || {
        let local = stream_head(conn, &fixed_target.stream_id)?;
        let (start_seq, start_parent) = match &local {
            None => (1i64, [0u8; 32]),
            Some(l) => {
                if fixed_target.epoch < l.epoch {
                    return Ok(ApplyOutcome::Ignored);
                }
                if fixed_target.epoch > l.epoch {
                    return Err(Error::bad(
                        "higher-epoch commits must stream via staging before commit",
                    ));
                }
                if has_conflict(conn, &fixed_target.stream_id)? {
                    return Ok(ApplyOutcome::Conflict {
                        newly_recorded: false,
                    });
                }
                if l.retired && commits.iter().any(|c| c.op_type != "mcp_delete") {
                    return Ok(ApplyOutcome::Ignored);
                }
                if l.synced_seq == 0 {
                    (1i64, [0u8; 32])
                } else {
                    if l.synced_epoch != l.epoch {
                        return Err(Error::internal("corrupt synced_epoch state"));
                    }
                    (l.synced_seq + 1, l.synced_hash)
                }
            }
        };

        let first = commits.first().unwrap();
        if first.epoch != fixed_target.epoch || first.stream_id != fixed_target.stream_id {
            return Err(Error::bad("commit stream or epoch mismatch"));
        }
        if first.seq != start_seq || first.parent_hash != start_parent {
            if let Some(l) = &local {
                let newly_recorded = record_conflict_row(
                    conn,
                    &ConflictWrite {
                        stream_id: &fixed_target.stream_id,
                        kind: fixed_target.kind,
                        local_epoch: l.epoch,
                        local_seq: l.head_seq,
                        local_hash: &l.head_hash,
                        remote: fixed_target,
                        remote_node_id,
                        reason: "diverged",
                        related_local_id: None,
                        remote_snapshot: fixed_target.head_snapshot.as_deref(),
                    },
                )?;
                return Ok(ApplyOutcome::Conflict { newly_recorded });
            }
            return Err(Error::bad("non-contiguous commit batch for new stream"));
        }

        let mut expected_parent = start_parent;
        for (expected_seq, c) in (start_seq..).zip(commits.iter()) {
            if c.epoch != fixed_target.epoch
                || c.stream_id != fixed_target.stream_id
                || c.seq > fixed_target.head_seq
                || !verify_commit(c, expected_seq, &expected_parent)
            {
                return Err(Error::bad("invalid commit hash chain"));
            }
            if c.seq == fixed_target.head_seq && c.commit_hash != fixed_target.head_hash {
                return Err(Error::bad("commit batch did not match pledged target head_hash"));
            }
            if let Some(ref l) = local
                && l.head_seq > 0
                && l.synced_seq < l.head_seq
                && c.seq == l.head_seq
                && c.commit_hash != l.head_hash
            {
                let newly_recorded = record_conflict_row(
                    conn,
                    &ConflictWrite {
                        stream_id: &fixed_target.stream_id,
                        kind: fixed_target.kind,
                        local_epoch: l.epoch,
                        local_seq: l.head_seq,
                        local_hash: &l.head_hash,
                        remote: fixed_target,
                        remote_node_id,
                        reason: "diverged",
                        related_local_id: None,
                        remote_snapshot: fixed_target.head_snapshot.as_deref(),
                    },
                )?;
                return Ok(ApplyOutcome::Conflict { newly_recorded });
            }
            expected_parent = c.commit_hash;
        }

        let mut all_new_items = Vec::new();
        let mut deleted = false;
        let mut retired = local.as_ref().is_some_and(|l| l.retired);
        let mut last_snapshot = local.as_ref().and_then(|l| l.head_snapshot.clone());
        let mut session_id = None;
        let mut deleted_session = false;
        let mut mcp_changes = Vec::new();
        let mut last_commit = commits.first().unwrap();

        for c in commits {
            match apply_one_commit(conn, c)? {
                CommitEffect::Applied {
                    deleted: d,
                    retired: ret,
                    head_snapshot,
                    session_id: s_id,
                    deleted_session: ds,
                    new_items,
                    mcp_change,
                } => {
                    deleted = d;
                    retired |= ret;
                    if d {
                        last_snapshot = None;
                    } else if head_snapshot.is_some() {
                        last_snapshot = head_snapshot;
                    }
                    if s_id.is_some() {
                        session_id = s_id;
                    }
                    deleted_session |= ds;
                    all_new_items.extend(new_items);
                    if let Some(mc) = mcp_change {
                        mcp_changes.push(mc);
                    }
                    last_commit = c;
                }
                CommitEffect::Blocked => {
                    conn.execute_batch("ROLLBACK TO apply_remote_page")?;
                    return Ok(ApplyOutcome::BlockedOnDependency);
                }
                CommitEffect::NameConflict { related_local_id } => {
                    let payload = c.payload.clone();
                    let le = local.as_ref().map_or(0, |l| l.epoch);
                    let ls = local.as_ref().map_or(0, |l| l.head_seq);
                    let lh = local.as_ref().map_or([0u8; 32], |l| l.head_hash);
                    conn.execute_batch("ROLLBACK TO apply_remote_page")?;
                    let newly_recorded = record_conflict_row(
                        conn,
                        &ConflictWrite {
                            stream_id: &fixed_target.stream_id,
                            kind: fixed_target.kind,
                            local_epoch: le,
                            local_seq: ls,
                            local_hash: &lh,
                            remote: fixed_target,
                            remote_node_id,
                            reason: "name_conflict",
                            related_local_id: related_local_id.as_deref(),
                            remote_snapshot: Some(&payload),
                        },
                    )?;
                    return Ok(ApplyOutcome::Conflict { newly_recorded });
                }
            }
        }

        // Preserve pinned target head on intermediate pages, and set watched=1 ONLY upon
        // reaching fixed_target's exact (seq, hash) (P1-03).
        let (final_head_seq, final_head_hash) = if local
            .as_ref()
            .is_some_and(|l| l.head_seq > fixed_target.head_seq)
        {
            let l = local.as_ref().unwrap();
            (l.head_seq, l.head_hash)
        } else if fixed_target.head_seq > last_commit.seq {
            (fixed_target.head_seq, fixed_target.head_hash)
        } else {
            (last_commit.seq, last_commit.commit_hash)
        };
        let reached_head =
            last_commit.seq == final_head_seq && last_commit.commit_hash == final_head_hash;
        // Treat Session `watched` as persistent watch intent; body readiness is tracked via
        // is_body_complete() using synced_epoch/synced_seq/synced_hash.
        let watched = match fixed_target.kind {
            StreamKind::Session => {
                mark_watched || local.as_ref().is_some_and(|l| l.watched) || deleted
            }
            _ => reached_head,
        };
        let checkpoint_seq = local
            .as_ref()
            .filter(|l| l.checkpoint_seq > 0)
            .map_or(1, |l| l.checkpoint_seq);
        let head_snap = if last_commit.seq < final_head_seq {
            local.as_ref().and_then(|l| l.head_snapshot.clone()).or(fixed_target.head_snapshot.clone()).or(last_snapshot)
        } else {
            last_snapshot
        };
        let updated_at = if last_commit.seq < final_head_seq {
            fixed_target.updated_at.max(last_commit.created_at)
        } else {
            last_commit.created_at
        };

        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
             VALUES(?1,?2,?3,?4,?5,?3,?6,?7,?8,?9,?10,?11,?12,?13)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
               checkpoint_seq=excluded.checkpoint_seq,watched=excluded.watched,
               deleted=excluded.deleted,retired=excluded.retired,
               head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                fixed_target.stream_id,
                fixed_target.kind.as_str(),
                last_commit.epoch,
                final_head_seq,
                final_head_hash.as_slice(),
                last_commit.seq,
                last_commit.commit_hash.as_slice(),
                checkpoint_seq,
                watched,
                deleted,
                retired,
                head_snap,
                updated_at
            ],
        )?;
        for c in commits {
            conn.execute(
                "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
                 VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(stream_id,epoch,seq) DO NOTHING",
                params![
                    c.stream_id,
                    c.epoch,
                    c.seq,
                    c.parent_hash.as_slice(),
                    c.commit_hash.as_slice(),
                    c.author_node_id,
                    c.op_type,
                    c.payload,
                    c.created_at
                ],
            )?;
        }

        // P1-05: if a stream that previously failed name_conflict just applied cleanly (e.g. local=None
        // succeeded after the colliding entry was archived), remove its stale sync_conflicts row!
        let resolved_conflict = reached_head
            && conn.execute(
                "DELETE FROM sync_conflicts WHERE stream_id=?",
                [&fixed_target.stream_id],
            )? > 0;
        let mut unblocked_conflicts = Vec::new();
        if matches!(fixed_target.kind, StreamKind::Provider | StreamKind::Model | StreamKind::Mcp) {
            unblocked_conflicts = collect_unblocked_conflicts(conn, &fixed_target.stream_id)?;
            unblocked_conflicts.sort();
            unblocked_conflicts.dedup();
        }

        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id,
            session_reset_epoch: None,
            deleted_session,
            new_items: all_new_items,
            mcp_changes,
            unblocked_conflicts,
            resolved_conflict,
        })
    })
}

pub fn mark_session_watched(conn: &Connection, stream_id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sync_streams SET watched=1 WHERE stream_id=? AND kind='session' AND watched=0",
        [stream_id],
    )?;
    Ok(changed > 0)
}

pub fn pin_session_target_head(
    conn: &Connection,
    remote: &StreamHead,
    remote_node_id: &str,
) -> Result<ApplyOutcome> {
    if remote.kind != StreamKind::Session {
        return Err(Error::internal("expected session head"));
    }
    with_savepoint(conn, "pin_session_head", || {
        let Some(l) = stream_head(conn, &remote.stream_id)? else {
            return Ok(ApplyOutcome::Ignored);
        };
        if remote.epoch < l.epoch || remote.epoch > l.epoch {
            return Ok(ApplyOutcome::Ignored);
        }
        if remote.head_seq > l.head_seq {
            if let Some(raw_snap) = &remote.head_snapshot {
                let snap: SessionSnapshot = serde_json::from_str(raw_snap)
                    .map_err(|e| Error::bad(format!("invalid session snapshot: {e}")))?;
                if !upsert_session_snapshot(conn, &snap)? {
                    return Ok(ApplyOutcome::BlockedOnDependency);
                }
            }
            conn.execute(
                "UPDATE sync_streams
                 SET head_seq=?2, head_hash=?3,
                     head_snapshot=coalesce(?4, head_snapshot), updated_at=?5
                 WHERE stream_id=?1",
                params![
                    remote.stream_id,
                    remote.head_seq,
                    remote.head_hash.as_slice(),
                    remote.head_snapshot,
                    remote.updated_at
                ],
            )?;
            return Ok(ApplyOutcome::Applied {
                updated: true,
                session_id: remote.stream_id.parse().ok(),
                session_reset_epoch: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_changes: Vec::new(),
                unblocked_conflicts: Vec::new(),
                resolved_conflict: false,
            });
        }
        let _ = remote_node_id;
        Ok(ApplyOutcome::Ignored)
    })
}

// --- Staging Table Operations for High-Epoch / Cluster Atomic Overwrites (P1-13) ---

pub fn clear_staging_for_stream(conn: &Connection, stream_id: &str) -> Result<()> {
    conn.execute("DELETE FROM sync_staging_commits WHERE stream_id=?", [stream_id])?;
    Ok(())
}

pub fn stage_remote_commits_page(
    conn: &Connection,
    target_head: &StreamHead,
    commits: &[CommitWire],
) -> Result<i64> {
    if commits.is_empty() {
        return Err(Error::bad("empty staged commit page"));
    }
    with_savepoint(conn, "stage_commits", || {
        let last_staged: Option<(i64, Vec<u8>)> = conn
            .prepare_cached(
                "SELECT seq, commit_hash FROM sync_staging_commits
                 WHERE stream_id=?1 AND epoch=?2 ORDER BY seq DESC LIMIT 1",
            )?
            .query_row(params![&target_head.stream_id, target_head.epoch], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        let (expected_start_seq, expected_start_parent) = match last_staged {
            Some((seq, hvec)) => (seq + 1, to_hash32(hvec)?),
            None => (1, [0u8; 32]),
        };

        let mut expected_parent = expected_start_parent;
        for (expected_seq, c) in (expected_start_seq..).zip(commits.iter()) {
            if c.stream_id != target_head.stream_id
                || c.epoch != target_head.epoch
                || c.seq > target_head.head_seq
                || !verify_commit(c, expected_seq, &expected_parent)
            {
                return Err(Error::bad("invalid staged commit hash chain"));
            }
            if c.seq == target_head.head_seq && c.commit_hash != target_head.head_hash {
                return Err(Error::bad("staged commits did not reach pledged head_hash"));
            }
            expected_parent = c.commit_hash;
        }
        for c in commits {
            conn.execute(
                "INSERT INTO sync_staging_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
                 VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(stream_id,epoch,seq) DO NOTHING",
                params![
                    c.stream_id,
                    c.epoch,
                    c.seq,
                    c.parent_hash.as_slice(),
                    c.commit_hash.as_slice(),
                    c.author_node_id,
                    c.op_type,
                    c.payload,
                    c.created_at
                ],
            )?;
        }
        Ok(commits.last().unwrap().seq)
    })
}

pub fn commit_staged_epoch(
    conn: &Connection,
    target_head: &StreamHead,
    remote_node_id: &str,
    mark_watched: bool,
    expected_conflict: Option<&ConflictRecord>,
) -> Result<ApplyOutcome> {
    with_savepoint(conn, "commit_staged", || {
        // Verify full staged chain 1..=target_head.head_seq exists and ends at target_head.head_hash.
        let mut check_stmt = conn.prepare_cached(
            "SELECT seq,parent_hash,commit_hash FROM sync_staging_commits
             WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
        )?;
        let staged_hdrs = check_stmt
            .query_map(params![&target_head.stream_id, target_head.epoch], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if staged_hdrs.len() != usize::try_from(target_head.head_seq).unwrap_or(usize::MAX) {
            return Err(Error::bad("incomplete staged epoch commits"));
        }
        let mut exp_parent = [0u8; 32];
        let mut last_hash = [0u8; 32];
        for (idx, (seq, pvec, cvec)) in staged_hdrs.into_iter().enumerate() {
            let p = to_hash32(pvec)?;
            let ch = to_hash32(cvec)?;
            if seq != (idx as i64) + 1 || p != exp_parent {
                return Err(Error::bad("non-contiguous staged epoch chain"));
            }
            exp_parent = ch;
            last_hash = ch;
        }
        if last_hash != target_head.head_hash {
            return Err(Error::bad("staged chain head_hash mismatch"));
        }

        let mut extra_mcp_changes = Vec::new();
        let mut resolved_conflict = false;
        let local = stream_head(conn, &target_head.stream_id)?;

        if let Some(exp_c) = expected_conflict {
            let current_c = get_conflict(conn, &target_head.stream_id)?;
            if &current_c != exp_c {
                return Err(Error::conflict(
                    "sync conflict state changed concurrently; retry resolution",
                ));
            }
            let now = chrono::Utc::now().timestamp_millis();
            // Name conflict retirement/rename of colliding local entity (P2-04).
            if exp_c.reason == "name_conflict"
                && let Some(local_id) = exp_c.related_local_id.as_deref()
                && !local_id.is_empty()
            {
                match exp_c.kind {
                    StreamKind::Provider => {
                        if let Ok(pid) = local_id.parse::<ProviderId>() {
                            conn.execute(
                                "UPDATE llm_providers SET archived_at=coalesce(archived_at, ?) WHERE id=?",
                                params![now, pid],
                            )?;
                            if stream_head(conn, pid.as_str())?.is_some() {
                                record_provider(conn, pid)?;
                            }
                        }
                    }
                    StreamKind::Model => {
                        if let Ok(mid) = local_id.parse::<ModelId>() {
                            conn.execute(
                                "UPDATE llm_models SET archived_at=coalesce(archived_at, ?) WHERE id=?",
                                params![now, mid],
                            )?;
                            if stream_head(conn, mid.as_str())?.is_some() {
                                record_model(conn, mid)?;
                            }
                        }
                    }
                    StreamKind::Mcp => {
                        if let Ok(mid) = local_id.parse::<McpServerId>() {
                            let shared: Option<bool> = conn
                                .prepare_cached("SELECT shared FROM mcp_servers WHERE id=?")?
                                .query_row([mid], |r| r.get(0))
                                .optional()?;
                            match shared {
                                Some(true) => {
                                    conn.execute("DELETE FROM mcp_servers WHERE id=?", [mid])?;
                                    record_mcp_deleted(conn, mid)?;
                                    extra_mcp_changes.push(McpChange {
                                        id: mid,
                                        cancel_login: true,
                                    });
                                }
                                Some(false) => {
                                    let old_name: String = conn.query_row(
                                        "SELECT name FROM mcp_servers WHERE id=?",
                                        [mid],
                                        |r| r.get(0),
                                    )?;
                                    let mut n = 0usize;
                                    let new_name = loop {
                                        let suffix = if n == 0 {
                                            format!("-local-{}", &mid.suffix()[..6])
                                        } else {
                                            format!("-local-{}-{n}", &mid.suffix()[..6])
                                        };
                                        let prefix_cap = 128usize.saturating_sub(suffix.len());
                                        let candidate = format!(
                                            "{}{}",
                                            &old_name[..old_name.floor_char_boundary(prefix_cap)],
                                            suffix
                                        );
                                        let taken: bool = conn.query_row(
                                            "SELECT EXISTS(SELECT 1 FROM mcp_servers WHERE name=?)",
                                            [&candidate],
                                            |r| r.get(0),
                                        )?;
                                        if !taken {
                                            break candidate;
                                        }
                                        n += 1;
                                    };
                                    conn.execute(
                                        "UPDATE mcp_servers SET name=?, revision=revision+1 WHERE id=?",
                                        params![new_name, mid],
                                    )?;
                                    extra_mcp_changes.push(McpChange {
                                        id: mid,
                                        cancel_login: false,
                                    });
                                }
                                None => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
            resolved_conflict = conn.execute(
                "DELETE FROM sync_conflicts WHERE stream_id=?",
                [&target_head.stream_id],
            )? > 0;
        } else if let Some(ref l) = local {
            if target_head.epoch <= l.epoch {
                return Ok(ApplyOutcome::Ignored);
            }
            if l.retired && !target_head.deleted {
                return Err(Error::conflict("cannot reopen retired stream via higher epoch"));
            }
            resolved_conflict = conn.execute(
                "DELETE FROM sync_conflicts WHERE stream_id=?",
                [&target_head.stream_id],
            )? > 0;
        }

        // Clear existing local session body cleanly BEFORE replaying winner epoch:
        // ONLY unbind llm_calls / delete sessions if winner is actually deleted (P1-02).
        if target_head.kind == StreamKind::Session
            && let Ok(sid) = target_head.stream_id.parse::<SessionId>()
        {
            if target_head.deleted {
                delete_session_local_rows(conn, sid)?;
            } else {
                clear_session_body_rows(conn, sid)?;
            }
        }
        conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [&target_head.stream_id])?;

        // Replay staged commits from SQLite in bounded cursor loop.
        let mut all_new_items = Vec::new();
        let mut deleted = false;
        let mut retired = false;
        let mut last_snapshot = None;
        let mut session_id = None;
        let mut deleted_session = false;
        let mut mcp_changes = extra_mcp_changes;
        let mut last_created_at = target_head.updated_at;

        {
            let mut replay_stmt = conn.prepare_cached(
                "SELECT seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
                 FROM sync_staging_commits WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
            )?;
            let mut rows =
                replay_stmt.query(params![&target_head.stream_id, target_head.epoch])?;
            while let Some(r) = rows.next()? {
                let seq: i64 = r.get(0)?;
                let pvec: Vec<u8> = r.get(1)?;
                let cvec: Vec<u8> = r.get(2)?;
                let author_node_id: String = r.get(3)?;
                let op_type: String = r.get(4)?;
                let payload: String = r.get(5)?;
                let created_at: i64 = r.get(6)?;
                last_created_at = created_at;
                let wire = CommitWire {
                    stream_id: target_head.stream_id.clone(),
                    kind: target_head.kind,
                    epoch: target_head.epoch,
                    seq,
                    parent_hash: to_hash32(pvec)?,
                    commit_hash: to_hash32(cvec)?,
                    author_node_id,
                    op_type,
                    payload,
                    created_at,
                };
                match apply_one_commit(conn, &wire)? {
                    CommitEffect::Applied {
                        deleted: d,
                        retired: ret,
                        head_snapshot,
                        session_id: s_id,
                        deleted_session: ds,
                        new_items,
                        mcp_change,
                    } => {
                        deleted = d;
                        retired |= ret;
                        if d {
                            last_snapshot = None;
                        } else if head_snapshot.is_some() {
                            last_snapshot = head_snapshot;
                        }
                        if s_id.is_some() {
                            session_id = s_id;
                        }
                        deleted_session |= ds;
                        all_new_items.extend(new_items);
                        if let Some(mc) = mcp_change {
                            mcp_changes.push(mc);
                        }
                    }
                    CommitEffect::Blocked => {
                        conn.execute_batch("ROLLBACK TO commit_staged")?;
                        if expected_conflict.is_some() {
                            return Err(Error::conflict(
                                "cannot apply winning stream yet because a referenced provider, model, or call is missing",
                            ));
                        }
                        return Ok(ApplyOutcome::BlockedOnDependency);
                    }
                    CommitEffect::NameConflict { related_local_id } => {
                        let payload = wire.payload.clone();
                        let le = local.as_ref().map_or(0, |l| l.epoch);
                        let ls = local.as_ref().map_or(0, |l| l.head_seq);
                        let lh = local.as_ref().map_or([0u8; 32], |l| l.head_hash);
                        conn.execute_batch("ROLLBACK TO commit_staged")?;
                        if expected_conflict.is_some() {
                            return Err(Error::conflict(
                                "winning stream hit another local unique name conflict",
                            ));
                        }
                        let newly_recorded = record_conflict_row(
                            conn,
                            &ConflictWrite {
                                stream_id: &target_head.stream_id,
                                kind: target_head.kind,
                                local_epoch: le,
                                local_seq: ls,
                                local_hash: &lh,
                                remote: target_head,
                                remote_node_id,
                                reason: "name_conflict",
                                related_local_id: related_local_id.as_deref(),
                                remote_snapshot: Some(&payload),
                            },
                        )?;
                        return Ok(ApplyOutcome::Conflict { newly_recorded });
                    }
                }
            }
        }

        let watched = if target_head.kind == StreamKind::Session {
            mark_watched
                || local.as_ref().is_some_and(|l| l.watched)
                || expected_conflict.is_some()
                || deleted
        } else {
            true
        };
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,reset_gen,head_snapshot,updated_at)
             VALUES(?1,?2,?3,?4,?5,?3,?4,?5,1,?6,?7,?8,0,?9,?10)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
               checkpoint_seq=1,watched=excluded.watched,deleted=excluded.deleted,retired=excluded.retired,
               reset_gen=CASE WHEN excluded.kind='session' AND NOT excluded.deleted THEN sync_streams.reset_gen+1 ELSE sync_streams.reset_gen END,
               head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                target_head.stream_id,
                target_head.kind.as_str(),
                target_head.epoch,
                target_head.head_seq,
                target_head.head_hash.as_slice(),
                watched,
                deleted,
                retired,
                last_snapshot,
                last_created_at
            ],
        )?;
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             SELECT stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
             FROM sync_staging_commits WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
            params![&target_head.stream_id, target_head.epoch],
        )?;
        conn.execute(
            "DELETE FROM sync_staging_commits WHERE stream_id=?",
            [&target_head.stream_id],
        )?;

        if let Some(exp_c) = expected_conflict {
            let resolver = load_local_node(conn)?.node_id;
            let now = chrono::Utc::now().timestamp_millis();
            record_resolution_receipt(
                conn,
                &ResolutionReceipt {
                    stream_id: target_head.stream_id.clone(),
                    kind: target_head.kind,
                    chosen: "cluster".to_owned(),
                    resolver_node_id: resolver,
                    local_epoch: exp_c.local_epoch,
                    local_seq: exp_c.local_seq,
                    local_hash: exp_c.local_hash,
                    remote_epoch: exp_c.remote_epoch,
                    remote_seq: exp_c.remote_seq,
                    remote_hash: exp_c.remote_hash,
                    winner_epoch: target_head.epoch,
                    winner_seq: target_head.head_seq,
                    winner_hash: target_head.head_hash,
                    resolved_at: now,
                },
            )?;
        }

        let mut unblocked_conflicts = Vec::new();
        if matches!(target_head.kind, StreamKind::Provider | StreamKind::Model | StreamKind::Mcp) {
            unblocked_conflicts = collect_unblocked_conflicts(conn, &target_head.stream_id)?;
            unblocked_conflicts.sort();
            unblocked_conflicts.dedup();
        }
        let session_reset_epoch =
            (target_head.kind == StreamKind::Session && !deleted).then_some(target_head.epoch);

        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id,
            session_reset_epoch,
            deleted_session,
            new_items: all_new_items,
            mcp_changes,
            unblocked_conflicts,
            resolved_conflict,
        })
    })
}

pub fn record_fork_conflict(
    conn: &Connection,
    local: &LocalStreamState,
    remote: &StreamHead,
    remote_node_id: &str,
) -> Result<bool> {
    record_conflict_row(
        conn,
        &ConflictWrite {
            stream_id: &local.stream_id,
            kind: local.kind,
            local_epoch: local.epoch,
            local_seq: local.head_seq,
            local_hash: &local.head_hash,
            remote,
            remote_node_id,
            reason: "diverged",
            related_local_id: None,
            remote_snapshot: remote.head_snapshot.as_deref(),
        },
    )
}

fn record_resolution_receipt(conn: &Connection, r: &ResolutionReceipt) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_resolutions(stream_id,kind,chosen,resolver_node_id,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,winner_epoch,winner_seq,winner_hash,resolved_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
         ON CONFLICT(stream_id) DO UPDATE SET
           kind=excluded.kind,chosen=excluded.chosen,resolver_node_id=excluded.resolver_node_id,
           local_epoch=excluded.local_epoch,local_seq=excluded.local_seq,local_hash=excluded.local_hash,
           remote_epoch=excluded.remote_epoch,remote_seq=excluded.remote_seq,remote_hash=excluded.remote_hash,
           winner_epoch=excluded.winner_epoch,winner_seq=excluded.winner_seq,winner_hash=excluded.winner_hash,
           resolved_at=excluded.resolved_at",
        params![
            r.stream_id,
            r.kind.as_str(),
            r.chosen,
            r.resolver_node_id,
            r.local_epoch,
            r.local_seq,
            r.local_hash.as_slice(),
            r.remote_epoch,
            r.remote_seq,
            r.remote_hash.as_slice(),
            r.winner_epoch,
            r.winner_seq,
            r.winner_hash.as_slice(),
            r.resolved_at,
        ],
    )?;
    conn.execute(
        "DELETE FROM sync_resolutions WHERE stream_id IN (
            SELECT stream_id FROM sync_resolutions
            ORDER BY resolved_at DESC, stream_id DESC
            LIMIT -1 OFFSET ?1
        )",
        [MAX_SYNC_RESOLUTIONS],
    )?;
    Ok(())
}

pub fn list_conflict_stream_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id FROM sync_conflicts ORDER BY detected_at ASC LIMIT 256",
    )?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn read_resolutions_for_streams(
    conn: &Connection,
    stream_ids: &[String],
) -> Result<Vec<ResolutionReceipt>> {
    let mut out = Vec::new();
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id,kind,chosen,resolver_node_id,local_epoch,local_seq,local_hash,
                remote_epoch,remote_seq,remote_hash,winner_epoch,winner_seq,winner_hash,resolved_at
         FROM sync_resolutions WHERE stream_id=?",
    )?;
    for sid in stream_ids.iter().take(256) {
        if let Some(r) = stmt
            .query_row([sid], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, Vec<u8>>(12)?,
                    row.get::<_, i64>(13)?,
                ))
            })
            .optional()?
        {
            out.push(ResolutionReceipt {
                stream_id: r.0,
                kind: StreamKind::from_db(&r.1)?,
                chosen: r.2,
                resolver_node_id: r.3,
                local_epoch: r.4,
                local_seq: r.5,
                local_hash: to_hash32(r.6)?,
                remote_epoch: r.7,
                remote_seq: r.8,
                remote_hash: to_hash32(r.9)?,
                winner_epoch: r.10,
                winner_seq: r.11,
                winner_hash: to_hash32(r.12)?,
                resolved_at: r.13,
            });
        }
    }
    Ok(out)
}

pub fn apply_resolution_receipt(
    conn: &Connection,
    from_peer: &str,
    receipt: &ResolutionReceipt,
) -> Result<bool> {
    with_savepoint(conn, "apply_res_receipt", || {
        let Some(c) = get_conflict_opt(conn, &receipt.stream_id)? else {
            return Ok(false);
        };
        let Some(l) = stream_head(conn, &receipt.stream_id)? else {
            return Ok(false);
        };
        let local_matches_winner = l.epoch == receipt.winner_epoch
            && l.head_seq == receipt.winner_seq
            && l.head_hash == receipt.winner_hash;
        if !local_matches_winner {
            return Ok(false);
        }
        let exact_pair = (c.local_epoch == receipt.local_epoch
            && c.local_seq == receipt.local_seq
            && c.local_hash == receipt.local_hash
            && c.remote_epoch == receipt.remote_epoch
            && c.remote_seq == receipt.remote_seq
            && c.remote_hash == receipt.remote_hash)
            || (c.local_epoch == receipt.remote_epoch
                && c.local_seq == receipt.remote_seq
                && c.local_hash == receipt.remote_hash
                && c.remote_epoch == receipt.local_epoch
                && c.remote_seq == receipt.local_seq
                && c.remote_hash == receipt.local_hash);
        let peer_matched = c.remote_node_id == from_peer
            && receipt.resolver_node_id == from_peer
            && c.local_epoch == receipt.winner_epoch
            && c.local_seq == receipt.winner_seq
            && c.local_hash == receipt.winner_hash;
        if exact_pair || peer_matched {
            let deleted =
                conn.execute("DELETE FROM sync_conflicts WHERE stream_id=?", [&receipt.stream_id])? > 0;
            return Ok(deleted);
        }
        Ok(false)
    })
}

// --- Epoch Snapshot Writers & Local Conflict Resolution ---

struct EpochSnapshot<'a> {
    stream_id: &'a str,
    kind: StreamKind,
    new_epoch: i64,
    op_type: &'a str,
    payload: String,
    deleted: bool,
    retired: bool,
    head_snapshot: Option<String>,
}

fn replace_stream_with_epoch_snapshot(conn: &Connection, snap: EpochSnapshot<'_>) -> Result<()> {
    let author = load_local_node(conn)?.node_id;
    let now = chrono::Utc::now().timestamp_millis();
    let parent_hash = [0u8; 32];
    let commit_hash = compute_commit_hash(&CommitHashInput {
        stream_id: snap.stream_id,
        kind: snap.kind,
        epoch: snap.new_epoch,
        seq: 1,
        parent_hash: &parent_hash,
        author_node_id: &author,
        op_type: snap.op_type,
        payload: &snap.payload,
        created_at: now,
    });
    conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [snap.stream_id])?;
    conn.execute(
        "DELETE FROM sync_staging_commits WHERE stream_id=?",
        [snap.stream_id],
    )?;
    conn.execute(
        "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,head_snapshot,updated_at)
         VALUES(?1,?2,?3,1,?4,?3,1,?4,1,1,?5,?6,?7,?8)
         ON CONFLICT(stream_id) DO UPDATE SET
           epoch=excluded.epoch,head_seq=1,head_hash=excluded.head_hash,
           synced_epoch=excluded.epoch,synced_seq=1,synced_hash=excluded.head_hash,
           checkpoint_seq=1,watched=1,deleted=excluded.deleted,retired=excluded.retired,
           head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
        params![
            snap.stream_id,
            snap.kind.as_str(),
            snap.new_epoch,
            commit_hash.as_slice(),
            snap.deleted,
            snap.retired,
            snap.head_snapshot,
            now
        ],
    )?;
    conn.execute(
        "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
         VALUES(?,?,1,?,?,?,?,?,?)",
        params![
            snap.stream_id,
            snap.new_epoch,
            parent_hash.as_slice(),
            commit_hash.as_slice(),
            author,
            snap.op_type,
            snap.payload,
            now
        ],
    )?;
    conn.execute("DELETE FROM sync_conflicts WHERE stream_id=?", [snap.stream_id])?;
    stage_stream(snap.stream_id);
    Ok(())
}

/// Write a full session snapshot across bounded `<= 1 MiB` commits in `new_epoch` (P1-13).
fn replace_session_with_chunked_epoch_snapshot(
    conn: &Connection,
    sid: SessionId,
    new_epoch: i64,
) -> Result<()> {
    let (snap, items, context_items, invalidated_warnings, att_refs) =
        load_full_session_rows(conn, sid)?;
    let head_snap_json = serde_json::to_string(&snap)
        .map_err(|e| Error::internal(format!("session snapshot encode failed: {e}")))?;

    let mut deltas: Vec<SessionDelta> = Vec::new();
    let mut cur = SessionDelta::default();
    let mut cur_bytes = 0usize;
    let mut seen_atts = HashSet::new();

    for item in items {
        let mut att_ids = HashSet::new();
        collect_attachment_ids_from_item(&item, &mut att_ids);
        let mut item_atts = Vec::new();
        for id in att_ids {
            if seen_atts.insert(id)
                && let Some(r) = att_refs.get(&id)
            {
                item_atts.push(r.clone());
            }
        }
        let approx = item.payload.len() + item_atts.len() * 128 + 256;
        if (!cur.items.is_empty())
            && (cur.items.len() >= MAX_SESSION_CHUNK_ITEMS
                || cur_bytes + approx > MAX_SESSION_CHUNK_BYTES)
        {
            cur.session = Some(snap.clone());
            deltas.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur.attachments.extend(item_atts);
        cur.items.push(item);
        cur_bytes += approx;
    }

    for ctx in context_items {
        let approx = 48;
        if (!cur.items.is_empty() || !cur.context_items.is_empty())
            && (cur.items.len() + cur.context_items.len() >= MAX_SESSION_CHUNK_ITEMS * 2
                || cur_bytes + approx > MAX_SESSION_CHUNK_BYTES)
        {
            cur.session = Some(snap.clone());
            deltas.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur.context_items.push(ctx);
        cur_bytes += approx;
    }

    for warn_id in invalidated_warnings {
        let approx = 48;
        if (!cur.items.is_empty() || !cur.context_items.is_empty() || !cur.invalidated_warnings.is_empty())
            && cur_bytes + approx > MAX_SESSION_CHUNK_BYTES
        {
            cur.session = Some(snap.clone());
            deltas.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur.invalidated_warnings.push(warn_id);
        cur_bytes += approx;
    }

    cur.session = Some(snap.clone());
    deltas.push(cur);

    let stream_id = sid.as_str();
    let author = load_local_node(conn)?.node_id;
    let now = chrono::Utc::now().timestamp_millis();
    conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [stream_id])?;
    conn.execute(
        "DELETE FROM sync_staging_commits WHERE stream_id=?",
        [stream_id],
    )?;

    let total = deltas.len();
    let mut parent_hash = [0u8; 32];
    let mut final_hash = [0u8; 32];
    let mut seq = 0i64;
    let mut staged_rows = Vec::with_capacity(total);
    for (idx, delta) in deltas.into_iter().enumerate() {
        seq += 1;
        let op_type = if idx == 0 {
            "session_reset_snapshot"
        } else {
            "session_delta"
        };
        let payload = serde_json::to_string(&delta)
            .map_err(|e| Error::internal(format!("chunk encode failed: {e}")))?;
        let commit_hash = compute_commit_hash(&CommitHashInput {
            stream_id,
            kind: StreamKind::Session,
            epoch: new_epoch,
            seq,
            parent_hash: &parent_hash,
            author_node_id: &author,
            op_type,
            payload: &payload,
            created_at: now,
        });
        staged_rows.push((seq, parent_hash, commit_hash, op_type, payload));
        parent_hash = commit_hash;
        final_hash = commit_hash;
    }

    conn.execute(
        "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,reset_gen,head_snapshot,updated_at)
         VALUES(?1,'session',?2,?3,?4,?2,?3,?4,1,1,0,0,0,?5,?6)
         ON CONFLICT(stream_id) DO UPDATE SET
           epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
           synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
           checkpoint_seq=1,watched=1,deleted=0,retired=0,
           reset_gen=sync_streams.reset_gen+1,
           head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
        params![
            stream_id,
            new_epoch,
            seq,
            final_hash.as_slice(),
            head_snap_json,
            now
        ],
    )?;
    for (seq, p_hash, c_hash, op_type, payload) in staged_rows {
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             VALUES(?,?,?,?,?,?,?,?,?)",
            params![
                stream_id,
                new_epoch,
                seq,
                p_hash.as_slice(),
                c_hash.as_slice(),
                author,
                op_type,
                payload,
                now
            ],
        )?;
    }
    conn.execute("DELETE FROM sync_conflicts WHERE stream_id=?", [stream_id])?;
    stage_stream(stream_id);
    Ok(())
}

pub struct ResolveLocalResult {
    pub mcp_changes: Vec<McpChange>,
    pub unblocked_conflicts: Vec<String>,
    pub new_head: Option<StreamHead>,
}

pub fn resolve_conflict_local(
    conn: &Connection,
    stream_id: &str,
    expected: &ConflictRecord,
) -> Result<ResolveLocalResult> {
    with_savepoint(conn, "resolve_local", || {
        let c = get_conflict(conn, stream_id)?;
        if &c != expected {
            return Err(Error::conflict(
                "sync conflict state changed concurrently; retry resolution",
            ));
        }
        let new_epoch = c.local_epoch.max(c.remote_epoch) + 1;
        let mut mcp_changes = Vec::new();
        let now = chrono::Utc::now().timestamp_millis();

        if c.reason == "name_conflict" {
            match c.kind {
                StreamKind::Provider => {
                    let pid: ProviderId =
                        stream_id.parse().map_err(|_| Error::bad("invalid provider id"))?;
                    let exists: bool = conn
                        .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_providers WHERE id=?)")?
                        .query_row([pid], |r| r.get(0))?;
                    let snap = if exists {
                        conn.execute(
                            "UPDATE llm_providers SET archived_at=coalesce(archived_at, ?) WHERE id=?",
                            params![now, pid],
                        )?;
                        read_provider_snapshot(conn, pid)?
                    } else {
                        let raw = c.remote_snapshot.as_deref().ok_or_else(|| {
                            Error::conflict("missing remote provider snapshot to retire")
                        })?;
                        let mut s: ProviderSnapshot = serde_json::from_str(raw)
                            .map_err(|_| Error::bad("invalid provider snapshot"))?;
                        s.archived_at = s.archived_at.or(Some(now));
                        conn.execute(
                            "INSERT INTO llm_providers(id,name,base_url,api_key,archived_at) VALUES(?,?,?,?,?)
                             ON CONFLICT(id) DO UPDATE SET archived_at=excluded.archived_at",
                            params![s.id, s.name, s.base_url, s.api_key, s.archived_at],
                        )?;
                        read_provider_snapshot(conn, pid)?
                    };
                    let payload =
                        serde_json::to_string(&snap).map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Provider,
                            new_epoch,
                            op_type: "provider_upsert",
                            payload,
                            deleted: false,
                            retired: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Model => {
                    let mid: ModelId =
                        stream_id.parse().map_err(|_| Error::bad("invalid model id"))?;
                    let exists: bool = conn
                        .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_models WHERE id=?)")?
                        .query_row([mid], |r| r.get(0))?;
                    let snap = if exists {
                        // P1-26: update archived_at on existing model row, then re-read actual row!
                        conn.execute(
                            "UPDATE llm_models SET archived_at=coalesce(archived_at, ?) WHERE id=?",
                            params![now, mid],
                        )?;
                        read_model_snapshot(conn, mid)?
                    } else {
                        let raw = c.remote_snapshot.as_deref().ok_or_else(|| {
                            Error::conflict("missing remote model snapshot to retire")
                        })?;
                        let mut s: ModelSnapshot = serde_json::from_str(raw)
                            .map_err(|_| Error::bad("invalid model snapshot"))?;
                        s.archived_at = s.archived_at.or(Some(now));
                        let has_prov: bool = conn
                            .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_providers WHERE id=?)")?
                            .query_row([s.provider_id], |r| r.get(0))?;
                        if !has_prov {
                            return Err(Error::conflict(
                                "cannot resolve model name conflict before its provider exists locally",
                            ));
                        }
                        conn.execute(
                            "INSERT INTO llm_models(id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing,archived_at)
                             VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET archived_at=excluded.archived_at",
                            params![
                                s.id,
                                s.provider_id,
                                s.model_key,
                                s.context_window_tokens,
                                s.input_modalities,
                                s.reasoning_efforts,
                                s.pricing,
                                s.archived_at
                            ],
                        )?;
                        read_model_snapshot(conn, mid)?
                    };
                    let payload =
                        serde_json::to_string(&snap).map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Model,
                            new_epoch,
                            op_type: "model_upsert",
                            payload,
                            deleted: false,
                            retired: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Mcp => {
                    let mid: McpServerId =
                        stream_id.parse().map_err(|_| Error::bad("invalid mcp id"))?;
                    conn.execute("DELETE FROM mcp_servers WHERE id=?", [mid])?;
                    mcp_changes.push(McpChange {
                        id: mid,
                        cancel_login: true,
                    });
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Mcp,
                            new_epoch,
                            op_type: "mcp_delete",
                            payload: json!({
                                "id": mid,
                                "base_epoch": c.remote_epoch,
                                "base_seq": c.remote_seq,
                                "base_hash_hex": hex_hash(&c.remote_hash),
                            })
                            .to_string(),
                            deleted: true,
                            retired: true,
                            head_snapshot: None,
                        },
                    )?;
                }
            _ => return Err(Error::bad("unexpected name conflict stream kind")),
            }
        } else {
            // Standard fork or retired_diverged conflict -> resolve keeping local state.
            match c.kind {
                StreamKind::Provider => {
                    let pid: ProviderId =
                        stream_id.parse().map_err(|_| Error::bad("invalid provider id"))?;
                    let snap = read_provider_snapshot(conn, pid)?;
                    let payload =
                        serde_json::to_string(&snap).map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Provider,
                            new_epoch,
                            op_type: "provider_upsert",
                            payload,
                            deleted: false,
                            retired: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Model => {
                    let mid: ModelId =
                        stream_id.parse().map_err(|_| Error::bad("invalid model id"))?;
                    let snap = read_model_snapshot(conn, mid)?;
                    let payload =
                        serde_json::to_string(&snap).map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Model,
                            new_epoch,
                            op_type: "model_upsert",
                            payload,
                            deleted: false,
                            retired: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Mcp => {
                    let mid: McpServerId =
                        stream_id.parse().map_err(|_| Error::bad("invalid mcp id"))?;
                    let is_remote_retired = c.reason == "retired_diverged"
                        || c.remote_snapshot
                            .as_deref()
                            .and_then(|s| serde_json::from_str::<Value>(s).ok())
                            .is_some_and(|v| v.get("base_epoch").is_some() || v.get("server").is_none());
                    let exists_local: Option<bool> = conn
                        .prepare_cached("SELECT shared FROM mcp_servers WHERE id=?")?
                        .query_row([mid], |r| r.get(0))
                        .optional()?;
                    if is_remote_retired {
                        // P1-09: Never resurrect a retired mcp_* ID! Detach local edits to a fresh private
                        // McpServerId and accept retirement on the old stream ID.
                        let (ancestor_start_seq, ancestor_hashes_hex) =
                            collect_ancestor_hashes_hex(conn, stream_id, c.local_epoch, c.local_seq)?;
                        if exists_local.is_some() {
                            detach_conflicted_mcp_to_private(conn, mid)?;
                        }
                        mcp_changes.push(McpChange {
                            id: mid,
                            cancel_login: true,
                        });
                        replace_stream_with_epoch_snapshot(
                            conn,
                            EpochSnapshot {
                                stream_id,
                                kind: StreamKind::Mcp,
                                new_epoch,
                                op_type: "mcp_delete",
                                payload: json!({
                                    "id": mid,
                                    "base_epoch": c.local_epoch,
                                    "base_seq": c.local_seq,
                                    "base_hash_hex": hex_hash(&c.local_hash),
                                    "ancestor_start_seq": ancestor_start_seq,
                                    "ancestor_hashes_hex": ancestor_hashes_hex,
                                })
                                .to_string(),
                                deleted: true,
                                retired: true,
                                head_snapshot: None,
                            },
                        )?;
                    } else if exists_local == Some(true) {
                        // P1-27: promote any staged OAuth refresh credentials matching current generation.
                        promote_staged_oauth_credentials(conn, mid)?;
                        let snap = read_mcp_snapshot(conn, mid)?;
                        let payload = serde_json::to_string(&snap)
                            .map_err(|e| Error::internal(e.to_string()))?;
                        mcp_changes.push(McpChange {
                            id: mid,
                            cancel_login: false,
                        });
                        replace_stream_with_epoch_snapshot(
                            conn,
                            EpochSnapshot {
                                stream_id,
                                kind: StreamKind::Mcp,
                                new_epoch,
                                op_type: "mcp_upsert",
                                payload,
                                deleted: false,
                                retired: false,
                                head_snapshot: None,
                            },
                        )?;
                    } else {
                        let (ancestor_start_seq, ancestor_hashes_hex) =
                            collect_ancestor_hashes_hex(conn, stream_id, c.local_epoch, c.local_seq)?;
                        mcp_changes.push(McpChange {
                            id: mid,
                            cancel_login: true,
                        });
                        replace_stream_with_epoch_snapshot(
                            conn,
                            EpochSnapshot {
                                stream_id,
                                kind: StreamKind::Mcp,
                                new_epoch,
                                op_type: "mcp_delete",
                                payload: json!({
                                    "id": mid,
                                    "base_epoch": c.local_epoch,
                                    "base_seq": c.local_seq,
                                    "base_hash_hex": hex_hash(&c.local_hash),
                                    "ancestor_start_seq": ancestor_start_seq,
                                    "ancestor_hashes_hex": ancestor_hashes_hex,
                                })
                                .to_string(),
                                deleted: true,
                                retired: true,
                                head_snapshot: None,
                            },
                        )?;
                    }
                }
                StreamKind::Call => {
                    let cid: CallId =
                        stream_id.parse().map_err(|_| Error::bad("invalid call id"))?;
                    let snap = read_call_snapshot(conn, cid)?;
                    let payload =
                        serde_json::to_string(&snap).map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Call,
                            new_epoch,
                            op_type: "call_upsert",
                            payload,
                            deleted: false,
                            retired: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Session => {
                    let sid: SessionId =
                        stream_id.parse().map_err(|_| Error::bad("invalid session id"))?;
                    let exists: bool = conn
                        .prepare_cached("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)")?
                        .query_row([sid], |r| r.get(0))?;
                    if exists {
                        // P0-01 CRITICAL: Refuse local resolution unless this node actually possesses
                        // a complete, verified local commit chain matching the conflicted local head!
                        let st = stream_head(conn, stream_id)?;
                        let complete = st.as_ref().is_some_and(|l| {
                            l.is_body_complete()
                                && l.epoch == c.local_epoch
                                && l.head_seq == c.local_seq
                                && l.head_hash == c.local_hash
                                && verify_local_prefix(
                                    conn,
                                    stream_id,
                                    l.epoch,
                                    l.checkpoint_seq,
                                    l.head_seq,
                                    &l.head_hash,
                                )
                                .unwrap_or(false)
                        });
                        if !complete {
                            return Err(Error::conflict(
                                "local node does not hold complete session history for the local branch; sync the local branch from an online holder first",
                            ));
                        }
                        replace_session_with_chunked_epoch_snapshot(conn, sid, new_epoch)?;
                    } else {
                        let st = stream_head(conn, stream_id)?;
                        let verified_deleted = st.as_ref().is_some_and(|l| {
                            l.deleted
                                && l.epoch == c.local_epoch
                                && l.head_seq == c.local_seq
                                && l.head_hash == c.local_hash
                        });
                        if !verified_deleted {
                            return Err(Error::conflict(
                                "local branch missing verified state for conflict resolution",
                            ));
                        }
                        replace_stream_with_epoch_snapshot(
                            conn,
                            EpochSnapshot {
                                stream_id,
                                kind: StreamKind::Session,
                                new_epoch,
                                op_type: "session_delete",
                                payload: json!({ "id": sid }).to_string(),
                                deleted: true,
                                retired: false,
                                head_snapshot: None,
                            },
                        )?;
                    }
                }
            }
        }
        let mut unblocked = Vec::new();
        if c.reason != "name_conflict"
            && matches!(c.kind, StreamKind::Provider | StreamKind::Model | StreamKind::Mcp)
        {
            unblocked = collect_unblocked_conflicts(conn, stream_id)?;
            unblocked.sort();
            unblocked.dedup();
        }
        let new_head = stream_head_wire(conn, stream_id)?;
        if let Some(ref nh) = new_head {
            let resolver = load_local_node(conn)?.node_id;
            record_resolution_receipt(
                conn,
                &ResolutionReceipt {
                    stream_id: stream_id.to_owned(),
                    kind: c.kind,
                    chosen: "local".to_owned(),
                    resolver_node_id: resolver,
                    local_epoch: c.local_epoch,
                    local_seq: c.local_seq,
                    local_hash: c.local_hash,
                    remote_epoch: c.remote_epoch,
                    remote_seq: c.remote_seq,
                    remote_hash: c.remote_hash,
                    winner_epoch: nh.epoch,
                    winner_seq: nh.head_seq,
                    winner_hash: nh.head_hash,
                    resolved_at: now,
                },
            )?;
        }
        Ok(ResolveLocalResult {
            mcp_changes,
            unblocked_conflicts: unblocked,
            new_head,
        })
    })
}

fn promote_staged_oauth_credentials(conn: &Connection, mid: McpServerId) -> Result<()> {
    conn.execute(
        "UPDATE mcp_oauth
         SET credentials=staged_credentials, staged_credentials=NULL, rejected=0
         WHERE server_id=? AND staged_credentials IS NOT NULL",
        [mid],
    )?;
    Ok(())
}

fn detach_conflicted_mcp_to_private(conn: &Connection, old_id: McpServerId) -> Result<()> {
    promote_staged_oauth_credentials(conn, old_id)?;
    let (name, description, enabled, transport, allowed_tools, config_json, timeout_ms): (
        String,
        String,
        bool,
        String,
        Option<String>,
        String,
        i64,
    ) = conn.query_row(
        "SELECT name,description,enabled,transport,allowed_tools,config_json,timeout_ms
         FROM mcp_servers WHERE id=?",
        [old_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
    )?;
    let oauth: Option<(String, Option<String>, Option<String>, bool)> = conn
        .query_row(
            "SELECT generation,binding,credentials,rejected FROM mcp_oauth
             WHERE server_id=? AND (credentials IS NOT NULL OR rejected=1)",
            [old_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let new_id = McpServerId::new()?;
    conn.execute("DELETE FROM mcp_servers WHERE id=?", [old_id])?;
    conn.execute(
        "INSERT INTO mcp_servers(id,name,description,enabled,shared,transport,allowed_tools,config_json,timeout_ms,revision)
         VALUES(?,?,?,?,0,?,?,?,?,1)",
        params![
            new_id,
            name,
            description,
            enabled,
            transport,
            allowed_tools,
            config_json,
            timeout_ms
        ],
    )?;
    if let Some((generation, binding, credentials, rejected)) = oauth {
        conn.execute(
            "INSERT INTO mcp_oauth(server_id,generation,binding,pending,credentials,staged_credentials,rejected)
             VALUES(?,?,?,NULL,?,NULL,?)",
            params![new_id, generation, binding, credentials, rejected],
        )?;
    }
    Ok(())
}

/// Apply a verified, complete staged epoch branch onto a conflicted session WITHOUT deleting the
/// `sync_conflicts` row, so P0-01 conflict resolution has full local branch content locally.
pub fn commit_staged_branch_preserving_conflict(
    conn: &Connection,
    target_head: &StreamHead,
    stream_id: &str,
) -> Result<()> {
    if target_head.stream_id != stream_id
        || target_head.kind != StreamKind::Session
        || target_head.deleted
    {
        return Err(Error::conflict("invalid target branch for conflict staging"));
    }
    let sid: SessionId = stream_id
        .parse()
        .map_err(|_| Error::internal("invalid session stream_id"))?;
    with_savepoint(conn, "commit_staged_preserve_conflict", || {
        let mut check_stmt = conn.prepare_cached(
            "SELECT seq,parent_hash,commit_hash FROM sync_staging_commits
             WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
        )?;
        let staged_hdrs = check_stmt
            .query_map(params![stream_id, target_head.epoch], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if staged_hdrs.len() != usize::try_from(target_head.head_seq).unwrap_or(usize::MAX) {
            return Err(Error::conflict("incomplete staged local branch history"));
        }
        let mut exp_parent = [0u8; 32];
        let mut last_hash = [0u8; 32];
        for (idx, (seq, pvec, cvec)) in staged_hdrs.into_iter().enumerate() {
            let p = to_hash32(pvec)?;
            let ch = to_hash32(cvec)?;
            if seq != (idx as i64) + 1 || p != exp_parent {
                return Err(Error::conflict("non-contiguous staged local branch chain"));
            }
            exp_parent = ch;
            last_hash = ch;
        }
        if last_hash != target_head.head_hash {
            return Err(Error::conflict("staged local branch head_hash mismatch"));
        }

        clear_session_body_rows(conn, sid)?;
        conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [stream_id])?;

        let mut last_snapshot = None;
        let mut last_created_at = target_head.updated_at;
        {
            let mut replay_stmt = conn.prepare_cached(
                "SELECT seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
                 FROM sync_staging_commits WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
            )?;
            let mut rows = replay_stmt.query(params![stream_id, target_head.epoch])?;
            while let Some(r) = rows.next()? {
                let wire = CommitWire {
                    stream_id: stream_id.to_owned(),
                    kind: target_head.kind,
                    epoch: target_head.epoch,
                    seq: r.get(0)?,
                    parent_hash: to_hash32(r.get::<_, Vec<u8>>(1)?)?,
                    commit_hash: to_hash32(r.get::<_, Vec<u8>>(2)?)?,
                    author_node_id: r.get(3)?,
                    op_type: r.get(4)?,
                    payload: r.get(5)?,
                    created_at: r.get(6)?,
                };
                last_created_at = wire.created_at;
                match apply_one_commit(conn, &wire)? {
                    CommitEffect::Applied {
                        deleted,
                        head_snapshot,
                        ..
                    } => {
                        if deleted {
                            return Err(Error::conflict(
                                "local conflict branch unexpectedly contains terminal delete",
                            ));
                        }
                        if head_snapshot.is_some() {
                            last_snapshot = head_snapshot;
                        }
                    }
                    CommitEffect::Blocked | CommitEffect::NameConflict { .. } => {
                        return Err(Error::conflict(
                            "cannot materialize complete local branch while dependency is missing",
                        ));
                    }
                }
            }
        }

        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_epoch,synced_seq,synced_hash,checkpoint_seq,watched,deleted,retired,reset_gen,head_snapshot,updated_at)
             VALUES(?1,'session',?2,?3,?4,?2,?3,?4,1,1,0,0,0,?5,?6)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_epoch=excluded.synced_epoch,synced_seq=excluded.synced_seq,synced_hash=excluded.synced_hash,
               checkpoint_seq=1,watched=1,deleted=0,retired=0,
               reset_gen=sync_streams.reset_gen+1,
               head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                stream_id,
                target_head.epoch,
                target_head.head_seq,
                target_head.head_hash.as_slice(),
                last_snapshot,
                last_created_at
            ],
        )?;
        conn.execute(
            "INSERT INTO sync_commits(stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at)
             SELECT stream_id,epoch,seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
             FROM sync_staging_commits WHERE stream_id=?1 AND epoch=?2 ORDER BY seq ASC",
            params![stream_id, target_head.epoch],
        )?;
        conn.execute(
            "DELETE FROM sync_staging_commits WHERE stream_id=?",
            [stream_id],
        )?;
        Ok(())
    })
}
