use std::{
    cell::RefCell,
    collections::HashSet,
};

use rusqlite::{Connection, OptionalExtension, ffi, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Error, Result,
    cluster::identity::load_local_node,
    id::{self, AttachmentId, CallId, ItemId, McpServerId, ModelId, ProviderId, SessionId},
};

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
pub struct StreamHead {
    pub stream_id: String,
    pub kind: StreamKind,
    pub epoch: i64,
    pub head_seq: i64,
    pub head_hash: [u8; 32],
    pub deleted: bool,
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
    pub synced_seq: i64,
    pub watched: bool,
    pub deleted: bool,
    pub head_snapshot: Option<String>,
    pub updated_at: i64,
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
    pub remote_snapshot: Option<Value>,
    pub detected_at: i64,
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
    pub model_id: Option<ModelId>,
    pub input_queue_cursor: String,
    pub needs_model: bool,
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
}

pub enum ApplyOutcome {
    Applied {
        updated: bool,
        session_id: Option<SessionId>,
        deleted_session: bool,
        new_items: Vec<ItemId>,
        mcp_changed: Option<McpServerId>,
    },
    Conflict,
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
    bytes
        .try_into()
        .map_err(|_| rusqlite::Error::InvalidColumnType(0, "hash".into(), rusqlite::types::Type::Blob))
}

pub fn hex_hash(hash: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

pub fn assert_no_conflict(conn: &Connection, stream_id: &str) -> Result<()> {
    let conflicted: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_conflicts WHERE stream_id=?)")?
        .query_row([stream_id], |r| r.get(0))?;
    if conflicted {
        return Err(Error::conflict(format!(
            "sync conflict on {stream_id}; resolve it before modifying"
        )));
    }
    Ok(())
}

fn with_savepoint<T>(
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

pub fn record_commit(
    conn: &Connection,
    stream_id: &str,
    kind: StreamKind,
    op_type: &str,
    payload_val: &impl Serialize,
    deleted: bool,
    head_snapshot: Option<&str>,
) -> Result<()> {
    let payload = serde_json::to_string(payload_val)
        .map_err(|e| Error::internal(format!("commit encode failed: {e}")))?;
    with_savepoint(conn, "cluster_commit", || {
        assert_no_conflict(conn, stream_id)?;
        let author = load_local_node(conn)?.node_id;
        let now = chrono::Utc::now().timestamp_millis();
        let existing: Option<(i64, i64, Vec<u8>)> = conn
            .prepare_cached(
                "SELECT epoch,head_seq,head_hash FROM sync_streams WHERE stream_id=?",
            )?
            .query_row([stream_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        let (epoch, seq, parent_hash) = match existing {
            Some((epoch, head_seq, head_hash)) => {
                let hash = to_hash32(head_hash)?;
                (epoch, head_seq + 1, hash)
            }
            None => (1, 1, [0u8; 32]),
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
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
             VALUES(?1,?2,?3,?4,?5,?4,1,?6,?7,?8)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,
               head_seq=excluded.head_seq,
               head_hash=excluded.head_hash,
               synced_seq=excluded.synced_seq,
               watched=1,
               deleted=excluded.deleted,
               head_snapshot=excluded.head_snapshot,
               updated_at=excluded.updated_at",
            params![
                stream_id,
                kind.as_str(),
                epoch,
                seq,
                commit_hash.as_slice(),
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

// --- Snapshot readers & local commit hooks ---

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
    // Never sync in-progress login pending state; only sync binding/credentials once authorized or rejected.
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

pub fn record_mcp_deleted(conn: &Connection, id: McpServerId) -> Result<()> {
    let has_stream: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_streams WHERE stream_id=?)")?
        .query_row([id.as_str()], |r| r.get(0))?;
    if !has_stream {
        return Ok(());
    }
    record_commit(
        conn,
        id.as_str(),
        StreamKind::Mcp,
        "mcp_delete",
        &json!({ "id": id }),
        true,
        None,
    )
}

pub fn read_call_snapshot(conn: &Connection, id: CallId) -> Result<CallSnapshot> {
    Ok(conn.query_row(
        "SELECT id,session_id,model_id,reasoning_effort,input_end_position,started_at,finished_at,status,
                upstream_response_id,http_status,error_summary,raw_usage,input_uncached_tokens,
                cache_read_tokens,cache_write_tokens,output_tokens,pricing_snapshot,cost_nano_usd
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

pub fn read_session_snapshot(conn: &Connection, sid: SessionId) -> Result<SessionSnapshot> {
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
        })
    })
    .map_err(Into::into)
}

pub fn read_item_snapshot(conn: &Connection, id: ItemId) -> Result<ItemSnapshot> {
    conn.prepare_cached(
        "SELECT id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id
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
    include_session: bool,
) -> Result<()> {
    let mut items = Vec::with_capacity(item_ids.len());
    for &id in item_ids {
        items.push(read_item_snapshot(conn, id)?);
    }
    items.sort_by_key(|item| item.id);
    let attachments = load_attachment_refs(conn, &items)?;
    let snap = read_session_snapshot(conn, sid)?;
    let head_snap_json = serde_json::to_string(&snap)
        .map_err(|e| Error::internal(format!("session snapshot encode failed: {e}")))?;
    let delta = SessionDelta {
        attachments,
        items,
        context_items,
        invalidated_warnings,
        session: include_session.then_some(snap),
    };
    record_commit(
        conn,
        sid.as_str(),
        StreamKind::Session,
        op_type,
        &delta,
        false,
        Some(&head_snap_json),
    )
}

pub fn record_session_deleted(conn: &Connection, sid: SessionId) -> Result<()> {
    record_commit(
        conn,
        sid.as_str(),
        StreamKind::Session,
        "session_delete",
        &json!({ "id": sid }),
        true,
        None,
    )
}

fn build_full_session_delta(conn: &Connection, sid: SessionId) -> Result<(SessionDelta, String)> {
    let snap = read_session_snapshot(conn, sid)?;
    let head_snap_json = serde_json::to_string(&snap)
        .map_err(|e| Error::internal(format!("session snapshot encode failed: {e}")))?;
    let mut item_stmt = conn.prepare_cached(
        "SELECT id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id
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
    let attachments = load_attachment_refs(conn, &items)?;
    let delta = SessionDelta {
        attachments,
        items,
        context_items,
        invalidated_warnings,
        session: Some(snap),
    };
    Ok((delta, head_snap_json))
}

// --- Manifest, Commit Queries, and Remote Replay ---

pub fn local_manifest(conn: &Connection) -> Result<Vec<StreamHead>> {
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id,kind,epoch,head_seq,head_hash,deleted,head_snapshot,updated_at
         FROM sync_streams ORDER BY kind, stream_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let kind_str: String = r.get(1)?;
            let hash_vec: Vec<u8> = r.get(4)?;
            Ok((kind_str, r.get(0)?, r.get(2)?, r.get(3)?, hash_vec, r.get(5)?, r.get(6)?, r.get(7)?))
        })?
        .collect::<rusqlite::Result<Vec<(String, String, i64, i64, Vec<u8>, bool, Option<String>, i64)>>>()?;
    let mut heads = Vec::with_capacity(rows.len());
    for (kind_str, stream_id, epoch, head_seq, hash_vec, deleted, head_snapshot, updated_at) in rows {
        heads.push(StreamHead {
            stream_id,
            kind: StreamKind::from_db(&kind_str)?,
            epoch,
            head_seq,
            head_hash: to_hash32(hash_vec)?,
            deleted,
            head_snapshot,
            updated_at,
        });
    }
    Ok(heads)
}

pub fn stream_head(conn: &Connection, stream_id: &str) -> Result<Option<LocalStreamState>> {
    let row = conn
        .prepare_cached(
            "SELECT stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at
             FROM sync_streams WHERE stream_id=?",
        )?
        .query_row([stream_id], |r| {
            let kind_str: String = r.get(1)?;
            let hash_vec: Vec<u8> = r.get(4)?;
            Ok((
                r.get::<_, String>(0)?,
                kind_str,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                hash_vec,
                r.get::<_, i64>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, bool>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, i64>(9)?,
            ))
        })
        .optional()?;
    let Some((stream_id, kind_str, epoch, head_seq, hash_vec, synced_seq, watched, deleted, head_snapshot, updated_at)) = row else {
        return Ok(None);
    };
    Ok(Some(LocalStreamState {
        stream_id,
        kind: StreamKind::from_db(&kind_str)?,
        epoch,
        head_seq,
        head_hash: to_hash32(hash_vec)?,
        synced_seq,
        watched,
        deleted,
        head_snapshot,
        updated_at,
    }))
}

pub fn read_commits_range(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    after_seq: i64,
    max_count: usize,
    max_bytes: usize,
) -> Result<(Vec<CommitWire>, bool)> {
    let stream_kind = conn
        .prepare_cached("SELECT kind FROM sync_streams WHERE stream_id=?")?
        .query_row([stream_id], |r| r.get::<_, String>(0))
        .optional()?;
    let Some(kind_str) = stream_kind else {
        return Ok((Vec::new(), false));
    };
    let kind = StreamKind::from_db(&kind_str)?;
    let mut stmt = conn.prepare_cached(
        "SELECT seq,parent_hash,commit_hash,author_node_id,op_type,payload,created_at
         FROM sync_commits WHERE stream_id=?1 AND epoch=?2 AND seq>?3
         ORDER BY seq ASC LIMIT ?4",
    )?;
    let limit = i64::try_from(max_count + 1).unwrap_or(100);
    let rows = stmt
        .query_map(params![stream_id, epoch, after_seq, limit], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut out = Vec::new();
    let mut total_bytes = 0usize;
    let mut has_more = false;
    for (idx, (seq, parent_vec, commit_vec, author_node_id, op_type, payload, created_at)) in
        rows.into_iter().enumerate()
    {
        let approx = payload.len() + 256;
        if idx >= max_count || (!out.is_empty() && total_bytes + approx > max_bytes) {
            has_more = true;
            break;
        }
        total_bytes += approx;
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
    Ok((out, has_more))
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

struct ConflictWrite<'a> {
    stream_id: &'a str,
    kind: StreamKind,
    local_epoch: i64,
    local_seq: i64,
    local_hash: &'a [u8; 32],
    remote: &'a StreamHead,
    remote_node_id: &'a str,
    reason: &'a str,
    remote_snapshot: Option<&'a str>,
}

fn record_conflict_row(conn: &Connection, c: &ConflictWrite<'_>) -> Result<()> {
    let now = chrono::Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO sync_conflicts(stream_id,kind,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,remote_node_id,reason,remote_snapshot,detected_at)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
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
            c.remote_snapshot,
            now
        ],
    )?;
    Ok(())
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
    if let Some(mid) = snap.model_id {
        id::observe_id(mid);
        let exists: bool = conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_models WHERE id=?)")?
            .query_row([mid], |r| r.get(0))?;
        if !exists {
            return Ok(false);
        }
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

pub fn apply_unwatched_session_head(
    conn: &Connection,
    remote: &StreamHead,
    remote_node_id: &str,
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
                    // Watched sessions must sync actual commits rather than bare snapshots.
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote.head_seq < l.head_seq {
                    // Lagging remote peer for an unwatched session is not a conflict.
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote.head_seq == l.head_seq && remote.head_hash != l.head_hash {
                    record_conflict_row(
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
                            remote_snapshot: remote.head_snapshot.as_deref(),
                        },
                    )?;
                    return Ok(ApplyOutcome::Conflict);
                }
            } else {
                // Higher epoch supersedes unwatched & watched alike; clear stale conflict/commits.
                conn.execute(
                    "DELETE FROM sync_conflicts WHERE stream_id=?",
                    [&remote.stream_id],
                )?;
                if l.synced_seq > 0 || l.watched {
                    clear_session_body_rows(conn, sid)?;
                }
                conn.execute(
                    "DELETE FROM sync_commits WHERE stream_id=? AND epoch<?",
                    params![&remote.stream_id, remote.epoch],
                )?;
            }
        }
        if remote.deleted {
            delete_session_local_rows(conn, sid)?;
            conn.execute(
                "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
                 VALUES(?1,'session',?2,?3,?4,0,0,1,NULL,?5)
                 ON CONFLICT(stream_id) DO UPDATE SET
                   epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
                   synced_seq=0,watched=0,deleted=1,head_snapshot=NULL,updated_at=excluded.updated_at",
                params![
                    remote.stream_id,
                    remote.epoch,
                    remote.head_seq,
                    remote.head_hash.as_slice(),
                    remote.updated_at
                ],
            )?;
            return Ok(ApplyOutcome::Applied {
                updated: true,
                session_id: Some(sid),
                deleted_session: true,
                new_items: Vec::new(),
                mcp_changed: None,
            });
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
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
             VALUES(?1,'session',?2,?3,?4,0,0,0,?5,?6)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_seq=0,watched=0,deleted=0,head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                remote.stream_id,
                remote.epoch,
                remote.head_seq,
                remote.head_hash.as_slice(),
                raw_snap,
                remote.updated_at
            ],
        )?;
        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id: Some(sid),
            deleted_session: false,
            new_items: Vec::new(),
            mcp_changed: None,
        })
    })
}

enum CommitEffect {
    Applied {
        deleted: bool,
        head_snapshot: Option<String>,
        session_id: Option<SessionId>,
        deleted_session: bool,
        new_items: Vec<ItemId>,
        mcp_changed: Option<McpServerId>,
    },
    NameConflict(String),
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
                    let existing_id: String = conn
                        .query_row(
                            "SELECT id FROM llm_providers WHERE name=? AND archived_at IS NULL AND id!=?",
                            params![snap.name, snap.id],
                            |r| r.get(0),
                        )
                        .optional()?
                        .unwrap_or_default();
                    return Ok(CommitEffect::NameConflict(format!(
                        "name_conflict:{existing_id}"
                    )));
                }
                return Err(err.into());
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_changed: None,
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
                    let existing_id: String = conn
                        .query_row(
                            "SELECT id FROM llm_models WHERE provider_id=? AND model_key=? AND archived_at IS NULL AND id!=?",
                            params![snap.provider_id, snap.model_key, snap.id],
                            |r| r.get(0),
                        )
                        .optional()?
                        .unwrap_or_default();
                    return Ok(CommitEffect::NameConflict(format!(
                        "name_conflict:{existing_id}"
                    )));
                }
                return Err(err.into());
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_changed: None,
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
                    head_snapshot: None,
                    session_id: None,
                    deleted_session: false,
                    new_items: Vec::new(),
                    mcp_changed: Some(id),
                });
            }
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
                    let existing_id: String = conn
                        .query_row(
                            "SELECT id FROM mcp_servers WHERE name=? AND id!=?",
                            params![s.name, s.id],
                            |r| r.get(0),
                        )
                        .optional()?
                        .unwrap_or_default();
                    return Ok(CommitEffect::NameConflict(format!(
                        "name_conflict:{existing_id}"
                    )));
                }
                return Err(err.into());
            }
            if let Some(oauth) = &snap.oauth {
                conn.execute(
                    "INSERT INTO mcp_oauth(server_id,generation,binding,pending,credentials,rejected)
                     VALUES(?1,?2,?3,NULL,?4,?5)
                     ON CONFLICT(server_id) DO UPDATE SET
                       generation=excluded.generation,binding=excluded.binding,pending=NULL,
                       credentials=excluded.credentials,rejected=excluded.rejected",
                    params![
                        s.id,
                        oauth.generation,
                        oauth.binding,
                        oauth.credentials,
                        oauth.rejected
                    ],
                )?;
            } else {
                // Only remove local OAuth if there is no active pending login or if credentials had existed.
                conn.execute(
                    "DELETE FROM mcp_oauth WHERE server_id=? AND pending IS NULL",
                    [s.id],
                )?;
            }
            Ok(CommitEffect::Applied {
                deleted: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_changed: Some(id),
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
            conn.execute(
                "INSERT INTO llm_calls(id,session_id,model_id,reasoning_effort,input_end_position,started_at,finished_at,status,
                   upstream_response_id,http_status,error_summary,raw_usage,input_uncached_tokens,
                   cache_read_tokens,cache_write_tokens,output_tokens,pricing_snapshot,cost_nano_usd)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                 ON CONFLICT(id) DO UPDATE SET
                   session_id=excluded.session_id,model_id=excluded.model_id,reasoning_effort=excluded.reasoning_effort,
                   input_end_position=excluded.input_end_position,started_at=excluded.started_at,
                   finished_at=excluded.finished_at,status=excluded.status,upstream_response_id=excluded.upstream_response_id,
                   http_status=excluded.http_status,error_summary=excluded.error_summary,raw_usage=excluded.raw_usage,
                   input_uncached_tokens=excluded.input_uncached_tokens,cache_read_tokens=excluded.cache_read_tokens,
                   cache_write_tokens=excluded.cache_write_tokens,output_tokens=excluded.output_tokens,
                   pricing_snapshot=excluded.pricing_snapshot,cost_nano_usd=excluded.cost_nano_usd",
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
                    snap.cost_nano_usd
                ],
            )?;
            Ok(CommitEffect::Applied {
                deleted: false,
                head_snapshot: None,
                session_id: None,
                deleted_session: false,
                new_items: Vec::new(),
                mcp_changed: None,
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
                    head_snapshot: None,
                    session_id: Some(sid),
                    deleted_session: true,
                    new_items: Vec::new(),
                    mcp_changed: None,
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
                let inserted = conn.execute(
                    "INSERT INTO items(id,session_id,kind,payload,target_id,llm_call_id,idempotency_key,codemode_call_item_id)
                     VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
                    params![
                        item.id,
                        item.session_id,
                        item.kind,
                        item.payload,
                        item.target_id,
                        item.llm_call_id,
                        item.idempotency_key,
                        item.codemode_call_item_id
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
            let head_snapshot = match &delta.session {
                Some(snap) => Some(serde_json::to_string(snap).map_err(|e| {
                    Error::internal(format!("failed to encode session head_snapshot: {e}"))
                })?),
                None => {
                    let snap = read_session_snapshot(conn, sid)?;
                    Some(serde_json::to_string(&snap).map_err(|e| {
                        Error::internal(format!("failed to encode session head_snapshot: {e}"))
                    })?)
                }
            };
            Ok(CommitEffect::Applied {
                deleted: false,
                head_snapshot,
                session_id: Some(sid),
                deleted_session: false,
                new_items,
                mcp_changed: None,
            })
        }
    }
}

pub fn apply_remote_commits(
    conn: &Connection,
    remote_head: &StreamHead,
    commits: &[CommitWire],
    remote_node_id: &str,
    mark_watched: bool,
) -> Result<ApplyOutcome> {
    if commits.is_empty() {
        return Ok(ApplyOutcome::Ignored);
    }
    with_savepoint(conn, "apply_remote", || {
        let local = stream_head(conn, &remote_head.stream_id)?;
        let (start_seq, start_parent, overwrite_epoch) = match &local {
            None => (1i64, [0u8; 32], false),
            Some(l) => {
                if remote_head.epoch < l.epoch {
                    return Ok(ApplyOutcome::Ignored);
                }
                if remote_head.epoch > l.epoch {
                    (1i64, [0u8; 32], true)
                } else {
                    let conflicted: bool = conn
                        .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_conflicts WHERE stream_id=?)")?
                        .query_row([&remote_head.stream_id], |r| r.get(0))?;
                    if conflicted {
                        return Ok(ApplyOutcome::Conflict);
                    }
                    if l.synced_seq == 0 {
                        (1i64, [0u8; 32], false)
                    } else {
                        let synced_hash = conn
                            .prepare_cached(
                                "SELECT commit_hash FROM sync_commits WHERE stream_id=? AND epoch=? AND seq=?",
                            )?
                            .query_row(params![&remote_head.stream_id, l.epoch, l.synced_seq], |r| {
                                to_hash32(r.get(0)?)
                            })?;
                        (l.synced_seq + 1, synced_hash, false)
                    }
                }
            }
        };

        // Check continuity against expected start_seq & start_parent.
        let Some(first) = commits.first() else {
            return Ok(ApplyOutcome::Ignored);
        };
        if first.epoch != remote_head.epoch || first.stream_id != remote_head.stream_id {
            return Err(Error::bad("commit stream or epoch mismatch"));
        }
        if first.seq != start_seq || first.parent_hash != start_parent {
            if let Some(l) = &local {
                record_conflict_row(
                    conn,
                    &ConflictWrite {
                        stream_id: &remote_head.stream_id,
                        kind: remote_head.kind,
                        local_epoch: l.epoch,
                        local_seq: l.head_seq,
                        local_hash: &l.head_hash,
                        remote: remote_head,
                        remote_node_id,
                        reason: "diverged",
                        remote_snapshot: remote_head.head_snapshot.as_deref(),
                    },
                )?;
                return Ok(ApplyOutcome::Conflict);
            }
            return Err(Error::bad("non-contiguous commit batch for new stream"));
        }

        // Verify the entire BLAKE3 chain before touching any SQLite table.
        let mut expected_parent = start_parent;
        for (expected_seq, c) in (start_seq..).zip(commits.iter()) {
            if c.epoch != remote_head.epoch
                || c.stream_id != remote_head.stream_id
                || !verify_commit(c, expected_seq, &expected_parent)
            {
                return Err(Error::bad("invalid commit hash chain"));
            }
            expected_parent = c.commit_hash;
        }

        if overwrite_epoch {
            conn.execute(
                "DELETE FROM sync_conflicts WHERE stream_id=?",
                [&remote_head.stream_id],
            )?;
            if remote_head.kind == StreamKind::Session
                && let Ok(sid) = remote_head.stream_id.parse::<SessionId>()
            {
                clear_session_body_rows(conn, sid)?;
            }
            conn.execute(
                "DELETE FROM sync_commits WHERE stream_id=? AND epoch<?",
                params![&remote_head.stream_id, remote_head.epoch],
            )?;
        }

        let mut all_new_items = Vec::new();
        let mut deleted = false;
        let mut last_snapshot = local.as_ref().and_then(|l| l.head_snapshot.clone());
        let mut session_id = None;
        let mut deleted_session = false;
        let mut mcp_changed = None;
        let mut last_commit = commits.first().unwrap();

        for c in commits {
            match apply_one_commit(conn, c)? {
                CommitEffect::Applied {
                    deleted: d,
                    head_snapshot,
                    session_id: s_id,
                    deleted_session: ds,
                    new_items,
                    mcp_changed: mc,
                } => {
                    deleted = d;
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
                    if mc.is_some() {
                        mcp_changed = mc;
                    }
                    last_commit = c;
                }
                CommitEffect::Blocked => {
                    conn.execute_batch("ROLLBACK TO apply_remote")?;
                    return Ok(ApplyOutcome::BlockedOnDependency);
                }
                CommitEffect::NameConflict(reason) => {
                    let payload = c.payload.clone();
                    let le = local.as_ref().map_or(0, |l| l.epoch);
                    let ls = local.as_ref().map_or(0, |l| l.head_seq);
                    let lh = local.as_ref().map_or([0u8; 32], |l| l.head_hash);
                    conn.execute_batch("ROLLBACK TO apply_remote")?;
                    record_conflict_row(
                        conn,
                        &ConflictWrite {
                            stream_id: &remote_head.stream_id,
                            kind: remote_head.kind,
                            local_epoch: le,
                            local_seq: ls,
                            local_hash: &lh,
                            remote: remote_head,
                            remote_node_id,
                            reason: &reason,
                            remote_snapshot: Some(&payload),
                        },
                    )?;
                    return Ok(ApplyOutcome::Conflict);
                }
            }
        }

        let watched = match remote_head.kind {
            StreamKind::Session => mark_watched || local.as_ref().is_some_and(|l| l.watched),
            _ => true,
        };
        let (final_head_seq, final_head_hash) = if local
            .as_ref()
            .is_some_and(|l| !overwrite_epoch && l.head_seq > last_commit.seq && !watched)
        {
            let l = local.as_ref().unwrap();
            (l.head_seq, l.head_hash)
        } else {
            (last_commit.seq, last_commit.commit_hash)
        };
        conn.execute(
            "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(stream_id) DO UPDATE SET
               epoch=excluded.epoch,head_seq=excluded.head_seq,head_hash=excluded.head_hash,
               synced_seq=excluded.synced_seq,watched=excluded.watched,deleted=excluded.deleted,
               head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
            params![
                remote_head.stream_id,
                remote_head.kind.as_str(),
                last_commit.epoch,
                final_head_seq,
                final_head_hash.as_slice(),
                last_commit.seq,
                watched,
                deleted,
                last_snapshot,
                last_commit.created_at
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
        Ok(ApplyOutcome::Applied {
            updated: true,
            session_id,
            deleted_session,
            new_items: all_new_items,
            mcp_changed,
        })
    })
}

pub fn record_fork_conflict(
    conn: &Connection,
    local: &LocalStreamState,
    remote: &StreamHead,
    remote_node_id: &str,
) -> Result<()> {
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
            remote_snapshot: remote.head_snapshot.as_deref(),
        },
    )
}

pub fn list_conflicts(conn: &Connection) -> Result<Vec<ConflictInfo>> {
    let mut stmt = conn.prepare_cached(
        "SELECT stream_id,kind,local_epoch,local_seq,local_hash,remote_epoch,remote_seq,remote_hash,
                remote_node_id,reason,remote_snapshot,detected_at
         FROM sync_conflicts ORDER BY detected_at DESC, stream_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let kind_str: String = r.get(1)?;
            let lh: Vec<u8> = r.get(4)?;
            let rh: Vec<u8> = r.get(7)?;
            let raw_snap: Option<String> = r.get(10)?;
            Ok((
                r.get::<_, String>(0)?,
                kind_str,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                lh,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                rh,
                r.get::<_, String>(8)?,
                r.get::<_, String>(9)?,
                raw_snap,
                r.get::<_, i64>(11)?,
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
        raw_snap,
        detected_at,
    ) in rows
    {
        let remote_snapshot = raw_snap.and_then(|s| serde_json::from_str::<Value>(&s).ok());
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
            remote_snapshot,
            detected_at,
        });
    }
    Ok(out)
}

#[derive(Clone, Debug)]
pub struct ConflictRecord {
    pub kind: StreamKind,
    pub local_epoch: i64,
    pub remote_epoch: i64,
    pub remote_node_id: String,
    pub reason: String,
}

pub fn get_conflict(conn: &Connection, stream_id: &str) -> Result<ConflictRecord> {
    let (kind_str, local_epoch, remote_epoch, remote_node_id, reason): (
        String,
        i64,
        i64,
        String,
        String,
    ) = conn.query_row(
        "SELECT kind,local_epoch,remote_epoch,remote_node_id,reason
         FROM sync_conflicts WHERE stream_id=?",
        [stream_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?;
    Ok(ConflictRecord {
        kind: StreamKind::from_db(&kind_str)?,
        local_epoch,
        remote_epoch,
        remote_node_id,
        reason,
    })
}

struct EpochSnapshot<'a> {
    stream_id: &'a str,
    kind: StreamKind,
    new_epoch: i64,
    op_type: &'a str,
    payload: String,
    deleted: bool,
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
        "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
         VALUES(?1,?2,?3,1,?4,1,1,?5,?6,?7)
         ON CONFLICT(stream_id) DO UPDATE SET
           epoch=excluded.epoch,head_seq=1,head_hash=excluded.head_hash,synced_seq=1,
           watched=1,deleted=excluded.deleted,head_snapshot=excluded.head_snapshot,updated_at=excluded.updated_at",
        params![
            snap.stream_id,
            snap.kind.as_str(),
            snap.new_epoch,
            commit_hash.as_slice(),
            snap.deleted,
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

pub fn resolve_conflict_local(conn: &Connection, stream_id: &str) -> Result<Option<McpServerId>> {
    with_savepoint(conn, "resolve_local", || {
        let c = get_conflict(conn, stream_id)?;
        let new_epoch = c.local_epoch.max(c.remote_epoch) + 1;
        let mut mcp_changed = None;
        if let Some(local_id) = c.reason.strip_prefix("name_conflict:") {
            // Remote stream conflicted on unique name with local entity `local_id`:
            // keep local entity intact and force-retire the remote stream at new_epoch.
            let now = chrono::Utc::now().timestamp_millis();
            match c.kind {
                StreamKind::Provider => {
                    let pid: ProviderId = stream_id.parse().map_err(|_| Error::bad("invalid provider id"))?;
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
                        let raw: String = conn.query_row(
                            "SELECT remote_snapshot FROM sync_conflicts WHERE stream_id=?",
                            [stream_id],
                            |r| r.get(0),
                        )?;
                        let mut s: ProviderSnapshot = serde_json::from_str(&raw)
                            .map_err(|_| Error::bad("missing provider snapshot"))?;
                        s.archived_at = s.archived_at.or(Some(now));
                        conn.execute(
                            "INSERT INTO llm_providers(id,name,base_url,api_key,archived_at) VALUES(?,?,?,?,?)
                             ON CONFLICT(id) DO UPDATE SET archived_at=excluded.archived_at",
                            params![s.id, s.name, s.base_url, s.api_key, s.archived_at],
                        )?;
                        s
                    };
                    let payload = serde_json::to_string(&snap)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Provider,
                            new_epoch,
                            op_type: "provider_upsert",
                            payload,
                            deleted: false,
                            head_snapshot: None,
                        },
                    )?;
                }
                StreamKind::Model => {
                    let mid: ModelId = stream_id.parse().map_err(|_| Error::bad("invalid model id"))?;
                    let raw: String = conn.query_row(
                        "SELECT remote_snapshot FROM sync_conflicts WHERE stream_id=?",
                        [stream_id],
                        |r| r.get(0),
                    )?;
                    let mut s: ModelSnapshot = serde_json::from_str(&raw)
                        .map_err(|_| Error::bad("missing model snapshot"))?;
                    s.archived_at = s.archived_at.or(Some(now));
                    let has_prov: bool = conn
                        .prepare_cached("SELECT EXISTS(SELECT 1 FROM llm_providers WHERE id=?)")?
                        .query_row([s.provider_id], |r| r.get(0))?;
                    if has_prov {
                        conn.execute(
                            "INSERT INTO llm_models(id,provider_id,model_key,context_window_tokens,input_modalities,reasoning_efforts,pricing,archived_at)
                             VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET archived_at=excluded.archived_at",
                            params![s.id, s.provider_id, s.model_key, s.context_window_tokens, s.input_modalities, s.reasoning_efforts, s.pricing, s.archived_at],
                        )?;
                    }
                    let payload = serde_json::to_string(&s)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Model,
                            new_epoch,
                            op_type: "model_upsert",
                            payload,
                            deleted: false,
                            head_snapshot: None,
                        },
                    )?;
                    let _ = mid;
                }
                StreamKind::Mcp => {
                    let mid: McpServerId = stream_id.parse().map_err(|_| Error::bad("invalid mcp id"))?;
                    conn.execute("DELETE FROM mcp_servers WHERE id=?", [mid])?;
                    mcp_changed = Some(mid);
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Mcp,
                            new_epoch,
                            op_type: "mcp_delete",
                            payload: json!({ "id": mid }).to_string(),
                            deleted: true,
                            head_snapshot: None,
                        },
                    )?;
                }
                _ => return Err(Error::bad("unexpected name conflict stream kind")),
            }
            let _ = local_id;
            return Ok(mcp_changed);
        }

        // Standard fork conflict: force-push local state at new_epoch.
        match c.kind {
            StreamKind::Provider => {
                let pid: ProviderId = stream_id.parse().map_err(|_| Error::bad("invalid provider id"))?;
                let snap = read_provider_snapshot(conn, pid)?;
                let payload = serde_json::to_string(&snap)
                    .map_err(|e| Error::internal(e.to_string()))?;
                replace_stream_with_epoch_snapshot(
                    conn,
                    EpochSnapshot {
                        stream_id,
                        kind: StreamKind::Provider,
                        new_epoch,
                        op_type: "provider_upsert",
                        payload,
                        deleted: false,
                        head_snapshot: None,
                    },
                )?;
            }
            StreamKind::Model => {
                let mid: ModelId = stream_id.parse().map_err(|_| Error::bad("invalid model id"))?;
                let snap = read_model_snapshot(conn, mid)?;
                let payload = serde_json::to_string(&snap)
                    .map_err(|e| Error::internal(e.to_string()))?;
                replace_stream_with_epoch_snapshot(
                    conn,
                    EpochSnapshot {
                        stream_id,
                        kind: StreamKind::Model,
                        new_epoch,
                        op_type: "model_upsert",
                        payload,
                        deleted: false,
                        head_snapshot: None,
                    },
                )?;
            }
            StreamKind::Mcp => {
                let mid: McpServerId = stream_id.parse().map_err(|_| Error::bad("invalid mcp id"))?;
                mcp_changed = Some(mid);
                let exists: bool = conn
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM mcp_servers WHERE id=? AND shared=1)")?
                    .query_row([mid], |r| r.get(0))?;
                if exists {
                    let snap = read_mcp_snapshot(conn, mid)?;
                    let payload = serde_json::to_string(&snap)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Mcp,
                            new_epoch,
                            op_type: "mcp_upsert",
                            payload,
                            deleted: false,
                            head_snapshot: None,
                        },
                    )?;
                } else {
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Mcp,
                            new_epoch,
                            op_type: "mcp_delete",
                            payload: json!({ "id": mid }).to_string(),
                            deleted: true,
                            head_snapshot: None,
                        },
                    )?;
                }
            }
            StreamKind::Call => {
                let cid: CallId = stream_id.parse().map_err(|_| Error::bad("invalid call id"))?;
                let snap = read_call_snapshot(conn, cid)?;
                let payload = serde_json::to_string(&snap)
                    .map_err(|e| Error::internal(e.to_string()))?;
                replace_stream_with_epoch_snapshot(
                    conn,
                    EpochSnapshot {
                        stream_id,
                        kind: StreamKind::Call,
                        new_epoch,
                        op_type: "call_upsert",
                        payload,
                        deleted: false,
                        head_snapshot: None,
                    },
                )?;
            }
            StreamKind::Session => {
                let sid: SessionId = stream_id.parse().map_err(|_| Error::bad("invalid session id"))?;
                let exists: bool = conn
                    .prepare_cached("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)")?
                    .query_row([sid], |r| r.get(0))?;
                if exists {
                    let (delta, head_snap_json) = build_full_session_delta(conn, sid)?;
                    let payload = serde_json::to_string(&delta)
                        .map_err(|e| Error::internal(e.to_string()))?;
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Session,
                            new_epoch,
                            op_type: "session_snapshot",
                            payload,
                            deleted: false,
                            head_snapshot: Some(head_snap_json),
                        },
                    )?;
                } else {
                    replace_stream_with_epoch_snapshot(
                        conn,
                        EpochSnapshot {
                            stream_id,
                            kind: StreamKind::Session,
                            new_epoch,
                            op_type: "session_delete",
                            payload: json!({ "id": sid }).to_string(),
                            deleted: true,
                            head_snapshot: None,
                        },
                    )?;
                }
            }
        }
        Ok(mcp_changed)
    })
}

pub fn resolve_conflict_cluster(
    conn: &Connection,
    stream_id: &str,
    remote_head: &StreamHead,
    remote_commits: &[CommitWire],
    remote_node_id: &str,
) -> Result<(Option<SessionId>, bool, Option<McpServerId>)> {
    with_savepoint(conn, "resolve_cluster", || {
        let c = get_conflict(conn, stream_id)?;
        let now = chrono::Utc::now().timestamp_millis();
        // If this was a name conflict, retire/rename the colliding local entry first.
        if let Some(local_id) = c.reason.strip_prefix("name_conflict:")
            && !local_id.is_empty()
        {
            match c.kind {
                StreamKind::Provider => {
                    if let Ok(pid) = local_id.parse::<ProviderId>() {
                        conn.execute(
                            "UPDATE llm_providers SET archived_at=coalesce(archived_at, ?) WHERE id=?",
                            params![now, pid],
                        )?;
                        let has_stream: bool = conn
                            .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_streams WHERE stream_id=?)")?
                            .query_row([pid.as_str()], |r| r.get(0))?;
                        if has_stream {
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
                        let has_stream: bool = conn
                            .prepare_cached("SELECT EXISTS(SELECT 1 FROM sync_streams WHERE stream_id=?)")?
                            .query_row([mid.as_str()], |r| r.get(0))?;
                        if has_stream {
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
                            }
                            Some(false) => {
                                let old_name: String = conn.query_row(
                                    "SELECT name FROM mcp_servers WHERE id=?",
                                    [mid],
                                    |r| r.get(0),
                                )?;
                                let new_name = format!("{}-local-{}", &old_name[..old_name.floor_char_boundary(96)], &mid.suffix()[..6]);
                                conn.execute(
                                    "UPDATE mcp_servers SET name=? WHERE id=?",
                                    params![new_name, mid],
                                )?;
                            }
                            None => {}
                        }
                    }
                }
                _ => {}
            }
        }

        // Clear local stream history & projection so remote chain replays cleanly from seq=1.
        conn.execute("DELETE FROM sync_conflicts WHERE stream_id=?", [stream_id])?;
        conn.execute("DELETE FROM sync_commits WHERE stream_id=?", [stream_id])?;
        conn.execute("DELETE FROM sync_streams WHERE stream_id=?", [stream_id])?;
        if c.kind == StreamKind::Session
            && let Ok(sid) = stream_id.parse::<SessionId>()
        {
            delete_session_local_rows(conn, sid)?;
        }

        if remote_commits.is_empty() && c.kind == StreamKind::Session && remote_head.deleted {
            // Remote deleted an unwatched session without commit history fetched.
            let sid: SessionId = stream_id.parse().map_err(|_| Error::bad("invalid session id"))?;
            conn.execute(
                "INSERT INTO sync_streams(stream_id,kind,epoch,head_seq,head_hash,synced_seq,watched,deleted,head_snapshot,updated_at)
                 VALUES(?1,'session',?2,?3,?4,0,0,1,NULL,?5)",
                params![
                    stream_id,
                    remote_head.epoch,
                    remote_head.head_seq,
                    remote_head.head_hash.as_slice(),
                    remote_head.updated_at
                ],
            )?;
            return Ok((Some(sid), true, None));
        }

        let mark_watched = c.kind == StreamKind::Session;
        match apply_remote_commits(conn, remote_head, remote_commits, remote_node_id, mark_watched)? {
            ApplyOutcome::Applied {
                session_id,
                deleted_session,
                mcp_changed,
                ..
            } => Ok((session_id, deleted_session, mcp_changed)),
            ApplyOutcome::BlockedOnDependency => Err(Error::conflict(
                "cannot apply remote stream yet because a referenced provider, model, or session is missing or conflicted",
            )),
            ApplyOutcome::Conflict => Err(Error::conflict("remote stream still conflicts")),
            ApplyOutcome::Ignored => Err(Error::internal("unexpected ignored cluster resolution")),
        }
    })
}

pub fn has_commit_hash(
    conn: &Connection,
    stream_id: &str,
    epoch: i64,
    seq: i64,
    expected_hash: &[u8; 32],
) -> Result<bool> {
    let found: Option<Vec<u8>> = conn
        .prepare_cached(
            "SELECT commit_hash FROM sync_commits WHERE stream_id=? AND epoch=? AND seq=?",
        )?
        .query_row(params![stream_id, epoch, seq], |r| r.get(0))
        .optional()?;
    Ok(found.as_deref() == Some(expected_hash.as_slice()))
}
