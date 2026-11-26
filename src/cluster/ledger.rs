use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::atomic::{AtomicI64, Ordering},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Error, Result,
    cluster::identity::local_node_id,
    id::{
        self, AttachmentId, CallId, ContextNodeId, ItemId, McpServerId, ModelId, ProviderId,
        SessionId,
    },
};

pub const CALLS_SCOPE: &str = "calls";
const MAX_DELTA_BYTES: usize = 1_500_000;
const DELTA_PAGE_LIMIT: usize = 256;

#[derive(Default)]
struct StagedSignals {
    catalog: bool,
    sessions: Vec<SessionId>,
}

thread_local! {
    static SYNC_STAGED: RefCell<StagedSignals> = const { RefCell::new(StagedSignals { catalog: false, sessions: Vec::new() }) };
}

pub fn stage_catalog() {
    SYNC_STAGED.with(|cell| {
        cell.borrow_mut().catalog = true;
    });
}

pub fn stage_session(sid: SessionId) {
    SYNC_STAGED.with(|cell| {
        let mut st = cell.borrow_mut();
        if !st.sessions.contains(&sid) {
            st.sessions.push(sid);
        }
    });
}

pub fn take_staged() -> (bool, Vec<SessionId>) {
    SYNC_STAGED.with(|cell| {
        let st = std::mem::take(&mut *cell.borrow_mut());
        (st.catalog, st.sessions)
    })
}

pub fn clear_staged() {
    SYNC_STAGED.with(|cell| {
        let mut st = cell.borrow_mut();
        st.catalog = false;
        st.sessions.clear();
    });
}

pub fn session_scope(sid: SessionId) -> String {
    format!("session:{sid}")
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

#[derive(Clone, Copy)]
struct HlcState {
    wall_ms: u64,
    logical: u32,
}

static HLC: std::sync::Mutex<HlcState> = std::sync::Mutex::new(HlcState {
    wall_ms: 0,
    logical: 0,
});

fn parse_hlc(ts: &str) -> Option<(u64, u32)> {
    let b = ts.as_bytes();
    if b.len() < 27 || b.len() > 256 || b[16] != b':' || b[25] != b':' {
        return None;
    }
    let valid_hex = |slice: &[u8]| slice.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    if !valid_hex(&b[..16]) || !valid_hex(&b[17..25]) {
        return None;
    }
    let node_part = &b[26..];
    if node_part.is_empty() || node_part.contains(&0) {
        return None;
    }
    let wall_ms = u64::from_str_radix(&ts[..16], 16).ok()?;
    let logical = u32::from_str_radix(&ts[17..25], 16).ok()?;
    Some((wall_ms, logical))
}

pub fn validate_hlc(ts: &str) -> Result<()> {
    if ts.is_empty() {
        return Ok(());
    }
    parse_hlc(ts).ok_or_else(|| Error::bad("invalid HLC timestamp"))?;
    Ok(())
}

pub fn observe_hlc(ts: &str) {
    if let Some((w, l)) = parse_hlc(ts) {
        let mut st = HLC.lock().unwrap();
        if (w, l) > (st.wall_ms, st.logical) {
            st.wall_ms = w;
            st.logical = l;
        }
    }
}

pub fn tick_hlc_for(node_id: &str) -> String {
    let now_ms = u64::try_from(chrono::Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    let (wall_ms, logical) = {
        let mut st = HLC.lock().unwrap();
        if now_ms > st.wall_ms {
            st.wall_ms = now_ms;
            st.logical = 0;
        } else if st.logical < u32::MAX {
            st.logical += 1;
        } else {
            st.wall_ms = st.wall_ms.saturating_add(1);
            st.logical = 0;
        }
        (st.wall_ms, st.logical)
    };
    format!("{wall_ms:016x}:{logical:08x}:{node_id}")
}

pub fn tick_hlc(conn: &Connection) -> Result<String> {
    let node_id = local_node_id(conn)?;
    Ok(tick_hlc_for(&node_id))
}

pub fn seed_hlc_from_db(conn: &Connection) -> Result<()> {
    const QUERIES: [&str; 21] = [
        "SELECT max(name_ts) FROM llm_providers",
        "SELECT max(base_url_ts) FROM llm_providers",
        "SELECT max(api_key_ts) FROM llm_providers",
        "SELECT max(archived_ts) FROM llm_providers",
        "SELECT max(identity_ts) FROM llm_models",
        "SELECT max(context_window_ts) FROM llm_models",
        "SELECT max(input_modalities_ts) FROM llm_models",
        "SELECT max(reasoning_efforts_ts) FROM llm_models",
        "SELECT max(pricing_ts) FROM llm_models",
        "SELECT max(archived_ts) FROM llm_models",
        "SELECT max(name_ts) FROM mcp_servers",
        "SELECT max(description_ts) FROM mcp_servers",
        "SELECT max(enabled_ts) FROM mcp_servers",
        "SELECT max(allowed_tools_ts) FROM mcp_servers",
        "SELECT max(timeout_ts) FROM mcp_servers",
        "SELECT max(config_ts) FROM mcp_servers",
        "SELECT max(oauth_ts) FROM mcp_servers",
        "SELECT max(title_ts) FROM sessions",
        "SELECT max(status_ts) FROM sessions",
        "SELECT max(selection_ts) FROM sessions",
        "SELECT max(active_head_ts) FROM sessions",
    ];
    for sql in QUERIES {
        if let Some(ts) = conn.query_row(sql, [], |r| r.get::<_, Option<String>>(0))? {
            observe_hlc(&ts);
        }
    }
    const SESSION_QUERIES: [&str; 4] = [
        "SELECT max(title_ts) FROM sessions",
        "SELECT max(status_ts) FROM sessions",
        "SELECT max(selection_ts) FROM sessions",
        "SELECT max(active_head_ts) FROM sessions",
    ];
    for sql in SESSION_QUERIES {
        if let Some(ts) = conn.query_row(sql, [], |r| r.get::<_, Option<String>>(0))? {
            observe_hlc(&ts);
        }
    }
    Ok(())
}

static CATALOG_REV: AtomicI64 = AtomicI64::new(0);

pub fn seed_catalog_rev(conn: &Connection) -> Result<()> {
    const QUERIES: [&str; 4] = [
        "SELECT coalesce(max(sync_rev), 0) FROM llm_providers",
        "SELECT coalesce(max(sync_rev), 0) FROM llm_models",
        "SELECT coalesce(max(sync_rev), 0) FROM mcp_servers WHERE shared = 1",
        "SELECT coalesce(max(sync_rev), 0) FROM sessions",
    ];
    let mut max_rev = 0i64;
    for sql in QUERIES {
        let rev: i64 = conn.query_row(sql, [], |r| r.get(0))?;
        max_rev = max_rev.max(rev);
    }
    CATALOG_REV.store(max_rev, Ordering::Release);
    Ok(())
}

pub fn current_catalog_rev() -> i64 {
    CATALOG_REV.load(Ordering::Acquire)
}

pub fn next_catalog_rev() -> i64 {
    CATALOG_REV.fetch_add(1, Ordering::AcqRel) + 1
}

pub fn next_origin_seq(conn: &Connection, scope: &str, node_id: &str) -> Result<i64> {
    let seq: i64 = conn
        .prepare_cached(
            "INSERT INTO sync_vectors(scope, node_id, seq) VALUES(?1, ?2, 1)
             ON CONFLICT(scope, node_id) DO UPDATE SET seq = sync_vectors.seq + 1
             RETURNING seq",
        )?
        .query_row(params![scope, node_id], |r| r.get(0))?;
    Ok(seq)
}

pub fn advance_vector(conn: &Connection, scope: &str, node_id: &str, seq: i64) -> Result<()> {
    if seq <= 0 || node_id.is_empty() {
        return Ok(());
    }
    conn.prepare_cached(
        "INSERT INTO sync_vectors(scope, node_id, seq) VALUES(?1, ?2, ?3)
         ON CONFLICT(scope, node_id) DO UPDATE SET seq = max(sync_vectors.seq, excluded.seq)",
    )?
    .execute(params![scope, node_id, seq])?;
    Ok(())
}

pub fn read_vector(conn: &Connection, scope: &str) -> Result<BTreeMap<String, i64>> {
    let mut stmt = conn.prepare_cached(
        "SELECT node_id, seq FROM sync_vectors WHERE scope = ?1 ORDER BY node_id",
    )?;
    let rows = stmt
        .query_map([scope], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
    Ok(rows)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchHead {
    pub head_id: ContextNodeId,
    pub depth: i64,
    pub last_item_id: ItemId,
    pub origin_node_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveContextChange {
    Unchanged,
    Appended(usize),
    Switched,
    Cleared,
    Incomplete,
}

#[derive(Clone, Copy, Debug)]
pub struct McpChange {
    pub id: McpServerId,
    pub cancel_login: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub id: AttachmentId,
    pub mime_type: String,
    pub content_hash: [u8; 32],
    pub byte_len: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderWire {
    pub id: ProviderId,
    pub name: String,
    pub name_ts: String,
    pub base_url: String,
    pub base_url_ts: String,
    pub api_key: Option<String>,
    pub api_key_ts: String,
    pub archived_at: Option<i64>,
    pub archived_ts: String,
    pub sync_rev: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelWire {
    pub id: ModelId,
    pub provider_id: ProviderId,
    pub model_key: String,
    pub identity_ts: String,
    pub context_window_tokens: Option<i64>,
    pub context_window_ts: String,
    pub input_modalities: String,
    pub input_modalities_ts: String,
    pub reasoning_efforts: String,
    pub reasoning_efforts_ts: String,
    pub pricing: Option<String>,
    pub pricing_ts: String,
    pub archived_at: Option<i64>,
    pub archived_ts: String,
    pub sync_rev: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpOAuthGrantWire {
    pub generation: String,
    pub binding: Option<String>,
    pub credentials: Option<String>,
    pub rejected: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct McpServerWire {
    pub id: McpServerId,
    pub name: String,
    pub name_ts: String,
    pub description: String,
    pub description_ts: String,
    pub enabled: bool,
    pub enabled_ts: String,
    pub transport: String,
    pub allowed_tools: Option<String>,
    pub allowed_tools_ts: String,
    pub config_json: String,
    pub timeout_ms: i64,
    pub timeout_ts: String,
    pub config_ts: String,
    pub oauth: Option<McpOAuthGrantWire>,
    pub oauth_ts: String,
    pub deleted: bool,
    pub sync_rev: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionWire {
    pub id: SessionId,
    pub title: String,
    pub title_ts: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: String,
    pub last_error: String,
    pub status_ts: String,
    pub model_id: String,
    pub reasoning_effort: Option<String>,
    pub selection_ts: String,
    pub active_head: Option<ContextNodeId>,
    pub active_head_ts: String,
    pub deleted: bool,
    pub sync_rev: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LlmCallWire {
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
    pub origin_node_id: String,
    pub origin_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ItemWire {
    pub id: ItemId,
    pub session_id: SessionId,
    pub kind: String,
    pub payload: String,
    pub target_id: Option<ItemId>,
    pub llm_call_id: Option<CallId>,
    pub idempotency_key: Option<String>,
    pub codemode_call_item_id: Option<ItemId>,
    pub origin_node_id: String,
    pub origin_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextNodeWire {
    pub id: ContextNodeId,
    pub session_id: SessionId,
    pub parent_id: Option<ContextNodeId>,
    pub depth: i64,
    pub item_id: ItemId,
    pub origin_node_id: String,
    pub origin_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnSettledWire {
    pub context_node_id: ContextNodeId,
    pub session_id: SessionId,
    pub origin_node_id: String,
    pub origin_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvalidatedWarningWire {
    pub item_id: ItemId,
    pub session_id: SessionId,
    pub origin_node_id: String,
    pub origin_seq: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CatalogDelta {
    pub db_epoch: String,
    pub head_rev: i64,
    pub next_rev: i64,
    pub has_more: bool,
    #[serde(default)]
    pub providers: Vec<ProviderWire>,
    #[serde(default)]
    pub models: Vec<ModelWire>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerWire>,
    #[serde(default)]
    pub sessions: Vec<SessionWire>,
    #[serde(default)]
    pub calls: Vec<LlmCallWire>,
    #[serde(default)]
    pub calls_vector: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionDelta {
    pub session_id: SessionId,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub has_more: bool,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
    #[serde(default)]
    pub items: Vec<ItemWire>,
    #[serde(default)]
    pub context_nodes: Vec<ContextNodeWire>,
    #[serde(default)]
    pub turn_settled: Vec<TurnSettledWire>,
    #[serde(default)]
    pub invalidated_warnings: Vec<InvalidatedWarningWire>,
    #[serde(default)]
    pub advanced_vector: BTreeMap<String, i64>,
}

#[derive(Default)]
pub struct CatalogMergeOutcome {
    pub changed: bool,
    pub catalog_changed: bool,
    pub mcp_changes: Vec<McpChange>,
    pub changed_sessions: BTreeSet<SessionId>,
    pub resync_sessions: BTreeSet<SessionId>,
    pub switched_sessions: BTreeSet<SessionId>,
    pub deleted_sessions: BTreeSet<SessionId>,
}

#[derive(Default)]
pub struct SessionMergeOutcome {
    pub changed: bool,
    pub deleted: bool,
    pub reset_view: bool,
    pub new_items: Vec<ItemId>,
    pub has_null_attachments: bool,
}

fn lww_take<T>(cur_val: &mut T, cur_ts: &mut String, in_val: T, in_ts: &str) -> bool {
    if in_ts > cur_ts.as_str() {
        *cur_val = in_val;
        *cur_ts = in_ts.to_owned();
        true
    } else {
        false
    }
}

fn lww_take_pair<A, B>(
    cur_a: &mut A,
    cur_b: &mut B,
    cur_ts: &mut String,
    in_a: A,
    in_b: B,
    in_ts: &str,
) -> bool {
    if in_ts > cur_ts.as_str() {
        *cur_a = in_a;
        *cur_b = in_b;
        *cur_ts = in_ts.to_owned();
        true
    } else {
        false
    }
}

pub fn mark_session_watched(conn: &Connection, sid: SessionId) -> Result<bool> {
    let changed = conn
        .prepare_cached("INSERT OR IGNORE INTO watched_sessions(session_id) VALUES(?)")?
        .execute([sid])?;
    Ok(changed > 0)
}

pub fn is_session_watched(conn: &Connection, sid: SessionId) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM watched_sessions WHERE session_id=?)")?
        .query_row([sid], |r| r.get(0))?)
}

pub fn list_watched_sessions(conn: &Connection) -> Result<Vec<SessionId>> {
    let mut stmt = conn.prepare_cached(
        "SELECT w.session_id FROM watched_sessions w
         JOIN sessions s ON s.id = w.session_id
         WHERE s.deleted = 0
         ORDER BY w.session_id",
    )?;
    let rows = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<SessionId>>>()?;
    Ok(rows)
}

pub fn list_session_leaves(conn: &Connection, sid: SessionId) -> Result<Vec<BranchHead>> {
    let mut stmt = conn.prepare_cached(
        "SELECT head_id, depth, last_item_id, origin_node_id
         FROM session_leaves
         WHERE session_id = ?1
         ORDER BY depth DESC, head_id DESC",
    )?;
    let rows = stmt
        .query_map([sid], |r| {
            Ok(BranchHead {
                head_id: r.get(0)?,
                depth: r.get(1)?,
                last_item_id: r.get(2)?,
                origin_node_id: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn node_branch_complete(
    conn: &Connection,
    sid: SessionId,
    head_id: ContextNodeId,
) -> Result<bool> {
    let Some(expected_depth) = conn
        .prepare_cached("SELECT depth FROM context_nodes WHERE id=?1 AND session_id=?2")?
        .query_row(params![head_id, sid], |r| r.get::<_, i64>(0))
        .optional()?
    else {
        return Ok(false);
    };
    let mut stmt = conn.prepare_cached(
        "SELECT c.parent_id, c.depth
         FROM context_nodes c
         JOIN items i ON i.id = c.item_id AND i.session_id = c.session_id
         WHERE c.id = ?1 AND c.session_id = ?2",
    )?;
    let mut cur = Some(head_id);
    let mut expect = expected_depth;
    let mut visited = 0i64;
    while let Some(nid) = cur {
        let Some((parent_id, depth)) = stmt
            .query_row(params![nid, sid], |r| {
                Ok((r.get::<_, Option<ContextNodeId>>(0)?, r.get::<_, i64>(1)?))
            })
            .optional()?
        else {
            return Ok(false);
        };
        if depth != expect || visited >= expected_depth {
            return Ok(false);
        }
        visited += 1;
        expect -= 1;
        if expect == 0 {
            return Ok(parent_id.is_none() && visited == expected_depth);
        }
        if parent_id.is_none() {
            return Ok(false);
        }
        cur = parent_id;
    }
    Ok(false)
}

pub fn session_branch_complete(conn: &Connection, sid: SessionId) -> Result<bool> {
    let row: Option<(bool, Option<ContextNodeId>)> = conn
        .prepare_cached("SELECT deleted, active_head FROM sessions WHERE id=?")?
        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let Some((deleted, active_head)) = row else {
        return Ok(true);
    };
    if deleted {
        return Ok(false);
    }
    match active_head {
        None => Ok(true),
        Some(head_id) => node_branch_complete(conn, sid, head_id),
    }
}

pub fn reconcile_active_context(
    conn: &Connection,
    sid: SessionId,
) -> Result<ActiveContextChange> {
    let row: Option<(bool, Option<ContextNodeId>)> = conn
        .prepare_cached("SELECT deleted, active_head FROM sessions WHERE id=?")?
        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let active_head = match row {
        Some((false, Some(head_id))) => head_id,
        _ => {
            let deleted = conn
                .prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                .execute([sid])?;
            return Ok(if deleted > 0 {
                ActiveContextChange::Cleared
            } else {
                ActiveContextChange::Unchanged
            });
        }
    };

    let cur_tip: Option<(i64, ContextNodeId)> = conn
        .prepare_cached(
            "SELECT position, node_id FROM active_context
             WHERE session_id=?1 ORDER BY position DESC LIMIT 1",
        )?
        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let had_rows = cur_tip.is_some();
    let incomplete = if had_rows {
        ActiveContextChange::Cleared
    } else {
        ActiveContextChange::Incomplete
    };

    let Some(target_depth) = conn
        .prepare_cached("SELECT depth FROM context_nodes WHERE id=?1 AND session_id=?2")?
        .query_row(params![active_head, sid], |r| r.get::<_, i64>(0))
        .optional()?
    else {
        conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
            .execute([sid])?;
        return Ok(incomplete);
    };

    if let Some((cur_pos, cur_node)) = cur_tip {
        if cur_pos == target_depth && cur_node == active_head {
            return Ok(ActiveContextChange::Unchanged);
        }
        if target_depth > cur_pos {
            let mut stmt = conn.prepare_cached(
                "SELECT c.parent_id, c.depth, c.item_id
                 FROM context_nodes c
                 JOIN items i ON i.id = c.item_id AND i.session_id = c.session_id
                 WHERE c.id = ?1 AND c.session_id = ?2",
            )?;
            let mut cur = Some(active_head);
            let mut expect = target_depth;
            let mut tail = Vec::new();
            let mut matched_prefix = false;
            while let Some(nid) = cur {
                if expect == cur_pos {
                    matched_prefix = nid == cur_node;
                    break;
                }
                if expect < cur_pos {
                    break;
                }
                let Some((parent_id, depth, item_id)) = stmt
                    .query_row(params![nid, sid], |r| {
                        Ok((
                            r.get::<_, Option<ContextNodeId>>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, ItemId>(2)?,
                        ))
                    })
                    .optional()?
                else {
                    conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                        .execute([sid])?;
                    return Ok(incomplete);
                };
                if depth != expect {
                    conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                        .execute([sid])?;
                    return Ok(incomplete);
                }
                tail.push((depth, nid, item_id));
                expect -= 1;
                cur = parent_id;
            }
            if matched_prefix && !tail.is_empty() {
                tail.reverse();
                let count = tail.len();
                let mut ins = conn.prepare_cached(
                    "INSERT INTO active_context(session_id, position, node_id, item_id)
                     VALUES(?1, ?2, ?3, ?4)",
                )?;
                for (pos, nid, item_id) in tail {
                    ins.execute(params![sid, pos, nid, item_id])?;
                }
                return Ok(ActiveContextChange::Appended(count));
            }
        }
    }

    let mut stmt = conn.prepare_cached(
        "SELECT c.parent_id, c.depth, c.item_id
         FROM context_nodes c
         JOIN items i ON i.id = c.item_id AND i.session_id = c.session_id
         WHERE c.id = ?1 AND c.session_id = ?2",
    )?;
    let mut cur = Some(active_head);
    let mut expect = target_depth;
    let mut chain = Vec::with_capacity(usize::try_from(target_depth).unwrap_or(0).min(1024));
    while let Some(nid) = cur {
        let Some((parent_id, depth, item_id)) = stmt
            .query_row(params![nid, sid], |r| {
                Ok((
                    r.get::<_, Option<ContextNodeId>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, ItemId>(2)?,
                ))
            })
            .optional()?
        else {
            conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                .execute([sid])?;
            return Ok(incomplete);
        };
        if depth != expect || expect <= 0 {
            conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                .execute([sid])?;
            return Ok(incomplete);
        }
        chain.push((depth, nid, item_id));
        expect -= 1;
        if expect == 0 {
            if parent_id.is_some() {
                conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                    .execute([sid])?;
                return Ok(incomplete);
            }
            break;
        }
        if parent_id.is_none() {
            conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
                .execute([sid])?;
            return Ok(incomplete);
        }
        cur = parent_id;
    }
    if expect != 0 {
        conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
            .execute([sid])?;
        return Ok(incomplete);
    }

    conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
        .execute([sid])?;
    chain.reverse();
    let count = chain.len();
    let mut ins = conn.prepare_cached(
        "INSERT INTO active_context(session_id, position, node_id, item_id)
         VALUES(?1, ?2, ?3, ?4)",
    )?;
    for (pos, nid, item_id) in chain {
        ins.execute(params![sid, pos, nid, item_id])?;
    }
    Ok(if had_rows {
        ActiveContextChange::Switched
    } else {
        ActiveContextChange::Appended(count)
    })
}

pub fn on_item_inserted(
    conn: &Connection,
    sid: SessionId,
    id: ItemId,
    kind: &str,
    payload: &Value,
    target_id: Option<ItemId>,
) -> Result<()> {
    match kind {
        "user_message" => {
            conn.prepare_cached(
                "INSERT OR IGNORE INTO pending_inputs(session_id, item_id, kind)
                 SELECT ?1, ?2, 'user_message'
                 WHERE NOT EXISTS(SELECT 1 FROM items w WHERE w.session_id=?1 AND w.kind='withdrawal' AND w.target_id=?2)
                   AND NOT EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=?1 AND c.item_id=?2)",
            )?
            .execute(params![sid, id])?;
        }
        "codemode_event" => {
            if payload.get("notify").and_then(Value::as_bool) == Some(true) {
                conn.prepare_cached(
                    "INSERT OR IGNORE INTO pending_inputs(session_id, item_id, kind)
                     SELECT ?1, ?2, 'codemode_event'
                     WHERE NOT EXISTS(SELECT 1 FROM invalidated_warnings iw WHERE iw.item_id=?2)
                       AND NOT EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=?1 AND c.item_id=?2)",
                )?
                .execute(params![sid, id])?;
            }
        }
        "withdrawal" => {
            if let Some(target) = target_id {
                conn.prepare_cached(
                    "DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2",
                )?
                .execute(params![sid, target])?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn on_context_node_inserted(
    conn: &Connection,
    sid: SessionId,
    id: ContextNodeId,
    parent_id: Option<ContextNodeId>,
    depth: i64,
    item_id: ItemId,
    origin_node_id: &str,
) -> Result<()> {
    conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?
        .execute(params![sid, item_id])?;
    if let Some(pid) = parent_id {
        conn.prepare_cached("DELETE FROM session_leaves WHERE session_id=?1 AND head_id=?2")?
            .execute(params![sid, pid])?;
    }
    conn.prepare_cached(
        "INSERT OR IGNORE INTO session_leaves(session_id, head_id, depth, last_item_id, origin_node_id)
         SELECT ?1, ?2, ?3, ?4, ?5
         WHERE NOT EXISTS(SELECT 1 FROM context_nodes WHERE session_id=?1 AND parent_id=?2)",
    )?
    .execute(params![sid, id, depth, item_id, origin_node_id])?;
    Ok(())
}

pub fn append_context_items(
    conn: &Connection,
    sid: SessionId,
    item_ids: &[ItemId],
    touch_updated_at: Option<i64>,
) -> Result<Vec<ContextNodeId>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let node_id = local_node_id(conn)?.to_string();
    let scope = session_scope(sid);
    let mut parent_id: Option<ContextNodeId> = conn
        .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
        .query_row([sid], |r| r.get(0))
        .optional()?
        .flatten();
    let mut depth: i64 = match parent_id {
        Some(hid) => conn
            .prepare_cached("SELECT depth FROM context_nodes WHERE id=?1 AND session_id=?2")?
            .query_row(params![hid, sid], |r| r.get(0))?,
        None => 0,
    };

    let mut created = Vec::with_capacity(item_ids.len());
    for &item_id in item_ids {
        let cid = ContextNodeId::new()?;
        depth += 1;
        let origin_seq = next_origin_seq(conn, &scope, &node_id)?;
        conn.prepare_cached(
            "INSERT INTO context_nodes(id, session_id, parent_id, depth, item_id, origin_node_id, origin_seq)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?
        .execute(params![cid, sid, parent_id, depth, item_id, &node_id, origin_seq])?;
        on_context_node_inserted(conn, sid, cid, parent_id, depth, item_id, &node_id)?;
        conn.prepare_cached(
            "INSERT INTO active_context(session_id, position, node_id, item_id)
             VALUES(?1, ?2, ?3, ?4)",
        )?
        .execute(params![sid, depth, cid, item_id])?;
        parent_id = Some(cid);
        created.push(cid);
    }

    let last_head = *created.last().unwrap();
    let ts = tick_hlc_for(&node_id);
    let rev = next_catalog_rev();
    if let Some(now) = touch_updated_at {
        conn.prepare_cached(
            "UPDATE sessions
             SET active_head=?2, active_head_ts=?3, updated_at=max(updated_at, ?4), sync_rev=?5
             WHERE id=?1",
        )?
        .execute(params![sid, last_head, ts, now, rev])?;
    } else {
        conn.prepare_cached(
            "UPDATE sessions
             SET active_head=?2, active_head_ts=?3, sync_rev=?4
             WHERE id=?1",
        )?
        .execute(params![sid, last_head, ts, rev])?;
    }
    mark_session_watched(conn, sid)?;
    stage_session(sid);
    stage_catalog();
    Ok(created)
}

pub fn mark_turn_settled(conn: &Connection, sid: SessionId) -> Result<bool> {
    let active_head: Option<ContextNodeId> = conn
        .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
        .query_row([sid], |r| r.get(0))
        .optional()?
        .flatten();
    let Some(head_id) = active_head else {
        return Ok(false);
    };
    let already: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM turn_settled WHERE context_node_id=?)")?
        .query_row([head_id], |r| r.get(0))?;
    if already {
        return Ok(false);
    }
    let node_id = local_node_id(conn)?;
    let origin_seq = next_origin_seq(conn, &session_scope(sid), &node_id)?;
    let changed = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO turn_settled(context_node_id, session_id, origin_node_id, origin_seq)
             VALUES(?1, ?2, ?3, ?4)",
        )?
        .execute(params![head_id, sid, &*node_id, origin_seq])?;
    if changed > 0 {
        stage_session(sid);
    }
    Ok(changed > 0)
}

pub fn tombstone_session(conn: &Connection, sid: SessionId, now: i64) -> Result<bool> {
    let exists: Option<bool> = conn
        .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
        .query_row([sid], |r| r.get(0))
        .optional()?;
    let Some(already_deleted) = exists else {
        return Ok(false);
    };
    let mut bump = false;
    if !already_deleted {
        let rev = next_catalog_rev();
        conn.prepare_cached(
            "UPDATE sessions
             SET deleted=1, title='', title_ts='', updated_at=max(updated_at, ?2),
                 status='idle', last_error='', status_ts='',
                 model_id='', reasoning_effort=NULL, selection_ts='',
                 active_head=NULL, active_head_ts='', sync_rev=?3
             WHERE id=?1",
        )?
        .execute(params![sid, now, rev])?;
        bump = true;
    }
    cleanup_deleted_session_data(conn, sid)?;
    if bump {
        stage_catalog();
        stage_session(sid);
    }
    Ok(true)
}

fn cleanup_deleted_session_data(conn: &Connection, sid: SessionId) -> Result<()> {
    conn.prepare_cached("UPDATE llm_calls SET session_id=NULL WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM active_context WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM session_leaves WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM turn_settled WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM invalidated_warnings WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM context_nodes WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM items WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM watched_sessions WHERE session_id=?")?
        .execute([sid])?;
    conn.prepare_cached("DELETE FROM sync_vectors WHERE scope=?")?
        .execute([session_scope(sid)])?;
    Ok(())
}

pub fn tombstone_shared_mcp(conn: &Connection, id: McpServerId) -> Result<bool> {
    let row: Option<(bool, bool)> = conn
        .prepare_cached("SELECT shared, deleted FROM mcp_servers WHERE id=?")?
        .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    let Some((shared, deleted)) = row else {
        return Ok(false);
    };
    if !shared {
        let changed = conn
            .prepare_cached("DELETE FROM mcp_servers WHERE id=? AND shared=0")?
            .execute([id])?;
        conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
            .execute([id])?;
        return Ok(changed > 0);
    }
    if deleted {
        return Ok(false);
    }
    let ts = tick_hlc(conn)?;
    let rev = next_catalog_rev();
    conn.prepare_cached(
        "UPDATE mcp_servers
         SET deleted=1, name='', name_ts=?2,
             description='', description_ts=?2,
             enabled=0, enabled_ts=?2,
             transport='streamable-http', config_json='{}', revision=revision+1, config_ts=?2,
             allowed_tools=NULL, allowed_tools_ts=?2,
             timeout_ms=1, timeout_ts=?2,
             oauth_ts=?2, sync_rev=?3
         WHERE id=?1",
    )?
    .execute(params![id, ts, rev])?;
    conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
        .execute([id])?;
    stage_catalog();
    Ok(true)
}

pub fn load_mcp_wire(conn: &Connection, id: McpServerId) -> Result<Option<McpServerWire>> {
    let row: Option<McpServerWire> = conn
        .prepare_cached(
            "SELECT m.id, m.name, m.name_ts, m.description, m.description_ts,
                    m.enabled, m.enabled_ts, m.transport, m.allowed_tools, m.allowed_tools_ts,
                    m.config_json, m.timeout_ms, m.timeout_ts, m.config_ts, m.oauth_ts,
                    m.deleted, m.sync_rev,
                    o.generation, o.binding, o.credentials, o.rejected
             FROM mcp_servers m
             LEFT JOIN mcp_oauth o ON o.server_id = m.id
             WHERE m.id=?1 AND m.shared=1",
        )?
        .query_row([id], |r| {
            let deleted: bool = r.get(15)?;
            let oauth_gen: Option<String> = r.get(17)?;
            let credentials: Option<String> = r.get(19)?;
            let rejected: Option<bool> = r.get(20)?;
            let oauth = if !deleted
                && let Some(generation) = oauth_gen
                && (credentials.is_some() || rejected == Some(true))
            {
                Some(McpOAuthGrantWire {
                    generation,
                    binding: r.get(18)?,
                    credentials,
                    rejected: rejected.unwrap_or(false),
                })
            } else {
                None
            };
            Ok(McpServerWire {
                id: r.get(0)?,
                name: r.get(1)?,
                name_ts: r.get(2)?,
                description: r.get(3)?,
                description_ts: r.get(4)?,
                enabled: r.get(5)?,
                enabled_ts: r.get(6)?,
                transport: r.get(7)?,
                allowed_tools: r.get(8)?,
                allowed_tools_ts: r.get(9)?,
                config_json: r.get(10)?,
                timeout_ms: r.get(11)?,
                timeout_ts: r.get(12)?,
                config_ts: r.get(13)?,
                oauth,
                oauth_ts: r.get(14)?,
                deleted,
                sync_rev: r.get(16)?,
            })
        })
        .optional()?;
    Ok(row)
}

pub fn merge_provider_wire(
    conn: &Connection,
    w: &ProviderWire,
) -> Result<(bool, Option<ProviderId>)> {
    id::observe_id(w.id);
    validate_hlc(&w.name_ts)?;
    validate_hlc(&w.base_url_ts)?;
    validate_hlc(&w.api_key_ts)?;
    validate_hlc(&w.archived_ts)?;
    observe_hlc(&w.name_ts);
    observe_hlc(&w.base_url_ts);
    observe_hlc(&w.api_key_ts);
    observe_hlc(&w.archived_ts);

    let local = conn
        .prepare_cached(
            "SELECT name, name_ts, base_url, base_url_ts, api_key, api_key_ts, archived_at, archived_ts
             FROM llm_providers WHERE id=?",
        )?
        .query_row([w.id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, String>(7)?,
            ))
        })
        .optional()?;

    let Some((
        mut name,
        mut name_ts,
        mut base_url,
        mut base_url_ts,
        mut api_key,
        mut api_key_ts,
        mut archived_at,
        mut archived_ts,
    )) = local
    else {
        let rev = next_catalog_rev();
        conn.prepare_cached(
            "INSERT INTO llm_providers(id,name,name_ts,base_url,base_url_ts,api_key,api_key_ts,archived_at,archived_ts,sync_rev)
             VALUES(?,?,?,?,?,?,?,?,?,?)",
        )?
        .execute(params![
            w.id,
            &w.name,
            &w.name_ts,
            &w.base_url,
            &w.base_url_ts,
            &w.api_key,
            &w.api_key_ts,
            w.archived_at,
            &w.archived_ts,
            rev
        ])?;
        return Ok((true, Some(w.id)));
    };

    let mut changed = false;
    changed |= lww_take(&mut name, &mut name_ts, w.name.clone(), &w.name_ts);
    changed |= lww_take(
        &mut base_url,
        &mut base_url_ts,
        w.base_url.clone(),
        &w.base_url_ts,
    );
    changed |= lww_take(&mut api_key, &mut api_key_ts, w.api_key.clone(), &w.api_key_ts);
    changed |= lww_take(
        &mut archived_at,
        &mut archived_ts,
        w.archived_at,
        &w.archived_ts,
    );
    if !changed {
        return Ok((false, None));
    }
    let rev = next_catalog_rev();
    conn.prepare_cached(
        "UPDATE llm_providers
         SET name=?2, name_ts=?3, base_url=?4, base_url_ts=?5,
             api_key=?6, api_key_ts=?7, archived_at=?8, archived_ts=?9, sync_rev=?10
         WHERE id=?1",
    )?
    .execute(params![
        w.id,
        name,
        name_ts,
        base_url,
        base_url_ts,
        api_key,
        api_key_ts,
        archived_at,
        archived_ts,
        rev
    ])?;
    Ok((true, Some(w.id)))
}

pub fn merge_model_wire(conn: &Connection, w: &ModelWire) -> Result<(bool, Option<ModelId>)> {
    id::observe_id(w.id);
    id::observe_id(w.provider_id);
    validate_hlc(&w.identity_ts)?;
    validate_hlc(&w.context_window_ts)?;
    validate_hlc(&w.input_modalities_ts)?;
    validate_hlc(&w.reasoning_efforts_ts)?;
    validate_hlc(&w.pricing_ts)?;
    validate_hlc(&w.archived_ts)?;
    observe_hlc(&w.identity_ts);
    observe_hlc(&w.context_window_ts);
    observe_hlc(&w.input_modalities_ts);
    observe_hlc(&w.reasoning_efforts_ts);
    observe_hlc(&w.pricing_ts);
    observe_hlc(&w.archived_ts);

    let local = conn
        .prepare_cached(
            "SELECT provider_id, model_key, identity_ts,
                    context_window_tokens, context_window_ts,
                    input_modalities, input_modalities_ts,
                    reasoning_efforts, reasoning_efforts_ts,
                    pricing, pricing_ts, archived_at, archived_ts
             FROM llm_models WHERE id=?",
        )?
        .query_row([w.id], |r| {
            Ok((
                r.get::<_, ProviderId>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
                r.get::<_, Option<String>>(9)?,
                r.get::<_, String>(10)?,
                r.get::<_, Option<i64>>(11)?,
                r.get::<_, String>(12)?,
            ))
        })
        .optional()?;

    let Some((
        mut provider_id,
        mut model_key,
        mut identity_ts,
        mut cw,
        mut cw_ts,
        mut modalities,
        mut modalities_ts,
        mut efforts,
        mut efforts_ts,
        mut pricing,
        mut pricing_ts,
        mut archived_at,
        mut archived_ts,
    )) = local
    else {
        let rev = next_catalog_rev();
        conn.prepare_cached(
            "INSERT INTO llm_models(
                id, provider_id, model_key, identity_ts,
                context_window_tokens, context_window_ts,
                input_modalities, input_modalities_ts,
                reasoning_efforts, reasoning_efforts_ts,
                pricing, pricing_ts, archived_at, archived_ts, sync_rev
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )?
        .execute(params![
            w.id,
            w.provider_id,
            &w.model_key,
            &w.identity_ts,
            w.context_window_tokens,
            &w.context_window_ts,
            &w.input_modalities,
            &w.input_modalities_ts,
            &w.reasoning_efforts,
            &w.reasoning_efforts_ts,
            &w.pricing,
            &w.pricing_ts,
            w.archived_at,
            &w.archived_ts,
            rev
        ])?;
        return Ok((true, Some(w.id)));
    };

    let mut changed = false;
    changed |= lww_take_pair(
        &mut provider_id,
        &mut model_key,
        &mut identity_ts,
        w.provider_id,
        w.model_key.clone(),
        &w.identity_ts,
    );
    changed |= lww_take(&mut cw, &mut cw_ts, w.context_window_tokens, &w.context_window_ts);
    changed |= lww_take(
        &mut modalities,
        &mut modalities_ts,
        w.input_modalities.clone(),
        &w.input_modalities_ts,
    );
    changed |= lww_take(
        &mut efforts,
        &mut efforts_ts,
        w.reasoning_efforts.clone(),
        &w.reasoning_efforts_ts,
    );
    changed |= lww_take(&mut pricing, &mut pricing_ts, w.pricing.clone(), &w.pricing_ts);
    changed |= lww_take(
        &mut archived_at,
        &mut archived_ts,
        w.archived_at,
        &w.archived_ts,
    );
    if !changed {
        return Ok((false, None));
    }
    let rev = next_catalog_rev();
    conn.prepare_cached(
        "UPDATE llm_models
         SET provider_id=?2, model_key=?3, identity_ts=?4,
             context_window_tokens=?5, context_window_ts=?6,
             input_modalities=?7, input_modalities_ts=?8,
             reasoning_efforts=?9, reasoning_efforts_ts=?10,
             pricing=?11, pricing_ts=?12, archived_at=?13, archived_ts=?14, sync_rev=?15
         WHERE id=?1",
    )?
    .execute(params![
        w.id,
        provider_id,
        model_key,
        identity_ts,
        cw,
        cw_ts,
        modalities,
        modalities_ts,
        efforts,
        efforts_ts,
        pricing,
        pricing_ts,
        archived_at,
        archived_ts,
        rev
    ])?;
    Ok((true, Some(w.id)))
}

pub fn merge_mcp_server_wire(
    conn: &Connection,
    w: &McpServerWire,
) -> Result<(bool, Option<McpChange>)> {
    id::observe_id(w.id);
    for ts in [
        &w.name_ts,
        &w.description_ts,
        &w.enabled_ts,
        &w.allowed_tools_ts,
        &w.timeout_ts,
        &w.config_ts,
        &w.oauth_ts,
    ] {
        validate_hlc(ts)?;
        observe_hlc(ts);
    }

    let local = conn
        .prepare_cached(
            "SELECT m.name, m.name_ts, m.description, m.description_ts,
                    m.enabled, m.enabled_ts, m.shared, m.transport,
                    m.allowed_tools, m.allowed_tools_ts, m.config_json,
                    m.timeout_ms, m.timeout_ts, m.config_ts, m.oauth_ts, m.deleted,
                    o.generation, o.binding, o.credentials, o.rejected
             FROM mcp_servers m
             LEFT JOIN mcp_oauth o ON o.server_id = m.id
             WHERE m.id=?1",
        )?
        .query_row([w.id], |r| {
            let oauth_gen: Option<String> = r.get(16)?;
            let oauth = oauth_gen.map(|generation| McpOAuthGrantWire {
                generation,
                binding: r.get(17).unwrap_or(None),
                credentials: r.get(18).unwrap_or(None),
                rejected: r.get::<_, Option<bool>>(19).unwrap_or(None).unwrap_or(false),
            });
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, bool>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, String>(9)?,
                r.get::<_, String>(10)?,
                r.get::<_, i64>(11)?,
                r.get::<_, String>(12)?,
                r.get::<_, String>(13)?,
                r.get::<_, String>(14)?,
                r.get::<_, bool>(15)?,
                oauth,
            ))
        })
        .optional()?;

    let Some((
        mut name,
        mut name_ts,
        mut description,
        mut description_ts,
        mut enabled,
        mut enabled_ts,
        shared,
        mut transport,
        mut allowed_tools,
        mut allowed_tools_ts,
        mut config_json,
        mut timeout_ms,
        mut timeout_ts,
        mut config_ts,
        mut oauth_ts,
        local_deleted,
        local_oauth,
    )) = local
    else {
        let rev = next_catalog_rev();
        if w.deleted {
            conn.prepare_cached(
                "INSERT INTO mcp_servers(
                    id, name, name_ts, description, description_ts, enabled, enabled_ts,
                    shared, transport, allowed_tools, allowed_tools_ts, config_json,
                    timeout_ms, timeout_ts, revision, config_ts, oauth_ts, deleted, sync_rev
                 ) VALUES(?1,'',?2,'',?2,0,?2,1,'streamable-http',NULL,?2,'{}',1,?2,1,?2,?2,1,?3)",
            )?
            .execute(params![w.id, &w.name_ts, rev])?;
            return Ok((true, Some(McpChange { id: w.id, cancel_login: true })));
        }
        conn.prepare_cached(
            "INSERT INTO mcp_servers(
                id, name, name_ts, description, description_ts, enabled, enabled_ts,
                shared, transport, allowed_tools, allowed_tools_ts, config_json,
                timeout_ms, timeout_ts, revision, config_ts, oauth_ts, deleted, sync_rev
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8,?9,?10,?11,?12,?13,1,?14,?15,0,?16)",
        )?
        .execute(params![
            w.id,
            &w.name,
            &w.name_ts,
            &w.description,
            &w.description_ts,
            w.enabled,
            &w.enabled_ts,
            &w.transport,
            &w.allowed_tools,
            &w.allowed_tools_ts,
            &w.config_json,
            w.timeout_ms,
            &w.timeout_ts,
            &w.config_ts,
            &w.oauth_ts,
            rev
        ])?;
        if w.oauth_ts >= w.config_ts
            && let Some(ref g) = w.oauth
        {
            conn.prepare_cached(
                "INSERT INTO mcp_oauth(server_id, generation, binding, pending, credentials, pending_refresh, rejected)
                 VALUES(?1, ?2, ?3, NULL, ?4, NULL, ?5)",
            )?
            .execute(params![w.id, &g.generation, &g.binding, &g.credentials, g.rejected])?;
        }
        return Ok((true, Some(McpChange { id: w.id, cancel_login: false })));
    };

    if local_deleted {
        return Ok((false, None));
    }
    if w.deleted {
        if !shared {
            return Ok((false, None));
        }
        let rev = next_catalog_rev();
        let ts = tick_hlc(conn)?;
        conn.prepare_cached(
            "UPDATE mcp_servers
             SET deleted=1, name='', name_ts=?2,
                 description='', description_ts=?2,
                 enabled=0, enabled_ts=?2,
                 transport='streamable-http', config_json='{}', revision=revision+1, config_ts=?2,
                 allowed_tools=NULL, allowed_tools_ts=?2,
                 timeout_ms=1, timeout_ts=?2,
                 oauth_ts=?2, sync_rev=?3
             WHERE id=?1",
        )?
        .execute(params![w.id, ts, rev])?;
        conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
            .execute([w.id])?;
        return Ok((true, Some(McpChange { id: w.id, cancel_login: true })));
    }
    if !shared {
        return Ok((false, None));
    }

    let mut changed = false;
    let mut cancel_login = false;
    changed |= lww_take(&mut name, &mut name_ts, w.name.clone(), &w.name_ts);
    changed |= lww_take(
        &mut description,
        &mut description_ts,
        w.description.clone(),
        &w.description_ts,
    );
    changed |= lww_take(&mut enabled, &mut enabled_ts, w.enabled, &w.enabled_ts);
    if !enabled && changed {
        cancel_login = true;
    }
    changed |= lww_take(
        &mut allowed_tools,
        &mut allowed_tools_ts,
        w.allowed_tools.clone(),
        &w.allowed_tools_ts,
    );
    changed |= lww_take(&mut timeout_ms, &mut timeout_ts, w.timeout_ms, &w.timeout_ts);
    let config_changed = lww_take_pair(
        &mut transport,
        &mut config_json,
        &mut config_ts,
        w.transport.clone(),
        w.config_json.clone(),
        &w.config_ts,
    );
    changed |= config_changed;
    if config_changed {
        cancel_login = true;
    }

    let incoming_grant_ts = w.oauth_ts.as_str();
    let grant_newer = incoming_grant_ts > oauth_ts.as_str();
    let config_invalidates_local = config_changed && oauth_ts.as_str() < config_ts.as_str();

    let mut oauth_changed = false;
    let next_grant: Option<Option<&McpOAuthGrantWire>> = if grant_newer {
        if w.oauth.is_none() || incoming_grant_ts >= config_ts.as_str() {
            oauth_ts = incoming_grant_ts.to_owned();
            oauth_changed = true;
            Some(w.oauth.as_ref())
        } else {
            oauth_ts = config_ts.clone();
            oauth_changed = local_oauth.is_some();
            Some(None)
        }
    } else if config_invalidates_local {
        oauth_ts = config_ts.clone();
        oauth_changed = local_oauth.is_some();
        Some(None)
    } else {
        None
    };

    if oauth_changed {
        changed = true;
        cancel_login = true;
    }
    if !changed {
        return Ok((false, None));
    }

    let rev = next_catalog_rev();
    conn.prepare_cached(
        "UPDATE mcp_servers
         SET name=?2, name_ts=?3, description=?4, description_ts=?5,
             enabled=?6, enabled_ts=?7, transport=?8,
             allowed_tools=?9, allowed_tools_ts=?10,
             config_json=?11, timeout_ms=?12, timeout_ts=?13,
             revision=revision+1, config_ts=?14, oauth_ts=?15, sync_rev=?16
         WHERE id=?1",
    )?
    .execute(params![
        w.id,
        name,
        name_ts,
        description,
        description_ts,
        enabled,
        enabled_ts,
        transport,
        allowed_tools,
        allowed_tools_ts,
        config_json,
        timeout_ms,
        timeout_ts,
        config_ts,
        oauth_ts,
        rev
    ])?;

    if let Some(grant_opt) = next_grant {
        match grant_opt {
            Some(g) => {
                conn.prepare_cached(
                    "INSERT INTO mcp_oauth(server_id, generation, binding, pending, credentials, pending_refresh, rejected)
                     VALUES(?1, ?2, ?3, NULL, ?4, NULL, ?5)
                     ON CONFLICT(server_id) DO UPDATE SET
                       generation=excluded.generation,
                       binding=excluded.binding,
                       pending=NULL,
                       credentials=excluded.credentials,
                       pending_refresh=NULL,
                       rejected=excluded.rejected",
                )?
                .execute(params![w.id, &g.generation, &g.binding, &g.credentials, g.rejected])?;
            }
            None => {
                conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
                    .execute([w.id])?;
            }
        }
    }

    Ok((true, Some(McpChange { id: w.id, cancel_login })))
}

pub enum SessionMergeResult {
    Unchanged,
    Updated {
        deleted: bool,
        branch_switched: bool,
        context_advanced: bool,
    },
}

pub fn merge_session_wire(conn: &Connection, w: &SessionWire) -> Result<SessionMergeResult> {
    id::observe_id(w.id);
    if !w.model_id.is_empty() {
        let _ = id::observe_id_str(&w.model_id);
    }
    if let Some(head) = w.active_head {
        id::observe_id(head);
    }
    for ts in [&w.title_ts, &w.status_ts, &w.selection_ts, &w.active_head_ts] {
        validate_hlc(ts)?;
        observe_hlc(ts);
    }

    let local = conn
        .prepare_cached(
            "SELECT title, title_ts, created_at, updated_at,
                    status, last_error, status_ts,
                    model_id, reasoning_effort, selection_ts,
                    active_head, active_head_ts, deleted
             FROM sessions WHERE id=?",
        )?
        .query_row([w.id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, String>(9)?,
                r.get::<_, Option<ContextNodeId>>(10)?,
                r.get::<_, String>(11)?,
                r.get::<_, bool>(12)?,
            ))
        })
        .optional()?;

    let wire_status = match w.status.as_str() {
        "paused" => "paused",
        _ => "idle",
    };
    let wire_err = if wire_status == "paused" {
        w.last_error.as_str()
    } else {
        ""
    };

    let Some((
        mut title,
        mut title_ts,
        created_at,
        mut updated_at,
        mut status,
        mut last_error,
        mut status_ts,
        mut model_id,
        mut reasoning_effort,
        mut selection_ts,
        mut active_head,
        mut active_head_ts,
        local_deleted,
    )) = local
    else {
        let rev = next_catalog_rev();
        if w.deleted {
            conn.prepare_cached(
                "INSERT INTO sessions(
                    id, title, title_ts, created_at, updated_at,
                    status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                    active_head, active_head_ts, deleted, sync_rev
                 ) VALUES(?1,'','',?2,?3,'idle','','','',NULL,'',NULL,'',1,?4)",
            )?
            .execute(params![w.id, w.created_at, w.updated_at, rev])?;
            cleanup_deleted_session_data(conn, w.id)?;
            return Ok(SessionMergeResult::Updated {
                deleted: true,
                branch_switched: false,
                context_advanced: false,
            });
        }
        conn.prepare_cached(
            "INSERT INTO sessions(
                id, title, title_ts, created_at, updated_at,
                status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                active_head, active_head_ts, deleted, sync_rev
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,?14)",
        )?
        .execute(params![
            w.id,
            &w.title,
            &w.title_ts,
            w.created_at,
            w.updated_at,
            wire_status,
            wire_err,
            &w.status_ts,
            &w.model_id,
            &w.reasoning_effort,
            &w.selection_ts,
            w.active_head,
            &w.active_head_ts,
            rev
        ])?;
        let (branch_switched, context_advanced) = if w.active_head.is_some() {
            match reconcile_active_context(conn, w.id)? {
                ActiveContextChange::Switched | ActiveContextChange::Cleared => (true, false),
                ActiveContextChange::Appended(_) => (false, true),
                ActiveContextChange::Unchanged | ActiveContextChange::Incomplete => (false, false),
            }
        } else {
            (false, false)
        };
        return Ok(SessionMergeResult::Updated {
            deleted: false,
            branch_switched,
            context_advanced,
        });
    };

    if local_deleted {
        return Ok(SessionMergeResult::Unchanged);
    }
    if w.deleted {
        let rev = next_catalog_rev();
        let next_updated = updated_at.max(w.updated_at);
        conn.prepare_cached(
            "UPDATE sessions
             SET deleted=1, title='', title_ts='', updated_at=?2,
                 status='idle', last_error='', status_ts='',
                 model_id='', reasoning_effort=NULL, selection_ts='',
                 active_head=NULL, active_head_ts='', sync_rev=?3
             WHERE id=?1",
        )?
        .execute(params![w.id, next_updated, rev])?;
        cleanup_deleted_session_data(conn, w.id)?;
        return Ok(SessionMergeResult::Updated {
            deleted: true,
            branch_switched: false,
            context_advanced: false,
        });
    }

    let mut changed = false;
    changed |= lww_take(&mut title, &mut title_ts, w.title.clone(), &w.title_ts);
    let next_created = if created_at == 0 || (w.created_at > 0 && w.created_at < created_at) {
        w.created_at
    } else {
        created_at
    };
    if next_created != created_at {
        changed = true;
    }
    if w.updated_at > updated_at {
        updated_at = w.updated_at;
        changed = true;
    }
    if !matches!(status.as_str(), "running" | "pausing") {
        changed |= lww_take_pair(
            &mut status,
            &mut last_error,
            &mut status_ts,
            wire_status.to_owned(),
            wire_err.to_owned(),
            &w.status_ts,
        );
    }
    if !w.model_id.is_empty() {
        changed |= lww_take_pair(
            &mut model_id,
            &mut reasoning_effort,
            &mut selection_ts,
            w.model_id.clone(),
            w.reasoning_effort.clone(),
            &w.selection_ts,
        );
    }
    let head_changed = lww_take(
        &mut active_head,
        &mut active_head_ts,
        w.active_head,
        &w.active_head_ts,
    );
    changed |= head_changed;

    if !changed {
        return Ok(SessionMergeResult::Unchanged);
    }

    let rev = next_catalog_rev();
    conn.prepare_cached(
        "UPDATE sessions
         SET title=?2, title_ts=?3, created_at=?4, updated_at=?5,
             status=?6, last_error=?7, status_ts=?8,
             model_id=?9, reasoning_effort=?10, selection_ts=?11,
             active_head=?12, active_head_ts=?13, sync_rev=?14
         WHERE id=?1",
    )?
    .execute(params![
        w.id,
        title,
        title_ts,
        next_created,
        updated_at,
        status,
        last_error,
        status_ts,
        model_id,
        reasoning_effort,
        selection_ts,
        active_head,
        active_head_ts,
        rev
    ])?;

    let (branch_switched, context_advanced) = if head_changed {
        match reconcile_active_context(conn, w.id)? {
            ActiveContextChange::Switched | ActiveContextChange::Cleared => (true, false),
            ActiveContextChange::Appended(_) => (false, true),
            ActiveContextChange::Unchanged | ActiveContextChange::Incomplete => (false, false),
        }
    } else {
        (false, false)
    };

    Ok(SessionMergeResult::Updated {
        deleted: false,
        branch_switched,
        context_advanced,
    })
}

pub fn merge_llm_call_wire(conn: &Connection, w: &LlmCallWire) -> Result<bool> {
    id::observe_id(w.id);
    id::observe_id(w.model_id);
    if let Some(sid) = w.session_id {
        id::observe_id(sid);
    }
    if !matches!(
        w.status.as_str(),
        "in_flight" | "succeeded" | "failed" | "cancelled" | "interrupted"
    ) {
        return Err(Error::bad("invalid llm_calls status"));
    }

    let effective_sid = match w.session_id {
        Some(sid) => {
            let deleted: bool = conn
                .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)")?
                .query_row([sid], |r| r.get(0))?;
            if deleted { None } else { Some(sid) }
        }
        None => None,
    };

    let existing: Option<(Option<SessionId>, String, i64)> = conn
        .prepare_cached("SELECT session_id, status, origin_seq FROM llm_calls WHERE id=?")?
        .query_row([w.id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .optional()?;

    let mut changed = false;
    match existing {
        None => {
            conn.prepare_cached(
                "INSERT INTO llm_calls(
                    id, session_id, model_id, reasoning_effort, input_end_position,
                    started_at, finished_at, status, upstream_response_id, http_status,
                    error_summary, raw_usage, input_uncached_tokens, cache_read_tokens,
                    cache_write_tokens, output_tokens, pricing_snapshot, cost_nano_usd,
                    origin_node_id, origin_seq
                 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            )?
            .execute(params![
                w.id,
                effective_sid,
                w.model_id,
                &w.reasoning_effort,
                w.input_end_position,
                w.started_at,
                w.finished_at,
                &w.status,
                &w.upstream_response_id,
                w.http_status,
                &w.error_summary,
                &w.raw_usage,
                w.input_uncached_tokens,
                w.cache_read_tokens,
                w.cache_write_tokens,
                w.output_tokens,
                &w.pricing_snapshot,
                w.cost_nano_usd,
                &w.origin_node_id,
                w.origin_seq
            ])?;
            changed = true;
        }
        Some((cur_sid, cur_status, cur_seq)) => {
            let cur_terminal = cur_status != "in_flight";
            let in_terminal = w.status != "in_flight";
            let upgrade_status = !cur_terminal && in_terminal
                || (cur_terminal && in_terminal && w.origin_seq > cur_seq);
            let detach_sid = cur_sid.is_some() && effective_sid.is_none();
            if upgrade_status || detach_sid {
                let next_sid = if cur_sid.is_none() || effective_sid.is_none() {
                    None
                } else {
                    cur_sid
                };
                if upgrade_status {
                    conn.prepare_cached(
                        "UPDATE llm_calls
                         SET session_id=?2, finished_at=?3, status=?4, upstream_response_id=?5,
                             http_status=?6, error_summary=?7, raw_usage=?8,
                             input_uncached_tokens=?9, cache_read_tokens=?10,
                             cache_write_tokens=?11, output_tokens=?12,
                             pricing_snapshot=?13, cost_nano_usd=?14, origin_seq=?15
                         WHERE id=?1",
                    )?
                    .execute(params![
                        w.id,
                        next_sid,
                        w.finished_at,
                        &w.status,
                        &w.upstream_response_id,
                        w.http_status,
                        &w.error_summary,
                        &w.raw_usage,
                        w.input_uncached_tokens,
                        w.cache_read_tokens,
                        w.cache_write_tokens,
                        w.output_tokens,
                        &w.pricing_snapshot,
                        w.cost_nano_usd,
                        w.origin_seq.max(cur_seq)
                    ])?;
                } else {
                    conn.prepare_cached("UPDATE llm_calls SET session_id=NULL WHERE id=?")?
                        .execute([w.id])?;
                }
                changed = true;
            }
        }
    }
    advance_vector(conn, CALLS_SCOPE, &w.origin_node_id, w.origin_seq)?;
    Ok(changed)
}

pub fn apply_catalog_delta(conn: &Connection, delta: &CatalogDelta) -> Result<CatalogMergeOutcome> {
    with_savepoint(conn, "merge_catalog_delta", || {
        let mut out = CatalogMergeOutcome::default();
        for p in &delta.providers {
            let (changed, _) = merge_provider_wire(conn, p)?;
            if changed {
                out.changed = true;
                out.catalog_changed = true;
            }
        }
        for m in &delta.models {
            let (changed, _) = merge_model_wire(conn, m)?;
            if changed {
                out.changed = true;
                out.catalog_changed = true;
            }
        }
        for s in &delta.mcp_servers {
            let (changed, ch) = merge_mcp_server_wire(conn, s)?;
            if changed {
                out.changed = true;
            }
            if let Some(ch) = ch {
                out.mcp_changes.push(ch);
            }
        }
        for sess in &delta.sessions {
            match merge_session_wire(conn, sess)? {
                SessionMergeResult::Unchanged => {}
                SessionMergeResult::Updated {
                    deleted,
                    branch_switched,
                    context_advanced,
                } => {
                    out.changed = true;
                    if deleted {
                        out.deleted_sessions.insert(sess.id);
                    } else {
                        out.changed_sessions.insert(sess.id);
                        if branch_switched {
                            out.switched_sessions.insert(sess.id);
                        } else if context_advanced {
                            out.resync_sessions.insert(sess.id);
                        }
                    }
                }
            }
        }
        for call in &delta.calls {
            if merge_llm_call_wire(conn, call)? {
                out.changed = true;
            }
        }
        for (node_id, &seq) in &delta.calls_vector {
            advance_vector(conn, CALLS_SCOPE, node_id, seq)?;
        }
        Ok(out)
    })
}

pub fn merge_attachment_refs(conn: &Connection, refs: &[AttachmentRef]) -> Result<bool> {
    let mut has_missing = false;
    for r in refs {
        id::observe_id(r.id);
        if r.byte_len <= 0 || r.byte_len > crate::media::MAX_ATTACHMENT_BYTES as i64 {
            return Err(Error::bad("invalid attachment metadata byte_len"));
        }
        let inserted = conn
            .prepare_cached(
                "INSERT OR IGNORE INTO attachments(id, mime_type, content_hash, byte_len, data)
                 VALUES(?1, ?2, ?3, ?4, NULL)",
            )?
            .execute(params![r.id, &r.mime_type, r.content_hash.as_slice(), r.byte_len])?;
        if inserted > 0 {
            let alias: Option<i64> = conn
                .prepare_cached(
                    "SELECT rowid FROM attachments
                     WHERE content_hash=?1 AND byte_len=?2 AND data IS NOT NULL LIMIT 1",
                )?
                .query_row(params![r.content_hash.as_slice(), r.byte_len], |row| row.get(0))
                .optional()?;
            if let Some(rowid) = alias {
                conn.prepare_cached(
                    "UPDATE attachments SET data=(SELECT data FROM attachments WHERE rowid=?1)
                     WHERE id=?2 AND data IS NULL",
                )?
                .execute(params![rowid, r.id])?;
            } else {
                has_missing = true;
            }
        }
    }
    Ok(has_missing)
}

pub fn apply_session_delta(conn: &Connection, delta: &SessionDelta) -> Result<SessionMergeOutcome> {
    let sid = delta.session_id;
    id::observe_id(sid);
    with_savepoint(conn, "merge_session_delta", || {
        let mut out = SessionMergeOutcome::default();
        let local_deleted: bool = conn
            .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)")?
            .query_row([sid], |r| r.get(0))?;
        if local_deleted {
            return Ok(out);
        }
        if delta.deleted {
            let exists: bool = conn
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?)")?
                .query_row([sid], |r| r.get(0))?;
            let now = chrono::Utc::now().timestamp_millis();
            if exists {
                tombstone_session(conn, sid, now)?;
            } else {
                let rev = next_catalog_rev();
                conn.prepare_cached(
                    "INSERT INTO sessions(
                        id, title, title_ts, created_at, updated_at,
                        status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                        active_head, active_head_ts, deleted, sync_rev
                     ) VALUES(?1,'','',?2,?2,'idle','','','',NULL,'',NULL,'',1,?3)",
                )?
                .execute(params![sid, now, rev])?;
                cleanup_deleted_session_data(conn, sid)?;
            }
            out.changed = true;
            out.deleted = true;
            return Ok(out);
        }

        if !delta.attachments.is_empty() {
            out.has_null_attachments |= merge_attachment_refs(conn, &delta.attachments)?;
        }

        let scope = session_scope(sid);
        let mut content_touched = false;

        for iw in &delta.invalidated_warnings {
            id::observe_id(iw.item_id);
            id::observe_id(iw.session_id);
            if iw.session_id != sid {
                return Err(Error::bad("invalidated_warning session mismatch"));
            }
            let ins = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO invalidated_warnings(item_id, session_id, origin_node_id, origin_seq)
                     VALUES(?1, ?2, ?3, ?4)",
                )?
                .execute(params![iw.item_id, sid, &iw.origin_node_id, iw.origin_seq])?;
            if ins > 0 {
                conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?
                    .execute(params![sid, iw.item_id])?;
                out.changed = true;
                content_touched = true;
            }
        }

        for item in &delta.items {
            id::observe_id(item.id);
            id::observe_id(item.session_id);
            if let Some(t) = item.target_id {
                id::observe_id(t);
            }
            if let Some(c) = item.llm_call_id {
                id::observe_id(c);
            }
            if let Some(cm) = item.codemode_call_item_id {
                id::observe_id(cm);
            }
            if item.session_id != sid {
                return Err(Error::bad("item session mismatch"));
            }
            let payload_val: Value = serde_json::from_str(&item.payload)
                .map_err(|_| Error::bad("invalid item payload JSON"))?;
            let ins = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO items(
                        id, session_id, kind, payload, target_id, llm_call_id,
                        idempotency_key, codemode_call_item_id, origin_node_id, origin_seq
                     ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                )?
                .execute(params![
                    item.id,
                    sid,
                    &item.kind,
                    &item.payload,
                    item.target_id,
                    item.llm_call_id,
                    &item.idempotency_key,
                    item.codemode_call_item_id,
                    &item.origin_node_id,
                    item.origin_seq
                ])?;
            if ins > 0 {
                on_item_inserted(conn, sid, item.id, &item.kind, &payload_val, item.target_id)?;
                out.new_items.push(item.id);
                out.changed = true;
                content_touched = true;
            }
        }

        for node in &delta.context_nodes {
            id::observe_id(node.id);
            id::observe_id(node.session_id);
            id::observe_id(node.item_id);
            if let Some(p) = node.parent_id {
                id::observe_id(p);
            }
            if node.session_id != sid {
                return Err(Error::bad("context_node session mismatch"));
            }
            let ins = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO context_nodes(
                        id, session_id, parent_id, depth, item_id, origin_node_id, origin_seq
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )?
                .execute(params![
                    node.id,
                    sid,
                    node.parent_id,
                    node.depth,
                    node.item_id,
                    &node.origin_node_id,
                    node.origin_seq
                ])?;
            if ins > 0 {
                on_context_node_inserted(
                    conn,
                    sid,
                    node.id,
                    node.parent_id,
                    node.depth,
                    node.item_id,
                    &node.origin_node_id,
                )?;
                out.changed = true;
                content_touched = true;
            }
        }

        for ts in &delta.turn_settled {
            id::observe_id(ts.context_node_id);
            id::observe_id(ts.session_id);
            if ts.session_id != sid {
                return Err(Error::bad("turn_settled session mismatch"));
            }
            let ins = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO turn_settled(
                        context_node_id, session_id, origin_node_id, origin_seq
                     ) VALUES(?1, ?2, ?3, ?4)",
                )?
                .execute(params![
                    ts.context_node_id,
                    sid,
                    &ts.origin_node_id,
                    ts.origin_seq
                ])?;
            if ins > 0 {
                out.changed = true;
            }
        }

        for (origin_node_id, &seq) in &delta.advanced_vector {
            advance_vector(conn, &scope, origin_node_id, seq)?;
        }

        if content_touched {
            let change = reconcile_active_context(conn, sid)?;
            if matches!(
                change,
                ActiveContextChange::Switched | ActiveContextChange::Cleared
            ) {
                out.reset_view = true;
            }
        }
        Ok(out)
    })
}

enum CatalogRow {
    Provider(ProviderWire),
    Model(ModelWire),
    Mcp(McpServerWire),
    Session(SessionWire),
}

impl CatalogRow {
    fn sync_rev(&self) -> i64 {
        match self {
            Self::Provider(r) => r.sync_rev,
            Self::Model(r) => r.sync_rev,
            Self::Mcp(r) => r.sync_rev,
            Self::Session(r) => r.sync_rev,
        }
    }

    fn approx_bytes(&self) -> usize {
        match self {
            Self::Provider(r) => r.name.len() + r.base_url.len() + r.api_key.as_deref().map_or(0, str::len) + 128,
            Self::Model(r) => {
                r.model_key.len()
                    + r.input_modalities.len()
                    + r.reasoning_efforts.len()
                    + r.pricing.as_deref().map_or(0, str::len)
                    + 192
            }
            Self::Mcp(r) => {
                r.name.len()
                    + r.description.len()
                    + r.config_json.len()
                    + r.allowed_tools.as_deref().map_or(0, str::len)
                    + r.oauth.as_ref().map_or(256, |o| {
                        o.generation.len()
                            + o.binding.as_deref().map_or(0, str::len)
                            + o.credentials.as_deref().map_or(0, str::len)
                    })
                    + 256
            }
            Self::Session(r) => r.title.len() + r.last_error.len() + 256,
        }
    }
}

pub fn read_catalog_delta(
    conn: &Connection,
    since_rev: i64,
    remote_calls_vector: &BTreeMap<String, i64>,
) -> Result<CatalogDelta> {
    let db_epoch: String =
        conn.query_row("SELECT db_epoch FROM local_node WHERE id=1", [], |r| r.get(0))?;
    let head_rev = current_catalog_rev();
    let mut rows: Vec<CatalogRow> = Vec::new();
    let mut safe_ceiling = i64::MAX;

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, name, name_ts, base_url, base_url_ts, api_key, api_key_ts,
                    archived_at, archived_ts, sync_rev
             FROM llm_providers WHERE sync_rev > ?1
             ORDER BY sync_rev ASC, id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                Ok(ProviderWire {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    name_ts: r.get(2)?,
                    base_url: r.get(3)?,
                    base_url_ts: r.get(4)?,
                    api_key: r.get(5)?,
                    api_key_ts: r.get(6)?,
                    archived_at: r.get(7)?,
                    archived_ts: r.get(8)?,
                    sync_rev: r.get(9)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if list.len() == DELTA_PAGE_LIMIT
            && let Some(last) = list.last()
        {
            safe_ceiling = safe_ceiling.min(last.sync_rev);
        }
        rows.extend(list.into_iter().map(CatalogRow::Provider));
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, provider_id, model_key, identity_ts,
                    context_window_tokens, context_window_ts,
                    input_modalities, input_modalities_ts,
                    reasoning_efforts, reasoning_efforts_ts,
                    pricing, pricing_ts, archived_at, archived_ts, sync_rev
             FROM llm_models WHERE sync_rev > ?1
             ORDER BY sync_rev ASC, id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                Ok(ModelWire {
                    id: r.get(0)?,
                    provider_id: r.get(1)?,
                    model_key: r.get(2)?,
                    identity_ts: r.get(3)?,
                    context_window_tokens: r.get(4)?,
                    context_window_ts: r.get(5)?,
                    input_modalities: r.get(6)?,
                    input_modalities_ts: r.get(7)?,
                    reasoning_efforts: r.get(8)?,
                    reasoning_efforts_ts: r.get(9)?,
                    pricing: r.get(10)?,
                    pricing_ts: r.get(11)?,
                    archived_at: r.get(12)?,
                    archived_ts: r.get(13)?,
                    sync_rev: r.get(14)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if list.len() == DELTA_PAGE_LIMIT
            && let Some(last) = list.last()
        {
            safe_ceiling = safe_ceiling.min(last.sync_rev);
        }
        rows.extend(list.into_iter().map(CatalogRow::Model));
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT m.id, m.name, m.name_ts, m.description, m.description_ts,
                    m.enabled, m.enabled_ts, m.transport, m.allowed_tools, m.allowed_tools_ts,
                    m.config_json, m.timeout_ms, m.timeout_ts, m.config_ts, m.oauth_ts,
                    m.deleted, m.sync_rev,
                    o.generation, o.binding, o.credentials, o.rejected
             FROM mcp_servers m
             LEFT JOIN mcp_oauth o ON o.server_id = m.id
             WHERE m.shared = 1 AND m.sync_rev > ?1
             ORDER BY m.sync_rev ASC, m.id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                let deleted: bool = r.get(15)?;
                let oauth_gen: Option<String> = r.get(17)?;
                let credentials: Option<String> = r.get(19)?;
                let rejected: Option<bool> = r.get(20)?;
                let oauth = if !deleted
                    && let Some(generation) = oauth_gen
                    && (credentials.is_some() || rejected == Some(true))
                {
                    Some(McpOAuthGrantWire {
                        generation,
                        binding: r.get(18)?,
                        credentials,
                        rejected: rejected.unwrap_or(false),
                    })
                } else {
                    None
                };
                Ok(McpServerWire {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    name_ts: r.get(2)?,
                    description: r.get(3)?,
                    description_ts: r.get(4)?,
                    enabled: r.get(5)?,
                    enabled_ts: r.get(6)?,
                    transport: r.get(7)?,
                    allowed_tools: r.get(8)?,
                    allowed_tools_ts: r.get(9)?,
                    config_json: r.get(10)?,
                    timeout_ms: r.get(11)?,
                    timeout_ts: r.get(12)?,
                    config_ts: r.get(13)?,
                    oauth,
                    oauth_ts: r.get(14)?,
                    deleted,
                    sync_rev: r.get(16)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if list.len() == DELTA_PAGE_LIMIT
            && let Some(last) = list.last()
        {
            safe_ceiling = safe_ceiling.min(last.sync_rev);
        }
        rows.extend(list.into_iter().map(CatalogRow::Mcp));
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, title, title_ts, created_at, updated_at,
                    CASE WHEN status IN ('running','pausing') THEN 'idle' ELSE status END,
                    CASE WHEN status IN ('running','pausing') THEN '' ELSE last_error END,
                    status_ts, model_id, reasoning_effort, selection_ts,
                    active_head, active_head_ts, deleted, sync_rev
             FROM sessions WHERE sync_rev > ?1
             ORDER BY sync_rev ASC, id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                Ok(SessionWire {
                    id: r.get(0)?,
                    title: r.get(1)?,
                    title_ts: r.get(2)?,
                    created_at: r.get(3)?,
                    updated_at: r.get(4)?,
                    status: r.get(5)?,
                    last_error: r.get(6)?,
                    status_ts: r.get(7)?,
                    model_id: r.get(8)?,
                    reasoning_effort: r.get(9)?,
                    selection_ts: r.get(10)?,
                    active_head: r.get(11)?,
                    active_head_ts: r.get(12)?,
                    deleted: r.get(13)?,
                    sync_rev: r.get(14)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if list.len() == DELTA_PAGE_LIMIT
            && let Some(last) = list.last()
        {
            safe_ceiling = safe_ceiling.min(last.sync_rev);
        }
        rows.extend(list.into_iter().map(CatalogRow::Session));
    }

    rows.retain(|r| r.sync_rev() <= safe_ceiling);
    rows.sort_by_key(CatalogRow::sync_rev);

    let mut delta = CatalogDelta {
        db_epoch,
        head_rev,
        next_rev: since_rev,
        has_more: safe_ceiling < i64::MAX,
        ..Default::default()
    };

    let mut total_bytes = 0usize;
    let total_rows = rows.len();
    for (idx, row) in rows.into_iter().enumerate() {
        let row_bytes = row.approx_bytes();
        if idx > 0 && total_bytes.saturating_add(row_bytes) > MAX_DELTA_BYTES {
            delta.has_more = true;
            break;
        }
        total_bytes += row_bytes;
        delta.next_rev = row.sync_rev();
        match row {
            CatalogRow::Provider(p) => delta.providers.push(p),
            CatalogRow::Model(m) => delta.models.push(m),
            CatalogRow::Mcp(s) => delta.mcp_servers.push(s),
            CatalogRow::Session(s) => delta.sessions.push(s),
        }
    }
    if delta.providers.is_empty()
        && delta.models.is_empty()
        && delta.mcp_servers.is_empty()
        && delta.sessions.is_empty()
    {
        delta.next_rev = head_rev;
    } else if !delta.has_more && total_rows > 0 {
        delta.next_rev = delta.next_rev.max(head_rev);
    }

    // Fill remaining byte budget with llm_calls past remote_calls_vector.
    let local_calls_vec = read_vector(conn, CALLS_SCOPE)?;
    let mut stmt = conn.prepare_cached(
        "SELECT id, session_id, model_id, reasoning_effort, input_end_position,
                started_at, finished_at, status, upstream_response_id, http_status,
                error_summary, raw_usage, input_uncached_tokens, cache_read_tokens,
                cache_write_tokens, output_tokens, pricing_snapshot, cost_nano_usd,
                origin_node_id, origin_seq
         FROM llm_calls
         WHERE origin_node_id = ?1 AND origin_seq > ?2
         ORDER BY origin_seq ASC, id ASC LIMIT ?3",
    )?;
    for (origin_node_id, local_max) in local_calls_vec {
        let remote_seq = remote_calls_vector.get(&origin_node_id).copied().unwrap_or(0);
        if local_max <= remote_seq {
            continue;
        }
        if total_bytes >= MAX_DELTA_BYTES {
            delta.has_more = true;
            break;
        }
        let batch = stmt
            .query_map(
                params![&origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                |r| {
                    Ok(LlmCallWire {
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
                        origin_seq: r.get(19)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let saturated = batch.len() == DELTA_PAGE_LIMIT;
        let mut last_seq = remote_seq;
        let mut broke_early = false;
        for call in batch {
            let approx = call.raw_usage.as_deref().map_or(0, str::len)
                + call.error_summary.as_deref().map_or(0, str::len)
                + call.pricing_snapshot.as_deref().map_or(0, str::len)
                + 256;
            if total_bytes > 0 && total_bytes.saturating_add(approx) > MAX_DELTA_BYTES {
                delta.has_more = true;
                broke_early = true;
                break;
            }
            total_bytes += approx;
            last_seq = last_seq.max(call.origin_seq);
            delta.calls.push(call);
        }
        if !broke_early && !saturated {
            last_seq = local_max;
        } else if saturated {
            delta.has_more = true;
        }
        if last_seq > remote_seq {
            delta.calls_vector.insert(origin_node_id, last_seq);
        }
    }

    Ok(delta)
}

enum SessionEventRow {
    Item(ItemWire),
    ContextNode(ContextNodeWire),
    TurnSettled(TurnSettledWire),
    InvalidatedWarning(InvalidatedWarningWire),
}

impl SessionEventRow {
    fn origin_seq(&self) -> i64 {
        match self {
            Self::Item(r) => r.origin_seq,
            Self::ContextNode(r) => r.origin_seq,
            Self::TurnSettled(r) => r.origin_seq,
            Self::InvalidatedWarning(r) => r.origin_seq,
        }
    }

    fn approx_bytes(&self) -> usize {
        match self {
            Self::Item(r) => r.payload.len() + 256,
            Self::ContextNode(_) => 192,
            Self::TurnSettled(_) => 128,
            Self::InvalidatedWarning(_) => 128,
        }
    }
}

fn collect_attachment_refs_from_item(payload: &str, out: &mut HashSet<AttachmentId>) {
    let Ok(val) = serde_json::from_str::<Value>(payload) else {
        return;
    };
    for field in ["content", "output"] {
        if let Some(arr) = val.get(field).and_then(Value::as_array) {
            for part in arr {
                if let Some(raw) = part.get("attachment_id").and_then(Value::as_str)
                    && let Ok(aid) = raw.parse::<AttachmentId>()
                {
                    out.insert(aid);
                }
            }
        }
    }
}

pub fn read_session_delta(
    conn: &Connection,
    sid: SessionId,
    remote_vector: &BTreeMap<String, i64>,
) -> Result<SessionDelta> {
    let deleted: bool = conn
        .prepare_cached("SELECT coalesce((SELECT deleted FROM sessions WHERE id=?), 0)")?
        .query_row([sid], |r| r.get(0))?;
    if deleted {
        return Ok(SessionDelta {
            session_id: sid,
            deleted: true,
            has_more: false,
            attachments: Vec::new(),
            items: Vec::new(),
            context_nodes: Vec::new(),
            turn_settled: Vec::new(),
            invalidated_warnings: Vec::new(),
            advanced_vector: BTreeMap::new(),
        });
    }

    let scope = session_scope(sid);
    let local_vec = read_vector(conn, &scope)?;
    let mut delta = SessionDelta {
        session_id: sid,
        deleted: false,
        has_more: false,
        attachments: Vec::new(),
        items: Vec::new(),
        context_nodes: Vec::new(),
        turn_settled: Vec::new(),
        invalidated_warnings: Vec::new(),
        advanced_vector: BTreeMap::new(),
    };
    let mut total_bytes = 0usize;
    let mut referenced_attachments = HashSet::new();

    for (origin_node_id, local_max) in local_vec {
        let remote_seq = remote_vector.get(&origin_node_id).copied().unwrap_or(0);
        if local_max <= remote_seq {
            continue;
        }
        if total_bytes >= MAX_DELTA_BYTES {
            delta.has_more = true;
            break;
        }

        let mut events: Vec<SessionEventRow> = Vec::new();
        let mut safe_ceiling = i64::MAX;

        {
            let mut stmt = conn.prepare_cached(
                "SELECT id, session_id, kind, payload, target_id, llm_call_id,
                        idempotency_key, codemode_call_item_id, origin_node_id, origin_seq
                 FROM items
                 WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
                 ORDER BY origin_seq ASC, id ASC LIMIT ?4",
            )?;
            let list = stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(ItemWire {
                            id: r.get(0)?,
                            session_id: r.get(1)?,
                            kind: r.get(2)?,
                            payload: r.get(3)?,
                            target_id: r.get(4)?,
                            llm_call_id: r.get(5)?,
                            idempotency_key: r.get(6)?,
                            codemode_call_item_id: r.get(7)?,
                            origin_node_id: r.get(8)?,
                            origin_seq: r.get(9)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if list.len() == DELTA_PAGE_LIMIT
                && let Some(last) = list.last()
            {
                safe_ceiling = safe_ceiling.min(last.origin_seq);
            }
            events.extend(list.into_iter().map(SessionEventRow::Item));
        }

        {
            let mut stmt = conn.prepare_cached(
                "SELECT id, session_id, parent_id, depth, item_id, origin_node_id, origin_seq
                 FROM context_nodes
                 WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
                 ORDER BY origin_seq ASC, id ASC LIMIT ?4",
            )?;
            let list = stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(ContextNodeWire {
                            id: r.get(0)?,
                            session_id: r.get(1)?,
                            parent_id: r.get(2)?,
                            depth: r.get(3)?,
                            item_id: r.get(4)?,
                            origin_node_id: r.get(5)?,
                            origin_seq: r.get(6)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if list.len() == DELTA_PAGE_LIMIT
                && let Some(last) = list.last()
            {
                safe_ceiling = safe_ceiling.min(last.origin_seq);
            }
            events.extend(list.into_iter().map(SessionEventRow::ContextNode));
        }

        {
            let mut stmt = conn.prepare_cached(
                "SELECT context_node_id, session_id, origin_node_id, origin_seq
                 FROM turn_settled
                 WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
                 ORDER BY origin_seq ASC, context_node_id ASC LIMIT ?4",
            )?;
            let list = stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(TurnSettledWire {
                            context_node_id: r.get(0)?,
                            session_id: r.get(1)?,
                            origin_node_id: r.get(2)?,
                            origin_seq: r.get(3)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if list.len() == DELTA_PAGE_LIMIT
                && let Some(last) = list.last()
            {
                safe_ceiling = safe_ceiling.min(last.origin_seq);
            }
            events.extend(list.into_iter().map(SessionEventRow::TurnSettled));
        }

        {
            let mut stmt = conn.prepare_cached(
                "SELECT item_id, session_id, origin_node_id, origin_seq
                 FROM invalidated_warnings
                 WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
                 ORDER BY origin_seq ASC, item_id ASC LIMIT ?4",
            )?;
            let list = stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(InvalidatedWarningWire {
                            item_id: r.get(0)?,
                            session_id: r.get(1)?,
                            origin_node_id: r.get(2)?,
                            origin_seq: r.get(3)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if list.len() == DELTA_PAGE_LIMIT
                && let Some(last) = list.last()
            {
                safe_ceiling = safe_ceiling.min(last.origin_seq);
            }
            events.extend(list.into_iter().map(SessionEventRow::InvalidatedWarning));
        }

        events.retain(|e| e.origin_seq() <= safe_ceiling);
        events.sort_by_key(SessionEventRow::origin_seq);

        let mut last_seq = remote_seq;
        let mut broke_early = false;
        for ev in events {
            let approx = ev.approx_bytes();
            if total_bytes > 0 && total_bytes.saturating_add(approx) > MAX_DELTA_BYTES {
                delta.has_more = true;
                broke_early = true;
                break;
            }
            total_bytes += approx;
            last_seq = last_seq.max(ev.origin_seq());
            match ev {
                SessionEventRow::Item(it) => {
                    collect_attachment_refs_from_item(&it.payload, &mut referenced_attachments);
                    delta.items.push(it);
                }
                SessionEventRow::ContextNode(node) => delta.context_nodes.push(node),
                SessionEventRow::TurnSettled(ts) => delta.turn_settled.push(ts),
                SessionEventRow::InvalidatedWarning(iw) => delta.invalidated_warnings.push(iw),
            }
        }
        if !broke_early && safe_ceiling == i64::MAX {
            last_seq = local_max;
        } else if safe_ceiling < i64::MAX {
            delta.has_more = true;
        }
        if last_seq > remote_seq {
            delta.advanced_vector.insert(origin_node_id, last_seq);
        }
    }

    if !referenced_attachments.is_empty() {
        let mut att_stmt = conn.prepare_cached(
            "SELECT id, mime_type, content_hash, byte_len FROM attachments WHERE id=?",
        )?;
        for aid in referenced_attachments {
            if let Some(att) = att_stmt
                .query_row([aid], |r| {
                    let blob: Vec<u8> = r.get(2)?;
                    let content_hash: [u8; 32] = blob.try_into().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            2,
                            "content_hash".into(),
                            rusqlite::types::Type::Blob,
                        )
                    })?;
                    Ok(AttachmentRef {
                        id: r.get(0)?,
                        mime_type: r.get(1)?,
                        content_hash,
                        byte_len: r.get(3)?,
                    })
                })
                .optional()?
            {
                delta.attachments.push(att);
            }
        }
    }

    Ok(delta)
}
