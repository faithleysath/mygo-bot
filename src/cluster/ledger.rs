use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashSet},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Error, Result,
    cluster::identity::local_node_id,
    id::{
        self, AttachmentId, CallId, ContextNodeId, ItemId, McpServerId, ModelId, ProviderId,
        SessionId,
    },
};

pub const CALLS_SCOPE: &str = "calls";
pub const MAX_INLINE_ITEM_PAYLOAD_BYTES: usize = 1_000_000;
pub const MAX_DELTA_BYTES: usize = 1_500_000;
const DELTA_PAGE_LIMIT: usize = 256;
// Strict upper bound for HLC physical wall milliseconds (~36M years), preventing u64 saturation.
pub const MAX_HLC_WALL_MS: u64 = 1u64 << 60;

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

fn valid_hlc_node_part(node: &str) -> bool {
    !node.is_empty() && node.len() <= 128 && node.bytes().all(|b| b.is_ascii_graphic())
}

#[inline]
fn hlc_in_bounds(wall_ms: u64, logical: u32) -> bool {
    wall_ms < MAX_HLC_WALL_MS && !(wall_ms == MAX_HLC_WALL_MS - 1 && logical == u32::MAX)
}

fn parse_hlc(ts: &str) -> Option<(u64, u32)> {
    let b = ts.as_bytes();
    if b.len() < 27 || b.len() > 154 || b[16] != b':' || b[25] != b':' {
        return None;
    }
    let valid_hex = |slice: &[u8]| slice.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    if !valid_hex(&b[..16]) || !valid_hex(&b[17..25]) {
        return None;
    }
    if !valid_hlc_node_part(&ts[26..]) {
        return None;
    }
    let wall_ms = u64::from_str_radix(&ts[..16], 16).ok()?;
    let logical = u32::from_str_radix(&ts[17..25], 16).ok()?;
    // Reject wall_ms at/above MAX_HLC_WALL_MS and the final slot before MAX_HLC_WALL_MS so a
    // strictly monotonic successor is always representable.
    if !hlc_in_bounds(wall_ms, logical) {
        return None;
    }
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

pub fn tick_hlc_for(node_id: &str) -> Result<String> {
    if !valid_hlc_node_part(node_id) {
        return Err(Error::bad("invalid local node_id for HLC"));
    }
    let raw_now = u64::try_from(chrono::Utc::now().timestamp_millis().max(0)).unwrap_or(0);
    if raw_now >= MAX_HLC_WALL_MS {
        return Err(Error::internal("system clock exceeds HLC upper bound"));
    }
    let (wall_ms, logical) = {
        let mut st = HLC.lock().unwrap();
        let (next_wall, next_logical) = if raw_now > st.wall_ms {
            (raw_now, 0)
        } else if st.logical < u32::MAX {
            (st.wall_ms, st.logical + 1)
        } else {
            let next_wall = st
                .wall_ms
                .checked_add(1)
                .ok_or_else(|| Error::internal("HLC timestamp space exhausted"))?;
            (next_wall, 0)
        };
        if !hlc_in_bounds(next_wall, next_logical) {
            return Err(Error::internal("HLC timestamp space exhausted"));
        }
        st.wall_ms = next_wall;
        st.logical = next_logical;
        (next_wall, next_logical)
    };
    Ok(format!("{wall_ms:016x}:{logical:08x}:{node_id}"))
}

pub fn tick_hlc(conn: &Connection) -> Result<String> {
    let node_id = local_node_id(conn)?;
    tick_hlc_for(&node_id)
}

pub fn seed_hlc_from_db(conn: &Connection) -> Result<()> {
    {
        let (a, b, c): (Option<String>, Option<String>, Option<String>) = conn.query_row(
            "SELECT max(name_ts), max(endpoint_ts), max(archived_ts) FROM llm_providers",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        for ts in [a, b, c].into_iter().flatten() {
            observe_hlc(&ts);
        }
    }
    {
        let (a, b, c, d, e, f): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn.query_row(
            "SELECT max(identity_ts), max(context_window_ts), max(input_modalities_ts),
                    max(reasoning_efforts_ts), max(pricing_ts), max(archived_ts)
             FROM llm_models",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )?;
        for ts in [a, b, c, d, e, f].into_iter().flatten() {
            observe_hlc(&ts);
        }
    }
    {
        let row: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn.query_row(
            "SELECT max(name_ts), max(description_ts), max(enabled_ts), max(allowed_tools_ts),
                    max(timeout_ts), max(headers_epoch_ts), max(headers_ts),
                    max(auth_config_ts), max(oauth_epoch_ts), max(oauth_ts)
             FROM mcp_servers",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                ))
            },
        )?;
        for ts in [
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9,
        ]
        .into_iter()
        .flatten()
        {
            observe_hlc(&ts);
        }
    }
    {
        let (a, b, c, d): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn.query_row(
            "SELECT max(title_ts), max(status_ts), max(selection_ts), max(active_head_ts) FROM sessions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        for ts in [a, b, c, d].into_iter().flatten() {
            observe_hlc(&ts);
        }
    }
    Ok(())
}

pub fn current_catalog_rev(conn: &Connection) -> Result<i64> {
    Ok(conn
        .prepare_cached("SELECT catalog_rev FROM local_node WHERE id=1")?
        .query_row([], |r| r.get(0))?)
}

pub fn next_catalog_rev(conn: &Connection) -> Result<i64> {
    Ok(conn
        .prepare_cached("UPDATE local_node SET catalog_rev = catalog_rev + 1 WHERE id=1 RETURNING catalog_rev")?
        .query_row([], |r| r.get(0))?)
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
    pub evict_pool: bool,
}

pub(crate) fn auth_config_fingerprint(transport: &str, config_json: &str) -> String {
    let val = serde_json::from_str::<Value>(config_json).unwrap_or(Value::Null);
    let url = val.get("url").and_then(Value::as_str).unwrap_or("");
    let oauth = val.get("oauth").cloned().unwrap_or(Value::Null);
    let canonical = json!({
        "transport": transport,
        "url": url,
        "oauth": oauth,
    });
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

pub(crate) fn split_mcp_config(
    transport: &str,
    config_json: &str,
) -> (serde_json::Map<String, Value>, serde_json::Map<String, Value>) {
    let mut core = serde_json::from_str::<Value>(config_json)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    if transport == "stdio" {
        return (core, serde_json::Map::new());
    }
    let headers = core
        .remove("headers")
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    (core, headers)
}

pub(crate) fn compose_mcp_config(
    transport: &str,
    mut core: serde_json::Map<String, Value>,
    headers: serde_json::Map<String, Value>,
) -> String {
    if transport != "stdio" {
        core.insert("headers".into(), Value::Object(headers));
    }
    serde_json::to_string(&Value::Object(core)).unwrap_or_else(|_| "{}".into())
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
    pub api_key: Option<String>,
    pub endpoint_ts: String,
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
    pub auth_config_ts: String,
    pub auth_fingerprint: String,
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
    pub headers_epoch_ts: String,
    pub headers_ts: String,
    pub auth_config_ts: String,
    pub oauth_epoch_ts: String,
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
    #[serde(default)]
    pub input_head: Option<ContextNodeId>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_hash: Option<[u8; 32]>,
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
    pub out_of_order_view: bool,
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
    let Some(head_id) = active_head else {
        return Ok(true);
    };
    let Some(target_depth) = conn
        .prepare_cached("SELECT depth FROM context_nodes WHERE id=?1 AND session_id=?2")?
        .query_row(params![head_id, sid], |r| r.get::<_, i64>(0))
        .optional()?
    else {
        return Ok(false);
    };
    // Fast path: active_context is populated only after verifying a complete root-to-tip chain.
    let tip: Option<(i64, ContextNodeId)> = conn
        .prepare_cached(
            "SELECT position, node_id FROM active_context WHERE session_id=?1 ORDER BY position DESC LIMIT 1",
        )?
        .query_row([sid], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    if let Some((pos, tip_node)) = tip
        && pos == target_depth
        && tip_node == head_id
        && conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM active_context WHERE session_id=?1 AND position=1)")?
            .query_row([sid], |r| r.get::<_, bool>(0))?
    {
        return Ok(true);
    }
    node_branch_complete(conn, sid, head_id)
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

pub fn reconcile_idempotency_key(conn: &Connection, sid: SessionId, key: &str) -> Result<()> {
    let settled_or_withdrawn: bool = conn
        .prepare_cached(
            "SELECT EXISTS(
                SELECT 1 FROM items i
                WHERE i.session_id=?1 AND i.idempotency_key=?2 AND i.kind='user_message'
                  AND (EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=?1 AND c.item_id=i.id)
                       OR EXISTS(SELECT 1 FROM items w WHERE w.session_id=?1 AND w.kind='withdrawal' AND w.target_id=i.id))
            )",
        )?
        .query_row(params![sid, key], |r| r.get(0))?;
    conn.prepare_cached(
        "DELETE FROM pending_inputs
         WHERE session_id=?1
           AND item_id IN (SELECT id FROM items WHERE session_id=?1 AND idempotency_key=?2 AND kind='user_message')",
    )?
    .execute(params![sid, key])?;
    if !settled_or_withdrawn {
        conn.prepare_cached(
            "INSERT INTO pending_inputs(session_id, item_id, kind)
             SELECT ?1, id, 'user_message' FROM items
             WHERE session_id=?1 AND idempotency_key=?2 AND kind='user_message'
             ORDER BY id ASC LIMIT 1
             ON CONFLICT(session_id, item_id) DO NOTHING",
        )?
        .execute(params![sid, key])?;
    }
    Ok(())
}

pub fn on_item_inserted(
    conn: &Connection,
    sid: SessionId,
    id: ItemId,
    kind: &str,
    payload: &Value,
    target_id: Option<ItemId>,
) -> Result<()> {
    let mut att_refs = HashSet::new();
    collect_attachment_ids_from_value(payload, &mut att_refs);
    if !att_refs.is_empty() {
        let mut stmt = conn.prepare_cached(
            "INSERT INTO item_attachments(session_id, item_id, attachment_id)
             VALUES(?1, ?2, ?3)
             ON CONFLICT(attachment_id, session_id, item_id) DO NOTHING",
        )?;
        for aid in att_refs {
            stmt.execute(params![sid, id, aid])?;
        }
    }
    match kind {
        "user_message" => {
            let key: Option<String> = conn
                .prepare_cached("SELECT idempotency_key FROM items WHERE id=?1 AND session_id=?2")?
                .query_row(params![id, sid], |r| r.get(0))
                .optional()?
                .flatten();
            if let Some(key) = key {
                reconcile_idempotency_key(conn, sid, &key)?;
            } else {
                conn.prepare_cached(
                    "INSERT INTO pending_inputs(session_id, item_id, kind)
                     SELECT ?1, ?2, 'user_message'
                     WHERE NOT EXISTS(SELECT 1 FROM items w WHERE w.session_id=?1 AND w.kind='withdrawal' AND w.target_id=?2)
                       AND NOT EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=?1 AND c.item_id=?2)
                     ON CONFLICT(session_id, item_id) DO NOTHING",
                )?
                .execute(params![sid, id])?;
            }
        }
        "codemode_event" => {
            if payload.get("notify").and_then(Value::as_bool) == Some(true) {
                conn.prepare_cached(
                    "INSERT INTO pending_inputs(session_id, item_id, kind)
                     SELECT ?1, ?2, 'codemode_event'
                     WHERE NOT EXISTS(SELECT 1 FROM invalidated_warnings iw WHERE iw.item_id=?2)
                       AND NOT EXISTS(SELECT 1 FROM context_nodes c WHERE c.session_id=?1 AND c.item_id=?2)
                     ON CONFLICT(session_id, item_id) DO NOTHING",
                )?
                .execute(params![sid, id])?;
            }
        }
        "withdrawal" => {
            if let Some(target) = target_id {
                let key: Option<String> = conn
                    .prepare_cached("SELECT idempotency_key FROM items WHERE id=?1 AND session_id=?2")?
                    .query_row(params![target, sid], |r| r.get(0))
                    .optional()?
                    .flatten();
                if let Some(key) = key {
                    reconcile_idempotency_key(conn, sid, &key)?;
                } else {
                    conn.prepare_cached(
                        "DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2",
                    )?
                    .execute(params![sid, target])?;
                }
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
    let key: Option<String> = conn
        .prepare_cached("SELECT idempotency_key FROM items WHERE id=?1 AND session_id=?2")?
        .query_row(params![item_id, sid], |r| r.get(0))
        .optional()?
        .flatten();
    if let Some(key) = key {
        reconcile_idempotency_key(conn, sid, &key)?;
    } else {
        conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?
            .execute(params![sid, item_id])?;
    }
    if let Some(pid) = parent_id {
        conn.prepare_cached("DELETE FROM session_leaves WHERE session_id=?1 AND head_id=?2")?
            .execute(params![sid, pid])?;
    }
    conn.prepare_cached(
        "INSERT INTO session_leaves(session_id, head_id, depth, last_item_id, origin_node_id)
         SELECT ?1, ?2, ?3, ?4, ?5
         WHERE NOT EXISTS(SELECT 1 FROM context_nodes WHERE session_id=?1 AND parent_id=?2)
         ON CONFLICT(session_id, head_id) DO NOTHING",
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
    append_context_items_at(conn, sid, item_ids, None, touch_updated_at)
}

pub fn append_context_items_at(
    conn: &Connection,
    sid: SessionId,
    item_ids: &[ItemId],
    expected_head: Option<Option<ContextNodeId>>,
    touch_updated_at: Option<i64>,
) -> Result<Vec<ContextNodeId>> {
    let node_id = local_node_id(conn)?.to_string();
    let scope = session_scope(sid);
    let mut parent_id: Option<ContextNodeId> = conn
        .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
        .query_row([sid], |r| r.get(0))
        .optional()?
        .flatten();
    if let Some(expected) = expected_head
        && parent_id != expected
    {
        return Err(Error::conflict("session branch changed during execution"));
    }
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
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
    let ts = tick_hlc_for(&node_id)?;
    let rev = next_catalog_rev(conn)?;
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

pub fn mark_turn_settled_node(
    conn: &Connection,
    sid: SessionId,
    expected_head: ContextNodeId,
) -> Result<bool> {
    let active_head: Option<ContextNodeId> = conn
        .prepare_cached("SELECT active_head FROM sessions WHERE id=? AND deleted=0")?
        .query_row([sid], |r| r.get(0))
        .optional()?
        .flatten();
    if active_head != Some(expected_head) {
        return Err(Error::conflict("session branch changed before settling turn"));
    }
    let already: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM turn_settled WHERE context_node_id=?)")?
        .query_row([expected_head], |r| r.get(0))?;
    if already {
        return Ok(false);
    }
    let node_id = local_node_id(conn)?;
    let origin_seq = next_origin_seq(conn, &session_scope(sid), &node_id)?;
    let changed = conn
        .prepare_cached(
            "INSERT INTO turn_settled(context_node_id, session_id, origin_node_id, origin_seq)
             VALUES(?1, ?2, ?3, ?4)
             ON CONFLICT(context_node_id, origin_node_id) DO NOTHING",
        )?
        .execute(params![expected_head, sid, &*node_id, origin_seq])?;
    if changed > 0 {
        stage_session(sid);
    }
    Ok(changed > 0)
}

pub fn tombstone_session(conn: &Connection, sid: SessionId, _now: i64) -> Result<bool> {
    let exists: Option<bool> = conn
        .prepare_cached("SELECT deleted FROM sessions WHERE id=?")?
        .query_row([sid], |r| r.get(0))
        .optional()?;
    let Some(already_deleted) = exists else {
        return Ok(false);
    };
    let mut bump = false;
    if !already_deleted {
        let rev = next_catalog_rev(conn)?;
        conn.prepare_cached(
            "UPDATE sessions
             SET deleted=1, title='', title_ts='', created_at=0, updated_at=0,
                 status='idle', last_error='', status_ts='',
                 local_status=NULL, local_error='',
                 model_id='', reasoning_effort=NULL, selection_ts='',
                 active_head=NULL, active_head_ts='', sync_rev=?2
             WHERE id=?1",
        )?
        .execute(params![sid, rev])?;
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
    conn.prepare_cached("DELETE FROM item_attachments WHERE session_id=?")?
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
    let rev = next_catalog_rev(conn)?;
    conn.prepare_cached(
        "UPDATE mcp_servers
         SET deleted=1, name='', name_ts='',
             description='', description_ts='',
             enabled=0, enabled_ts='',
             transport='streamable-http', config_json='{}', revision=revision+1,
             headers_epoch_ts='', headers_ts='', auth_config_ts='', oauth_epoch_ts='',
             allowed_tools=NULL, allowed_tools_ts='',
             timeout_ms=1, timeout_ts='',
             oauth_ts='', sync_rev=?2
         WHERE id=?1",
    )?
    .execute(params![id, rev])?;
    conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
        .execute([id])?;
    stage_catalog();
    Ok(true)
}

const MCP_WIRE_COLUMNS: &str = "m.id, m.name, m.name_ts, m.description, m.description_ts,
                m.enabled, m.enabled_ts, m.transport, m.allowed_tools, m.allowed_tools_ts,
                m.config_json, m.timeout_ms, m.timeout_ts, m.headers_epoch_ts, m.headers_ts,
                m.auth_config_ts, m.oauth_epoch_ts, m.oauth_ts, m.deleted, m.sync_rev,
                o.generation, o.auth_config_ts, o.auth_fingerprint, o.binding, o.credentials, o.rejected";

fn mcp_wire_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<McpServerWire> {
    let deleted: bool = r.get(18)?;
    let oauth_gen: Option<String> = r.get(20)?;
    let credentials: Option<String> = r.get(24)?;
    let rejected: Option<bool> = r.get(25)?;
    let oauth = if !deleted
        && let Some(generation) = oauth_gen
        && (credentials.is_some() || rejected == Some(true))
    {
        Some(McpOAuthGrantWire {
            generation,
            auth_config_ts: r.get::<_, Option<String>>(21)?.unwrap_or_default(),
            auth_fingerprint: r.get::<_, Option<String>>(22)?.unwrap_or_default(),
            binding: r.get(23)?,
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
        headers_epoch_ts: r.get(13)?,
        headers_ts: r.get(14)?,
        auth_config_ts: r.get(15)?,
        oauth_epoch_ts: r.get(16)?,
        oauth,
        oauth_ts: r.get(17)?,
        deleted,
        sync_rev: r.get(19)?,
    })
}

pub fn load_mcp_wire(conn: &Connection, id: McpServerId) -> Result<Option<McpServerWire>> {
    let sql = format!(
        "SELECT {MCP_WIRE_COLUMNS} FROM mcp_servers m LEFT JOIN mcp_oauth o ON o.server_id = m.id WHERE m.id=?1 AND m.shared=1"
    );
    Ok(conn
        .prepare_cached(&sql)?
        .query_row([id], mcp_wire_from_row)
        .optional()?)
}

pub fn merge_provider_wire(
    conn: &Connection,
    w: &ProviderWire,
) -> Result<(bool, Option<ProviderId>)> {
    id::observe_id(w.id);
    for ts in [&w.name_ts, &w.endpoint_ts, &w.archived_ts] {
        validate_hlc(ts)?;
        observe_hlc(ts);
    }

    let local = conn
        .prepare_cached(
            "SELECT name, name_ts, base_url, api_key, endpoint_ts, archived_at, archived_ts
             FROM llm_providers WHERE id=?",
        )?
        .query_row([w.id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, String>(6)?,
            ))
        })
        .optional()?;

    let (name, name_ts, base_url, api_key, endpoint_ts, archived_at, archived_ts) = match local {
        None => (
            w.name.clone(),
            w.name_ts.clone(),
            w.base_url.clone(),
            w.api_key.clone(),
            w.endpoint_ts.clone(),
            w.archived_at,
            w.archived_ts.clone(),
        ),
        Some((
            mut name,
            mut name_ts,
            mut base_url,
            mut api_key,
            mut endpoint_ts,
            mut archived_at,
            mut archived_ts,
        )) => {
            let mut changed = false;
            changed |= lww_take(&mut name, &mut name_ts, w.name.clone(), &w.name_ts);
            changed |= lww_take_pair(
                &mut base_url,
                &mut api_key,
                &mut endpoint_ts,
                w.base_url.clone(),
                w.api_key.clone(),
                &w.endpoint_ts,
            );
            changed |= lww_take(
                &mut archived_at,
                &mut archived_ts,
                w.archived_at,
                &w.archived_ts,
            );
            if !changed {
                return Ok((false, None));
            }
            (name, name_ts, base_url, api_key, endpoint_ts, archived_at, archived_ts)
        }
    };

    let rev = next_catalog_rev(conn)?;
    conn.prepare_cached(
        "INSERT INTO llm_providers(id,name,name_ts,base_url,api_key,endpoint_ts,archived_at,archived_ts,sync_rev)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(id) DO UPDATE SET
           name=excluded.name, name_ts=excluded.name_ts,
           base_url=excluded.base_url, api_key=excluded.api_key, endpoint_ts=excluded.endpoint_ts,
           archived_at=excluded.archived_at, archived_ts=excluded.archived_ts,
           sync_rev=excluded.sync_rev",
    )?
    .execute(params![
        w.id, name, name_ts, base_url, api_key, endpoint_ts, archived_at, archived_ts, rev
    ])?;
    Ok((true, Some(w.id)))
}

pub fn merge_model_wire(conn: &Connection, w: &ModelWire) -> Result<(bool, Option<ModelId>)> {
    id::observe_id(w.id);
    id::observe_id(w.provider_id);
    for ts in [
        &w.identity_ts,
        &w.context_window_ts,
        &w.input_modalities_ts,
        &w.reasoning_efforts_ts,
        &w.pricing_ts,
        &w.archived_ts,
    ] {
        validate_hlc(ts)?;
        observe_hlc(ts);
    }

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

    let (
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
    ) = match local {
        None => (
            w.provider_id,
            w.model_key.clone(),
            w.identity_ts.clone(),
            w.context_window_tokens,
            w.context_window_ts.clone(),
            w.input_modalities.clone(),
            w.input_modalities_ts.clone(),
            w.reasoning_efforts.clone(),
            w.reasoning_efforts_ts.clone(),
            w.pricing.clone(),
            w.pricing_ts.clone(),
            w.archived_at,
            w.archived_ts.clone(),
        ),
        Some((
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
        )) => {
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
            (
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
            )
        }
    };

    let rev = next_catalog_rev(conn)?;
    conn.prepare_cached(
        "INSERT INTO llm_models(
            id, provider_id, model_key, identity_ts,
            context_window_tokens, context_window_ts,
            input_modalities, input_modalities_ts,
            reasoning_efforts, reasoning_efforts_ts,
            pricing, pricing_ts, archived_at, archived_ts, sync_rev
         ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(id) DO UPDATE SET
            provider_id=excluded.provider_id, model_key=excluded.model_key, identity_ts=excluded.identity_ts,
            context_window_tokens=excluded.context_window_tokens, context_window_ts=excluded.context_window_ts,
            input_modalities=excluded.input_modalities, input_modalities_ts=excluded.input_modalities_ts,
            reasoning_efforts=excluded.reasoning_efforts, reasoning_efforts_ts=excluded.reasoning_efforts_ts,
            pricing=excluded.pricing, pricing_ts=excluded.pricing_ts,
            archived_at=excluded.archived_at, archived_ts=excluded.archived_ts,
            sync_rev=excluded.sync_rev",
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum EpochLwwAction {
    KeepLocal,
    TakeIncoming,
    Reset,
}

fn resolve_epoch_lww(
    local_eligible: bool,
    incoming_eligible: bool,
    local_ts: &str,
    incoming_ts: &str,
    local_needs_reset: bool,
) -> EpochLwwAction {
    match (local_eligible, incoming_eligible) {
        (true, true) => {
            if incoming_ts > local_ts {
                EpochLwwAction::TakeIncoming
            } else {
                EpochLwwAction::KeepLocal
            }
        }
        (true, false) => EpochLwwAction::KeepLocal,
        (false, true) => EpochLwwAction::TakeIncoming,
        (false, false) => {
            if local_needs_reset {
                EpochLwwAction::Reset
            } else {
                EpochLwwAction::KeepLocal
            }
        }
    }
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
        &w.headers_epoch_ts,
        &w.headers_ts,
        &w.auth_config_ts,
        &w.oauth_epoch_ts,
        &w.oauth_ts,
    ] {
        validate_hlc(ts)?;
        observe_hlc(ts);
    }
    if let Some(ref g) = w.oauth {
        validate_hlc(&g.auth_config_ts)?;
        observe_hlc(&g.auth_config_ts);
    }

    with_savepoint(conn, "merge_mcp_server", || {
        struct LocalMcp {
            name: String,
            name_ts: String,
            description: String,
            description_ts: String,
            enabled: bool,
            enabled_ts: String,
            shared: bool,
            transport: String,
            allowed_tools: Option<String>,
            allowed_tools_ts: String,
            config_json: String,
            timeout_ms: i64,
            timeout_ts: String,
            headers_epoch_ts: String,
            headers_ts: String,
            auth_config_ts: String,
            oauth_epoch_ts: String,
            oauth_ts: String,
            deleted: bool,
            oauth: Option<McpOAuthGrantWire>,
        }

        let existing = conn
            .prepare_cached(
                "SELECT m.name, m.name_ts, m.description, m.description_ts,
                        m.enabled, m.enabled_ts, m.shared, m.transport,
                        m.allowed_tools, m.allowed_tools_ts, m.config_json,
                        m.timeout_ms, m.timeout_ts, m.headers_epoch_ts, m.headers_ts,
                        m.auth_config_ts, m.oauth_epoch_ts, m.oauth_ts, m.deleted,
                        o.generation, o.auth_config_ts, o.auth_fingerprint, o.binding, o.credentials, o.rejected
                 FROM mcp_servers m
                 LEFT JOIN mcp_oauth o ON o.server_id = m.id
                 WHERE m.id=?1",
            )?
            .query_row([w.id], |r| {
                let oauth_gen: Option<String> = r.get(19)?;
                let oauth = oauth_gen.map(|generation| McpOAuthGrantWire {
                    generation,
                    auth_config_ts: r.get::<_, Option<String>>(20).ok().flatten().unwrap_or_default(),
                    auth_fingerprint: r.get::<_, Option<String>>(21).ok().flatten().unwrap_or_default(),
                    binding: r.get(22).unwrap_or(None),
                    credentials: r.get(23).unwrap_or(None),
                    rejected: r.get::<_, Option<bool>>(24).unwrap_or(None).unwrap_or(false),
                });
                Ok(LocalMcp {
                    name: r.get(0)?,
                    name_ts: r.get(1)?,
                    description: r.get(2)?,
                    description_ts: r.get(3)?,
                    enabled: r.get(4)?,
                    enabled_ts: r.get(5)?,
                    shared: r.get(6)?,
                    transport: r.get(7)?,
                    allowed_tools: r.get(8)?,
                    allowed_tools_ts: r.get(9)?,
                    config_json: r.get(10)?,
                    timeout_ms: r.get(11)?,
                    timeout_ts: r.get(12)?,
                    headers_epoch_ts: r.get(13)?,
                    headers_ts: r.get(14)?,
                    auth_config_ts: r.get(15)?,
                    oauth_epoch_ts: r.get(16)?,
                    oauth_ts: r.get(17)?,
                    deleted: r.get(18)?,
                    oauth,
                })
            })
            .optional()?;

        if existing.as_ref().is_some_and(|r| r.deleted) {
            return Ok((false, None));
        }
        if existing.as_ref().is_some_and(|r| !r.shared) {
            return Ok((false, None));
        }
        if w.deleted {
            let rev = next_catalog_rev(conn)?;
            conn.prepare_cached(
                "INSERT INTO mcp_servers(
                    id, name, name_ts, description, description_ts, enabled, enabled_ts,
                    shared, transport, allowed_tools, allowed_tools_ts, config_json,
                    timeout_ms, timeout_ts, revision, headers_epoch_ts, headers_ts,
                    auth_config_ts, oauth_epoch_ts, oauth_ts, deleted, sync_rev
                 ) VALUES(?1,'','','','',0,'',1,'streamable-http',NULL,'','{}',1,'',1,'','','','','',1,?2)
                 ON CONFLICT(id) DO UPDATE SET
                    deleted=1, name='', name_ts='', description='', description_ts='',
                    enabled=0, enabled_ts='', transport='streamable-http', config_json='{}',
                    revision=mcp_servers.revision+1, headers_epoch_ts='', headers_ts='',
                    auth_config_ts='', oauth_epoch_ts='', allowed_tools=NULL,
                    allowed_tools_ts='', timeout_ms=1, timeout_ts='', oauth_ts='',
                    sync_rev=excluded.sync_rev",
            )?
            .execute(params![w.id, rev])?;
            conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
                .execute([w.id])?;
            return Ok((
                true,
                Some(McpChange {
                    id: w.id,
                    cancel_login: true,
                    evict_pool: true,
                }),
            ));
        }

        let is_new = existing.is_none();
        let (w_core, w_headers) = split_mcp_config(&w.transport, &w.config_json);
        let mut cur = existing.unwrap_or_else(|| LocalMcp {
            name: w.name.clone(),
            name_ts: w.name_ts.clone(),
            description: w.description.clone(),
            description_ts: w.description_ts.clone(),
            enabled: w.enabled,
            enabled_ts: w.enabled_ts.clone(),
            shared: true,
            transport: w.transport.clone(),
            allowed_tools: w.allowed_tools.clone(),
            allowed_tools_ts: w.allowed_tools_ts.clone(),
            config_json: String::new(),
            timeout_ms: w.timeout_ms,
            timeout_ts: w.timeout_ts.clone(),
            headers_epoch_ts: String::new(),
            headers_ts: String::new(),
            auth_config_ts: w.auth_config_ts.clone(),
            oauth_epoch_ts: String::new(),
            oauth_ts: String::new(),
            deleted: false,
            oauth: None,
        });
        let (
            core_map,
            mut headers_map,
            scalar_changed,
            enabled_changed,
            allowed_tools_changed,
            timeout_changed,
            auth_changed,
        ) = if is_new {
            (w_core, serde_json::Map::new(), true, false, false, false, false)
        } else {
            let name_changed = lww_take(&mut cur.name, &mut cur.name_ts, w.name.clone(), &w.name_ts);
            let desc_changed = lww_take(
                &mut cur.description,
                &mut cur.description_ts,
                w.description.clone(),
                &w.description_ts,
            );
            let enabled_changed =
                lww_take(&mut cur.enabled, &mut cur.enabled_ts, w.enabled, &w.enabled_ts);
            let allowed_tools_changed = lww_take(
                &mut cur.allowed_tools,
                &mut cur.allowed_tools_ts,
                w.allowed_tools.clone(),
                &w.allowed_tools_ts,
            );
            let timeout_changed =
                lww_take(&mut cur.timeout_ms, &mut cur.timeout_ts, w.timeout_ms, &w.timeout_ts);
            let (mut core_map, headers_map) = split_mcp_config(&cur.transport, &cur.config_json);
            let mut auth_changed = false;
            let mut auth_meta_changed = false;
            if w.auth_config_ts > cur.auth_config_ts {
                if cur.transport != w.transport || core_map != w_core {
                    auth_changed = true;
                }
                cur.transport = w.transport.clone();
                core_map = w_core;
                cur.auth_config_ts = w.auth_config_ts.clone();
                auth_meta_changed = true;
            }
            let scalar_changed = name_changed
                || desc_changed
                || enabled_changed
                || allowed_tools_changed
                || timeout_changed
                || auth_meta_changed;
            (
                core_map,
                headers_map,
                scalar_changed,
                enabled_changed,
                allowed_tools_changed,
                timeout_changed,
                auth_changed,
            )
        };

        let local_hdr_eligible =
            !is_new && cur.transport != "stdio" && cur.headers_epoch_ts == cur.auth_config_ts;
        let incoming_hdr_eligible = cur.transport != "stdio"
            && w.transport == cur.transport
            && w.headers_epoch_ts == cur.auth_config_ts;
        let hdr_needs_reset = is_new
            || !headers_map.is_empty()
            || cur.headers_epoch_ts != cur.auth_config_ts
            || cur.headers_ts != cur.auth_config_ts;
        let mut headers_changed = false;
        let mut headers_meta_changed = false;
        match resolve_epoch_lww(
            local_hdr_eligible,
            incoming_hdr_eligible,
            &cur.headers_ts,
            &w.headers_ts,
            hdr_needs_reset,
        ) {
            EpochLwwAction::KeepLocal => {}
            EpochLwwAction::TakeIncoming => {
                if headers_map != w_headers {
                    headers_changed = true;
                }
                headers_map = w_headers;
                cur.headers_epoch_ts = cur.auth_config_ts.clone();
                cur.headers_ts = w.headers_ts.clone();
                headers_meta_changed = true;
            }
            EpochLwwAction::Reset => {
                if !headers_map.is_empty() {
                    headers_changed = true;
                }
                headers_map.clear();
                cur.headers_epoch_ts = cur.auth_config_ts.clone();
                cur.headers_ts = cur.auth_config_ts.clone();
                headers_meta_changed = true;
            }
        }
        cur.config_json = compose_mcp_config(&cur.transport, core_map, headers_map);

        let winning_fp = auth_config_fingerprint(&cur.transport, &cur.config_json);
        let local_oauth_eligible = !is_new
            && cur.oauth_epoch_ts == cur.auth_config_ts
            && cur.oauth.as_ref().is_none_or(|g| {
                g.auth_config_ts == cur.auth_config_ts && g.auth_fingerprint == winning_fp
            });
        let incoming_oauth_eligible = w.oauth_epoch_ts == cur.auth_config_ts
            && w.oauth.as_ref().is_none_or(|g| {
                g.auth_config_ts == cur.auth_config_ts && g.auth_fingerprint == winning_fp
            });
        let oauth_needs_reset = is_new
            || cur.oauth.is_some()
            || cur.oauth_epoch_ts != cur.auth_config_ts
            || cur.oauth_ts != cur.auth_config_ts;

        let mut oauth_changed = false;
        let mut oauth_binding_changed = false;
        let next_grant: Option<Option<&McpOAuthGrantWire>> = match resolve_epoch_lww(
            local_oauth_eligible,
            incoming_oauth_eligible,
            &cur.oauth_ts,
            &w.oauth_ts,
            oauth_needs_reset,
        ) {
            EpochLwwAction::KeepLocal => None,
            EpochLwwAction::TakeIncoming => {
                cur.oauth_ts = w.oauth_ts.clone();
                cur.oauth_epoch_ts = cur.auth_config_ts.clone();
                oauth_changed = true;
                if local_oauth_eligible {
                    let same_binding = match (&cur.oauth, &w.oauth) {
                        (None, None) => true,
                        (Some(l), Some(r)) => {
                            l.generation == r.generation
                                && l.binding == r.binding
                                && l.auth_config_ts == r.auth_config_ts
                                && l.auth_fingerprint == r.auth_fingerprint
                        }
                        _ => false,
                    };
                    let same_cred = match (&cur.oauth, &w.oauth) {
                        (None, None) => true,
                        (Some(l), Some(r)) => {
                            same_binding && l.credentials == r.credentials && l.rejected == r.rejected
                        }
                        _ => false,
                    };
                    if same_cred {
                        // Persist newer oauth_ts so retries/peers converge cleanly.
                        None
                    } else {
                        oauth_binding_changed = !same_binding;
                        Some(w.oauth.as_ref())
                    }
                } else {
                    oauth_binding_changed = !is_new;
                    Some(w.oauth.as_ref())
                }
            }
            EpochLwwAction::Reset => {
                cur.oauth_ts = cur.auth_config_ts.clone();
                cur.oauth_epoch_ts = cur.auth_config_ts.clone();
                oauth_changed = true;
                oauth_binding_changed = !is_new && cur.oauth.is_some();
                if is_new { None } else { Some(None) }
            }
        };

        if !is_new && !scalar_changed && !headers_meta_changed && !oauth_changed {
            return Ok((false, None));
        }

        let evict_pool = is_new
            || enabled_changed
            || allowed_tools_changed
            || timeout_changed
            || auth_changed
            || headers_changed
            || oauth_binding_changed;
        let cancel_login = !is_new
            && ((enabled_changed && !cur.enabled) || auth_changed || oauth_binding_changed);

        let rev = next_catalog_rev(conn)?;
        let rev_bump = i64::from(evict_pool);
        conn.prepare_cached(
            "INSERT INTO mcp_servers(
                id, name, name_ts, description, description_ts, enabled, enabled_ts,
                shared, transport, allowed_tools, allowed_tools_ts, config_json,
                timeout_ms, timeout_ts, revision, headers_epoch_ts, headers_ts,
                auth_config_ts, oauth_epoch_ts, oauth_ts, deleted, sync_rev
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8,?9,?10,?11,?12,?13,1,?14,?15,?16,?17,?18,0,?20)
             ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, name_ts=excluded.name_ts,
                description=excluded.description, description_ts=excluded.description_ts,
                enabled=excluded.enabled, enabled_ts=excluded.enabled_ts,
                transport=excluded.transport,
                allowed_tools=excluded.allowed_tools, allowed_tools_ts=excluded.allowed_tools_ts,
                config_json=excluded.config_json,
                timeout_ms=excluded.timeout_ms, timeout_ts=excluded.timeout_ts,
                revision=mcp_servers.revision+?19,
                headers_epoch_ts=excluded.headers_epoch_ts, headers_ts=excluded.headers_ts,
                auth_config_ts=excluded.auth_config_ts,
                oauth_epoch_ts=excluded.oauth_epoch_ts, oauth_ts=excluded.oauth_ts,
                sync_rev=excluded.sync_rev",
        )?
        .execute(params![
            w.id,
            cur.name,
            cur.name_ts,
            cur.description,
            cur.description_ts,
            cur.enabled,
            cur.enabled_ts,
            cur.transport,
            cur.allowed_tools,
            cur.allowed_tools_ts,
            cur.config_json,
            cur.timeout_ms,
            cur.timeout_ts,
            cur.headers_epoch_ts,
            cur.headers_ts,
            cur.auth_config_ts,
            cur.oauth_epoch_ts,
            cur.oauth_ts,
            rev_bump,
            rev
        ])?;

        if let Some(grant_opt) = next_grant {
            match grant_opt {
                Some(g) => {
                    conn.prepare_cached(
                        "INSERT INTO mcp_oauth(server_id, generation, auth_config_ts, auth_fingerprint, binding, pending, credentials, pending_refresh, rejected)
                         VALUES(?1, ?2, ?3, ?4, ?5, NULL, ?6, NULL, ?7)
                         ON CONFLICT(server_id) DO UPDATE SET
                           generation=excluded.generation,
                           auth_config_ts=excluded.auth_config_ts,
                           auth_fingerprint=excluded.auth_fingerprint,
                           binding=excluded.binding,
                           pending=NULL,
                           credentials=excluded.credentials,
                           pending_refresh=NULL,
                           rejected=excluded.rejected",
                    )?
                    .execute(params![
                        w.id,
                        &g.generation,
                        &g.auth_config_ts,
                        &g.auth_fingerprint,
                        &g.binding,
                        &g.credentials,
                        g.rejected
                    ])?;
                }
                None => {
                    conn.prepare_cached("DELETE FROM mcp_oauth WHERE server_id=?")?
                        .execute([w.id])?;
                }
            }
        }

        let change = (cancel_login || evict_pool).then_some(McpChange {
            id: w.id,
            cancel_login,
            evict_pool,
        });
        Ok((true, change))
    })
}

pub enum SessionMergeResult {
    Unchanged,
    Updated {
        deleted: bool,
        branch_switched: bool,
        context_advanced: bool,
    },
}

fn reconcile_context_flags(conn: &Connection, sid: SessionId) -> Result<(bool, bool)> {
    match reconcile_active_context(conn, sid)? {
        ActiveContextChange::Switched | ActiveContextChange::Cleared => Ok((true, false)),
        ActiveContextChange::Appended(_) => Ok((false, true)),
        ActiveContextChange::Unchanged | ActiveContextChange::Incomplete => Ok((false, false)),
    }
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

    if local.as_ref().is_some_and(|r| r.12) {
        return Ok(SessionMergeResult::Unchanged);
    }
    if w.deleted {
        let rev = next_catalog_rev(conn)?;
        conn.prepare_cached(
            "INSERT INTO sessions(
                id, title, title_ts, created_at, updated_at,
                status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                active_head, active_head_ts, deleted, sync_rev
             ) VALUES(?1,'','',0,0,'idle','','','',NULL,'',NULL,'',1,?2)
             ON CONFLICT(id) DO UPDATE SET
                deleted=1, title='', title_ts='', created_at=0, updated_at=0,
                status='idle', last_error='', status_ts='',
                local_status=NULL, local_error='',
                model_id='', reasoning_effort=NULL, selection_ts='',
                active_head=NULL, active_head_ts='', sync_rev=excluded.sync_rev",
        )?
        .execute(params![w.id, rev])?;
        cleanup_deleted_session_data(conn, w.id)?;
        return Ok(SessionMergeResult::Updated {
            deleted: true,
            branch_switched: false,
            context_advanced: false,
        });
    }

    let wire_status = if w.status == "paused" { "paused" } else { "idle" };
    let bounded_wire_err = crate::calls::truncate_utf8(
        w.last_error.as_str(),
        crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES,
    );
    let wire_err = if wire_status == "paused" {
        bounded_wire_err.as_str()
    } else {
        ""
    };

    let (
        title,
        title_ts,
        created_at,
        updated_at,
        status,
        last_error,
        status_ts,
        model_id,
        reasoning_effort,
        selection_ts,
        active_head,
        active_head_ts,
        check_context,
    ) = match local {
        None => (
            w.title.clone(),
            w.title_ts.clone(),
            w.created_at,
            w.updated_at,
            wire_status.to_owned(),
            wire_err.to_owned(),
            w.status_ts.clone(),
            w.model_id.clone(),
            w.reasoning_effort.clone(),
            w.selection_ts.clone(),
            w.active_head,
            w.active_head_ts.clone(),
            w.active_head.is_some(),
        ),
        Some((
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
            _,
        )) => {
            let mut changed = lww_take(&mut title, &mut title_ts, w.title.clone(), &w.title_ts);
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
            changed |= lww_take_pair(
                &mut status,
                &mut last_error,
                &mut status_ts,
                wire_status.to_owned(),
                wire_err.to_owned(),
                &w.status_ts,
            );
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
            if !(changed || head_changed) {
                return Ok(SessionMergeResult::Unchanged);
            }
            (
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
                head_changed,
            )
        }
    };

    let rev = next_catalog_rev(conn)?;
    conn.prepare_cached(
        "INSERT INTO sessions(
            id, title, title_ts, created_at, updated_at,
            status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
            active_head, active_head_ts, deleted, sync_rev
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,0,?14)
         ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, title_ts=excluded.title_ts,
            created_at=excluded.created_at, updated_at=excluded.updated_at,
            status=excluded.status, last_error=excluded.last_error, status_ts=excluded.status_ts,
            model_id=excluded.model_id, reasoning_effort=excluded.reasoning_effort,
            selection_ts=excluded.selection_ts,
            active_head=excluded.active_head, active_head_ts=excluded.active_head_ts,
            sync_rev=excluded.sync_rev",
    )?
    .execute(params![
        w.id,
        title,
        title_ts,
        created_at,
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

    let (branch_switched, context_advanced) = if check_context {
        reconcile_context_flags(conn, w.id)?
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
    if let Some(head) = w.input_head {
        id::observe_id(head);
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

    let bounded_err = w.error_summary.as_deref().map(|s| {
        crate::calls::truncate_utf8(s, crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES)
    });
    let bounded_usage = crate::calls::clamp_raw_usage_str(w.raw_usage.clone());
    let bounded_resp_id = crate::calls::clamp_response_id(w.upstream_response_id.clone());

    let mut changed = false;
    match existing {
        None => {
            conn.prepare_cached(
                "INSERT INTO llm_calls(
                    id, session_id, model_id, reasoning_effort, input_end_position, input_head,
                    started_at, finished_at, status, upstream_response_id, http_status,
                    error_summary, raw_usage, input_uncached_tokens, cache_read_tokens,
                    cache_write_tokens, output_tokens, pricing_snapshot, cost_nano_usd,
                    origin_node_id, origin_seq
                 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            )?
            .execute(params![
                w.id,
                effective_sid,
                w.model_id,
                &w.reasoning_effort,
                w.input_end_position,
                w.input_head,
                w.started_at,
                w.finished_at,
                &w.status,
                &bounded_resp_id,
                w.http_status,
                &bounded_err,
                &bounded_usage,
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
                        &bounded_resp_id,
                        w.http_status,
                        &bounded_err,
                        &bounded_usage,
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
    let now = chrono::Utc::now().timestamp_millis();
    for r in refs {
        id::observe_id(r.id);
        if r.byte_len <= 0 || r.byte_len > crate::media::MAX_ATTACHMENT_BYTES as i64 {
            return Err(Error::bad("invalid attachment metadata byte_len"));
        }
        let inserted = conn
            .prepare_cached(
                "INSERT INTO attachments(id, mime_type, content_hash, byte_len, created_at, data)
                 VALUES(?1, ?2, ?3, ?4, ?5, NULL)
                 ON CONFLICT(id) DO NOTHING",
            )?
            .execute(params![r.id, &r.mime_type, r.content_hash.as_slice(), r.byte_len, now])?;
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
            if exists {
                tombstone_session(conn, sid, 0)?;
            } else {
                let rev = next_catalog_rev(conn)?;
                conn.prepare_cached(
                    "INSERT INTO sessions(
                        id, title, title_ts, created_at, updated_at,
                        status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                        active_head, active_head_ts, deleted, sync_rev
                     ) VALUES(?1,'','',0,0,'idle','','','',NULL,'',NULL,'',1,?2)",
                )?
                .execute(params![sid, rev])?;
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
        let (prev_max_visible, prev_active_pos): (Option<ItemId>, i64) =
            if !delta.items.is_empty() || !delta.context_nodes.is_empty() {
                let max_vis: Option<ItemId> = conn
                    .prepare_cached(
                        "SELECT max(id) FROM items
                         WHERE session_id=?1
                           AND (EXISTS(SELECT 1 FROM active_context ac WHERE ac.session_id=?1 AND ac.item_id=items.id)
                                OR NOT EXISTS(SELECT 1 FROM context_nodes cn WHERE cn.session_id=?1 AND cn.item_id=items.id))",
                    )?
                    .query_row([sid], |r| r.get(0))?;
                let active_pos: i64 = conn
                    .prepare_cached(
                        "SELECT coalesce(max(position), -1) FROM active_context WHERE session_id=?1",
                    )?
                    .query_row([sid], |r| r.get(0))?;
                (max_vis, active_pos)
            } else {
                (None, -1)
            };

        for iw in &delta.invalidated_warnings {
            id::observe_id(iw.item_id);
            id::observe_id(iw.session_id);
            if iw.session_id != sid {
                return Err(Error::bad("invalidated_warning session mismatch"));
            }
            let ins = conn
                .prepare_cached(
                    "INSERT INTO invalidated_warnings(item_id, session_id, origin_node_id, origin_seq)
                     VALUES(?1, ?2, ?3, ?4)
                     ON CONFLICT(item_id, origin_node_id) DO NOTHING",
                )?
                .execute(params![iw.item_id, sid, &iw.origin_node_id, iw.origin_seq])?;
            if ins > 0 {
                conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?
                    .execute(params![sid, iw.item_id])?;
                out.changed = true;
                content_touched = true;
            }
        }

        let mut newly_inserted_items = HashSet::new();
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
            if (item.payload_len.is_some() || item.payload_hash.is_some()) && item.payload.is_empty() {
                return Err(Error::bad("unresolved oversized item payload"));
            }
            let payload_val: Value = serde_json::from_str(&item.payload)
                .map_err(|_| Error::bad("invalid item payload JSON"))?;
            let ins = conn
                .prepare_cached(
                    "INSERT INTO items(
                        id, session_id, kind, payload, target_id, llm_call_id,
                        idempotency_key, codemode_call_item_id, origin_node_id, origin_seq
                     ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                     ON CONFLICT(id) DO NOTHING",
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
                newly_inserted_items.insert(item.id);
                out.new_items.push(item.id);
                out.changed = true;
                content_touched = true;
            }
        }

        let mut newly_branched_preexisting = HashSet::new();
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
            if !newly_inserted_items.contains(&node.item_id) {
                let was_unbranched: bool = conn
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM items WHERE id=?1 AND session_id=?2)
                           AND NOT EXISTS(SELECT 1 FROM context_nodes WHERE session_id=?2 AND item_id=?1)",
                    )?
                    .query_row(params![node.item_id, sid], |r| r.get(0))?;
                if was_unbranched {
                    newly_branched_preexisting.insert(node.item_id);
                }
            }
            let ins = conn
                .prepare_cached(
                    "INSERT INTO context_nodes(
                        id, session_id, parent_id, depth, item_id, origin_node_id, origin_seq
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(id) DO NOTHING",
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
                    "INSERT INTO turn_settled(
                        context_node_id, session_id, origin_node_id, origin_seq
                     ) VALUES(?1, ?2, ?3, ?4)
                     ON CONFLICT(context_node_id, origin_node_id) DO NOTHING",
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

        for (origin_node_id, &claimed_seq) in &delta.advanced_vector {
            let cur_seq: i64 = conn
                .prepare_cached(
                    "SELECT coalesce((SELECT seq FROM sync_vectors WHERE scope=?1 AND node_id=?2), 0)",
                )?
                .query_row(params![&scope, origin_node_id], |r| r.get(0))?;
            if claimed_seq > cur_seq {
                let mut seq_stmt = conn.prepare_cached(
                    "SELECT origin_seq FROM items
                     WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3 AND origin_seq<=?4
                     UNION ALL
                     SELECT origin_seq FROM context_nodes
                     WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3 AND origin_seq<=?4
                     UNION ALL
                     SELECT origin_seq FROM turn_settled
                     WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3 AND origin_seq<=?4
                     UNION ALL
                     SELECT origin_seq FROM invalidated_warnings
                     WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3 AND origin_seq<=?4",
                )?;
                let present: HashSet<i64> = seq_stmt
                    .query_map(params![sid, origin_node_id, cur_seq, claimed_seq], |r| r.get(0))?
                    .collect::<rusqlite::Result<HashSet<_>>>()?;
                for seq in (cur_seq + 1)..=claimed_seq {
                    if !present.contains(&seq) {
                        return Err(Error::bad(format!(
                            "incomplete session delta vector for origin {origin_node_id}: missing origin_seq {seq}"
                        )));
                    }
                }
                advance_vector(conn, &scope, origin_node_id, claimed_seq)?;
            }
        }

        if content_touched {
            let change = reconcile_active_context(conn, sid)?;
            match change {
                ActiveContextChange::Switched | ActiveContextChange::Cleared => {
                    out.reset_view = true;
                }
                ActiveContextChange::Appended(_) => {
                    let mut appended_stmt = conn.prepare_cached(
                        "SELECT item_id FROM active_context
                         WHERE session_id=?1 AND position>?2
                         ORDER BY position ASC",
                    )?;
                    let appended_ids = appended_stmt
                        .query_map(params![sid, prev_active_pos], |r| r.get::<_, ItemId>(0))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    for item_id in appended_ids {
                        if !newly_inserted_items.contains(&item_id)
                            && !newly_branched_preexisting.contains(&item_id)
                        {
                            out.reset_view = true;
                            break;
                        }
                    }
                }
                ActiveContextChange::Unchanged | ActiveContextChange::Incomplete => {}
            }
        }

        if !out.reset_view && !newly_branched_preexisting.is_empty() {
            let mut in_active = conn.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM active_context WHERE session_id=?1 AND item_id=?2)",
            )?;
            for &item_id in &newly_branched_preexisting {
                if !in_active.query_row(params![sid, item_id], |r| r.get::<_, bool>(0))? {
                    out.reset_view = true;
                    break;
                }
            }
        }

        if !out.reset_view && !out.new_items.is_empty() {
            let mut vis_stmt = conn.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM active_context WHERE session_id=?1 AND item_id=?2)
                 OR NOT EXISTS(SELECT 1 FROM context_nodes WHERE session_id=?1 AND item_id=?2)",
            )?;
            let mut visible = Vec::with_capacity(out.new_items.len());
            for item_id in out.new_items {
                if vis_stmt.query_row(params![sid, item_id], |r| r.get::<_, bool>(0))? {
                    if prev_max_visible.is_some_and(|prev_max| item_id < prev_max) {
                        out.out_of_order_view = true;
                    }
                    visible.push(item_id);
                }
            }
            out.new_items = visible;
        } else if out.reset_view {
            out.new_items.clear();
        }
        Ok(out)
    })
}

enum CatalogKey {
    Provider(ProviderId),
    Model(ModelId),
    Mcp(McpServerId),
    Session(SessionId),
}

struct CatalogMeta {
    sync_rev: i64,
    est_bytes: usize,
    key: CatalogKey,
}

fn collect_bounded_metas<T>(
    rows: Vec<T>,
    seq_of: impl Fn(&T) -> i64,
    out: &mut Vec<T>,
    safe_ceiling: &mut i64,
) {
    if rows.len() == DELTA_PAGE_LIMIT
        && let Some(last) = rows.last()
    {
        *safe_ceiling = (*safe_ceiling).min(seq_of(last));
    }
    out.extend(rows);
}

pub fn read_catalog_delta(
    conn: &Connection,
    since_rev: i64,
    remote_calls_vector: &BTreeMap<String, i64>,
) -> Result<CatalogDelta> {
    let db_epoch: String =
        conn.query_row("SELECT db_epoch FROM local_node WHERE id=1", [], |r| r.get(0))?;
    let head_rev = current_catalog_rev(conn)?;
    let mut metas: Vec<CatalogMeta> = Vec::new();
    let mut safe_ceiling = i64::MAX;

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, sync_rev,
                    length(CAST(name AS BLOB)) + length(CAST(base_url AS BLOB))
                    + coalesce(length(CAST(api_key AS BLOB)), 0) + 192
             FROM llm_providers WHERE sync_rev > ?1
             ORDER BY sync_rev ASC, id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                let raw_bytes: i64 = r.get(2)?;
                Ok(CatalogMeta {
                    sync_rev: r.get(1)?,
                    est_bytes: usize::try_from(raw_bytes).unwrap_or(256).saturating_mul(2).saturating_add(192),
                    key: CatalogKey::Provider(r.get(0)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        collect_bounded_metas(list, |m| m.sync_rev, &mut metas, &mut safe_ceiling);
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, sync_rev,
                    length(CAST(model_key AS BLOB)) + length(CAST(input_modalities AS BLOB))
                    + length(CAST(reasoning_efforts AS BLOB))
                    + coalesce(length(CAST(pricing AS BLOB)), 0) + 256
             FROM llm_models WHERE sync_rev > ?1
             ORDER BY sync_rev ASC, id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                let raw_bytes: i64 = r.get(2)?;
                Ok(CatalogMeta {
                    sync_rev: r.get(1)?,
                    est_bytes: usize::try_from(raw_bytes).unwrap_or(256).saturating_mul(2).saturating_add(256),
                    key: CatalogKey::Model(r.get(0)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        collect_bounded_metas(list, |m| m.sync_rev, &mut metas, &mut safe_ceiling);
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT m.id, m.sync_rev,
                    length(CAST(m.name AS BLOB)) + length(CAST(m.description AS BLOB))
                    + length(CAST(m.config_json AS BLOB))
                    + coalesce(length(CAST(m.allowed_tools AS BLOB)), 0)
                    + coalesce(length(CAST(o.binding AS BLOB)), 0)
                    + coalesce(length(CAST(o.credentials AS BLOB)), 0) + 512
             FROM mcp_servers m
             LEFT JOIN mcp_oauth o ON o.server_id = m.id
             WHERE m.shared = 1 AND m.sync_rev > ?1
             ORDER BY m.sync_rev ASC, m.id ASC LIMIT ?2",
        )?;
        let list = stmt
            .query_map(params![since_rev, DELTA_PAGE_LIMIT as i64], |r| {
                let raw_bytes: i64 = r.get(2)?;
                Ok(CatalogMeta {
                    sync_rev: r.get(1)?,
                    est_bytes: usize::try_from(raw_bytes).unwrap_or(512).saturating_mul(2).saturating_add(512),
                    key: CatalogKey::Mcp(r.get(0)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        collect_bounded_metas(list, |m| m.sync_rev, &mut metas, &mut safe_ceiling);
    }

    {
        let mut stmt = conn.prepare_cached(
            "SELECT id, sync_rev,
                    length(CAST(title AS BLOB))
                    + min(length(CAST(last_error AS BLOB)), ?1)
                    + coalesce(length(CAST(reasoning_effort AS BLOB)), 0) + 512
             FROM sessions WHERE sync_rev > ?2
             ORDER BY sync_rev ASC, id ASC LIMIT ?3",
        )?;
        let list = stmt
            .query_map(
                params![
                    crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES as i64,
                    since_rev,
                    DELTA_PAGE_LIMIT as i64
                ],
                |r| {
                    let raw_bytes: i64 = r.get(2)?;
                    Ok(CatalogMeta {
                        sync_rev: r.get(1)?,
                        est_bytes: usize::try_from(raw_bytes).unwrap_or(512).saturating_mul(2).saturating_add(512),
                        key: CatalogKey::Session(r.get(0)?),
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        collect_bounded_metas(list, |m| m.sync_rev, &mut metas, &mut safe_ceiling);
    }

    metas.retain(|r| r.sync_rev <= safe_ceiling);
    metas.sort_by_key(|m| m.sync_rev);

    let mut delta = CatalogDelta {
        db_epoch,
        head_rev,
        next_rev: since_rev,
        has_more: safe_ceiling < i64::MAX,
        ..Default::default()
    };

    let mut total_bytes = 0usize;
    let total_rows = metas.len();
    let mut selected = Vec::new();
    for (idx, meta) in metas.into_iter().enumerate() {
        if idx > 0 && total_bytes.saturating_add(meta.est_bytes) > MAX_DELTA_BYTES {
            delta.has_more = true;
            break;
        }
        total_bytes += meta.est_bytes;
        delta.next_rev = meta.sync_rev;
        selected.push(meta.key);
    }

    let mut prov_stmt = conn.prepare_cached(
        "SELECT id, name, name_ts, base_url, api_key, endpoint_ts, archived_at, archived_ts, sync_rev
         FROM llm_providers WHERE id=?1",
    )?;
    let mut model_stmt = conn.prepare_cached(
        "SELECT id, provider_id, model_key, identity_ts,
                context_window_tokens, context_window_ts,
                input_modalities, input_modalities_ts,
                reasoning_efforts, reasoning_efforts_ts,
                pricing, pricing_ts, archived_at, archived_ts, sync_rev
         FROM llm_models WHERE id=?1",
    )?;
    let mcp_sql = format!(
        "SELECT {MCP_WIRE_COLUMNS} FROM mcp_servers m LEFT JOIN mcp_oauth o ON o.server_id = m.id WHERE m.id=?1"
    );
    let mut mcp_stmt = conn.prepare_cached(&mcp_sql)?;
    let mut sess_stmt = conn.prepare_cached(
        "SELECT id, title, title_ts, created_at, updated_at,
                status, last_error, status_ts, model_id, reasoning_effort, selection_ts,
                active_head, active_head_ts, deleted, sync_rev
         FROM sessions WHERE id=?1",
    )?;

    for key in selected {
        match key {
            CatalogKey::Provider(id) => {
                if let Some(p) = prov_stmt
                    .query_row([id], |r| {
                        Ok(ProviderWire {
                            id: r.get(0)?,
                            name: r.get(1)?,
                            name_ts: r.get(2)?,
                            base_url: r.get(3)?,
                            api_key: r.get(4)?,
                            endpoint_ts: r.get(5)?,
                            archived_at: r.get(6)?,
                            archived_ts: r.get(7)?,
                            sync_rev: r.get(8)?,
                        })
                    })
                    .optional()?
                {
                    delta.providers.push(p);
                }
            }
            CatalogKey::Model(id) => {
                if let Some(m) = model_stmt
                    .query_row([id], |r| {
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
                    })
                    .optional()?
                {
                    delta.models.push(m);
                }
            }
            CatalogKey::Mcp(id) => {
                if let Some(s) = mcp_stmt.query_row([id], mcp_wire_from_row).optional()? {
                    delta.mcp_servers.push(s);
                }
            }
            CatalogKey::Session(id) => {
                if let Some(s) = sess_stmt
                    .query_row([id], |r| {
                        Ok(SessionWire {
                            id: r.get(0)?,
                            title: r.get(1)?,
                            title_ts: r.get(2)?,
                            created_at: r.get(3)?,
                            updated_at: r.get(4)?,
                            status: r.get(5)?,
                            last_error: crate::calls::truncate_utf8(
                                &r.get::<_, String>(6)?,
                                crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES,
                            ),
                            status_ts: r.get(7)?,
                            model_id: r.get(8)?,
                            reasoning_effort: r.get(9)?,
                            selection_ts: r.get(10)?,
                            active_head: r.get(11)?,
                            active_head_ts: r.get(12)?,
                            deleted: r.get(13)?,
                            sync_rev: r.get(14)?,
                        })
                    })
                    .optional()?
                {
                    delta.sessions.push(s);
                }
            }
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

    // Two-phase bounded llm_calls fetch past remote_calls_vector.
    let local_calls_vec = read_vector(conn, CALLS_SCOPE)?;
    let mut call_meta_stmt = conn.prepare_cached(
        "SELECT id, origin_seq,
                min(coalesce(length(CAST(raw_usage AS BLOB)), 0), ?1)
                + min(coalesce(length(CAST(error_summary AS BLOB)), 0), ?2)
                + min(coalesce(length(CAST(upstream_response_id AS BLOB)), 0), ?3)
                + coalesce(length(CAST(reasoning_effort AS BLOB)), 0)
                + length(CAST(origin_node_id AS BLOB))
                + coalesce(length(CAST(pricing_snapshot AS BLOB)), 0) + 256
         FROM llm_calls
         WHERE origin_node_id = ?4 AND origin_seq > ?5
         ORDER BY origin_seq ASC, id ASC LIMIT ?6",
    )?;
    let mut call_full_stmt = conn.prepare_cached(
        "SELECT id, session_id, model_id, reasoning_effort, input_end_position, input_head,
                started_at, finished_at, status, upstream_response_id, http_status,
                error_summary, raw_usage, input_uncached_tokens, cache_read_tokens,
                cache_write_tokens, output_tokens, pricing_snapshot, cost_nano_usd,
                origin_node_id, origin_seq
         FROM llm_calls WHERE id = ?1",
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
        let batch = call_meta_stmt
            .query_map(
                params![
                    crate::calls::MAX_STORED_RAW_USAGE_BYTES as i64,
                    crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES as i64,
                    crate::calls::MAX_STORED_RESPONSE_ID_BYTES as i64,
                    &origin_node_id,
                    remote_seq,
                    DELTA_PAGE_LIMIT as i64
                ],
                |r| {
                    let raw_bytes: i64 = r.get(2)?;
                    Ok((
                        r.get::<_, CallId>(0)?,
                        r.get::<_, i64>(1)?,
                        usize::try_from(raw_bytes).unwrap_or(256).saturating_mul(2).saturating_add(256),
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let saturated = batch.len() == DELTA_PAGE_LIMIT;
        let mut last_seq = remote_seq;
        let mut broke_early = false;
        let mut selected_calls = Vec::new();
        for (cid, seq, approx) in batch {
            if total_bytes > 0 && total_bytes.saturating_add(approx) > MAX_DELTA_BYTES {
                delta.has_more = true;
                broke_early = true;
                break;
            }
            total_bytes += approx;
            last_seq = last_seq.max(seq);
            selected_calls.push(cid);
        }
        for cid in selected_calls {
            if let Some(call) = call_full_stmt
                .query_row([cid], |r| {
                    Ok(LlmCallWire {
                        id: r.get(0)?,
                        session_id: r.get(1)?,
                        model_id: r.get(2)?,
                        reasoning_effort: r.get(3)?,
                        input_end_position: r.get(4)?,
                        input_head: r.get(5)?,
                        started_at: r.get(6)?,
                        finished_at: r.get(7)?,
                        status: r.get(8)?,
                        upstream_response_id: crate::calls::clamp_response_id(r.get(9)?),
                        http_status: r.get(10)?,
                        error_summary: r.get::<_, Option<String>>(11)?.map(|s| {
                            crate::calls::truncate_utf8(
                                &s,
                                crate::calls::MAX_STORED_ERROR_SUMMARY_BYTES,
                            )
                        }),
                        raw_usage: crate::calls::clamp_raw_usage_str(r.get(12)?),
                        input_uncached_tokens: r.get(13)?,
                        cache_read_tokens: r.get(14)?,
                        cache_write_tokens: r.get(15)?,
                        output_tokens: r.get(16)?,
                        pricing_snapshot: r.get(17)?,
                        cost_nano_usd: r.get(18)?,
                        origin_node_id: r.get(19)?,
                        origin_seq: r.get(20)?,
                    })
                })
                .optional()?
            {
                delta.calls.push(call);
            }
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

enum SessionEventKey {
    Item { id: ItemId, oversized: bool },
    ContextNode(ContextNodeId),
    TurnSettled(ContextNodeId),
    InvalidatedWarning(ItemId),
}

struct SessionEventMeta {
    origin_seq: i64,
    est_bytes: usize,
    unpacked_bytes: usize,
    key: SessionEventKey,
}

fn collect_attachment_ids_from_value(val: &Value, out: &mut HashSet<AttachmentId>) {
    for field in ["content", "output", "summary"] {
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

fn collect_attachment_refs_from_item(payload: &str, out: &mut HashSet<AttachmentId>) {
    let Ok(val) = serde_json::from_str::<Value>(payload) else {
        return;
    };
    collect_attachment_ids_from_value(&val, out);
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
    let mut unpacked_total = 0usize;
    let mut referenced_attachments = HashSet::new();

    let mut item_meta_stmt = conn.prepare_cached(
        "SELECT id, origin_seq, length(CAST(payload AS BLOB))
         FROM items
         WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
         ORDER BY origin_seq ASC, id ASC LIMIT ?4",
    )?;
    let mut node_meta_stmt = conn.prepare_cached(
        "SELECT id, origin_seq
         FROM context_nodes
         WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
         ORDER BY origin_seq ASC, id ASC LIMIT ?4",
    )?;
    let mut settled_meta_stmt = conn.prepare_cached(
        "SELECT context_node_id, origin_seq
         FROM turn_settled
         WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
         ORDER BY origin_seq ASC, context_node_id ASC LIMIT ?4",
    )?;
    let mut warn_meta_stmt = conn.prepare_cached(
        "SELECT item_id, origin_seq
         FROM invalidated_warnings
         WHERE session_id=?1 AND origin_node_id=?2 AND origin_seq>?3
         ORDER BY origin_seq ASC, item_id ASC LIMIT ?4",
    )?;

    let mut item_full_stmt = conn.prepare_cached(
        "SELECT id, session_id, kind, payload, target_id, llm_call_id,
                idempotency_key, codemode_call_item_id, origin_node_id, origin_seq
         FROM items WHERE id=?1 AND session_id=?2",
    )?;
    let mut node_full_stmt = conn.prepare_cached(
        "SELECT id, session_id, parent_id, depth, item_id, origin_node_id, origin_seq
         FROM context_nodes WHERE id=?1 AND session_id=?2",
    )?;
    let mut settled_full_stmt = conn.prepare_cached(
        "SELECT context_node_id, session_id, origin_node_id, origin_seq
         FROM turn_settled WHERE context_node_id=?1 AND session_id=?2 AND origin_node_id=?3",
    )?;
    let mut warn_full_stmt = conn.prepare_cached(
        "SELECT item_id, session_id, origin_node_id, origin_seq
         FROM invalidated_warnings WHERE item_id=?1 AND session_id=?2 AND origin_node_id=?3",
    )?;

    for (origin_node_id, local_max) in local_vec {
        let remote_seq = remote_vector.get(&origin_node_id).copied().unwrap_or(0);
        if local_max <= remote_seq {
            continue;
        }
        if total_bytes >= MAX_DELTA_BYTES || unpacked_total >= MAX_DELTA_BYTES {
            delta.has_more = true;
            break;
        }

        let mut events: Vec<SessionEventMeta> = Vec::new();
        let mut safe_ceiling = i64::MAX;

        {
            let list = item_meta_stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        let raw_len = usize::try_from(r.get::<_, i64>(2)?).unwrap_or(0);
                        let oversized = raw_len > MAX_INLINE_ITEM_PAYLOAD_BYTES;
                        let est_bytes = if oversized {
                            512
                        } else {
                            raw_len.saturating_mul(2).saturating_add(512)
                        };
                        Ok(SessionEventMeta {
                            origin_seq: r.get(1)?,
                            est_bytes,
                            unpacked_bytes: raw_len.saturating_add(512),
                            key: SessionEventKey::Item {
                                id: r.get(0)?,
                                oversized,
                            },
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            collect_bounded_metas(list, |e| e.origin_seq, &mut events, &mut safe_ceiling);
        }

        {
            let list = node_meta_stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(SessionEventMeta {
                            origin_seq: r.get(1)?,
                            est_bytes: 256,
                            unpacked_bytes: 256,
                            key: SessionEventKey::ContextNode(r.get(0)?),
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            collect_bounded_metas(list, |e| e.origin_seq, &mut events, &mut safe_ceiling);
        }

        {
            let list = settled_meta_stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(SessionEventMeta {
                            origin_seq: r.get(1)?,
                            est_bytes: 192,
                            unpacked_bytes: 192,
                            key: SessionEventKey::TurnSettled(r.get(0)?),
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            collect_bounded_metas(list, |e| e.origin_seq, &mut events, &mut safe_ceiling);
        }

        {
            let list = warn_meta_stmt
                .query_map(
                    params![sid, &origin_node_id, remote_seq, DELTA_PAGE_LIMIT as i64],
                    |r| {
                        Ok(SessionEventMeta {
                            origin_seq: r.get(1)?,
                            est_bytes: 192,
                            unpacked_bytes: 192,
                            key: SessionEventKey::InvalidatedWarning(r.get(0)?),
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            collect_bounded_metas(list, |e| e.origin_seq, &mut events, &mut safe_ceiling);
        }

        events.retain(|e| e.origin_seq <= safe_ceiling);
        events.sort_by_key(|e| e.origin_seq);

        let mut last_seq = remote_seq;
        let mut broke_early = false;
        let mut selected = Vec::new();
        for ev in events {
            if total_bytes > 0
                && (total_bytes.saturating_add(ev.est_bytes) > MAX_DELTA_BYTES
                    || unpacked_total.saturating_add(ev.unpacked_bytes) > MAX_DELTA_BYTES)
            {
                delta.has_more = true;
                broke_early = true;
                break;
            }
            total_bytes += ev.est_bytes;
            unpacked_total += ev.unpacked_bytes;
            last_seq = last_seq.max(ev.origin_seq);
            selected.push(ev.key);
        }

        for key in selected {
            match key {
                SessionEventKey::Item { id, oversized } => {
                    if let Some(mut it) = item_full_stmt
                        .query_row(params![id, sid], |r| {
                            Ok(ItemWire {
                                id: r.get(0)?,
                                session_id: r.get(1)?,
                                kind: r.get(2)?,
                                payload: r.get(3)?,
                                payload_len: None,
                                payload_hash: None,
                                target_id: r.get(4)?,
                                llm_call_id: r.get(5)?,
                                idempotency_key: r.get(6)?,
                                codemode_call_item_id: r.get(7)?,
                                origin_node_id: r.get(8)?,
                                origin_seq: r.get(9)?,
                            })
                        })
                        .optional()?
                    {
                        if matches!(
                            it.kind.as_str(),
                            "user_message" | "function_call_output" | "codemode_event"
                        ) {
                            collect_attachment_refs_from_item(&it.payload, &mut referenced_attachments);
                        }
                        if oversized || it.payload.len() > MAX_INLINE_ITEM_PAYLOAD_BYTES {
                            let bytes = it.payload.as_bytes();
                            it.payload_len = Some(bytes.len() as u64);
                            it.payload_hash = Some(*blake3::hash(bytes).as_bytes());
                            it.payload = String::new();
                        }
                        delta.items.push(it);
                    }
                }
                SessionEventKey::ContextNode(id) => {
                    if let Some(node) = node_full_stmt
                        .query_row(params![id, sid], |r| {
                            Ok(ContextNodeWire {
                                id: r.get(0)?,
                                session_id: r.get(1)?,
                                parent_id: r.get(2)?,
                                depth: r.get(3)?,
                                item_id: r.get(4)?,
                                origin_node_id: r.get(5)?,
                                origin_seq: r.get(6)?,
                            })
                        })
                        .optional()?
                    {
                        delta.context_nodes.push(node);
                    }
                }
                SessionEventKey::TurnSettled(id) => {
                    if let Some(ts) = settled_full_stmt
                        .query_row(params![id, sid, &origin_node_id], |r| {
                            Ok(TurnSettledWire {
                                context_node_id: r.get(0)?,
                                session_id: r.get(1)?,
                                origin_node_id: r.get(2)?,
                                origin_seq: r.get(3)?,
                            })
                        })
                        .optional()?
                    {
                        delta.turn_settled.push(ts);
                    }
                }
                SessionEventKey::InvalidatedWarning(id) => {
                    if let Some(iw) = warn_full_stmt
                        .query_row(params![id, sid, &origin_node_id], |r| {
                            Ok(InvalidatedWarningWire {
                                item_id: r.get(0)?,
                                session_id: r.get(1)?,
                                origin_node_id: r.get(2)?,
                                origin_seq: r.get(3)?,
                            })
                        })
                        .optional()?
                    {
                        delta.invalidated_warnings.push(iw);
                    }
                }
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
