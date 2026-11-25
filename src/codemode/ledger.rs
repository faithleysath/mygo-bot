use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use super::{Blocks, Part};
use crate::{
    Error, Result,
    id::{ItemId, SessionId},
    media,
};

pub fn tool_id(conn: &Connection, sid: SessionId, original: ItemId) -> Result<Option<ItemId>> {
    Ok(conn
        .prepare_cached(
            "SELECT id FROM items WHERE session_id=? AND kind='function_call_output' AND target_id=?",
        )?
        .query_row((sid, original), |r| r.get(0))
        .optional()?)
}

pub fn completed_id(conn: &Connection, sid: SessionId, origin: ItemId) -> Result<Option<ItemId>> {
    Ok(conn
        .prepare_cached(
            "SELECT id FROM items WHERE session_id=? AND kind='codemode_event'
         AND codemode_call_item_id=? AND json_extract(payload,'$.event')='completed'",
        )?
        .query_row((sid, origin), |r| r.get(0))
        .optional()?)
}

// All writes for one delivery share the caller's transaction. The controller publishes
// the returned item ID only after that transaction commits.
pub fn tool(
    conn: &Connection,
    sid: SessionId,
    original: ItemId,
    call: &str,
    output: &Value,
    background: bool,
) -> Result<ItemId> {
    tool_at_epoch(conn, sid, original, call, output, background, None)
}

pub fn tool_at_epoch(
    conn: &Connection,
    sid: SessionId,
    original: ItemId,
    call: &str,
    output: &Value,
    background: bool,
    expected_epoch: Option<i64>,
) -> Result<ItemId> {
    crate::cluster::ledger::with_savepoint(conn, "codemode_tool", || {
        if let Some(id) = tool_id(conn, sid, original)? {
            return Ok(id);
        }
        let id = ItemId::new()?;
        let payload = json!({"type":"function_call_output","call_id":call,"output":output});
        let origin_node_id = crate::cluster::identity::load_local_node(conn)?.node_id;
        let id = conn.prepare_cached(
            "INSERT INTO items(id,session_id,kind,payload,target_id,codemode_call_item_id,origin_node_id)
             SELECT ?6,session_id,'function_call_output',?4,id,?5,?7 FROM items
             WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.call_id')=?3
             RETURNING id",
        )?.query_row(params![sid, original, call, payload, background.then_some(original), id, origin_node_id], |r| r.get(0))?;
        let invalidated = if !background {
            invalidate_only(conn, sid, original)?
        } else {
            Vec::new()
        };
        crate::cluster::ledger::record_session_delta_at_epoch(
            conn,
            sid,
            "session_delta",
            &[id],
            Vec::new(),
            invalidated,
            expected_epoch,
        )?;
        Ok(id)
    })
}

fn invalidate_only(conn: &Connection, sid: SessionId, origin: ItemId) -> Result<Vec<ItemId>> {
    let mut stmt = conn.prepare_cached(
        "INSERT INTO invalidated_warnings(item_id)
         SELECT i.id FROM items i JOIN sessions s ON s.id=i.session_id
         WHERE i.session_id=? AND i.codemode_call_item_id=? AND i.kind='codemode_event'
           AND i.id>s.input_queue_cursor AND json_extract(i.payload,'$.event')='lease_warning'
         ON CONFLICT(item_id) DO NOTHING RETURNING item_id",
    )?;
    let ids = stmt
        .query_map((sid, origin), |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<ItemId>>>()?;
    Ok(ids)
}

pub fn invalidate_at_epoch(
    conn: &Connection,
    sid: SessionId,
    origin: ItemId,
    expected_epoch: Option<i64>,
) -> Result<()> {
    crate::cluster::ledger::with_savepoint(conn, "codemode_invalidate", || {
        let ids = invalidate_only(conn, sid, origin)?;
        if !ids.is_empty() {
            crate::cluster::ledger::record_session_delta_at_epoch(
                conn,
                sid,
                "session_delta",
                &[],
                Vec::new(),
                ids,
                expected_epoch,
            )?;
        }
        Ok(())
    })
}

pub fn event(
    conn: &Connection,
    sid: SessionId,
    origin: ItemId,
    kind: &str,
    content: &Value,
    notify: bool,
) -> Result<ItemId> {
    event_at_epoch(conn, sid, origin, kind, content, notify, None)
}

pub fn event_at_epoch(
    conn: &Connection,
    sid: SessionId,
    origin: ItemId,
    kind: &str,
    content: &Value,
    notify: bool,
    expected_epoch: Option<i64>,
) -> Result<ItemId> {
    if !matches!(kind, "message" | "lease_warning" | "completed")
        || (!notify && kind != "completed")
        || !content.is_array()
    {
        return Err(Error::internal("invalid codemode event"));
    }
    crate::cluster::ledger::with_savepoint(conn, "codemode_event", || {
        if let Some(id) = completed_id(conn, sid, origin)? {
            return if kind == "completed" {
                Ok(id)
            } else {
                Err(Error::conflict("codemode task already completed"))
            };
        }
        let background: Option<bool> = conn
            .prepare_cached(
                "SELECT codemode_call_item_id IS NOT NULL FROM items
             WHERE session_id=? AND kind='function_call_output' AND target_id=?",
            )?
            .query_row((sid, origin), |row| row.get(0))
            .optional()?;
        if background == Some(false) || (kind == "completed" && background.is_none()) {
            return Err(Error::conflict(
                "codemode event conflicts with tool delivery",
            ));
        }
        if kind == "message" {
            media::check_event(conn, content, &modalities(conn, sid)?)?;
        }
        let sequence: i64 = conn
            .prepare_cached(
                "SELECT coalesce(max(json_extract(payload,'$.sequence')),0)+1 FROM items
             WHERE codemode_call_item_id=? AND kind='codemode_event'",
            )?
            .query_row([origin], |r| r.get(0))?;
        let id = ItemId::new()?;
        let payload = json!({"event":kind,"task_id":origin.task_id(),"sequence":sequence,"notify":notify,"content":content});
        let origin_node_id = crate::cluster::identity::load_local_node(conn)?.node_id;
        let id = conn.prepare_cached(
            "INSERT INTO items(id,session_id,kind,payload,codemode_call_item_id,origin_node_id)
             SELECT ?4,session_id,'codemode_event',?3,id,?5 FROM items
             WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.name')='codemode'
             RETURNING id",
        )?.query_row(params![sid, origin, payload, id, origin_node_id], |r| r.get(0))?;
        let invalidated = if kind == "completed" {
            invalidate_only(conn, sid, origin)?
        } else {
            Vec::new()
        };
        crate::cluster::ledger::record_session_delta_at_epoch(
            conn,
            sid,
            "session_delta",
            &[id],
            Vec::new(),
            invalidated,
            expected_epoch,
        )?;
        Ok(id)
    })
}

pub fn starting_sibling(conn: &Connection, sid: SessionId, call: ItemId) -> Result<bool> {
    Ok(conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM items c WHERE c.session_id=?1 AND c.id!=?2
         AND c.llm_call_id=(SELECT llm_call_id FROM items WHERE id=?2)
         AND c.kind='function_call' AND json_extract(c.payload,'$.name')='codemode'
         AND NOT EXISTS(SELECT 1 FROM items r WHERE r.kind='function_call_output' AND r.target_id=c.id))",
    )?.query_row((sid, call), |r| r.get(0))?)
}

pub fn task_status(conn: &Connection, sid: SessionId, origin: ItemId, fold_queued: bool) -> Result<Value> {
    let id = origin.task_id();
    let row: Option<(Option<Value>, Option<Value>, bool)> = conn.prepare_cached(
        "WITH result AS (
           SELECT r.payload AS receipt, e.payload AS event,
                  (?3 AND e.id>s.input_queue_cursor AND json_extract(e.payload,'$.notify')=1) AS queued
           FROM items c JOIN sessions s ON s.id=c.session_id
           LEFT JOIN items r ON r.kind='function_call_output' AND r.target_id=c.id AND r.codemode_call_item_id IS NULL
           LEFT JOIN items e ON e.kind='codemode_event' AND e.codemode_call_item_id=c.id AND json_extract(e.payload,'$.event')='completed'
           WHERE c.session_id=?1 AND c.id=?2 AND c.kind='function_call' AND json_extract(c.payload,'$.name')='codemode'
         ) SELECT receipt, CASE WHEN queued THEN json_array(json_extract(event,'$.content[0]'))
                                ELSE json_extract(event,'$.content') END, coalesce(queued,0) FROM result",
    )?.query_row((sid, origin, fold_queued), |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).optional()?;
    let Some((receipt, content, queued)) = row else {
        return Ok(
            json!({"id":id,"status":"not_found","error":"unknown codemode task in this session"}),
        );
    };
    let wire = match (receipt, content) {
        (Some(mut receipt), _) => receipt["output"].take(),
        (None, Some(content)) => content,
        (None, None) => return Ok(json!({"id":id,"state":"starting"})),
    };
    let parts = wire.as_array().map(Vec::as_slice).unwrap_or_default();
    let header = wire
        .as_str()
        .or_else(|| parts.first()?["text"].as_str())
        .unwrap_or_default();
    let summary: Value = serde_json::from_str(header)
        .unwrap_or_else(|_| json!({"status":"interrupted","error":header}));
    let mut info = json!({"id":id,"state":"completed","status":match summary["status"].as_str() {
        Some("error") | None => "failed", Some(status) => status,
    }});
    if let Some(duration_ms) = summary.get("duration_ms").filter(|v| v.is_number()) {
        info["duration_ms"] = duration_ms.clone();
    }
    if queued {
        if let Some(error) = summary["error"].as_str() {
            info["error"] = json!(&error[..error.floor_char_boundary(512)]);
        }
        info["delivery"] = json!("queued");
        info["message"] =
            json!("Completion event is already queued and will be delivered in the next step.");
        return Ok(info);
    }
    if let Some(error) = summary.get("error") {
        info["error"] = error.clone();
    }
    let mut preview = String::new();
    let content = parts.get(1..).unwrap_or_default();
    let attachments = content.iter().filter(|part| part["type"] != "text").count();
    let mut truncated = false;
    let texts = summary["output"]
        .as_str()
        .into_iter()
        .chain(content.iter().filter_map(|part| part["text"].as_str()));
    for text in texts {
        const LIMIT: usize = 64 * 1024;
        if !preview.is_empty() && preview.len() < LIMIT {
            preview.push('\n');
        }
        let keep = text.floor_char_boundary(LIMIT - preview.len());
        preview.push_str(&text[..keep]);
        if keep < text.len() {
            truncated = true;
            break;
        }
    }
    info["output"] = Value::String(preview);
    info["output_truncated"] = json!(truncated);
    info["attachment_count"] = json!(attachments);
    Ok(info)
}

pub fn pending_messages(conn: &Connection, sid: SessionId) -> Result<usize> {
    let count: i64 = conn
        .prepare_cached(
            "SELECT count(*) FROM items i JOIN sessions s ON s.id=i.session_id
         WHERE i.session_id=? AND i.id>s.input_queue_cursor AND i.kind='codemode_event'
           AND json_extract(i.payload,'$.event')='message'",
        )?
        .query_row([sid], |r| r.get(0))?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

// An archived model still has a declared input shape. Its queued output survives until
// a user selects an available model; that selection checks the pending content again.
fn modalities(conn: &Connection, sid: SessionId) -> Result<Value> {
    Ok(conn.prepare_cached(
        "SELECT m.input_modalities FROM sessions s JOIN llm_models m ON m.id=s.model_id WHERE s.id=?",
    )?.query_row([sid], |r| r.get(0))?)
}

pub fn materialize(conn: &Connection, sid: SessionId, blocks: &Blocks) -> Result<Value> {
    if conn.is_autocommit() {
        return Err(Error::internal(
            "codemode output requires a delivery transaction",
        ));
    }
    // A client error can be converted to a text-only terminal status in the same
    // outer transaction. Roll back any attachments created before that error.
    conn.execute_batch("SAVEPOINT codemode_blocks")?;
    let result = (|| {
        let mut parts = Vec::with_capacity(blocks.parts.len());
        for part in &blocks.parts {
            let block = match part {
                Part::Text(text) => json!({"type":"text","text":text}),
                Part::Attachment { kind, id } => json!({"type":kind,"attachment_id":id}),
                Part::Bytes { kind, mime, data } => {
                    let mime = media::attachment_mime(mime)?;
                    if mime.type_().as_str() != kind {
                        return Err(Error::bad(format!(
                            "{kind} block cannot contain MIME {mime}"
                        )));
                    }
                    let id = media::save_attachment(conn, &mime, data)?;
                    json!({"type":kind,"attachment_id":id})
                }
            };
            parts.push(block);
        }
        let content = Value::Array(parts);
        media::check_parts(conn, &content, &modalities(conn, sid)?)?;
        Ok(content)
    })();
    if result.is_err() {
        conn.execute_batch("ROLLBACK TO codemode_blocks")?;
    }
    conn.execute_batch("RELEASE codemode_blocks")?;
    result
}

pub fn record_deferred_crash_session(
    conn: &Connection,
    sid: SessionId,
    reason: &str,
) -> Result<()> {
    if let Some(head) = crate::cluster::ledger::stream_head(conn, sid.as_str())? {
        conn.prepare_cached(
            "INSERT INTO deferred_crash_sessions(session_id, head_epoch, head_seq, head_hash, reason)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id) DO NOTHING",
        )?
        .execute(params![
            sid,
            head.epoch,
            head.head_seq,
            head.head_hash.as_slice(),
            reason
        ])?;
    }
    Ok(())
}

pub fn recover(conn: &Connection, commit_sync: bool) -> Result<bool> {
    let local_node_id = crate::cluster::identity::load_local_node(conn)?.node_id;
    let mut deferred_any = false;
    // Foreground tasks may have emitted lease warnings too. No lease survives a restart.
    let warning_candidates = {
        let mut stmt = conn.prepare(
            "SELECT i.session_id, i.id
             FROM items i INDEXED BY codemode_warnings
             JOIN sessions s ON s.id=i.session_id
             WHERE i.kind='codemode_event'
               AND json_extract(i.payload,'$.event')='lease_warning'
               AND i.id>s.input_queue_cursor
               AND coalesce(i.origin_node_id, ?1) = ?1
               AND NOT EXISTS(SELECT 1 FROM invalidated_warnings iw WHERE iw.item_id=i.id)
             ORDER BY i.session_id, i.id",
        )?;
        stmt.query_map([&local_node_id], |r| {
            Ok((r.get::<_, SessionId>(0)?, r.get::<_, ItemId>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if !warning_candidates.is_empty() {
        let mut by_session: std::collections::HashMap<SessionId, Vec<ItemId>> =
            std::collections::HashMap::new();
        for (sid, item_id) in warning_candidates {
            by_session.entry(sid).or_default().push(item_id);
        }
        for (sid, item_ids) in by_session {
            if crate::cluster::ledger::has_conflict(conn, sid.as_str())?
                || !crate::cluster::ledger::is_session_writable_local(conn, sid)?
            {
                continue;
            }
            if !commit_sync {
                record_deferred_crash_session(conn, sid, "daemon restarted")?;
                deferred_any = true;
                continue;
            }
            crate::cluster::ledger::with_savepoint(conn, "recover_warnings", || {
                let mut inserted = Vec::with_capacity(item_ids.len());
                let mut ins_stmt = conn.prepare_cached(
                    "INSERT INTO invalidated_warnings(item_id) VALUES(?) ON CONFLICT(item_id) DO NOTHING RETURNING item_id",
                )?;
                for item_id in item_ids {
                    if let Some(id) = ins_stmt.query_row([item_id], |r| r.get::<_, ItemId>(0)).optional()? {
                        inserted.push(id);
                    }
                }
                if !inserted.is_empty() {
                    crate::cluster::ledger::record_session_delta(
                        conn,
                        sid,
                        "session_delta",
                        &[],
                        Vec::new(),
                        inserted,
                    )?;
                }
                Ok(())
            })?;
        }
    }
    let mut receipts = conn.prepare(
        "SELECT r.session_id,r.codemode_call_item_id FROM items r INDEXED BY codemode_receipts
         WHERE r.kind='function_call_output' AND r.codemode_call_item_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM items e WHERE e.kind='codemode_event'
                           AND e.codemode_call_item_id=r.codemode_call_item_id AND json_extract(e.payload,'$.event')='completed')
           AND coalesce(r.origin_node_id, ?1) = ?1
         ORDER BY r.id",
    )?;
    let missing = receipts
        .query_map([&local_node_id], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(SessionId, ItemId)>>>()?;
    for (sid, origin) in missing {
        if crate::cluster::ledger::has_conflict(conn, sid.as_str())?
            || !crate::cluster::ledger::is_session_writable_local(conn, sid)?
        {
            continue;
        }
        if !commit_sync {
            conn.execute(
                "UPDATE sessions SET status='paused',last_error='daemon restarted' WHERE id=? AND status='idle'",
                [sid],
            )?;
            record_deferred_crash_session(conn, sid, "daemon restarted")?;
            deferred_any = true;
            continue;
        }
        crate::cluster::ledger::with_savepoint(conn, "recover_receipt", || {
            let status = json!({"id":origin.task_id(),"status":"interrupted","error":"Daemon restarted; result unknown."});
            let content = json!([{"type":"text","text":status.to_string()}]);
            event(conn, sid, origin, "completed", &content, true)?;
            conn.execute(
                "UPDATE sessions SET status='paused',last_error='daemon restarted' WHERE id=?",
                [sid],
            )?;
            crate::cluster::ledger::record_session_delta(
                conn,
                sid,
                "session_delta",
                &[],
                Vec::new(),
                Vec::new(),
            )?;
            Ok(())
        })?;
    }
    // A crash after committing an event can precede the idle worker's wake-up.
    let pending_sids = {
        let mut stmt = conn.prepare(
            "SELECT id FROM sessions WHERE status='idle' AND EXISTS (
                 SELECT 1 FROM pending p JOIN items i ON i.id=p.id
                 WHERE p.session_id=sessions.id AND i.kind='codemode_event'
                   AND coalesce(i.origin_node_id, ?1) = ?1
             )",
        )?;
        stmt.query_map([&local_node_id], |r| r.get::<_, SessionId>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for sid in pending_sids {
        if crate::cluster::ledger::has_conflict(conn, sid.as_str())?
            || !crate::cluster::ledger::is_session_writable_local(conn, sid)?
        {
            continue;
        }
        if !commit_sync {
            conn.execute(
                "UPDATE sessions SET status='paused',last_error='daemon restarted with pending codemode events' WHERE id=? AND status='idle'",
                [sid],
            )?;
            record_deferred_crash_session(conn, sid, "daemon restarted with pending codemode events")?;
            deferred_any = true;
            continue;
        }
        crate::cluster::ledger::with_savepoint(conn, "recover_pending", || {
            conn.execute(
                "UPDATE sessions SET status='paused',last_error='daemon restarted with pending codemode events' WHERE id=?",
                [sid],
            )?;
            crate::cluster::ledger::record_session_delta(
                conn,
                sid,
                "session_delta",
                &[],
                Vec::new(),
                Vec::new(),
            )?;
            Ok(())
        })?;
    }
    Ok(deferred_any)
}

#[derive(Clone, Debug)]
pub struct DeferredCrashSession {
    pub session_id: SessionId,
    pub head_epoch: i64,
    pub head_seq: i64,
    pub head_hash: [u8; 32],
    pub reason: String,
}

pub fn list_deferred_crash_sessions(conn: &Connection) -> Result<Vec<DeferredCrashSession>> {
    let mut stmt = conn.prepare(
        "SELECT session_id, head_epoch, head_seq, head_hash, reason FROM deferred_crash_sessions ORDER BY session_id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let hash_blob: Vec<u8> = r.get(3)?;
            let head_hash: [u8; 32] = hash_blob.try_into().map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Blob,
                    "invalid deferred crash head hash length".into(),
                )
            })?;
            Ok(DeferredCrashSession {
                session_id: r.get(0)?,
                head_epoch: r.get(1)?,
                head_seq: r.get(2)?,
                head_hash,
                reason: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn apply_deferred_crash_recovery(
    conn: &Connection,
    entry: &DeferredCrashSession,
    expected_head: &crate::cluster::ledger::StreamHead,
) -> Result<bool> {
    let sid = entry.session_id;
    let local_node_id = crate::cluster::identity::load_local_node(conn)?.node_id;
    crate::cluster::ledger::with_savepoint(conn, "deferred_crash_recovery", || {
        conn.execute("DELETE FROM deferred_crash_sessions WHERE session_id=?", [sid])?;
        if crate::cluster::ledger::has_conflict(conn, sid.as_str())?
            || !crate::cluster::ledger::is_session_writable_local(conn, sid)?
        {
            return Ok(false);
        }
        let Some(cur_head) = crate::cluster::ledger::stream_head(conn, sid.as_str())? else {
            return Ok(false);
        };
        if cur_head.epoch != expected_head.epoch
            || cur_head.head_seq != expected_head.head_seq
            || cur_head.head_hash != expected_head.head_hash
        {
            return Err(Error::conflict("session head changed before crash recovery commit"));
        }
        let head_changed = cur_head.epoch != entry.head_epoch
            || cur_head.head_seq != entry.head_seq
            || cur_head.head_hash != entry.head_hash;

        let invalidated = {
            let mut stmt = conn.prepare_cached(
                "INSERT INTO invalidated_warnings(item_id)
                 SELECT i.id FROM items i JOIN sessions s ON s.id=i.session_id
                 WHERE i.session_id=?1 AND i.kind='codemode_event'
                   AND json_extract(i.payload,'$.event')='lease_warning'
                   AND i.id>s.input_queue_cursor
                   AND coalesce(i.origin_node_id, ?2) = ?2
                 ON CONFLICT(item_id) DO NOTHING RETURNING item_id",
            )?;
            stmt.query_map(params![sid, &local_node_id], |r| r.get::<_, ItemId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let missing_receipts = {
            let mut stmt = conn.prepare_cached(
                "SELECT r.codemode_call_item_id FROM items r INDEXED BY codemode_receipts
                 WHERE r.session_id=?1 AND r.kind='function_call_output'
                   AND r.codemode_call_item_id IS NOT NULL
                   AND NOT EXISTS (SELECT 1 FROM items e WHERE e.kind='codemode_event'
                                   AND e.codemode_call_item_id=r.codemode_call_item_id
                                   AND json_extract(e.payload,'$.event')='completed')
                   AND coalesce(r.origin_node_id, ?2) = ?2
                 ORDER BY r.id",
            )?;
            stmt.query_map(params![sid, &local_node_id], |r| r.get::<_, ItemId>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut added_items = Vec::new();
        for origin in missing_receipts {
            let status = json!({"id":origin.task_id(),"status":"interrupted","error":"Daemon restarted; result unknown."});
            let content = json!([{"type":"text","text":status.to_string()}]);
            let saved = event_at_epoch(
                conn,
                sid,
                origin,
                "completed",
                &content,
                true,
                Some(cur_head.epoch),
            )?;
            added_items.push(saved);
        }

        let need_pause = if !added_items.is_empty() {
            conn.execute(
                "UPDATE sessions SET status='paused',last_error=?2 WHERE id=?1",
                params![sid, &entry.reason],
            )?;
            true
        } else if !head_changed {
            // Nobody else touched this session since our crash: keep local paused status and publish it cleanly.
            let changed_status: String = conn.query_row(
                "SELECT status FROM sessions WHERE id=?",
                [sid],
                |r| r.get(0),
            )?;
            changed_status == "paused"
        } else {
            // Another node already took over and advanced this session while we were down.
            false
        };

        if !added_items.is_empty() || !invalidated.is_empty() || need_pause {
            crate::cluster::ledger::record_session_delta_at_epoch(
                conn,
                sid,
                "session_delta",
                &added_items,
                Vec::new(),
                invalidated,
                Some(cur_head.epoch),
            )?;
            return Ok(true);
        }
        Ok(false)
    })
}
