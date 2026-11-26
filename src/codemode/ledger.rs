use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use super::{Blocks, Part};
use crate::{
    Error, Result,
    cluster::ledger::{
        next_catalog_rev, next_origin_seq, on_item_inserted, session_scope, stage_catalog,
        stage_session, tick_hlc, with_savepoint,
    },
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
    with_savepoint(conn, "codemode_tool", || {
        if let Some(id) = tool_id(conn, sid, original)? {
            return Ok(id);
        }
        let id = ItemId::new()?;
        let payload = json!({"type":"function_call_output","call_id":call,"output":output});
        let origin_node_id = crate::cluster::identity::local_node_id(conn)?;
        let origin_seq = next_origin_seq(conn, &session_scope(sid), &origin_node_id)?;
        let inserted: ItemId = conn
            .prepare_cached(
                "INSERT INTO items(id,session_id,kind,payload,target_id,codemode_call_item_id,origin_node_id,origin_seq)
                 SELECT ?6,session_id,'function_call_output',?4,id,?5,?7,?8 FROM items
                 WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.call_id')=?3
                 RETURNING id",
            )?
            .query_row(
                params![
                    sid,
                    original,
                    call,
                    payload,
                    background.then_some(original),
                    id,
                    &*origin_node_id,
                    origin_seq
                ],
                |r| r.get(0),
            )?;
        on_item_inserted(
            conn,
            sid,
            inserted,
            "function_call_output",
            &payload,
            Some(original),
        )?;
        if !background {
            invalidate_only(conn, sid, original)?;
        }
        stage_session(sid);
        Ok(inserted)
    })
}

fn invalidate_only(conn: &Connection, sid: SessionId, origin: ItemId) -> Result<usize> {
    let ids = {
        let mut stmt = conn.prepare_cached(
            "SELECT i.id FROM pending_inputs p
             JOIN items i ON i.id=p.item_id
             WHERE p.session_id=?1 AND p.kind='codemode_event'
               AND i.codemode_call_item_id=?2 AND json_extract(i.payload,'$.event')='lease_warning'
             ORDER BY i.id",
        )?;
        stmt.query_map(params![sid, origin], |r| r.get::<_, ItemId>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if ids.is_empty() {
        return Ok(0);
    }
    let origin_node_id = crate::cluster::identity::local_node_id(conn)?;
    let scope = session_scope(sid);
    let mut inserted = 0usize;
    let mut exists_stmt = conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM invalidated_warnings WHERE item_id=?1)",
    )?;
    let mut ins_stmt = conn.prepare_cached(
        "INSERT INTO invalidated_warnings(item_id, session_id, origin_node_id, origin_seq)
         VALUES(?1, ?2, ?3, ?4)
         ON CONFLICT(item_id, origin_node_id) DO NOTHING",
    )?;
    let mut del_stmt =
        conn.prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?;
    for item_id in ids {
        let already: bool = exists_stmt.query_row([item_id], |r| r.get(0))?;
        del_stmt.execute(params![sid, item_id])?;
        if !already {
            let origin_seq = next_origin_seq(conn, &scope, &origin_node_id)?;
            let n = ins_stmt.execute(params![item_id, sid, &*origin_node_id, origin_seq])?;
            if n > 0 {
                inserted += 1;
            }
        }
    }
    if inserted > 0 {
        stage_session(sid);
    }
    Ok(inserted)
}

pub fn invalidate(conn: &Connection, sid: SessionId, origin: ItemId) -> Result<()> {
    with_savepoint(conn, "codemode_invalidate", || {
        invalidate_only(conn, sid, origin)?;
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
    if !matches!(kind, "message" | "lease_warning" | "completed")
        || (!notify && kind != "completed")
        || !content.is_array()
    {
        return Err(Error::internal("invalid codemode event"));
    }
    with_savepoint(conn, "codemode_event", || {
        if let Some(id) = completed_id(conn, sid, origin)? {
            if kind != "completed" {
                return Err(Error::conflict("codemode task already completed"));
            }
            return Ok(id);
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
        let origin_node_id = crate::cluster::identity::local_node_id(conn)?;
        let origin_seq = next_origin_seq(conn, &session_scope(sid), &origin_node_id)?;
        let inserted: ItemId = conn
            .prepare_cached(
                "INSERT INTO items(id,session_id,kind,payload,codemode_call_item_id,origin_node_id,origin_seq)
                 SELECT ?4,session_id,'codemode_event',?3,id,?5,?6 FROM items
                 WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.name')='codemode'
                 RETURNING id",
            )?
            .query_row(
                params![sid, origin, payload, id, &*origin_node_id, origin_seq],
                |r| r.get(0),
            )?;
        on_item_inserted(conn, sid, inserted, "codemode_event", &payload, None)?;
        if kind == "completed" {
            invalidate_only(conn, sid, origin)?;
        }
        stage_session(sid);
        Ok(inserted)
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
                  (?3 AND e.id IS NOT NULL AND EXISTS(
                      SELECT 1 FROM pending_inputs p WHERE p.session_id=c.session_id AND p.item_id=e.id
                  )) AS queued
           FROM items c JOIN sessions s ON s.id=c.session_id
           LEFT JOIN items r ON r.kind='function_call_output' AND r.target_id=c.id AND r.codemode_call_item_id IS NULL
           LEFT JOIN items e ON e.kind='codemode_event' AND e.codemode_call_item_id=c.id AND json_extract(e.payload,'$.event')='completed'
           WHERE c.session_id=?1 AND c.id=?2 AND s.deleted=0 AND c.kind='function_call' AND json_extract(c.payload,'$.name')='codemode'
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
            "SELECT count(*) FROM pending_inputs p
             JOIN items i ON i.id=p.item_id
             WHERE p.session_id=?1 AND p.kind='codemode_event'
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

pub fn recover(conn: &Connection) -> Result<()> {
    let local_node_id = crate::cluster::identity::local_node_id(conn)?;
    // Foreground tasks may have emitted lease warnings too. No lease survives a restart.
    let warning_candidates = {
        let mut stmt = conn.prepare(
            "SELECT p.session_id, p.item_id
             FROM pending_inputs p
             JOIN items i ON i.id=p.item_id
             JOIN sessions s ON s.id=p.session_id
             WHERE s.deleted=0
               AND p.kind='codemode_event'
               AND json_extract(i.payload,'$.event')='lease_warning'
               AND coalesce(i.origin_node_id, ?1) = ?1
             ORDER BY p.session_id, p.item_id",
        )?;
        stmt.query_map([&local_node_id], |r| {
            Ok((r.get::<_, SessionId>(0)?, r.get::<_, ItemId>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if !warning_candidates.is_empty() {
        with_savepoint(conn, "recover_warnings", || {
            let mut exists_stmt = conn.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM invalidated_warnings WHERE item_id=?1)",
            )?;
            let mut ins_stmt = conn.prepare_cached(
                "INSERT INTO invalidated_warnings(item_id, session_id, origin_node_id, origin_seq)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(item_id, origin_node_id) DO NOTHING",
            )?;
            let mut del_stmt = conn
                .prepare_cached("DELETE FROM pending_inputs WHERE session_id=?1 AND item_id=?2")?;
            for (sid, item_id) in warning_candidates {
                let already: bool = exists_stmt.query_row([item_id], |r| r.get(0))?;
                del_stmt.execute(params![sid, item_id])?;
                if !already {
                    let origin_seq = next_origin_seq(conn, &session_scope(sid), &local_node_id)?;
                    let n = ins_stmt.execute(params![item_id, sid, &local_node_id, origin_seq])?;
                    if n > 0 {
                        stage_session(sid);
                    }
                }
            }
            Ok(())
        })?;
    }

    let missing = {
        let mut receipts = conn.prepare(
            "SELECT r.session_id, r.codemode_call_item_id
             FROM items r INDEXED BY codemode_receipts
             JOIN sessions s ON s.id=r.session_id
             WHERE s.deleted=0
               AND r.kind='function_call_output'
               AND r.codemode_call_item_id IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM items e WHERE e.kind='codemode_event'
                               AND e.codemode_call_item_id=r.codemode_call_item_id
                               AND json_extract(e.payload,'$.event')='completed')
               AND coalesce(r.origin_node_id, ?1) = ?1
             ORDER BY r.id",
        )?;
        receipts
            .query_map([&local_node_id], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<(SessionId, ItemId)>>>()?
    };
    for (sid, origin) in missing {
        with_savepoint(conn, "recover_receipt", || {
            let status = json!({"id":origin.task_id(),"status":"interrupted","error":"Daemon restarted; result unknown."});
            let content = json!([{"type":"text","text":status.to_string()}]);
            event(conn, sid, origin, "completed", &content, true)?;
            let now = chrono::Utc::now().timestamp_millis();
            let ts = tick_hlc(conn)?;
            let rev = next_catalog_rev(conn)?;
            let changed = conn.execute(
                "UPDATE sessions
                 SET status='paused', last_error='daemon restarted',
                     local_status=NULL, local_error='',
                     updated_at=max(updated_at,?2), status_ts=?3, sync_rev=?4
                 WHERE id=?1 AND deleted=0",
                params![sid, now, ts, rev],
            )?;
            if changed > 0 {
                stage_catalog();
            }
            Ok(())
        })?;
    }

    // A crash after committing an event can precede the idle worker's wake-up.
    let pending_sids = {
        let mut stmt = conn.prepare(
            "SELECT id FROM sessions WHERE coalesce(local_status, status)='idle' AND deleted=0 AND EXISTS (
                 SELECT 1 FROM pending_inputs p JOIN items i ON i.id=p.item_id
                 WHERE p.session_id=sessions.id AND p.kind='codemode_event'
                   AND coalesce(i.origin_node_id, ?1) = ?1
             )",
        )?;
        stmt.query_map([&local_node_id], |r| r.get::<_, SessionId>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for sid in pending_sids {
        with_savepoint(conn, "recover_pending", || {
            let now = chrono::Utc::now().timestamp_millis();
            let ts = tick_hlc(conn)?;
            let rev = next_catalog_rev(conn)?;
            let changed = conn.execute(
                "UPDATE sessions
                 SET status='paused', last_error='daemon restarted with pending codemode events',
                     local_status=NULL, local_error='',
                     updated_at=max(updated_at,?2), status_ts=?3, sync_rev=?4
                 WHERE id=?1 AND deleted=0",
                params![sid, now, ts, rev],
            )?;
            if changed > 0 {
                stage_catalog();
            }
            Ok(())
        })?;
    }
    Ok(())
}
