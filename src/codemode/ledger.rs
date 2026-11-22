use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

use super::{Blocks, Part};
use crate::{Error, Result, media};

pub fn tool_id(conn: &Connection, sid: i64, original: i64) -> Result<Option<i64>> {
    Ok(conn.prepare_cached(
        "SELECT id FROM items WHERE session_id=? AND kind='function_call_output' AND target_id=?",
    )?.query_row((sid, original), |r| r.get(0)).optional()?)
}

pub fn completed_id(conn: &Connection, sid: i64, origin: i64) -> Result<Option<i64>> {
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
    sid: i64,
    original: i64,
    call: &str,
    output: &Value,
    background: bool,
) -> Result<i64> {
    if let Some(id) = tool_id(conn, sid, original)? {
        return Ok(id);
    }
    let payload = json!({"type":"function_call_output","call_id":call,"output":output});
    let id = conn.prepare_cached(
        "INSERT INTO items(session_id,kind,payload,target_id,codemode_call_item_id)
         SELECT session_id,'function_call_output',?4,id,?5 FROM items
         WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.call_id')=?3
         RETURNING id",
    )?.query_row(params![sid, original, call, payload, background.then_some(original)], |r| r.get(0))?;
    if !background {
        invalidate(conn, sid, original)?;
    }
    Ok(id)
}

pub fn invalidate(conn: &Connection, sid: i64, origin: i64) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO invalidated_warnings(item_id)
         SELECT i.id FROM items i JOIN sessions s ON s.id=i.session_id
         WHERE i.session_id=? AND i.codemode_call_item_id=? AND i.kind='codemode_event'
           AND i.id>s.input_queue_cursor AND json_extract(i.payload,'$.event')='lease_warning'
         ON CONFLICT(item_id) DO NOTHING",
    )?
    .execute((sid, origin))?;
    Ok(())
}

pub fn event(
    conn: &Connection,
    sid: i64,
    origin: i64,
    kind: &str,
    content: &Value,
    notify: bool,
) -> Result<i64> {
    if !matches!(kind, "message" | "lease_warning" | "completed")
        || (!notify && kind != "completed")
        || !content.is_array()
    {
        return Err(Error::internal("invalid codemode event"));
    }
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
    let payload = json!({"event":kind,"task_id":format!("cm_{origin}"),"sequence":sequence,"notify":notify,"content":content});
    let id = conn.prepare_cached(
        "INSERT INTO items(session_id,kind,payload,codemode_call_item_id)
         SELECT session_id,'codemode_event',?3,id FROM items
         WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.name')='codemode'
         RETURNING id",
    )?.query_row(params![sid, origin, payload], |r| r.get(0))?;
    if kind == "completed" {
        invalidate(conn, sid, origin)?;
    }
    Ok(id)
}

pub fn starting_sibling(conn: &Connection, sid: i64, call: i64) -> Result<bool> {
    Ok(conn.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM items c WHERE c.session_id=?1 AND c.id!=?2
         AND c.llm_call_id=(SELECT llm_call_id FROM items WHERE id=?2)
         AND c.kind='function_call' AND json_extract(c.payload,'$.name')='codemode'
         AND NOT EXISTS(SELECT 1 FROM items r WHERE r.kind='function_call_output' AND r.target_id=c.id))",
    )?.query_row((sid, call), |r| r.get(0))?)
}

pub fn task_status(conn: &Connection, sid: i64, origin: i64) -> Result<Value> {
    let id = format!("cm_{origin}");
    let row: Option<(Option<Value>, Option<Value>)> = conn.prepare_cached(
        "SELECT r.payload,e.payload FROM items c
         LEFT JOIN items r ON r.kind='function_call_output' AND r.target_id=c.id AND r.codemode_call_item_id IS NULL
         LEFT JOIN items e ON e.kind='codemode_event' AND e.codemode_call_item_id=c.id AND json_extract(e.payload,'$.event')='completed'
         WHERE c.session_id=? AND c.id=? AND c.kind='function_call' AND json_extract(c.payload,'$.name')='codemode'",
    )?.query_row((sid, origin), |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
    let wire = match row {
        None => {
            return Ok(
                json!({"id":id,"status":"not_found","error":"unknown codemode task in this session"}),
            );
        }
        Some((None, None)) => return Ok(json!({"id":id,"state":"starting"})),
        Some((Some(mut receipt), _)) => receipt["output"].take(),
        Some((None, Some(mut event))) => event["content"].take(),
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

pub fn pending_messages(conn: &Connection, sid: i64) -> Result<usize> {
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
fn modalities(conn: &Connection, sid: i64) -> Result<Value> {
    Ok(conn.prepare_cached(
        "SELECT m.input_modalities FROM sessions s JOIN llm_models m ON m.id=s.model_id WHERE s.id=?",
    )?.query_row([sid], |r| r.get(0))?)
}

pub fn materialize(conn: &Connection, sid: i64, blocks: &Blocks) -> Result<Value> {
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
    // Foreground tasks may have emitted lease warnings too. No lease survives a restart.
    conn.execute(
        "INSERT INTO invalidated_warnings(item_id)
         SELECT i.id FROM items i INDEXED BY codemode_warnings JOIN sessions s ON s.id=i.session_id
         WHERE i.kind='codemode_event' AND json_extract(i.payload,'$.event')='lease_warning'
           AND i.id>s.input_queue_cursor ON CONFLICT(item_id) DO NOTHING",
        [],
    )?;
    let mut receipts = conn.prepare(
        "SELECT r.session_id,r.codemode_call_item_id FROM items r INDEXED BY codemode_receipts
         WHERE r.kind='function_call_output' AND r.codemode_call_item_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM items e WHERE e.kind='codemode_event'
                           AND e.codemode_call_item_id=r.codemode_call_item_id AND json_extract(e.payload,'$.event')='completed')
         ORDER BY r.id",
    )?;
    let missing = receipts
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(i64, i64)>>>()?;
    for (sid, origin) in missing {
        let status = json!({"id":format!("cm_{origin}"),"status":"interrupted","error":"Daemon restarted; result unknown."});
        let content = json!([{"type":"text","text":status.to_string()}]);
        event(conn, sid, origin, "completed", &content, true)?;
        conn.execute(
            "UPDATE sessions SET status='paused',last_error='daemon restarted' WHERE id=?",
            [sid],
        )?;
    }
    // A crash after committing an event can precede the idle worker's wake-up.
    conn.execute(
        "UPDATE sessions SET status='paused',last_error='daemon restarted with pending codemode events'
         WHERE status='idle' AND EXISTS (
             SELECT 1 FROM pending p JOIN items i ON i.id=p.id
             WHERE p.session_id=sessions.id AND i.kind='codemode_event'
         )",
        [],
    )?;
    Ok(())
}
