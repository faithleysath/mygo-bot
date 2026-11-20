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
            "SELECT id FROM items WHERE session_id=? AND kind='sandbox_event'
         AND sandbox_call_item_id=? AND json_extract(payload,'$.event')='completed'",
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
        "INSERT INTO items(session_id,kind,payload,target_id,sandbox_call_item_id)
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
         WHERE i.session_id=? AND i.sandbox_call_item_id=? AND i.kind='sandbox_event'
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
        return Err(Error::internal("invalid sandbox event"));
    }
    if let Some(id) = completed_id(conn, sid, origin)? {
        return if kind == "completed" {
            Ok(id)
        } else {
            Err(Error::conflict("sandbox already completed"))
        };
    }
    let background: Option<bool> = conn
        .prepare_cached(
            "SELECT sandbox_call_item_id IS NOT NULL FROM items
         WHERE session_id=? AND kind='function_call_output' AND target_id=?",
        )?
        .query_row((sid, origin), |row| row.get(0))
        .optional()?;
    if background == Some(false) || (kind == "completed" && background.is_none()) {
        return Err(Error::conflict(
            "sandbox event conflicts with tool delivery",
        ));
    }
    if kind == "message" {
        media::check_event(conn, content, &modalities(conn, sid)?)?;
    }
    let sequence: i64 = conn
        .prepare_cached(
            "SELECT coalesce(max(json_extract(payload,'$.sequence')),0)+1 FROM items
         WHERE sandbox_call_item_id=? AND kind='sandbox_event'",
        )?
        .query_row([origin], |r| r.get(0))?;
    let payload = json!({"event":kind,"sandbox_id":format!("sb_{origin}"),"sequence":sequence,"notify":notify,"content":content});
    let id = conn.prepare_cached(
        "INSERT INTO items(session_id,kind,payload,sandbox_call_item_id)
         SELECT session_id,'sandbox_event',?3,id FROM items
         WHERE session_id=?1 AND id=?2 AND kind='function_call' AND json_extract(payload,'$.name')='codemode'
         RETURNING id",
    )?.query_row(params![sid, origin, payload], |r| r.get(0))?;
    if kind == "completed" {
        invalidate(conn, sid, origin)?;
    }
    Ok(id)
}

pub fn pending_messages(conn: &Connection, sid: i64) -> Result<usize> {
    let count: i64 = conn
        .prepare_cached(
            "SELECT count(*) FROM items i JOIN sessions s ON s.id=i.session_id
         WHERE i.session_id=? AND i.id>s.input_queue_cursor AND i.kind='sandbox_event'
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
            "sandbox output requires a delivery transaction",
        ));
    }
    // A client error can be converted to a text-only terminal status in the same
    // outer transaction. Roll back any attachments created before that error.
    conn.execute_batch("SAVEPOINT sandbox_blocks")?;
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
        conn.execute_batch("ROLLBACK TO sandbox_blocks")?;
    }
    conn.execute_batch("RELEASE sandbox_blocks")?;
    result
}

pub fn recover(conn: &Connection) -> Result<()> {
    // Foreground sandboxes may have emitted lease warnings too. No lease survives a restart.
    conn.execute(
        "INSERT INTO invalidated_warnings(item_id)
         SELECT i.id FROM items i INDEXED BY sandbox_warnings JOIN sessions s ON s.id=i.session_id
         WHERE i.kind='sandbox_event' AND json_extract(i.payload,'$.event')='lease_warning'
           AND i.id>s.input_queue_cursor ON CONFLICT(item_id) DO NOTHING",
        [],
    )?;
    let mut receipts = conn.prepare(
        "SELECT r.session_id,r.sandbox_call_item_id FROM items r INDEXED BY sandbox_receipts
         WHERE r.kind='function_call_output' AND r.sandbox_call_item_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM items e WHERE e.kind='sandbox_event'
                           AND e.sandbox_call_item_id=r.sandbox_call_item_id AND json_extract(e.payload,'$.event')='completed')
         ORDER BY r.id",
    )?;
    let missing = receipts
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(i64, i64)>>>()?;
    for (sid, origin) in missing {
        let status = json!({"id":format!("sb_{origin}"),"status":"interrupted","error":"Daemon restarted; result unknown."});
        let content = json!([{"type":"text","text":status.to_string()}]);
        event(conn, sid, origin, "completed", &content, true)?;
        conn.execute(
            "UPDATE sessions SET status='paused',last_error='daemon restarted' WHERE id=?",
            [sid],
        )?;
    }
    // A crash after committing an event can precede the idle worker's wake-up.
    conn.execute(
        "UPDATE sessions SET status='paused',last_error='daemon restarted with pending sandbox events'
         WHERE status='idle' AND EXISTS (
             SELECT 1 FROM pending p JOIN items i ON i.id=p.id
             WHERE p.session_id=sessions.id AND i.kind='sandbox_event'
         )",
        [],
    )?;
    Ok(())
}
