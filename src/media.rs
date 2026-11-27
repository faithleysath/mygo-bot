mod wire;
pub(crate) use wire::model_input;

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::{Stream, StreamExt};
use rusqlite::{Connection, OptionalExtension, blob::ZeroBlob};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::{
    App, Error, Result,
    id::{AttachmentId, ModelId, SessionId},
    store::Db,
};

pub(crate) const MAX_ATTACHMENT_BYTES: usize = 50 * 1024 * 1024;

pub fn routes() -> Router<App> {
    Router::new()
        .route("/attachments", post(upload))
        .route("/attachments/{id}", get(download))
}

#[derive(serde::Serialize)]
struct Attachment {
    id: AttachmentId,
    mime_type: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TextKind {
    Text,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaKind {
    Image,
    Audio,
    Video,
}

impl MediaKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }
}

/// 消息内容块。媒体引用通过附件上传接口取得的 ID。
#[derive(serde::Serialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ContentBlock<'a> {
    Text {
        #[serde(rename = "type")]
        kind: TextKind,
        text: &'a str,
    },
    Media {
        #[serde(rename = "type")]
        kind: MediaKind,
        attachment_id: AttachmentId,
    },
}

// Count slow clients before reading their bodies. Byte permits travel with the buffers
// until the database worker has consumed them, including when the HTTP task is dropped.
static UPLOAD_SLOTS: Semaphore = Semaphore::const_new(8);
static UPLOAD_BYTES: Semaphore = Semaphore::const_new(2 * MAX_ATTACHMENT_BYTES);
const UPLOAD_BLOCK: usize = 64 * 1024;

async fn upload(State(app): State<App>, headers: HeaderMap, body: Body) -> Result<Response> {
    let mut types = headers.get_all(header::CONTENT_TYPE).iter();
    let mime = attachment_mime(types.next().and_then(|v| v.to_str().ok()).unwrap_or(""))?;
    if types.next().is_some() {
        return Err(Error::bad("expected one Content-Type"));
    }
    let too_large = || {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Failed to buffer the request body: length limit exceeded",
        )
            .into_response()
    };
    let busy = |_| {
        Error(
            StatusCode::TOO_MANY_REQUESTS,
            "upload capacity exhausted".into(),
        )
    };
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .map(|v| {
            v.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| Error::bad("invalid Content-Length"))
        })
        .transpose()?;
    if declared.is_some_and(|n| n > MAX_ATTACHMENT_BYTES as u64) {
        return Ok(too_large());
    }
    let declared = declared.map(|n| n as usize);
    let slot = UPLOAD_SLOTS.try_acquire().map_err(busy)?;
    let mut budget = UPLOAD_BYTES
        .try_acquire_many(declared.unwrap_or(0) as u32)
        .map_err(busy)?;
    let mut blocks: Vec<Vec<u8>> = Vec::new();
    let mut bytes = 0;
    let mut hash = blake3::Hasher::new();
    let mut stream = body.into_data_stream();
    loop {
        let frame = tokio::select! {
            biased;
            _ = app.stopping.cancelled() => return Err(Error(StatusCode::SERVICE_UNAVAILABLE, "daemon stopping".into())),
            next = stream.next() => next,
        };
        let Some(frame) = frame else { break };
        let frame = frame.map_err(|_| Error::bad("incomplete attachment body"))?;
        if frame.len() > MAX_ATTACHMENT_BYTES - bytes {
            return Ok(too_large());
        }
        if declared.is_some_and(|n| bytes + frame.len() > n) {
            return Err(Error::bad("attachment length differs from Content-Length"));
        }
        hash.update(&frame);
        let mut slice = frame.as_ref();
        while !slice.is_empty() {
            if blocks.last().is_none_or(|b| b.len() == b.capacity()) {
                let capacity = UPLOAD_BLOCK.min(declared.unwrap_or(MAX_ATTACHMENT_BYTES) - bytes);
                if declared.is_none() {
                    budget.merge(
                        UPLOAD_BYTES
                            .try_acquire_many(capacity as u32)
                            .map_err(busy)?,
                    );
                }
                blocks.push(Vec::with_capacity(capacity));
            }
            let block = blocks.last_mut().unwrap();
            let n = slice.len().min(block.capacity() - block.len());
            block.extend_from_slice(&slice[..n]);
            slice = &slice[n..];
            bytes += n;
        }
    }
    if bytes == 0 || declared.is_some_and(|n| n != bytes) {
        return Err(Error::bad("expected a complete, nonempty attachment"));
    }
    let hash = hash.finalize();
    let created_at = chrono::Utc::now().timestamp_millis();
    let saved = app
        .db
        .write(
            move |conn| {
                let _permits = (slot, budget);
                let id = AttachmentId::new()?;
                let rowid = conn.prepare_cached(INSERT_ATTACHMENT)?.insert((
                    id,
                    mime.as_ref(),
                    hash.as_bytes().as_slice(),
                    bytes as i64,
                    created_at,
                    ZeroBlob(bytes as u64),
                ))?;
                let mut blob =
                    conn.blob_open(rusqlite::MAIN_DB, "attachments", "data", rowid, false)?;
                let mut offset = 0;
                for block in blocks {
                    blob.write_at(&block, offset)?;
                    offset += block.len();
                }
                blob.close()?;
                Ok(Attachment {
                    id,
                    mime_type: mime.to_string(),
                })
            },
            |_, saved| Ok(saved),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(saved)).into_response())
}

const INSERT_ATTACHMENT: &str =
    "INSERT INTO attachments(id,mime_type,content_hash,byte_len,created_at,data) VALUES(?,?,?,?,?,?)";

pub(crate) const ATTACHMENT_GC_GRACE_MS: i64 = 24 * 60 * 60 * 1000;
const ATTACHMENT_GC_BATCH: usize = 64;
const ATTACHMENT_GC_MAX_BATCHES_PER_SWEEP: usize = 16;
const ATTACHMENT_GC_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_secs(60);
const ATTACHMENT_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

pub fn gc_orphan_attachments(conn: &Connection, cutoff_ms: i64, limit: usize) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
    let deleted = conn
        .prepare_cached(
            "DELETE FROM attachments
             WHERE id IN (
                 SELECT a.id FROM attachments a
                 WHERE a.created_at <= ?1
                   AND NOT EXISTS (SELECT 1 FROM item_attachments ia WHERE ia.attachment_id = a.id)
                 ORDER BY a.created_at, a.id
                 LIMIT ?2
             )",
        )?
        .execute(rusqlite::params![cutoff_ms, limit_i64])?;
    Ok(deleted)
}

pub async fn reap_orphan_attachments(db: Db, stopping: tokio_util::sync::CancellationToken) {
    tokio::select! {
        biased;
        _ = stopping.cancelled() => return,
        _ = tokio::time::sleep(ATTACHMENT_GC_INITIAL_DELAY) => {}
    }
    loop {
        if stopping.is_cancelled() {
            return;
        }
        let cutoff_ms = chrono::Utc::now().timestamp_millis() - ATTACHMENT_GC_GRACE_MS;
        for _ in 0..ATTACHMENT_GC_MAX_BATCHES_PER_SWEEP {
            if stopping.is_cancelled() {
                return;
            }
            let res = db
                .write(
                    move |conn| gc_orphan_attachments(conn, cutoff_ms, ATTACHMENT_GC_BATCH),
                    |_, deleted| Ok(deleted),
                )
                .await;
            match res {
                Ok(deleted) if deleted >= ATTACHMENT_GC_BATCH => {
                    tokio::task::yield_now().await;
                }
                Ok(_) => break,
                Err(err) => {
                    tracing::warn!(error = %err, "orphan attachment GC sweep failed");
                    break;
                }
            }
        }
        tokio::select! {
            biased;
            _ = stopping.cancelled() => return,
            _ = tokio::time::sleep(ATTACHMENT_GC_INTERVAL) => {}
        }
    }
}

pub fn attachment_mime(value: &str) -> Result<mime::Mime> {
    value
        .parse::<mime::Mime>()
        .ok()
        .filter(|m| {
            !m.essence_str().contains('*')
                && matches!(m.type_().as_str(), "image" | "audio" | "video")
        })
        .ok_or_else(|| Error::bad("expected a concrete image, audio or video Content-Type"))
}

pub fn save_attachment(conn: &Connection, mime: &mime::Mime, data: &[u8]) -> Result<AttachmentId> {
    if data.is_empty() || data.len() > MAX_ATTACHMENT_BYTES {
        return Err(Error::bad("expected 1 byte to 50 MiB of attachment data"));
    }
    let hash = blake3::hash(data);
    let byte_len = i64::try_from(data.len()).map_err(|_| Error::bad("attachment too large"))?;
    let created_at = chrono::Utc::now().timestamp_millis();
    let id = AttachmentId::new()?;
    conn.prepare_cached(INSERT_ATTACHMENT)?.execute((
        id,
        mime.as_ref(),
        hash.as_bytes().as_slice(),
        byte_len,
        created_at,
        data,
    ))?;
    Ok(id)
}

#[derive(Clone, Copy)]
struct AttachmentBlob {
    rowid: i64,
    len: usize,
}

impl AttachmentBlob {
    fn lookup(conn: &Connection, id: AttachmentId) -> Result<(String, Self)> {
        let (mime, rowid, len): (String, i64, Option<u32>) = conn
            .prepare_cached("SELECT mime_type,rowid,length(data) FROM attachments WHERE id=?")?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        let len = len.ok_or_else(|| {
            Error::conflict(format!("attachment {id} bytes are unavailable locally"))
        })?;
        Ok((
            mime,
            Self {
                rowid,
                len: len as usize,
            },
        ))
    }

    fn chunks(self, db: Db) -> impl Stream<Item = Result<Bytes>> {
        // Each fresh blob handle walks the overflow chain to its offset. Read four
        // frames per open, then release SQLite before yielding any network bytes.
        const FRAME: usize = 192 * 1024; // multiple of three for unpadded interior base64
        const READ: usize = 4 * FRAME;
        async_stream::try_stream! {
            for offset in (0..self.len).step_by(READ) {
                let n = READ.min(self.len - offset);
                let mut bytes = db.call(move |s| {
                    let blob = s.conn.blob_open(rusqlite::MAIN_DB, "attachments", "data", self.rowid, true)?;
                    let mut bytes = vec![0; n];
                    blob.read_at_exact(&mut bytes, offset)?;
                    Ok(Bytes::from(bytes))
                }).await?;
                while !bytes.is_empty() {
                    yield bytes.split_to(FRAME.min(bytes.len()));
                }
            }
        }
    }
}

async fn download(State(app): State<App>, Path(id): Path<AttachmentId>) -> Result<Response> {
    app.cluster.ensure_attachment_data(id).await?;
    let (mime, blob) = app
        .db
        .call(move |s| AttachmentBlob::lookup(&s.conn, id))
        .await?;
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::CONTENT_LENGTH, blob.len.to_string()),
            (header::CONTENT_DISPOSITION, "attachment".to_owned()),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_owned()),
        ],
        Body::from_stream(blob.chunks(app.db)),
    )
        .into_response())
}

impl<'a> ContentBlock<'a> {
    fn parse(part: &'a Value) -> Result<Self> {
        let fields = part
            .as_object()
            .ok_or_else(|| Error::bad("expected a content block"))?;
        let kind = match part["type"].as_str() {
            Some("text") => {
                if fields.len() != 2 {
                    return Err(Error::bad("text block accepts only type and text"));
                }
                let text = part["text"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| Error::bad("text block requires nonempty text"))?;
                return Ok(Self::Text {
                    kind: TextKind::Text,
                    text,
                });
            }
            Some("image") => MediaKind::Image,
            Some("audio") => MediaKind::Audio,
            Some("video") => MediaKind::Video,
            _ => {
                return Err(Error::bad(
                    "expected block type text, image, audio or video",
                ));
            }
        };
        if fields.len() != 2 {
            return Err(Error::bad(
                "media block accepts only type and attachment_id",
            ));
        }
        let attachment_id = part["attachment_id"]
            .as_str()
            .and_then(|s| s.parse::<AttachmentId>().ok())
            .ok_or_else(|| Error::bad("media block requires a valid attachment_id"))?;
        Ok(Self::Media {
            kind,
            attachment_id,
        })
    }

    fn modality(&self, conn: &Connection) -> Result<&str> {
        match self {
            Self::Text { .. } => Ok("text"),
            Self::Media {
                kind,
                attachment_id: id,
            } => {
                let kind = kind.as_str();
                let mime: String = conn
                    .prepare_cached("SELECT mime_type FROM attachments WHERE id=?")?
                    .query_row([id], |row| row.get(0))
                    .optional()?
                    .ok_or_else(|| Error::bad(format!("attachment {id} does not exist")))?;
                check_mime(kind, *id, &mime)?;
                Ok(kind)
            }
        }
    }
}

fn check_mime(kind: &str, id: AttachmentId, mime: &str) -> Result<()> {
    if mime
        .split_once('/')
        .is_some_and(|(family, _)| family == kind)
    {
        Ok(())
    } else {
        Err(Error::bad(format!(
            "{kind} block references attachment {id} with MIME {mime}"
        )))
    }
}

fn require(modalities: &Value, kind: &str) -> Result<()> {
    if modalities
        .as_array()
        .is_some_and(|allowed| allowed.iter().any(|v| v == kind))
    {
        Ok(())
    } else {
        Err(Error::bad(format!("model does not support {kind} input")))
    }
}

fn at_part(index: usize, error: Error) -> Error {
    Error(error.0, format!("content[{index}]: {error}"))
}

pub fn validate_message(conn: &Connection, content: &Value, modalities: &Value) -> Result<()> {
    let parts = content
        .as_array()
        .filter(|parts| !parts.is_empty())
        .ok_or_else(|| Error::bad("content must be a nonempty array"))?;
    for (index, part) in parts.iter().enumerate() {
        ContentBlock::parse(part)
            .and_then(|block| require(modalities, block.modality(conn)?))
            .map_err(|e| at_part(index, e))?;
    }
    Ok(())
}

// Only inspect actual content slots. A tool name, argument, citation or JSON-looking
// result string can mention media without carrying any media to the next request.
pub fn check_parts(conn: &Connection, content: &Value, modalities: &Value) -> Result<()> {
    if content.is_string() {
        return require(modalities, "text");
    }
    let parts = content
        .as_array()
        .ok_or_else(|| Error::bad("expected text or content parts"))?;
    for (index, part) in parts.iter().enumerate() {
        let checked = match part["type"].as_str() {
            Some("input_text" | "output_text" | "refusal" | "reasoning_text" | "summary_text") => {
                require(modalities, "text")
            }
            Some("input_image" | "output_image") => require(modalities, "image"),
            Some("input_audio" | "output_audio") => require(modalities, "audio"),
            Some("input_file") => {
                let family = part["file_data"]
                    .as_str()
                    .and_then(|url| url.strip_prefix("data:"))
                    .and_then(|data| data.split_once('/'))
                    .map(|(family, _)| family);
                match family {
                    Some(kind @ ("audio" | "video" | "image" | "text")) => {
                        require(modalities, kind)
                    }
                    _ => Err(Error::bad(
                        "cannot determine input_file modality from its media type",
                    )),
                }
            }
            _ => ContentBlock::parse(part)
                .and_then(|block| require(modalities, block.modality(conn)?)),
        };
        checked.map_err(|e| at_part(index, e))?;
    }
    Ok(())
}

pub fn check_event(conn: &Connection, content: &Value, modalities: &Value) -> Result<()> {
    require(modalities, "text")?; // The host adds a text block identifying the source.
    check_parts(conn, content, modalities)
}

// Evidence is local to the SQLite connection. Savepoint/transaction rollbacks restore
// the prefix and its invalidations together with the rows that justified them.
// Only ordered context is reusable: queued inputs and unattached tool results are
// checked on every call, including items with IDs older than the latest context item.
pub fn init_validation_cache(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TEMP TABLE checked_input (
            sid TEXT UNIQUE, model TEXT, modalities TEXT, position INTEGER
         );
         CREATE TEMP TRIGGER trim_checked_input AFTER INSERT ON checked_input
         WHEN (SELECT count(*) FROM checked_input)>256 BEGIN
            DELETE FROM checked_input WHERE rowid=(SELECT min(rowid) FROM checked_input);
         END;
         CREATE TEMP TRIGGER invalidate_context_insert AFTER INSERT ON main.active_context BEGIN
            DELETE FROM checked_input WHERE sid=NEW.session_id AND position>=NEW.position;
         END;
         CREATE TEMP TRIGGER invalidate_context_replace AFTER UPDATE ON main.active_context BEGIN
            DELETE FROM checked_input WHERE sid IN (OLD.session_id,NEW.session_id);
         END;
         CREATE TEMP TRIGGER invalidate_context_delete AFTER DELETE ON main.active_context BEGIN
            DELETE FROM checked_input WHERE sid=OLD.session_id;
         END;
         CREATE TEMP TRIGGER invalidate_item_edit AFTER UPDATE OF kind,payload ON main.items BEGIN
            DELETE FROM checked_input WHERE sid=OLD.session_id;
         END;
         CREATE TEMP TRIGGER invalidate_withdrawal AFTER INSERT ON main.items WHEN NEW.kind='withdrawal' BEGIN
            DELETE FROM checked_input WHERE sid=NEW.session_id;
         END;
         CREATE TEMP TRIGGER invalidate_attachment_edit AFTER UPDATE ON main.attachments BEGIN
            DELETE FROM checked_input;
         END;
         CREATE TEMP TRIGGER invalidate_attachment_delete AFTER DELETE ON main.attachments BEGIN
            DELETE FROM checked_input;
         END;",
    )?;
    Ok(())
}

pub fn check_call_input(
    conn: &Connection,
    sid: SessionId,
    model: ModelId,
    modalities: &Value,
    end: i64,
) -> Result<()> {
    let prefix = conn
        .prepare_cached(
            "SELECT position FROM checked_input WHERE sid=? AND model=? AND modalities=? AND position<=?",
        )?
        .query_row((sid, model, modalities, end), |row| row.get(0))
        .optional()?
        .unwrap_or(0);
    check_session_after(conn, sid, modalities, prefix)?;
    conn.prepare_cached(
        "INSERT OR REPLACE INTO checked_input(sid,model,modalities,position) VALUES(?,?,?,?)",
    )?
    .execute((sid, model, modalities, end))?;
    Ok(())
}

pub fn check_session(conn: &Connection, sid: SessionId, modalities: &Value) -> Result<()> {
    check_session_after(conn, sid, modalities, 0)
}

fn check_session_after(conn: &Connection, sid: SessionId, modalities: &Value, position: i64) -> Result<()> {
    // Match context order first, then the tool results publish_tools will append in
    // call order, and finally the unwithdrawn queue take_pending will append by id.
    let queries = [
        "SELECT i.id,i.kind,i.payload FROM active_context c JOIN items i ON i.id=c.item_id
         WHERE c.session_id=?1 AND c.position>?2 AND i.kind IN ('user_message','codemode_event','assistant_message','reasoning','function_call_output','image_generation_call')
         ORDER BY c.position",
        "SELECT r.id,r.kind,r.payload FROM items c JOIN items r ON r.session_id=c.session_id AND r.target_id=c.id AND r.kind='function_call_output'
         WHERE c.session_id=? AND c.kind='function_call'
           AND NOT EXISTS (SELECT 1 FROM context_nodes n WHERE n.session_id=?1 AND n.item_id=c.id)
         ORDER BY c.id",
        "SELECT i.id,i.kind,i.payload FROM pending p JOIN items i ON i.id=p.id
         WHERE p.session_id=? ORDER BY p.id",
    ];
    for (index, query) in queries.into_iter().enumerate() {
        let mut stmt = conn.prepare_cached(query)?;
        let mut rows = if index == 0 { stmt.query((sid, position))? } else { stmt.query([sid])? };
        while let Some(row) = rows.next()? {
            let id: crate::id::ItemId = row.get(0)?;
            let kind: String = row.get(1)?;
            let payload: Value = row.get(2)?;
            let checked = match kind.as_str() {
                "user_message" => validate_message(conn, &payload["content"], modalities),
                "codemode_event" => check_event(conn, &payload["content"], modalities),
                "assistant_message" => check_parts(conn, &payload["content"], modalities),
                "function_call_output" => check_parts(conn, &payload["output"], modalities),
                "reasoning" => ["summary", "content"]
                    .iter()
                    .filter_map(|key| payload.get(*key))
                    .filter(|v| !v.is_null())
                    .try_for_each(|parts| check_parts(conn, parts, modalities)),
                "image_generation_call" if payload["result"].is_string() => {
                    require(modalities, "image")
                }
                _ => Ok(()),
            };
            if let Err(err) = checked {
                let status = if err.0 == StatusCode::BAD_REQUEST {
                    StatusCode::CONFLICT
                } else {
                    err.0
                };
                return Err(Error(status, format!("item {id}: {err}")));
            }
        }
    }
    Ok(())
}

pub fn missing_session_context_attachments(
    conn: &Connection,
    sid: SessionId,
) -> Result<Vec<AttachmentId>> {
    let mut stmt = conn.prepare_cached(
        "SELECT i.kind,i.payload FROM items i
         WHERE i.session_id=?1
           AND i.kind IN ('user_message','codemode_event','function_call_output')
           AND (
             EXISTS (SELECT 1 FROM active_context c WHERE c.session_id=?1 AND c.item_id=i.id)
             OR EXISTS (SELECT 1 FROM pending p WHERE p.session_id=?1 AND p.id=i.id)
           )
         ORDER BY i.id",
    )?;
    let mut rows = stmt.query([sid])?;
    let mut ids = std::collections::BTreeSet::new();
    while let Some(row) = rows.next()? {
        let kind: String = row.get(0)?;
        let payload: Value = row.get(1)?;
        let parts = match kind.as_str() {
            "user_message" | "codemode_event" => payload.get("content").and_then(Value::as_array),
            "function_call_output" => payload.get("output").and_then(Value::as_array),
            _ => None,
        };
        if let Some(parts) = parts {
            for part in parts {
                if let Some(raw) = part.get("attachment_id").and_then(Value::as_str)
                    && let Ok(att_id) = raw.parse::<AttachmentId>()
                {
                    ids.insert(att_id);
                }
            }
        }
    }
    drop(rows);
    drop(stmt);
    let mut check = conn.prepare_cached("SELECT data IS NULL FROM attachments WHERE id=?")?;
    let mut missing = Vec::new();
    for id in ids {
        if check.query_row([id], |r| r.get::<_, bool>(0)).optional()? == Some(true) {
            missing.push(id);
        }
    }
    Ok(missing)
}

pub fn collect_content_attachment_meta(
    conn: &Connection,
    content: &Value,
) -> Result<Vec<crate::cluster::ledger::AttachmentRef>> {
    let Some(parts) = content.as_array() else {
        return Ok(Vec::new());
    };
    let mut ids = std::collections::BTreeSet::new();
    for part in parts {
        if let Some(raw) = part.get("attachment_id").and_then(Value::as_str)
            && let Ok(att_id) = raw.parse::<AttachmentId>()
        {
            ids.insert(att_id);
        }
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare_cached("SELECT mime_type, content_hash, byte_len FROM attachments WHERE id=?")?;
    let mut refs = Vec::new();
    for id in ids {
        if let Some((mime_type, hash_blob, byte_len)) = stmt
            .query_row([id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .optional()?
        {
            let content_hash: [u8; 32] = hash_blob
                .try_into()
                .map_err(|_| Error::internal("invalid stored attachment hash length"))?;
            refs.push(crate::cluster::ledger::AttachmentRef {
                id,
                mime_type,
                content_hash,
                byte_len,
            });
        }
    }
    Ok(refs)
}
