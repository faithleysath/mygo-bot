use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use rusqlite::{Connection, OptionalExtension, types::ValueRef};
use serde_json::{Value, json};

use crate::{
    App, Error, Result,
    id::{AttachmentId, SessionId},
};

pub(crate) const MAX_ATTACHMENT_BYTES: usize = 50 * 1024 * 1024;

pub fn routes() -> Router<App> {
    Router::new()
        .route("/attachments", post(upload))
        .layer(DefaultBodyLimit::max(MAX_ATTACHMENT_BYTES))
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

async fn upload(
    State(app): State<App>,
    headers: HeaderMap,
    data: Bytes,
) -> Result<(StatusCode, Json<Attachment>)> {
    let mut types = headers.get_all(header::CONTENT_TYPE).iter();
    let mime = attachment_mime(types.next().and_then(|v| v.to_str().ok()).unwrap_or(""))?;
    if types.next().is_some() || data.is_empty() {
        return Err(Error::bad(
            "expected one Content-Type and a nonempty attachment",
        ));
    }
    let saved = app
        .db
        .write(
            move |conn| {
                Ok(Attachment {
                    id: save_attachment(conn, &mime, &data)?,
                    mime_type: mime.to_string(),
                })
            },
            |_, saved| Ok(saved),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(saved)))
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
    let id = AttachmentId::new()?;
    conn.prepare_cached(
        "INSERT INTO attachments(id,mime_type,content_hash,byte_len,data) VALUES(?,?,?,?,?)",
    )?
    .execute((
        id,
        mime.as_ref(),
        hash.as_bytes().as_slice(),
        byte_len,
        data,
    ))?;
    Ok(id)
}

async fn download(State(app): State<App>, Path(id): Path<AttachmentId>) -> Result<Response> {
    app.cluster.ensure_attachment_data(id).await?;
    let (mime, data) = app
        .db
        .call(move |store| {
            Ok(store.conn.query_row(
                "SELECT mime_type,data FROM attachments WHERE id=? AND data IS NOT NULL",
                [id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )?)
        })
        .await?;
    Ok((
        [
            (header::CONTENT_TYPE, mime.as_str()),
            (header::CONTENT_DISPOSITION, "attachment"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        data,
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

    fn wire(self, conn: &Connection) -> Result<Value> {
        let (kind, id) = match self {
            Self::Text { text, .. } => return Ok(json!({"type":"input_text","text":text})),
            Self::Media {
                kind,
                attachment_id,
            } => (kind.as_str(), attachment_id),
        };
        let mut stmt = conn.prepare_cached("SELECT mime_type,data FROM attachments WHERE id=?")?;
        let mut rows = stmt.query([id])?;
        let row = rows
            .next()?
            .ok_or_else(|| Error::bad(format!("attachment {id} does not exist")))?;
        let mime: String = row.get(0)?;
        let data = match row.get_ref(1)? {
            ValueRef::Blob(data) => data,
            ValueRef::Null => {
                return Err(Error::conflict(format!(
                    "attachment {id} bytes are unavailable locally"
                )));
            }
            value => {
                return Err(rusqlite::Error::InvalidColumnType(
                    1,
                    "data".into(),
                    value.data_type(),
                )
                .into());
            }
        };
        check_mime(kind, id, &mime)?;
        let essence = mime.split(';').next().unwrap_or(&mime);
        let mut url = format!("data:{essence};base64,");
        BASE64_STANDARD.encode_string(data, &mut url);
        drop(rows);
        if kind == "image" {
            return Ok(json!({"type":"input_image","image_url":url,"detail":"auto"}));
        }
        // The endpoint must support this audio/video format as a Responses file input.
        let extension = match essence {
            "audio/mpeg" => "mp3",
            "audio/x-wav" | "audio/vnd.wave" => "wav",
            "audio/mp4" => "m4a",
            "video/quicktime" => "mov",
            other => other.split_once('/').map_or("bin", |(_, ext)| ext),
        };
        Ok(
            json!({"type":"input_file","filename":format!("attachment-{id}.{extension}"),"file_data":url}),
        )
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

pub fn check_session(conn: &Connection, sid: SessionId, modalities: &Value) -> Result<()> {
    // Match context order first, then the tool results publish_tools will append in
    // call order, and finally the unwithdrawn queue take_pending will append by id.
    let queries = [
        "SELECT i.id,i.kind,i.payload FROM context_items c JOIN items i ON i.id=c.item_id
         WHERE c.session_id=? AND i.kind IN ('user_message','codemode_event','assistant_message','reasoning','function_call_output','image_generation_call')
         ORDER BY c.position",
        "SELECT r.id,r.kind,r.payload FROM items c JOIN items r ON r.target_id=c.id AND r.kind='function_call_output'
         WHERE c.session_id=? AND c.kind='function_call'
           AND NOT EXISTS (SELECT 1 FROM context_items WHERE item_id=c.id)
         ORDER BY c.id",
        "SELECT i.id,i.kind,i.payload FROM pending p JOIN items i ON i.id=p.id
         WHERE p.session_id=? ORDER BY p.id",
    ];
    for query in queries {
        let mut stmt = conn.prepare_cached(query)?;
        let mut rows = stmt.query([sid])?;
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

fn collect_part_modalities(
    conn: &Connection,
    parts: &Value,
    set: &mut std::collections::BTreeSet<String>,
) -> Result<()> {
    if parts.is_string() {
        set.insert("text".to_owned());
        return Ok(());
    }
    let Some(arr) = parts.as_array() else {
        return Ok(());
    };
    for part in arr {
        match part["type"].as_str() {
            Some(
                "input_text" | "output_text" | "refusal" | "reasoning_text" | "summary_text"
                | "text",
            ) => {
                set.insert("text".to_owned());
            }
            Some("input_image" | "output_image" | "image") => {
                set.insert("image".to_owned());
            }
            Some("input_audio" | "output_audio" | "audio") => {
                set.insert("audio".to_owned());
            }
            Some("video") => {
                set.insert("video".to_owned());
            }
            Some("input_file") => {
                if let Some(family) = part["file_data"]
                    .as_str()
                    .and_then(|url| url.strip_prefix("data:"))
                    .and_then(|data| data.split_once('/'))
                    .map(|(f, _)| f)
                    && matches!(family, "audio" | "video" | "image" | "text")
                {
                    set.insert(family.to_owned());
                }
            }
            _ => {
                if let Ok(block) = ContentBlock::parse(part)
                    && let Ok(m) = block.modality(conn)
                {
                    set.insert(m.to_owned());
                }
            }
        }
    }
    Ok(())
}

pub fn item_required_modalities(
    conn: &Connection,
    kind: &str,
    payload: &Value,
    mods: &mut std::collections::BTreeSet<String>,
) -> Result<()> {
    match kind {
        "user_message" | "assistant_message" => {
            collect_part_modalities(conn, &payload["content"], mods)?;
        }
        "codemode_event" => {
            mods.insert("text".to_owned());
            collect_part_modalities(conn, &payload["content"], mods)?;
        }
        "function_call_output" => collect_part_modalities(conn, &payload["output"], mods)?,
        "reasoning" => {
            for key in ["summary", "content"] {
                if let Some(parts) = payload.get(key).filter(|v| !v.is_null()) {
                    collect_part_modalities(conn, parts, mods)?;
                }
            }
        }
        "image_generation_call" if payload["result"].is_string() => {
            mods.insert("image".to_owned());
        }
        _ => {}
    }
    Ok(())
}

pub fn session_required_modalities(conn: &Connection, sid: SessionId) -> Result<Vec<String>> {
    let mut mods = std::collections::BTreeSet::new();
    let queries = [
        "SELECT i.kind,i.payload FROM context_items c JOIN items i ON i.id=c.item_id
         WHERE c.session_id=? AND i.kind IN ('user_message','codemode_event','assistant_message','reasoning','function_call_output','image_generation_call')
         ORDER BY c.position",
        "SELECT r.kind,r.payload FROM items c JOIN items r ON r.target_id=c.id AND r.kind='function_call_output'
         WHERE c.session_id=? AND c.kind='function_call'
           AND NOT EXISTS (SELECT 1 FROM context_items WHERE item_id=c.id)
         ORDER BY c.id",
        "SELECT i.kind,i.payload FROM pending p JOIN items i ON i.id=p.id
         WHERE p.session_id=? ORDER BY p.id",
    ];
    for query in queries {
        let mut stmt = conn.prepare_cached(query)?;
        let mut rows = stmt.query([sid])?;
        while let Some(row) = rows.next()? {
            let kind: String = row.get(0)?;
            let payload: Value = row.get(1)?;
            item_required_modalities(conn, &kind, &payload, &mut mods)?;
        }
    }
    Ok(mods.into_iter().collect())
}

pub fn wire_message(conn: &Connection, kind: &str, mut payload: Value) -> Result<Value> {
    if kind == "codemode_event" {
        let source = format!(
            "Codemode {} event from {} (sequence {}).",
            payload["event"]
                .as_str()
                .ok_or_else(|| Error::internal("missing event kind"))?,
            payload["task_id"]
                .as_str()
                .ok_or_else(|| Error::internal("missing task ID"))?,
            payload["sequence"]
                .as_i64()
                .ok_or_else(|| Error::internal("missing event sequence"))?
        );
        let mut content = payload["content"].take();
        content
            .as_array_mut()
            .ok_or_else(|| Error::internal("missing event content"))?
            .insert(0, json!({"type":"text","text":source}));
        payload = json!({"role":"user","content":content});
    }
    let is_tool = kind == "function_call_output";
    let field = if is_tool {
        "output"
    } else if payload["role"] == "user" {
        "content"
    } else {
        return Ok(payload);
    };
    if is_tool && payload[field].is_string() {
        return Ok(payload);
    }
    let parts = payload[field]
        .as_array_mut()
        .ok_or_else(|| Error::bad("expected content parts"))?;
    for (index, part) in parts.iter_mut().enumerate() {
        if is_tool
            && !matches!(
                part["type"].as_str(),
                Some("text" | "image" | "audio" | "video")
            )
        {
            continue;
        }
        *part = ContentBlock::parse(part)
            .and_then(|block| block.wire(conn))
            .map_err(|e| at_part(index, e))?;
    }
    Ok(payload)
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
             EXISTS (SELECT 1 FROM context_items c WHERE c.session_id=?1 AND c.item_id=i.id)
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
    let mut stmt = conn.prepare_cached(
        "SELECT mime_type, content_hash, byte_len FROM attachments WHERE id=?",
    )?;
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
