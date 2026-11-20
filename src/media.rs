use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};

use crate::{App, Error, Result};

pub fn routes() -> Router<App> {
    Router::new()
        .route(
            "/attachments",
            post(upload).layer(DefaultBodyLimit::max(50 * 1024 * 1024)),
        )
        .route("/attachments/{id}", get(download))
}

async fn upload(
    State(app): State<App>,
    headers: HeaderMap,
    data: Bytes,
) -> Result<(StatusCode, Json<Value>)> {
    let mut types = headers.get_all(header::CONTENT_TYPE).iter();
    let mime: mime::Mime = types
        .next()
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .filter(|m: &mime::Mime| {
            !m.essence_str().contains('*')
                && matches!(m.type_().as_str(), "image" | "audio" | "video")
        })
        .ok_or_else(|| Error::bad("expected a concrete image, audio or video Content-Type"))?;
    if types.next().is_some() || data.is_empty() {
        return Err(Error::bad(
            "expected one Content-Type and a nonempty attachment",
        ));
    }
    let saved = app
        .db
        .write(
            move |conn| {
                conn.execute(
                    "INSERT INTO attachments(mime_type,data) VALUES(?,?)",
                    (mime.as_ref(), data.as_ref()),
                )?;
                Ok(json!({"id":conn.last_insert_rowid(),"mime_type":mime.as_ref()}))
            },
            |_, saved| Ok(saved),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(saved)))
}

async fn download(State(app): State<App>, Path(id): Path<i64>) -> Result<Response> {
    let (mime, data) = app
        .db
        .call(move |store| {
            Ok(store.conn.query_row(
                "SELECT mime_type,data FROM attachments WHERE id=?",
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

enum Block<'a> {
    Text(&'a str),
    Media { kind: &'a str, id: i64 },
}

impl<'a> Block<'a> {
    fn parse(part: &'a Value) -> Result<Self> {
        let fields = part
            .as_object()
            .ok_or_else(|| Error::bad("expected a content block"))?;
        match part["type"].as_str() {
            Some("text") if fields.len() == 2 => part["text"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .map(Self::Text)
                .ok_or_else(|| Error::bad("text block requires nonempty text")),
            Some(kind @ ("image" | "audio" | "video")) if fields.len() == 2 => {
                let id = part["attachment_id"]
                    .as_i64()
                    .filter(|&id| id > 0)
                    .ok_or_else(|| {
                        Error::bad("media block requires a positive integer attachment_id")
                    })?;
                Ok(Self::Media { kind, id })
            }
            Some("text") => Err(Error::bad("text block accepts only type and text")),
            Some("image" | "audio" | "video") => Err(Error::bad(
                "media block accepts only type and attachment_id",
            )),
            _ => Err(Error::bad(
                "expected block type text, image, audio or video",
            )),
        }
    }

    fn modality(&self, conn: &Connection) -> Result<&str> {
        match self {
            Self::Text(_) => Ok("text"),
            Self::Media { kind, id } => {
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
            Self::Text(text) => return Ok(json!({"type":"input_text","text":text})),
            Self::Media { kind, id } => (kind, id),
        };
        let (mime, data): (String, Vec<u8>) = conn
            .prepare_cached("SELECT mime_type,data FROM attachments WHERE id=?")?
            .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?
            .ok_or_else(|| Error::bad(format!("attachment {id} does not exist")))?;
        check_mime(kind, id, &mime)?;
        let essence = mime.split(';').next().unwrap_or(&mime);
        let mut url = format!("data:{essence};base64,");
        BASE64_STANDARD.encode_string(data, &mut url);
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

fn check_mime(kind: &str, id: i64, mime: &str) -> Result<()> {
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
        Block::parse(part)
            .and_then(|block| require(modalities, block.modality(conn)?))
            .map_err(|e| at_part(index, e))?;
    }
    Ok(())
}

// Only inspect actual content slots. A tool name, argument, citation or JSON-looking
// result string can mention media without carrying any media to the next request.
fn check_parts(conn: &Connection, content: &Value, modalities: &Value) -> Result<()> {
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
            _ => Block::parse(part).and_then(|block| require(modalities, block.modality(conn)?)),
        };
        checked.map_err(|e| at_part(index, e))?;
    }
    Ok(())
}

pub fn check_session(conn: &Connection, sid: i64, modalities: &Value) -> Result<()> {
    // Match context order first, then the tool results publish_tools will append in
    // call order, and finally the unwithdrawn queue take_pending will append by id.
    let queries = [
        "SELECT i.id,i.kind,i.payload FROM context_items c JOIN items i ON i.id=c.item_id
         WHERE c.session_id=? AND i.kind IN ('user_message','assistant_message','reasoning','function_call_output','image_generation_call')
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
            let id: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let payload: Value = row.get(2)?;
            let checked = match kind.as_str() {
                "user_message" => validate_message(conn, &payload["content"], modalities),
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

pub fn wire_message(conn: &Connection, mut payload: Value) -> Result<Value> {
    let is_tool = payload["type"] == "function_call_output";
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
        *part = Block::parse(part)
            .and_then(|block| block.wire(conn))
            .map_err(|e| at_part(index, e))?;
    }
    Ok(payload)
}
