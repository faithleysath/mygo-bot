use axum::body::Bytes;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use futures_util::{StreamExt, pin_mut};
use rusqlite::Connection;
use serde_json::{Value, json};

use super::{AttachmentBlob, ContentBlock, at_part, check_mime};
use crate::{Error, Result, id::SessionId, store::Db};

// Offsets refer to the JSON before inserting any base64. They are recorded while
// serializing real content fields, so arbitrary strings can never act as markers.
struct Page {
    json: Vec<u8>,
    blobs: Vec<(usize, AttachmentBlob)>,
    position: i64,
}

impl Page {
    fn value(&mut self, value: &impl serde::Serialize) -> Result<()> {
        serde_json::to_writer(&mut self.json, value).map_err(|e| Error::internal(e.to_string()))
    }

    fn part(&mut self, conn: &Connection, part: &Value) -> Result<()> {
        let (kind, id) = match ContentBlock::parse(part)? {
            ContentBlock::Text { text, .. } => {
                return self.value(&json!({"type":"input_text","text":text}));
            }
            ContentBlock::Media {
                kind,
                attachment_id,
            } => (kind.as_str(), attachment_id),
        };
        let (mime, blob) = AttachmentBlob::lookup(conn, id).map_err(|err| {
            if err.0 == axum::http::StatusCode::NOT_FOUND {
                Error::bad(format!("attachment {id} does not exist"))
            } else {
                err
            }
        })?;
        check_mime(kind, id, &mime)?;
        let essence = mime.split(';').next().unwrap_or(&mime);
        let url = format!("data:{essence};base64,");
        let (url_key, block) = if kind == "image" {
            (
                "image_url",
                json!({"type":"input_image","image_url":url,"detail":"auto"}),
            )
        } else {
            let extension = match essence {
                "audio/mpeg" => "mp3",
                "audio/x-wav" | "audio/vnd.wave" => "wav",
                "audio/mp4" => "m4a",
                "video/quicktime" => "mov",
                other => other.split_once('/').map_or("bin", |(_, ext)| ext),
            };
            (
                "file_data",
                json!({"type":"input_file","filename":format!("attachment-{id}.{extension}"),"file_data":url}),
            )
        };
        self.json.push(b'{');
        for (i, (key, value)) in block.as_object().unwrap().iter().enumerate() {
            if i != 0 {
                self.json.push(b',');
            }
            self.value(key)?;
            self.json.push(b':');
            self.value(value)?;
            if key == url_key {
                self.blobs.push((self.json.len() - 1, blob)); // immediately before the closing quote
            }
        }
        self.json.push(b'}');
        Ok(())
    }

    fn message(&mut self, conn: &Connection, kind: &str, mut payload: Value) -> Result<()> {
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
        let tool = kind == "function_call_output";
        let field = if tool { "output" } else { "content" };
        if tool && payload[field].is_string() || !tool && payload["role"] != "user" {
            return self.value(&payload);
        }
        let parts = payload[field]
            .as_array()
            .ok_or_else(|| Error::bad("expected content parts"))?;
        self.json.push(b'{');
        for (i, (key, value)) in payload.as_object().unwrap().iter().enumerate() {
            if i != 0 {
                self.json.push(b',');
            }
            self.value(key)?;
            self.json.push(b':');
            if key != field {
                self.value(value)?;
                continue;
            }
            self.json.push(b'[');
            for (index, part) in parts.iter().enumerate() {
                if index != 0 {
                    self.json.push(b',');
                }
                if tool
                    && !matches!(
                        part["type"].as_str(),
                        Some("text" | "image" | "audio" | "video")
                    )
                {
                    self.value(part)?;
                } else {
                    self.part(conn, part).map_err(|e| at_part(index, e))?;
                }
            }
            self.json.push(b']');
        }
        self.json.push(b'}');
        Ok(())
    }

    fn read(
        conn: &Connection,
        sid: SessionId,
        after: i64,
        end: i64,
        prefix: Vec<u8>,
    ) -> Result<Self> {
        let mut page = Self {
            json: prefix,
            blobs: Vec::new(),
            position: after,
        };
        let mut stmt = conn.prepare_cached(
            "SELECT c.position,i.payload,i.kind FROM active_context c JOIN items i ON i.id=c.item_id
             WHERE c.session_id=? AND c.position>? AND c.position<=? ORDER BY c.position LIMIT 256",
        )?;
        let mut rows = stmt.query((sid, after, end))?;
        while let Some(row) = rows.next()? {
            page.json.push(b','); // follows the developer message, even in the first page
            match row.get_ref(2)?.as_str().ok() {
                Some(kind @ ("user_message" | "function_call_output" | "codemode_event")) => {
                    page.message(conn, kind, row.get(1)?)?;
                }
                _ => page.json.extend_from_slice(
                    row.get_ref(1)?
                        .as_bytes()
                        .map_err(|e| Error::internal(e.to_string()))?,
                ),
            }
            page.position = row.get(0)?;
            if page.json.len() >= 64 * 1024 || !page.blobs.is_empty() {
                break;
            }
        }
        if page.position == after {
            return Err(Error::internal("missing row in saved history"));
        }
        if page.position == end {
            page.json.extend_from_slice(b"]}");
        }
        Ok(page)
    }
}

pub(crate) async fn model_input(
    db: Db,
    sid: SessionId,
    end: i64,
    mut prefix: Vec<u8>,
) -> Result<reqwest::Body> {
    if end == 0 {
        prefix.extend_from_slice(b"]}");
        return Ok(prefix.into());
    }
    let mut page = db
        .call(move |s| Page::read(&s.conn, sid, 0, end, prefix))
        .await?;
    if page.position == end && page.blobs.is_empty() {
        return Ok(page.json.into());
    }
    let stream = async_stream::try_stream! {
        loop {
            let mut json = Bytes::from(page.json);
            let mut start = 0;
            for (at, blob) in page.blobs {
                yield json.split_to(at - start);
                start = at;
                let chunks = blob.chunks(db.clone());
                pin_mut!(chunks);
                while let Some(bytes) = chunks.next().await {
                    yield Bytes::from(BASE64_STANDARD.encode(bytes?));
                }
            }
            yield json;
            let after = page.position;
            if after == end { break; }
            page = db.call(move |s| Page::read(&s.conn, sid, after, end, Vec::new())).await?;
        }
    };
    // Give try_stream's error a concrete type for reqwest's boxed body.
    Ok(reqwest::Body::wrap_stream(stream.map(|r: Result<Bytes>| r)))
}
