use crate::{
    codemode::{Reply, TASK_BYTES},
    media::{MAX_ATTACHMENT_BYTES, attachment_mime},
};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use serde_json::{Value, json};

// Only these content positions refer to the binary buffer. Remote JSON (including
// structuredContent) never controls which ranges the VM turns into typed arrays.
#[derive(Default)]
struct BinaryData(Option<Vec<u8>>);

impl BinaryData {
    fn reply(self, value: Value) -> Reply {
        Reply {
            value,
            data: self.0,
        }
    }

    fn media(&mut self, kind: &str, mime: &str, data: &str) -> Result<Value, String> {
        if !attachment_mime(mime).is_ok_and(|m| m.type_().as_str() == kind) {
            return Err(format!("invalid MIME type for MCP {kind} content"));
        }
        self.decode(kind, mime, data)
    }

    fn decode(&mut self, kind: &str, mime: &str, data: &str) -> Result<Value, String> {
        let bytes = self.0.get_or_insert_with(Vec::new);
        let start = bytes.len();
        let limit = MAX_ATTACHMENT_BYTES.min(TASK_BYTES - start);
        // Padded base64 can round a decoded size up by two bytes. Check the exact
        // result too, so the last partial group cannot slip over the budget.
        if kind != "blob" && data.is_empty() {
            return Err("MCP media content is empty".into());
        }
        if data.len() > base64::encoded_len(limit, true).unwrap() {
            return Err("MCP binary content exceeds the attachment budget".into());
        }
        BASE64_STANDARD
            .decode_vec(data, bytes)
            .map_err(|_| format!("invalid base64 in MCP {kind} content"))?;
        let end = bytes.len();
        if end - start > limit {
            return Err("MCP binary content exceeds the attachment budget".into());
        }
        Ok(json!({"type":kind,"mime_type":mime,"data":[start,end]}))
    }

    fn resource(&mut self, resource: &Value) -> Result<Value, String> {
        let obj = resource
            .as_object()
            .ok_or_else(|| "unsupported MCP resource content".to_owned())?;
        let uri = obj
            .get("uri")
            .and_then(Value::as_str)
            .ok_or_else(|| "unsupported MCP resource content".to_owned())?;
        let mime_type = obj.get("mimeType").and_then(Value::as_str);
        let block = if let Some(text) = obj.get("text").and_then(Value::as_str) {
            json!({"type":"text","text":text})
        } else if let Some(blob) = obj.get("blob").and_then(Value::as_str) {
            let mime = mime_type.unwrap_or("application/octet-stream");
            let parsed = attachment_mime(mime).ok().filter(|_| !blob.is_empty());
            let kind = parsed.as_ref().map_or("blob", |m| m.type_().as_str());
            self.decode(kind, mime, blob)?
        } else {
            return Err("unsupported MCP resource content".into());
        };
        let mut entry = json!({"uri":uri,"content":block});
        if let Some(mime) = mime_type {
            entry["mimeType"] = Value::String(mime.to_owned());
        }
        Ok(entry)
    }

    fn block(&mut self, block: &Value) -> Result<Value, String> {
        let Some(kind) = block.get("type").and_then(Value::as_str) else {
            return Ok(json!({"type":"text","text":"Unsupported MCP content block"}));
        };
        Ok(match kind {
            "text" => {
                let text = block
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "invalid MCP text content".to_owned())?;
                json!({"type":"text","text":text})
            }
            "image" | "audio" => {
                let mime = block
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("invalid MIME type for MCP {kind} content"))?;
                let data = block
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("invalid base64 in MCP {kind} content"))?;
                self.media(kind, mime, data)?
            }
            "resource" => {
                let embedded = block
                    .get("resource")
                    .ok_or_else(|| "unsupported MCP resource content".to_owned())?;
                json!({"type":"resource","resource":self.resource(embedded)?})
            }
            "resource_link" => {
                let Some(obj) = block.as_object() else {
                    return Err("invalid MCP resource link".into());
                };
                if !obj.get("uri").is_some_and(Value::is_string)
                    || !obj.get("name").is_some_and(Value::is_string)
                {
                    return Err("invalid MCP resource link".into());
                }
                block.clone()
            }
            _ => json!({"type":"text","text":"Unsupported MCP content block"}),
        })
    }
}

pub(super) fn tool(result: Value) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let is_error = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let items = match result.get("content") {
        None | Some(Value::Null) => &[][..],
        Some(Value::Array(arr)) => arr.as_slice(),
        _ => return Err("invalid MCP tool content".into()),
    };
    let mut content = Vec::with_capacity(items.len());
    for item in items {
        content.push(media.block(item)?);
    }
    let mut value = json!({"isError":is_error,"content":content});
    if let Some(structured) = result.get("structuredContent").filter(|v| !v.is_null()) {
        value["structuredContent"] = structured.clone();
    }
    Ok(media.reply(value))
}

pub(super) fn resource(result: Value) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let items = result
        .get("contents")
        .and_then(Value::as_array)
        .ok_or_else(|| "invalid MCP resource response".to_owned())?;
    let mut contents = Vec::with_capacity(items.len());
    for item in items {
        contents.push(media.resource(item)?);
    }
    Ok(media.reply(json!({"contents":contents})))
}

pub(super) fn prompt(result: Value) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let items = result
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "invalid MCP prompt response".to_owned())?;
    let mut messages = Vec::with_capacity(items.len());
    for msg in items {
        let role = msg
            .get("role")
            .and_then(Value::as_str)
            .filter(|r| matches!(*r, "user" | "assistant"))
            .ok_or_else(|| "invalid MCP prompt message".to_owned())?;
        let block = msg
            .get("content")
            .ok_or_else(|| "invalid MCP prompt message".to_owned())?;
        messages.push(json!({"role":role,"content":media.block(block)?}));
    }
    let mut value = json!({"messages":messages});
    if let Some(desc) = result.get("description").and_then(Value::as_str) {
        value["description"] = Value::String(desc.to_owned());
    }
    Ok(media.reply(value))
}
