use crate::{
    codemode::{Reply, TASK_BYTES},
    media::{MAX_ATTACHMENT_BYTES, attachment_mime},
};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use rmcp::model::{
    CallToolResult, ContentBlock, GetPromptResult, ReadResourceResult, ResourceContents,
};
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

    fn media(&mut self, kind: &str, mime: &str, data: String) -> Result<Value, String> {
        if !attachment_mime(mime).is_ok_and(|m| m.type_().as_str() == kind) {
            return Err(format!("invalid MIME type for MCP {kind} content"));
        }
        self.decode(kind, mime, data)
    }

    fn decode(&mut self, kind: &str, mime: &str, data: String) -> Result<Value, String> {
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

    fn resource(&mut self, resource: ResourceContents) -> Result<Value, String> {
        let (uri, mime_type, block) = match resource {
            ResourceContents::TextResourceContents {
                uri,
                mime_type,
                text,
                ..
            } => (uri, mime_type, json!({"type":"text","text":text})),
            ResourceContents::BlobResourceContents {
                uri,
                mime_type,
                blob,
                ..
            } => {
                let mime = mime_type.as_deref().unwrap_or("application/octet-stream");
                let parsed = attachment_mime(mime).ok().filter(|_| !blob.is_empty());
                let kind = parsed.as_ref().map_or("blob", |m| m.type_().as_str());
                let content = self.decode(kind, mime, blob)?;
                (uri, mime_type, content)
            }
            _ => return Err("unsupported MCP resource content".into()),
        };
        let mut entry = json!({"uri":uri});
        entry["content"] = block;
        if let Some(mime) = mime_type {
            entry["mimeType"] = Value::String(mime);
        }
        Ok(entry)
    }

    fn block(&mut self, block: ContentBlock) -> Result<Value, String> {
        Ok(match block {
            ContentBlock::Text(t) => json!({"type":"text","text":t.text}),
            ContentBlock::Image(i) => self.media("image", &i.mime_type, i.data)?,
            ContentBlock::Audio(a) => self.media("audio", &a.mime_type, a.data)?,
            ContentBlock::Resource(embedded) => {
                let mut block = json!({"type":"resource"});
                block["resource"] = self.resource(embedded.resource)?;
                block
            }
            link @ ContentBlock::ResourceLink(_) => {
                serde_json::to_value(link).map_err(|_| "invalid MCP resource link".to_owned())?
            }
            _ => json!({"type":"text","text":"Unsupported MCP content block"}),
        })
    }
}

pub(super) fn tool(result: CallToolResult) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let mut value = json!({"isError":result.is_error.unwrap_or(false)});
    value["content"] = Value::Array(
        result
            .content
            .into_iter()
            .map(|c| media.block(c))
            .collect::<Result<_, _>>()?,
    );
    if let Some(structured) = result.structured_content {
        value["structuredContent"] = structured;
    }
    Ok(media.reply(value))
}

pub(super) fn resource(result: ReadResourceResult) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let mut value = json!({});
    value["contents"] = Value::Array(
        result
            .contents
            .into_iter()
            .map(|r| media.resource(r))
            .collect::<Result<_, _>>()?,
    );
    Ok(media.reply(value))
}

pub(super) fn prompt(result: GetPromptResult) -> Result<Reply, String> {
    let mut media = BinaryData::default();
    let mut messages = Vec::with_capacity(result.messages.len());
    for msg in result.messages {
        let mut entry = json!({"role":msg.role});
        entry["content"] = media.block(msg.content)?;
        messages.push(entry);
    }
    let mut value = json!({});
    value["messages"] = Value::Array(messages);
    if let Some(desc) = result.description {
        value["description"] = Value::String(desc);
    }
    Ok(media.reply(value))
}
