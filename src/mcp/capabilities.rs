use super::{Client, client::McpError, content};
use crate::codemode::Reply;
use serde_json::{Map, Value, json};

#[derive(Debug)]
pub(super) enum AttemptError {
    Service(McpError),
    Local(String),
}

impl From<McpError> for AttemptError {
    fn from(error: McpError) -> Self {
        Self::Service(error)
    }
}

impl From<String> for AttemptError {
    fn from(error: String) -> Self {
        Self::Local(error)
    }
}

fn string<'a>(args: &'a Value, field: &str) -> Result<&'a str, String> {
    args[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("expected a nonempty MCP {field}"))
}

fn arguments(args: &Value) -> Result<Map<String, Value>, String> {
    args["args"]
        .as_object()
        .cloned()
        .ok_or_else(|| "expected an MCP arguments object".into())
}

// A cursor must fit back into the VM host argument envelope (including JSON escaping).
// This also bounds the set of seen cursors while filtering multiple tools/list pages.
pub(super) fn checked_cursor(cursor: Option<String>) -> Result<Option<String>, String> {
    if cursor.as_ref().is_some_and(|s| s.len() > 16 * 1024) {
        return Err("MCP pagination cursor exceeds 16 KiB".into());
    }
    Ok(cursor)
}

pub(super) fn next_cursor(page: &Value) -> Result<Option<String>, AttemptError> {
    let cursor = match page.get("nextCursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        _ => return Err(McpError::InvalidResponse.into()),
    };
    Ok(checked_cursor(cursor)?)
}

fn page_cursor(args: &Value) -> Result<Option<String>, String> {
    match &args["cursor"] {
        Value::Null => Ok(None),
        Value::String(cursor) => Ok(Some(cursor.clone())),
        _ => Err("expected a string MCP cursor".into()),
    }
}

/// Exactly one peer operation. Dropping the future cancels any stdio waiter.
pub(super) async fn attempt(
    client: &Client,
    op: &str,
    args: &Value,
) -> Result<Reply, AttemptError> {
    let reply = match op {
        "mcp.call" => {
            let response = client.call_tool(string(args, "tool")?, arguments(args)?).await?;
            content::tool(response).map_err(|reason| {
                format!(
                    "MCP tool replied, but its result could not be read: {reason}; check remote state before repeating a write"
                )
            })?
        }
        "mcp.resources" => {
            let mut page = client.list_resources(page_cursor(args)?).await?;
            let next = next_cursor(&page)?;
            let resources = page
                .get_mut("resources")
                .filter(|v| v.is_array())
                .map(Value::take)
                .ok_or(McpError::InvalidResponse)?;
            Reply::value(json!({"resources":resources,"nextCursor":next}))
        }
        "mcp.resourceTemplates" => {
            let mut page = client.list_resource_templates(page_cursor(args)?).await?;
            let next = next_cursor(&page)?;
            let templates = page
                .get_mut("resourceTemplates")
                .filter(|v| v.is_array())
                .map(Value::take)
                .ok_or(McpError::InvalidResponse)?;
            Reply::value(json!({"resourceTemplates":templates,"nextCursor":next}))
        }
        "mcp.readResource" => content::resource(client.read_resource(string(args, "uri")?).await?)?,
        "mcp.prompts" => {
            let mut page = client.list_prompts(page_cursor(args)?).await?;
            let next = next_cursor(&page)?;
            let prompts = page
                .get_mut("prompts")
                .filter(|v| v.is_array())
                .map(Value::take)
                .ok_or(McpError::InvalidResponse)?;
            Reply::value(json!({"prompts":prompts,"nextCursor":next}))
        }
        "mcp.getPrompt" => {
            let prompt_args = if args.get("args").is_some() {
                let parsed = arguments(args)?;
                if parsed.values().any(|v| !v.is_string()) {
                    return Err(AttemptError::Local(
                        "MCP prompt argument values must be strings".into(),
                    ));
                }
                Some(parsed)
            } else {
                None
            };
            content::prompt(client.get_prompt(string(args, "name")?, prompt_args).await?)?
        }
        _ => return Err(AttemptError::Local("unknown MCP operation".into())),
    };
    Ok(reply)
}
