use super::{Client, content};
use crate::codemode::Reply;
use rmcp::{
    model::{
        CallToolRequestParams, GetPromptRequestParams, PaginatedRequestParams,
        ReadResourceRequestParams,
    },
    service::ServiceError,
};
use serde_json::{Map, Value, json};

#[derive(Debug)]
pub(super) enum AttemptError {
    Service(ServiceError),
    Local(String),
}

impl From<ServiceError> for AttemptError {
    fn from(error: ServiceError) -> Self {
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

fn page(args: &Value) -> Result<Option<PaginatedRequestParams>, String> {
    match &args["cursor"] {
        Value::Null => Ok(None),
        Value::String(cursor) => Ok(Some(
            PaginatedRequestParams::default().with_cursor(Some(cursor.clone())),
        )),
        _ => Err("expected a string MCP cursor".into()),
    }
}

/// Exactly one peer operation. These methods cancel an outstanding request on drop.
pub(super) async fn attempt(
    client: &Client,
    op: &str,
    args: &Value,
) -> Result<Reply, AttemptError> {
    let peer = client.peer();
    let reply = match op {
        "mcp.call" => {
            let params = CallToolRequestParams::new(string(args, "tool")?.to_owned())
                .with_arguments(arguments(args)?);
            let response = peer.call_tool(params).await?;
            content::tool(response).map_err(|reason| format!(
                "MCP tool replied, but its result could not be read: {reason}; check remote state before repeating a write"
            ))?
        }
        "mcp.resources" => {
            let page = peer.list_resources(page(args)?).await?;
            Reply::value(
                json!({"resources":page.resources,"nextCursor":checked_cursor(page.next_cursor)?}),
            )
        }
        "mcp.resourceTemplates" => {
            let page = peer.list_resource_templates(page(args)?).await?;
            Reply::value(
                json!({"resourceTemplates":page.resource_templates,"nextCursor":checked_cursor(page.next_cursor)?}),
            )
        }
        "mcp.readResource" => {
            let params = ReadResourceRequestParams::new(string(args, "uri")?);
            content::resource(peer.read_resource(params).await?)?
        }
        "mcp.prompts" => {
            let page = peer.list_prompts(page(args)?).await?;
            Reply::value(
                json!({"prompts":page.prompts,"nextCursor":checked_cursor(page.next_cursor)?}),
            )
        }
        "mcp.getPrompt" => {
            let mut params = GetPromptRequestParams::new(string(args, "name")?);
            if args.get("args").is_some() {
                let args = arguments(args)?;
                if args.values().any(|v| !v.is_string()) {
                    return Err(AttemptError::Local(
                        "MCP prompt argument values must be strings".into(),
                    ));
                }
                params = params.with_arguments(args);
            }
            content::prompt(peer.get_prompt(params).await?)?
        }
        _ => return Err(AttemptError::Local("unknown MCP operation".into())),
    };
    Ok(reply)
}
