use super::{
    Client,
    client::{KNOWN_VERSIONS, McpError},
    config, connect,
};
use crate::{App, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{Instant, timeout};

pub(crate) fn routes() -> Router<App> {
    Router::new().route("/mcp/servers/{id}/test", post(test_server))
}

#[derive(Serialize)]
struct ListSummary {
    operation: &'static str,
    count: usize,
    has_more: bool,
}

#[derive(Serialize)]
struct Probe {
    id: i64,
    revision: i64,
    ok: bool,
    stage: &'static str,
    elapsed_ms: u64,
    protocol_version: Option<String>,
    lists: Vec<ListSummary>,
    error: Option<String>,
}

impl Probe {
    fn record_page(&mut self, page: &Value, field: &str) -> std::result::Result<(), String> {
        let count = page
            .get(field)
            .and_then(Value::as_array)
            .map(Vec::len)
            .ok_or_else(|| list_error(McpError::InvalidResponse))?;
        let has_more = page
            .get("nextCursor")
            .is_some_and(|v| !v.is_null());
        self.lists.push(ListSummary {
            operation: self.stage,
            count,
            has_more,
        });
        Ok(())
    }

    async fn list(&mut self, client: &Client) -> std::result::Result<(), String> {
        let info = client.peer_info();
        self.protocol_version = KNOWN_VERSIONS
            .contains(&info.protocol_version.as_str())
            .then(|| info.protocol_version.clone());
        if info.capabilities.tools.is_some() {
            self.stage = "tools/list";
            let page = client.list_tools(None).await.map_err(list_error)?;
            self.record_page(&page, "tools")?;
        }
        if info.capabilities.resources {
            self.stage = "resources/list";
            let page = client.list_resources(None).await.map_err(list_error)?;
            self.record_page(&page, "resources")?;
            self.stage = "resources/templates/list";
            match client.list_resource_templates(None).await {
                Ok(page) => self.record_page(&page, "resourceTemplates")?,
                // MCP has no separate capability flag for template discovery.
                Err(McpError::Protocol { code: -32601, .. }) => (),
                Err(e) => return Err(list_error(e)),
            }
        }
        if info.capabilities.prompts.is_some() {
            self.stage = "prompts/list";
            let page = client.list_prompts(None).await.map_err(list_error)?;
            self.record_page(&page, "prompts")?;
        }
        Ok(())
    }
}

fn list_error(error: McpError) -> String {
    if error.is_auth_required() {
        return "MCP authentication required".into();
    }
    if let McpError::Protocol { code, .. } = error {
        return format!("MCP protocol error ({code})");
    }
    "MCP catalog request failed; check the connection and server capabilities".into()
}

async fn test_server(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Probe>> {
    let endpoint = app
        .db
        .call(move |s| config::endpoint_by_id(&s.conn, id))
        .await?;
    let _permit = app.mcp_probes.try_acquire().map_err(|_| {
        crate::Error(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "MCP probe concurrency limit reached".into(),
        )
    })?;
    let duration = Duration::from_millis(endpoint.timeout_ms.into());
    let mut probe = Probe {
        id,
        revision: endpoint.revision,
        ok: false,
        stage: "handshake",
        elapsed_ms: 0,
        protocol_version: None,
        lists: Vec::new(),
        error: None,
    };
    let start = Instant::now();
    let mut client = None;
    let result = tokio::select! {
        biased;
        _ = app.stopping.cancelled() => Err("MCP probe interrupted by shutdown".into()),
        result = timeout(duration, async {
            let client = client.insert(connect(&app, endpoint).await?);
            probe.list(client).await
        }) => result.unwrap_or_else(|_| Err("MCP probe timed out".into())),
    };
    probe.elapsed_ms = start.elapsed().as_millis() as u64;
    match result {
        Ok(()) => {
            probe.ok = true;
            probe.stage = "complete";
        }
        Err(error) => probe.error = Some(error),
    }
    if let Some(client) = client {
        client.close(Duration::from_secs(1)).await;
    }
    Ok(Json(probe))
}
