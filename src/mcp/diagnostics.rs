use super::{Client, config, connect};
use crate::{App, Result};
use axum::{
    Json,
    extract::{Path, State},
};
use rmcp::{
    model::{ErrorCode, ProtocolVersion},
    service::ServiceError,
};
use serde::Serialize;
use std::time::Duration;
use tokio::time::{Instant, timeout};

pub(crate) fn routes(documented: bool) -> crate::docs::RouteRegistry {
    crate::docs::RouteRegistry::new(documented).register(crate::docs::register!(test_server))
}

#[derive(Serialize, utoipa::ToSchema)]
struct ListSummary {
    operation: &'static str,
    /// 首页条目数。
    count: usize,
    has_more: bool,
}

#[derive(Serialize, utoipa::ToSchema)]
struct Probe {
    id: i64,
    revision: i64,
    ok: bool,
    /// 成功时为 complete，失败时为出错的握手或目录阶段。
    stage: &'static str,
    /// 从开始握手到结束目录检查的耗时。
    elapsed_ms: u64,
    protocol_version: Option<String>,
    /// 已成功读取的目录首页。服务未实现可选的资源模板目录时省略该项。
    lists: Vec<ListSummary>,
    error: Option<String>,
}

impl Probe {
    fn page(&mut self, count: usize, has_more: bool) {
        self.lists.push(ListSummary {
            operation: self.stage,
            count,
            has_more,
        });
    }

    async fn list(&mut self, client: &Client) -> std::result::Result<(), String> {
        let info = client
            .peer_info()
            .ok_or("MCP handshake did not return server capabilities")?;
        self.protocol_version = ProtocolVersion::KNOWN_VERSIONS
            .contains(&info.protocol_version)
            .then(|| info.protocol_version.to_string());
        if info.capabilities.tools.is_some() {
            self.stage = "tools/list";
            let page = client.list_tools(None).await.map_err(list_error)?;
            self.page(page.tools.len(), page.next_cursor.is_some());
        }
        if info.capabilities.resources.is_some() {
            self.stage = "resources/list";
            let page = client.list_resources(None).await.map_err(list_error)?;
            self.page(page.resources.len(), page.next_cursor.is_some());
            self.stage = "resources/templates/list";
            match client.list_resource_templates(None).await {
                Ok(page) => self.page(page.resource_templates.len(), page.next_cursor.is_some()),
                // MCP has no separate capability flag for template discovery.
                Err(ServiceError::McpError(e)) if e.code == ErrorCode::METHOD_NOT_FOUND => (),
                Err(e) => return Err(list_error(e)),
            }
        }
        if info.capabilities.prompts.is_some() {
            self.stage = "prompts/list";
            let page = client.list_prompts(None).await.map_err(list_error)?;
            self.page(page.prompts.len(), page.next_cursor.is_some());
        }
        Ok(())
    }
}

fn list_error(error: ServiceError) -> String {
    if error.is_authorization_required() {
        return "MCP authentication required".into();
    }
    if let ServiceError::McpError(error) = error {
        return format!("MCP protocol error ({})", error.code.0);
    }
    "MCP catalog request failed; check the connection and server capabilities".into()
}

/// 试连接 MCP 服务，可测试已停用的配置。握手后按能力读取目录首页，只返回条数和有无下一页。
#[utoipa::path(post, path="/mcp/servers/{id}/test", tag="MCP", params(("id"=i64, Path)), responses(
    (status=200, description="试连结果，ok 表示握手和目录读取是否成功", body=Probe),
    (status=400, description="服务 ID 无效", body=String, content_type="text/plain"),
    (status=404, body=crate::ErrorBody), (status=429,description="试连接并发已满",body=crate::ErrorBody), crate::docs::ServiceErrors
))]
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
    if let Some(mut client) = client {
        let _ = client.close_with_timeout(Duration::from_secs(1)).await;
    }
    Ok(Json(probe))
}
