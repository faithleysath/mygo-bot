use crate::{
    App, Error, ErrorBody, Result, agent, events,
    store::{self, Session, session},
};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub async fn authorize_tcp(
    State(token): State<Arc<str>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    let mut headers = request.headers().get_all(header::AUTHORIZATION).iter();
    let authorized = headers
        .next()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .is_some_and(|(scheme, credential)| {
            scheme.eq_ignore_ascii_case("Bearer")
                && bool::from(
                    token
                        .as_bytes()
                        .ct_eq(credential.trim_start_matches(' ').as_bytes()),
                )
        });
    if authorized && headers.next().is_none() {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(ErrorBody {
                error: "unauthorized".into(),
            }),
        )
            .into_response()
    }
}

#[derive(utoipa::OpenApi)]
#[openapi(
    components(schemas(Selection)),
    info(
        title = "MyGo-Bot API",
        description = "输入服务的 Bearer token，即可在本页试调接口。"
    )
)]
struct ApiDoc;

pub fn routes(app: App) -> (Router, utoipa::openapi::OpenApi) {
    use utoipa::OpenApi;
    use utoipa_axum::{router::OpenApiRouter, routes};
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(get_config, patch_config))
        .routes(routes!(list_sessions, create_session))
        .routes(routes!(get_session, patch_session))
        .routes(routes!(enqueue, messages))
        .routes(routes!(withdraw))
        .routes(routes!(agent::start, agent::pause))
        .routes(routes!(history))
        .routes(routes!(context))
        .routes(routes!(events::subscribe))
        .merge(crate::catalog::routes())
        .merge(crate::mcp::routes())
        .merge(crate::mcp::diagnostics::routes())
        .merge(crate::media::routes())
        .merge(crate::calls::routes())
        .with_state(app)
        .split_for_parts()
}

type ConfigPatch = crate::docs::Patch<Config>;
type SelectionPatch = crate::docs::Patch<Selection>;
type Document = Json<Map<String, Value>>;
#[derive(Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct Config {
    #[schema(required)]
    default_model_id: Option<i64>,
    #[schema(required)]
    default_reasoning_effort: Option<String>,
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct Selection {
    model_id: Option<i64>,
    reasoning_effort: Option<String>,
}

impl Selection {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            model_id: row.get(0)?,
            reasoning_effort: row.get(1)?,
        })
    }

    fn defaults(conn: &Connection) -> Result<Self> {
        Ok(conn.query_row(
            "SELECT default_model_id,default_reasoning_effort FROM config WHERE id=1",
            [],
            Self::read,
        )?)
    }

    fn apply(self, fields: Map<String, Value>) -> Result<Self> {
        let mut data = json!(self);
        if fields.get("model_id").is_some_and(Value::is_null) {
            data["reasoning_effort"] = Value::Null;
        }
        data.as_object_mut().unwrap().extend(fields);
        serde_json::from_value(data).map_err(|e| Error::bad(e.to_string()))
    }

    fn validate(&self, conn: &Connection) -> Result<Option<Value>> {
        let Some(id) = self.model_id else {
            if self.reasoning_effort.is_some() {
                return Err(Error::bad(
                    "select a model before specifying reasoning effort",
                ));
            }
            return Ok(None);
        };
        let (modalities, efforts) = crate::catalog::available_capabilities(conn, id)?;
        if let Some(effort) = &self.reasoning_effort
            && !efforts
                .as_array()
                .is_some_and(|opts| opts.iter().any(|v| v.as_str() == Some(effort)))
        {
            return Err(Error::bad(format!(
                "model {id} does not support effort {effort}; available: {efforts}"
            )));
        }
        Ok(Some(modalities))
    }

    fn config(self) -> Config {
        Config {
            default_model_id: self.model_id,
            default_reasoning_effort: self.reasoning_effort,
        }
    }
}

/// 读取默认模型设置
#[utoipa::path(
    get,
    path = "/config",
    tag = "配置",
    responses(
        (status = 200, description = "读取默认模型设置", body = Config),
        crate::docs::ServiceErrors
    )
)]
async fn get_config(State(app): State<App>) -> Result<Json<Config>> {
    app.db
        .call(|s| Ok(Json(Selection::defaults(&s.conn)?.config())))
        .await
}
/// 更新默认模型设置
///
/// 至少传一个字段。省略的字段沿用当前值。default_model_id 为 null 时清除默认模型，同时清除原推理强度。
#[utoipa::path(
    patch,
    path = "/config",
    tag = "配置",
    request_body = ConfigPatch,
    responses(
        (status = 200, description = "更新默认模型设置", body = Config),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        crate::docs::ServiceErrors,
        crate::docs::JsonBodyErrors
    )
)]
async fn patch_config(State(app): State<App>, Json(fields): Document) -> Result<Json<Config>> {
    let mut selection = Map::new();
    for (key, value) in fields {
        let field = match key.as_str() {
            "default_model_id" => "model_id",
            "default_reasoning_effort" => "reasoning_effort",
            _ => return Err(Error::bad("unknown configuration field")),
        };
        selection.insert(field.into(), value);
    }
    if selection.is_empty() {
        return Err(Error::bad("expected configuration fields"));
    }
    app.db
        .call(move |s| {
            let next = Selection::defaults(&s.conn)?.apply(selection)?;
            next.validate(&s.conn)?;
            s.conn.execute(
                "UPDATE config SET default_model_id=?, default_reasoning_effort=? WHERE id=1",
                params![next.model_id, next.reasoning_effort],
            )?;
            Ok(Json(next.config()))
        })
        .await
}

/// 创建会话
///
/// 可发送空对象以使用默认配置，也可用 model_id 和 reasoning_effort 覆盖默认值。
#[utoipa::path(
    post,
    path = "/sessions",
    tag = "会话",
    request_body(content = ref("#/components/schemas/Selection"), content_type = "application/json", example = json!({})),
    responses(
        (status = 201, description = "创建成功", body = Session),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        crate::docs::ServiceErrors,
        crate::docs::JsonBodyErrors
    )
)]
async fn create_session(
    State(app): State<App>,
    body: Option<Document>,
) -> Result<(StatusCode, Json<Session>)> {
    let fields = body.map(|x| x.0).unwrap_or_default();
    let created = app
        .db
        .write(
            move |conn| {
                let next = Selection::defaults(conn)?.apply(fields)?;
                next.validate(conn)?;
                conn.execute(
                    "INSERT INTO sessions(model_id,reasoning_effort) VALUES(?,?)",
                    params![next.model_id, next.reasoning_effort],
                )?;
                session(conn, conn.last_insert_rowid())
            },
            |_, created| Ok(created),
        )
        .await?;
    Ok((StatusCode::CREATED, Json(created)))
}
/// 读取会话
#[utoipa::path(
    get,
    path = "/sessions/{sid}",
    tag = "会话",
    params(("sid" = i64, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "读取会话", body = Session),
        (status = 400, description = "参数无法解析", body = String, content_type = "text/plain"),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn get_session(State(app): State<App>, Path(sid): Path<i64>) -> Result<Json<Session>> {
    app.db.call(move |s| Ok(Json(session(&s.conn, sid)?))).await
}
/// 更新会话模型设置
///
/// 省略字段表示保留。传 model_id: null 可清空空会话的模型，也会清除原推理强度。
#[utoipa::path(
    patch,
    path = "/sessions/{sid}",
    tag = "会话",
    params(("sid" = i64, Path, description = "会话 ID")),
    request_body = SelectionPatch,
    responses(
        (status = 200, description = "更新会话模型设置", body = Session),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        crate::docs::ServiceErrors,
        crate::docs::JsonBodyErrors
    )
)]
async fn patch_session(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Json(fields): Document,
) -> Result<Json<Session>> {
    if fields.is_empty() {
        return Err(Error::bad("expected session settings"));
    }
    app.db
        .write(
            move |conn| {
                let current = conn.query_row(
                    "SELECT model_id,reasoning_effort FROM sessions WHERE id=?",
                    [sid],
                    Selection::read,
                )?;
                let old_id = current.model_id;
                let next = current.apply(fields)?;
                let modalities = next.validate(conn)?;
                if next.model_id != old_id {
                    if let Some(modalities) = modalities {
                        crate::media::check_session(conn, sid, &modalities)?;
                    } else {
                        let has_context: bool = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM context_items WHERE session_id=?1
                     UNION ALL SELECT 1 FROM pending WHERE session_id=?1
                     UNION ALL SELECT 1 FROM items WHERE session_id=?1 AND kind='function_call')",
                            [sid],
                            |r| r.get(0),
                        )?;
                        if has_context {
                            return Err(Error::conflict("only an empty session may have no model"));
                        }
                    }
                }
                conn.execute(
                    "UPDATE sessions SET model_id=?,reasoning_effort=? WHERE id=?",
                    params![next.model_id, next.reasoning_effort, sid],
                )?;
                session(conn, sid)
            },
            |_, updated| Ok(Json(updated)),
        )
        .await
}

#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
struct Page {
    /// 上一页末项的游标，默认从 0 开始。
    #[serde(default)]
    #[param(minimum = 0, default = 0)]
    after: i64,
    /// 最多返回的记录数。
    #[serde(default = "default_limit")]
    #[param(minimum = 1, maximum = 200, default = default_limit)]
    limit: i64,
}
fn default_limit() -> i64 {
    50
}
impl Page {
    fn check(&self) -> Result<()> {
        if self.after < 0 || !(1..=200).contains(&self.limit) {
            Err(Error::bad("expected after >= 0 and limit 1..200"))
        } else {
            Ok(())
        }
    }
}
/// 查询会话列表
///
/// 按 ID 升序返回会话。下一页使用本页最后一个会话的 ID 作为 `after`。
#[utoipa::path(get, path = "/sessions", tag = "会话", params(Page), responses(
    (status = 200, description = "当前页的会话", body = Vec<Session>,
        example = json!([{"id": 1, "status": "idle", "model_id": null, "reasoning_effort": null, "last_error": ""}])),
    (status = 400, description = "分页参数越界（JSON）或无法解析查询参数（纯文本）",
        content((ErrorBody = "application/json"), (String = "text/plain"))),
    crate::docs::ServiceErrors
))]
async fn list_sessions(
    State(app): State<App>,
    Query(page): Query<Page>,
) -> Result<Json<Vec<Session>>> {
    page.check()?;
    app.db
        .call(move |s| {
            let sql = "SELECT id,status,reasoning_effort,last_error,model_id FROM sessions
                   WHERE id>? ORDER BY id LIMIT ?";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let rows = stmt
                .query_map((page.after, page.limit), store::session_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(rows))
        })
        .await
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct Message {
    #[schema(value_type = Vec<crate::media::ContentBlock>, min_items = 1)]
    content: Value,
}
#[derive(Serialize, utoipa::ToSchema)]
struct MessageId {
    id: i64,
}

#[derive(Serialize, utoipa::ToSchema)]
struct QueuedMessage {
    id: i64,
    #[schema(value_type = Vec<crate::media::ContentBlock>)]
    content: Value,
    /// queued、included 或 withdrawn。
    #[schema(pattern = "^(queued|included|withdrawn)$")]
    status: String,
}

/// 提交用户消息
///
/// content 是非空内容块数组。相同的幂等键和内容重试返回已有消息，不同内容返回 409。
#[utoipa::path(
    post,
    path = "/sessions/{sid}/messages",
    tag = "消息",
    params(("sid" = i64, Path, description = "会话 ID"), ("Idempotency-Key" = Option<String>, Header, description = "可选，去除首尾空白后为 1 到 256 字节。")),
    request_body(content = Message, example = json!({"content":[{"type":"text","text":"你好"}]})),
    responses(
        (status = 201, description = "创建成功", body = MessageId),
        (status = 200, description = "命中已提交的幂等消息", body = MessageId),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        crate::docs::ServiceErrors,
        crate::docs::JsonBodyErrors
    )
)]
async fn enqueue(
    State(app): State<App>,
    Path(sid): Path<i64>,
    headers: HeaderMap,
    Json(message): Json<Message>,
) -> Result<(StatusCode, Json<MessageId>)> {
    let mut keys = headers.get_all("idempotency-key").iter();
    let key = keys
        .next()
        .map(|v| {
            v.to_str()
                .map(str::trim)
                .map(String::from)
                .map_err(|_| Error::bad("invalid idempotency key"))
        })
        .transpose()?;
    if keys.next().is_some() || key.as_ref().is_some_and(|k| k.is_empty() || k.len() > 256) {
        return Err(Error::bad("invalid idempotency key"));
    }
    let (status, id) = match app.db.enqueue(sid, message.content, key).await? {
        store::Enqueued::Created(id) => (StatusCode::CREATED, id),
        store::Enqueued::Existing(id) => (StatusCode::OK, id),
    };
    Ok((status, Json(MessageId { id })))
}
#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
struct Filter {
    id: Option<i64>,
    /// queued、included 或 withdrawn。
    #[param(pattern = "^(queued|included|withdrawn)$")]
    status: Option<String>,
    #[serde(default)]
    #[param(minimum = 0, default = 0)]
    after: i64,
    #[serde(default = "default_limit")]
    #[param(minimum = 1, maximum = 200, default = default_limit)]
    limit: i64,
}
/// 查询会话消息
///
/// 按消息 ID 升序分页，支持按消息 ID 和状态筛选。
#[utoipa::path(
    get,
    path = "/sessions/{sid}/messages",
    tag = "消息",
    params(("sid" = i64, Path, description = "会话 ID"), Filter),
    responses(
        (status = 200, description = "查询会话消息", body = Vec<QueuedMessage>),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn messages(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(q): Query<Filter>,
) -> Result<Json<Vec<QueuedMessage>>> {
    Page {
        after: q.after,
        limit: q.limit,
    }
    .check()?;
    if !matches!(
        q.status.as_deref(),
        None | Some("queued" | "included" | "withdrawn")
    ) {
        return Err(Error::bad("unknown message status"));
    }
    app.db
        .call(move |s| {
            session(&s.conn, sid)?;
            let sql = "SELECT i.id, json_extract(i.payload,'$.content'),
                     CASE WHEN w.id IS NOT NULL THEN 'withdrawn'
                          WHEN c.item_id IS NOT NULL THEN 'included' ELSE 'queued' END AS status
                   FROM items i
                   LEFT JOIN items w ON w.target_id=i.id AND w.kind='withdrawal'
                   LEFT JOIN context_items c ON c.item_id=i.id
                   WHERE i.session_id=?1 AND i.kind='user_message'
                     AND i.id BETWEEN ?2 AND ?3 AND i.id>?4
                     AND (?5 IS NULL OR status=?5)
                   ORDER BY i.id LIMIT ?6";
            let mut stmt = s.conn.prepare_cached(sql)?;
            let args = params![
                sid,
                q.id.unwrap_or(1),
                q.id.unwrap_or(i64::MAX),
                q.after,
                q.status,
                q.limit
            ];
            let data = stmt
                .query_map(args, |r| {
                    Ok(QueuedMessage {
                        id: r.get(0)?,
                        content: r.get(1)?,
                        status: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(Json(data))
        })
        .await
}
/// 撤回排队的消息
///
/// 重复撤回返回 204。已进入上下文的消息无法撤回。
#[utoipa::path(
    delete,
    path = "/sessions/{sid}/messages/{mid}",
    tag = "消息",
    params(("sid" = i64, Path, description = "会话 ID"), ("mid" = i64, Path, description = "消息 ID")),
    responses(
        (status = 204, description = "操作成功，无响应体", ),
        (status = 400, description = "参数无法解析", body = String, content_type = "text/plain"),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        (status = 409, description = "当前状态与操作冲突", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn withdraw(
    State(app): State<App>,
    Path((sid, mid)): Path<(i64, i64)>,
) -> Result<StatusCode> {
    app.db
        .call(move |s| {
            let sql = "SELECT EXISTS(SELECT 1 FROM items WHERE kind='withdrawal' AND target_id=?2),
                          EXISTS(SELECT 1 FROM context_items WHERE item_id=?2)
                   FROM items WHERE session_id=?1 AND id=?2 AND kind='user_message'";
            let (withdrawn, included) = s.conn.query_row(sql, (sid, mid), |r| {
                Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?))
            })?;
            if included {
                return Err(Error::conflict("message already included"));
            }
            if !withdrawn {
                let id = store::add_item(&s.conn, sid, "withdrawal", &json!({}), Some(mid), None)?;
                s.item_saved(sid, id)?;
            }
            Ok(StatusCode::NO_CONTENT)
        })
        .await
}
async fn record_page(app: App, sid: i64, page: Page, rows: store::JsonRows) -> Result<Response> {
    page.check()?;
    let end = app
        .db
        .call(move |s| {
            session(&s.conn, sid)?;
            rows.page_end(&s.conn, sid, page.after, page.limit)
        })
        .await?;
    let body = rows
        .body(app.db, sid, page.after, end, vec![b'['], b"]")
        .await?;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        axum::body::Body::new(body),
    )
        .into_response())
}

/// 读取历史记录
///
/// 按历史记录 ID 升序分页，after 填上页末项的 id。
#[utoipa::path(
    get,
    path = "/sessions/{sid}/history",
    tag = "会话",
    params(("sid" = i64, Path, description = "会话 ID"), Page),
    responses(
        (status = 200, description = "读取历史记录", body = Vec<store::HistoryItem>),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn history(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(page): Query<Page>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::History).await
}

/// 读取模型上下文
///
/// 按上下文位置升序分页，after 填上页末项的 position。
#[utoipa::path(
    get,
    path = "/sessions/{sid}/context",
    tag = "会话",
    params(("sid" = i64, Path, description = "会话 ID"), Page),
    responses(
        (status = 200, description = "读取模型上下文", body = Vec<store::ContextItem>),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn context(
    State(app): State<App>,
    Path(sid): Path<i64>,
    Query(page): Query<Page>,
) -> Result<Response> {
    record_page(app, sid, page, store::JsonRows::Context).await
}
