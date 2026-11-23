use crate::{App, Error, Result, media, model::Input, pricing};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::Utc;
use rusqlite::{Connection, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Called inside the transaction which pins the session's input prefix.
pub fn begin(conn: &Connection, sid: i64, end: i64) -> Result<Input> {
    let (model_id, model, url, key, effort, prices) = conn.query_row(
        "SELECT m.id,m.model_key,p.base_url,p.api_key,s.reasoning_effort,m.pricing
         FROM sessions s JOIN llm_models m ON m.id=s.model_id JOIN llm_providers p ON p.id=m.provider_id WHERE s.id=?",
        [sid],
        |r| Ok((
            r.get::<_, i64>(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
            r.get::<_, Option<String>>(4)?, r.get::<_, Option<Value>>(5)?,
        )),
    )?;
    let (modalities, _) = crate::catalog::available_capabilities(conn, model_id)?;
    media::check_session(conn, sid, &modalities)?;
    let started_at = Utc::now().timestamp_millis();
    let snapshot = prices
        .as_ref()
        .and_then(|p| pricing::snapshot(p, started_at));
    conn.execute(
        "INSERT INTO llm_calls(session_id,model_id,reasoning_effort,input_end_position,started_at,status,pricing_snapshot)
         VALUES(?,?,?,?,?,'in_flight',?)",
        params![sid, model_id, effort, end, started_at, snapshot],
    )?;
    Ok(Input {
        call_id: conn.last_insert_rowid(),
        model,
        base_url: url,
        api_key: key,
        effort,
        end,
    })
}

#[derive(Default)]
pub struct Report {
    pub response_id: Option<String>,
    pub http_status: Option<u16>,
    pub usage: Option<Value>,
}

impl Report {
    pub fn finish(
        self,
        conn: &Connection,
        id: i64,
        status: &str,
        reason: Option<&str>,
        at: i64,
    ) -> Result<()> {
        let snapshot = conn.query_row(
            "SELECT pricing_snapshot FROM llm_calls WHERE id=? AND status='in_flight'",
            [id],
            |r| r.get(0),
        )?;
        let usage = self.usage.filter(Value::is_object);
        let bill = pricing::settle(snapshot, usage.as_ref());
        conn.execute(
            "UPDATE llm_calls SET finished_at=?, status=?, error_summary=?, upstream_response_id=?,
             http_status=?, raw_usage=?, input_uncached_tokens=?, cache_read_tokens=?, cache_write_tokens=?,
             output_tokens=?, cost_nano_usd=?, pricing_snapshot=? WHERE id=? AND status='in_flight'",
            params![at, status, reason, self.response_id, self.http_status, usage,
                bill.input_uncached_tokens, bill.cache_read_tokens, bill.cache_write_tokens,
                bill.output_tokens, bill.cost_nano_usd, bill.snapshot, id],
        )?;
        Ok(())
    }
}

pub fn routes(documented: bool) -> crate::docs::RouteRegistry {
    crate::docs::RouteRegistry::new(documented)
        .register(crate::docs::register!(list))
        .register(crate::docs::register!(one))
}

#[derive(Serialize, utoipa::ToSchema)]
struct Call {
    id: i64,
    #[schema(required)]
    session_id: Option<i64>,
    model_id: i64,
    #[schema(required)]
    reasoning_effort: Option<String>,
    #[schema(required)]
    input_end_position: Option<i64>,
    /// 调用开始时的 Unix 毫秒时间戳。
    started_at: i64,
    /// 调用结束时的 Unix 毫秒时间戳。
    #[schema(required)]
    finished_at: Option<i64>,
    /// in_flight、succeeded、failed、cancelled 或 interrupted。
    #[schema(pattern = "^(in_flight|succeeded|failed|cancelled|interrupted)$")]
    status: String,
    #[schema(required)]
    upstream_response_id: Option<String>,
    #[schema(required)]
    http_status: Option<i64>,
    #[schema(required)]
    error_summary: Option<String>,
    /// 供应商返回的原始 usage 对象。
    #[schema(required, value_type = Option<Object>)]
    raw_usage: Option<Value>,
    #[schema(required)]
    input_uncached_tokens: Option<i64>,
    #[schema(required)]
    cache_read_tokens: Option<i64>,
    #[schema(required)]
    cache_write_tokens: Option<i64>,
    #[schema(required)]
    output_tokens: Option<i64>,
    /// 记录调用时选用的 UTC 价目表版本、时段和阶梯。
    #[schema(required, value_type = Option<pricing::PricingSnapshot>)]
    pricing_snapshot: Option<Value>,
    /// 费用，单位为十亿分之一美元。无法结算时为 null。
    #[schema(required)]
    cost_nano_usd: Option<i64>,
}

fn row(r: &Row<'_>) -> rusqlite::Result<Call> {
    Ok(Call {
        id: r.get("id")?,
        session_id: r.get("session_id")?,
        model_id: r.get("model_id")?,
        reasoning_effort: r.get("reasoning_effort")?,
        input_end_position: r.get("input_end_position")?,
        started_at: r.get("started_at")?,
        finished_at: r.get("finished_at")?,
        status: r.get("status")?,
        upstream_response_id: r.get("upstream_response_id")?,
        http_status: r.get("http_status")?,
        error_summary: r.get("error_summary")?,
        raw_usage: r.get("raw_usage")?,
        input_uncached_tokens: r.get("input_uncached_tokens")?,
        cache_read_tokens: r.get("cache_read_tokens")?,
        cache_write_tokens: r.get("cache_write_tokens")?,
        output_tokens: r.get("output_tokens")?,
        pricing_snapshot: r.get("pricing_snapshot")?,
        cost_nano_usd: r.get("cost_nano_usd")?,
    })
}

#[derive(Deserialize, utoipa::IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
struct Filter {
    session_id: Option<i64>,
    model_id: Option<i64>,
    #[serde(default)]
    #[param(minimum = 0, default = 0)]
    after: i64,
    #[param(minimum = 1, maximum = 200, default = 50)]
    limit: Option<i64>,
}

/// 查询模型调用记录
///
/// 按调用 ID 升序分页，可按会话或模型筛选。
#[utoipa::path(
    get,
    path = "/calls",
    tag = "调用记录",
    params(Filter),
    responses(
        (status = 200, description = "查询模型调用记录", body = Vec<Call>),
        (status = 400, description = "无效的参数或请求", content((crate::ErrorBody = "application/json"), (String = "text/plain"))),
        crate::docs::ServiceErrors
    )
)]
async fn list(State(app): State<App>, Query(q): Query<Filter>) -> Result<Json<Vec<Call>>> {
    let limit = q.limit.unwrap_or(50);
    if q.after < 0 || !(1..=200).contains(&limit) {
        return Err(Error::bad("expected after >= 0 and limit 1..200"));
    }
    app.db.call(move |s| {
        let mut stmt = s.conn.prepare_cached("SELECT * FROM llm_calls WHERE id>?1 AND (?2 IS NULL OR session_id=?2) AND (?3 IS NULL OR model_id=?3) ORDER BY id LIMIT ?4")?;
        Ok(Json(stmt.query_map(params![q.after,q.session_id,q.model_id,limit],row)?.collect::<rusqlite::Result<_>>()?))
    }).await
}

/// 读取模型调用详情
#[utoipa::path(
    get,
    path = "/calls/{id}",
    tag = "调用记录",
    params(("id" = i64, Path, description = "调用 ID")),
    responses(
        (status = 200, description = "读取模型调用详情", body = Call),
        (status = 400, description = "参数无法解析", body = String, content_type = "text/plain"),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
async fn one(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Call>> {
    app.db
        .call(move |s| {
            Ok(Json(s.conn.query_row(
                "SELECT * FROM llm_calls WHERE id=?",
                [id],
                row,
            )?))
        })
        .await
}
