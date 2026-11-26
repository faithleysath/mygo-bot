use crate::{
    App, Error, Result,
    id::{CallId, ModelId, SessionId},
    media,
    model::Input,
    pricing,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::Utc;
use rusqlite::{Connection, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Called inside the transaction which pins the session's input prefix.
pub fn begin(conn: &Connection, sid: SessionId, end: i64) -> Result<Input> {
    let (model_id, model, url, key, effort, prices) = conn
        .prepare_cached(
            "SELECT m.id,m.model_key,p.base_url,p.api_key,s.reasoning_effort,m.pricing
             FROM sessions s JOIN llm_models m ON m.id=s.model_id JOIN llm_providers p ON p.id=m.provider_id WHERE s.id=?",
        )?
        .query_row([sid], |r| {
            Ok((
                r.get::<_, ModelId>(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<Value>>(5)?,
            ))
        })?;
    let (modalities, _) = crate::catalog::available_capabilities(conn, model_id)?;
    media::check_session(conn, sid, &modalities)?;
    let started_at = Utc::now().timestamp_millis();
    let snapshot = prices
        .as_ref()
        .and_then(|p| pricing::snapshot(p, started_at));
    let call_id = CallId::new()?;
    let origin_node_id = crate::cluster::identity::local_node_id(conn)?;
    conn.prepare_cached(
        "INSERT INTO llm_calls(id,session_id,model_id,reasoning_effort,input_end_position,started_at,status,pricing_snapshot,origin_node_id)
         VALUES(?,?,?,?,?,?,'in_flight',?,?)",
    )?
    .execute(params![
        call_id,
        sid,
        model_id,
        effort,
        end,
        started_at,
        snapshot,
        origin_node_id
    ])?;
    crate::cluster::ledger::record_call(conn, call_id)?;
    Ok(Input {
        call_id,
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
        id: CallId,
        status: &str,
        reason: Option<&str>,
        at: i64,
    ) -> Result<()> {
        crate::cluster::ledger::with_savepoint(conn, "finish_call", || {
            let snapshot = conn
                .prepare_cached(
                    "SELECT pricing_snapshot FROM llm_calls WHERE id=? AND status='in_flight'",
                )?
                .query_row([id], |r| r.get(0))?;
            let usage = self.usage.filter(Value::is_object);
            let bill = pricing::settle(snapshot, usage.as_ref());
            conn.prepare_cached(
                "UPDATE llm_calls SET finished_at=?, status=?, error_summary=?, upstream_response_id=?,
                 http_status=?, raw_usage=?, input_uncached_tokens=?, cache_read_tokens=?, cache_write_tokens=?,
                 output_tokens=?, cost_nano_usd=?, pricing_snapshot=? WHERE id=? AND status='in_flight'",
            )?
            .execute(params![
                at,
                status,
                reason,
                self.response_id,
                self.http_status,
                usage,
                bill.input_uncached_tokens,
                bill.cache_read_tokens,
                bill.cache_write_tokens,
                bill.output_tokens,
                bill.cost_nano_usd,
                bill.snapshot,
                id
            ])?;
            crate::cluster::ledger::record_call(conn, id)?;
            Ok(())
        })
    }
}

pub fn routes() -> Router<App> {
    Router::new()
        .route("/calls", get(list))
        .route("/calls/summary", get(summary))
        .route("/calls/{id}", get(one))
}

#[derive(Serialize)]
struct Call {
    id: CallId,
    session_id: Option<SessionId>,
    model_id: ModelId,
    reasoning_effort: Option<String>,
    input_end_position: Option<i64>,
    started_at: i64,
    finished_at: Option<i64>,
    status: String,
    upstream_response_id: Option<String>,
    http_status: Option<i64>,
    error_summary: Option<String>,
    raw_usage: Option<Value>,
    input_uncached_tokens: Option<i64>,
    cache_read_tokens: Option<i64>,
    cache_write_tokens: Option<i64>,
    output_tokens: Option<i64>,
    pricing_snapshot: Option<Value>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Filter {
    session_id: Option<SessionId>,
    model_id: Option<ModelId>,
    after: Option<CallId>,
    limit: Option<i64>,
}

async fn list(State(app): State<App>, Query(q): Query<Filter>) -> Result<Json<Vec<Call>>> {
    let limit = q.limit.unwrap_or(50);
    if !(1..=200).contains(&limit) {
        return Err(Error::bad("expected limit 1..200"));
    }
    app.db.call(move |s| {
        let mut stmt = s.conn.prepare_cached("SELECT * FROM llm_calls WHERE id>?1 AND (?2 IS NULL OR session_id=?2) AND (?3 IS NULL OR model_id=?3) ORDER BY id LIMIT ?4")?;
        let after = q.after.as_ref().map_or("", CallId::as_str);
        Ok(Json(stmt.query_map(params![after, q.session_id, q.model_id, limit], row)?.collect::<rusqlite::Result<_>>()?))
    }).await
}

async fn one(State(app): State<App>, Path(id): Path<CallId>) -> Result<Json<Call>> {
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryFilter {
    session_id: Option<SessionId>,
    model_id: Option<ModelId>,
}

#[derive(Serialize)]
struct CallsSummary {
    call_count: i64,
    succeeded_count: i64,
    failed_count: i64,
    input_uncached_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    output_tokens: i64,
    cost_nano_usd: i64,
    unpriced_succeeded_count: i64,
    last_input_tokens: Option<i64>,
}

async fn summary(
    State(app): State<App>,
    Query(q): Query<SummaryFilter>,
) -> Result<Json<CallsSummary>> {
    app.db
        .call(move |s| {
            let mut summary = s.conn.prepare_cached(
                "SELECT
                   count(*),
                   coalesce(sum(CASE WHEN status='succeeded' THEN 1 ELSE 0 END), 0),
                   coalesce(sum(CASE WHEN status IN ('failed','cancelled','interrupted') THEN 1 ELSE 0 END), 0),
                   coalesce(sum(coalesce(input_uncached_tokens, 0)), 0),
                   coalesce(sum(coalesce(cache_read_tokens, 0)), 0),
                   coalesce(sum(coalesce(cache_write_tokens, 0)), 0),
                   coalesce(sum(coalesce(output_tokens, 0)), 0),
                   coalesce(sum(coalesce(cost_nano_usd, 0)), 0),
                   coalesce(sum(CASE WHEN status='succeeded' AND cost_nano_usd IS NULL THEN 1 ELSE 0 END), 0)
                 FROM llm_calls
                 WHERE (?1 IS NULL OR session_id=?1) AND (?2 IS NULL OR model_id=?2)",
            )?
            .query_row((q.session_id, q.model_id), |r| {
                Ok(CallsSummary {
                    call_count: r.get(0)?,
                    succeeded_count: r.get(1)?,
                    failed_count: r.get(2)?,
                    input_uncached_tokens: r.get(3)?,
                    cache_read_tokens: r.get(4)?,
                    cache_write_tokens: r.get(5)?,
                    output_tokens: r.get(6)?,
                    cost_nano_usd: r.get(7)?,
                    unpriced_succeeded_count: r.get(8)?,
                    last_input_tokens: None,
                })
            })?;
            use rusqlite::OptionalExtension;
            summary.last_input_tokens = s.conn.prepare_cached(
                "SELECT coalesce(input_uncached_tokens, 0) + coalesce(cache_read_tokens, 0) + coalesce(cache_write_tokens, 0)
                 FROM llm_calls
                 WHERE (?1 IS NULL OR session_id=?1) AND (?2 IS NULL OR model_id=?2)
                   AND status='succeeded'
                   AND (input_uncached_tokens IS NOT NULL OR cache_read_tokens IS NOT NULL OR cache_write_tokens IS NOT NULL)
                 ORDER BY id DESC LIMIT 1",
            )?
            .query_row((q.session_id, q.model_id), |r| r.get(0))
            .optional()?;
            Ok(Json(summary))
        })
        .await
}
