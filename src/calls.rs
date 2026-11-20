use crate::{App, Error, Result, media, model::Input, pricing};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use chrono::Utc;
use rusqlite::{Connection, Row, params, types::ValueRef};
use serde::Deserialize;
use serde_json::{Map, Value};

// Called inside the transaction which pins the session's input prefix.
pub fn begin(conn: &Connection, sid: i64, end: i64) -> Result<Input> {
    let (model_id, model, url, key, effort, modalities, prices) = conn.query_row(
        "SELECT m.id,m.model_key,p.base_url,p.api_key,s.reasoning_effort,m.input_modalities,m.pricing
         FROM sessions s JOIN llm_models m ON m.id=s.model_id JOIN llm_providers p ON p.id=m.provider_id WHERE s.id=?",
        [sid],
        |r| Ok((
            r.get::<_, i64>(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
            r.get::<_, Option<String>>(4)?, r.get(5)?, r.get::<_, Option<Value>>(6)?,
        )),
    )?;
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

pub fn routes() -> Router<App> {
    Router::new()
        .route("/calls", get(list))
        .route("/calls/{id}", get(one))
}

fn row(r: &Row<'_>) -> rusqlite::Result<Value> {
    let mut record = Map::new();
    for (i, key) in r.as_ref().column_names().enumerate() {
        let value = match r.get_ref(i)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => n.into(),
            _ if matches!(key, "pricing_snapshot" | "raw_usage") => r.get::<_, Value>(i)?,
            _ => Value::String(r.get(i)?),
        };
        record.insert(key.to_owned(), value);
    }
    Ok(record.into())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Filter {
    session_id: Option<i64>,
    model_id: Option<i64>,
    #[serde(default)]
    after: i64,
    limit: Option<i64>,
}

async fn list(State(app): State<App>, Query(q): Query<Filter>) -> Result<Json<Vec<Value>>> {
    let limit = q.limit.unwrap_or(50);
    if q.after < 0 || !(1..=200).contains(&limit) {
        return Err(Error::bad("expected after >= 0 and limit 1..200"));
    }
    app.db.call(move |s| {
        let mut stmt = s.conn.prepare_cached("SELECT * FROM llm_calls WHERE id>?1 AND (?2 IS NULL OR session_id=?2) AND (?3 IS NULL OR model_id=?3) ORDER BY id LIMIT ?4")?;
        Ok(Json(stmt.query_map(params![q.after,q.session_id,q.model_id,limit],row)?.collect::<rusqlite::Result<_>>()?))
    }).await
}

async fn one(State(app): State<App>, Path(id): Path<i64>) -> Result<Json<Value>> {
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
