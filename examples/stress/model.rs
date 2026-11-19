use crate::{Result, check};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

#[derive(Default)]
pub struct Control {
    pub entered: Notify,
    pub release: Notify,
    attempts: AtomicUsize,
}
impl Control {
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

type Pending = Arc<Mutex<HashMap<String, Arc<Control>>>>;
#[derive(Clone, Default)]
pub struct Model(Pending);

pub struct Ticket {
    pub control: Arc<Control>,
    key: String,
    pending: Pending,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.key);
        self.control.release.notify_one();
    }
}

impl Model {
    pub async fn serve(&self) -> Result<(String, JoinHandle<std::io::Result<()>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/v1", listener.local_addr()?);
        let app = Router::new()
            .route("/v1/responses", post(respond))
            .layer(DefaultBodyLimit::disable())
            .with_state(self.clone());
        Ok((
            url,
            tokio::spawn(async move { axum::serve(listener, app).await }),
        ))
    }

    pub fn register(&self, key: &str) -> Ticket {
        let control = Arc::new(Control::default());
        self.0
            .lock()
            .unwrap()
            .insert(key.to_owned(), control.clone());
        Ticket {
            control,
            key: key.to_owned(),
            pending: self.0.clone(),
        }
    }
}

fn item(value: Value) -> String {
    format!(
        "data: {}\n\n",
        json!({"type":"response.output_item.done","item":value})
    )
}
const DONE: &str = "data: {\"type\":\"response.completed\"}\n\n";
fn sse(body: Body) -> Response {
    ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response()
}

async fn respond(State(model): State<Model>, Json(request): Json<Value>) -> Response {
    match reply(model, request).await {
        Ok(response) => response,
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

async fn reply(model: Model, request: Value) -> Result<Response> {
    let input = request["input"].as_array().ok_or("missing input")?;
    let key = input
        .first()
        .and_then(|v| v["content"].as_str())
        .ok_or("missing user message")?;
    let control = model
        .0
        .lock()
        .unwrap()
        .get(key)
        .cloned()
        .ok_or("unknown turn")?;
    let attempt = control.attempts.fetch_add(1, Ordering::Relaxed) + 1;
    let scenario = request["model"].as_str().ok_or("missing model")?;
    check(
        request["stream"] == true && request["store"] == false,
        "wrong Responses settings",
    )?;
    match scenario {
        "slow-turn" => tokio::time::sleep(Duration::from_millis(100)).await,
        "pause-soft" | "pause-hard" | "restart" | "enqueue-running" if attempt == 1 => {
            control.entered.notify_one();
            control.release.notified().await;
        }
        "retry-http" if attempt == 1 => return Ok(StatusCode::TOO_MANY_REQUESTS.into_response()),
        "retry-http" if attempt == 2 => return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response()),
        "retry-exhausted" => return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response()),
        "retry-eof" if attempt == 1 => return Ok(sse(Body::from(": connected\n\n"))),
        "long-context" => {
            check(input.len() == 9, "missing context items")?;
            for (i, v) in input.iter().skip(1).enumerate() {
                let text = v["content"].as_str().ok_or("missing context text")?;
                check(
                    text.len() == 16385
                        && text.starts_with(&i.to_string())
                        && text.as_bytes()[1..].iter().all(|&b| b == b'x'),
                    "long context payload changed",
                )?;
            }
        }
        "multi-turn" | "enqueue-running" => {
            check(input.len() == 2 * attempt - 1, "wrong history length")?;
            for (i, v) in input.iter().enumerate() {
                let role = if i % 2 == 0 { "user" } else { "assistant" };
                check(v["role"] == role, "wrong history order")?;
            }
        }
        "tools" if attempt == 1 => {
            let mut body = String::new();
            for i in 0..2 {
                body.push_str(&item(json!({"type":"function_call","call_id":format!("{key}-{i}"),"name":"current_time","arguments":"{}"})));
            }
            body.push_str(DONE);
            return Ok(sse(Body::from(body)));
        }
        "tools" => {
            check(
                attempt == 2 && input.len() == 5,
                "wrong tool follow-up context",
            )?;
            for (i, [call, result]) in input[1..].as_chunks::<2>().0.iter().enumerate() {
                check(
                    call["type"] == "function_call"
                        && result["type"] == "function_call_output"
                        && call["call_id"] == format!("{key}-{i}")
                        && call["call_id"] == result["call_id"],
                    "unpaired tool output",
                )?;
                chrono::DateTime::parse_from_rfc3339(
                    result["output"].as_str().ok_or("missing tool output")?,
                )?;
            }
        }
        _ => (),
    }
    let answer = item(
        json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}),
    );
    if scenario == "chunked-turn" {
        let body = async_stream::stream! {
            for part in [": keepalive\n\n", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n", &answer[..7], &answer[7..], DONE] {
                yield Ok::<_,Infallible>(Bytes::copy_from_slice(part.as_bytes()));
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        };
        return Ok(sse(Body::from_stream(body)));
    }
    Ok(sse(Body::from(answer + DONE)))
}
