use crate::{App, Result, store::session};
use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;
use serde_json::Value;
use std::{convert::Infallible, sync::Arc};
use tokio::sync::broadcast::{self, error::RecvError};

#[derive(Clone)]
pub struct Notice {
    name: &'static str,
    data: Arc<str>,
}
impl Notice {
    pub fn publish(channel: &broadcast::Sender<Self>, name: &'static str, data: Value) {
        if channel.receiver_count() > 0 {
            let _ = channel.send(Self {
                name,
                data: data.to_string().into(),
            });
        }
    }
}
/// 订阅会话事件
///
/// 连接后首先收到 ready 和当前会话。item.saved、session.status、attempt.retry 等事件携带 JSON。收到 resync 时重新查询状态和历史。
#[utoipa::path(
    get,
    path = "/sessions/{sid}/events",
    tag = "运行",
    params(("sid" = i64, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "持续推送的 SSE 事件", body = String, content_type = "text/event-stream", example = json!("event: ready\ndata: {\"id\":1,\"status\":\"idle\",\"reasoning_effort\":null,\"last_error\":\"\",\"model_id\":null}\n\n")),
        (status = 400, description = "参数无法解析", body = String, content_type = "text/plain"),
        (status = 404, description = "记录不存在", body = crate::ErrorBody),
        crate::docs::ServiceErrors
    )
)]
pub async fn subscribe(
    State(app): State<App>,
    Path(sid): Path<i64>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    let (channel, mut rx, initial) = app
        .db
        .call(move |s| {
            let initial = session(&s.conn, sid)?;
            let channel = s.channel(sid);
            let rx = channel.subscribe();
            Ok((channel, rx, initial))
        })
        .await?;
    let stream = async_stream::stream! {
        let _channel=channel;
        yield Ok(Event::default().event("ready").data(serde_json::json!(initial).to_string()));
        loop {
            let received=tokio::select!{ biased; _=app.stopping.cancelled()=>break, msg=rx.recv()=>msg };
            match received {
                Ok(msg)=>yield Ok(Event::default().event(msg.name).data(msg.data.as_ref())),
                Err(RecvError::Lagged(_))=>yield Ok(Event::default().event("resync").data("{}")),
                Err(RecvError::Closed)=>break,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
