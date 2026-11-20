use crate::{
    Result, check,
    client::{Api, Events, text_message},
    model::{Model, Ticket},
};
use reqwest::Method;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub const CASES: &[(&str, &str)] = &[
    ("read", "读取已有会话"),
    ("sessions", "创建会话、修改设置，通过分页游标查询"),
    ("enqueue", "多个会话分别写入短消息"),
    ("hot-enqueue", "所有客户端同时写入同一会话"),
    ("large-enqueue", "每条消息 64 KiB"),
    ("mixed", "每次操作并发执行一次入队和三次查询"),
    ("idempotency", "并发发送相同幂等键，再用同一键发送冲突内容"),
    ("withdraw", "连续撤回同一条消息两次，核对队列状态"),
    ("invalid", "空消息、不存在的会话和无效分页参数"),
    ("turn", "单个完整回合，模拟模型立即回答"),
    ("slow-turn", "模型等待 100 ms 后回答"),
    (
        "chunked-turn",
        "模型输出含心跳、增量文本和跨数据块的 SSE 事件",
    ),
    ("multi-turn", "同一会话连续三轮，核对模型收到的上下文顺序"),
    (
        "enqueue-running",
        "等待模型期间入队第二条消息，验证自动发起下一轮",
    ),
    (
        "held-sse",
        "同一会话保持指定数量的订阅，持续 --duration 秒后核对广播送达",
    ),
    (
        "long-context",
        "128 KiB 上下文上传，同时核对历史和上下文分页",
    ),
    (
        "tools",
        "并行调用两次 current_time，核对输出与调用配对再完成回答",
    ),
    ("duplicate-run", "同一会话并发启动四次，核对模型调用次数"),
    ("fanout", "每客户端复用八个 SSE 订阅，逐条核对持久化广播"),
    (
        "sse-churn",
        "反复打开八个 SSE 订阅并断开，重连后用历史游标补读",
    ),
    ("retry-http", "模型依次返回 429、503，重试后成功"),
    ("retry-eof", "模型事件流提前结束，重试后成功"),
    ("retry-exhausted", "四次模型请求失败后检查暂停状态与原因"),
    ("pause-soft", "模型请求进行中软暂停，保存回答并开始下一轮"),
    ("pause-hard", "硬暂停取消模型请求，恢复时继续处理原消息"),
    ("restart", "所有会话等待模型时强制重启，逐个恢复并核对答案"),
];

#[derive(Default)]
pub struct Outcome {
    pub users: u64,
    pub answers: u64,
    pub turn_time: Option<Duration>,
}
impl Outcome {
    fn writes(users: u64) -> Self {
        Self {
            users,
            ..Self::default()
        }
    }
}

struct Subscribers {
    path: String,
    readers: Vec<Events>,
}
impl Subscribers {
    async fn connect(api: &Api) -> Result<Self> {
        let path = api.session().await?;
        let mut readers = Vec::new();
        for _ in 0..8 {
            readers.push(api.listen(&path).await?);
        }
        Ok(Self { path, readers })
    }
    async fn broadcast(&mut self, api: &Api) -> Result<u64> {
        let id = api.enqueue(&self.path, "broadcast").await?;
        for reader in &mut self.readers {
            let (name, item) = reader.next().await?;
            check(
                name == "item.saved" && item["id"] == id,
                "fanout event missing or out of order",
            )?;
        }
        Ok(id)
    }
}

#[derive(Clone)]
pub struct Work {
    pub api: Api,
    pub model: Model,
    pub case: String,
    pub sessions: Arc<Vec<String>>,
    sequence: Arc<AtomicU64>,
    subscribers: Arc<Vec<tokio::sync::Mutex<Subscribers>>>,
}

impl Work {
    pub async fn new(api: Api, model: Model, case: &str, clients: usize) -> Result<Self> {
        let mut sessions = vec![];
        if matches!(
            case,
            "read"
                | "enqueue"
                | "hot-enqueue"
                | "large-enqueue"
                | "mixed"
                | "idempotency"
                | "withdraw"
                | "invalid"
        ) {
            for _ in 0..if case == "hot-enqueue" { 1 } else { clients } {
                sessions.push(api.session().await?);
            }
        }
        let mut subscribers = Vec::new();
        if case == "fanout" {
            let mut setup = tokio::task::JoinSet::new();
            for _ in 0..clients {
                let api = api.clone();
                setup.spawn(async move { Subscribers::connect(&api).await });
            }
            while let Some(group) = setup.join_next().await {
                subscribers.push(tokio::sync::Mutex::new(group??));
            }
        }
        Ok(Self {
            subscribers: Arc::new(subscribers),
            api,
            model,
            case: case.to_owned(),
            sessions: Arc::new(sessions),
            sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn run(&self, slot: usize) -> Result<Outcome> {
        if self.case == "sessions" {
            let path = self.api.session().await?;
            let updated = self
                .api
                .json(
                    Method::PATCH,
                    &path,
                    Some(json!({"reasoning_effort":"high"})),
                    200,
                )
                .await?;
            let id = updated["id"].as_u64().ok_or("missing session id")?;
            let list = self
                .api
                .get(&format!("/sessions?after={}&limit=1", id - 1))
                .await?;
            check(
                list[0] == updated && updated["reasoning_effort"] == "high",
                "session update or page mismatch",
            )?;
            return Ok(Outcome::default());
        }
        if self.case == "fanout" {
            self.subscribers[slot]
                .lock()
                .await
                .broadcast(&self.api)
                .await?;
            return Ok(Outcome::writes(1));
        }
        if self.case == "sse-churn" {
            return self.churn().await;
        }
        if self.sessions.is_empty() {
            return self.turn().await;
        }
        let path = &self.sessions[slot % self.sessions.len()];
        let api = &self.api;
        let messages = format!("{path}/messages");
        match self.case.as_str() {
            "read" => {
                check(
                    api.get(path).await?["status"] == "idle",
                    "read wrong session state",
                )?;
                Ok(Outcome::default())
            }
            "enqueue" | "hot-enqueue" | "large-enqueue" => {
                let text = if self.case == "large-enqueue" {
                    "x".repeat(64 * 1024)
                } else {
                    "hello".into()
                };
                api.enqueue(path, &text).await?;
                Ok(Outcome::writes(1))
            }
            "mixed" => {
                let page = format!("{messages}?limit=1");
                let (_, session, config, list) = tokio::try_join!(
                    api.enqueue(path, "mixed"),
                    api.get(path),
                    api.get("/config"),
                    api.get(&page)
                )?;
                check(
                    session["status"] == "idle"
                        && config["default_model_id"].is_u64()
                        && session["model_id"] == config["default_model_id"]
                        && list.is_array(),
                    "invalid mixed response",
                )?;
                Ok(Outcome::writes(1))
            }
            "idempotency" => {
                let key = self.sequence.fetch_add(1, Ordering::Relaxed).to_string();
                let request = |text: &str| {
                    api.request(Method::POST, &messages, Some(text_message(text)))
                        .header("Idempotency-Key", &key)
                };
                let (a, b) = tokio::try_join!(
                    Api::send(request("one"), &[200, 201]),
                    Api::send(request("one"), &[200, 201])
                )?;
                check(
                    a.0 != b.0 && a.1["id"].is_u64() && a.1 == b.1,
                    "duplicate key created inconsistent replies",
                )?;
                Api::send(request("two"), &[409]).await?;
                Ok(Outcome::writes(1))
            }
            "withdraw" => {
                let id = api.enqueue(path, "withdraw").await?;
                for _ in 0..2 {
                    api.json(Method::DELETE, &format!("{messages}/{id}"), None, 204)
                        .await?;
                }
                let found = api
                    .get(&format!("{messages}?id={id}&status=withdrawn"))
                    .await?;
                check(
                    found[0]["id"] == id && found[0]["status"] == "withdrawn",
                    "withdrawal not visible",
                )?;
                Ok(Outcome::writes(1))
            }
            "invalid" => {
                api.json(Method::POST, &messages, Some(text_message(" ")), 400)
                    .await?;
                api.json(Method::GET, "/sessions/0", None, 404).await?;
                api.json(Method::GET, "/sessions?limit=201", None, 400)
                    .await?;
                Ok(Outcome::default())
            }
            _ => Err("unknown API scenario".into()),
        }
    }

    pub async fn held_subscribers(
        &self,
        clients: usize,
        hold: Duration,
        limit: Duration,
    ) -> Result<serde_json::Value> {
        let path = self.api.session().await?;
        let mut subscriptions = tokio::task::JoinSet::new();
        for _ in 0..clients {
            let api = self.api.clone();
            let path = path.clone();
            subscriptions.spawn(async move { api.listen(&path).await });
        }
        let mut readers = Vec::with_capacity(clients);
        tokio::time::timeout(limit, async {
            while let Some(result) = subscriptions.join_next().await {
                readers.push(result??);
            }
            Ok::<(), crate::Error>(())
        })
        .await??;
        tokio::time::sleep(hold).await;
        let start = Instant::now();
        let id = self.api.enqueue(&path, "broadcast").await?;
        let mut deliveries = tokio::task::JoinSet::new();
        for mut reader in readers {
            deliveries.spawn(async move {
                let (name, item) = reader.next().await?;
                check(
                    name == "item.saved" && item["id"] == id,
                    "held subscriber missed broadcast",
                )
            });
        }
        tokio::time::timeout(limit, async {
            while let Some(result) = deliveries.join_next().await {
                result??;
            }
            Ok::<(), crate::Error>(())
        })
        .await??;
        Ok(
            json!({"subscribers":clients,"hold_s":hold.as_secs(),"broadcast_to_all_ms":start.elapsed().as_secs_f64()*1000.0}),
        )
    }

    async fn churn(&self) -> Result<Outcome> {
        let mut group = Subscribers::connect(&self.api).await?;
        let first = group.broadcast(&self.api).await?;
        let path = group.path;
        drop(group.readers);
        let second = self.api.enqueue(&path, "offline").await?;
        let _reconnected = self.api.listen(&path).await?;
        let missed = self
            .api
            .get(&format!("{path}/history?after={first}"))
            .await?;
        check(
            missed.as_array().is_some_and(|items| items.len() == 1) && missed[0]["id"] == second,
            "SSE catch-up failed",
        )?;
        Ok(Outcome::writes(2))
    }

    pub async fn prepare_turn(&self) -> Result<(String, String, Ticket)> {
        let path = self.api.session().await?;
        let key = format!("message-{path}");
        let ticket = self.model.register(&key);
        Ok((path, key, ticket))
    }

    async fn turn(&self) -> Result<Outcome> {
        let (path, key, ticket) = self.prepare_turn().await?;
        let mut events = self.api.listen(&path).await?;
        let start = Instant::now();
        self.api.enqueue(&path, &key).await?;
        let mut result = Outcome::writes(1);
        if self.case == "long-context" {
            for i in 0..8 {
                self.api
                    .enqueue(&path, &format!("{i}{}", "x".repeat(16384)))
                    .await?;
            }
            result.users += 8;
        }
        if self.case == "duplicate-run" {
            tokio::try_join!(
                self.api.start(&path),
                self.api.start(&path),
                self.api.start(&path),
                self.api.start(&path)
            )?;
        } else {
            self.api.start(&path).await?;
        }
        let queued = self.case == "enqueue-running";
        if queued {
            ticket.control.entered.notified().await;
            self.api.enqueue(&path, "next round").await?;
            result.users += 1;
            ticket.control.release.notify_one();
        }
        let soft = self.case == "pause-soft";
        if soft || self.case == "pause-hard" {
            ticket.control.entered.notified().await;
            let how = if soft { "soft" } else { "hard" };
            self.api
                .json(Method::DELETE, &format!("{path}/run?mode={how}"), None, 200)
                .await?;
            if soft {
                ticket.control.release.notify_one();
            }
            let paused = events.terminal("paused").await?;
            check(
                paused.answers == usize::from(soft) && paused.reason == "paused by user",
                "incorrect pause outcome",
            )?;
            result.answers += paused.answers as u64;
            if soft {
                self.api.enqueue(&path, "next round").await?;
                result.users += 1;
            } else {
                ticket.control.release.notify_one();
            }
            self.api.start(&path).await?;
        }
        let retries = match self.case.as_str() {
            "retry-http" => 2,
            "retry-eof" => 1,
            "retry-exhausted" => 3,
            _ => 0,
        };
        if self.case == "retry-exhausted" {
            let paused = events.terminal("paused").await?;
            check(
                paused.answers == 0
                    && paused.retries == retries
                    && paused.reason.contains("retry limit reached")
                    && ticket.control.attempts() == 4,
                "incorrect exhausted retry outcome",
            )?;
            return Ok(result);
        }
        let rounds = if self.case == "multi-turn" { 3 } else { 1 };
        let tools = if self.case == "tools" { 2 } else { 0 };
        for i in 0..rounds {
            if i > 0 {
                self.api.enqueue(&path, "next round").await?;
                result.users += 1;
                self.api.start(&path).await?;
            }
            let seen = events.terminal("idle").await?;
            check(
                seen.answers == 1 + usize::from(queued)
                    && seen.retries == retries
                    && seen.tools == tools
                    && seen.outputs == tools,
                "unexpected number of SSE answers, retries or tool events",
            )?;
            result.answers += seen.answers as u64;
        }
        result.turn_time = Some(start.elapsed());
        let extra = usize::from(tools > 0 || soft || self.case == "pause-hard" || queued);
        check(
            ticket.control.attempts() == rounds + retries + extra,
            "unexpected upstream call count",
        )?;
        if self.case == "long-context" {
            self.pages(&path).await?;
        }
        Ok(result)
    }

    async fn pages(&self, path: &str) -> Result<()> {
        let mut history = Vec::new();
        for (endpoint, cursor) in [("history", "id"), ("context", "position")] {
            let mut ids = Vec::new();
            let mut after = 0;
            loop {
                let page = self
                    .api
                    .get(&format!("{path}/{endpoint}?after={after}&limit=3"))
                    .await?;
                let items = page.as_array().ok_or("invalid page")?;
                check(items.len() <= 3, "page limit exceeded")?;
                if items.is_empty() {
                    break;
                }
                for item in items {
                    let next = item[cursor].as_u64().ok_or("missing page cursor")?;
                    check(
                        next > after && ids.len() < 10,
                        "invalid pagination order or count",
                    )?;
                    after = next;
                    let id = if endpoint == "history" {
                        next
                    } else {
                        item["item_id"].as_u64().ok_or("missing context item")?
                    };
                    ids.push(id);
                }
            }
            check(ids.len() == 10, "incomplete paginated data")?;
            if endpoint == "history" {
                history = ids;
            } else {
                check(ids == history, "history and context disagree")?;
            }
        }
        Ok(())
    }
}
