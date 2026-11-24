use crate::{
    App, Result,
    id::{CallId, SessionId},
    store::session,
};
use axum::{
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::Stream;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
};

const MAX_DRAFT_FIELD_BYTES: usize = 256 * 1024;
const GLOBAL_CHANNEL_CAPACITY: usize = 256;
const EPHEMERAL_CHANNEL_CAPACITY: usize = 256;
static NEXT_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Notice {
    seq: u64,
    name: &'static str,
    data: Arc<str>,
}

impl Notice {
    pub fn publish(channel: &broadcast::Sender<Self>, name: &'static str, data: Value) {
        if channel.receiver_count() > 0 {
            let _ = channel.send(Self {
                seq: NEXT_SEQ.fetch_add(1, Ordering::AcqRel),
                name,
                data: data.to_string().into(),
            });
        }
    }

    fn into_event(self) -> Event {
        Event::default().event(self.name).data(self.data.as_ref())
    }
}

#[derive(Serialize)]
struct DraftItem {
    output_index: u32,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    call_id: Option<String>,
    text: String,
    reasoning: String,
    arguments: String,
}

impl DraftItem {
    fn new(output_index: u32, kind: String, name: Option<String>, call_id: Option<String>) -> Self {
        Self {
            output_index,
            kind,
            name,
            call_id,
            text: String::new(),
            reasoning: String::new(),
            arguments: String::new(),
        }
    }
}

struct ActiveDraft {
    llm_call_id: CallId,
    items: BTreeMap<u32, DraftItem>,
}

#[derive(Default)]
struct SessionStream {
    tx: Option<broadcast::WeakSender<Notice>>,
    draft: Option<ActiveDraft>,
}

struct HubInner {
    global: broadcast::Sender<Notice>,
    sessions: Mutex<HashMap<SessionId, SessionStream>>,
}

#[derive(Clone)]
pub struct Hub(Arc<HubInner>);

impl Default for Hub {
    fn default() -> Self {
        let (global, _) = broadcast::channel(GLOBAL_CHANNEL_CAPACITY);
        Self(Arc::new(HubInner {
            global,
            sessions: Mutex::default(),
        }))
    }
}

impl Hub {
    pub fn global_sender(&self) -> broadcast::Sender<Notice> {
        self.0.global.clone()
    }

    pub fn subscribe_global(&self) -> (broadcast::Sender<Notice>, broadcast::Receiver<Notice>) {
        (self.0.global.clone(), self.0.global.subscribe())
    }

    pub fn subscribe_delta(
        &self,
        sid: SessionId,
    ) -> (
        broadcast::Sender<Notice>,
        broadcast::Receiver<Notice>,
        Option<Value>,
        u64,
    ) {
        let mut sessions = self.0.sessions.lock().unwrap();
        let entry = sessions.entry(sid).or_default();
        let tx = match entry.tx.as_ref().and_then(|w| w.upgrade()) {
            Some(tx) => tx,
            None => {
                let (tx, _) = broadcast::channel(EPHEMERAL_CHANNEL_CAPACITY);
                entry.tx = Some(tx.downgrade());
                tx
            }
        };
        let rx = tx.subscribe();
        let min_seq = NEXT_SEQ.load(Ordering::Acquire);
        let snapshot = entry
            .draft
            .as_ref()
            .filter(|d| !d.items.is_empty())
            .map(|d| {
                json!({
                    "llm_call_id": d.llm_call_id,
                    "items": d.items.values().collect::<Vec<_>>(),
                })
            });
        (tx, rx, snapshot, min_seq)
    }

    pub fn start_call(&self, sid: SessionId, llm_call_id: CallId) {
        let mut sessions = self.0.sessions.lock().unwrap();
        let entry = sessions.entry(sid).or_default();
        entry.draft = Some(ActiveDraft {
            llm_call_id,
            items: BTreeMap::new(),
        });
    }

    pub fn item_started(
        &self,
        sid: SessionId,
        llm_call_id: CallId,
        output_index: u32,
        kind: String,
        name: Option<String>,
        call_id: Option<String>,
    ) {
        let mut sessions = self.0.sessions.lock().unwrap();
        let entry = sessions.entry(sid).or_default();
        let draft = entry.draft.get_or_insert_with(|| ActiveDraft {
            llm_call_id,
            items: BTreeMap::new(),
        });
        if draft.llm_call_id != llm_call_id {
            *draft = ActiveDraft {
                llm_call_id,
                items: BTreeMap::new(),
            };
        }
        draft.items.insert(
            output_index,
            DraftItem::new(output_index, kind.clone(), name.clone(), call_id.clone()),
        );
        if let Some(tx) = entry.tx.as_ref().and_then(|w| w.upgrade()) {
            Notice::publish(
                &tx,
                "item.started",
                json!({
                    "llm_call_id": llm_call_id,
                    "output_index": output_index,
                    "kind": kind,
                    "name": name,
                    "call_id": call_id,
                }),
            );
        }
    }

    pub fn append_delta(
        &self,
        sid: SessionId,
        llm_call_id: CallId,
        output_index: u32,
        field: &'static str,
        delta: &str,
    ) {
        if delta.is_empty() {
            return;
        }
        let mut sessions = self.0.sessions.lock().unwrap();
        let entry = sessions.entry(sid).or_default();
        let draft = entry.draft.get_or_insert_with(|| ActiveDraft {
            llm_call_id,
            items: BTreeMap::new(),
        });
        if draft.llm_call_id != llm_call_id {
            *draft = ActiveDraft {
                llm_call_id,
                items: BTreeMap::new(),
            };
        }
        let item = draft.items.entry(output_index).or_insert_with(|| {
            let kind = match field {
                "arguments" => "function_call",
                "reasoning" => "reasoning",
                _ => "message",
            };
            DraftItem::new(output_index, kind.to_owned(), None, None)
        });
        let target = match field {
            "reasoning" => &mut item.reasoning,
            "arguments" => &mut item.arguments,
            _ => &mut item.text,
        };
        if target.len() < MAX_DRAFT_FIELD_BYTES {
            let keep = delta.floor_char_boundary(MAX_DRAFT_FIELD_BYTES - target.len());
            target.push_str(&delta[..keep]);
        }
        if let Some(tx) = entry.tx.as_ref().and_then(|w| w.upgrade()) {
            Notice::publish(
                &tx,
                "item.delta",
                json!({
                    "llm_call_id": llm_call_id,
                    "output_index": output_index,
                    "field": field,
                    "delta": delta,
                }),
            );
        }
    }

    pub fn finish_item(&self, sid: SessionId, llm_call_id: CallId, output_index: u32) {
        let mut sessions = self.0.sessions.lock().unwrap();
        let Some(entry) = sessions.get_mut(&sid) else {
            return;
        };
        if let Some(draft) = &mut entry.draft
            && draft.llm_call_id == llm_call_id
        {
            draft.items.remove(&output_index);
        }
    }

    pub fn clear_call(&self, sid: SessionId, llm_call_id: CallId, emit_clear: bool) {
        let mut sessions = self.0.sessions.lock().unwrap();
        if let Some(entry) = sessions.get_mut(&sid)
            && entry
                .draft
                .as_ref()
                .is_some_and(|d| d.llm_call_id == llm_call_id)
        {
            let old = entry.draft.take().unwrap();
            if emit_clear
                && !old.items.is_empty()
                && let Some(tx) = entry.tx.as_ref().and_then(|w| w.upgrade())
            {
                Notice::publish(&tx, "stream.clear", json!({"llm_call_id": llm_call_id}));
            }
        }
        sessions.retain(|_, s| {
            s.draft.is_some() || s.tx.as_ref().is_some_and(|w| w.strong_count() > 0)
        });
    }

    pub fn remove_session(&self, sid: SessionId) {
        self.0.sessions.lock().unwrap().remove(&sid);
    }
}

pub async fn subscribe_global(
    State(app): State<App>,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let (tx, mut rx) = app.streams.subscribe_global();
    let stream = async_stream::stream! {
        let _tx = tx;
        yield Ok(Event::default().event("ready").data("{}"));
        loop {
            let received = tokio::select! {
                biased;
                _ = app.stopping.cancelled() => break,
                msg = rx.recv() => msg,
            };
            match received {
                Ok(msg) => yield Ok(msg.into_event()),
                Err(RecvError::Lagged(_)) => yield Ok(Event::default().event("resync").data("{}")),
                Err(RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn take_older_deltas(
    buffered_delta: &mut Option<Notice>,
    delta_rx: &mut broadcast::Receiver<Notice>,
    delta_open: &mut bool,
    min_seq: u64,
    before_seq: u64,
) -> Vec<Notice> {
    let mut ready = Vec::new();
    if let Some(b) = buffered_delta.as_ref()
        && b.seq < before_seq
    {
        let b = buffered_delta.take().unwrap();
        if b.seq >= min_seq {
            ready.push(b);
        }
    }
    while buffered_delta.is_none() && *delta_open {
        match delta_rx.try_recv() {
            Ok(m) if m.seq < min_seq => continue,
            Ok(m) if m.seq < before_seq => ready.push(m),
            Ok(m) => *buffered_delta = Some(m),
            Err(TryRecvError::Lagged(_)) => continue,
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Closed) => *delta_open = false,
        }
    }
    ready
}

pub async fn subscribe(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
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
    let (delta_tx, mut delta_rx, snapshot, min_seq) = app.streams.subscribe_delta(sid);
    let stream = async_stream::stream! {
        let _channel = channel;
        let _delta = delta_tx;
        yield Ok(Event::default().event("ready").data(serde_json::json!(initial).to_string()));
        if let Some(snap) = snapshot {
            yield Ok(Event::default().event("stream.snapshot").data(snap.to_string()));
        }
        let mut buffered_delta: Option<Notice> = None;
        let mut delta_open = true;
        loop {
            while buffered_delta.is_some() {
                match rx.try_recv() {
                    Ok(d) => {
                        for m in take_older_deltas(
                            &mut buffered_delta,
                            &mut delta_rx,
                            &mut delta_open,
                            min_seq,
                            d.seq,
                        ) {
                            yield Ok(m.into_event());
                        }
                        yield Ok(d.into_event());
                    }
                    Err(TryRecvError::Lagged(_)) => {
                        buffered_delta = None;
                        yield Ok(Event::default().event("resync").data("{}"));
                    }
                    Err(TryRecvError::Closed) => {
                        if let Some(b) = buffered_delta.take()
                            && b.seq >= min_seq
                        {
                            yield Ok(b.into_event());
                        }
                        return;
                    }
                    Err(TryRecvError::Empty) => {
                        let b = buffered_delta.take().unwrap();
                        if b.seq >= min_seq {
                            yield Ok(b.into_event());
                        }
                    }
                }
            }
            enum Incoming {
                Durable(std::result::Result<Notice, RecvError>),
                Delta(std::result::Result<Notice, RecvError>),
            }
            let received = tokio::select! {
                biased;
                _ = app.stopping.cancelled() => break,
                msg = rx.recv() => Incoming::Durable(msg),
                msg = delta_rx.recv(), if delta_open => Incoming::Delta(msg),
            };
            match received {
                Incoming::Durable(Ok(d)) => {
                    for m in take_older_deltas(
                        &mut buffered_delta,
                        &mut delta_rx,
                        &mut delta_open,
                        min_seq,
                        d.seq,
                    ) {
                        yield Ok(m.into_event());
                    }
                    yield Ok(d.into_event());
                }
                Incoming::Durable(Err(RecvError::Lagged(_))) => {
                    buffered_delta = None;
                    yield Ok(Event::default().event("resync").data("{}"));
                }
                Incoming::Durable(Err(RecvError::Closed)) => break,
                Incoming::Delta(Ok(m)) => {
                    if m.seq >= min_seq {
                        buffered_delta = Some(m);
                    }
                }
                Incoming::Delta(Err(RecvError::Lagged(_))) => continue,
                Incoming::Delta(Err(RecvError::Closed)) => delta_open = false,
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
