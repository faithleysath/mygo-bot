use crate::{
    App, Result,
    cluster::exec::RelayedEvent,
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
    borrow::Cow,
    collections::{BTreeMap, HashMap, VecDeque},
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
};
use tokio_util::sync::CancellationToken;

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

    pub fn publish_serializable<T: Serialize>(
        channel: &broadcast::Sender<Self>,
        name: &'static str,
        data: &T,
    ) {
        if channel.receiver_count() > 0
            && let Ok(text) = serde_json::to_string(data)
        {
            let _ = channel.send(Self {
                seq: NEXT_SEQ.fetch_add(1, Ordering::AcqRel),
                name,
                data: text.into(),
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
    last_cleared: Option<CallId>,
}

enum DraftRecovery {
    Snapshot(Value),
    Clear(CallId),
    None,
}

fn draft_snapshot(draft: &ActiveDraft) -> Value {
    json!({
        "llm_call_id": draft.llm_call_id,
        "items": draft.items.values().collect::<Vec<_>>(),
    })
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

    fn subscribe_delta(
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
            .map(draft_snapshot);
        (tx, rx, snapshot, min_seq)
    }

    fn recovery(&self, sid: SessionId) -> (DraftRecovery, u64) {
        let sessions = self.0.sessions.lock().unwrap();
        let min_seq = NEXT_SEQ.load(Ordering::Acquire);
        let recovery = match sessions.get(&sid) {
            Some(entry) => {
                if let Some(d) = &entry.draft {
                    DraftRecovery::Snapshot(draft_snapshot(d))
                } else if let Some(call_id) = entry.last_cleared {
                    DraftRecovery::Clear(call_id)
                } else {
                    DraftRecovery::None
                }
            }
            None => DraftRecovery::None,
        };
        (recovery, min_seq)
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
            entry.last_cleared = Some(llm_call_id);
            if emit_clear
                && !old.items.is_empty()
                && let Some(tx) = entry.tx.as_ref().and_then(|w| w.upgrade())
            {
                Notice::publish(&tx, "stream.clear", json!({"llm_call_id": llm_call_id}));
            }
        }
        Self::remove_idle_stream(&mut sessions, sid);
    }

    fn remove_idle_stream(sessions: &mut HashMap<SessionId, SessionStream>, sid: SessionId) {
        if let std::collections::hash_map::Entry::Occupied(entry) = sessions.entry(sid) {
            let stream = entry.get();
            if stream.draft.is_none() && stream.tx.as_ref().is_none_or(|w| w.strong_count() == 0) {
                entry.remove();
            }
        }
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
) -> (Vec<Notice>, bool) {
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
            Err(TryRecvError::Lagged(_)) => {
                *buffered_delta = None;
                return (Vec::new(), true);
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Closed) => *delta_open = false,
        }
    }
    (ready, false)
}

pub enum NextSessionNotice {
    Event {
        event: Cow<'static, str>,
        data: Arc<str>,
    },
    ClusterActive,
    Closed,
}

pub struct SessionEventStream {
    sid: SessionId,
    _channel: broadcast::Sender<Notice>,
    rx: broadcast::Receiver<Notice>,
    hub: Hub,
    delta_sender: Option<broadcast::Sender<Notice>>,
    delta_rx: broadcast::Receiver<Notice>,
    initial_payload: String,
    initial_snapshot: Option<Arc<str>>,
    buffered_delta: Option<Notice>,
    queued: VecDeque<(Cow<'static, str>, Arc<str>)>,
    delta_open: bool,
    delta_lagged: bool,
    min_seq: u64,
}

impl Drop for SessionEventStream {
    fn drop(&mut self) {
        drop(self.delta_sender.take());
        Hub::remove_idle_stream(&mut self.hub.0.sessions.lock().unwrap(), self.sid);
    }
}

impl SessionEventStream {
    pub async fn open(app: &App, sid: SessionId) -> Result<Option<Self>> {
        let opened = app
            .db
            .call(move |s| {
                let info = match session(&s.conn, sid) {
                    Ok(v) => v,
                    Err(e) if e.0 == axum::http::StatusCode::NOT_FOUND => return Ok(None),
                    Err(e) => return Err(e),
                };
                let initial = s.session_view(info);
                let history_rev = crate::cluster::ledger::session_history_rev(&s.conn, sid)?;
                let mut payload = serde_json::to_string(&initial)
                    .map_err(|e| crate::Error::internal(e.to_string()))?;
                if payload.ends_with('}') {
                    payload.pop();
                    use std::fmt::Write;
                    let _ = write!(payload, ",\"history_rev\":{history_rev}}}");
                }
                let channel = s.channel(sid);
                let rx = channel.subscribe();
                Ok(Some((channel, rx, payload)))
            })
            .await?;
        let Some((channel, rx, initial_payload)) = opened else {
            return Ok(None);
        };
        let (delta_tx, delta_rx, snapshot, min_seq) = app.streams.subscribe_delta(sid);
        Ok(Some(Self {
            sid,
            _channel: channel,
            rx,
            hub: app.streams.clone(),
            delta_sender: Some(delta_tx),
            delta_rx,
            initial_payload,
            initial_snapshot: snapshot.map(|s| Arc::from(s.to_string())),
            buffered_delta: None,
            queued: VecDeque::new(),
            delta_open: true,
            delta_lagged: false,
            min_seq,
        }))
    }

    pub fn initial_payload(&self) -> &str {
        &self.initial_payload
    }

    pub fn take_initial_snapshot(&mut self) -> Option<Arc<str>> {
        self.initial_snapshot.take()
    }

    fn push_notice(&mut self, notice: Notice) {
        self.queued
            .push_back((Cow::Borrowed(notice.name), notice.data));
    }

    async fn resync_payload(&self, app: &App) -> Arc<str> {
        let sid = self.sid;
        let rev = app
            .db
            .call(move |s| crate::cluster::ledger::session_history_rev(&s.conn, sid))
            .await
            .unwrap_or(0);
        Arc::from(json!({"history_rev": rev}).to_string())
    }

    pub async fn next(&mut self, app: &App) -> NextSessionNotice {
        self.next_inner(app, None).await
    }

    async fn next_or_cluster(
        &mut self,
        app: &App,
        cluster_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> NextSessionNotice {
        self.next_inner(app, Some(cluster_rx)).await
    }

    async fn next_inner(
        &mut self,
        app: &App,
        mut cluster_rx: Option<&mut tokio::sync::watch::Receiver<bool>>,
    ) -> NextSessionNotice {
        loop {
            if let Some((event, data)) = self.queued.pop_front() {
                return NextSessionNotice::Event { event, data };
            }
            while self.delta_lagged {
                if app.stopping.is_cancelled() {
                    return NextSessionNotice::Closed;
                }
                self.buffered_delta = None;
                loop {
                    match self.rx.try_recv() {
                        Ok(d) => {
                            self.push_notice(d);
                        }
                        Err(TryRecvError::Lagged(_)) => {
                            let data = self.resync_payload(app).await;
                            self.queued.push_back((Cow::Borrowed("resync"), data));
                        }
                        Err(TryRecvError::Closed) => {
                            if let Some((event, data)) = self.queued.pop_front() {
                                return NextSessionNotice::Event { event, data };
                            }
                            return NextSessionNotice::Closed;
                        }
                        Err(TryRecvError::Empty) => break,
                    }
                }
                if let Some((event, data)) = self.queued.pop_front() {
                    return NextSessionNotice::Event { event, data };
                }
                let (recovery, new_min_seq) = app.streams.recovery(self.sid);
                self.min_seq = self.min_seq.max(new_min_seq);
                let mut relagged = false;
                while self.buffered_delta.is_none() && self.delta_open {
                    match self.delta_rx.try_recv() {
                        Ok(m) if m.seq < self.min_seq => continue,
                        Ok(m) => self.buffered_delta = Some(m),
                        Err(TryRecvError::Lagged(_)) => {
                            relagged = true;
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => self.delta_open = false,
                    }
                }
                if relagged {
                    continue;
                }
                match self.rx.try_recv() {
                    Ok(d) => {
                        return NextSessionNotice::Event {
                            event: Cow::Borrowed(d.name),
                            data: d.data,
                        };
                    }
                    Err(TryRecvError::Lagged(_)) => {
                        return NextSessionNotice::Event {
                            event: Cow::Borrowed("resync"),
                            data: self.resync_payload(app).await,
                        };
                    }
                    Err(TryRecvError::Closed) => return NextSessionNotice::Closed,
                    Err(TryRecvError::Empty) => {}
                }
                self.delta_lagged = false;
                match recovery {
                    DraftRecovery::Snapshot(snap) => {
                        return NextSessionNotice::Event {
                            event: Cow::Borrowed("stream.snapshot"),
                            data: Arc::from(snap.to_string()),
                        };
                    }
                    DraftRecovery::Clear(call_id) => {
                        return NextSessionNotice::Event {
                            event: Cow::Borrowed("stream.clear"),
                            data: Arc::from(json!({"llm_call_id": call_id}).to_string()),
                        };
                    }
                    DraftRecovery::None => {}
                }
            }
            while self.buffered_delta.is_some() {
                match self.rx.try_recv() {
                    Ok(d) => {
                        let (older, lagged) = take_older_deltas(
                            &mut self.buffered_delta,
                            &mut self.delta_rx,
                            &mut self.delta_open,
                            self.min_seq,
                            d.seq,
                        );
                        for m in older {
                            self.push_notice(m);
                        }
                        self.push_notice(d);
                        if lagged {
                            self.delta_lagged = true;
                            self.buffered_delta = None;
                            break;
                        }
                        if let Some((event, data)) = self.queued.pop_front() {
                            return NextSessionNotice::Event { event, data };
                        }
                    }
                    Err(TryRecvError::Lagged(_)) => {
                        self.buffered_delta = None;
                        self.delta_lagged = true;
                        return NextSessionNotice::Event {
                            event: Cow::Borrowed("resync"),
                            data: self.resync_payload(app).await,
                        };
                    }
                    Err(TryRecvError::Closed) => {
                        if let Some(b) = self.buffered_delta.take()
                            && b.seq >= self.min_seq
                        {
                            return NextSessionNotice::Event {
                                event: Cow::Borrowed(b.name),
                                data: b.data,
                            };
                        }
                        return NextSessionNotice::Closed;
                    }
                    Err(TryRecvError::Empty) => {
                        let b = self.buffered_delta.take().unwrap();
                        if b.seq >= self.min_seq {
                            return NextSessionNotice::Event {
                                event: Cow::Borrowed(b.name),
                                data: b.data,
                            };
                        }
                    }
                }
            }
            if let Some((event, data)) = self.queued.pop_front() {
                return NextSessionNotice::Event { event, data };
            }
            if self.delta_lagged {
                continue;
            }
            enum Incoming {
                Durable(std::result::Result<Notice, RecvError>),
                Delta(std::result::Result<Notice, RecvError>),
                ClusterChanged(std::result::Result<(), tokio::sync::watch::error::RecvError>),
            }
            let received = match cluster_rx.as_deref_mut() {
                Some(rx_cluster) => tokio::select! {
                    biased;
                    _ = app.stopping.cancelled() => return NextSessionNotice::Closed,
                    res = rx_cluster.changed() => Incoming::ClusterChanged(res),
                    msg = self.rx.recv() => Incoming::Durable(msg),
                    msg = self.delta_rx.recv(), if self.delta_open => Incoming::Delta(msg),
                },
                None => tokio::select! {
                    biased;
                    _ = app.stopping.cancelled() => return NextSessionNotice::Closed,
                    msg = self.rx.recv() => Incoming::Durable(msg),
                    msg = self.delta_rx.recv(), if self.delta_open => Incoming::Delta(msg),
                },
            };
            match received {
                Incoming::ClusterChanged(res) => {
                    if let Some(rx_cluster) = cluster_rx.as_deref_mut()
                        && res.is_ok()
                        && *rx_cluster.borrow_and_update()
                    {
                        return NextSessionNotice::ClusterActive;
                    }
                }
                Incoming::Durable(Ok(d)) => {
                    let (older, lagged) = take_older_deltas(
                        &mut self.buffered_delta,
                        &mut self.delta_rx,
                        &mut self.delta_open,
                        self.min_seq,
                        d.seq,
                    );
                    for m in older {
                        self.push_notice(m);
                    }
                    self.push_notice(d);
                    if lagged {
                        self.delta_lagged = true;
                        self.buffered_delta = None;
                    }
                }
                Incoming::Durable(Err(RecvError::Lagged(_))) => {
                    self.buffered_delta = None;
                    self.delta_lagged = true;
                    return NextSessionNotice::Event {
                        event: Cow::Borrowed("resync"),
                        data: self.resync_payload(app).await,
                    };
                }
                Incoming::Durable(Err(RecvError::Closed)) => return NextSessionNotice::Closed,
                Incoming::Delta(Ok(m)) => {
                    if m.seq >= self.min_seq {
                        self.buffered_delta = Some(m);
                    }
                }
                Incoming::Delta(Err(RecvError::Lagged(_))) => {
                    self.delta_lagged = true;
                    self.buffered_delta = None;
                }
                Incoming::Delta(Err(RecvError::Closed)) => self.delta_open = false,
            }
        }
    }
}

struct SeenSavedItems {
    order: VecDeque<String>,
    set: std::collections::HashSet<String>,
}

impl SeenSavedItems {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            set: std::collections::HashSet::new(),
        }
    }

    fn accept(&mut self, event: &str, data: &str) -> bool {
        if event != "item.saved" {
            return true;
        }
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            return true;
        };
        let Some(id) = v.get("id").and_then(Value::as_str) else {
            return true;
        };
        if !self.set.insert(id.to_owned()) {
            return false;
        }
        self.order.push_back(id.to_owned());
        if self.order.len() > 256
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        true
    }
}

struct ActiveRelay {
    peer_id: String,
    cancel: CancellationToken,
    rx: tokio::sync::mpsc::Receiver<RelayedEvent>,
}

impl Drop for ActiveRelay {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub async fn subscribe(
    State(app): State<App>,
    Path(sid): Path<SessionId>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    let multi_node = app.exec.presence().known_peer_count() > 0;
    if multi_node {
        app.cluster.ensure_session_watched(sid).await?;
    }
    let mut local = SessionEventStream::open(&app, sid)
        .await?
        .ok_or_else(|| crate::Error::from(rusqlite::Error::QueryReturnedNoRows))?;
    let mut cluster_rx = app.exec.presence().subscribe_cluster_active();
    let stream = async_stream::stream! {
        yield Ok(Event::default().event("ready").data(local.initial_payload()));
        let mut snapshot_sent = false;
        if !multi_node && !*cluster_rx.borrow_and_update() {
            if let Some(snap) = local.take_initial_snapshot() {
                yield Ok(Event::default().event("stream.snapshot").data(snap.as_ref()));
            }
            snapshot_sent = true;
            loop {
                match local.next_or_cluster(&app, &mut cluster_rx).await {
                    NextSessionNotice::Event { event, data } => {
                        yield Ok(Event::default().event(event).data(data.as_ref()));
                    }
                    NextSessionNotice::Closed => return,
                    NextSessionNotice::ClusterActive => break,
                }
            }
        }
        drop(cluster_rx);
        if !multi_node {
            let _ = app.cluster.ensure_session_watched(sid).await;
        }
        let mut runner_rx = app.exec.presence().watch_remote_runner(sid);
        let mut relay: Option<ActiveRelay> = None;
        let mut seen: Option<SeenSavedItems> = None;
        if let Some(peer_id) = runner_rx.borrow().clone() {
            let cancel = app.stopping.child_token();
            let rx = app.exec.spawn_event_relay(peer_id.clone(), sid, cancel.clone());
            relay = Some(ActiveRelay { peer_id, cancel, rx });
            seen = Some(SeenSavedItems::new());
            local.take_initial_snapshot();
        } else if !snapshot_sent && let Some(snap) = local.take_initial_snapshot() {
            yield Ok(Event::default().event("stream.snapshot").data(snap.as_ref()));
        } else {
            local.take_initial_snapshot();
        }
        let mut retry_attempts: u32 = 0;
        let mut retry_at: Option<tokio::time::Instant> = None;
        loop {
            enum SseTurn {
                Local(NextSessionNotice),
                RunnerChanged(std::result::Result<(), tokio::sync::watch::error::RecvError>),
                Relayed(Option<RelayedEvent>),
                RetryTimer,
            }
            let turn = match (relay.as_mut(), retry_at) {
                (Some(active), _) => tokio::select! {
                    biased;
                    _ = app.stopping.cancelled() => break,
                    ch = runner_rx.changed() => SseTurn::RunnerChanged(ch),
                    ev = active.rx.recv() => SseTurn::Relayed(ev),
                    ev = local.next(&app) => SseTurn::Local(ev),
                },
                (None, Some(deadline)) => tokio::select! {
                    biased;
                    _ = app.stopping.cancelled() => break,
                    ch = runner_rx.changed() => SseTurn::RunnerChanged(ch),
                    _ = tokio::time::sleep_until(deadline) => SseTurn::RetryTimer,
                    ev = local.next(&app) => SseTurn::Local(ev),
                },
                (None, None) => tokio::select! {
                    biased;
                    _ = app.stopping.cancelled() => break,
                    ch = runner_rx.changed() => SseTurn::RunnerChanged(ch),
                    ev = local.next(&app) => SseTurn::Local(ev),
                },
            };
            match turn {
                SseTurn::RunnerChanged(Ok(())) => {
                    let next_peer = runner_rx.borrow().clone();
                    if relay.as_ref().map(|r| &r.peer_id) != next_peer.as_ref() {
                        if let Some(old) = relay.take() {
                            old.cancel.cancel();
                        }
                        retry_at = None;
                        retry_attempts = 0;
                        if let Some(peer_id) = next_peer {
                            let cancel = app.stopping.child_token();
                            let rx = app.exec.spawn_event_relay(peer_id.clone(), sid, cancel.clone());
                            relay = Some(ActiveRelay { peer_id, cancel, rx });
                            if seen.is_none() {
                                seen = Some(SeenSavedItems::new());
                            }
                        }
                    }
                }
                SseTurn::RunnerChanged(Err(_)) => break,
                SseTurn::RetryTimer => {
                    retry_at = None;
                    if relay.is_none()
                        && let Some(peer_id) = runner_rx.borrow().clone()
                    {
                        let cancel = app.stopping.child_token();
                        let rx = app.exec.spawn_event_relay(peer_id.clone(), sid, cancel.clone());
                        relay = Some(ActiveRelay { peer_id, cancel, rx });
                        if seen.is_none() {
                            seen = Some(SeenSavedItems::new());
                        }
                    }
                }
                SseTurn::Relayed(Some(RelayedEvent::Event { event, data })) => {
                    retry_attempts = 0;
                    let accept = seen
                        .as_mut()
                        .is_none_or(|s| s.accept(&event, &data));
                    if accept {
                        yield Ok(Event::default().event(event).data(data));
                    }
                }
                SseTurn::Relayed(Some(RelayedEvent::Disconnected)) | SseTurn::Relayed(None) => {
                    if let Some(old) = relay.take() {
                        old.cancel.cancel();
                    }
                    let rev = app
                        .db
                        .call(move |s| crate::cluster::ledger::session_history_rev(&s.conn, sid))
                        .await
                        .unwrap_or(0);
                    yield Ok(Event::default().event("resync").data(json!({"history_rev": rev}).to_string()));
                    if runner_rx.borrow().is_some() {
                        let shift = retry_attempts.min(3);
                        let delay_ms = 250u64 << shift;
                        retry_attempts = retry_attempts.saturating_add(1);
                        retry_at = Some(tokio::time::Instant::now() + Duration::from_millis(delay_ms));
                    } else {
                        retry_at = None;
                        retry_attempts = 0;
                    }
                }
                SseTurn::Local(NextSessionNotice::Event { event, data }) => {
                    let accept = seen
                        .as_mut()
                        .is_none_or(|s| s.accept(event.as_ref(), data.as_ref()));
                    if accept {
                        yield Ok(Event::default().event(event).data(data.as_ref()));
                    }
                }
                SseTurn::Local(NextSessionNotice::ClusterActive) => continue,
                SseTurn::Local(NextSessionNotice::Closed) => break,
            }
        }
        if let Some(old) = relay.take() {
            old.cancel.cancel();
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
