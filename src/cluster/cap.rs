use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use axum::body::Bytes;
use futures_util::StreamExt;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, mpsc},
};
use tokio_util::sync::CancellationToken;

use crate::{
    App,
    cluster::{
        identity::PeerInfo,
        net::{
            ClusterNet, FrameReadErr as ReadFrameErr, PermittedBytes, read_json_frame,
            write_json_frame,
        },
    },
    codemode::{
        CommandGuard, CommandPreview, Host, HostError, McpGuard, RemoteCommandProgress, Reply,
        Stream, StreamResource, files,
        io::{self as cm_io, BODY_LIMIT},
    },
    id::{ItemId, SessionId},
};

const CAP_REQ_HEADER_MAX: usize = 512 * 1024;
const CAP_REQ_BODY_MAX: usize = BODY_LIMIT;
const CAP_RESP_HEADER_MAX: usize = 4 * 1024 * 1024;
const CAP_RESP_BODY_MAX: usize = 16 * 1024 * 1024;
const CAP_CHUNK_MAX: usize = 64 * 1024;
const CAP_BYTE_UNIT: usize = 64 * 1024;
const MAX_CAP_BYTE_PERMITS: usize = 1024; // 64 MiB global cap byte budget
const MAX_GLOBAL_INBOUND_CAP: usize = 128;
const MAX_PEER_INBOUND_CAP: usize = 64;

const CAP_REQ_READ_TIMEOUT: Duration = Duration::from_secs(15);
const CAP_ERROR_PEEK_TIMEOUT: Duration = Duration::from_secs(2);
const CAP_PROC_REAP_TIMEOUT: Duration = Duration::from_secs(3);

fn client_byte_budget() -> Arc<Semaphore> {
    static BUDGET: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    BUDGET
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CAP_BYTE_PERMITS)))
        .clone()
}

async fn write_cap_frame(
    send: &mut SendStream,
    header: &impl Serialize,
    body: Option<&[u8]>,
) -> std::result::Result<(), String> {
    write_json_frame(send, header, body, "cap").await
}

async fn read_cap_frame<T: DeserializeOwned>(
    recv: &mut RecvStream,
    max_header: usize,
    max_body: usize,
    byte_budget: &Arc<Semaphore>,
) -> std::result::Result<(T, PermittedBytes), ReadFrameErr> {
    read_json_frame(recv, max_header, max_body, byte_budget, CAP_BYTE_UNIT, "capability").await
}

async fn read_next_server_msg(
    mut recv: RecvStream,
    max_body: usize,
    byte_budget: Arc<Semaphore>,
) -> (
    RecvStream,
    std::result::Result<(CapServerMsg, PermittedBytes), ReadFrameErr>,
) {
    let res = read_cap_frame(&mut recv, CAP_RESP_HEADER_MAX, max_body, &byte_budget).await;
    (recv, res)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HostErrorWire {
    message: String,
    code: Option<String>,
}

impl From<HostError> for HostErrorWire {
    fn from(err: HostError) -> Self {
        Self {
            code: err.code().map(str::to_owned),
            message: err.message().to_owned(),
        }
    }
}

impl From<HostErrorWire> for HostError {
    fn from(wire: HostErrorWire) -> Self {
        match wire.code {
            Some(code) => HostError::coded(code, wire.message),
            None => HostError::from(wire.message),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CapReq {
    Call {
        method: String,
        args: Value,
        has_data: bool,
        sid: Option<String>,
        origin: Option<String>,
    },
    FsReadStream {
        path: String,
    },
    FsWriteStream {
        path: String,
        append: bool,
    },
    FsReadDir {
        path: String,
    },
    HttpStream {
        method: String,
        args: Value,
        has_data: bool,
        has_upload: bool,
        stream_response: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CapClientMsg {
    Chunk,
    Eof,
    UploadAbort,
    Cancel,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CapServerMsg {
    CommandStarted {
        cwd: String,
    },
    CommandProgress {
        stream: String,
        dropped: u64,
    },
    McpPhase {
        phase: String,
    },
    Opened,
    HttpHead {
        status: u16,
        headers: Map<String, Value>,
    },
    Chunk,
    DirEntry {
        name: String,
        #[serde(rename = "type")]
        kind: String,
    },
    End,
    ProcResult {
        exit_code: Option<i64>,
        signal: Option<i32>,
        timed_out: bool,
        stdout_len: usize,
        stderr_len: usize,
        truncated: bool,
    },
    Reply {
        value: Value,
        has_data: bool,
    },
    Err(HostErrorWire),
}

struct CapStreamGuard {
    send: Option<SendStream>,
    recv: Option<RecvStream>,
    finished: bool,
}

impl CapStreamGuard {
    fn new(send: Option<SendStream>, recv: RecvStream) -> Self {
        Self {
            send,
            recv: Some(recv),
            finished: false,
        }
    }

    fn mark_finished(&mut self) {
        self.finished = true;
    }

    fn take_halves(&mut self) -> (Option<SendStream>, RecvStream) {
        self.finished = true;
        (self.send.take(), self.recv.take().expect("recv present"))
    }
}

impl Drop for CapStreamGuard {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(mut send) = self.send.take() {
                let _ = send.reset(1u32.into());
            }
            if let Some(mut recv) = self.recv.take() {
                let _ = recv.stop(1u32.into());
            }
        }
    }
}

pub(crate) struct RemoteByteReader {
    guard: CapStreamGuard,
    byte_budget: Arc<Semaphore>,
}

impl RemoteByteReader {
    fn new(send: Option<SendStream>, recv: RecvStream, byte_budget: Arc<Semaphore>) -> Self {
        Self {
            guard: CapStreamGuard::new(send, recv),
            byte_budget,
        }
    }

    pub(crate) async fn next_chunk(&mut self) -> std::result::Result<Option<Vec<u8>>, HostError> {
        if self.guard.finished {
            return Ok(None);
        }
        let recv = self.guard.recv.as_mut().expect("recv present");
        match read_cap_frame::<CapServerMsg>(
            recv,
            CAP_RESP_HEADER_MAX,
            CAP_CHUNK_MAX,
            &self.byte_budget,
        )
        .await
        {
            Ok((CapServerMsg::Chunk, data)) => Ok(Some(data.into_vec())),
            Ok((CapServerMsg::End, _)) => {
                self.guard.mark_finished();
                if let Some(send) = self.guard.send.as_mut() {
                    let _ = send.finish();
                }
                Ok(None)
            }
            Ok((CapServerMsg::Err(e), _)) => {
                self.guard.mark_finished();
                if let Some(send) = self.guard.send.as_mut() {
                    let _ = send.finish();
                }
                Err(e.into())
            }
            Ok(_) => Err("unexpected remote stream chunk frame".into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded("ENODEUNREACH", e)),
        }
    }
}

pub(crate) struct RemoteDirReader {
    guard: CapStreamGuard,
    byte_budget: Arc<Semaphore>,
}

impl RemoteDirReader {
    fn new(send: Option<SendStream>, recv: RecvStream, byte_budget: Arc<Semaphore>) -> Self {
        Self {
            guard: CapStreamGuard::new(send, recv),
            byte_budget,
        }
    }

    pub(crate) async fn next_entry(
        &mut self,
    ) -> std::result::Result<Option<(String, String)>, HostError> {
        if self.guard.finished {
            return Ok(None);
        }
        let recv = self.guard.recv.as_mut().expect("recv present");
        match read_cap_frame::<CapServerMsg>(
            recv,
            CAP_RESP_HEADER_MAX,
            CAP_CHUNK_MAX,
            &self.byte_budget,
        )
        .await
        {
            Ok((CapServerMsg::DirEntry { name, kind }, _)) => Ok(Some((name, kind))),
            Ok((CapServerMsg::End, _)) => {
                self.guard.mark_finished();
                if let Some(send) = self.guard.send.as_mut() {
                    let _ = send.finish();
                }
                Ok(None)
            }
            Ok((CapServerMsg::Err(e), _)) => {
                self.guard.mark_finished();
                if let Some(send) = self.guard.send.as_mut() {
                    let _ = send.finish();
                }
                Err(e.into())
            }
            Ok(_) => Err("unexpected remote directory frame".into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded("ENODEUNREACH", e)),
        }
    }
}

pub(crate) struct RemoteByteWriter {
    guard: CapStreamGuard,
    byte_budget: Arc<Semaphore>,
}

impl RemoteByteWriter {
    fn new(send: SendStream, recv: RecvStream, byte_budget: Arc<Semaphore>) -> Self {
        Self {
            guard: CapStreamGuard::new(Some(send), recv),
            byte_budget,
        }
    }

    async fn read_error_or_unreach(&mut self, write_err: String) -> HostError {
        let recv = self.guard.recv.as_mut().expect("recv present");
        if let Ok(Ok((CapServerMsg::Err(e), _))) = tokio::time::timeout(
            CAP_ERROR_PEEK_TIMEOUT,
            read_cap_frame::<CapServerMsg>(recv, CAP_RESP_HEADER_MAX, CAP_CHUNK_MAX, &self.byte_budget),
        )
        .await
        {
            self.guard.mark_finished();
            return e.into();
        }
        HostError::coded("ENODEUNREACH", write_err)
    }

    pub(crate) async fn write_chunk(&mut self, bytes: &[u8]) -> std::result::Result<(), HostError> {
        if self.guard.finished {
            return Err("stream closed".into());
        }
        let res = {
            let send = self.guard.send.as_mut().expect("send present");
            write_cap_frame(send, &CapClientMsg::Chunk, Some(bytes)).await
        };
        if let Err(e) = res {
            return Err(self.read_error_or_unreach(e).await);
        }
        Ok(())
    }

    pub(crate) async fn finish(&mut self) -> std::result::Result<(), HostError> {
        if self.guard.finished {
            return Ok(());
        }
        let send_res = {
            let send = self.guard.send.as_mut().expect("send present");
            let r = write_cap_frame(send, &CapClientMsg::Eof, None).await;
            let _ = send.finish();
            r
        };
        if let Err(e) = send_res {
            return Err(self.read_error_or_unreach(e).await);
        }
        let recv = self.guard.recv.as_mut().expect("recv present");
        match read_cap_frame::<CapServerMsg>(
            recv,
            CAP_RESP_HEADER_MAX,
            CAP_CHUNK_MAX,
            &self.byte_budget,
        )
        .await
        {
            Ok((CapServerMsg::End, _)) => {
                self.guard.mark_finished();
                Ok(())
            }
            Ok((CapServerMsg::Err(e), _)) => {
                self.guard.mark_finished();
                Err(e.into())
            }
            Ok(_) => Err("unexpected remote stream finish response".into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded("ENODEUNREACH", e)),
        }
    }
}

pub struct CapServer {
    app: App,
    global_sem: Arc<Semaphore>,
    peer_sems: Mutex<HashMap<String, Weak<Semaphore>>>,
    byte_budget: Arc<Semaphore>,
}

impl CapServer {
    pub fn new(app: App) -> Self {
        Self {
            app,
            global_sem: Arc::new(Semaphore::new(MAX_GLOBAL_INBOUND_CAP)),
            peer_sems: Mutex::default(),
            byte_budget: Arc::new(Semaphore::new(MAX_CAP_BYTE_PERMITS)),
        }
    }

    fn peer_sem(&self, node_id: &str) -> Arc<Semaphore> {
        let mut map = self.peer_sems.lock().unwrap();
        map.retain(|_, w| w.strong_count() > 0);
        if let Some(sem) = map.get(node_id).and_then(Weak::upgrade) {
            return sem;
        }
        let sem = Arc::new(Semaphore::new(MAX_PEER_INBOUND_CAP));
        map.insert(node_id.to_owned(), Arc::downgrade(&sem));
        sem
    }

    async fn send_err(send: &mut SendStream, err: impl Into<HostError>) {
        let wire: HostErrorWire = err.into().into();
        let _ = write_cap_frame(send, &CapServerMsg::Err(wire), None).await;
        let _ = send.finish();
    }

    pub async fn handle_stream(
        &self,
        from_peer: String,
        mut send: SendStream,
        mut recv: RecvStream,
        peer_cancel: CancellationToken,
    ) {
        let Ok(_global) = self.global_sem.try_acquire() else {
            Self::send_err(&mut send, "remote capability global concurrency limit reached").await;
            return;
        };
        let peer_sem = self.peer_sem(&from_peer);
        let Ok(_peer) = peer_sem.try_acquire() else {
            Self::send_err(&mut send, "remote capability peer concurrency limit reached").await;
            return;
        };

        let call_cancel = peer_cancel.child_token();
        let _cancel_on_exit = call_cancel.clone().drop_guard();
        let req = match tokio::time::timeout(
            CAP_REQ_READ_TIMEOUT,
            read_cap_frame::<CapReq>(&mut recv, CAP_REQ_HEADER_MAX, CAP_REQ_BODY_MAX, &self.byte_budget),
        )
        .await
        {
            Ok(Ok(req)) => req,
            Ok(Err(ReadFrameErr::Protocol(msg))) => {
                Self::send_err(&mut send, msg).await;
                return;
            }
            Ok(Err(ReadFrameErr::Transport(_))) | Err(_) => return,
        };

        let (req, req_body) = req;
        match req {
            CapReq::Call {
                method,
                args,
                has_data,
                sid,
                origin,
            } => {
                self.handle_call(
                    method,
                    args,
                    has_data.then_some(req_body),
                    sid,
                    origin,
                    send,
                    recv,
                    call_cancel,
                )
                .await;
            }
            CapReq::FsReadStream { path } => {
                self.spawn_passive_cancel_watcher(&send, recv, call_cancel.clone());
                self.handle_fs_read_stream(path, send, call_cancel).await;
            }
            CapReq::FsWriteStream { path, append } => {
                self.handle_fs_write_stream(path, append, send, recv, call_cancel)
                    .await;
            }
            CapReq::FsReadDir { path } => {
                self.spawn_passive_cancel_watcher(&send, recv, call_cancel.clone());
                self.handle_fs_read_dir(path, send, call_cancel).await;
            }
            CapReq::HttpStream {
                method,
                args,
                has_data,
                has_upload,
                stream_response,
            } => {
                self.handle_http_stream(
                    method,
                    args,
                    (has_data && !has_upload).then_some(req_body),
                    has_upload,
                    stream_response,
                    send,
                    recv,
                    call_cancel,
                )
                .await;
            }
        }
    }

    fn spawn_passive_cancel_watcher(
        &self,
        send: &SendStream,
        mut recv: RecvStream,
        call_cancel: CancellationToken,
    ) {
        let stopped = send.stopped();
        tokio::spawn(async move {
            let mut probe = [0u8; 1];
            tokio::select! {
                biased;
                _ = call_cancel.cancelled() => {}
                _ = stopped => {
                    call_cancel.cancel();
                }
                _ = recv.read_exact(&mut probe) => {
                    call_cancel.cancel();
                }
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_call(
        &self,
        method: String,
        args: Value,
        data: Option<PermittedBytes>,
        sid: Option<String>,
        origin: Option<String>,
        mut send: SendStream,
        recv: RecvStream,
        call_cancel: CancellationToken,
    ) {
        if !args.is_object() {
            Self::send_err(&mut send, format!("{method} expects an options object")).await;
            return;
        }
        if method == "proc.exec" {
            self.handle_proc_call(args, data, send, recv, call_cancel).await;
            return;
        }
        let (data_vec, _data_permit) = match data {
            Some(pb) => {
                let (v, p) = pb.into_parts();
                (Some(v), p)
            }
            None => (None, None),
        };
        self.spawn_passive_cancel_watcher(&send, recv, call_cancel.clone());
        let reply = if method.starts_with("fs.") {
            tokio::select! {
                biased;
                _ = call_cancel.cancelled() => Err("cancelled".into()),
                res = files::execute_local(&self.app, &call_cancel, None, &method, &args, data_vec) => res,
            }
        } else if matches!(
            method.as_str(),
            "mcp.servers"
                | "mcp.tools"
                | "mcp.call"
                | "mcp.resources"
                | "mcp.resourceTemplates"
                | "mcp.readResource"
                | "mcp.prompts"
                | "mcp.getPrompt"
        ) {
            self.handle_mcp_call(&method, args, sid, origin, &mut send, &call_cancel)
                .await
        } else if method == "http.request" {
            tokio::select! {
                biased;
                _ = call_cancel.cancelled() => Err("cancelled".into()),
                res = async {
                    let prep = cm_io::execute_http_prepared(self.app.codemode.client(), &args, None, data_vec, false).await?;
                    cm_io::collect_http_body("http.request", prep).await
                } => res,
            }
        } else if method == "mygo.request" {
            tokio::select! {
                biased;
                _ = call_cancel.cancelled() => Err("cancelled".into()),
                res = async {
                    let prep = cm_io::execute_mygo_prepared(&self.app, &args, None, data_vec, false).await?;
                    cm_io::collect_http_body("mygo.request", prep).await
                } => res,
            }
        } else {
            Err(format!("unsupported remote capability operation {method}").into())
        };

        match reply {
            Ok(reply) => {
                let msg = CapServerMsg::Reply {
                    value: reply.value,
                    has_data: reply.data.is_some(),
                };
                if write_cap_frame(&mut send, &msg, reply.data.as_deref())
                    .await
                    .is_ok()
                {
                    let _ = send.finish();
                }
            }
            Err(e) => {
                Self::send_err(&mut send, e).await;
            }
        }
    }

    async fn handle_proc_call(
        &self,
        args: Value,
        stdin: Option<PermittedBytes>,
        mut send: SendStream,
        mut recv: RecvStream,
        call_cancel: CancellationToken,
    ) {
        let (stdin_vec, _stdin_permit) = match stdin {
            Some(pb) => {
                let (v, p) = pb.into_parts();
                (Some(v), p)
            }
            None => (None, None),
        };
        let (program, argv, cwd) = match cm_io::validate_command(&self.app, &args) {
            Ok(validated) => validated,
            Err(e) => {
                Self::send_err(&mut send, HostError::from(e)).await;
                return;
            }
        };
        let cwd_str = cwd.to_string_lossy().into_owned();
        if write_cap_frame(&mut send, &CapServerMsg::CommandStarted { cwd: cwd_str }, None)
            .await
            .is_err()
        {
            return;
        }
        // Watch client Cancel message, EOF/Reset, or SendStream STOP_SENDING.
        let cancel_tok = call_cancel.clone();
        let stopped = send.stopped();
        let budget = self.byte_budget.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = cancel_tok.cancelled() => {}
                _ = stopped => {
                    cancel_tok.cancel();
                }
                _ = read_cap_frame::<CapClientMsg>(&mut recv, CAP_REQ_HEADER_MAX, CAP_CHUNK_MAX, &budget) => {
                    cancel_tok.cancel();
                }
            }
        });

        let progress = Arc::new(RemoteCommandProgress::default());
        let cmd_progress = progress.clone();
        let cmd_cancel = call_cancel.clone();
        let mut cmd_handle = tokio::spawn(async move {
            let preview = CommandPreview::Remote(&cmd_progress);
            cm_io::execute_command_validated(
                &program,
                &argv,
                &cwd,
                &args,
                stdin_vec,
                cmd_cancel,
                &preview,
            )
            .await
        });

        let cmd_res = loop {
            tokio::select! {
                biased;
                res = &mut cmd_handle => {
                    break res;
                }
                _ = progress.wait_pending(), if !call_cancel.is_cancelled() => {
                    let (out_d, out_b, err_d, err_b) = progress.drain();
                    if out_d > 0 || !out_b.is_empty() {
                        let _ = write_cap_frame(
                            &mut send,
                            &CapServerMsg::CommandProgress {
                                stream: "stdout".into(),
                                dropped: out_d,
                            },
                            Some(&out_b),
                        )
                        .await;
                    }
                    if err_d > 0 || !err_b.is_empty() {
                        let _ = write_cap_frame(
                            &mut send,
                            &CapServerMsg::CommandProgress {
                                stream: "stderr".into(),
                                dropped: err_d,
                            },
                            Some(&err_b),
                        )
                        .await;
                    }
                    tokio::select! {
                        biased;
                        res = &mut cmd_handle => break res,
                        _ = call_cancel.cancelled() => {}
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        };

        let result = match cmd_res {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(HostError::from(e)),
            Err(e) => Err(HostError::from(format!("proc.exec worker panicked: {e}"))),
        };
        match result {
            Ok(val) => {
                let stdout_bytes = val.get("stdout").and_then(Value::as_str).unwrap_or_default().as_bytes();
                let stderr_bytes = val.get("stderr").and_then(Value::as_str).unwrap_or_default().as_bytes();
                let mut body = Vec::with_capacity(stdout_bytes.len() + stderr_bytes.len());
                body.extend_from_slice(stdout_bytes);
                body.extend_from_slice(stderr_bytes);
                let msg = CapServerMsg::ProcResult {
                    exit_code: val.get("exit_code").and_then(Value::as_i64),
                    signal: val
                        .get("signal")
                        .and_then(Value::as_i64)
                        .and_then(|v| i32::try_from(v).ok()),
                    timed_out: val
                        .get("timed_out")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    stdout_len: stdout_bytes.len(),
                    stderr_len: stderr_bytes.len(),
                    truncated: val
                        .get("truncated")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                };
                if write_cap_frame(&mut send, &msg, Some(&body)).await.is_ok() {
                    let _ = send.finish();
                }
            }
            Err(e) => {
                Self::send_err(&mut send, e).await;
            }
        }
    }

    async fn handle_mcp_call(
        &self,
        method: &str,
        args: Value,
        sid: Option<String>,
        origin: Option<String>,
        send: &mut SendStream,
        call_cancel: &CancellationToken,
    ) -> std::result::Result<Reply, HostError> {
        let sid: SessionId = sid
            .as_deref()
            .ok_or_else(|| HostError::from("missing session id for mcp operation"))?
            .parse()
            .map_err(|_| HostError::from("invalid session id for mcp operation"))?;
        let origin: ItemId = origin
            .as_deref()
            .ok_or_else(|| HostError::from("missing origin id for mcp operation"))?
            .parse()
            .map_err(|_| HostError::from("invalid origin id for mcp operation"))?;

        let (phase_tx, mut phase_rx) = mpsc::unbounded_channel::<&'static str>();
        let fut = crate::mcp::execute(&self.app, sid, origin, method, args, |phase| {
            let _ = phase_tx.send(phase);
        });
        tokio::pin!(fut);

        loop {
            tokio::select! {
                biased;
                _ = call_cancel.cancelled() => return Err("cancelled".into()),
                res = &mut fut => return res.map_err(HostError::from),
                Some(phase) = phase_rx.recv() => {
                    let _ = write_cap_frame(
                        send,
                        &CapServerMsg::McpPhase { phase: phase.to_owned() },
                        None,
                    )
                    .await;
                }
            }
        }
    }

    async fn handle_fs_read_stream(
        &self,
        path: String,
        mut send: SendStream,
        call_cancel: CancellationToken,
    ) {
        let mut file = match files::open_stream_read(&self.app, &path, &call_cancel).await {
            Ok(f) => f,
            Err(e) => {
                Self::send_err(&mut send, e).await;
                return;
            }
        };
        if write_cap_frame(&mut send, &CapServerMsg::Opened, None)
            .await
            .is_err()
        {
            return;
        }
        let mut buf = vec![0u8; CAP_CHUNK_MAX];
        loop {
            let n = tokio::select! {
                biased;
                _ = call_cancel.cancelled() => return,
                r = file.read(&mut buf) => match r {
                    Ok(n) => n,
                    Err(e) => {
                        Self::send_err(&mut send, HostError::from(e).context(format!("fs.read {path}"))).await;
                        return;
                    }
                }
            };
            if n == 0 {
                break;
            }
            if write_cap_frame(&mut send, &CapServerMsg::Chunk, Some(&buf[..n]))
                .await
                .is_err()
            {
                return;
            }
        }
        if write_cap_frame(&mut send, &CapServerMsg::End, None)
            .await
            .is_ok()
        {
            let _ = send.finish();
        }
    }

    async fn handle_fs_write_stream(
        &self,
        path: String,
        append: bool,
        mut send: SendStream,
        mut recv: RecvStream,
        call_cancel: CancellationToken,
    ) {
        let mut file = match files::open_stream_write(&self.app, &path, append, &call_cancel).await
        {
            Ok(f) => f,
            Err(e) => {
                Self::send_err(&mut send, e).await;
                let _ = recv.stop(1u32.into());
                return;
            }
        };
        if write_cap_frame(&mut send, &CapServerMsg::Opened, None)
            .await
            .is_err()
        {
            return;
        }
        loop {
            let msg = tokio::select! {
                biased;
                _ = call_cancel.cancelled() => return,
                r = read_cap_frame::<CapClientMsg>(&mut recv, CAP_REQ_HEADER_MAX, CAP_CHUNK_MAX, &self.byte_budget) => r,
            };
            match msg {
                Ok((CapClientMsg::Chunk, bytes)) => {
                    if let Err(e) = file.write_all(&bytes).await {
                        Self::send_err(&mut send, HostError::from(e).context(format!("fs.write {path}"))).await;
                        let _ = recv.stop(1u32.into());
                        return;
                    }
                }
                Ok((CapClientMsg::Eof, _)) => {
                    if let Err(e) = file.flush().await {
                        Self::send_err(&mut send, HostError::from(e).context(format!("fs.write {path}"))).await;
                        let _ = recv.stop(1u32.into());
                        return;
                    }
                    if write_cap_frame(&mut send, &CapServerMsg::End, None)
                        .await
                        .is_ok()
                    {
                        let _ = send.finish();
                    }
                    return;
                }
                Ok((CapClientMsg::Cancel | CapClientMsg::UploadAbort, _)) => return,
                Err(ReadFrameErr::Protocol(e)) => {
                    Self::send_err(&mut send, e).await;
                    let _ = recv.stop(1u32.into());
                    return;
                }
                Err(ReadFrameErr::Transport(_)) => return,
            }
        }
    }

    async fn handle_fs_read_dir(
        &self,
        path: String,
        mut send: SendStream,
        call_cancel: CancellationToken,
    ) {
        let mut dir = match files::open_read_dir(&self.app, &path, &call_cancel).await {
            Ok(d) => d,
            Err(e) => {
                Self::send_err(&mut send, e).await;
                return;
            }
        };
        if write_cap_frame(&mut send, &CapServerMsg::Opened, None)
            .await
            .is_err()
        {
            return;
        }
        loop {
            let entry = tokio::select! {
                biased;
                _ = call_cancel.cancelled() => return,
                r = files::next_dir_entry(&mut dir) => match r {
                    Ok(e) => e,
                    Err(e) => {
                        Self::send_err(&mut send, e.context(format!("fs.readDir {path}"))).await;
                        return;
                    }
                }
            };
            let Some((name, kind)) = entry else {
                break;
            };
            let msg = CapServerMsg::DirEntry {
                name,
                kind: kind.to_owned(),
            };
            if write_cap_frame(&mut send, &msg, None).await.is_err() {
                return;
            }
        }
        if write_cap_frame(&mut send, &CapServerMsg::End, None)
            .await
            .is_ok()
        {
            let _ = send.finish();
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_http_stream(
        &self,
        method: String,
        args: Value,
        data: Option<PermittedBytes>,
        has_upload: bool,
        stream_response: bool,
        mut send: SendStream,
        mut recv: RecvStream,
        call_cancel: CancellationToken,
    ) {
        if !matches!(method.as_str(), "http.request" | "mygo.request") {
            Self::send_err(&mut send, format!("unsupported stream operation {method}")).await;
            return;
        }
        if !args.is_object() {
            Self::send_err(&mut send, format!("{method} expects an options object")).await;
            return;
        }
        let (data_vec, _data_permit) = match data {
            Some(pb) => {
                let (v, p) = pb.into_parts();
                (Some(v), p)
            }
            None => (None, None),
        };
        let upload: Option<(cm_io::BoxUploadStream, Option<CancellationToken>)> = if has_upload {
            let (tx, mut rx) = mpsc::channel::<std::result::Result<PermittedBytes, io::Error>>(2);
            let upload_cancel = call_cancel.clone();
            let stopped = send.stopped();
            let budget = self.byte_budget.clone();
            tokio::spawn(async move {
                let mut drop_rx = false;
                let mut eof_or_aborted = false;
                loop {
                    if upload_cancel.is_cancelled() {
                        break;
                    }
                    let res = read_cap_frame::<CapClientMsg>(
                        &mut recv,
                        CAP_REQ_HEADER_MAX,
                        CAP_CHUNK_MAX,
                        &budget,
                    )
                    .await;
                    match res {
                        Ok((CapClientMsg::Chunk, bytes)) => {
                            if !drop_rx {
                                let send_res = tokio::select! {
                                    biased;
                                    _ = upload_cancel.cancelled() => break,
                                    r = tx.send(Ok(bytes)) => r,
                                };
                                if send_res.is_err() {
                                    drop_rx = true;
                                }
                            }
                        }
                        Ok((CapClientMsg::Eof | CapClientMsg::UploadAbort, _)) => {
                            eof_or_aborted = true;
                            break;
                        }
                        Ok((CapClientMsg::Cancel, _)) => {
                            upload_cancel.cancel();
                            return;
                        }
                        Err(_) => {
                            if !drop_rx {
                                let _ = tx
                                    .send(Err(io::Error::new(
                                        io::ErrorKind::Interrupted,
                                        "upload stopped",
                                    )))
                                    .await;
                            }
                            upload_cancel.cancel();
                            return;
                        }
                    }
                }
                drop(tx);
                if eof_or_aborted {
                    let mut buf = [0u8; 1];
                    tokio::select! {
                        biased;
                        _ = upload_cancel.cancelled() => {}
                        _ = stopped => {
                            upload_cancel.cancel();
                        }
                        _ = recv.read_exact(&mut buf) => {
                            upload_cancel.cancel();
                        }
                    }
                }
            });
            let stream = async_stream::stream! {
                while let Some(part) = rx.recv().await {
                    yield part.map(PermittedBytes::into_vec);
                }
            };
            Some((Box::pin(stream), None))
        } else {
            self.spawn_passive_cancel_watcher(&send, recv, call_cancel.clone());
            None
        };

        let prep = tokio::select! {
            biased;
            _ = call_cancel.cancelled() => Err(HostError::from("cancelled")),
            res = async {
                if method == "http.request" {
                    cm_io::execute_http_prepared(
                        self.app.codemode.client(),
                        &args,
                        upload,
                        data_vec,
                        stream_response,
                    )
                    .await
                } else {
                    cm_io::execute_mygo_prepared(&self.app, &args, upload, data_vec, stream_response)
                        .await
                }
            } => res,
        };
        let mut prep = match prep {
            Ok(p) => p,
            Err(e) => {
                Self::send_err(&mut send, e).await;
                return;
            }
        };

        if !stream_response {
            let collected = tokio::select! {
                biased;
                _ = call_cancel.cancelled() => Err(HostError::from("cancelled")),
                res = cm_io::collect_http_body(&method, prep) => res,
            };
            match collected {
                Ok(reply) => {
                    let msg = CapServerMsg::Reply {
                        value: reply.value,
                        has_data: reply.data.is_some(),
                    };
                    if write_cap_frame(&mut send, &msg, reply.data.as_deref())
                        .await
                        .is_ok()
                    {
                        let _ = send.finish();
                    }
                }
                Err(e) => {
                    Self::send_err(&mut send, e).await;
                }
            }
            return;
        }

        let head = CapServerMsg::HttpHead {
            status: prep.status,
            headers: prep.headers,
        };
        if write_cap_frame(&mut send, &head, None).await.is_err() {
            return;
        }
        loop {
            let next: Option<std::result::Result<Bytes, String>> = tokio::select! {
                biased;
                _ = call_cancel.cancelled() => return,
                _ = async {
                    match prep.stream_deadline {
                        Some((at, _)) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let msg = prep.stream_deadline.map_or("timeout", |(_, m)| m);
                    Self::send_err(&mut send, HostError::from(msg)).await;
                    return;
                }
                chunk = prep.stream.next() => chunk,
            };
            let Some(chunk_res) = next else {
                break;
            };
            let bytes = match chunk_res {
                Ok(b) => b,
                Err(e) => {
                    Self::send_err(&mut send, HostError::from(e)).await;
                    return;
                }
            };
            for piece in bytes.chunks(CAP_CHUNK_MAX) {
                if piece.is_empty() {
                    continue;
                }
                if write_cap_frame(&mut send, &CapServerMsg::Chunk, Some(piece))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        if write_cap_frame(&mut send, &CapServerMsg::End, None)
            .await
            .is_ok()
        {
            let _ = send.finish();
        }
    }
}

async fn open_cap_bi(
    net: &Arc<ClusterNet>,
    peer: &PeerInfo,
    cancel: &CancellationToken,
) -> std::result::Result<(Connection, CancellationToken, SendStream, RecvStream), HostError> {
    for attempt in 0..2 {
        let (conn, peer_cancel) = net
            .get_or_dial_cap_connection(peer, cancel)
            .await
            .map_err(|msg| {
                if cancel.is_cancelled() {
                    HostError::from("cancelled")
                } else {
                    HostError::coded("ENODEUNREACH", msg)
                }
            })?;
        let bi = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("cancelled".into()),
            _ = peer_cancel.cancelled() => Err("peer cap connection closed".to_owned()),
            res = conn.open_bi() => res.map_err(|e| e.to_string()),
        };
        match bi {
            Ok((send, recv)) => return Ok((conn, peer_cancel, send, recv)),
            Err(_) if attempt == 0 => {
                net.evict_cap_conn(&peer.node_id, &conn);
                continue;
            }
            Err(e) => {
                net.evict_cap_conn(&peer.node_id, &conn);
                return Err(HostError::coded(
                    "ENODEUNREACH",
                    format!("failed to open stream to {}: {e}", peer.name),
                ));
            }
        }
    }
    unreachable!()
}

pub(crate) async fn execute_remote(
    host: &Host,
    peer: PeerInfo,
    op: &str,
    mut args: Value,
    data: Option<Vec<u8>>,
) -> std::result::Result<Reply, HostError> {
    if host.cancel.is_cancelled() {
        return Err("cancelled".into());
    }
    match op {
        "fs.read" if files::flag(&args, "stream", false)? => {
            if args.get("encoding").is_some() {
                return Err("encoding requires a buffered utf8 read".into());
            }
            if data.is_some() {
                return Err("fs.read does not take data in this mode".into());
            }
            let path = files::path_arg(&args, "path")?.to_string_lossy().into_owned();
            let slot = host.streams.reserve()?;
            let (_conn, peer_cancel, mut send, mut recv) =
                open_cap_bi(&host.app.cluster.net, &peer, &host.cancel).await?;
            if let Err(e) =
                write_cap_frame(&mut send, &CapReq::FsReadStream { path }, None).await
            {
                let _ = send.reset(1u32.into());
                let _ = recv.stop(1u32.into());
                return Err(HostError::coded("ENODEUNREACH", e));
            }
            let mut guard = CapStreamGuard::new(Some(send), recv);
            let budget = client_byte_budget();
            read_opened(guard.recv.as_mut().unwrap(), &host.cancel, &peer_cancel, &peer.name, &budget).await?;
            let (send, recv) = guard.take_halves();
            let id = slot.insert(StreamResource::RemoteReader(RemoteByteReader::new(
                send, recv, budget,
            )));
            Ok(Reply::value(json!(id)))
        }
        "fs.write" if files::flag(&args, "stream", false)? => {
            if data.is_some() {
                return Err("fs.write does not take data in this mode".into());
            }
            let append = files::flag(&args, "append", false)?;
            let path = files::path_arg(&args, "path")?.to_string_lossy().into_owned();
            let slot = host.streams.reserve()?;
            let (_conn, peer_cancel, mut send, mut recv) =
                open_cap_bi(&host.app.cluster.net, &peer, &host.cancel).await?;
            if let Err(e) =
                write_cap_frame(&mut send, &CapReq::FsWriteStream { path, append }, None).await
            {
                let _ = send.reset(1u32.into());
                let _ = recv.stop(1u32.into());
                return Err(HostError::coded("ENODEUNREACH", e));
            }
            let mut guard = CapStreamGuard::new(Some(send), recv);
            let budget = client_byte_budget();
            read_opened(guard.recv.as_mut().unwrap(), &host.cancel, &peer_cancel, &peer.name, &budget).await?;
            let (send, recv) = guard.take_halves();
            let id = slot.insert(StreamResource::RemoteWriter(RemoteByteWriter::new(
                send.expect("send present"),
                recv,
                budget,
            )));
            Ok(Reply::value(json!(id)))
        }
        "fs.readDir" => {
            let path = files::path_arg(&args, "path")?.to_string_lossy().into_owned();
            let slot = host.streams.reserve()?;
            let (_conn, peer_cancel, mut send, mut recv) =
                open_cap_bi(&host.app.cluster.net, &peer, &host.cancel).await?;
            if let Err(e) = write_cap_frame(&mut send, &CapReq::FsReadDir { path }, None).await {
                let _ = send.reset(1u32.into());
                let _ = recv.stop(1u32.into());
                return Err(HostError::coded("ENODEUNREACH", e));
            }
            let mut guard = CapStreamGuard::new(Some(send), recv);
            let budget = client_byte_budget();
            read_opened(guard.recv.as_mut().unwrap(), &host.cancel, &peer_cancel, &peer.name, &budget).await?;
            let (send, recv) = guard.take_halves();
            let id = slot.insert(StreamResource::RemoteDir(RemoteDirReader::new(
                send, recv, budget,
            )));
            Ok(Reply::value(json!(id)))
        }
        "http.request" | "mygo.request"
            if args.get("stream").and_then(Value::as_bool) == Some(true)
                || args.get("upload").is_some_and(|v| !v.is_null()) =>
        {
            execute_remote_http_stream(host, &peer, op, &mut args, data).await
        }
        "proc.exec" => execute_remote_proc(host, &peer, args, data).await,
        op_name if op_name.starts_with("mcp.") => {
            execute_remote_mcp(host, &peer, op_name, args).await
        }
        _ => execute_remote_simple_call(host, &peer, op, args, data).await,
    }
}

async fn read_opened(
    recv: &mut RecvStream,
    cancel: &CancellationToken,
    peer_cancel: &CancellationToken,
    peer_name: &str,
    budget: &Arc<Semaphore>,
) -> std::result::Result<(), HostError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err("cancelled".into()),
        _ = peer_cancel.cancelled() => Err(HostError::coded(
            "ENODEUNREACH",
            format!("node {peer_name} disconnected"),
        )),
        frame = read_cap_frame::<CapServerMsg>(recv, CAP_RESP_HEADER_MAX, CAP_CHUNK_MAX, budget) => match frame {
            Ok((CapServerMsg::Opened, _)) => Ok(()),
            Ok((CapServerMsg::Err(e), _)) => Err(e.into()),
            Ok(_) => Err("unexpected capability open response".into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded("ENODEUNREACH", e)),
        }
    }
}

async fn execute_remote_simple_call(
    host: &Host,
    peer: &PeerInfo,
    op: &str,
    args: Value,
    data: Option<Vec<u8>>,
) -> std::result::Result<Reply, HostError> {
    if data.as_ref().is_some_and(|b| b.len() > CAP_REQ_BODY_MAX) {
        return Err("host data exceeds 8 MiB".into());
    }
    let http_deadline = match op {
        "http.request" => cm_io::timeout(&args)?
            .map(|d| (tokio::time::Instant::now() + d, "timeout: http.request")),
        "mygo.request" => cm_io::timeout(&args)?
            .map(|d| (tokio::time::Instant::now() + d, "timeout: mygo.request")),
        _ => None,
    };
    let (_conn, peer_cancel, mut send, mut recv) =
        open_cap_bi(&host.app.cluster.net, peer, &host.cancel).await?;
    let req = CapReq::Call {
        method: op.to_owned(),
        args,
        has_data: data.is_some(),
        sid: None,
        origin: None,
    };
    if let Err(e) = write_cap_frame(&mut send, &req, data.as_deref()).await {
        let _ = send.reset(1u32.into());
        let _ = recv.stop(1u32.into());
        return Err(HostError::coded("ENODEUNREACH", e));
    }
    let mut guard = CapStreamGuard::new(Some(send), recv);
    let budget = client_byte_budget();
    tokio::select! {
        biased;
        _ = host.cancel.cancelled() => Err("cancelled".into()),
        _ = peer_cancel.cancelled() => Err(HostError::coded(
            "ENODEUNREACH",
            format!("node {} disconnected during {op}", peer.name),
        )),
        _ = async {
            match http_deadline {
                Some((at, _)) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        } => Err(HostError::from(http_deadline.map_or("timeout", |(_, m)| m))),
        frame = read_cap_frame::<CapServerMsg>(
            guard.recv.as_mut().unwrap(),
            CAP_RESP_HEADER_MAX,
            CAP_RESP_BODY_MAX,
            &budget,
        ) => match frame {
            Ok((CapServerMsg::Reply { value, has_data }, body)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                Ok(Reply {
                    value,
                    data: has_data.then(|| body.into_vec()),
                })
            }
            Ok((CapServerMsg::Err(e), _)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                Err(e.into())
            }
            Ok(_) => Err(format!("unexpected remote reply for {op}").into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded(
                "ENODEUNREACH",
                format!("node {} disconnected during {op}: {e}", peer.name),
            )),
        }
    }
}

async fn execute_remote_mcp(
    host: &Host,
    peer: &PeerInfo,
    op: &str,
    args: Value,
) -> std::result::Result<Reply, HostError> {
    let mcp_guard = McpGuard::start(&host.live, &peer.name, op, &args);
    let (_conn, peer_cancel, mut send, mut recv) =
        open_cap_bi(&host.app.cluster.net, peer, &host.cancel).await?;
    let req = CapReq::Call {
        method: op.to_owned(),
        args,
        has_data: false,
        sid: Some(host.sid.as_str().to_owned()),
        origin: Some(host.origin.as_str().to_owned()),
    };
    if let Err(e) = write_cap_frame(&mut send, &req, None).await {
        let _ = send.reset(1u32.into());
        let _ = recv.stop(1u32.into());
        return Err(disconnect_mcp_error(op, &peer.name, &e));
    }
    let mut guard = CapStreamGuard::new(Some(send), recv);
    let budget = client_byte_budget();
    loop {
        let frame = tokio::select! {
            biased;
            _ = host.cancel.cancelled() => return Err("cancelled".into()),
            _ = peer_cancel.cancelled() => {
                return Err(disconnect_mcp_error(op, &peer.name, "connection closed"));
            }
            frame = read_cap_frame::<CapServerMsg>(
                guard.recv.as_mut().unwrap(),
                CAP_RESP_HEADER_MAX,
                CAP_RESP_BODY_MAX,
                &budget,
            ) => frame,
        };
        match frame {
            Ok((CapServerMsg::McpPhase { phase }, _)) => {
                mcp_guard.set_phase(phase);
            }
            Ok((CapServerMsg::Reply { value, has_data }, body)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                return Ok(Reply {
                    value,
                    data: has_data.then(|| body.into_vec()),
                });
            }
            Ok((CapServerMsg::Err(e), _)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                return Err(e.into());
            }
            Ok(_) => return Err(format!("unexpected remote reply for {op}").into()),
            Err(ReadFrameErr::Protocol(e)) => return Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => {
                return Err(disconnect_mcp_error(op, &peer.name, &e));
            }
        }
    }
}

fn disconnect_mcp_error(op: &str, peer_name: &str, detail: &str) -> HostError {
    if op == "mcp.call" {
        HostError::coded(
            "ENODEUNREACH",
            format!(
                "node {peer_name} disconnected during mcp.call; result unknown; request was not replayed ({detail})"
            ),
        )
    } else {
        HostError::coded(
            "ENODEUNREACH",
            format!("node {peer_name} disconnected during {op}: {detail}"),
        )
    }
}

async fn execute_remote_proc(
    host: &Host,
    peer: &PeerInfo,
    args: Value,
    stdin: Option<Vec<u8>>,
) -> std::result::Result<Reply, HostError> {
    if stdin.as_ref().is_some_and(|b| b.len() > CAP_REQ_BODY_MAX) {
        return Err("host data exceeds 8 MiB".into());
    }
    let preview = args.get("program").and_then(Value::as_str).map(|program| {
        CommandGuard::start(
            &host.live,
            &peer.name,
            program,
            args.get("cwd").and_then(Value::as_str),
        )
    });

    let (_conn, peer_cancel, mut send, mut recv) =
        open_cap_bi(&host.app.cluster.net, peer, &host.cancel).await?;
    let req = CapReq::Call {
        method: "proc.exec".to_owned(),
        args,
        has_data: stdin.is_some(),
        sid: None,
        origin: None,
    };
    if let Err(e) = write_cap_frame(&mut send, &req, stdin.as_deref()).await {
        let _ = send.reset(1u32.into());
        let _ = recv.stop(1u32.into());
        return Err(HostError::coded("ENODEUNREACH", e));
    }

    let budget = client_byte_budget();
    let mut cancel_armed = !host.cancel.is_cancelled();
    let mut reap_deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = if !cancel_armed {
        let _ = write_cap_frame(&mut send, &CapClientMsg::Cancel, None).await;
        let _ = send.finish();
        Some(Box::pin(tokio::time::sleep(CAP_PROC_REAP_TIMEOUT)))
    } else {
        None
    };

    let mut send = Some(send);
    let mut reader = Box::pin(read_next_server_msg(recv, CAP_RESP_BODY_MAX, budget.clone()));

    loop {
        tokio::select! {
            biased;
            _ = peer_cancel.cancelled() => {
                if let Some(mut s) = send.take() {
                    let _ = s.reset(1u32.into());
                }
                return Err(if cancel_armed {
                    HostError::coded(
                        "ENODEUNREACH",
                        format!("node {} disconnected during proc.exec", peer.name),
                    )
                } else {
                    HostError::from("cancelled")
                });
            }
            _ = async {
                match &mut reap_deadline {
                    Some(sleep) => sleep.as_mut().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(mut s) = send.take() {
                    let _ = s.reset(1u32.into());
                }
                return Err("cancelled".into());
            }
            _ = host.cancel.cancelled(), if cancel_armed => {
                cancel_armed = false;
                if let Some(s) = send.as_mut() {
                    if write_cap_frame(s, &CapClientMsg::Cancel, None).await.is_err() {
                        let _ = s.reset(1u32.into());
                        send = None;
                    } else {
                        let _ = s.finish();
                    }
                }
                reap_deadline = Some(Box::pin(tokio::time::sleep(CAP_PROC_REAP_TIMEOUT)));
            }
            (next_recv, frame) = &mut reader => {
                recv = next_recv;
                match frame {
                    Ok((CapServerMsg::CommandStarted { cwd }, _)) => {
                        if let Some(g) = &preview {
                            g.set_cwd(cwd);
                        }
                        reader = Box::pin(read_next_server_msg(recv, CAP_RESP_BODY_MAX, budget.clone()));
                    }
                    Ok((CapServerMsg::CommandProgress { stream, dropped }, body)) => {
                        if let Some(g) = &preview {
                            let st = if stream == "stderr" { Stream::Stderr } else { Stream::Stdout };
                            g.append_with_dropped(&st, dropped, &body);
                        }
                        reader = Box::pin(read_next_server_msg(recv, CAP_RESP_BODY_MAX, budget.clone()));
                    }
                    Ok((
                        CapServerMsg::ProcResult {
                            exit_code,
                            signal,
                            timed_out,
                            stdout_len,
                            stderr_len,
                            truncated,
                        },
                        body,
                    )) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.finish();
                        }
                        if host.cancel.is_cancelled() {
                            return Err("cancelled".into());
                        }
                        let expected_total = stdout_len
                            .checked_add(stderr_len)
                            .ok_or_else(|| HostError::from("remote proc.exec output length overflow"))?;
                        if body.len() != expected_total {
                            return Err("remote proc.exec output length mismatch".into());
                        }
                        let (bytes, _permit) = body.into_parts();
                        let stdout = String::from_utf8_lossy(&bytes[..stdout_len]).into_owned();
                        let stderr =
                            String::from_utf8_lossy(&bytes[stdout_len..expected_total]).into_owned();
                        return Ok(Reply::value(cm_io::command_output(
                            exit_code,
                            signal,
                            timed_out,
                            stdout,
                            stderr,
                            truncated,
                        )));
                    }
                    Ok((CapServerMsg::Reply { value, has_data }, body)) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.finish();
                        }
                        if host.cancel.is_cancelled() {
                            return Err("cancelled".into());
                        }
                        return Ok(Reply {
                            value,
                            data: has_data.then(|| body.into_vec()),
                        });
                    }
                    Ok((CapServerMsg::Err(e), _)) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.finish();
                        }
                        return Err(e.into());
                    }
                    Ok(_) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.reset(1u32.into());
                        }
                        let _ = recv.stop(1u32.into());
                        return Err("unexpected remote proc.exec response".into());
                    }
                    Err(ReadFrameErr::Protocol(e)) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.reset(1u32.into());
                        }
                        let _ = recv.stop(1u32.into());
                        return Err(e.into());
                    }
                    Err(ReadFrameErr::Transport(e)) => {
                        if let Some(mut s) = send.take() {
                            let _ = s.reset(1u32.into());
                        }
                        return Err(if cancel_armed {
                            HostError::coded(
                                "ENODEUNREACH",
                                format!("node {} disconnected during proc.exec: {e}", peer.name),
                            )
                        } else {
                            HostError::from("cancelled")
                        });
                    }
                }
            }
        }
    }
}

async fn execute_remote_http_stream(
    host: &Host,
    peer: &PeerInfo,
    op: &str,
    args: &mut Value,
    data: Option<Vec<u8>>,
) -> std::result::Result<Reply, HostError> {
    let stream_response = match args.get("stream") {
        None => false,
        Some(v) => v.as_bool().ok_or("stream must be a boolean")?,
    };
    let duration = cm_io::timeout(args)?;
    let timeout_label = if op == "http.request" {
        "timeout: http.request"
    } else {
        "timeout: mygo.request"
    };
    let stream_deadline = duration.map(|d| (tokio::time::Instant::now() + d, timeout_label));

    let upload_id = args
        .as_object_mut()
        .and_then(|m| m.remove("upload"))
        .filter(|v| !v.is_null());
    let has_upload = upload_id.is_some();
    if has_upload && data.is_some() {
        return Err(format!("{op} cannot pass both upload and body").into());
    }
    if data.as_ref().is_some_and(|b| b.len() > CAP_REQ_BODY_MAX) {
        return Err("host data exceeds 8 MiB".into());
    }

    let upload_pair = match upload_id {
        Some(id) => Some(host.streams.upload_stream(&id, host.cancel.clone()).await?),
        None => None,
    };
    let slot = if stream_response {
        Some(host.streams.reserve()?)
    } else {
        None
    };

    let (_conn, peer_cancel, mut send, mut recv) =
        open_cap_bi(&host.app.cluster.net, peer, &host.cancel).await?;
    let inline = (!has_upload).then_some(data).flatten();
    let req = CapReq::HttpStream {
        method: op.to_owned(),
        args: args.clone(),
        has_data: inline.is_some(),
        has_upload,
        stream_response,
    };
    if let Err(e) = write_cap_frame(&mut send, &req, inline.as_deref()).await {
        let _ = send.reset(1u32.into());
        let _ = recv.stop(1u32.into());
        return Err(HostError::coded("ENODEUNREACH", e));
    }

    let budget = client_byte_budget();
    let max_body = if stream_response {
        CAP_CHUNK_MAX
    } else {
        CAP_RESP_BODY_MAX
    };

    let Some((upload_stream, upload_closed)) = upload_pair else {
        let mut guard = CapStreamGuard::new(Some(send), recv);
        return finish_remote_http_wait(
            host,
            peer,
            op,
            slot,
            stream_deadline,
            &peer_cancel,
            &mut guard,
            &budget,
            max_body,
        )
        .await;
    };

    let abort_upload = Arc::new(tokio::sync::Notify::new());
    let abort_signal = abort_upload.clone();
    let pump_cancel = host.cancel.clone();
    let mut upload_task = tokio::spawn(async move {
        let mut send = send;
        let mut upload_stopped = false;
        tokio::pin!(upload_stream);
        loop {
            let next = tokio::select! {
                biased;
                _ = abort_signal.notified() => return Ok((send, false)),
                _ = upload_closed.cancelled() => return Err("upload stopped".to_owned()),
                _ = pump_cancel.cancelled() => return Err("cancelled".to_owned()),
                part = upload_stream.next() => part,
            };
            match next {
                Some(Ok(bytes)) => {
                    for chunk in bytes.chunks(CAP_CHUNK_MAX) {
                        write_cap_frame(&mut send, &CapClientMsg::Chunk, Some(chunk)).await?;
                    }
                }
                Some(Err(_)) => {
                    upload_stopped = true;
                    break;
                }
                None => break,
            }
        }
        if upload_stopped {
            return Err("upload stopped".to_owned());
        }
        write_cap_frame(&mut send, &CapClientMsg::Eof, None).await?;
        Ok((send, true))
    });

    let mut reader = Box::pin(read_next_server_msg(recv, max_body, budget.clone()));
    let mut upload_done = false;
    let mut send_holder: Option<SendStream> = None;

    let result = loop {
        tokio::select! {
            biased;
            _ = host.cancel.cancelled() => {
                abort_upload.notify_one();
                upload_task.abort();
                break Err("cancelled".into());
            }
            _ = peer_cancel.cancelled() => {
                abort_upload.notify_one();
                upload_task.abort();
                break Err(HostError::coded(
                    "ENODEUNREACH",
                    format!("node {} disconnected during {op}", peer.name),
                ));
            }
            _ = async {
                match stream_deadline {
                    Some((at, _)) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                abort_upload.notify_one();
                upload_task.abort();
                break Err(HostError::from(stream_deadline.map_or("timeout", |(_, m)| m)));
            }
            res = &mut upload_task, if !upload_done => {
                upload_done = true;
                match res {
                    Ok(Ok((s, _sent_eof))) => {
                        send_holder = Some(s);
                    }
                    Ok(Err(msg)) => {
                        if msg == "upload stopped" || msg == "cancelled" {
                            break Err(HostError::from(msg));
                        }
                        // Early HTTP response (e.g., 413) may stop the upload stream before sending
                        // Reply/HttpHead on `recv`; keep waiting on `&mut reader` for the response
                        // (or `ENODEUNREACH` if the link itself died).
                    }
                    Err(e) => break Err(HostError::from(format!("upload worker failed: {e}"))),
                }
            }
            (next_recv, frame) = &mut reader => {
                recv = next_recv;
                let reclaimed_send = if upload_done {
                    send_holder.take()
                } else {
                    abort_upload.notify_one();
                    match tokio::time::timeout(Duration::from_secs(2), &mut upload_task).await {
                        Ok(Ok(Ok((mut s, sent_eof)))) => {
                            if !sent_eof {
                                let _ = write_cap_frame(&mut s, &CapClientMsg::UploadAbort, None).await;
                            }
                            Some(s)
                        }
                        _ => {
                            upload_task.abort();
                            None
                        }
                    }
                };
                match (frame, reclaimed_send) {
                    (Ok((CapServerMsg::HttpHead { status, headers }, _)), maybe_s) => {
                        let slot = slot.expect("stream slot reserved");
                        let id = slot.insert_with_deadline(
                            StreamResource::RemoteReader(RemoteByteReader::new(
                                maybe_s,
                                recv,
                                budget.clone(),
                            )),
                            stream_deadline,
                        );
                        break Ok(Reply::value(json!({
                            "status": status,
                            "headers": headers,
                            "body": id,
                        })));
                    }
                    (Ok((CapServerMsg::Reply { value, has_data }, body)), maybe_s) => {
                        if let Some(mut s) = maybe_s {
                            let _ = s.finish();
                        }
                        break Ok(Reply {
                            value,
                            data: has_data.then(|| body.into_vec()),
                        });
                    }
                    (Ok((CapServerMsg::Err(e), _)), maybe_s) => {
                        if let Some(mut s) = maybe_s {
                            let _ = s.finish();
                        }
                        break Err(e.into());
                    }
                    (Ok(_), maybe_s) => {
                        if let Some(mut s) = maybe_s {
                            let _ = s.reset(1u32.into());
                        }
                        let _ = recv.stop(1u32.into());
                        break Err(format!("unexpected remote reply for {op}").into());
                    }
                    (Err(ReadFrameErr::Protocol(e)), maybe_s) => {
                        if let Some(mut s) = maybe_s {
                            let _ = s.reset(1u32.into());
                        }
                        let _ = recv.stop(1u32.into());
                        break Err(e.into());
                    }
                    (Err(ReadFrameErr::Transport(e)), maybe_s) => {
                        if let Some(mut s) = maybe_s {
                            let _ = s.reset(1u32.into());
                        }
                        break Err(HostError::coded(
                            "ENODEUNREACH",
                            format!("node {} disconnected during {op}: {e}", peer.name),
                        ));
                    }
                }
            }
        }
    };

    if result.is_err()
        && let Some(mut s) = send_holder.take()
    {
        let _ = s.reset(1u32.into());
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn finish_remote_http_wait(
    host: &Host,
    peer: &PeerInfo,
    op: &str,
    slot: Option<crate::codemode::StreamReservation<'_>>,
    stream_deadline: Option<(tokio::time::Instant, &'static str)>,
    peer_cancel: &CancellationToken,
    guard: &mut CapStreamGuard,
    budget: &Arc<Semaphore>,
    max_body: usize,
) -> std::result::Result<Reply, HostError> {
    tokio::select! {
        biased;
        _ = host.cancel.cancelled() => Err("cancelled".into()),
        _ = peer_cancel.cancelled() => Err(HostError::coded(
            "ENODEUNREACH",
            format!("node {} disconnected during {op}", peer.name),
        )),
        _ = async {
            match stream_deadline {
                Some((at, _)) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        } => Err(HostError::from(stream_deadline.map_or("timeout", |(_, m)| m))),
        frame = read_cap_frame::<CapServerMsg>(
            guard.recv.as_mut().unwrap(),
            CAP_RESP_HEADER_MAX,
            max_body,
            budget,
        ) => match frame {
            Ok((CapServerMsg::HttpHead { status, headers }, _)) => {
                let slot = slot.expect("stream slot reserved");
                let (send, recv) = guard.take_halves();
                let id = slot.insert_with_deadline(
                    StreamResource::RemoteReader(RemoteByteReader::new(send, recv, budget.clone())),
                    stream_deadline,
                );
                Ok(Reply::value(json!({
                    "status": status,
                    "headers": headers,
                    "body": id,
                })))
            }
            Ok((CapServerMsg::Reply { value, has_data }, body)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                Ok(Reply {
                    value,
                    data: has_data.then(|| body.into_vec()),
                })
            }
            Ok((CapServerMsg::Err(e), _)) => {
                guard.mark_finished();
                if let Some(s) = guard.send.as_mut() {
                    let _ = s.finish();
                }
                Err(e.into())
            }
            Ok(_) => Err(format!("unexpected remote reply for {op}").into()),
            Err(ReadFrameErr::Protocol(e)) => Err(e.into()),
            Err(ReadFrameErr::Transport(e)) => Err(HostError::coded(
                "ENODEUNREACH",
                format!("node {} disconnected during {op}: {e}", peer.name),
            )),
        }
    }
}
