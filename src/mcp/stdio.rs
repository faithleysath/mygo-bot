use super::client::McpError;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    codec::{FramedRead, LinesCodec, LinesCodecError},
    sync::CancellationToken,
};

pub(super) struct StdioConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<String>,
}

pub(super) struct SpawnedChild {
    child: platform::Child,
    stdout: ChildStdout,
    stdin: ChildStdin,
}

pub(super) fn spawn(config: StdioConfig) -> io::Result<SpawnedChild> {
    #[cfg(windows)]
    if std::path::Path::new(config.command.trim_end_matches([' ', '.']))
        .extension()
        .is_some_and(|s| s.eq_ignore_ascii_case("cmd") || s.eq_ignore_ascii_case("bat"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "stdio requires a native executable",
        ));
    }
    let mut cmd = Command::new(config.command);
    // Keep executable lookup and OS runtime paths; never inherit the daemon's secret environment.
    cmd.env_clear();
    for name in [
        "PATH",
        "HOME",
        "USERPROFILE",
        "SystemRoot",
        "WINDIR",
        "TMPDIR",
        "TMP",
        "TEMP",
        "LANG",
        "LC_ALL",
    ] {
        if let Some(value) = std::env::var_os(name) {
            cmd.env(name, value);
        }
    }
    cmd.args(config.args).envs(config.env);
    if let Some(cwd) = config.cwd {
        cmd.current_dir(cwd);
    }
    // A server may log credentials or flood stderr. The null device provides a nonblocking sink.
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let (child, stdout, stdin) = platform::spawn(cmd)?;
    Ok(SpawnedChild {
        child,
        stdout,
        stdin,
    })
}

type PendingMap = Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpError>>>>>;

struct WriteFrame {
    bytes: Vec<u8>,
    ack: Option<oneshot::Sender<Result<(), ()>>>,
}

pub(super) struct StdioTransport {
    writer_tx: mpsc::Sender<WriteFrame>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
    reader: Mutex<Option<JoinHandle<()>>>,
}

struct PendingGuard {
    id: u64,
    pending: PendingMap,
    writer_tx: mpsc::Sender<WriteFrame>,
    closed: Arc<AtomicBool>,
    notify_cancel: bool,
    disarmed: bool,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.pending.lock().unwrap().remove(&self.id);
            if self.notify_cancel && !self.closed.load(Ordering::Acquire) {
                let notice = json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {
                        "requestId": self.id,
                        "reason": "request cancelled",
                    }
                });
                if let Ok(mut bytes) = serde_json::to_vec(&notice) {
                    bytes.push(b'\n');
                    let _ = self.writer_tx.try_send(WriteFrame { bytes, ack: None });
                }
            }
        }
    }
}

fn framed_line(payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len() + 1);
    bytes.extend_from_slice(payload);
    bytes.push(b'\n');
    bytes
}

impl StdioTransport {
    pub(super) fn start(spawned: SpawnedChild, cache_epoch: Arc<AtomicU64>) -> Self {
        let (writer_tx, writer_rx) = mpsc::channel::<WriteFrame>(128);
        let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let writer_closed = closed.clone();
        let writer_shutdown = shutdown.clone();
        let writer_handle = tokio::spawn(run_writer(
            spawned.stdin,
            writer_rx,
            writer_closed,
            writer_shutdown,
        ));

        let task_writer_tx = writer_tx.clone();
        let task_pending = pending.clone();
        let task_closed = closed.clone();
        let task_shutdown = shutdown.clone();
        let reader = tokio::spawn(async move {
            run_reader(
                spawned.child,
                spawned.stdout,
                writer_handle,
                task_writer_tx,
                task_pending,
                task_closed,
                task_shutdown,
                cache_epoch,
            )
            .await;
        });

        Self {
            writer_tx,
            pending,
            closed,
            shutdown,
            reader: Mutex::new(Some(reader)),
        }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(super) fn cancel(&self) {
        self.closed.store(true, Ordering::Release);
        self.shutdown.cancel();
    }

    pub(super) async fn notify(&self, payload: &[u8]) -> Result<(), McpError> {
        if self.is_closed() {
            return Err(McpError::TransportClosed);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriteFrame {
                bytes: framed_line(payload),
                ack: Some(ack_tx),
            })
            .await
            .map_err(|_| McpError::TransportClosed)?;
        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            _ => Err(McpError::TransportClosed),
        }
    }

    pub(super) async fn request(&self, id: u64, payload: &[u8]) -> Result<Value, McpError> {
        if self.is_closed() {
            return Err(McpError::TransportClosed);
        }
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let mut guard = PendingGuard {
            id,
            pending: self.pending.clone(),
            writer_tx: self.writer_tx.clone(),
            closed: self.closed.clone(),
            notify_cancel: false,
            disarmed: false,
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .writer_tx
            .send(WriteFrame {
                bytes: framed_line(payload),
                ack: Some(ack_tx),
            })
            .await
            .is_err()
        {
            return Err(McpError::TransportClosed);
        }
        // The writer task now owns writing the entire frame ahead of any queued cancellation.
        guard.notify_cancel = true;
        match ack_rx.await {
            Ok(Ok(())) => {}
            _ => {
                guard.notify_cancel = false;
                return Err(McpError::TransportClosed);
            }
        }
        let result = rx.await.unwrap_or(Err(McpError::TransportClosed));
        guard.disarmed = true;
        result
    }

    pub(super) async fn close(&self, timeout: Duration) {
        self.cancel();
        if let Some(handle) = self.reader.lock().await.take() {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        self.cancel();
    }
}

async fn run_writer<W: tokio::io::AsyncWrite + Unpin>(
    mut stdin: W,
    mut rx: mpsc::Receiver<WriteFrame>,
    closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
) {
    loop {
        let frame = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            frame = rx.recv() => match frame {
                Some(frame) => frame,
                None => break,
            },
        };
        let write_res = tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err(()),
            res = async {
                stdin.write_all(&frame.bytes).await.map_err(|_| ())?;
                stdin.flush().await.map_err(|_| ())
            } => res,
        };
        let failed = write_res.is_err();
        if let Some(ack) = frame.ack {
            let _ = ack.send(write_res);
        }
        if failed {
            closed.store(true, Ordering::Release);
            shutdown.cancel();
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_reader(
    mut child: platform::Child,
    stdout: ChildStdout,
    writer_handle: JoinHandle<()>,
    writer_tx: mpsc::Sender<WriteFrame>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
    cache_epoch: Arc<AtomicU64>,
) {
    let mut lines = FramedRead::new(stdout, LinesCodec::new_with_max_length(super::WIRE_LIMIT));
    let mut terminal_error = None;
    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = child.leader_exit() => break,
            line = lines.next() => line,
        };
        let Some(result) = next else { break };
        let line = match result {
            Ok(line) => line,
            Err(LinesCodecError::MaxLineLengthExceeded) => {
                terminal_error = Some(McpError::ResponseTooLarge(super::WIRE_LIMIT));
                break;
            }
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(method) = msg.get("method").and_then(Value::as_str) {
            if let Some(req_id) = msg.get("id")
                && !req_id.is_null()
            {
                let reply = if method == "ping" {
                    json!({"jsonrpc":"2.0","id":req_id,"result":{}})
                } else {
                    json!({"jsonrpc":"2.0","id":req_id,"error":{"code":-32601,"message":"method not found"}})
                };
                if let Ok(mut bytes) = serde_json::to_vec(&reply) {
                    bytes.push(b'\n');
                    let _ = writer_tx.try_send(WriteFrame { bytes, ack: None });
                }
            } else if matches!(
                method,
                "notifications/tools/list_changed"
                    | "notifications/prompts/list_changed"
                    | "notifications/resources/list_changed"
            ) {
                cache_epoch.fetch_add(1, Ordering::Release);
            }
            continue;
        }
        if let Some(id) = msg.get("id").and_then(Value::as_u64) {
            let res = super::client::parse_rpc_message(&msg);
            if let Some(tx) = pending.lock().unwrap().remove(&id) {
                let _ = tx.send(res);
            }
        }
    }

    closed.store(true, Ordering::Release);
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_millis(150), writer_handle).await;
    let _ = tokio::time::timeout(Duration::from_millis(150), child.leader_exit()).await;
    let _ = child.finish().await;

    let mut waiters = pending.lock().unwrap();
    for (_, tx) in waiters.drain() {
        let err = terminal_error
            .clone()
            .unwrap_or(McpError::TransportClosed);
        let _ = tx.send(Err(err));
    }
}

#[cfg(unix)]
mod platform {
    use super::*;
    use rustix::process::Pid;
    use tokio::signal::unix::{SignalKind, signal};

    pub(super) struct Child {
        inner: Option<std::process::Child>,
        pgid: Pid,
        exits: tokio::signal::unix::Signal,
    }
    pub(super) fn spawn(mut cmd: Command) -> io::Result<(Child, ChildStdout, ChildStdin)> {
        let exits = signal(SignalKind::child())?;
        let inner = cmd.process_group(0).as_std_mut().spawn()?;
        let pgid = Pid::from_child(&inner);
        let mut guard = Child {
            inner: Some(inner),
            pgid,
            exits,
        };
        // Own the process group before the fallible registrations with the async reactor.
        let child = guard.inner.as_mut().unwrap();
        let stdout = ChildStdout::from_std(
            child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("stdio stdout unavailable"))?,
        )?;
        let stdin = ChildStdin::from_std(
            child
                .stdin
                .take()
                .ok_or_else(|| io::Error::other("stdio stdin unavailable"))?,
        )?;
        Ok((guard, stdout, stdin))
    }
    impl Child {
        pub(super) async fn leader_exit(&mut self) -> io::Result<()> {
            crate::codemode::observe_exit(self.pgid, &mut self.exits).await
        }
        pub(super) async fn finish(&mut self) -> io::Result<()> {
            if let Some(child) = self.inner.take() {
                crate::codemode::reap_group(child, self.pgid)
                    .await
                    .map_err(io::Error::other)??;
            }
            Ok(())
        }
    }
    impl Drop for Child {
        fn drop(&mut self) {
            if let Some(child) = self.inner.take() {
                drop(crate::codemode::reap_group(child, self.pgid));
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{
        mem::{offset_of, size_of, size_of_val, zeroed},
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        ptr::{null, null_mut},
    };
    use windows_sys::Win32::{
        Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE},
        System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First,
                Thread32Next,
            },
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
                QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
            },
            Threading::{
                CREATE_NO_WINDOW, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
            },
        },
    };

    fn checked(ok: i32) -> io::Result<()> {
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn resume_process_threads(pid: u32) -> io::Result<()> {
        let snap = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snap == INVALID_HANDLE_VALUE || snap.is_null() {
            return Err(io::Error::last_os_error());
        }
        let snap = unsafe { OwnedHandle::from_raw_handle(snap) };
        let mut entry: THREADENTRY32 = unsafe { zeroed() };
        entry.dwSize = size_of::<THREADENTRY32>() as u32;
        checked(unsafe { Thread32First(snap.as_raw_handle(), &mut entry) })?;
        let min_size = (offset_of!(THREADENTRY32, th32OwnerProcessID) + size_of::<u32>()) as u32;
        let mut resumed = 0usize;
        loop {
            if entry.dwSize >= min_size && entry.th32OwnerProcessID == pid {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let thread = unsafe { OwnedHandle::from_raw_handle(thread) };
                if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
                    return Err(io::Error::last_os_error());
                }
                resumed += 1;
            }
            entry.dwSize = size_of::<THREADENTRY32>() as u32;
            if unsafe { Thread32Next(snap.as_raw_handle(), &mut entry) } == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    break;
                }
                return Err(err);
            }
        }
        if resumed == 0 {
            return Err(io::Error::other("stdio process has no initial thread"));
        }
        Ok(())
    }

    pub(super) struct Child {
        inner: tokio::process::Child,
        job: OwnedHandle,
        exited: bool,
    }

    pub(super) fn spawn(mut cmd: Command) -> io::Result<(Child, ChildStdout, ChildStdin)> {
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = unsafe { OwnedHandle::from_raw_handle(handle) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        checked(unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                size_of_val(&limits) as u32,
            )
        })?;
        // Assign the suspended child to the job before user code can spawn grandchildren.
        cmd.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
        let mut inner = cmd.spawn()?;
        let Some(proc_handle) = inner.raw_handle() else {
            let _ = inner.start_kill();
            return Err(io::Error::other("stdio process handle unavailable"));
        };
        let Some(pid) = inner.id() else {
            let _ = inner.start_kill();
            return Err(io::Error::other("stdio process id unavailable"));
        };
        if let Err(err) = checked(unsafe { AssignProcessToJobObject(job.as_raw_handle(), proc_handle) })
        {
            let _ = inner.start_kill();
            return Err(err);
        }
        if let Err(err) = resume_process_threads(pid) {
            let _ = unsafe { TerminateJobObject(job.as_raw_handle(), 1) };
            let _ = inner.start_kill();
            return Err(err);
        }
        let stdout = inner
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("stdio pipe unavailable"))?;
        let stdin = inner
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("stdio pipe unavailable"))?;
        Ok((
            Child {
                inner,
                job,
                exited: false,
            },
            stdout,
            stdin,
        ))
    }

    impl Child {
        pub(super) async fn leader_exit(&mut self) -> io::Result<()> {
            if !self.exited {
                let _ = self.inner.wait().await?;
                self.exited = true;
            }
            Ok(())
        }

        pub(super) async fn finish(&mut self) -> io::Result<()> {
            let _ = unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
            if !self.exited {
                let _ = self.inner.start_kill();
                let _ = tokio::time::timeout(Duration::from_millis(500), self.inner.wait()).await;
                self.exited = true;
            }
            for _ in 0..50 {
                let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
                if unsafe {
                    QueryInformationJobObject(
                        self.job.as_raw_handle(),
                        JobObjectBasicAccountingInformation,
                        (&raw mut info).cast(),
                        size_of_val(&info) as u32,
                        null_mut(),
                    )
                } == 0
                    || info.ActiveProcesses == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        }
    }
}
