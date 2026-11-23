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
    sync::{Mutex, oneshot},
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

pub(super) struct StdioTransport {
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
    shutdown: CancellationToken,
    reader: Mutex<Option<JoinHandle<()>>>,
}

struct PendingGuard {
    id: u64,
    pending: PendingMap,
    disarmed: bool,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.pending.lock().unwrap().remove(&self.id);
        }
    }
}

impl StdioTransport {
    pub(super) fn start(spawned: SpawnedChild, cache_epoch: Arc<AtomicU64>) -> Self {
        let stdin = Arc::new(Mutex::new(Some(spawned.stdin)));
        let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let task_stdin = stdin.clone();
        let task_pending = pending.clone();
        let task_closed = closed.clone();
        let task_shutdown = shutdown.clone();
        let reader = tokio::spawn(async move {
            run_reader(
                spawned.child,
                spawned.stdout,
                task_stdin,
                task_pending,
                task_closed,
                task_shutdown,
                cache_epoch,
            )
            .await;
        });

        Self {
            stdin,
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
        write_line(&self.stdin, payload)
            .await
            .map_err(|_| McpError::TransportClosed)
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
            disarmed: false,
        };
        if let Err(()) = write_line(&self.stdin, payload).await {
            return Err(McpError::TransportClosed);
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

async fn write_line(stdin: &Mutex<Option<ChildStdin>>, payload: &[u8]) -> Result<(), ()> {
    let mut guard = stdin.lock().await;
    let pipe = guard.as_mut().ok_or(())?;
    pipe.write_all(payload).await.map_err(|_| ())?;
    pipe.write_all(b"\n").await.map_err(|_| ())?;
    pipe.flush().await.map_err(|_| ())?;
    Ok(())
}

async fn run_reader(
    mut child: platform::Child,
    stdout: ChildStdout,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
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
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    let _ = write_line(&stdin, &bytes).await;
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
    if let Ok(mut guard) = tokio::time::timeout(Duration::from_millis(150), stdin.lock()).await {
        *guard = None;
    }
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
        mem::size_of_val,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        ptr::{null, null_mut},
    };
    use windows_sys::Win32::System::{
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        },
        Threading::CREATE_NO_WINDOW,
    };

    fn checked(ok: i32) -> io::Result<()> {
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
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
        cmd.creation_flags(CREATE_NO_WINDOW);
        let mut inner = cmd.spawn()?;
        let Some(proc_handle) = inner.raw_handle() else {
            let _ = inner.start_kill();
            return Err(io::Error::other("stdio process handle unavailable"));
        };
        if let Err(err) = checked(unsafe { AssignProcessToJobObject(job.as_raw_handle(), proc_handle) })
        {
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
