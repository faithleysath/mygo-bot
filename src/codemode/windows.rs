use std::{
    cmp::Ordering,
    env,
    ffi::{OsStr, OsString, c_void},
    fs::{File, OpenOptions},
    io::{self, Write},
    mem::{size_of, size_of_val},
    os::windows::{
        ffi::{OsStrExt, OsStringExt},
        fs::OpenOptionsExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    },
    path::{Path, PathBuf},
    ptr::{null, null_mut},
    sync::Arc,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
    sync::Notify,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::{
    Foundation::{
        ERROR_INSUFFICIENT_BUFFER, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
    },
    Globalization::{CSTR_EQUAL, CompareStringOrdinal},
    Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_OUTBOUND, SECURITY_ANONYMOUS, SearchPathW,
    },
    System::{
        JobObjects::*,
        Pipes::{CreateNamedPipeW, PIPE_NOWAIT, PIPE_REJECT_REMOTE_CLIENTS},
        Threading::*,
    },
};

use super::io::drain;

fn checked(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn wide(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(Some(0)).collect()
}

// The first token is parsed as argv[0]. Later tokens use the Windows C runtime
// rules: double backslashes preceding a quote, including the closing quote.
fn command_line(program: &str, argv: &[String]) -> io::Result<Vec<u16>> {
    if program.is_empty() || program.contains(['"', '\0']) || argv.iter().any(|a| a.contains('\0'))
    {
        return Err(io::Error::other("invalid program or argument"));
    }
    let mut line = format!("\"{program}\"");
    for arg in argv {
        line.push(' ');
        if !arg.is_empty() && !arg.contains([' ', '\t', '"']) {
            line.push_str(arg);
            continue;
        }
        line.push('"');
        let mut slashes = 0;
        for ch in arg.chars() {
            if ch == '\\' {
                slashes += 1;
                continue;
            }
            let count = if ch == '"' { 2 * slashes + 1 } else { slashes };
            line.extend(std::iter::repeat_n('\\', count));
            line.push(ch);
            slashes = 0;
        }
        line.extend(std::iter::repeat_n('\\', 2 * slashes));
        line.push('"');
    }
    Ok(wide(line.as_ref()))
}

fn environment(opts: &Value) -> io::Result<Option<Vec<u16>>> {
    let Some(overrides) = opts["env"].as_object().filter(|e| !e.is_empty()) else {
        return Ok(None);
    };
    let mut entries = Vec::new();
    // Stable sorting below keeps overrides ahead of inherited keys of any case.
    for (key, value) in overrides.iter().rev() {
        let value = value
            .as_str()
            .ok_or_else(|| io::Error::other("env values must be strings"))?;
        if key.is_empty() || key.contains(['\0', '=']) || value.contains('\0') {
            return Err(io::Error::other("invalid environment name or value"));
        }
        entries.push((wide(key.as_ref()), OsString::from(value)));
    }
    entries.extend(env::vars_os().map(|(key, value)| (wide(&key), value)));
    let compare = |a: &[u16], b: &[u16]| -> Ordering {
        // Each key is NUL-terminated. Windows uses this same ordinal case fold.
        unsafe { CompareStringOrdinal(a.as_ptr(), -1, b.as_ptr(), -1, 1) }.cmp(&CSTR_EQUAL)
    };
    entries.sort_by(|a, b| compare(&a.0, &b.0));
    entries.dedup_by(|a, b| compare(&a.0, &b.0).is_eq());
    let mut block = Vec::new();
    for (key, value) in entries {
        block.extend_from_slice(&key[..key.len() - 1]);
        block.push(b'=' as u16);
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    Ok(Some(block))
}

fn executable(program: &str, cwd: &Path, opts: &Value) -> io::Result<Vec<u16>> {
    let path = Path::new(program);
    let mut search = None;
    let name = if path.file_name() == Some(path.as_os_str()) {
        if let Some((_, value)) = opts["env"].as_object().and_then(|env| {
            env.iter()
                .rev()
                .find(|(k, _)| k.eq_ignore_ascii_case("PATH"))
        }) {
            let paths = env::split_paths(OsStr::new(value.as_str().unwrap_or_default()))
                .map(|p| cwd.join(p));
            search = Some(wide(&env::join_paths(paths).map_err(io::Error::other)?));
        }
        wide(path.as_os_str())
    } else {
        wide(cwd.join(path).as_os_str())
    };
    let mut buf = vec![0u16; 260];
    loop {
        let len = unsafe {
            SearchPathW(
                search.as_ref().map_or(null(), |v| v.as_ptr()),
                name.as_ptr(),
                windows_sys::w!(".exe"),
                buf.len() as u32,
                buf.as_mut_ptr(),
                null_mut(),
            )
        } as usize;
        if len == 0 {
            return Err(io::Error::last_os_error());
        }
        if len < buf.len() {
            buf.truncate(len + 1);
            let path = PathBuf::from(OsString::from_wide(&buf[..len]));
            if path
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("bat") || e.eq_ignore_ascii_case("cmd"))
            {
                return Err(io::Error::other(
                    "batch files require an explicit command interpreter",
                ));
            }
            return Ok(buf);
        }
        buf.resize(len, 0);
    }
}

fn pipe_name() -> io::Result<String> {
    Ok(format!(
        r"\\.\pipe\mygo-{}-{:016x}",
        std::process::id(),
        getrandom::u64().map_err(io::Error::other)?
    ))
}

async fn output_pipe() -> io::Result<(NamedPipeServer, File)> {
    let name = pipe_name()?;
    let server = ServerOptions::new()
        .access_outbound(false)
        .first_pipe_instance(true)
        .max_instances(1)
        .create(&name)?;
    let client = OpenOptions::new()
        .write(true)
        .share_mode(0)
        .security_qos_flags(SECURITY_ANONYMOUS)
        .open(name)?;
    server.connect().await?;
    Ok((server, client))
}

fn input_pipe() -> io::Result<(File, File)> {
    let name = pipe_name()?;
    // Only the parent's end is nonblocking. A full byte-mode pipe accepts a
    // partial write or zero bytes. Dropping this end delivers EOF to the child.
    let handle = unsafe {
        CreateNamedPipeW(
            wide(name.as_ref()).as_ptr(),
            PIPE_ACCESS_OUTBOUND | FILE_FLAG_FIRST_PIPE_INSTANCE,
            PIPE_NOWAIT | PIPE_REJECT_REMOTE_CLIENTS,
            1,
            64 * 1024,
            0,
            0,
            null(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let server = unsafe { File::from_raw_handle(handle) };
    let client = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .security_qos_flags(SECURITY_ANONYMOUS)
        .open(name)?;
    Ok((server, client))
}

struct Attributes(Vec<usize>);

impl Attributes {
    fn new() -> io::Result<Self> {
        let mut size = 0;
        unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut size) };
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) || size == 0 {
            return Err(error);
        }
        let mut storage = vec![0usize; size.div_ceil(size_of::<usize>())];
        checked(unsafe {
            InitializeProcThreadAttributeList(storage.as_mut_ptr().cast(), 2, 0, &mut size)
        })?;
        Ok(Self(storage))
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.0.as_mut_ptr().cast()) }
    }
}

struct Job(OwnedHandle);

impl Job {
    fn new() -> io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self(unsafe { OwnedHandle::from_raw_handle(handle) });
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        checked(unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                size_of_val(&limits) as u32,
            )
        })?;
        Ok(job)
    }

    async fn terminate(&self) -> io::Result<()> {
        checked(unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) })?;
        loop {
            let mut status = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            checked(unsafe {
                QueryInformationJobObject(
                    self.0.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    (&raw mut status).cast(),
                    size_of_val(&status) as u32,
                    null_mut(),
                )
            })?;
            if status.ActiveProcesses == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

struct Process {
    handle: OwnedHandle,
    wait: PTP_WAIT,
    exited: Arc<Notify>,
}

impl Process {
    fn new(handle: OwnedHandle) -> io::Result<Self> {
        unsafe extern "system" fn notify(
            _: PTP_CALLBACK_INSTANCE,
            context: *mut c_void,
            _: PTP_WAIT,
            _: u32,
        ) {
            // Drop disarms and joins this callback before freeing its storage.
            unsafe { &*context.cast::<Notify>() }.notify_one();
        }
        let exited = Arc::new(Notify::new());
        let context = Arc::as_ptr(&exited).cast_mut().cast();
        let wait = unsafe { CreateThreadpoolWait(Some(notify), context, null()) };
        if wait == 0 {
            return Err(io::Error::last_os_error());
        }
        unsafe { SetThreadpoolWait(wait, handle.as_raw_handle(), null()) };
        Ok(Self {
            handle,
            wait,
            exited,
        })
    }

    async fn exit_code(&self) -> io::Result<u32> {
        self.exited.notified().await;
        let mut code = 0;
        checked(unsafe { GetExitCodeProcess(self.handle.as_raw_handle(), &mut code) })?;
        Ok(code)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        unsafe {
            SetThreadpoolWait(self.wait, null_mut(), null());
            WaitForThreadpoolWaitCallbacks(self.wait, 1);
            CloseThreadpoolWait(self.wait);
        }
    }
}

fn launch(
    program: &str,
    argv: &[String],
    opts: &Value,
    stdio: [File; 3],
    cancel: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<(Job, Process), String> {
    let failed = |e: io::Error| format!("spawn_failed: {program}: {e}");
    let mut cmd = command_line(program, argv).map_err(failed)?;
    let cwd = std::path::absolute(opts["cwd"].as_str().unwrap_or(".")).map_err(failed)?;
    if !cwd.is_dir() {
        return Err(format!("spawn_failed: invalid cwd {}", cwd.display()));
    }
    let environment = environment(opts).map_err(failed)?;
    let path = executable(program, &cwd, opts).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            format!("program_not_found: {program}: {e}")
        } else {
            failed(e)
        }
    })?;
    let cwd = wide(cwd.as_os_str());
    let job = Job::new().map_err(failed)?;
    let jobs = [job.0.as_raw_handle()];
    let handles = stdio.each_ref().map(|f| f.as_raw_handle());
    for handle in handles {
        checked(unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) })
            .map_err(failed)?;
    }
    let mut attrs = Attributes::new().map_err(failed)?;
    let list = attrs.0.as_mut_ptr().cast();
    for (attr, values) in [
        (PROC_THREAD_ATTRIBUTE_JOB_LIST, jobs.as_slice()),
        (PROC_THREAD_ATTRIBUTE_HANDLE_LIST, handles.as_slice()),
    ] {
        checked(unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                attr as usize,
                values.as_ptr().cast(),
                size_of_val(values),
                null_mut(),
                null(),
            )
        })
        .map_err(failed)?;
    }
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of_val(&startup) as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    [
        startup.StartupInfo.hStdInput,
        startup.StartupInfo.hStdOutput,
        startup.StartupInfo.hStdError,
    ] = handles;
    startup.lpAttributeList = list;
    let mut info = PROCESS_INFORMATION::default();
    if cancel.is_cancelled() {
        return Err("cancelled".into());
    }
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err("timeout: command".into());
    }
    // Both attribute arrays and all handles live until CreateProcessW returns.
    // The job is already attached when the child's first instruction runs.
    checked(unsafe {
        CreateProcessW(
            path.as_ptr(),
            cmd.as_mut_ptr(),
            null(),
            null(),
            1,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW,
            environment.as_ref().map_or(null(), |v| v.as_ptr().cast()),
            cwd.as_ptr(),
            &startup.StartupInfo,
            &mut info,
        )
    })
    .map_err(failed)?;
    drop(unsafe { OwnedHandle::from_raw_handle(info.hThread) });
    let process =
        Process::new(unsafe { OwnedHandle::from_raw_handle(info.hProcess) }).map_err(failed)?;
    Ok((job, process))
}

async fn feed(mut pipe: File, bytes: Vec<u8>, finished: &CancellationToken) -> io::Result<()> {
    let mut remaining = bytes.as_slice();
    while !remaining.is_empty() && !finished.is_cancelled() {
        let chunk = &remaining[..remaining.len().min(64 * 1024)];
        match pipe.write(chunk) {
            Ok(0) => tokio::select! {
                _ = finished.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(5)) => (),
            },
            Ok(n) => remaining = &remaining[n..],
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => break,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub async fn execute(
    program: &str,
    argv: &[String],
    opts: &Value,
    stdin: Option<Vec<u8>>,
    cancel: CancellationToken,
) -> Result<Value, String> {
    let deadline = super::io::timeout(opts)?.map(|d| Instant::now() + d);
    if cancel.is_cancelled() {
        return Err("cancelled".into());
    }
    let pipe_error = |e: io::Error| format!("pipe_failed: {e}");
    let (input, child_input) = match stdin {
        Some(bytes) => {
            let (server, child) = input_pipe().map_err(pipe_error)?;
            (Some((server, bytes)), child)
        }
        None => (None, File::open(r"\\.\NUL").map_err(pipe_error)?),
    };
    let ((stdout, child_out), (stderr, child_err)) =
        tokio::try_join!(output_pipe(), output_pipe()).map_err(pipe_error)?;
    let program = program.to_owned();
    let argv = argv.to_owned();
    let opts = opts.clone();
    let launch_cancel = cancel.child_token();
    let _cancel_on_drop = launch_cancel.clone().drop_guard();
    let (job, process) = tokio::task::spawn_blocking(move || {
        launch(
            &program,
            &argv,
            &opts,
            [child_input, child_out, child_err],
            &launch_cancel,
            deadline,
        )
    })
    .await
    .map_err(|e| format!("spawn_failed: {e}"))??;
    let finished = CancellationToken::new();
    let monitor = async {
        let timeout = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        let status = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("cancelled".to_owned()),
            _ = timeout => Err("timeout: command".to_owned()),
            result = process.exit_code() => result.map_err(|e| format!("wait_failed: {e}")),
        };
        finished.cancel();
        job.terminate()
            .await
            .map_err(|e| format!("group_cleanup_failed: {e}"))?;
        status
    };
    let writer = async {
        if let Some((pipe, bytes)) = input {
            feed(pipe, bytes, &finished).await?;
        }
        Ok(())
    };
    let (status, written, out, err) = tokio::join!(
        monitor,
        writer,
        drain(stdout, &finished),
        drain(stderr, &finished)
    );
    let status = status?;
    written.map_err(pipe_error)?;
    let (out, out_truncated) = out.map_err(pipe_error)?;
    let (err, err_truncated) = err.map_err(pipe_error)?;
    Ok(json!({
        "exit_code": status,
        "signal": null,
        "stdout": out,
        "stderr": err,
        "truncated": out_truncated || err_truncated,
    }))
}
