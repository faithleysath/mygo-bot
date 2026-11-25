#![cfg(unix)]

use std::{
    io,
    os::unix::process::{CommandExt, ExitStatusExt},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
};

#[cfg(target_vendor = "apple")]
use rustix::process::test_kill_process_group;
use rustix::{
    io::Errno,
    process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid},
};
use serde_json::Value;
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStderr, ChildStdin, ChildStdout},
    signal::unix::{SignalKind, signal},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    io::drain,
    live::{CommandGuard, Stream},
};

pub(super) async fn execute(
    program: &str,
    argv: &[String],
    cwd: &Path,
    opts: &Value,
    stdin: Option<Vec<u8>>,
    cancel: CancellationToken,
    command_preview: &CommandGuard<'_>,
) -> Result<Value, String> {
    let deadline = super::io::timeout(opts)?.map(|d| Instant::now() + d);
    let mut command = Command::new(program);
    command
        .args(argv)
        .current_dir(cwd)
        .process_group(0)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(env) = opts["env"].as_object() {
        for (name, value) in env {
            command.env(name, value.as_str().ok_or("env values must be strings")?);
        }
    }
    // Subscribe before launch so even an immediate child exit wakes the waiter.
    let mut exits = signal(SignalKind::child()).map_err(|e| format!("spawn_failed: {e}"))?;
    enum SpawnOutcome {
        Child(Child),
        TimedOut,
    }
    let start_cancel = cancel.clone();
    let SpawnOutcome::Child(mut child) = tokio::task::spawn_blocking(move || {
        if start_cancel.is_cancelled() {
            return Err("cancelled".to_owned());
        }
        if deadline.is_some_and(|t| Instant::now() >= t) {
            return Ok(SpawnOutcome::TimedOut);
        }
        command.spawn().map(SpawnOutcome::Child).map_err(|e| {
            let code = if e.kind() == io::ErrorKind::NotFound
                && command.get_current_dir().is_none_or(|cwd| cwd.is_dir())
            {
                "program_not_found"
            } else {
                "spawn_failed"
            };
            format!("{code}: {}: {e}", command.get_program().to_string_lossy())
        })
    })
    .await
    .map_err(|e| e.to_string())??
    else {
        return Ok(super::io::command_output(
            None,
            None,
            true,
            String::new(),
            String::new(),
            false,
        ));
    };

    let pgid = Pid::from_child(&child);
    let (input, stdout, stderr) = match pipes(&mut child) {
        Ok(pipes) => pipes,
        Err(e) => {
            clean_group(child, pgid).await?;
            return Err(format!("pipe_failed: {e}"));
        }
    };
    let finished = CancellationToken::new();
    let monitor = async {
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("cancelled".to_owned()),
            _ = async { match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            } } => Ok(true),
            observed = observe_exit(pgid, &mut exits) => observed.map(|()| false).map_err(|e| e.to_string()),
        };
        finished.cancel();
        let status = clean_group(child, pgid).await?;
        let timed_out = outcome?;
        Ok::<_, String>((status, timed_out))
    };
    let write = async {
        if let (Some(mut pipe), Some(bytes)) = (input, stdin) {
            tokio::select! {
                biased;
                _ = finished.cancelled() => (),
                result = pipe.write_all(&bytes) => match result {
                    Err(e) if e.kind() == io::ErrorKind::BrokenPipe => (),
                    result => result?,
                },
            }
        }
        Ok::<_, io::Error>(())
    };
    let (status, input, stdout, stderr) = tokio::join!(
        monitor,
        write,
        drain(stdout, &finished, Some((command_preview, Stream::Stdout))),
        drain(stderr, &finished, Some((command_preview, Stream::Stderr)))
    );
    let (status, timed_out) = status?;
    let pipe_error = |e| format!("pipe_failed: {e}");
    input.map_err(pipe_error)?;
    let (stdout, out_truncated) = stdout.map_err(pipe_error)?;
    let (stderr, err_truncated) = stderr.map_err(pipe_error)?;
    Ok(super::io::command_output(
        status.code().map(i64::from),
        status.signal(),
        timed_out,
        stdout,
        stderr,
        out_truncated || err_truncated,
    ))
}

fn pipes(child: &mut Child) -> io::Result<(Option<ChildStdin>, ChildStdout, ChildStderr)> {
    Ok((
        child.stdin.take().map(ChildStdin::from_std).transpose()?,
        ChildStdout::from_std(
            child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("missing stdout"))?,
        )?,
        ChildStderr::from_std(
            child
                .stderr
                .take()
                .ok_or_else(|| io::Error::other("missing stderr"))?,
        )?,
    ))
}

pub(crate) async fn observe_exit(
    pid: Pid,
    exits: &mut tokio::signal::unix::Signal,
) -> io::Result<()> {
    let flags = WaitIdOptions::EXITED | WaitIdOptions::NOWAIT | WaitIdOptions::NOHANG;
    loop {
        match waitid(WaitId::Pid(pid), flags) {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {
                exits
                    .recv()
                    .await
                    .ok_or_else(|| io::Error::other("SIGCHLD listener closed"))?;
            }
            Err(Errno::INTR) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

async fn clean_group(child: Child, pgid: Pid) -> Result<ExitStatus, String> {
    reap_group(child, pgid)
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

// Signal while the leader is still waitable. Dropping the JoinHandle leaves its reap running.
pub(crate) fn reap_group(
    mut child: Child,
    pgid: Pid,
) -> tokio::task::JoinHandle<io::Result<ExitStatus>> {
    let killed = kill_process_group(pgid, Signal::KILL);
    tokio::task::spawn_blocking(move || {
        let status = child
            .wait()
            .map_err(|e| io::Error::other(format!("wait_failed: {e}")))?;
        match killed {
            Ok(()) | Err(Errno::SRCH) => Ok(status),
            // Darwin can report EPERM when only the zombie leader remains.
            #[cfg(target_vendor = "apple")]
            Err(Errno::PERM) if test_kill_process_group(pgid) == Err(Errno::SRCH) => Ok(status),
            Err(e) => Err(io::Error::other(format!("group_cleanup_failed: {e}"))),
        }
    })
}
