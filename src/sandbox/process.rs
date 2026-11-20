#![cfg(unix)]

use std::{
    io,
    os::unix::process::{CommandExt, ExitStatusExt},
    process::{Child, Command, ExitStatus, Stdio},
};

use rustix::{
    io::Errno,
    process::{
        Pid, Signal, WaitId, WaitIdOptions, kill_process_group, test_kill_process_group, waitid,
    },
};
use serde_json::{Value, json};
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStderr, ChildStdin, ChildStdout},
    signal::unix::{SignalKind, signal},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::io::drain;

pub(super) async fn execute(
    program: &str,
    argv: &[String],
    opts: &Value,
    stdin: Option<Vec<u8>>,
    cancel: CancellationToken,
) -> Result<Value, String> {
    let deadline = super::io::timeout(opts)?.map(|d| Instant::now() + d);
    let mut command = Command::new(program);
    command
        .args(argv)
        .process_group(0)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = opts["cwd"].as_str() {
        command.current_dir(cwd);
    }
    if let Some(env) = opts["env"].as_object() {
        for (name, value) in env {
            command.env(name, value.as_str().ok_or("env values must be strings")?);
        }
    }
    // Subscribe before launch so even an immediate child exit wakes the waiter.
    let mut exits = signal(SignalKind::child()).map_err(|e| format!("spawn_failed: {e}"))?;
    let start_cancel = cancel.clone();
    let mut child = tokio::task::spawn_blocking(move || {
        if start_cancel.is_cancelled() {
            return Err("cancelled".to_owned());
        }
        if deadline.is_some_and(|t| Instant::now() >= t) {
            return Err("timeout: command".to_owned());
        }
        command.spawn().map_err(|e| {
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
    .map_err(|e| e.to_string())??;

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
            } } => Err("timeout: command".to_owned()),
            observed = observe_exit(pgid, &mut exits) => observed.map_err(|e| e.to_string()),
        };
        finished.cancel();
        let status = clean_group(child, pgid).await?;
        outcome?;
        Ok::<_, String>(status)
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
        drain(stdout, &finished),
        drain(stderr, &finished)
    );
    let status = status?;
    let pipe_error = |e| format!("pipe_failed: {e}");
    input.map_err(pipe_error)?;
    let (stdout, out_truncated) = stdout.map_err(pipe_error)?;
    let (stderr, err_truncated) = stderr.map_err(pipe_error)?;
    Ok(json!({
        "exit_code": status.code(), "signal": status.signal(), "stdout": stdout, "stderr": stderr,
        "truncated": out_truncated || err_truncated,
    }))
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

async fn observe_exit(pid: Pid, exits: &mut tokio::signal::unix::Signal) -> io::Result<()> {
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

async fn clean_group(mut child: Child, pgid: Pid) -> Result<ExitStatus, String> {
    // The group leader is still waitable, so the OS cannot recycle its ID
    // between observing its exit and signalling any surviving descendants.
    let killed = kill_process_group(pgid, Signal::KILL);
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("wait_failed: {e}"))?;
    // Darwin reports EPERM for a group containing only a zombie. After
    // reaping our leader, probe with signal 0 to confirm the group is gone.
    if let Err(e) = killed
        && e != Errno::SRCH
        && !(e == Errno::PERM && test_kill_process_group(pgid) == Err(Errno::SRCH))
    {
        return Err(format!("group_cleanup_failed: {e}"));
    }
    Ok(status)
}
