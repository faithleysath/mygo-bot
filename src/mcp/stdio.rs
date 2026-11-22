use rmcp::{
    RoleClient,
    service::{RxJsonRpcMessage, TxJsonRpcMessage},
    transport::{Transport, async_rw::AsyncRwTransport},
};
use std::{collections::BTreeMap, io, process::Stdio, time::Duration};
use tokio::process::{ChildStdin, ChildStdout, Command};

pub(super) struct StdioConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<String>,
}

type Pipes = AsyncRwTransport<RoleClient, ChildStdout, ChildStdin>;
struct ServerProcess {
    pipes: Pipes,
    child: platform::Child,
}

pub(super) fn spawn(
    config: StdioConfig,
) -> io::Result<impl Transport<RoleClient, Error = io::Error>> {
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
    let pipes = Pipes::new(stdout, stdin).with_max_line_length(super::WIRE_LIMIT);
    Ok(ServerProcess { pipes, child })
}
impl Transport<RoleClient> for ServerProcess {
    type Error = io::Error;
    fn send(
        &mut self,
        msg: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        self.pipes.send(msg)
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        // Descendants can keep stdout open after the leader exits. Check the leader as well as the pipe.
        tokio::select! { biased; msg=self.pipes.receive()=>msg, _=self.child.leader_exit()=>None }
    }
    async fn close(&mut self) -> io::Result<()> {
        // A stuck send can own the writer mutex. Cleanup must still terminate the whole process tree.
        let _ = tokio::time::timeout(Duration::from_millis(150), self.pipes.close()).await;
        let _ = tokio::time::timeout(Duration::from_millis(150), self.child.leader_exit()).await;
        self.child.finish().await
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
    use process_wrap::tokio::{ChildWrapper, CommandWrap, JobObject, KillOnDrop};
    pub(super) struct Child(Box<dyn ChildWrapper>);
    pub(super) fn spawn(cmd: Command) -> io::Result<(Child, ChildStdout, ChildStdin)> {
        let mut command = CommandWrap::from(cmd);
        command.wrap(JobObject).wrap(KillOnDrop);
        let mut child = Child(command.spawn()?);
        let stdout = child
            .0
            .stdout()
            .take()
            .ok_or_else(|| io::Error::other("stdio pipe unavailable"))?;
        let stdin = child
            .0
            .stdin()
            .take()
            .ok_or_else(|| io::Error::other("stdio pipe unavailable"))?;
        Ok((child, stdout, stdin))
    }
    impl Child {
        pub(super) async fn leader_exit(&mut self) -> io::Result<()> {
            loop {
                if self.0.try_wait()?.is_some() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        pub(super) async fn finish(&mut self) -> io::Result<()> {
            self.0.start_kill()?;
            self.0.wait().await?;
            Ok(())
        }
    }
}
