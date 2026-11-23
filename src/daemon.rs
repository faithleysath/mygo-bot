use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    net::SocketAddr,
    process::{self, Command, Stdio},
};

const CHILD_ENV: &str = "MYGO_BOT_DAEMON_CHILD";
const LOG: &str = "mygo-bot.log";

#[derive(Serialize, Deserialize)]
pub struct Ready {
    pub listen: SocketAddr,
    #[cfg(unix)]
    pub socket: Option<String>,
    pub token: Option<String>,
}

impl Ready {
    fn log(&self, pid: u32) {
        if let Some(token) = &self.token {
            tracing::info!(%token, "generated TCP Bearer token");
        }
        #[cfg(unix)]
        if let Some(path) = &self.socket {
            tracing::info!(%path, "Unix socket ready");
        }
        tracing::info!(pid, listen = %self.listen, "TCP ready");
    }

    pub fn announce(mut self, parent: &mut Option<io::Stdout>) -> io::Result<()> {
        if let Some(out) = parent.take() {
            reply(out, Ok(&self))?;
            self.token = None;
        }
        self.log(process::id());
        Ok(())
    }
}

fn reply(out: io::Stdout, result: Result<&Ready, String>) -> io::Result<()> {
    let mut out = out.lock();
    serde_json::to_writer(&mut out, &result)?;
    writeln!(out)?;
    out.flush()
}

pub fn start(options: super::Options) -> Result<(), Box<dyn std::error::Error>> {
    if options.foreground {
        return super::run(options, &mut None);
    }
    if env::var_os(CHILD_ENV).is_none() {
        return Ok(launch()?);
    }
    let mut parent = Some(io::stdout());
    #[cfg(unix)]
    let result = rustix::process::setsid()
        .map_err(Into::into)
        .and_then(|_| super::run(options, &mut parent));
    #[cfg(not(unix))]
    let result = super::run(options, &mut parent);
    if let (Err(error), Some(out)) = (&result, parent) {
        reply(out, Err(error.to_string()))?;
    }
    result
}

fn open_log() -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use rustix::fs::OFlags;
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags((OFlags::NOFOLLOW | OFlags::NONBLOCK).bits() as i32);
    }
    let file = options.open(LOG)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::other("expected a regular log file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(file)
}

fn launch() -> io::Result<()> {
    let log = open_log().map_err(|e| io::Error::new(e.kind(), format!("open {LOG}: {e}")))?;
    let mut command = Command::new(env::current_exe()?);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    let mut child = command
        .args(env::args_os().skip(1))
        .env(CHILD_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(log)
        .spawn()?;
    let mut line = String::new();
    // Consume one frame. Waiting for EOF would keep the launcher alive with the daemon.
    let result = BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .and_then(|_| {
            serde_json::from_str::<Result<Ready, String>>(&line).map_err(io::Error::other)
        });
    match result {
        Ok(Ok(ready)) => {
            ready.log(child.id());
            Ok(())
        }
        Ok(Err(message)) => {
            child.wait()?;
            Err(io::Error::other(message))
        }
        Err(error) => {
            let _ = child.kill();
            child.wait()?;
            Err(io::Error::other(format!(
                "daemon startup interrupted: {error}; see {LOG}"
            )))
        }
    }
}
