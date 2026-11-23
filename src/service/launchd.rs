use super::{Scope, Service, checked, path_text, terminal};
use crate::{
    daemon::open_log,
    install::{atomic_write, home, private_dir},
};
use rustix::{fs::RenameFlags, process::geteuid};
use std::{
    io::{self, Write},
    num::NonZeroUsize,
    path::PathBuf,
    process::{Command, ExitCode, Output},
    thread,
    time::{Duration, Instant},
};

const LABEL: &str = "io.mygo.daemon";
const EXIT_TIMEOUT: Duration = Duration::from_secs(20);

fn launchctl() -> Command {
    Command::new("/bin/launchctl")
}

fn xml(value: &str) -> io::Result<String> {
    if value.chars().any(|c| matches!(c, '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{fffe}' | '\u{ffff}')) {
        return Err(io::Error::other("service path contains an invalid XML character"));
    }
    Ok(value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
        .replace('\r', "&#13;"))
}

fn missing(output: &Output) -> bool {
    matches!(output.status.code(), Some(3 | 113))
}
fn failure(output: Output) -> io::Error {
    let kind = match output.status.code() {
        Some(1 | 13) => io::ErrorKind::PermissionDenied,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(
        kind,
        format!(
            "launchctl ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    )
}

struct Job {
    state: String,
    pid: Option<String>,
}

impl Service {
    pub(super) fn config(scope: Scope) -> io::Result<PathBuf> {
        Ok(match scope {
            Scope::User => home()?.join("Library/LaunchAgents"),
            Scope::System => PathBuf::from("/Library/LaunchDaemons"),
        }
        .join("io.mygo.daemon.plist"))
    }
    pub(super) fn new(scope: Scope) -> io::Result<Self> {
        let (data, log) = match scope {
            Scope::User => {
                let home = home()?;
                (
                    home.join("Library/Application Support/mygo"),
                    home.join("Library/Logs/mygo/mygo.log"),
                )
            }
            Scope::System => (
                "/Library/Application Support/mygo".into(),
                "/var/log/mygo/mygo.log".into(),
            ),
        };
        Ok(Self {
            scope,
            binary: scope.binary()?,
            config: Self::config(scope)?,
            data,
            log,
        })
    }
    fn domain(&self) -> String {
        match self.scope {
            Scope::User => format!("gui/{}", geteuid().as_raw()),
            Scope::System => "system".into(),
        }
    }
    fn target(&self) -> String {
        format!("{}/{LABEL}", self.domain())
    }
    fn prepare_log(&self) -> io::Result<()> {
        private_dir(self.log.parent().unwrap())?;
        open_log(&self.log)?;
        Ok(())
    }
    fn job(&self) -> io::Result<Option<Job>> {
        let output = launchctl().arg("print").arg(self.target()).output()?;
        if missing(&output) {
            return Ok(None);
        }
        if !output.status.success() {
            return Err(failure(output));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let field = |key: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(key))
                .map(str::to_owned)
        };
        Ok(Some(Job {
            state: field("\tstate = ").unwrap_or_else(|| "loaded".into()),
            pid: field("\tpid = "),
        }))
    }
    pub(super) fn running(&self) -> io::Result<bool> {
        Ok(self.job()?.is_some_and(|job| job.state == "running"))
    }

    pub(super) fn register(&self) -> io::Result<()> {
        self.prepare_log()?;
        let binary = xml(path_text(&self.binary)?)?;
        let data = xml(path_text(&self.data)?)?;
        let log = xml(path_text(&self.log)?)?;
        let path = match self.scope {
            Scope::User => format!(
                "{}/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
                xml(path_text(&home()?)?)?
            ),
            Scope::System => {
                "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin".into()
            }
        };
        atomic_write(&self.config, 0o644, RenameFlags::empty(), |file| {
            write!(
                file,
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key><array><string>{binary}</string><string>daemon</string><string>--foreground</string></array>
  <key>WorkingDirectory</key><string>{data}</string>
  <key>RunAtLoad</key><true/>
  <key>ExitTimeOut</key><integer>{exit_timeout}</integer>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
  <key>EnvironmentVariables</key><dict><key>PATH</key><string>{path}</string></dict>
</dict>
</plist>
"#,
                exit_timeout = EXIT_TIMEOUT.as_secs(),
            )
        })?;
        checked(launchctl().arg("enable").arg(self.target()))
    }
    fn bootstrap(&self) -> io::Result<()> {
        checked(
            launchctl()
                .arg("bootstrap")
                .arg(self.domain())
                .arg(&self.config),
        )
    }
    pub(super) fn start(&self) -> io::Result<()> {
        self.prepare_log()?;
        checked(launchctl().arg("enable").arg(self.target()))?;
        if self.job()?.is_some() {
            checked(launchctl().arg("kickstart").arg(self.target()))
        } else {
            self.bootstrap()
        }
    }
    fn bootout(&self) -> io::Result<bool> {
        if self.job()?.is_none() {
            return Ok(false);
        }
        let result = launchctl().arg("bootout").arg(self.target()).output()?;
        if !result.status.success() && !missing(&result) {
            return Err(failure(result));
        }
        let deadline = Instant::now() + EXIT_TIMEOUT;
        while self.job()?.is_some() {
            if Instant::now() >= deadline {
                return Err(io::Error::other("timed out waiting for launchctl bootout"));
            }
            thread::sleep(Duration::from_millis(100));
        }
        Ok(true)
    }
    pub(super) fn stop(&self) -> io::Result<()> {
        if !self.bootout()? {
            println!("service is already stopped");
        }
        Ok(())
    }
    pub(super) fn restart(&self) -> io::Result<()> {
        self.prepare_log()?;
        self.bootout()?;
        checked(launchctl().arg("enable").arg(self.target()))?;
        self.bootstrap()
    }
    pub(super) fn status(&self) -> io::Result<ExitCode> {
        let Some(job) = self.job()? else {
            println!("State: stopped");
            return Ok(ExitCode::from(3));
        };
        println!("State: {}", job.state);
        if let Some(pid) = job.pid {
            println!("PID: {pid}");
        }
        Ok(if job.state == "running" {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(3)
        })
    }
    pub(super) fn log_source(&self) -> String {
        self.log.display().to_string()
    }
    pub(super) fn logs(&self, lines: NonZeroUsize, follow: bool) -> io::Result<ExitCode> {
        if !self.log.try_exists()? {
            println!("no log at {} yet", self.log.display());
            return Ok(ExitCode::SUCCESS);
        }
        let mut cmd = Command::new("/usr/bin/tail");
        cmd.arg("-n").arg(lines.to_string());
        if follow {
            cmd.arg("-f");
        }
        terminal(cmd.arg("--").arg(&self.log))
    }
    pub(super) fn unregister(&self) -> io::Result<bool> {
        self.bootout()?;
        self.remove_definition()
    }
}
