use super::{Scope, Service, checked, path_text, terminal};
use crate::install::{atomic_write, home};
use rustix::{fs::RenameFlags, process::geteuid};
use std::{
    env, fs,
    io::{self, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

const UNIT: &str = "mygo.service";

fn xdg(key: &str, default: &str) -> io::Result<PathBuf> {
    env::var_os(key)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .map_or_else(|| Ok(home()?.join(default)), Ok)
}

fn unit_path(path: &Path) -> io::Result<String> {
    let text = path_text(path)?;
    if text.chars().any(char::is_control) {
        return Err(io::Error::other("control character in systemd unit path"));
    }
    Ok(text.replace('%', "%%"))
}

impl Service {
    pub(super) fn config(scope: Scope) -> io::Result<PathBuf> {
        Ok(match scope {
            Scope::User => xdg("XDG_CONFIG_HOME", ".config")?.join("systemd/user"),
            Scope::System => PathBuf::from("/etc/systemd/system"),
        }
        .join(UNIT))
    }
    pub(super) fn new(scope: Scope) -> io::Result<Self> {
        let data = match scope {
            Scope::User => xdg("XDG_DATA_HOME", ".local/share")?.join("mygo"),
            Scope::System => "/var/lib/mygo".into(),
        };
        Ok(Self {
            scope,
            binary: scope.binary()?,
            config: Self::config(scope)?,
            data,
        })
    }
    fn command(&self, name: &str) -> Command {
        let mut command = Command::new(name);
        if self.scope == Scope::User {
            command.arg("--user");
        }
        command
    }
    fn systemctl(&self, args: &[&str]) -> io::Result<()> {
        checked(self.command("systemctl").args(args))
            .map_err(|err| io::Error::new(err.kind(), format!("systemd service manager: {err}")))
    }
    pub(super) fn check_manager(&self) -> io::Result<()> {
        self.systemctl(&["show", "-p", "Version", "--value"])
    }
    fn property(&self, name: &str) -> io::Result<String> {
        let out = self
            .command("systemctl")
            .args(["show", UNIT, "-p", name, "--value"])
            .output()
            .map_err(|e| io::Error::new(e.kind(), format!("systemctl: {e}")))?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "systemctl show: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    }
    pub(super) fn running(&self) -> io::Result<bool> {
        Ok(self.property("ActiveState")? == "active")
    }
    pub(super) fn register(&self) -> io::Result<()> {
        let (network, wanted) = match self.scope {
            Scope::User => ("", "default.target"),
            Scope::System => (
                "After=network-online.target\nWants=network-online.target\n",
                "multi-user.target",
            ),
        };
        // The executable path is literal, but argv undergoes environment expansion.
        // The ':' prefix keeps both identical when the path contains a dollar sign.
        let path = unit_path(&self.binary)?;
        let prefix = if path.contains('$') { ":" } else { "" };
        let binary = path.replace('\\', "\\\\").replace('"', "\\\"");
        let data = unit_path(&self.data)?;
        atomic_write(&self.config, 0o644, RenameFlags::empty(), |file| {
            write!(
                file,
                "[Unit]
Description=MyGo-Bot Daemon
{network}
[Service]
Type=simple
ExecStart=\"{prefix}{binary}\" daemon --foreground
WorkingDirectory={data}
Restart=on-failure
RestartSec=3
TimeoutStopSec=15
UMask=0077

[Install]
WantedBy={wanted}
"
            )
        })?;
        self.systemctl(&["daemon-reload"])?;
        self.systemctl(&["enable", UNIT])?;
        if self.scope == Scope::User {
            let uid = geteuid().as_raw().to_string();
            if Command::new("loginctl")
                .args(["show-user", &uid, "-p", "Linger", "--value"])
                .output()
                .is_ok_and(|out| out.status.success() && out.stdout.trim_ascii() == b"no")
            {
                println!(
                    "note: user lingering is disabled. For startup before login, run `sudo loginctl enable-linger {uid}`"
                );
            }
        }
        Ok(())
    }
    pub(super) fn start(&self) -> io::Result<()> {
        self.systemctl(&["start", UNIT])
    }
    pub(super) fn restart(&self) -> io::Result<()> {
        self.systemctl(&["daemon-reload"])?;
        self.systemctl(&["restart", UNIT])
    }
    pub(super) fn stop(&self) -> io::Result<()> {
        if self.property("ActiveState")? == "inactive" {
            println!("service is already stopped");
            return Ok(());
        }
        self.systemctl(&["stop", UNIT])
    }
    pub(super) fn status(&self) -> io::Result<ExitCode> {
        terminal(
            self.command("systemctl")
                .args(["status", "--no-pager", UNIT]),
        )
    }
    pub(super) fn log_source(&self) -> String {
        let scope = if self.scope == Scope::User {
            " --user"
        } else {
            ""
        };
        format!("journalctl{scope} -u {UNIT}")
    }
    pub(super) fn logs(&self, lines: NonZeroUsize, follow: bool) -> io::Result<ExitCode> {
        let mut cmd = self.command("journalctl");
        cmd.args(["-u", UNIT, "-n", &lines.to_string(), "--no-pager"]);
        if follow {
            cmd.arg("-f");
        }
        terminal(&mut cmd)
    }
    pub(super) fn unregister(&self) -> io::Result<()> {
        if self.config.try_exists()? {
            self.systemctl(&["disable", "--now", UNIT])?;
            match fs::remove_file(&self.config) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                result => result?,
            }
        } else if self.property("LoadState")? != "not-found" {
            self.stop()?;
        }
        self.systemctl(&["daemon-reload"])
    }
}
