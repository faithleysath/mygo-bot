#[cfg(target_os = "macos")]
mod launchd;
#[cfg(target_os = "linux")]
mod systemd;

use crate::{
    Cli, VERSION,
    install::{self, Scope},
};
use rustix::fs::RenameFlags;
use std::{
    env,
    ffi::OsString,
    io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

pub enum Action {
    InstallBinary,
    UninstallBinary,
    Install,
    Start,
    Stop,
    Status,
    Restart,
    Logs { lines: NonZeroUsize, follow: bool },
    Uninstall,
}

const SERVICE_HELP: &str = "Usage: mygo service [--system] <ACTION> [OPTIONS]

  install    Register automatic startup
  start      Start the service
  stop       Stop the service
  status     Show service state and paths
  restart    Reload the definition and restart
  logs       Show recent log lines
  uninstall  Remove the service definition

  --system   Manage the system service (default: current user)
  -h, --help Print help";

pub fn parse(name: &str, args: impl Iterator<Item = OsString>) -> Result<Cli, String> {
    let mut args = args.peekable();
    let mut scope = Scope::User;
    let service_action = if name == "service" {
        if args.next_if(|arg| arg == "--system").is_some() {
            scope = Scope::System;
        }
        match args.next() {
            None => {
                println!("{SERVICE_HELP}");
                return Ok(Cli::Exit(ExitCode::SUCCESS));
            }
            Some(arg) if arg == "-h" || arg == "--help" => {
                println!("{SERVICE_HELP}");
                return Ok(Cli::Exit(ExitCode::SUCCESS));
            }
            Some(arg) => Some(
                arg.into_string()
                    .map_err(|_| "unexpected non-UTF-8 service action")?,
            ),
        }
    } else {
        None
    };
    let usage = service_action.as_ref().map_or_else(
        || format!("mygo {name}"),
        |action| format!("mygo service {action}"),
    );
    let logs = service_action.as_deref() == Some("logs");
    let mut lines = None;
    let mut follow = false;
    let mut help = false;
    while let Some(arg) = args.next() {
        let arg = arg.to_str().ok_or("unexpected non-UTF-8 argument")?;
        let (flag, inline) = arg
            .split_once('=')
            .map_or((arg, None), |(k, v)| (k, Some(OsString::from(v))));
        match flag {
            "--system" if inline.is_none() => {
                if scope == Scope::System {
                    return Err("duplicate --system".into());
                }
                scope = Scope::System;
            }
            "-h" | "--help" if inline.is_none() => help = true,
            "-f" | "--follow" if logs && inline.is_none() && !follow => follow = true,
            "-n" | "--lines" if logs && lines.is_none() => {
                let value = crate::take_str_value(inline, &mut args, flag)?;
                lines = Some(
                    value
                        .parse()
                        .map_err(|_| "--lines requires a positive integer")?,
                );
            }
            _ => return Err(format!("unexpected argument {arg:?} for {usage}")),
        }
    }
    let action = match service_action.as_deref() {
        None if name == "install" => Action::InstallBinary,
        None => Action::UninstallBinary,
        Some("install") => Action::Install,
        Some("start") => Action::Start,
        Some("stop") => Action::Stop,
        Some("status") => Action::Status,
        Some("restart") => Action::Restart,
        Some("uninstall") => Action::Uninstall,
        Some("logs") => Action::Logs {
            lines: lines.unwrap_or(NonZeroUsize::new(50).unwrap()),
            follow,
        },
        Some(other) => return Err(format!("unknown service action {other:?}")),
    };
    if help {
        if logs {
            println!(
                "Usage: {usage} [--system] [-f|--follow] [-n|--lines <COUNT>]\n\nShow the last COUNT lines (default: 50). Use --follow to stream new entries."
            );
        } else {
            println!(
                "Usage: {usage} [--system]\n\nDefaults to the current user. Pass --system for system scope."
            );
        }
        return Ok(Cli::Exit(ExitCode::SUCCESS));
    }
    Ok(Cli::Manage(scope, action))
}

struct Service {
    scope: Scope,
    binary: PathBuf,
    config: PathBuf,
    data: PathBuf,
    #[cfg(target_os = "macos")]
    log: PathBuf,
}

pub fn is_installed(scope: Scope) -> io::Result<bool> {
    Service::config(scope)?.try_exists()
}

fn path_text(path: &Path) -> io::Result<&str> {
    path.to_str()
        .ok_or_else(|| io::Error::other(format!("service path must be UTF-8: {}", path.display())))
}

fn checked(command: &mut Command) -> io::Result<()> {
    let result = command
        .output()
        .map_err(|e| io::Error::new(e.kind(), format!("{command:?}: {e}")))?;
    if !result.status.success() {
        return Err(io::Error::other(format!(
            "{command:?} ({}): {}",
            result.status,
            String::from_utf8_lossy(&result.stderr).trim()
        )));
    }
    Ok(())
}

fn terminal(command: &mut Command) -> io::Result<ExitCode> {
    let status = command
        .status()
        .map_err(|e| io::Error::new(e.kind(), format!("{command:?}: {e}")))?;
    Ok(ExitCode::from(status.code().unwrap_or(1) as u8))
}

impl Service {
    fn ensure_binary(&self) -> io::Result<()> {
        if !self.binary.try_exists()? {
            // An explicit install may win the race between the probe and publication.
            match install::copy_binary(self.scope, RenameFlags::NOREPLACE) {
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                result => return result,
            }
        }
        if !install::same_file(&env::current_exe()?, &self.binary)? {
            let output = Command::new(&self.binary).arg("--version").output()?;
            let installed = String::from_utf8_lossy(&output.stdout);
            if !output.status.success() || installed.trim() != format!("mygo {VERSION}") {
                eprintln!(
                    "warning: keeping installed executable {} ({:?}); current build is mygo {VERSION}. To update, run `{}`",
                    self.binary.display(),
                    installed.trim(),
                    self.scope.command("install")
                );
            }
        }
        Ok(())
    }
}

pub fn run(scope: Scope, action: Action) -> io::Result<ExitCode> {
    match action {
        Action::InstallBinary => return install::install(scope).map(|()| ExitCode::SUCCESS),
        Action::UninstallBinary => return install::uninstall(scope).map(|()| ExitCode::SUCCESS),
        Action::Status | Action::Logs { .. } => (),
        _ => scope.check()?,
    }
    let svc = Service::new(scope)?;
    match action {
        Action::Install => {
            #[cfg(target_os = "linux")]
            svc.check_manager()?;
            svc.ensure_binary()?;
            install::private_dir(&svc.data)?;
            svc.register()?;
            println!("installed service definition at {}", svc.config.display());
            if svc.running()? {
                println!(
                    "run `{}` to apply the new definition",
                    scope.command("service restart")
                );
            }
        }
        Action::Start | Action::Restart if !svc.config.try_exists()? => {
            return Err(io::Error::other(format!(
                "service is not installed; run `{}`",
                scope.command("service install")
            )));
        }
        Action::Start => svc.start()?,
        Action::Restart => svc.restart()?,
        Action::Status if !svc.config.try_exists()? => {
            println!("service is not installed ({})", svc.config.display());
            return Ok(ExitCode::from(3));
        }
        Action::Status => {
            println!(
                "Config: {}\nExecutable: {}\nData: {}\nLogs: {}",
                svc.config.display(),
                svc.binary.display(),
                svc.data.display(),
                svc.log_source()
            );
            return svc.status();
        }
        Action::Logs { lines, follow } => return svc.logs(lines, follow),
        Action::Stop => svc.stop()?,
        Action::Uninstall => svc.unregister()?,
        Action::InstallBinary | Action::UninstallBinary => unreachable!(),
    }
    Ok(ExitCode::SUCCESS)
}
