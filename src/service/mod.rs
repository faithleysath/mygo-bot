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
    fs, io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

pub enum Action {
    InstallBinary,
    Install,
    Start,
    Stop,
    Status,
    Restart,
    Logs { lines: NonZeroUsize, follow: bool },
    Uninstall { remove_binary: bool },
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
        None => Action::Uninstall {
            remove_binary: true,
        },
        Some("install") => Action::Install,
        Some("start") => Action::Start,
        Some("stop") => Action::Stop,
        Some("status") => Action::Status,
        Some("restart") => Action::Restart,
        Some("uninstall") => Action::Uninstall {
            remove_binary: false,
        },
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
        } else if name == "uninstall" {
            println!(
                "Usage: {usage} [--system]\n\nStop and unregister the service, then remove the executable. Data and logs are kept.\nDefaults to the current user. Pass --system for system scope."
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
    fn remove_definition(&self) -> io::Result<bool> {
        match fs::remove_file(&self.config) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn ensure_binary(&self) -> io::Result<()> {
        let entry = match fs::symlink_metadata(&self.binary) {
            Ok(entry) => entry,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                match install::copy_binary(self.scope, RenameFlags::NOREPLACE) {
                    // Inspect whatever appeared during the copy just like an existing entry.
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                        fs::symlink_metadata(&self.binary)?
                    }
                    result => return result,
                }
            }
            Err(e) => return Err(e),
        };
        let path = self.binary.display();
        let reinstall = self.scope.command("install");
        if entry.is_symlink() && !self.binary.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{path} is a dangling symlink. Run `{reinstall}` to replace it"),
            ));
        }
        if install::same_file(&env::current_exe()?, &self.binary)? {
            return Ok(());
        }
        let reason = match Command::new(&self.binary).arg("--version").output() {
            Ok(output) if output.status.success() => {
                let installed = String::from_utf8_lossy(&output.stdout);
                if installed.trim() == format!("mygo {VERSION}") {
                    return Ok(());
                }
                format!(
                    "reported {:?}, current build is mygo {VERSION}",
                    installed.trim()
                )
            }
            Ok(output) => format!("--version returned {}", output.status),
            Err(e) => format!("could not run --version: {e}"),
        };
        eprintln!(
            "warning: keeping installed executable {path} ({reason}). To replace it, run `{reinstall}`"
        );
        Ok(())
    }
}

pub fn run(scope: Scope, action: Action) -> io::Result<ExitCode> {
    match action {
        Action::InstallBinary => return install::install(scope).map(|()| ExitCode::SUCCESS),
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
        Action::Start | Action::Restart => {
            if !svc.config.try_exists()? {
                return Err(io::Error::other(format!(
                    "service is not installed; run `{}`",
                    scope.command("service install")
                )));
            }
            install::private_dir(&svc.data)?;
            if matches!(action, Action::Start) {
                svc.start()?;
            } else {
                svc.restart()?;
            }
        }
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
        Action::Uninstall { remove_binary } => {
            // A standalone executable can be installed on Linux without a systemd manager.
            #[cfg(target_os = "linux")]
            if remove_binary && !svc.config.try_exists()? && svc.check_manager().is_err() {
                install::remove_binary(&svc.binary)?;
                return Ok(ExitCode::SUCCESS);
            }
            if svc.unregister()? {
                println!("uninstalled service definition at {}", svc.config.display());
            } else if !remove_binary {
                println!("service is not installed ({})", svc.config.display());
            }
            if remove_binary {
                install::remove_binary(&svc.binary)?;
            }
        }
        Action::InstallBinary => unreachable!(),
    }
    Ok(ExitCode::SUCCESS)
}
