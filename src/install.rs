use crate::{BIN_NAME, VERSION, service};
use rustix::{
    fs::{CWD, OFlags, RenameFlags, renameat_with},
    process::geteuid,
};
use std::{
    env,
    fs::{self, File, OpenOptions, Permissions},
    io,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    User,
    System,
}

pub fn home() -> io::Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .ok_or_else(|| io::Error::other("HOME must be an absolute path"))
}

impl Scope {
    pub fn binary(self) -> io::Result<PathBuf> {
        Ok(match self {
            Self::User => home()?.join(".local/bin"),
            Self::System => PathBuf::from("/usr/local/bin"),
        }
        .join(BIN_NAME))
    }

    pub fn check(self) -> io::Result<()> {
        let root = geteuid().is_root();
        match self {
            Self::User if root && env::var_os("SUDO_USER").is_some() => Err(io::Error::other(
                "refusing user-level operation under sudo; omit sudo for user scope or pass --system for system scope",
            )),
            Self::System if !root => Err(io::Error::other("use sudo for system-level changes")),
            _ => Ok(()),
        }
    }

    pub fn command(self, action: &str) -> String {
        match self {
            Self::User => format!("mygo {action}"),
            Self::System => format!("sudo mygo {action} --system"),
        }
    }
}

pub fn same_file(src: &Path, dest: &Path) -> io::Result<bool> {
    let a = fs::metadata(src)?;
    match fs::metadata(dest) {
        Ok(b) => Ok((a.dev(), a.ino()) == (b.dev(), b.ino())),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub fn private_dir(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    let dir = File::options()
        .read(true)
        .custom_flags((OFlags::DIRECTORY | OFlags::NOFOLLOW).bits() as i32)
        .open(path)?;
    dir.set_permissions(Permissions::from_mode(0o700))
}

/// Publish a completed file in one rename, including when the old executable is running.
pub fn atomic_write(
    dest: &Path,
    mode: u32,
    flags: RenameFlags,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let parent = dest.parent().unwrap();
    fs::create_dir_all(parent)?;
    let mut attempt = 0;
    let (tmp, mut file) = loop {
        let tmp = parent.join(format!(
            ".mygo.install.{}.{attempt}.tmp",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => attempt += 1,
            Err(err) => return Err(err),
        }
    };
    let result = (|| {
        write(&mut file)?;
        file.set_permissions(Permissions::from_mode(mode))?;
        file.sync_all()?;
        drop(file);
        if flags.is_empty() {
            fs::rename(&tmp, dest)
        } else {
            Ok(renameat_with(CWD, &tmp, CWD, dest, flags)?)
        }
    })();
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}

pub fn install(scope: Scope) -> io::Result<()> {
    copy_binary(scope, RenameFlags::empty())
}

pub fn copy_binary(scope: Scope, flags: RenameFlags) -> io::Result<()> {
    scope.check()?;
    let src = fs::canonicalize(env::current_exe()?)?;
    let dest = scope.binary()?;
    if same_file(&src, &dest)? {
        println!("mygo {VERSION} is already installed at {}", dest.display());
        return Ok(());
    }
    let bin_dir = dest.parent().unwrap();
    if !bin_dir.try_exists()? {
        fs::create_dir_all(bin_dir)?;
        fs::set_permissions(bin_dir, Permissions::from_mode(0o755))?;
    }
    atomic_write(&dest, 0o755, flags, |file| {
        io::copy(&mut File::open(src)?, file)?;
        Ok(())
    })?;
    println!("installed mygo {VERSION} to {}", dest.display());
    if scope == Scope::User
        && !env::split_paths(&env::var_os("PATH").unwrap_or_default()).any(|p| p == bin_dir)
    {
        println!("note: add {} to PATH", bin_dir.display());
    }
    if service::is_installed(scope)? {
        println!(
            "run `{}` to use the new executable",
            scope.command("service restart")
        );
    }
    Ok(())
}

pub(super) fn remove_binary(dest: &Path) -> io::Result<()> {
    match fs::remove_file(dest) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            println!("mygo is not installed at {}", dest.display())
        }
        Ok(()) => println!("uninstalled {}", dest.display()),
        Err(err) => return Err(err),
    }
    Ok(())
}
