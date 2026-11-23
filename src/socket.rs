use rustix::{
    fs::Mode,
    process::{geteuid, umask},
};
use std::{
    env, fs, io,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
};
use tokio::net::UnixListener;

fn runtime_dir(uid: u32) -> io::Result<PathBuf> {
    #[cfg(not(target_os = "macos"))]
    if let Some(dir) = env::var_os("XDG_RUNTIME_DIR").filter(|dir| !dir.is_empty()) {
        return Ok(PathBuf::from(dir));
    }
    let dir = env::temp_dir().join(format!("mygo-bot-{uid}"));
    match fs::DirBuilder::new().mode(0o700).create(&dir) {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
        result => result?,
    }
    Ok(dir)
}

fn default_path() -> io::Result<PathBuf> {
    let uid = geteuid().as_raw();
    let dir = runtime_dir(uid)?;
    let meta = fs::symlink_metadata(&dir)?;
    if !dir.is_absolute() || !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a private runtime directory owned by the current user",
                dir.display()
            ),
        ));
    }
    Ok(dir.join("mygo-bot.sock"))
}

pub struct SocketFile {
    pub path: PathBuf,
    metadata: fs::Metadata,
}

impl SocketFile {
    pub fn bind(path: Option<&Path>) -> io::Result<(UnixListener, Self)> {
        let path = match path {
            Some(path) => path.to_path_buf(),
            None => default_path()?,
        };
        // Bind during single-threaded startup so the socket is private from creation.
        let previous = umask(Mode::from_raw_mode(0o177));
        let bound = UnixListener::bind(&path);
        umask(previous);
        let listener =
            bound.map_err(|e| io::Error::new(e.kind(), format!("bind {}: {e}", path.display())))?;
        let metadata = fs::symlink_metadata(&path)?;
        Ok((listener, Self { path, metadata }))
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        if let Ok(current) = fs::symlink_metadata(&self.path)
            && current.dev() == self.metadata.dev()
            && current.ino() == self.metadata.ino()
            && let Err(error) = fs::remove_file(&self.path)
        {
            tracing::warn!(path = %self.path.display(), %error, "could not remove Unix socket");
        }
    }
}
