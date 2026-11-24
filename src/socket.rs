use rustix::{
    fs::Mode,
    process::{geteuid, umask},
};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::io::{Read, Write};
use std::{
    env, fs, io,
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::{os::unix::net::UnixStream, time::Duration};
use tokio::net::UnixListener;

pub const RUNTIME_PATH: &str = "/_runtime";

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RuntimeInfo {
    pub pid: u32,
    pub listen: SocketAddr,
    pub token: String,
}

pub fn runtime_route(listen: SocketAddr, token: Arc<str>) -> axum::Router {
    axum::Router::new().route(
        RUNTIME_PATH,
        axum::routing::get(move || {
            let token = token.clone();
            async move {
                (
                    [(axum::http::header::CACHE_CONTROL, "no-store")],
                    axum::Json(RuntimeInfo {
                        pid: std::process::id(),
                        listen,
                        token: token.to_string(),
                    }),
                )
            }
        }),
    )
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub fn query_runtime(path: &Path) -> io::Result<Option<RuntimeInfo>> {
    let mut stream = UnixStream::connect(path)?;
    let timeout = Some(Duration::from_secs(1));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;
    write!(
        stream,
        "GET {RUNTIME_PATH} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut buf = Vec::new();
    (&stream).take(8192).read_to_end(&mut buf)?;
    let Some(split) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed HTTP response",
        ));
    };
    let head = std::str::from_utf8(&buf[..split])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let status = head.lines().next().unwrap_or_default();
    if !(status.starts_with("HTTP/1.0 200") || status.starts_with("HTTP/1.1 200")) {
        return Ok(None);
    }
    let info: RuntimeInfo = serde_json::from_slice(&buf[split + 4..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(info))
}

fn runtime_dir(uid: u32) -> io::Result<PathBuf> {
    let dir = env::temp_dir().join(format!("mygo-{uid}"));
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
    Ok(dir.join("mygo.sock"))
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
