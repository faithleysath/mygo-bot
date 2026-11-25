use std::{
    borrow::Cow,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
    time::UNIX_EPOCH,
};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    Host, HostError, Reply,
    io::BODY_LIMIT,
    streams::{CHUNK, Resource},
};

pub(super) async fn execute(
    host: &Host,
    op: &str,
    args: &Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, HostError> {
    check_cancel(&host.cancel)?;
    let home = host.app.codemode.home_dir.as_deref();
    if op == "fs.rename" {
        let from = path_arg(args, "from")?;
        let to = path_arg(args, "to")?;
        tokio::fs::rename(resolve_path(home, from)?, resolve_path(home, to)?)
            .await
            .map_err(|e| {
                HostError::from(e).context(format!("{op} {} to {}", from.display(), to.display()))
            })?;
        return Ok(Reply::value(Value::Null));
    }
    let path = path_arg(args, "path")?;
    let resolved = resolve_path(home, path)?;
    let at_path = |e: io::Error| HostError::from(e).context(format!("{op} {}", path.display()));
    let value = match op {
        "fs.read" | "fs.write" => {
            let stream = flag(args, "stream", false)?;
            let access = if op == "fs.read" {
                if let Some(encoding) = args.get("encoding")
                    && (stream || encoding.as_str() != Some("utf8"))
                {
                    return Err("encoding requires a buffered utf8 read".into());
                }
                Access::Read
            } else if flag(args, "append", false)? {
                Access::Append
            } else {
                Access::Overwrite
            };
            if !matches!(access, Access::Read) && !stream && data.is_none() {
                return Err("fs.write requires data".into());
            }
            if data.is_some() && (stream || matches!(access, Access::Read)) {
                return Err(format!("{op} does not take data in this mode").into());
            }
            if data.as_ref().is_some_and(|bytes| bytes.len() > BODY_LIMIT) {
                return Err(
                    io::Error::new(io::ErrorKind::FileTooLarge, "input exceeds 8 MiB").into(),
                );
            }
            let owned_path = resolved.into_owned();
            let host = host.clone();
            return tokio::task::spawn_blocking(move || {
                let reservation = stream.then(|| host.streams.reserve()).transpose()?;
                let mut file = open_regular(&owned_path, access, &host.cancel)?;
                if let Some(reservation) = reservation {
                    let file = tokio::fs::File::from_std(file);
                    let resource = match access {
                        Access::Read => Resource::Reader(Box::pin(file)),
                        _ => Resource::Writer(file),
                    };
                    return Ok(Reply::value(json!(reservation.insert(resource))));
                }
                if let Some(bytes) = data {
                    for chunk in bytes.chunks(CHUNK) {
                        check_cancel(&host.cancel)?;
                        file.write_all(chunk)?;
                    }
                    return Ok(Reply::value(json!(bytes.len())));
                }
                Ok(Reply {
                    value: Value::Null,
                    data: Some(read_buffered(file, &host.cancel)?),
                })
            })
            .await
            .map_err(|e| format!("{op}: {e}"))?
            .map_err(|e: HostError| e.context(format!("{op} {}", path.display())));
        }
        "fs.readDir" => {
            let reservation = host.streams.reserve()?;
            let dir = tokio::fs::read_dir(resolved).await.map_err(at_path)?;
            check_cancel(&host.cancel)?;
            json!(reservation.insert(Resource::Directory(dir)))
        }
        "fs.stat" => {
            let metadata = if flag(args, "followSymlinks", true)? {
                tokio::fs::metadata(resolved).await
            } else {
                tokio::fs::symlink_metadata(resolved).await
            }
            .map_err(at_path)?;
            let modified = metadata.modified().map_err(at_path)?;
            let mtime_ms = match modified.duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs_f64() * 1000.0,
                Err(e) => -e.duration().as_secs_f64() * 1000.0,
            };
            json!({"type": file_type(metadata.file_type()), "size": metadata.len(), "mtimeMs": mtime_ms})
        }
        "fs.mkdir" => {
            if flag(args, "recursive", false)? {
                tokio::fs::create_dir_all(resolved).await
            } else {
                tokio::fs::create_dir(resolved).await
            }
            .map_err(at_path)?;
            Value::Null
        }
        "fs.remove" => {
            let recursive = flag(args, "recursive", false)?;
            remove(&resolved, recursive, &host.cancel)
                .await
                .map_err(at_path)?;
            Value::Null
        }
        _ => return Err(format!("unknown file operation {op}").into()),
    };
    Ok(Reply::value(value))
}

fn path_arg<'a>(args: &'a Value, key: &str) -> Result<&'a Path, HostError> {
    let path = args[key]
        .as_str()
        .filter(|s| !s.is_empty() && !s.contains('\0'))
        .ok_or_else(|| format!("{key} must be a nonempty path without NUL"))?;
    Ok(Path::new(path))
}

fn resolve_path<'a>(home: Option<&Path>, path: &'a Path) -> Result<Cow<'a, Path>, HostError> {
    if path.is_absolute() {
        return Ok(Cow::Borrowed(path));
    }
    let home =
        home.ok_or("service user home directory is unavailable; specify an absolute path")?;
    #[cfg(windows)]
    let path = {
        use std::path::{Component, Prefix};
        let mut parts = path.components();
        if let Some(Component::Prefix(prefix)) = parts.next() {
            // C:file names a path relative to the current directory on C:.
            let same_drive = matches!(home.components().next(), Some(Component::Prefix(base))
                if matches!(base.kind(), Prefix::Disk(d) | Prefix::VerbatimDisk(d)
                    if prefix.kind() == Prefix::Disk(d)));
            if !same_drive {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    "drive-relative path is on a different drive from home; specify an absolute path").into());
            }
            parts.as_path()
        } else {
            path
        }
    };
    Ok(Cow::Owned(home.join(path)))
}

fn flag(args: &Value, key: &str, default: bool) -> Result<bool, HostError> {
    match args.get(key) {
        Some(v) => v
            .as_bool()
            .ok_or_else(|| format!("{key} must be a boolean").into()),
        None => Ok(default),
    }
}

pub(super) fn file_type(ty: fs::FileType) -> &'static str {
    if ty.is_file() {
        "file"
    } else if ty.is_dir() {
        "directory"
    } else if ty.is_symlink() {
        "symlink"
    } else {
        "other"
    }
}

async fn remove(path: &Path, recursive: bool, cancel: &CancellationToken) -> io::Result<()> {
    // Strip a final separator or `/.` so that lstat examines the link itself.
    let leaf = path.components().as_path();
    let ty = tokio::fs::symlink_metadata(leaf).await?.file_type();
    check_cancel(cancel)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileTypeExt;
        if ty.is_symlink_dir() {
            return tokio::fs::remove_dir(leaf).await;
        }
    }
    if ty.is_symlink() {
        tokio::fs::remove_file(leaf).await
    } else if ty.is_dir() {
        if recursive {
            tokio::fs::remove_dir_all(leaf).await
        } else {
            tokio::fs::remove_dir(leaf).await
        }
    } else {
        tokio::fs::remove_file(path).await
    }
}

fn check_cancel(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Access {
    Read,
    Overwrite,
    Append,
}

fn open_regular(path: &Path, access: Access, cancel: &CancellationToken) -> io::Result<File> {
    check_cancel(cancel)?;
    let special = || io::Error::new(io::ErrorKind::Unsupported, "expected a regular file");
    #[cfg(windows)]
    if is_device_path(path) {
        return Err(special());
    }
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_file() => return Err(special()),
        Err(e) if e.kind() != io::ErrorKind::NotFound || matches!(access, Access::Read) => {
            return Err(e);
        }
        _ => (),
    }

    let mut options = OpenOptions::new();
    match access {
        Access::Read => options.read(true),
        Access::Overwrite => options.write(true).create(true),
        Access::Append => options.append(true).create(true),
    };
    #[cfg(unix)]
    {
        use rustix::fs::OFlags;
        use std::os::unix::fs::OpenOptionsExt;
        // A FIFO or terminal can replace the path between metadata and open.
        options.custom_flags((OFlags::NONBLOCK | OFlags::NOCTTY).bits() as _);
    }
    check_cancel(cancel)?;
    let file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        // SAFETY: the file owns the handle throughout this call.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(special());
        }
    }
    if !file.metadata()?.is_file() {
        return Err(special());
    }
    check_cancel(cancel)?;
    if matches!(access, Access::Overwrite) {
        file.set_len(0)?;
    }
    Ok(file)
}

fn read_buffered(mut file: File, cancel: &CancellationToken) -> io::Result<Vec<u8>> {
    let too_large = || io::Error::new(io::ErrorKind::FileTooLarge, "file exceeds 8 MiB");
    let size = file.metadata()?.len();
    if size > BODY_LIMIT as u64 {
        return Err(too_large());
    }
    let mut data = Vec::with_capacity(size as usize);
    let mut chunk = [0; CHUNK];
    loop {
        check_cancel(cancel)?;
        let n = match file.read(&mut chunk) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            n => n?,
        };
        if n == 0 {
            return Ok(data);
        }
        if n > BODY_LIMIT - data.len() {
            return Err(too_large());
        }
        data.extend_from_slice(&chunk[..n]);
    }
}

#[cfg(windows)]
fn is_device_path(path: &Path) -> bool {
    use std::path::{Component, Prefix};
    match path.components().next() {
        Some(Component::Prefix(p)) => match p.kind() {
            Prefix::DeviceNS(_) | Prefix::Verbatim(_) => true,
            Prefix::UNC(_, share) | Prefix::VerbatimUNC(_, share) => {
                share.eq_ignore_ascii_case("pipe") || share.eq_ignore_ascii_case("mailslot")
            }
            _ => false,
        },
        _ => false,
    }
}
