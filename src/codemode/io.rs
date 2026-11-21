use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::{Method, Url};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::sync::CancellationToken;

use super::{Host, Reply};

const BODY_LIMIT: usize = 8 * 1024 * 1024;
const FILE_CHUNK: usize = 64 * 1024;
const OUTPUT_LIMIT: usize = 256 * 1024;
const DRAIN_GRACE: Duration = Duration::from_millis(250);

pub async fn execute(
    host: &Host,
    op: &str,
    args: Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, String> {
    if host.cancel.is_cancelled() {
        return Err("cancelled".into());
    }
    if !args.is_object() {
        return Err(format!("{op} expects an options object"));
    }
    if data.as_ref().is_some_and(|bytes| bytes.len() > BODY_LIMIT) {
        return Err(format!("{op}: input exceeds {BODY_LIMIT} bytes"));
    }
    let value = match op {
        "fs.read" | "fs.write" => {
            let path = string(&args, "path")?;
            let append = match args.get("append") {
                Some(value) => value.as_bool().ok_or("append must be a boolean")?,
                None => false,
            };
            let write = if op == "fs.write" {
                Some(data.ok_or("fs.write requires data")?)
            } else {
                if let Some(encoding) = args.get("encoding")
                    && encoding.as_str() != Some("utf8")
                {
                    return Err("fs.read supports utf8 encoding".into());
                }
                None
            };
            let owned_path = PathBuf::from(path);
            let cancel = host.cancel.clone();
            return tokio::task::spawn_blocking(move || {
                file_io(&owned_path, write, append, &cancel)
            })
            .await
            .map_err(|e| format!("{op}: {e}"))?
            .map_err(|e| format!("{op} {path}: {e}"));
        }
        "http.request" => {
            tokio::select! {
                biased;
                _ = host.cancel.cancelled() => return Err("cancelled".into()),
                reply = request(&host.client, &args, data) => return reply,
            }
        }
        "proc.exec" | "bash" => command(host, op, &args, data).await?,
        "sleep" => {
            let ms = args["ms"]
                .as_u64()
                .ok_or("ms must be a nonnegative integer")?;
            tokio::select! {
                biased;
                _ = host.cancel.cancelled() => return Err("cancelled".into()),
                _ = tokio::time::sleep(Duration::from_millis(ms)) => Value::Null,
            }
        }
        _ => return Err(format!("unknown host operation {op}")),
    };
    Ok(Reply { value, data: None })
}

fn string<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args[key]
        .as_str()
        .ok_or_else(|| format!("{key} must be a string"))
}

pub(super) fn timeout(args: &Value) -> Result<Option<Duration>, String> {
    args.get("timeout_ms")
        .map(|v| {
            v.as_u64()
                .filter(|ms| *ms > 0 && *ms <= 9_007_199_254_740_991)
                .map(Duration::from_millis)
                .ok_or_else(|| "timeout_ms must be a positive safe integer".to_owned())
        })
        .transpose()
}

fn check_cancel(cancel: &CancellationToken) -> io::Result<()> {
    if cancel.is_cancelled() {
        return Err(io::Error::other("cancelled"));
    }
    Ok(())
}

fn file_io(
    path: &Path,
    write: Option<Vec<u8>>,
    append: bool,
    cancel: &CancellationToken,
) -> io::Result<Reply> {
    check_cancel(cancel)?;
    let special = || io::Error::new(io::ErrorKind::Unsupported, "expected a regular file");
    #[cfg(windows)]
    if is_device_path(path) {
        return Err(special());
    }
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_file() => return Err(special()),
        Err(e) if e.kind() != io::ErrorKind::NotFound || write.is_none() => return Err(e),
        _ => (),
    }

    let mut options = OpenOptions::new();
    if write.is_some() {
        options.write(true).create(true).append(append);
    } else {
        options.read(true);
    }
    #[cfg(unix)]
    {
        use rustix::fs::OFlags;
        use std::os::unix::fs::OpenOptionsExt;
        // A FIFO or terminal can replace the path between metadata and open.
        options.custom_flags((OFlags::NONBLOCK | OFlags::NOCTTY).bits() as _);
    }
    check_cancel(cancel)?;
    let mut file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        // SAFETY: the file owns the handle throughout this call.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(special());
        }
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(special());
    }
    check_cancel(cancel)?;
    if let Some(bytes) = write {
        if !append {
            file.set_len(0)?;
        }
        for chunk in bytes.chunks(FILE_CHUNK) {
            check_cancel(cancel)?;
            file.write_all(chunk)?;
        }
        return Ok(Reply {
            value: json!(bytes.len()),
            data: None,
        });
    }

    let too_large = || io::Error::other(format!("file exceeds {BODY_LIMIT} bytes"));
    if metadata.len() > BODY_LIMIT as u64 {
        return Err(too_large());
    }
    let mut data = Vec::new();
    let mut chunk = [0; FILE_CHUNK];
    loop {
        check_cancel(cancel)?;
        let n = match file.read(&mut chunk) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            n => n?,
        };
        if n == 0 {
            break;
        }
        if n > BODY_LIMIT - data.len() {
            return Err(too_large());
        }
        data.extend_from_slice(&chunk[..n]);
    }
    Ok(Reply {
        value: Value::Null,
        data: Some(data),
    })
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

async fn request(
    client: &reqwest::Client,
    args: &Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, String> {
    let url = Url::parse(string(args, "url")?).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("http.request requires an HTTP(S) URL".into());
    }
    let method = match args.get("method") {
        Some(v) => Method::from_bytes(v.as_str().ok_or("method must be a string")?.as_bytes())
            .map_err(|e| format!("invalid HTTP method: {e}"))?,
        None => Method::GET,
    };
    let mut request = client.request(method, url);
    if let Some(headers) = args.get("headers") {
        for (name, value) in headers.as_object().ok_or("headers must be an object")? {
            request = request.header(name, value.as_str().ok_or("header values must be strings")?);
        }
    }
    if let Some(duration) = timeout(args)? {
        request = request.timeout(duration);
    }
    if let Some(data) = data {
        request = request.body(data);
    }
    let network_error = |e: reqwest::Error| {
        if e.is_timeout() {
            "timeout: http.request".to_owned()
        } else {
            format!("http.request: {e}")
        }
    };
    let mut response = request.send().await.map_err(network_error)?;
    let too_large = || format!("http.request: response exceeds {BODY_LIMIT} bytes");
    if response
        .content_length()
        .is_some_and(|n| n > BODY_LIMIT as u64)
    {
        return Err(too_large());
    }
    let mut headers = Map::new();
    for name in response.headers().keys() {
        let mut values: Vec<_> = response
            .headers()
            .get_all(name)
            .iter()
            .map(|v| Value::String(String::from_utf8_lossy(v.as_bytes()).into_owned()))
            .collect();
        let value = if values.len() == 1 {
            values.remove(0)
        } else {
            Value::Array(values)
        };
        headers.insert(name.as_str().to_owned(), value);
    }
    let status = response.status().as_u16();
    let mut data = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(network_error)? {
        if chunk.len() > BODY_LIMIT - data.len() {
            return Err(too_large());
        }
        data.extend_from_slice(&chunk);
    }
    Ok(Reply {
        value: json!({"status": status, "headers": headers}),
        data: Some(data),
    })
}

async fn command(
    host: &Host,
    op: &str,
    args: &Value,
    stdin: Option<Vec<u8>>,
) -> Result<Value, String> {
    let (program, argv) = if op == "bash" {
        ("bash", vec!["-c".into(), string(args, "script")?.into()])
    } else {
        let argv = match args.get("args") {
            None => Vec::new(),
            Some(v) => v
                .as_array()
                .ok_or("args must be a string array")?
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or("args must be a string array")
                })
                .collect::<Result<_, _>>()?,
        };
        (string(args, "program")?, argv)
    };
    if program.is_empty() || program.contains('\0') || argv.iter().any(|arg| arg.contains('\0')) {
        return Err("invalid program or argument".into());
    }
    if let Some(cwd) = args.get("cwd")
        && !cwd
            .as_str()
            .is_some_and(|s| !s.is_empty() && !s.contains('\0'))
    {
        return Err("cwd must be a valid path".into());
    }
    if let Some(env) = args.get("env") {
        for (k, v) in env.as_object().ok_or("env must be an object")? {
            if k.is_empty()
                || k.contains(['=', '\0'])
                || !v.as_str().is_some_and(|s| !s.contains('\0'))
            {
                return Err("invalid environment name or value".into());
            }
        }
    }
    #[cfg(unix)]
    use super::process::execute;
    #[cfg(windows)]
    use super::windows::execute;
    execute(program, &argv, args, stdin, host.cancel.clone())
        .await
        .map_err(|e| {
            if op == "bash" && e.starts_with("program_not_found") {
                format!("bash_unavailable: {e}")
            } else {
                e
            }
        })
}

pub(super) async fn drain(
    mut reader: impl AsyncRead + Unpin,
    finished: &CancellationToken,
) -> io::Result<(String, bool)> {
    let deadline = async {
        finished.cancelled().await;
        tokio::time::sleep(DRAIN_GRACE).await;
    };
    tokio::pin!(deadline);
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut chunk = [0; 8192];
    loop {
        let n = tokio::select! {
            biased;
            _ = &mut deadline => { truncated = true; break; }
            n = reader.read(&mut chunk) => n?,
        };
        if n == 0 {
            break;
        }
        let keep = n.min(OUTPUT_LIMIT - bytes.len());
        bytes.extend_from_slice(&chunk[..keep]);
        truncated |= keep < n;
    }
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated))
}
