use std::{io, time::Duration};

use futures_util::TryStreamExt;
use reqwest::{Method, Url};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{io::StreamReader, sync::CancellationToken};

use super::{
    Host, HostError, Reply, files,
    live::{CommandGuard, McpGuard, Stream},
    streams::{Reservation, Resource},
};

pub(super) const BODY_LIMIT: usize = 8 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 256 * 1024;
const DRAIN_GRACE: Duration = Duration::from_millis(250);

pub async fn execute(
    host: &Host,
    op: &str,
    args: Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, HostError> {
    if !args.is_object() {
        return Err(format!("{op} expects an options object").into());
    }
    if op == "proc.exec" {
        // Managed processes must finish their own cancellation and group cleanup.
        return Ok(Reply::value(command(host, &args, data).await?));
    }
    if op.starts_with("fs.") {
        return files::execute(host, op, &args, data).await;
    }
    tokio::select! {
        biased;
        _ = host.cancel.cancelled() => Err("cancelled".into()),
        result = async {
            match op {
                op if op.starts_with("mcp.") => {
                    let call = McpGuard::start(&host.live, op, &args);
                    crate::mcp::execute(&host.app, host.sid, host.origin, op, args, |phase| {
                        call.set_phase(phase);
                    }).await.map_err(Into::into)
                }
                "task.list" | "task.status" => {
                    let id = match op {
                        "task.status" => Some(crate::tools::task_id(string(&args, "id")?)?),
                        _ => None,
                    };
                    let mut last = host.last_task_query.lock().await;
                    if let Some(at) = *last {
                        tokio::time::sleep_until(at + Duration::from_millis(100)).await;
                    }
                    *last = Some(tokio::time::Instant::now());
                    let sid = host.sid;
                    let value = host.app.db.call(move |s| match id {
                        Some(id) => super::full_status(s, sid, id),
                        None => Ok(super::task_list(s, sid)),
                    }).await.map_err(|e| e.to_string())?;
                    Ok(Reply::value(value))
                }
                "http.request" => request(host, &args, data).await,
                "upload.open" | "stream.next" | "stream.write" | "stream.finish" => host.streams.execute(op, &args["id"], data).await,
                "sleep" => {
                    let ms = args["ms"].as_u64().ok_or("ms must be a nonnegative integer")?;
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(Reply::value(Value::Null))
                }
                _ => Err(format!("unknown host operation {op}").into()),
            }
        } => result,
    }
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

async fn request(host: &Host, args: &Value, data: Option<Vec<u8>>) -> Result<Reply, HostError> {
    let url = Url::parse(string(args, "url")?).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("http.request requires an HTTP(S) URL".into());
    }
    let method = match args.get("method") {
        Some(v) => Method::from_bytes(v.as_str().ok_or("method must be a string")?.as_bytes())
            .map_err(|e| format!("invalid HTTP method: {e}"))?,
        None => Method::GET,
    };
    let mut request = host.client.request(method, url);
    if let Some(headers) = args.get("headers") {
        for (name, value) in headers.as_object().ok_or("headers must be an object")? {
            request = request.header(name, value.as_str().ok_or("header values must be strings")?);
        }
    }
    if let Some(duration) = timeout(args)? {
        request = request.timeout(duration);
    }
    let mut upload_closed = None;
    if let Some(id) = args.get("upload") {
        let (body, closed) = host.streams.upload(id, host.cancel.clone()).await?;
        request = request.body(body);
        upload_closed = Some(closed);
    } else if let Some(data) = data {
        request = request.body(data);
    }
    let slot = args["stream"]
        .as_bool()
        .unwrap_or(false)
        .then(|| host.streams.reserve())
        .transpose()?;
    let closed = async {
        match upload_closed {
            Some(closed) => closed.cancelled().await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        _ = closed => Err("upload stopped".into()),
        reply = async {
            let reply = request.send().await.map_err(network_error)?;
            receive(reply, slot).await
        } => reply,
    }
}

fn network_error(e: reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout: http.request".into()
    } else {
        format!("http.request: {e}")
    }
}

async fn receive(
    mut response: reqwest::Response,
    slot: Option<Reservation<'_>>,
) -> Result<Reply, HostError> {
    let too_large = || format!("http.request: response exceeds {BODY_LIMIT} bytes");
    if slot.is_none()
        && response
            .content_length()
            .is_some_and(|n| n > BODY_LIMIT as u64)
    {
        return Err(too_large().into());
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
    if let Some(slot) = slot {
        let reader = StreamReader::new(response.bytes_stream().map_err(io::Error::other));
        let id = slot.insert(Resource::Reader(Box::pin(reader)));
        return Ok(Reply::value(
            json!({"status":status, "headers":headers, "body":id}),
        ));
    }
    let mut data = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(network_error)? {
        if chunk.len() > BODY_LIMIT - data.len() {
            return Err(too_large().into());
        }
        data.extend_from_slice(&chunk);
    }
    Ok(Reply {
        value: json!({"status": status, "headers": headers}),
        data: Some(data),
    })
}

async fn command(host: &Host, args: &Value, stdin: Option<Vec<u8>>) -> Result<Value, String> {
    let program = string(args, "program")?;
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
            .collect::<Result<Vec<_>, _>>()?,
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
    let preview = CommandGuard::start(&host.live, program, &argv, args["cwd"].as_str());
    execute(program, &argv, args, stdin, host.cancel.clone(), &preview).await
}

pub(super) async fn drain(
    mut reader: impl AsyncRead + Unpin,
    finished: &CancellationToken,
    preview: Option<(&CommandGuard<'_>, Stream)>,
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
        if let Some((command, stream)) = &preview {
            command.append(stream, &chunk[..n]);
        }
        let keep = n.min(OUTPUT_LIMIT - bytes.len());
        bytes.extend_from_slice(&chunk[..keep]);
        truncated |= keep < n;
    }
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated))
}
