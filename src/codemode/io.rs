use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, Request, Uri, header},
};
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use reqwest::{Method, Url};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{io::StreamReader, sync::CancellationToken};
use tower::ServiceExt;

use super::{
    Host, HostError, Reply, files,
    live::{CommandGuard, CommandPreview, McpGuard, Stream},
    streams::{Reservation, Resource},
};
use crate::cluster::identity::ResolvedNode;

pub(crate) const BODY_LIMIT: usize = 8 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 1024 * 1024;
const DRAIN_GRACE: Duration = Duration::from_millis(250);

pub(crate) type BoxBodyStream = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, String>> + Send>>;
pub(crate) type BoxUploadStream = BoxStream<'static, io::Result<Vec<u8>>>;

pub(crate) struct PreparedHttpResponse {
    pub status: u16,
    pub headers: Map<String, Value>,
    pub stream: BoxBodyStream,
    pub stream_deadline: Option<(tokio::time::Instant, &'static str)>,
}

pub(crate) fn command_output(
    exit_code: Option<i64>,
    signal: Option<i32>,
    timed_out: bool,
    stdout: String,
    stderr: String,
    truncated: bool,
) -> Value {
    json!({
        "exit_code": exit_code,
        "signal": signal,
        "timed_out": timed_out,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": truncated,
    })
}

pub async fn execute(
    host: &Host,
    op: &str,
    mut args: Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, HostError> {
    if !args.is_object() {
        return Err(format!("{op} expects an options object").into());
    }
    match op {
        "cluster.nodes" => {
            if host.cancel.is_cancelled() {
                return Err("cancelled".into());
            }
            let net = host.app.cluster.net.clone();
            let nodes = host
                .app
                .db
                .call(move |s| crate::cluster::identity::list_cluster_nodes(&s.conn, &net))
                .await
                .map_err(|e| HostError::from(e.to_string()))?;
            return Ok(Reply::value(Value::Array(nodes)));
        }
        "task.list" | "task.status" | "upload.open" | "stream.next" | "stream.write"
        | "stream.finish" | "sleep" => {
            return execute_orchestration(host, op, &args, data).await;
        }
        _ => {}
    }

    let is_node_op = op == "proc.exec"
        || op.starts_with("fs.")
        || op.starts_with("mcp.")
        || op == "http.request"
        || op == "mygo.request";
    if !is_node_op {
        return Err(format!("unknown host operation {op}").into());
    }
    if host.cancel.is_cancelled() {
        return Err("cancelled".into());
    }

    let target = {
        let obj = args.as_object_mut().unwrap();
        match obj.remove("node").and_then(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        }) {
            Some(s) if !s.trim().is_empty() && !s.contains('\0') => s,
            _ => {
                return Err(HostError::coded(
                    "EINVAL",
                    format!("{op} requires a nonempty string 'node' target"),
                ));
            }
        }
    };

    let resolved = host
        .app
        .db
        .call(move |s| Ok(crate::cluster::identity::resolve_node_target(&s.conn, &target)))
        .await
        .map_err(|e| HostError::from(e.to_string()))?
        .map_err(|(code, msg)| HostError::coded(code, msg))?;

    host.wait_for_execution().await?;
    match resolved {
        ResolvedNode::Local { name, .. } => execute_local_cap(host, &name, op, args, data).await,
        ResolvedNode::Remote(peer) => {
            crate::cluster::cap::execute_remote(host, peer, op, args, data).await
        }
    }
}

async fn execute_orchestration(
    host: &Host,
    op: &str,
    args: &Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, HostError> {
    tokio::select! {
        biased;
        _ = host.cancel.cancelled() => Err("cancelled".into()),
        result = async {
            match op {
                "task.list" | "task.status" => {
                    let id = match op {
                        "task.status" => Some(crate::tools::task_id(string(args, "id")?)?),
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
                "upload.open" | "stream.next" | "stream.write" | "stream.finish" => {
                    host.wait_for_execution().await?;
                    host.streams.execute(op, &args["id"], data).await
                }
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

async fn execute_local_cap(
    host: &Host,
    node_name: &str,
    op: &str,
    args: Value,
    data: Option<Vec<u8>>,
) -> Result<Reply, HostError> {
    if op == "proc.exec" {
        // Managed processes must finish their own cancellation and group cleanup.
        return Ok(Reply::value(command_local(host, node_name, &args, data).await?));
    }
    if op.starts_with("fs.") {
        return files::execute_local(
            &host.app,
            &host.cancel,
            Some(&host.streams),
            op,
            &args,
            data,
        )
        .await;
    }
    tokio::select! {
        biased;
        _ = host.cancel.cancelled() => Err("cancelled".into()),
        result = async {
            match op {
                op if op.starts_with("mcp.") => {
                    let call = McpGuard::start(&host.live, node_name, op, &args);
                    crate::mcp::execute(&host.app, host.sid, host.origin, op, args, |phase| {
                        call.set_phase(phase);
                    }).await.map_err(Into::into)
                }
                "http.request" => local_http(host, &args, data).await,
                "mygo.request" => local_mygo(host, &args, data).await,
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

pub(crate) fn timeout(args: &Value) -> Result<Option<Duration>, String> {
    args.get("timeout_ms")
        .map(|v| {
            v.as_u64()
                .filter(|ms| *ms > 0 && *ms <= 9_007_199_254_740_991)
                .map(Duration::from_millis)
                .ok_or_else(|| "timeout_ms must be a positive safe integer".to_owned())
        })
        .transpose()
}

fn parse_method(args: &Value) -> Result<Method, HostError> {
    match args.get("method") {
        Some(v) => Method::from_bytes(v.as_str().ok_or("method must be a string")?.as_bytes())
            .map_err(|e| format!("invalid HTTP method: {e}").into()),
        None => Ok(Method::GET),
    }
}

fn parse_stream_slot<'a>(
    host: &'a Host,
    args: &Value,
) -> Result<(bool, Option<Reservation<'a>>), HostError> {
    let stream = match args.get("stream") {
        None => false,
        Some(v) => v.as_bool().ok_or("stream must be a boolean")?,
    };
    let slot = stream.then(|| host.streams.reserve()).transpose()?;
    Ok((stream, slot))
}

fn collect_headers(map: &HeaderMap) -> Map<String, Value> {
    let mut headers = Map::new();
    for name in map.keys() {
        let mut values: Vec<_> = map
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
    headers
}

async fn local_http(host: &Host, args: &Value, data: Option<Vec<u8>>) -> Result<Reply, HostError> {
    let (stream_mode, slot) = parse_stream_slot(host, args)?;
    let upload = if let Some(id) = args.get("upload").filter(|v| !v.is_null()) {
        if data.is_some() {
            return Err("http.request cannot pass both upload and body".into());
        }
        let (stream, closed) = host.streams.upload_stream(id, host.cancel.clone()).await?;
        Some((Box::pin(stream) as BoxUploadStream, Some(closed)))
    } else {
        None
    };
    let prep = execute_http_prepared(&host.client, args, upload, data, stream_mode).await?;
    deliver_prepared_http("http.request", prep, slot).await
}

async fn local_mygo(host: &Host, args: &Value, data: Option<Vec<u8>>) -> Result<Reply, HostError> {
    let (stream_mode, slot) = parse_stream_slot(host, args)?;
    let upload = if let Some(id) = args.get("upload").filter(|v| !v.is_null()) {
        if data.is_some() {
            return Err("mygo.request cannot pass both upload and body".into());
        }
        let (stream, closed) = host.streams.upload_stream(id, host.cancel.clone()).await?;
        Some((Box::pin(stream) as BoxUploadStream, Some(closed)))
    } else {
        None
    };
    let prep = execute_mygo_prepared(&host.app, args, upload, data, stream_mode).await?;
    deliver_prepared_http("mygo.request", prep, slot).await
}

async fn deliver_prepared_http(
    op: &str,
    prep: PreparedHttpResponse,
    slot: Option<Reservation<'_>>,
) -> Result<Reply, HostError> {
    if let Some(slot) = slot {
        let reader = StreamReader::new(prep.stream.map_err(io::Error::other));
        let id = slot.insert_with_deadline(Resource::Reader(Box::pin(reader)), prep.stream_deadline);
        return Ok(Reply::value(json!({
            "status": prep.status,
            "headers": prep.headers,
            "body": id,
        })));
    }
    collect_http_body(op, prep).await
}

pub(crate) async fn execute_http_prepared(
    client: &reqwest::Client,
    args: &Value,
    upload: Option<(BoxUploadStream, Option<CancellationToken>)>,
    data: Option<Vec<u8>>,
    stream_mode: bool,
) -> Result<PreparedHttpResponse, HostError> {
    let url = Url::parse(string(args, "url")?).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("http.request requires an HTTP(S) URL".into());
    }
    let mut request = client.request(parse_method(args)?, url);
    if let Some(headers) = args.get("headers") {
        for (name, value) in headers.as_object().ok_or("headers must be an object")? {
            request = request.header(name, value.as_str().ok_or("header values must be strings")?);
        }
    }
    let duration = timeout(args)?;
    let stream_deadline = duration.map(|d| (tokio::time::Instant::now() + d, "timeout: http.request"));
    if let Some(duration) = duration {
        request = request.timeout(duration);
    }
    let mut upload_closed = None;
    if let Some((stream, closed)) = upload {
        request = request.body(reqwest::Body::wrap_stream(stream));
        upload_closed = closed;
    } else if let Some(data) = data {
        request = request.body(data);
    }
    let closed = async {
        match upload_closed {
            Some(closed) => closed.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let response = tokio::select! {
        biased;
        _ = closed => return Err("upload stopped".into()),
        reply = request.send() => reply.map_err(network_error)?,
    };
    let status = response.status().as_u16();
    let headers_map = response.headers().clone();
    let content_length = response.content_length();
    if !stream_mode && content_length.is_some_and(|n| n > BODY_LIMIT as u64) {
        return Err(format!("http.request: response exceeds {BODY_LIMIT} bytes").into());
    }
    let headers = collect_headers(&headers_map);
    let stream = Box::pin(response.bytes_stream().map_err(network_error));
    Ok(PreparedHttpResponse {
        status,
        headers,
        stream,
        stream_deadline,
    })
}

pub(crate) async fn execute_mygo_prepared(
    app: &crate::App,
    args: &Value,
    upload: Option<(BoxUploadStream, Option<CancellationToken>)>,
    data: Option<Vec<u8>>,
    stream_mode: bool,
) -> Result<PreparedHttpResponse, HostError> {
    let path = string(args, "path")?;
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.bytes().any(|b| b.is_ascii_control() || b == b' ')
    {
        return Err("mygo.request requires a relative path starting with '/'".into());
    }
    let uri: Uri = path.parse().map_err(|e| format!("invalid path: {e}"))?;
    if uri.scheme().is_some() || uri.authority().is_some() {
        return Err("mygo.request requires a relative path starting with '/'".into());
    }
    let mut builder = Request::builder().method(parse_method(args)?).uri(uri);
    if let Some(headers) = args.get("headers") {
        for (name, value) in headers.as_object().ok_or("headers must be an object")? {
            let value = value.as_str().ok_or("header values must be strings")?;
            builder = builder.header(name.as_str(), value);
        }
    }
    let deadline = timeout(args)?.map(|d| tokio::time::Instant::now() + d);
    let mut upload_closed = None;
    let body = if let Some((stream, closed)) = upload {
        upload_closed = closed;
        Body::from_stream(stream)
    } else if let Some(data) = data {
        Body::from(data)
    } else {
        Body::empty()
    };
    let request = builder
        .body(body)
        .map_err(|e| format!("invalid mygo.request: {e}"))?;
    let router = crate::api::internal_routes(app.clone());
    let closed = async {
        match upload_closed {
            Some(closed) => closed.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let stream_deadline = deadline.map(|at| (at, "timeout: mygo.request"));
    let dispatch = async {
        Ok::<_, HostError>(
            router
                .oneshot(request)
                .await
                .unwrap_or_else(|e| match e {}),
        )
    };
    let response = tokio::select! {
        biased;
        _ = closed => return Err("upload stopped".into()),
        reply = async {
            match deadline {
                Some(at) => tokio::time::timeout_at(at, dispatch)
                    .await
                    .map_err(|_| HostError::from("timeout: mygo.request"))?,
                None => dispatch.await,
            }
        } => reply?,
    };
    let is_sse = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|s| s.trim().eq_ignore_ascii_case("text/event-stream"))
        });
    if !stream_mode && is_sse {
        return Err("mygo.request: text/event-stream responses require stream: true".into());
    }
    let content_length = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if !stream_mode && content_length.is_some_and(|n| n > BODY_LIMIT as u64) {
        return Err(format!("mygo.request: response exceeds {BODY_LIMIT} bytes").into());
    }
    let status = response.status().as_u16();
    let headers = collect_headers(response.headers());
    let stream = Box::pin(
        response
            .into_body()
            .into_data_stream()
            .map_err(|e| format!("mygo.request: {e}")),
    );
    Ok(PreparedHttpResponse {
        status,
        headers,
        stream,
        stream_deadline,
    })
}

pub(crate) async fn collect_http_body(
    op: &str,
    mut prep: PreparedHttpResponse,
) -> Result<Reply, HostError> {
    let too_large = || format!("{op}: response exceeds {BODY_LIMIT} bytes");
    let mut data = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = async {
                match prep.stream_deadline {
                    Some((at, _)) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => return Err(HostError::from(prep.stream_deadline.map_or("timeout", |(_, m)| m))),
            item = prep.stream.next() => item,
        };
        let Some(chunk) = next.transpose()? else {
            break;
        };
        if chunk.len() > BODY_LIMIT - data.len() {
            return Err(too_large().into());
        }
        data.extend_from_slice(&chunk);
    }
    Ok(Reply {
        value: json!({"status": prep.status, "headers": prep.headers}),
        data: Some(data),
    })
}

fn network_error(e: reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout: http.request".into()
    } else {
        format!("http.request: {e}")
    }
}

pub(crate) fn validate_command(
    app: &crate::App,
    args: &Value,
) -> Result<(String, Vec<String>, PathBuf), String> {
    let program = string(args, "program")?.to_owned();
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
    let home = app.codemode.home_dir();
    let cwd = match args.get("cwd") {
        Some(value) => {
            let s = value
                .as_str()
                .filter(|s| !s.is_empty() && !s.contains('\0'))
                .ok_or("cwd must be a valid path")?;
            let path = Path::new(s);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                let h = home.ok_or(
                    "spawn_failed: service user home directory is unavailable; specify cwd",
                )?;
                match files::resolve_path(Some(h), path) {
                    Ok(Cow::Owned(p)) => p,
                    Ok(Cow::Borrowed(p)) => p.to_path_buf(),
                    Err(e) => return Err(format!("spawn_failed: {}", e.message())),
                }
            }
        }
        None => home
            .ok_or("spawn_failed: service user home directory is unavailable; specify cwd")?
            .to_path_buf(),
    };
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
    let _ = timeout(args)?;
    Ok((program, argv, cwd))
}

pub(crate) async fn execute_command_validated(
    program: &str,
    argv: &[String],
    cwd: &Path,
    args: &Value,
    stdin: Option<Vec<u8>>,
    cancel: CancellationToken,
    preview: &CommandPreview<'_>,
) -> Result<Value, String> {
    #[cfg(unix)]
    use super::process::execute;
    #[cfg(windows)]
    use super::windows::execute;
    execute(program, argv, cwd, args, stdin, cancel, preview).await
}

async fn command_local(
    host: &Host,
    node_name: &str,
    args: &Value,
    stdin: Option<Vec<u8>>,
) -> Result<Value, String> {
    let (program, argv, cwd) = validate_command(&host.app, args)?;
    let guard = CommandGuard::start(&host.live, node_name, &program, cwd.to_str());
    let preview = CommandPreview::Local(&guard);
    execute_command_validated(
        &program,
        &argv,
        &cwd,
        args,
        stdin,
        host.cancel.clone(),
        &preview,
    )
    .await
}

pub(super) async fn drain(
    mut reader: impl AsyncRead + Unpin,
    finished: &CancellationToken,
    preview: Option<(&CommandPreview<'_>, Stream)>,
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
