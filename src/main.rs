mod agent;
mod api;
mod calls;
mod catalog;
mod cluster;
mod codemode;
mod daemon;
mod docs;
mod events;
mod id;
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod install;
mod mcp;
mod media;
mod model;
mod pricing;
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod service;
#[cfg(unix)]
mod socket;
mod store;
mod tools;
mod webui;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
#[cfg(unix)]
use std::path::PathBuf;
use std::{
    ffi::{OsStr, OsString},
    fmt, io,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};
use store::Db;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub const BIN_NAME: &str = "mygo";
pub const VERSION: &str = env!("MYGO_VERSION");
const DEFAULT_PORT: u16 = 26946;
const DEFAULT_PORT_ATTEMPTS: u16 = 100;

type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug)]
struct Error(StatusCode, String);
impl Error {
    fn bad(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
    fn conflict(message: impl Into<String>) -> Self {
        Self(StatusCode::CONFLICT, message.into())
    }
    fn internal(message: impl Into<String>) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, message.into())
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.1)
    }
}
impl std::error::Error for Error {}
impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Self(StatusCode::NOT_FOUND, "not found".into())
        } else {
            Self::internal(err.to_string())
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e.to_string())
    }
}
#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(ErrorBody { error: self.1 })).into_response()
    }
}

#[derive(Clone)]
struct App {
    db: Db,
    cluster: Arc<cluster::Cluster>,
    exec: Arc<cluster::exec::ExecPlane>,
    codemode: Arc<codemode::Engine>,
    client: reqwest::Client,
    mcp_probes: Arc<tokio::sync::Semaphore>,
    streams: events::Hub,
    stopping: CancellationToken,
    workers: TaskTracker,
}

#[cfg(unix)]
const HELP_SOCKET_OPTIONS: &str = concat!(
    "  --unix-socket <PATH>  Local socket (no token). Defaults to ",
    "mygo-<uid>/mygo.sock under the system temp directory\n",
    "  --no-unix-socket      Disable the local Unix socket and serve only TCP\n",
);
#[cfg(not(unix))]
const HELP_SOCKET_OPTIONS: &str = "";

const HELP: &str = concat!(
    "Usage: mygo <COMMAND> [OPTIONS]\n\nCommands:\n  daemon     Run the HTTP and SSE API\n",
    cfg_select! {
        any(target_os = "macos", target_os = "linux") => {
            "  install    Install the executable\n  uninstall  Unregister the service and remove the executable\n  service    Manage the system service\n"
        }
        _ => { "" }
    },
    "\n  -h, --help     Print help\n  -V, --version  Print version"
);

fn daemon_help() {
    print!(
        "Usage: mygo daemon [OPTIONS]

By default, start in the background and log to ./mygo.log.

Options:
  --foreground            Run attached to this terminal and log to stderr
  --js-threads <COUNT>    JavaScript workers (default: up to four logical CPUs)
  --listen <IP:PORT>      Bind this address once (0 selects a free port). Default: 127.0.0.1:{DEFAULT_PORT}, then the next free port ({DEFAULT_PORT_ATTEMPTS} attempts)
  --token <TOKEN>         TCP Bearer token (default: generate and print once)
  --cluster-local         Disable public relay and DNS discovery for local-only cluster mode
  --cluster-bind <IP:PORT> Bind the iroh cluster endpoint to a specific UDP address
{HELP_SOCKET_OPTIONS}  -h, --help              Print help
"
    );
}

struct DaemonOptions {
    foreground: bool,
    js_threads: Option<NonZeroUsize>,
    listen: Option<SocketAddr>,
    token: Option<String>,
    cluster_local: bool,
    cluster_bind: Option<SocketAddr>,
    #[cfg(unix)]
    unix_socket: Option<PathBuf>,
    #[cfg(unix)]
    no_unix_socket: bool,
}

enum Cli {
    Run(DaemonOptions),
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    Manage(install::Scope, service::Action),
    Exit(ExitCode),
}

fn split_long_eq(arg: &OsStr) -> Option<(&str, OsString)> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let bytes = arg.as_bytes();
        let eq = bytes.iter().position(|&b| b == b'=')?;
        let key = std::str::from_utf8(&bytes[..eq]).ok()?;
        Some((key, OsString::from_vec(bytes[eq + 1..].to_vec())))
    }
    #[cfg(not(unix))]
    {
        let text = arg.to_str()?;
        let (key, val) = text.split_once('=')?;
        Some((key, OsString::from(val)))
    }
}

fn starts_with_hyphen(value: &OsStr) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        value.as_bytes().starts_with(b"-")
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        value.encode_wide().next() == Some(b'-' as u16)
    }
    #[cfg(not(any(unix, windows)))]
    {
        value.to_str().is_some_and(|s| s.starts_with('-'))
    }
}

fn take_os_value(
    inline: Option<OsString>,
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> std::result::Result<OsString, String> {
    match inline {
        Some(value) => Ok(value),
        None => match args.next() {
            Some(value) if !starts_with_hyphen(&value) => Ok(value),
            _ => Err(format!(
                "a value is required for '{flag}' but none was supplied"
            )),
        },
    }
}

fn take_str_value(
    inline: Option<OsString>,
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> std::result::Result<String, String> {
    take_os_value(inline, args, flag)?
        .into_string()
        .map_err(|_| format!("invalid UTF-8 value for '{flag}'"))
}

fn parse() -> std::result::Result<Cli, String> {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next() else {
        println!("{HELP}");
        return Ok(Cli::Exit(ExitCode::SUCCESS));
    };
    match command.to_str() {
        Some("-h" | "--help" | "-V" | "--version") => {
            if let Some(extra) = args.next() {
                return Err(format!("unexpected argument {extra:?}"));
            }
            if command == "-V" || command == "--version" {
                println!("{BIN_NAME} {VERSION}");
            } else {
                println!("{HELP}");
            }
        }
        Some("daemon") => return DaemonOptions::parse(args),
        Some(name @ ("install" | "uninstall" | "service")) => {
            cfg_select! {
                any(target_os = "macos", target_os = "linux") => { return service::parse(name, args); }
                windows => { return Err(format!("{name} is not supported on Windows yet")); }
                _ => { return Err(format!("{name} is not supported on this platform")); }
            }
        }
        _ => return Err(format!("unknown command {command:?}")),
    }
    Ok(Cli::Exit(ExitCode::SUCCESS))
}

impl DaemonOptions {
    fn parse(mut args: impl Iterator<Item = OsString>) -> std::result::Result<Cli, String> {
        let mut foreground = false;
        let mut js_threads: Option<NonZeroUsize> = None;
        let mut listen: Option<SocketAddr> = None;
        let mut token: Option<String> = None;
        let mut cluster_local = false;
        let mut cluster_bind: Option<SocketAddr> = None;
        #[cfg(unix)]
        let mut unix_socket: Option<PathBuf> = None;
        #[cfg(unix)]
        let mut no_unix_socket = false;

        while let Some(arg) = args.next() {
            let (flag, inline) = match split_long_eq(&arg) {
                Some((key, val)) => (key.to_owned(), Some(val)),
                None => match arg.into_string() {
                    Ok(s) => (s, None),
                    Err(_) => return Err("unexpected non-UTF-8 argument".into()),
                },
            };
            match flag.as_str() {
                "-h" | "--help" if inline.is_none() => {
                    daemon_help();
                    return Ok(Cli::Exit(ExitCode::SUCCESS));
                }
                "--foreground" if inline.is_none() => {
                    if foreground {
                        return Err(
                            "the argument '--foreground' cannot be used multiple times".into()
                        );
                    }
                    foreground = true;
                }
                "--js-threads" => {
                    if js_threads.is_some() {
                        return Err(
                            "the argument '--js-threads <COUNT>' cannot be used multiple times"
                                .into(),
                        );
                    }
                    let val = take_str_value(inline, &mut args, "--js-threads")?;
                    js_threads =
                        Some(val.parse::<NonZeroUsize>().map_err(|e| {
                            format!("invalid value '{val}' for '--js-threads': {e}")
                        })?);
                }
                "--listen" => {
                    if listen.is_some() {
                        return Err(
                            "the argument '--listen <IP:PORT>' cannot be used multiple times"
                                .into(),
                        );
                    }
                    let val = take_str_value(inline, &mut args, "--listen")?;
                    listen = Some(
                        val.parse::<SocketAddr>()
                            .map_err(|e| format!("invalid value '{val}' for '--listen': {e}"))?,
                    );
                }
                "--token" => {
                    if token.is_some() {
                        return Err(
                            "the argument '--token <TOKEN>' cannot be used multiple times".into(),
                        );
                    }
                    token = Some(take_str_value(inline, &mut args, "--token")?);
                }
                "--cluster-local" if inline.is_none() => {
                    if cluster_local {
                        return Err(
                            "the argument '--cluster-local' cannot be used multiple times".into(),
                        );
                    }
                    cluster_local = true;
                }
                "--cluster-bind" => {
                    if cluster_bind.is_some() {
                        return Err(
                            "the argument '--cluster-bind <IP:PORT>' cannot be used multiple times"
                                .into(),
                        );
                    }
                    let val = take_str_value(inline, &mut args, "--cluster-bind")?;
                    cluster_bind = Some(val.parse::<SocketAddr>().map_err(|e| {
                        format!("invalid value '{val}' for '--cluster-bind': {e}")
                    })?);
                }
                #[cfg(unix)]
                "--unix-socket" => {
                    if unix_socket.is_some() {
                        return Err(
                            "the argument '--unix-socket <PATH>' cannot be used multiple times"
                                .into(),
                        );
                    }
                    if no_unix_socket {
                        return Err(
                            "the argument '--unix-socket <PATH>' cannot be used with '--no-unix-socket'"
                                .into(),
                        );
                    }
                    unix_socket = Some(PathBuf::from(take_os_value(
                        inline,
                        &mut args,
                        "--unix-socket",
                    )?));
                }
                #[cfg(unix)]
                "--no-unix-socket" if inline.is_none() => {
                    if no_unix_socket {
                        return Err(
                            "the argument '--no-unix-socket' cannot be used multiple times".into(),
                        );
                    }
                    if unix_socket.is_some() {
                        return Err(
                            "the argument '--no-unix-socket' cannot be used with '--unix-socket <PATH>'"
                                .into(),
                        );
                    }
                    no_unix_socket = true;
                }
                other => {
                    return Err(format!("unexpected argument '{other}' found"));
                }
            }
        }
        Ok(Cli::Run(Self {
            foreground,
            js_threads,
            listen,
            token,
            cluster_local,
            cluster_bind,
            #[cfg(unix)]
            unix_socket,
            #[cfg(unix)]
            no_unix_socket,
        }))
    }
}

fn main() -> ExitCode {
    let options = match parse() {
        Ok(Cli::Run(options)) => options,
        Ok(Cli::Exit(code)) => return code,
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        Ok(Cli::Manage(scope, action)) => {
            let system_query = scope == install::Scope::System
                && matches!(
                    action,
                    service::Action::Status | service::Action::Logs { .. }
                );
            return service::run(scope, action).unwrap_or_else(|err| {
                eprintln!("error: {err}");
                if system_query && err.kind() == io::ErrorKind::PermissionDenied {
                    eprintln!("try this query with sudo");
                }
                ExitCode::FAILURE
            });
        }
        Err(reason) => {
            eprintln!("error: {reason}");
            return ExitCode::from(2);
        }
    };
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_writer(io::stderr)
        .init();
    match daemon::start(options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "server failed");
            ExitCode::FAILURE
        }
    }
}

fn run(
    options: DaemonOptions,
    parent: &mut Option<io::Stdout>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(serve(options, parent));
    // Blocking filesystem calls may outlive their cancelled owner on an unresponsive volume.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn bind_tcp(listen: Option<SocketAddr>) -> io::Result<tokio::net::TcpListener> {
    if let Some(addr) = listen {
        return tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("bind TCP {addr}: {e}")));
    }
    for offset in 0..DEFAULT_PORT_ATTEMPTS {
        let Some(port) = DEFAULT_PORT.checked_add(offset) else {
            break;
        };
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        match tokio::net::TcpListener::bind(addr).await {
            Err(err) if err.kind() == io::ErrorKind::AddrInUse => continue,
            Err(e) => return Err(io::Error::new(e.kind(), format!("bind TCP {addr}: {e}"))),
            Ok(listener) => return Ok(listener),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        format!(
            "all loopback ports {DEFAULT_PORT}..={} are in use",
            DEFAULT_PORT.saturating_add(DEFAULT_PORT_ATTEMPTS - 1)
        ),
    ))
}

async fn serve(
    options: DaemonOptions,
    parent: &mut Option<io::Stdout>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let generated = options.token.is_none();
    let token = match options.token {
        Some(value) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_graphic()) => value,
        Some(_) => {
            return Err("--token must contain visible ASCII characters without spaces".into());
        }
        None => {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes)?;
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
    };
    let listener = bind_tcp(options.listen).await?;
    let listen = listener.local_addr()?;
    #[cfg(unix)]
    let unix = (!options.no_unix_socket)
        .then(|| socket::SocketFile::bind(options.unix_socket.as_deref()))
        .transpose()?;
    let threads = options.js_threads.map_or_else(
        || std::thread::available_parallelism().map_or(1, |n| n.get().min(4)),
        |n| n.get(),
    );
    let app = {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        let tls = {
            let mut builder = native_tls::TlsConnector::builder();
            // Match the ALPN list of the enabled reqwest HTTP/1.1 features.
            builder.request_alpns(&["http/1.1"]);
            builder.build()?
        };
        let make_client = || {
            let builder = reqwest::Client::builder()
                // Each retry gets its own llm_calls row in the worker loop.
                .retry(reqwest::retry::never())
                .connect_timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none());
            // Share the system trust store while building separate connection pools.
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            let builder = builder.tls_backend_preconfigured(tls.clone());
            builder.build()
        };
        let streams = events::Hub::default();
        let signal = Arc::new(cluster::ClusterSignal::new());
        let presence = cluster::exec::PresenceState::new();
        let db = Db::open(
            Path::new("mygo.sqlite3"),
            streams.global_sender(),
            signal.clone(),
            presence.clone(),
        )?;
        let client = make_client()?;
        let stopping = CancellationToken::new();
        let workers = TaskTracker::new();
        let cluster = cluster::Cluster::start(
            cluster::ClusterStart {
                db: db.clone(),
                client: client.clone(),
                streams: streams.clone(),
                signal,
                presence: presence.clone(),
                config: cluster::ClusterConfig {
                    local_only: options.cluster_local,
                    bind_addr: options.cluster_bind,
                },
                stopping: stopping.clone(),
            },
            &workers,
        )
        .await?;
        let exec = cluster::exec::ExecPlane::new(
            db.clone(),
            cluster.net.clone(),
            presence,
            cluster.sync.clone(),
            stopping.clone(),
            workers.clone(),
        );
        cluster.net.attach_exec_plane(Arc::downgrade(&exec));
        let app = App {
            codemode: Arc::new(codemode::Engine::new(threads, make_client()?)?),
            mcp_probes: Arc::new(tokio::sync::Semaphore::new(4)),
            db,
            cluster,
            exec: exec.clone(),
            client,
            streams,
            stopping,
            workers,
        };
        exec.spawn_loops(app.clone());
        let cap_server = Arc::new(cluster::cap::CapServer::new(app.clone()));
        app.cluster.net.attach_cap_handler(Arc::downgrade(&cap_server));
        let exec_server = Arc::new(cluster::exec::ExecServer::new(app.clone()));
        app.cluster
            .net
            .attach_exec_handler(Arc::downgrade(&exec_server));
        (app, cap_server, exec_server)
    };
    let (app, _cap_server, _exec_server) = app;
    app.workers
        .spawn(mcp::reap_idle(app.db.clone(), app.stopping.clone()));
    let routes = api::routes(app.clone());
    let web = webui::routes();
    let docs = listen.ip().is_loopback().then(docs::routes);
    let signal = shutdown_signal()?;
    daemon::Ready {
        listen,
        #[cfg(unix)]
        socket: unix
            .as_ref()
            .map(|(_, file)| file.path.display().to_string()),
        token: generated.then(|| token.clone()),
    }
    .announce(parent)?;
    let token = Arc::<str>::from(token);
    let mut tcp_routes = routes
        .clone()
        .layer(axum::middleware::from_fn_with_state(
            token.clone(),
            api::authorize_tcp,
        ))
        .merge(web.clone());
    if let Some(docs) = docs {
        tcp_routes = tcp_routes.merge(docs);
    }
    let tcp_server = axum::serve(listener, tcp_routes)
        .with_graceful_shutdown(app.stopping.clone().cancelled_owned());
    let server = async {
        #[cfg(unix)]
        if let Some((listener, _socket_file)) = unix {
            let unix_server = axum::serve(
                listener,
                routes.merge(web).merge(socket::runtime_route(listen, token)),
            )
            .with_graceful_shutdown(app.stopping.clone().cancelled_owned());
            return tokio::try_join!(tcp_server.into_future(), unix_server.into_future())
                .map(|_| ());
        }
        tcp_server.await
    };
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => return Ok(result?),
        result = signal => result?,
    }
    let (workers, http) = tokio::join!(
        app.shutdown(),
        tokio::time::timeout(Duration::from_secs(6), server)
    );
    app.cluster.shutdown().await;
    workers?;
    if let Ok(result) = http {
        result?;
    }
    Ok(())
}

fn shutdown_signal() -> io::Result<impl Future<Output = io::Result<()>>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! { _ = interrupt.recv() => Ok(()), _ = term.recv() => Ok(()) }
        })
    }
    #[cfg(not(unix))]
    Ok(tokio::signal::ctrl_c())
}
