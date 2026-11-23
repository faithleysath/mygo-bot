mod agent;
mod api;
mod calls;
mod catalog;
mod codemode;
mod daemon;
mod docs;
mod events;
mod mcp;
mod media;
mod model;
mod pricing;
#[cfg(unix)]
mod socket;
mod store;
mod tools;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{
    ffi::{OsStr, OsString},
    fmt, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};
#[cfg(unix)]
use std::path::PathBuf;
use store::Db;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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
    codemode: Arc<codemode::Engine>,
    client: reqwest::Client,
    mcp_probes: Arc<tokio::sync::Semaphore>,
    stopping: CancellationToken,
    workers: TaskTracker,
}

#[cfg(unix)]
const HELP_SOCKET_OPTIONS: &str = concat!(
    "      --unix-socket <PATH>  Local socket (no token). Defaults to ",
    cfg_select! {
        target_os = "macos" => { "" }
        _ => { "$XDG_RUNTIME_DIR/mygo-bot.sock or " }
    },
    "mygo-bot-<uid>/mygo-bot.sock under the system temp directory\n",
    "      --no-unix-socket      Disable the local Unix socket and serve only TCP\n",
);
#[cfg(not(unix))]
const HELP_SOCKET_OPTIONS: &str = "";

fn print_help() {
    print!(
        "Run the MyGo-Bot HTTP and SSE API\n\n\
Usage: mygo-bot [OPTIONS]\n\n\
Options:\n\
      --daemon              Start the server in the background and append logs to ./mygo-bot.log\n\
      --foreground          With --daemon, keep the server attached to this terminal and log to stderr\n\
      --js-threads <COUNT>  Fixed number of JavaScript worker threads (defaults to at most four logical CPUs)\n\
      --listen <IP:PORT>    TCP bind address. Port 0 lets the OS choose a free port [default: 127.0.0.1:0]\n\
      --token <TOKEN>       TCP Bearer token. Omit to generate a token and print it once at startup\n\
{HELP_SOCKET_OPTIONS}\
  -h, --help                Print help\n\
  -V, --version             Print version\n"
    );
}

struct Options {
    daemon: bool,
    foreground: bool,
    js_threads: Option<NonZeroUsize>,
    listen: SocketAddr,
    token: Option<String>,
    #[cfg(unix)]
    unix_socket: Option<PathBuf>,
    #[cfg(unix)]
    no_unix_socket: bool,
}

enum Cli {
    Run(Options),
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

impl Options {
    fn parse() -> Cli {
        match Self::parse_inner() {
            Ok(cli) => cli,
            Err(reason) => {
                eprintln!("error: {reason}\n\nUsage: mygo-bot [OPTIONS]");
                Cli::Exit(ExitCode::from(2))
            }
        }
    }

    fn parse_inner() -> std::result::Result<Cli, String> {
        let mut daemon = false;
        let mut foreground = false;
        let mut js_threads: Option<NonZeroUsize> = None;
        let mut listen: Option<SocketAddr> = None;
        let mut token: Option<String> = None;
        #[cfg(unix)]
        let mut unix_socket: Option<PathBuf> = None;
        #[cfg(unix)]
        let mut no_unix_socket = false;

        let mut args = std::env::args_os().skip(1);
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
                    print_help();
                    return Ok(Cli::Exit(ExitCode::SUCCESS));
                }
                "-V" | "--version" if inline.is_none() => {
                    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
                    return Ok(Cli::Exit(ExitCode::SUCCESS));
                }
                "--daemon" if inline.is_none() => {
                    if daemon {
                        return Err("the argument '--daemon' cannot be used multiple times".into());
                    }
                    daemon = true;
                }
                "--foreground" if inline.is_none() => {
                    if foreground {
                        return Err(
                            "the argument '--foreground' cannot be used multiple times".into(),
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
                    js_threads = Some(
                        val.parse::<NonZeroUsize>()
                            .map_err(|e| format!("invalid value '{val}' for '--js-threads': {e}"))?,
                    );
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
                other if other.starts_with('-') => {
                    return Err(format!("unexpected argument '{other}' found"));
                }
                other => {
                    return Err(format!("unexpected argument '{other}' found"));
                }
            }
        }
        Ok(Cli::Run(Options {
            daemon,
            foreground,
            js_threads,
            listen: listen.unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            token,
            #[cfg(unix)]
            unix_socket,
            #[cfg(unix)]
            no_unix_socket,
        }))
    }
}

fn main() -> ExitCode {
    let options = match Options::parse() {
        Cli::Run(options) => options,
        Cli::Exit(code) => return code,
    };
    if !options.daemon {
        print_help();
        return ExitCode::SUCCESS;
    }
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
    options: Options,
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

async fn serve(
    options: Options,
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
    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("bind TCP {}: {e}", options.listen)))?;
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
        App {
            codemode: Arc::new(codemode::Engine::new(threads, make_client()?)?),
            mcp_probes: Arc::new(tokio::sync::Semaphore::new(4)),
            db: Db::open(Path::new("mygo-bot.sqlite3"))?,
            client: make_client()?,
            stopping: CancellationToken::new(),
            workers: TaskTracker::new(),
        }
    };
    app.workers
        .spawn(mcp::reap_idle(app.db.clone(), app.stopping.clone()));
    let routes = api::routes(app.clone());
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
    let mut tcp_routes = routes.clone().layer(axum::middleware::from_fn_with_state(
        Arc::<str>::from(token),
        api::authorize_tcp,
    ));
    if let Some(docs) = docs {
        tcp_routes = tcp_routes.merge(docs);
    }
    let tcp_server = axum::serve(listener, tcp_routes)
        .with_graceful_shutdown(app.stopping.clone().cancelled_owned());
    let server = async {
        #[cfg(unix)]
        if let Some((listener, _socket_file)) = unix {
            let unix_server = axum::serve(listener, routes)
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
