mod agent;
mod api;
mod calls;
mod catalog;
mod daemon;
mod docs;
mod events;
mod media;
mod model;
mod pricing;
mod sandbox;
#[cfg(unix)]
mod socket;
mod store;
mod tools;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use clap::{CommandFactory, Parser};
use serde::Serialize;
use std::{fmt, io, net::SocketAddr, path::Path, process::ExitCode, sync::Arc, time::Duration};
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
#[derive(Serialize, utoipa::ToSchema)]
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
    sandbox: Arc<sandbox::Engine>,
    client: reqwest::Client,
    stopping: CancellationToken,
    workers: TaskTracker,
}

#[derive(Parser)]
#[command(version, about = "Run the MyGo-Bot HTTP and SSE API")]
struct Options {
    /// Start the server in the background and append logs to ./mygo-bot.log.
    #[arg(long)]
    daemon: bool,
    /// With --daemon, keep the server attached to this terminal and log to stderr.
    #[arg(long)]
    foreground: bool,
    /// Fixed number of JavaScript worker threads (defaults to at most four logical CPUs).
    #[arg(long, value_name = "COUNT")]
    js_threads: Option<std::num::NonZeroUsize>,
    /// TCP bind address. Port 0 lets the OS choose a free port.
    #[arg(long, default_value = "127.0.0.1:0", value_name = "IP:PORT")]
    listen: SocketAddr,
    /// TCP Bearer token. Omit to generate a token and print it once at startup.
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
    /// Local socket (no token). Defaults to $XDG_RUNTIME_DIR/mygo-bot.sock or a private mygo-bot-<uid> directory under the system temp directory.
    #[cfg(unix)]
    #[arg(long, value_name = "PATH")]
    unix_socket: Option<std::path::PathBuf>,
    /// Disable the local Unix socket and serve only TCP.
    #[cfg(unix)]
    #[arg(long, conflicts_with = "unix_socket")]
    no_unix_socket: bool,
}

fn main() -> ExitCode {
    let options = Options::parse();
    if !options.daemon {
        Options::command()
            .print_help()
            .unwrap_or_else(|e| clap::Error::from(e).exit());
        return ExitCode::SUCCESS;
    }
    tracing_subscriber::fmt()
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
    let app = App {
        sandbox: Arc::new(sandbox::Engine::new(threads)?),
        db: Db::open(Path::new("mygo-bot.sqlite3"))?,
        client: reqwest::Client::builder()
            // Each retry gets its own llm_calls row in the worker loop.
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        stopping: CancellationToken::new(),
        workers: TaskTracker::new(),
    };
    let (routes, openapi) = api::routes(app.clone());
    let docs = listen
        .ip()
        .is_loopback()
        .then(|| docs::routes(openapi))
        .transpose()?;
    let signal = shutdown_signal()?;
    daemon::Ready {
        listen,
        #[cfg(unix)]
        socket: unix
            .as_ref()
            .map(|(_, file)| file.path.display().to_string()),
        #[cfg(not(unix))]
        socket: None,
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
