mod agent;
mod api;
mod events;
mod model;
#[cfg(unix)]
mod socket;
mod store;
mod tools;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use clap::Parser;
use serde_json::json;
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
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}

#[derive(Clone)]
struct App {
    db: Db,
    client: reqwest::Client,
    stopping: CancellationToken,
    workers: TaskTracker,
}

#[derive(Parser)]
#[command(version, about = "Run the MyGo-Bot HTTP and SSE API")]
struct Options {
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let options = Options::parse();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_writer(io::stderr)
        .init();
    match run(options).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "server failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(options: Options) -> std::result::Result<(), Box<dyn std::error::Error>> {
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
    let (unix, socket_file) = socket::SocketFile::bind(options.unix_socket.as_deref())?;
    let app = App {
        db: Db::open(Path::new("mygo-bot.sqlite3"))?,
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        stopping: CancellationToken::new(),
        workers: TaskTracker::new(),
    };
    let routes = api::routes(app.clone());
    if generated {
        tracing::info!(%token, "generated TCP Bearer token");
    }
    #[cfg(unix)]
    tracing::info!(path = %socket_file.path.display(), "Unix socket ready");
    tracing::info!(%listen, "TCP ready");
    let tcp_routes = routes.clone().layer(axum::middleware::from_fn_with_state(
        Arc::<str>::from(token),
        api::authorize_tcp,
    ));
    let tcp_server = axum::serve(listener, tcp_routes)
        .with_graceful_shutdown(app.stopping.clone().cancelled_owned());
    #[cfg(unix)]
    let server = async {
        let unix_server = axum::serve(unix, routes)
            .with_graceful_shutdown(app.stopping.clone().cancelled_owned());
        tokio::try_join!(tcp_server.into_future(), unix_server.into_future()).map(|_| ())
    };
    #[cfg(not(unix))]
    let server = tcp_server.into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => return Ok(result?),
        signal = shutdown_signal() => signal?,
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

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result=tokio::signal::ctrl_c()=>result, _=term.recv()=>Ok(()) }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}
