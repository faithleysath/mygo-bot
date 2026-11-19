mod agent;
mod api;
mod events;
mod model;
mod store;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::{fmt, path::Path, time::Duration};
use store::Db;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    let app = App {
        db: Db::open(Path::new("mygo-bot.sqlite3"))?,
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        stopping: CancellationToken::new(),
        workers: TaskTracker::new(),
    };
    eprintln!("listening on http://{}", listener.local_addr()?);
    let stopping = app.stopping.clone();
    let server = axum::serve(listener, api::routes(app.clone()))
        .with_graceful_shutdown(stopping.cancelled_owned())
        .into_future();
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
