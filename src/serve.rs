//! Running a [`Server`]: bind, serve, shut down cleanly.
//!
//! For a local app that talks to a bog on the same machine, the usual
//! shape is spawn-or-connect: read the [discovery file](Server::discovery);
//! if it names a live server, use it; if not, start one (a second start
//! on the same store fails cleanly: bog locks the store's directory).
//! [`idle_exit`](Server::idle_exit) lets that server go away once nothing
//! has used it for a while.

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::Router;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use serde_json::json;
use tower_http::timeout::RequestBodyTimeoutLayer;

use crate::{Serve, Server};

/// How long a client may take to send a request's headers.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a request body may go without sending anything.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long shutdown waits for in-flight requests before dropping them
/// and checkpointing anyway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Where a server listens, as its discovery file reports it.
enum Endpoint {
    Tcp(std::net::SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl<S: Serve> Server<S> {
    /// Shut down, as on ctrl-c, once no request has arrived for `after`
    /// and no live stream is open: a server started on demand by a local
    /// app, that shouldn't outlive its use.
    pub fn idle_exit(mut self, after: Duration) -> Self {
        self.idle_exit = Some(after);
        self
    }

    /// While serving, keep a JSON file at `path` saying where:
    /// `{"pid", "url"}` (TCP) or `{"pid", "socket"}` (Unix), plus
    /// `"title"`, `"auth"` (whether a token is required) and the
    /// bog-http version. Written once listening, atomically; removed on
    /// shutdown. Advisory: a crash leaves it behind, so a client checks
    /// that the server answers before trusting it.
    pub fn discovery(mut self, path: impl Into<PathBuf>) -> Self {
        self.discovery = Some(path.into());
        self
    }

    /// Serve on `addr` until ctrl-c, SIGTERM, or [`idle_exit`], then shut
    /// down gracefully: end live streams, finish in-flight requests (for
    /// up to 5 s), remove the discovery file, and checkpoint, so the next
    /// open replays no WAL.
    ///
    /// A client gets 30 s to send its request's headers, and its body
    /// may go 30 s without sending anything; past either, the request is
    /// dropped, so a stalled client can't hold a connection for good.
    ///
    /// `layer` wraps the router first: CORS, extra routes, tracing. On a
    /// loopback address the server answers only to `localhost`,
    /// `127.0.0.1` and `[::1]` unless [`allowed_hosts`] said otherwise.
    /// Bind beyond loopback only with a [`bearer_token`].
    ///
    /// [`idle_exit`]: Server::idle_exit
    /// [`allowed_hosts`]: Server::allowed_hosts
    /// [`bearer_token`]: Server::bearer_token
    pub async fn serve(
        mut self,
        addr: impl tokio::net::ToSocketAddrs,
        layer: impl FnOnce(Router) -> Router,
    ) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        if self.guard.hosts.is_none() && local.ip().is_loopback() {
            self.guard.hosts = Some(crate::guard::loopback_hosts().into());
        }
        self.run(listener, Endpoint::Tcp(local), layer).await
    }

    /// Serve on a Unix socket at `path`, created owner-only (`0600`):
    /// for native apps on the same machine, with the filesystem as the
    /// access control. A socket file left by a server that's gone is
    /// replaced; one a server still answers on is an `AddrInUse` error,
    /// as is a path that isn't a socket. Removed on shutdown. Otherwise
    /// as [`serve`](Server::serve).
    #[cfg(unix)]
    pub async fn serve_unix(
        self,
        path: impl AsRef<Path>,
        layer: impl FnOnce(Router) -> Router,
    ) -> std::io::Result<()> {
        use std::os::unix::fs::{FileTypeExt, PermissionsExt};

        let path = path.as_ref().to_path_buf();
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if !meta.file_type().is_socket() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("{} exists and isn't a socket", path.display()),
                ));
            }
            if std::os::unix::net::UnixStream::connect(&path).is_ok() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("a server is already listening on {}", path.display()),
                ));
            }
            std::fs::remove_file(&path)?;
        }
        let listener = tokio::net::UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let served = self
            .run(listener, Endpoint::Unix(path.clone()), layer)
            .await;
        let _ = std::fs::remove_file(&path);
        served
    }

    async fn run<L>(
        self,
        listener: L,
        endpoint: Endpoint,
        layer: impl FnOnce(Router) -> Router,
    ) -> std::io::Result<()>
    where
        L: axum::serve::Listener,
        L::Addr: std::fmt::Debug,
    {
        let db = self.db.clone();
        let closer = self.closer();
        let activity = self.activity.clone();
        let idle_exit = self.idle_exit;
        let discovery = self.discovery.clone();
        let advert = advert(&endpoint, &self.title, self.guard.token.is_some());
        let app = layer(self.into_router()).layer(RequestBodyTimeoutLayer::new(BODY_READ_TIMEOUT));

        if let Some(path) = &discovery {
            write_atomically(path, &advert)?;
        }
        tracing::info!(target: "bog_http", endpoint = %advert["url"].as_str().or(advert["socket"].as_str()).unwrap_or("?"), "serving");

        let shutdown = async move {
            tokio::select! {
                _ = stop_signal() => tracing::info!(target: "bog_http", "stop signal: shutting down"),
                _ = idle(activity, idle_exit) => tracing::info!(target: "bog_http", "idle: shutting down"),
            }
            closer.close();
        };
        serve_until(listener, app, shutdown).await;

        if let Some(path) = &discovery {
            remove_if_ours(path);
        }
        tokio::task::spawn_blocking(move || db.checkpoint())
            .await
            .map_err(std::io::Error::other)?
            .map(|_| ())
            .map_err(std::io::Error::other)
    }
}

/// Serve connections until `shutdown`, then let in-flight requests
/// finish, for up to [`SHUTDOWN_GRACE`]; whatever is left is dropped.
///
/// Rather than `axum::serve`, which gives hyper no timer, and so no
/// header timeout, and waits on in-flight requests without limit.
async fn serve_until<L: axum::serve::Listener>(
    mut listener: L,
    app: Router,
    shutdown: impl Future<Output = ()>,
) {
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    let service = TowerToHyperService::new(app);
    let (stop, stopping) = tokio::sync::watch::channel(false);
    let mut conns = tokio::task::JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let (io, _) = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut shutdown => break,
        };
        while conns.try_join_next().is_some() {}
        let conn = http
            .serve_connection(TokioIo::new(io), service.clone())
            .with_upgrades();
        let mut stopping = stopping.clone();
        conns.spawn(async move {
            let mut conn = std::pin::pin!(conn);
            tokio::select! {
                _ = conn.as_mut() => return,
                _ = stopping.wait_for(|stop| *stop) => conn.as_mut().graceful_shutdown(),
            }
            let _ = conn.await;
        });
    }
    drop(listener);
    stop.send_replace(true);
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while conns.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(target: "bog_http", requests = conns.len(), "shutdown: dropping requests still in flight");
    }
}

fn advert(endpoint: &Endpoint, title: &str, auth: bool) -> serde_json::Value {
    let mut v = json!({
        "pid": std::process::id(),
        "title": title,
        "auth": auth,
        "bog_http": env!("CARGO_PKG_VERSION"),
    });
    match endpoint {
        Endpoint::Tcp(addr) => v["url"] = json!(format!("http://{addr}")),
        #[cfg(unix)]
        Endpoint::Unix(path) => v["socket"] = json!(path.display().to_string()),
    }
    v
}

/// Write via a temporary file and a rename, so a reader never sees half.
fn write_atomically(path: &Path, value: &serde_json::Value) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(
        &tmp,
        serde_json::to_vec_pretty(value).expect("JSON encodes"),
    )?;
    std::fs::rename(&tmp, path)
}

/// Remove the discovery file unless another server has since replaced it.
fn remove_if_ours(path: &Path) {
    let ours = std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .is_some_and(|v| v["pid"] == std::process::id());
    if ours {
        let _ = std::fs::remove_file(path);
    }
}

/// Resolve once nothing has happened for `after` (never, if `None`).
async fn idle(activity: std::sync::Arc<crate::server::Activity>, after: Option<Duration>) {
    let Some(after) = after else {
        return std::future::pending().await;
    };
    let check = (after / 4).clamp(Duration::from_millis(20), Duration::from_secs(1));
    loop {
        tokio::time::sleep(check).await;
        if activity.idle().is_some_and(|idle| idle >= after) {
            return;
        }
    }
}

/// Ctrl-c, or SIGTERM where there is one.
async fn stop_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
