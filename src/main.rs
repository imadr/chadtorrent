mod config;
mod db;
mod error;
mod http;
mod library;
mod metadata;
mod party;
mod remux;
mod segmenter;
mod subtitles;
mod torrent;

use std::path::Path;
use std::sync::Arc;

use config::Config;
use http::AppState;
use library::LibraryManager;
use party::PartyManager;
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info")),
        )
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let config = Config::parse();
    info!(
        "ChadTorrent starting — port={}, library={}, temp={}, db={}",
        config.port,
        config.library_dir.display(),
        config.temp_dir.display(),
        config.db_path.display()
    );

    let library = match LibraryManager::new(config.clone()).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to initialize: {e}");
            std::process::exit(1);
        }
    };
    library.spawn_piece_flusher();

    let keys = match config::ApiKeys::load_or_create(Path::new("chadtorrent.json")) {
        Ok(k) => k,
        Err(e) => {
            error!("Failed to read chadtorrent.json: {e}");
            std::process::exit(1);
        }
    };
    let tmdb = if keys.tmdb_api_key.is_empty() {
        None
    } else {
        Some(Arc::new(metadata::TmdbClient::new(keys.tmdb_api_key.clone())))
    };
    let subs = if keys.opensubtitles_api_keys.is_empty() {
        None
    } else {
        Some(Arc::new(subtitles::SubtitlesClient::new(
            Path::new("chadtorrent.json").to_path_buf(),
            keys.clone(),
        )))
    };

    let index_html = include_str!("../static/index.html").to_string();

    let party = PartyManager::new(library.clone());

    let state = AppState {
        library: library.clone(),
        index_html,
        tmdb,
        subs,
        party,
    };

    let router = http::build_router(state);
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind {addr}: {e}");
            std::process::exit(1);
        }
    };

    // UDP punch hub on the same port (TCP+UDP are separate namespaces):
    // LAN watch parties coordinate through this instance directly.
    match tokio::net::UdpSocket::bind(&addr).await {
        Ok(udp) => {
            tokio::spawn(party::punch::run_punch_server(udp));
        }
        Err(e) => warn!("UDP punch hub bind failed ({e}) — LAN parties won't sync"),
    }

    info!("Listening on http://{addr}");

    tokio::select! {
        r = axum::serve(listener, router) => {
            if let Err(e) = r {
                error!("Server error: {e}");
            }
        }
        _ = shutdown_signal() => {
            info!("Shutdown signal received, cleaning up...");
            library.stop().await;
            info!("Shutdown complete");
        }
    }
}