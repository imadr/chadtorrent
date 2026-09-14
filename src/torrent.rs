use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use librqbit::{
    AddTorrent, AddTorrentOptions, DhtSessionConfig, ManagedTorrent, Session,
    SessionOptions, TorrentMetadata, TorrentStats,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct TorrentEngine {
    session: Arc<Session>,
    torrents: Arc<RwLock<std::collections::HashMap<i64, Arc<ManagedTorrent>>>>,
}

impl TorrentEngine {
    pub async fn new(download_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&download_dir)?;
        let opts = SessionOptions {
            // Keep DHT enabled for peer discovery but disable on-disk
            // persistence (we manage our own state in sqlite; DHT persistence
            // was causing startup failures and stale state).
            dht: Some(DhtSessionConfig {
                persistence: None,
                ..Default::default()
            }),
            // No session-level persistence / fastresume in v1.
            persistence: None,
            fastresume: false,
            ..Default::default()
        };
        let session = Session::new_with_opts(download_dir, opts)
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;
        Ok(Self {
            session,
            torrents: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    pub async fn add_torrent(
        &self,
        library_id: i64,
        magnet: &str,
        output_dir: &Path,
    ) -> Result<Arc<ManagedTorrent>> {
        self.add_torrent_with_opts(
            library_id,
            magnet,
            output_dir,
            AddTorrentOptions::default(),
        )
        .await
    }

    /// Add a torrent with custom options (used by tests to inject a local
    /// seeder peer, avoiding any internet dependency).
    pub async fn add_torrent_with_opts(
        &self,
        library_id: i64,
        magnet: &str,
        output_dir: &Path,
        mut opts: AddTorrentOptions,
    ) -> Result<Arc<ManagedTorrent>> {
        opts.output_folder = Some(output_dir.to_string_lossy().to_string());
        opts.overwrite = true;

        let response = self
            .session
            .clone()
            .add_torrent(
                AddTorrent::from_url(magnet.to_string()),
                Some(opts),
            )
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;

        let handle = response
            .into_handle()
            .ok_or_else(|| Error::Torrent("torrent was not added".into()))?;

        let id = handle.id();
        let name = handle.name().unwrap_or_else(|| "Unknown".to_string());
        info!(
            "Torrent added: name={}, id={}, output={:?}",
            name, id, output_dir
        );

        self.torrents
            .write()
            .await
            .insert(library_id, handle.clone());
        Ok(handle)
    }

    /// The TCP listen address of the session, for use by local seeders in tests.
    #[allow(dead_code)]
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.session.listen_addr()
    }

    pub async fn get_torrent(&self, library_id: i64) -> Option<Arc<ManagedTorrent>> {
        self.torrents.read().await.get(&library_id).cloned()
    }

    pub async fn remove_torrent(&self, library_id: i64, delete_files: bool) -> Result<()> {
        let handle = self.get_torrent(library_id).await;
        if let Some(handle) = handle {
            let id = handle.id();
            self.session
                .delete(librqbit::api::TorrentIdOrHash::Id(id), delete_files)
                .await
                .map_err(|e| Error::Torrent(e.to_string()))?;
            self.torrents.write().await.remove(&library_id);
            info!("Torrent {} removed from engine", id);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_stats(&self, library_id: i64) -> Option<TorrentStats> {
        let handle = self.get_torrent(library_id).await?;
        Some(handle.stats())
    }

    pub async fn get_metadata(&self, library_id: i64) -> Option<Arc<TorrentMetadata>> {
        let handle = self.get_torrent(library_id).await?;
        handle.with_metadata(|m| m.clone()).ok()
    }

    pub async fn pause_torrent(&self, library_id: i64) -> Result<()> {
        let handle = self
            .get_torrent(library_id)
            .await
            .ok_or_else(|| Error::NotFound(format!("torrent {library_id} is not active")))?;
        self.session
            .pause(&handle)
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;
        info!("Torrent {library_id} paused");
        Ok(())
    }

    pub async fn resume_torrent(&self, library_id: i64) -> Result<()> {
        let handle = self
            .get_torrent(library_id)
            .await
            .ok_or_else(|| Error::NotFound(format!("torrent {library_id} is not active")))?;
        self.session
            .clone()
            .unpause(&handle)
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;
        info!("Torrent {library_id} resumed");
        Ok(())
    }

    pub async fn find_largest_video_file(
        &self,
        library_id: i64,
    ) -> Option<(usize, String, u64)> {
        let handle = self.get_torrent(library_id).await?;
        let metadata = handle.with_metadata(|m| m.clone()).ok()?;
        let mut best: Option<(usize, String, u64)> = None;
        let mut best_size: u64 = 0;

        for (i, fi) in metadata.file_infos.iter().enumerate() {
            let name = fi.relative_filename.to_string_lossy().to_string();
            let lower = name.to_lowercase();
            let is_video = [
                ".mp4", ".m4v", ".mkv", ".avi", ".webm", ".mov", ".ts",
                ".flv", ".wmv", ".mpg", ".mpeg", ".vob",
            ]
            .iter()
            .any(|ext| lower.ends_with(ext));

            if is_video && fi.len > best_size {
                best_size = fi.len;
                best = Some((i, name.clone(), fi.len));
            }
        }

        best
    }

    #[allow(dead_code)]
    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub async fn stop(&self) {
        info!("Stopping torrent session...");
        self.session.stop().await;
        info!("Torrent session stopped");
    }
}