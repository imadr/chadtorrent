use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use librqbit::{
    api::TorrentIdOrHash, AddTorrent, AddTorrentOptions, AddTorrentResponse, Api, BitV, BitVFactory,
    DhtSessionConfig, ManagedTorrent, Session, SessionOptions, TorrentStats, TorrentStatsState, BF,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::db::Database;
use crate::error::{Error, Result};

/// librqbit piece-state store backed by SQLite. `load`/`store_initial_check`
/// are called by the engine during torrent initialization; the haves bitfield
/// survives restarts so downloads resume without re-checking every piece.
pub struct SqliteBitVFactory {
    db: Arc<Database>,
}

fn info_hash_of(id: TorrentIdOrHash) -> Option<String> {
    match id {
        TorrentIdOrHash::Hash(h) => Some(h.as_string()),
        TorrentIdOrHash::Id(_) => None,
    }
}

/// FNV-1a 64-bit over raw bytes (piece-map fingerprints / `.seg` dir names).
fn fnv1a_64(input: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in input {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[async_trait::async_trait]
impl BitVFactory for SqliteBitVFactory {
    async fn load(&self, id: TorrentIdOrHash) -> anyhow::Result<Option<Box<dyn BitV>>> {
        let Some(hash) = info_hash_of(id) else {
            return Ok(None);
        };
        match self.db.get_pieces(&hash) {
            Ok(Some(bytes)) => {
                let bf = BF::from_boxed_slice(bytes.into_boxed_slice());
                Ok(Some(Box::new(bf)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("sqlite error loading pieces: {e}")),
        }
    }

    async fn clear(&self, id: TorrentIdOrHash) -> anyhow::Result<()> {
        let Some(hash) = info_hash_of(id) else {
            return Ok(());
        };
        self.db
            .delete_pieces(&hash)
            .map_err(|e| anyhow::anyhow!("sqlite error clearing pieces: {e}"))
    }

    async fn store_initial_check(
        &self,
        id: TorrentIdOrHash,
        b: BF,
    ) -> anyhow::Result<Box<dyn BitV>> {
        if let Some(hash) = info_hash_of(id) {
            self.db
                .save_pieces(&hash, b.as_raw_slice())
                .map_err(|e| anyhow::anyhow!("sqlite error saving pieces: {e}"))?;
        }
        Ok(Box::new(b))
    }
}

#[derive(Clone)]
pub struct TorrentEngine {
    session: Arc<Session>,
    torrents: Arc<RwLock<std::collections::HashMap<i64, Arc<ManagedTorrent>>>>,
}

impl TorrentEngine {
    pub async fn new(download_dir: PathBuf, db: Arc<Database>) -> Result<Self> {
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
            // Piece progress lives in sqlite (see SqliteBitVFactory) instead
            // of librqbit's own fastresume files.
            bitv_factory: Some(Arc::new(SqliteBitVFactory { db })),
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

    /// Add a torrent with custom options (used by tests to inject a local
    /// seeder peer, avoiding any internet dependency), and by the library
    /// manager to apply per-torrent options like sequential download.
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

        self.attach_torrent(library_id, response).await
    }

    /// Add a torrent from its cached .torrent file bytes (no magnet
    /// resolution needed). Used to resume across restarts.
    pub async fn add_torrent_bytes_with_opts(
        &self,
        library_id: i64,
        torrent_bytes: bytes::Bytes,
        output_dir: &Path,
        mut opts: AddTorrentOptions,
    ) -> Result<Arc<ManagedTorrent>> {
        opts.output_folder = Some(output_dir.to_string_lossy().to_string());
        opts.overwrite = true;

        let response = self
            .session
            .clone()
            .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes), Some(opts))
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;

        self.attach_torrent(library_id, response).await
    }

    async fn attach_torrent(
        &self,
        library_id: i64,
        response: AddTorrentResponse,
    ) -> Result<Arc<ManagedTorrent>> {
        let handle = response
            .into_handle()
            .ok_or_else(|| Error::Torrent("torrent was not added".into()))?;

        let id = handle.id();
        let name = handle.name().unwrap_or_else(|| "Unknown".to_string());
        info!(
            "Torrent added: name={}, id={}, library_id={}",
            name, id, library_id
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

    /// Toggle sequential piece downloading for an active torrent. Ignores
    /// failures when the torrent is still initializing — the add-time option
    /// already covered that case.
    pub async fn set_sequential(&self, library_id: i64, sequential: bool) {
        if let Some(handle) = self.get_torrent(library_id).await {
            if let Err(e) = handle.set_sequential(sequential) {
                info!("set_sequential({library_id}, {sequential}) skipped: {e}");
            }
        }
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

    pub async fn piece_map(&self, library_id: i64) -> Result<Option<(Vec<bool>, u32)>> {
        let handle = match self.get_torrent(library_id).await {
            Some(h) => h,
            None => return Ok(None),
        };
        let api = Api::new(self.session.clone(), None);
        let (bf, total) = api
            .api_dump_haves(handle.id().into())
            .map_err(|e| Error::Torrent(e.to_string()))?;
        let raw = bf.as_raw_slice();
        let mut bits = vec![false; total as usize];
        for (i, slot) in bits.iter_mut().enumerate() {
            *slot = raw[i / 8] & (1 << (7 - (i % 8))) != 0;
        }
        Ok(Some((bits, total)))
    }

    /// The raw downloaded-pieces bitfield bytes for a torrent, keyed by its
    /// info hash. Used to persist progress to sqlite.
    pub async fn piece_raw(&self, library_id: i64) -> Option<(String, Vec<u8>)> {
        let handle = self.get_torrent(library_id).await?;
        let api = Api::new(self.session.clone(), None);
        let (bf, _total) = api.api_dump_haves(handle.id().into()).ok()?;
        Some((handle.info_hash().as_string(), bf.as_raw_slice().to_vec()))
    }

    /// A cheap 64-bit fingerprint of a torrent's current piece map. The
    /// progress watcher compares consecutive fingerprints to detect real
    /// download progress (and broadcast a notification) without carrying the
    /// whole bitfield around.
    pub async fn piece_fingerprint(&self, library_id: i64) -> Option<u64> {
        let handle = self.get_torrent(library_id).await?;
        let api = Api::new(self.session.clone(), None);
        let (bf, _total) = api.api_dump_haves(handle.id().into()).ok()?;
        Some(fnv1a_64(bf.as_raw_slice()))
    }

    /// Cached .torrent file bytes so a torrent can be re-added offline.
    pub async fn torrent_bytes(&self, library_id: i64) -> Option<bytes::Bytes> {
        let handle = self.get_torrent(library_id).await?;
        handle.with_metadata(|m| m.torrent_bytes.clone()).ok()
    }

    /// Library ids of every torrent currently registered with the engine.
    pub async fn active_ids(&self) -> Vec<i64> {
        self.torrents.read().await.keys().copied().collect()
    }

    /// The info hash of a registered torrent (hex) if we have one.
    pub async fn info_hash_of(&self, library_id: i64) -> Option<String> {
        let handle = self.get_torrent(library_id).await?;
        Some(handle.info_hash().as_string())
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

    /// Every file in the torrent as `(file_id, normalized relative path,
    /// length)`, from the engine's resolved metadata. `None` when the torrent
    /// isn't attached or its metadata isn't resolved yet.
    pub async fn list_files(&self, library_id: i64) -> Option<Vec<(usize, String, u64)>> {
        let handle = self.get_torrent(library_id).await?;
        let metadata = handle.with_metadata(|m| m.clone()).ok()?;
        Some(
            metadata
                .file_infos
                .iter()
                .enumerate()
                .map(|(i, fi)| {
                    (
                        i,
                        crate::episodes::normalize_rel_path(&fi.relative_filename.to_string_lossy()),
                        fi.len,
                    )
                })
                .collect(),
        )
    }

    /// Per-file downloaded bytes (indexed by file id), matching `list_files`.
    pub async fn stats_file_progress(&self, library_id: i64) -> Option<Vec<u64>> {
        let handle = self.get_torrent(library_id).await?;
        Some(handle.stats().file_progress)
    }

    /// The set of file ids to download: every file whose normalized relative
    /// path is not in `skipped_paths`. `None` when metadata isn't resolvable.
    pub async fn included_file_ids(
        &self,
        library_id: i64,
        skipped_paths: &HashSet<String>,
    ) -> Option<HashSet<usize>> {
        let files = self.list_files(library_id).await?;
        Some(
            files
                .into_iter()
                .filter(|(_, path, _)| !skipped_paths.contains(path))
                .map(|(id, _, _)| id)
                .collect(),
        )
    }

    /// Replace the torrent's per-file download selection. The delta pieces of
    /// unselected files stop being requested (already-downloaded pieces stay
    /// on disk); re-selecting a file resumes its remaining pieces. Fails while
    /// the torrent is still initializing — callers retry until metadata is
    /// resolved.
    pub async fn set_only_files(&self, library_id: i64, only: &HashSet<usize>) -> Result<()> {
        let handle = self
            .get_torrent(library_id)
            .await
            .ok_or_else(|| Error::NotFound(format!("torrent {library_id} is not active")))?;
        if matches!(
            handle.stats().state,
            TorrentStatsState::Initializing { .. }
        ) {
            return Err(Error::Torrent("torrent is still initializing".into()));
        }
        self.session
            .clone()
            .update_only_files(&handle, only)
            .await
            .map_err(|e| Error::Torrent(e.to_string()))?;
        Ok(())
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