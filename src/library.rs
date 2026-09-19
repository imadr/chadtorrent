use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use librqbit::{ManagedTorrent, TorrentStatsState};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::config::Config;
use crate::db::{Database, LibraryItem, TorrentStatus};
use crate::error::{Error, Result};
use crate::segmenter::SegmenterManager;
use crate::torrent::TorrentEngine;

/// Video file extensions we consider "playable".
pub(crate) const VIDEO_EXTS: &[&str] = &[
    "mp4", "m4v", "mkv", "avi", "webm", "mov", "ts", "flv", "wmv", "mpg", "mpeg", "vob",
];

/// Find the largest video file under `dir` (recursively, or the file itself
/// if given a file path). Returns `None` if no video file exists.
pub(crate) fn find_largest_video_in(dir: &Path) -> Option<PathBuf> {
    if dir.is_file() {
        return Some(dir.to_path_buf());
    }
    let mut best: Option<(PathBuf, u64)> = None;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&cur) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();
            if !VIDEO_EXTS.contains(&ext.as_str()) {
                continue;
            }
            if let Ok(meta) = path.metadata() {
                if best.as_ref().map(|(_, size)| meta.len() > *size).unwrap_or(true) {
                    best = Some((path, meta.len()));
                }
            }
        }
    }
    best.map(|(path, _)| path)
}

#[allow(dead_code)]
pub struct LibraryManager {
    pub db: Arc<Database>,
    pub engine: Arc<TorrentEngine>,
    pub config: Config,
    pub status_tx: broadcast::Sender<StatusUpdate>,
    pub segments: Arc<SegmenterManager>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusUpdate {
    pub library_id: i64,
    pub status: TorrentStatus,
}

impl LibraryManager {
    pub async fn new(config: Config) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.library_dir)?;
        std::fs::create_dir_all(&config.temp_dir)?;

        let db = Arc::new(Database::new(&config.db_path.to_string_lossy())?);
        let download_base = config.temp_dir.join("rqbit");
        let engine = Arc::new(TorrentEngine::new(download_base, db.clone()).await?);
        let (status_tx, _) = broadcast::channel(256);
        let segments = Arc::new(SegmenterManager::new(
            config.library_dir.clone(),
            engine.clone(),
            db.clone(),
        ));

        let manager = Arc::new(Self {
            db,
            engine,
            config,
            status_tx,
            segments,
        });
        manager.resume_existing_torrents().await?;

        Ok(manager)
    }

    /// On startup, re-attach every in-progress library item to the torrent
    /// engine (piece progress, cached .torrent bytes and paused/downloading
    /// state all come from sqlite). There is deliberately no completion
    /// heuristic: nothing here marks an item "ready" by inspecting file sizes.
    async fn resume_existing_torrents(self: &Arc<Self>) -> Result<()> {
        for item in self.db.list_items()? {
            if item.magnet.is_empty() || matches!(item.state.as_str(), "ready" | "error") {
                continue;
            }
            let output_dir = self.config.library_dir.join(item.id.to_string());
            let _ = std::fs::create_dir_all(&output_dir);
            let me = self.clone();
            tokio::spawn(async move {
                me.register_torrent(item.id, output_dir, None).await;
            });
        }
        Ok(())
    }

    /// Persist the downloaded-pieces bitfield for a torrent to sqlite.
    async fn store_pieces(&self, id: i64) {
        if let Some((hash, raw)) = self.engine.piece_raw(id).await {
            if let Err(e) = self.db.save_pieces(&hash, &raw) {
                warn!("Failed to save pieces for {}: {}", id, e);
            }
        }
    }

    pub async fn flush_all_pieces(&self) {
        for id in self.engine.active_ids().await {
            self.store_pieces(id).await;
        }
    }

    /// Periodically flush piece progress so it survives a crash, not just a
    /// clean shutdown.
    pub fn spawn_piece_flusher(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                me.flush_all_pieces().await;
            }
        });
    }

    /// Record that a torrent finished downloading: store its on-disk location
    /// (so it can be streamed even after the engine is restarted) and mark it
    /// ready.
    async fn mark_complete(&self, id: i64, handle: &Arc<ManagedTorrent>) -> Result<()> {
        if let Ok(Some(item)) = self.db.get_item(id) {
            if item.file_path.is_none() {
                let dir = handle.output_folder().to_path_buf();
                let _ = self
                    .db
                    .update_file_path(id, &dir.to_string_lossy());
            }
        }
        self.db.update_state(id, "ready")?;
        // Kick off background segmentation so the first play of a finished
        // item is instant. If a stream-mode segmenter is already running for
        // this item (user watched during download), it reaches EOF on its own
        // and `ensure` won't double-spawn.
        let segments = self.segments.clone();
        tokio::spawn(async move {
            segments.ensure(id).await;
        });
        Ok(())
    }

    /// Add a torrent and set its sequential-download flag for both the DB row
    /// and the registrered engine torrent.
    pub async fn add_item_with_sequential(
        self: &Arc<Self>,
        magnet: &str,
        sequential: bool,
    ) -> Result<i64> {
        self.add_item_inner(magnet, None, sequential).await
    }

    /// Add a movie placeholder entry (no torrent yet) so it appears in the
    /// library and can get torrents attached later.
    pub async fn add_movie(
        &self,
        title: &str,
        year: Option<i32>,
        poster_path: Option<&str>,
        tmdb_id: i64,
        media_type: &str,
    ) -> Result<i64> {
        self.db
            .insert_movie(title, year, poster_path, tmdb_id, media_type)
    }

    #[allow(dead_code)]
    pub async fn add_item_with_initial_peers(
        self: &Arc<Self>,
        magnet: &str,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<i64> {
        self.add_item_inner(magnet, Some(initial_peers), true).await
    }

    /// Create the library row immediately and hand torrent registration off
    /// to a background task, so the caller never blocks on metadata lookup.
    /// The UI shows an "adding…" row right away and the state becomes
    /// "downloading" once the engine resolves the torrent.
    async fn add_item_inner(
        self: &Arc<Self>,
        magnet: &str,
        initial_peers: Option<Vec<SocketAddr>>,
        sequential: bool,
    ) -> Result<i64> {
        let library_id = self.db.insert_item(magnet)?;
        let output_dir = self.config.library_dir.join(library_id.to_string());
        std::fs::create_dir_all(&output_dir)?;
        self.db.update_state(library_id, "connecting")?;
        if !sequential {
            self.db.set_sequential(library_id, false)?;
        }

        let me = self.clone();
        tokio::spawn(async move {
            me.register_torrent(library_id, output_dir, initial_peers)
                .await;
        });

        info!("Library item {} queued for registration", library_id);
        Ok(library_id)
    }

    async fn register_torrent(
        self: &Arc<Self>,
        library_id: i64,
        output_dir: PathBuf,
        initial_peers: Option<Vec<SocketAddr>>,
    ) {
        let Ok(Some(item)) = self.db.get_item(library_id) else {
            return;
        };
        if item.magnet.is_empty() {
            return;
        }
        // A paused item must stay paused across restarts; everything else
        // resumes downloading.
        let resume_paused = item.state == "paused";
        let opts = librqbit::AddTorrentOptions {
            initial_peers,
            paused: resume_paused,
            sequential: Some(item.sequential),
            ..Default::default()
        };

        // Prefer the cached .torrent bytes (no network needed), fall back to
        // the magnet for items that never resolved.
        let handle = match item.torrent_bytes.as_ref().filter(|b| !b.is_empty()) {
            Some(bytes) => {
                self.engine
                    .add_torrent_bytes_with_opts(
                        library_id,
                        bytes::Bytes::from(bytes.clone()),
                        &output_dir,
                        opts,
                    )
                    .await
            }
            None => {
                self.engine
                    .add_torrent_with_opts(library_id, &item.magnet, &output_dir, opts)
                    .await
            }
        };

        let handle = match handle {
            Ok(h) => h,
            Err(e) => {
                let _ = self.db.update_state(library_id, "error");
                warn!("Failed to register torrent {}: {}", library_id, e);
                return;
            }
        };

        let name = handle.name().unwrap_or_else(|| "Unknown".to_string());
        if let Err(e) = self.db.update_title(library_id, &name) {
            warn!("Failed to set title for {}: {}", library_id, e);
        }

        // Keep the resolved .torrent bytes so the next restart can re-attach
        // this torrent without re-resolving its magnet over the network.
        if let Some(tb) = self.engine.torrent_bytes(library_id).await {
            if let Err(e) = self.db.update_torrent_bytes(library_id, &tb) {
                warn!("Failed to store torrent bytes for {}: {}", library_id, e);
            }
        }

        if let Err(e) = self
            .db
            .update_state(library_id, if resume_paused { "paused" } else { "downloading" })
        {
            warn!("Failed to set state for {}: {}", library_id, e);
        }

        if let Some((_file_id, file_name, size)) = self
            .engine
            .find_largest_video_file(library_id)
            .await
        {
            let title_with_ext = format!("{} / {}", name, file_name);
            if let Err(e) = self.db.update_title(library_id, &title_with_ext) {
                warn!("Failed to set extended title for {}: {}", library_id, e);
            }
            if let Err(e) = self.db.update_size(library_id, size as i64) {
                warn!("Failed to set size for {}: {}", library_id, e);
            }
        }

        let resolved = output_dir.to_string_lossy().to_string();
        if let Err(e) = self.db.update_file_path(library_id, &resolved) {
            warn!("Failed to set file path for {}: {}", library_id, e);
        }
        info!("Library item {} registered", library_id);
    }

    pub async fn pause_item(&self, id: i64) -> Result<()> {
        self.engine.pause_torrent(id).await?;
        self.store_pieces(id).await;
        self.db.update_state(id, "paused")?;
        Ok(())
    }

    pub async fn resume_item(&self, id: i64) -> Result<()> {
        self.engine.resume_torrent(id).await?;
        self.db.update_state(id, "downloading")?;
        Ok(())
    }

    pub async fn get_pieces(&self, id: i64) -> Result<Option<(Vec<bool>, u32)>> {
        self.engine.piece_map(id).await
    }

    /// Toggle sequential piece downloading: persist the preference and apply
    /// it to the active engine torrent if one exists.
    pub async fn set_sequential(&self, id: i64, sequential: bool) -> Result<()> {
        self.db.set_sequential(id, sequential)?;
        self.engine.set_sequential(id, sequential).await;
        Ok(())
    }

    pub async fn get_status(&self, id: i64) -> Result<TorrentStatus> {
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

        let is_local = item.file_path.is_some()
            && item
                .file_path
                .as_ref()
                .map(|p| Path::new(p).exists())
                .unwrap_or(false)
            && item.state == "ready";

        if is_local {
            return Ok(TorrentStatus {
                id,
                state: "ready".into(),
                progress_pct: 100.0,
                download_speed_bps: 0,
                peers: 0,
                contiguous_bytes: item.size_bytes.unwrap_or(0) as u64,
                total_bytes: item.size_bytes.unwrap_or(0) as u64,
                is_local: true,
            });
        }

        if let Some(handle) = self.engine.get_torrent(id).await {
            let stats = handle.stats();
            let state_str = match &stats.state {
                TorrentStatsState::Initializing { .. } => "connecting",
                TorrentStatsState::Live => "downloading",
                TorrentStatsState::Paused => "paused",
                TorrentStatsState::Error => "error",
            };

            // Contiguous bytes report against the largest video file, which
            // is the one that gets streamed: `file_progress` is the bytes
            // downloaded for that file (librqbit downloads pieces in
            // sequential order, so this is a faithful contiguous-from-start
            // approximation for the streamed file).
            let (contiguous, total) = handle
                .with_metadata(|m| {
                    let mut best: Option<(usize, u64)> = None;
                    let video_exts = [
                        "mp4", "m4v", "mkv", "avi", "webm", "mov", "ts",
                        "flv", "wmv", "mpg", "mpeg", "vob",
                    ];
                    for (i, fi) in m.file_infos.iter().enumerate() {
                        let name = fi.relative_filename.to_string_lossy().to_lowercase();
                        let is_video = video_exts
                            .iter()
                            .any(|e| name.ends_with(e));
                        if is_video
                            && best
                                .map(|(_, size)| fi.len > size)
                                .unwrap_or(true)
                        {
                            best = Some((i, fi.len));
                        }
                    }
                    match best {
                        Some((file_id, size)) => {
                            let prog = stats
                                .file_progress
                                .get(file_id)
                                .copied()
                                .unwrap_or(stats.progress_bytes);
                            (prog, size)
                        }
                        None => (stats.progress_bytes, stats.total_bytes),
                    }
                })
                .unwrap_or((stats.progress_bytes, stats.total_bytes));

            let progress = if total > 0 {
                (contiguous as f64 / total as f64) * 100.0
            } else {
                0.0
            };

            if stats.finished {
                self.mark_complete(id, &handle).await?;
                return Ok(TorrentStatus {
                    id,
                    state: "ready".into(),
                    progress_pct: 100.0,
                    download_speed_bps: 0,
                    peers: 0,
                    contiguous_bytes: total,
                    total_bytes: total,
                    is_local: true,
                });
            }

            let download_speed = stats
                .live
                .as_ref()
                .map(|l| l.download_speed.as_bytes())
                .unwrap_or(0);

            let peer_count = stats
                .live
                .as_ref()
                .map(|l| {
                    l.snapshot.peer_stats.live + l.snapshot.peer_stats.connecting
                })
                .unwrap_or(0);

            if !stats.finished && state_str == "downloading" {
                self.db.update_state(id, "downloading")?;
            }
            if state_str == "error" {
                self.db.update_state(id, "error")?;
            }

            return Ok(TorrentStatus {
                id,
                state: state_str.into(),
                progress_pct: progress,
                download_speed_bps: download_speed,
                peers: peer_count,
                contiguous_bytes: contiguous,
                total_bytes: total,
                is_local: false,
            });
        }

        Ok(TorrentStatus {
            id,
            state: item.state,
            progress_pct: 0.0,
            download_speed_bps: 0,
            peers: 0,
            contiguous_bytes: 0,
            total_bytes: item.size_bytes.unwrap_or(0) as u64,
            is_local: false,
        })
    }

    /// Remove a single torrent: deletes the library row and every file on
    /// disk (engine output folder plus any legacy temp leftover).
    pub async fn remove_item(&self, id: i64) -> Result<()> {
        self.remove_item_inner(id, false).await
    }

    /// Remove a movie from the library: deletes the movie's placeholder row,
    /// every torrent attached to it (same tmdb_id) and all their files.
    pub async fn remove_movie(&self, id: i64) -> Result<()> {
        self.remove_item_inner(id, true).await
    }

    async fn remove_item_inner(&self, id: i64, hard: bool) -> Result<()> {
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

        // Drop persisted piece progress before the engine torrent handle is
        // removed (at which point we no longer know its info hash), and stop
        // any running segmenter before its output dir gets deleted.
        if let Some(hash) = self.engine.info_hash_of(id).await {
            if let Err(e) = self.db.delete_pieces(&hash) {
                warn!("Failed to drop pieces for {}: {}", id, e);
            }
        }
        self.segments.remove(id).await;

        self.remove_files_of(id).await;

        if hard {
            // Movie delete: cascade to every other row sharing the tmdb_id.
            if let Some(tmdb_id) = item.tmdb_id {
                let siblings: Vec<i64> = self
                    .db
                    .list_items()?
                    .into_iter()
                    .filter(|s| s.tmdb_id == Some(tmdb_id) && s.id != id)
                    .map(|s| s.id)
                    .collect();
                for sibling in siblings {
                    self.remove_files_of(sibling).await;
                    self.db.delete_item(sibling)?;
                }
            }
        }

        self.db.delete_item(id)?;
        info!("Library item {} removed", id);
        Ok(())
    }

    /// Delete every file for an item on disk: whatever the torrent engine
    /// has written plus any leftover in the library/temp dirs.
    async fn remove_files_of(&self, id: i64) {
        if let Err(e) = self.engine.remove_torrent(id, true).await {
            warn!("Failed to remove torrent {}: {}", id, e);
        }
        for base in [&self.config.library_dir, &self.config.temp_dir] {
            let dir = base.join(id.to_string());
            if dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(&dir) {
                    warn!("Failed to remove dir {:?}: {}", dir, e);
                }
            }
        }
    }

    pub async fn list_items(&self) -> Result<Vec<LibraryItem>> {
        self.db.list_items()
    }

    pub async fn stop(&self) {
        info!("Stopping library manager...");
        self.flush_all_pieces().await;
        self.segments.stop_all().await;
        self.engine.stop().await;
        info!("Library manager stopped");
    }
}