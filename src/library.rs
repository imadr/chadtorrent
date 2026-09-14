use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use librqbit::{ManagedTorrent, TorrentStatsState};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::config::Config;
use crate::db::{Database, LibraryItem, TorrentStatus};
use crate::error::{Error, Result};
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
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatusUpdate {
    pub library_id: i64,
    pub status: TorrentStatus,
}

impl LibraryManager {
    pub async fn new(config: Config) -> Result<Self> {
        std::fs::create_dir_all(&config.library_dir)?;
        std::fs::create_dir_all(&config.temp_dir)?;

        let db = Arc::new(Database::new(&config.db_path.to_string_lossy())?);
        let download_base = config.temp_dir.join("rqbit");
        let engine = Arc::new(TorrentEngine::new(download_base).await?);
        let (status_tx, _) = broadcast::channel(256);

        let manager = Self {
            db,
            engine,
            config,
            status_tx,
        };
        manager.reconcile_completed().await?;

        Ok(manager)
    }

    /// On startup, mark fully-downloaded items as ready even though the
    /// torrent engine has no handle for them anymore (engine state is not
    /// persisted across restarts). A download is considered complete when a
    /// video file on disk matches the recorded size of the largest video.
    async fn reconcile_completed(&self) -> Result<()> {
        for item in self.db.list_items()? {
            if item.state == "ready" {
                continue;
            }
            let dir = match item.file_path.as_ref() {
                Some(p) => PathBuf::from(p),
                None => {
                    let base = if item.keep {
                        &self.config.library_dir
                    } else {
                        &self.config.temp_dir
                    };
                    base.join(item.id.to_string())
                }
            };
            if !dir.exists() {
                continue;
            }
            let Some(expected) = item.size_bytes.filter(|s| *s > 0) else {
                continue;
            };
            let complete = find_largest_video_in(&dir)
                .and_then(|p| p.metadata().ok().map(|m| m.len() as i64 == expected))
                .unwrap_or(false);
            if complete {
                if item.file_path.is_none() {
                    let _ = self
                        .db
                        .update_file_path(item.id, &dir.to_string_lossy());
                }
                self.db.update_state(item.id, "ready")?;
                info!("Reconciled item {} as ready at {:?}", item.id, dir);
            }
        }
        Ok(())
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
        Ok(())
    }

    pub async fn add_item(&self, magnet: &str, keep: bool) -> Result<i64> {
        self.add_item_inner(magnet, keep, None).await
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

    /// Same as [`add_item`], but lets tests inject a local seeder peer so the
    /// download works without any internet/network dependency.
    #[allow(dead_code)]
    pub async fn add_item_with_initial_peers(
        &self,
        magnet: &str,
        keep: bool,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<i64> {
        self.add_item_inner(magnet, keep, Some(initial_peers)).await
    }

    async fn add_item_inner(
        &self,
        magnet: &str,
        keep: bool,
        initial_peers: Option<Vec<SocketAddr>>,
    ) -> Result<i64> {
        let library_id = self.db.insert_item(magnet, keep)?;
        let output_dir = if keep {
            self.config.library_dir.join(library_id.to_string())
        } else {
            self.config.temp_dir.join(library_id.to_string())
        };
        std::fs::create_dir_all(&output_dir)?;

        let handle = if let Some(peers) = initial_peers {
            let opts = librqbit::AddTorrentOptions {
                initial_peers: Some(peers),
                ..Default::default()
            };
            self.engine
                .add_torrent_with_opts(library_id, magnet, &output_dir, opts)
                .await
        } else {
            self.engine
                .add_torrent(library_id, magnet, &output_dir)
                .await
        };

        let handle = match handle {
            Ok(h) => h,
            Err(e) => {
                self.db.update_state(library_id, "error")?;
                return Err(e);
            }
        };

        let name = handle.name().unwrap_or_else(|| "Unknown".to_string());
        self.db.update_title(library_id, &name)?;
        self.db.update_state(library_id, "downloading")?;

        info!(
            "Library item added: id={}, title={}",
            library_id, name
        );

        if let Some((_file_id, file_name, size)) = self
            .engine
            .find_largest_video_file(library_id)
            .await
        {
            let title_with_ext = format!("{} / {}", name, file_name);
            self.db.update_title(library_id, &title_with_ext)?;
            self.db.update_size(library_id, size as i64)?;
        }

        if keep {
            let resolved = output_dir.to_string_lossy().to_string();
            self.db.update_file_path(library_id, &resolved)?;
        }

        Ok(library_id)
    }

    pub async fn set_keep(&self, id: i64, keep: bool) -> Result<()> {
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

        if item.keep == keep {
            return Ok(());
        }
        self.db.set_keep(id, keep)?;

        if keep {
            let permanent_dir = self.config.library_dir.join(id.to_string());
            std::fs::create_dir_all(&permanent_dir)?;

            // Current location: recorded file_path if it exists on disk,
            // otherwise the engine's output folder (still downloading).
            let mut current: Option<PathBuf> = None;
            if let Some(fp) = item.file_path.as_ref() {
                let p = PathBuf::from(fp);
                if p.exists() {
                    current = Some(p);
                }
            }
            if current.is_none() {
                if let Some(handle) = self.engine.get_torrent(id).await {
                    current = Some(handle.output_folder().to_path_buf());
                }
            }

            if let Some(current) = current {
                if current != permanent_dir {
                    info!(
                        "Promoting item {} from {:?} to {:?}",
                        id, current, permanent_dir
                    );
                    if let Err(e) = self.move_files(&current, &permanent_dir).await {
                        warn!("Failed to move files, continuing: {}", e);
                    }
                }
            }

            let _ = self
                .db
                .update_file_path(id, &permanent_dir.to_string_lossy());

            let complete = match self.engine.get_torrent(id).await {
                Some(handle) => handle.stats().finished,
                None => self.is_complete_on_disk(&permanent_dir, item.size_bytes),
            };
            if complete {
                self.db.update_state(id, "ready")?;
            }
        } else {
            info!("Item {} keep toggled off, leaving files as-is", id);
        }

        info!("Item {} keep set to {}", id, keep);
        Ok(())
    }

    pub async fn pause_item(&self, id: i64) -> Result<()> {
        self.engine.pause_torrent(id).await?;
        self.db.update_state(id, "paused")?;
        Ok(())
    }

    pub async fn resume_item(&self, id: i64) -> Result<()> {
        self.engine.resume_torrent(id).await?;
        self.db.update_state(id, "downloading")?;
        Ok(())
    }

    fn is_complete_on_disk(&self, dir: &Path, size_bytes: Option<i64>) -> bool {
        match size_bytes {
            Some(expected) if expected > 0 => find_largest_video_in(dir)
                .and_then(|p| p.metadata().ok().map(|m| m.len() as i64 == expected))
                .unwrap_or(false),
            _ => false,
        }
    }

    async fn move_files(&self, from: &Path, to: &Path) -> Result<()> {
        if from.exists() {
            for entry in std::fs::read_dir(from)? {
                let entry = entry?;
                let dest = to.join(entry.file_name());
                std::fs::rename(entry.path(), dest)?;
            }
        }
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
                keep: item.keep,
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
                    keep: item.keep,
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
                keep: item.keep,
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
            keep: item.keep,
            is_local: false,
        })
    }

    /// Remove a single torrent/file from the library. For a legacy item that
    /// doubles as the movie itself (no TMDB metadata), the row is kept as an
    /// empty movie placeholder so the movie is not lost from the library.
    pub async fn remove_item(&self, id: i64) -> Result<()> {
        self.remove_item_inner(id, false).await
    }

    /// Remove every row of a movie unconditionally (torrents, files and the
    /// placeholder movie itself). Used by "delete movie".
    pub async fn remove_movie(&self, id: i64) -> Result<()> {
        self.remove_item_inner(id, true).await
    }

    async fn remove_item_inner(&self, id: i64, hard: bool) -> Result<()> {
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

        let delete_files = !item.keep;
        if let Err(e) = self.engine.remove_torrent(id, delete_files).await {
            warn!("Failed to remove torrent {}: {}", id, e);
        }

        if !item.keep {
            let temp_path = self.config.temp_dir.join(id.to_string());
            if temp_path.exists() {
                if let Err(e) = std::fs::remove_dir_all(&temp_path) {
                    warn!("Failed to remove temp dir {:?}: {}", temp_path, e);
                }
            }
        }

        if item.tmdb_id.is_none() && !hard {
            self.db.reset_to_placeholder(id)?;
            info!("Library item {} kept as movie placeholder", id);
        } else {
            self.db.delete_item(id)?;
        }
        info!("Library item {} removed", id);
        Ok(())
    }

    pub async fn list_items(&self) -> Result<Vec<LibraryItem>> {
        self.db.list_items()
    }

    pub async fn stop(&self) {
        info!("Stopping library manager...");
        self.engine.stop().await;
        info!("Library manager stopped");
    }
}