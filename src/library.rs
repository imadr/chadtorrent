use std::collections::HashSet;
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

pub(crate) fn is_video_path(path: &str) -> bool {
    let lower = path.to_lowercase();
    VIDEO_EXTS.iter().any(|ext| lower.ends_with(ext))
}

/// Sub-directories we never treat as content.
const IGNORED_DIRS: &[&str] = &[".seg", ".subs"];

/// Find the largest video file under `dir` (recursively, or the file itself
/// if given a file path). Returns `None` if no video file exists.
pub(crate) fn find_largest_video_in(dir: &Path) -> Option<PathBuf> {
    list_video_files_in(dir)
        .into_iter()
        .map(|(path, size)| (dir.join(path), size))
        .max_by_key(|(_, size)| *size)
        .map(|(path, _)| path)
}

/// Every video file under `dir` as `(relative path, size)`, `dir`-relative
/// and `/`-normalized. Skips internal `.seg`/`.subs` directories. Skips the
/// rendering-cache `segmenter` subfolder when `dir` is the item folder, too.
pub(crate) fn list_video_files_in(dir: &Path) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    if dir.is_file() {
        if let Ok(meta) = dir.metadata() {
            out.push((String::new(), meta.len()));
        }
        return out;
    }
    let mut stack = vec![(dir.to_path_buf(), String::new())];
    while let Some((cur, rel_prefix)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&cur) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = if rel_prefix.is_empty() {
                name.clone()
            } else {
                format!("{rel_prefix}/{name}")
            };
            if path.is_dir() {
                if IGNORED_DIRS.contains(&name.as_str()) {
                    continue;
                }
                stack.push((path, rel));
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
                out.push((rel, meta.len()));
            }
        }
    }
    out
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

/// One file of a torrent, as shown in the TV-show episode UI. `path` is the
/// normalized relative path inside the torrent (the stable key used by the
/// segmenter, skipped-file store and playhead store).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileEntry {
    pub path: String,
    pub name: String,
    pub size: u64,
    pub have: u64,
    pub video: bool,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub skipped: bool,
    pub playhead: f64,
    /// The torrent's default play file: the largest video file. Streaming
    /// with no `?file=` serves this one, and its playhead mirrors
    /// `library.playhead`.
    pub default: bool,
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
            segments.ensure(id, None).await;
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

        // Re-apply the persisted per-file selection (skipped episodes) once the
        // torrent is past initializing. `update_only_files` bails while
        // metadata is resolving; retry briefly so an early start doesn't waste
        // bandwidth on unselected files. Fresh adds have an empty skipped set.
        let skipped = match self.db.list_skipped_files(library_id) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to read skipped files for {}: {}", library_id, e);
                HashSet::new()
            }
        };
        if !skipped.is_empty() {
            let me = self.clone();
            tokio::spawn(async move {
                for _ in 0..40 {
                    // Resolve the included ids only once metadata is available,
                    // then hand the selection to the engine.
                    let applied = if let Some(only) =
                        me.engine.included_file_ids(library_id, &skipped).await
                    {
                        me.engine.set_only_files(library_id, &only).await.is_ok()
                    } else {
                        false
                    };
                    if applied {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                warn!("Gave up applying per-file selection for {library_id} after 10s");
            });
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

    /// The torrent-relative path of the item's canonical stream file (largest
    /// video). `None` when no video file is known yet.
    pub async fn default_video_path(&self, id: i64) -> Option<String> {
        let item = self.db.get_item(id).ok().flatten()?;
        // live torrent metadata first, then the on-disk layout
        if self.engine.get_torrent(id).await.is_some() {
            let files = self.engine.list_files(id).await?;
            let mut best: Option<(String, u64)> = None;
            for (_, path, size) in files {
                let is_video = is_video_path(&path);
                if is_video && best.as_ref().map(|(_, s)| size > *s).unwrap_or(true) {
                    best = Some((path, size));
                }
            }
            return best.map(|(path, _)| path);
        }
        let dir = self.item_dir(&item);
        list_video_files_in(&dir)
            .into_iter()
            .max_by_key(|(_, size)| *size)
            .map(|(path, _)| path)
    }

    fn item_dir(&self, item: &crate::db::LibraryItem) -> PathBuf {
        item.file_path
            .as_ref()
            .map(PathBuf::from)
            .filter(|p| p.is_dir())
            .unwrap_or_else(|| self.config.library_dir.join(item.id.to_string()))
    }

    /// Every (video) file of an item with its season/episode mapping, have
    /// bytes, skipped flag and saved playhead. Sources priority:
    ///   - ready items: what's actually on disk (selected files are complete;
    ///     skipped files may be missing or partial)
    ///   - live torrents: engine metadata + per-file progress
    ///   - paused/stopped: on-disk layout (partial files)
    ///
    /// Anything else returns an empty list. Non-video files from live torrent
    /// metadata are included (flagged `video: false`) so skipped-file state
    /// covers them too, but the frontend shows only video entries.
    pub async fn list_item_files(&self, id: i64) -> Result<Vec<FileEntry>> {
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
        let skipped = self.db.list_skipped_files(id)?;
        let playheads: std::collections::HashMap<String, f64> =
            self.db.list_playheads(id)?.into_iter().collect();

        let is_local_ready = item.state == "ready";
        let has_handle = self.engine.get_torrent(id).await.is_some();

        let mut entries: Vec<(String, u64, u64)> = Vec::new();
        if is_local_ready {
            let dir = self.item_dir(&item);
            for (path, size) in list_video_files_in(&dir) {
                let have = std::fs::metadata(dir.join(&path))
                    .map(|m| m.len())
                    .unwrap_or(0);
                entries.push((path, size, have));
            }
        } else if has_handle {
            let files = self.engine.list_files(id).await.unwrap_or_default();
            let progress = self.engine.stats_file_progress(id).await.unwrap_or_default();
            for (file_id, path, size) in files {
                let have = progress.get(file_id).copied().unwrap_or(0);
                entries.push((path, size, have));
            }
        } else if matches!(item.state.as_str(), "paused" | "stopped") {
            let dir = self.item_dir(&item);
            for (path, size) in list_video_files_in(&dir) {
                let have = std::fs::metadata(dir.join(&path))
                    .map(|m| m.len())
                    .unwrap_or(0);
                entries.push((path, size, have));
            }
        }

        // The default stream file: the largest video in the raw list. Every source
        // (disk or engine metadata) is already in `entries`, so this covers
        // ready, paused/stopped and live torrents.
        let default_path = entries
            .iter()
            .filter(|(path, _, _)| is_video_path(path))
            .max_by_key(|(_, size, _)| *size)
            .map(|(path, _, _)| path.clone());

        let mut out = Vec::with_capacity(entries.len());
        for (path, size, have) in entries {
            let video = is_video_path(&path);
            let (season, episode) = if video {
                match crate::episodes::parse_episode_path(&path) {
                    Some((s, e)) => (Some(s), Some(e)),
                    None => (None, None),
                }
            } else {
                (None, None)
            };
            let default = default_path
                .as_deref()
                .map(|d| d == path)
                .unwrap_or(false);
            let displayed_have = if is_local_ready && !skipped.contains(&path) {
                // a fully-selected ready file is complete on disk
                std::fs::metadata(
                    self.item_dir(&item)
                        .join(&path),
                )
                .map(|m| m.len())
                .unwrap_or(have)
            } else {
                have
            };
            let name = path.rsplit('/').next().unwrap_or(&path).to_string();
            let playhead = playheads
                .get(&path)
                .copied()
                .unwrap_or(if default { item.playhead } else { 0.0 });
            out.push(FileEntry {
                path: path.clone(),
                name,
                size,
                have: displayed_have,
                video,
                season,
                episode,
                skipped: skipped.contains(&path),
                playhead,
                default,
            });
        }
        // When no default could be pinned (no metadata, nothing on disk),
        // fall back to the biggest video entry so the UI still marks one.
        if default_path.is_none() {
            if let Some(entry) = out.iter_mut().filter(|e| e.video).max_by_key(|e| e.size) {
                entry.default = true;
                if entry.playhead == 0.0 {
                    entry.playhead = item.playhead;
                }
            }
        }
        out.sort_by(|a, b| match (a.season, b.season) {
            (Some(x), Some(y)) => x
                .cmp(&y)
                .then(a.episode.cmp(&b.episode)),
            (None, None) => a.path.cmp(&b.path),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
        });
        Ok(out)
    }

    /// Mark one torrent file as selected (downloaded) or skipped, persist the
    /// choice and apply it to the running torrent. Re-enabling a file on an
    /// item with no live engine (e.g. a finished torrent after a restart)
    /// re-registers the torrent so the new file actually starts downloading.
    pub async fn set_file_selected(
        self: &Arc<Self>,
        id: i64,
        path: &str,
        selected: bool,
    ) -> Result<()> {
        let path = crate::episodes::normalize_rel_path(path);
        let item = self
            .db
            .get_item(id)?
            .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
        if item.magnet.is_empty() {
            return Err(Error::BadRequest("item has no torrent".into()));
        }

        self.db.set_file_skipped(id, &path, !selected)?;
        let skipped = self.db.list_skipped_files(id)?;

        if self.engine.get_torrent(id).await.is_some() {
            if let Some(only) = self.engine.included_file_ids(id, &skipped).await {
                match self.engine.set_only_files(id, &only).await {
                    // still initializing: the register loop applies it shortly
                    Err(e) if e.to_string().contains("initializing") => {}
                    Err(e) => warn!("set_only_files({id}) failed: {e}"),
                    Ok(()) => {}
                }
            }
            return Ok(());
        }

        // No live torrent. If we are enabling a file, kick off a re-add so the
        // episode actually downloads (its pieces were never fetched).
        if selected {
            let output_dir = self.config.library_dir.join(id.to_string());
            let _ = std::fs::create_dir_all(&output_dir);
            let me = self.clone();
            tokio::spawn(async move {
                me.register_torrent(id, output_dir, None).await;
            });
        }
        Ok(())
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
        if let Err(e) = self.db.clear_item_files(id) {
            warn!("Failed to clear file selection for {}: {}", id, e);
        }

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
                    if let Err(e) = self.db.clear_item_files(sibling) {
                        warn!("Failed to clear file selection for {}: {}", sibling, e);
                    }
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