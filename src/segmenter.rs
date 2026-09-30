//! Segment pipeline: turns any playable source (finished file on disk or a
//! still-downloading torrent) into fragmented-MP4 *segments* on disk, so the
//! browser player can fetch them by index.
//!
//! One pipeline for every state:
//!   - finished file: ffmpeg reads the path once, all segments materialize in
//!     seconds and are cached in `library_dir/{id}/.seg/`.
//!   - downloading torrent: ffmpeg reads the librqbit `FileStream` (via stdin),
//!     which blocks exactly at the download frontier — segments appear as
//!     pieces arrive, and nothing is ever read that isn't downloaded.
//!
//! Nothing is re-encoded: `-c copy`, container-only. The raw ffmpeg output is
//! an interleaved fragmented-MP4 byte stream (ftyp+moov, then moof+mdat
//! fragments); this module splits that stream into `init.mp4` + `seg-N.m4s`
//! files by parsing the box structure, cutting a new segment every
//! ~SEG_TARGET_SECS at a fragment boundary (keyframe-aligned by
//! `frag_keyframe`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{info, warn};

use crate::db::Database;
use crate::library::find_largest_video_in;
use crate::torrent::TorrentEngine;

/// Target media duration per segment. Actual cuts happen at fragment
/// boundaries (keyframes with `frag_keyframe`), so real durations vary.
const SEG_TARGET_SECS: f64 = 4.0;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SegMeta {
    /// Segment number, starting at 1 (0 is the init segment).
    pub n: usize,
    /// Media time (seconds) of the segment's first frame.
    pub t: f64,
}

#[derive(Debug, Default, Clone)]
pub struct SegInfo {
    pub running: bool,
    pub done: bool,
    pub error: Option<String>,
    pub segments: Vec<SegMeta>,
    pub mime: Option<String>,
    pub duration: Option<f64>,
    /// On-disk path to probe for codec metadata (the partial file while
    /// streaming). Not part of the manifest JSON.
    pub probe_path: Option<PathBuf>,
    /// (mtime_secs, len) of the source file when this session started cutting.
    /// If the file has since changed (e.g. the torrent finished writing past
    /// where we cut), the segments are stale and must be re-cut.
    pub src_fingerprint: Option<(u64, u64)>,
}

pub struct Segmenter {
    pub id: i64,
    /// Which file of the torrent this segmenter cuts. `None` = the default
    /// (largest) video file — the one streamed with no `?file=`.
    pub file: Option<String>,
    pub dir: PathBuf,
    info: Mutex<SegInfo>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// (mtime_secs, len) of a file, used to detect that a source changed after
/// segments were cut from it.
fn file_fp(path: &Path) -> Option<(u64, u64)> {
    let md = std::fs::metadata(path).ok()?;
    let mtime = md
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some((mtime, md.len()))
}

impl Segmenter {
    fn new(id: i64, file: Option<String>, dir: PathBuf) -> Self {
        Self {
            id,
            file,
            dir,
            info: Mutex::new(SegInfo::default()),
            task: Mutex::new(None),
        }
    }

    pub fn snapshot(&self) -> SegInfo {
        self.info.lock().unwrap().clone()
    }

    pub fn set_probe(&self, mime: Option<String>, duration: Option<f64>) {
        let mut info = self.info.lock().unwrap();
        if info.mime.is_none() {
            info.mime = mime;
        }
        if info.duration.is_none() {
            info.duration = duration;
        }
    }

    fn set_error(&self, msg: String) {
        let mut info = self.info.lock().unwrap();
        info.running = false;
        info.error = Some(msg);
    }
}

enum Input {
    File(PathBuf),
    /// Live torrent: feed the FileStream of `file_id` through stdin; nothing
    /// is ever read that isn't already downloaded.
    Stream {
        handle: Arc<librqbit::ManagedTorrent>,
        file_id: usize,
    },
}

pub struct SegmenterManager {
    engine: Arc<TorrentEngine>,
    db: Arc<Database>,
    library_dir: PathBuf,
    /// Keyed by `(item id, file)` — separate pipelines per torrent file so TV
    /// episodes can each stream from their own `.seg` cache.
    map: AsyncMutex<SegMap>,
}

/// `(item id, file)` → running segmenter. `file` is the normalized relative
/// path of the torrent file being cut (`None` = the default/largest file).
type SegMap = HashMap<(i64, Option<String>), Arc<Segmenter>>;

/// FNV-1a 64-bit — deterministic, no crate deps. Used to derive a stable
/// per-file `.seg` subfolder so re-cutting never writes through a parent
/// cache and leftover dirs never accumulate.
fn fnv1a(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl SegmenterManager {
    pub fn new(library_dir: PathBuf, engine: Arc<TorrentEngine>, db: Arc<Database>) -> Self {
        Self {
            engine,
            db,
            library_dir,
            map: AsyncMutex::new(HashMap::new()),
        }
    }

    /// Get (creating if necessary) the segmenter for an item+file. Any `.seg`
    /// dir left over from a previous process run is stale (we don't know
    /// whether it was complete) and is wiped; re-segmentation is cheap.
    pub async fn ensure(&self, id: i64, file: Option<String>) -> Arc<Segmenter> {
        let key = (id, file.clone());
        let mut map = self.map.lock().await;
        if let Some(seg) = map.get(&key) {
            // If the source file changed since we last cut it (torrent resumed
            // mid-stream, piece reverification, re-download), the cached
            // segments no longer match the file: reset and re-cut.
            let fp_changed = {
                let info = seg.info.lock().unwrap();
                match (&info.src_fingerprint, &info.probe_path) {
                    (Some(fp), Some(path)) => file_fp(path).filter(|cur| cur == fp).is_none(),
                    _ => false,
                }
            };
            if fp_changed {
                warn!(
                    "source changed for item {} file {:?} — re-cutting segments",
                    id, file
                );
                if let Some(t) = seg.task.lock().unwrap().take() {
                    t.abort();
                }
                if let Err(e) = tokio::fs::remove_dir_all(&seg.dir).await {
                    warn!("failed to wipe stale segment dir {:?}: {e}", seg.dir);
                }
                if let Err(e) = tokio::fs::create_dir_all(&seg.dir).await {
                    warn!("failed to create segment dir {:?}: {e}", seg.dir);
                }
                *seg.info.lock().unwrap() = SegInfo::default();
                if let Some(input) = self.resolve_input(seg).await {
                    let seg2 = seg.clone();
                    let h = tokio::spawn(async move { run_segmenter(seg2, input).await });
                    *seg.task.lock().unwrap() = Some(h);
                }
                return seg.clone();
            }
            // The input may only become resolvable later (metadata still
            // downloading, torrent still registering) — retry on each poll
            // until a task is actually running or the run has concluded.
            let needs_start = {
                let info = seg.info.lock().unwrap();
                info.error.is_none() && !info.done && seg.task.lock().unwrap().is_none()
            };
            if needs_start {
                if let Some(input) = self.resolve_input(seg).await {
                    let seg2 = seg.clone();
                    let h = tokio::spawn(async move { run_segmenter(seg2, input).await });
                    *seg.task.lock().unwrap() = Some(h);
                }
            }
            return seg.clone();
        }
        let dir = self.seg_dir(id, file.as_deref());
        if dir.exists() {
            if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
                warn!("failed to wipe stale segment dir {:?}: {e}", dir);
            }
        }
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            warn!("failed to create segment dir {:?}: {e}", dir);
        }
        let seg = Arc::new(Segmenter::new(id, file, dir));
        map.insert(key, seg.clone());
        drop(map);

        if let Some(input) = self.resolve_input(&seg).await {
            let seg2 = seg.clone();
            let h = tokio::spawn(async move { run_segmenter(seg2, input).await });
            *seg.task.lock().unwrap() = Some(h);
        }
        seg
    }

    fn seg_root(&self, id: i64) -> PathBuf {
        self.library_dir.join(id.to_string()).join(".seg")
    }

    fn seg_dir(&self, id: i64, file: Option<&str>) -> PathBuf {
        match file {
            None => self.seg_root(id),
            Some(path) => self.seg_root(id).join(format!("{:016x}", fnv1a(path))),
        }
    }

    async fn resolve_input(&self, seg: &Arc<Segmenter>) -> Option<Input> {
        let item = self.db.get_item(seg.id).ok().flatten()?;

        // 1) File on disk: read the path directly.
        //    - no explicit file: largest video under `file_path` (a file, a
        //      directory, or stale/NULL) / canonical `library_dir/{id}`; heal
        //      the DB when found in the canonical spot.
        //    - explicit file: the exact relative path beneath the same base.
        //    Paused/stopped items must also use the file: their torrent's
        //    FileStream blocks at the piece frontier forever, which would
        //    wedge the segmenter into an eternal "buffering" with `done` never
        //    true.
        if matches!(item.state.as_str(), "ready" | "paused" | "stopped") {
            let base = item
                .file_path
                .as_ref()
                .map(PathBuf::from)
                .filter(|p| p.is_dir())
                .unwrap_or_else(|| self.library_dir.join(seg.id.to_string()));
            let candidate = match seg.file.as_deref() {
                Some(rel) => {
                    let full = base.join(rel);
                    full.exists().then_some(full)
                }
                None => find_largest_video_in(&base),
            };
            if let Some(full) = candidate {
                let canonical = self.library_dir.join(seg.id.to_string());
                if base == canonical
                    && item.file_path.as_deref() != Some(canonical.to_str().unwrap())
                {
                    if let Err(e) = self
                        .db
                        .update_file_path(seg.id, &canonical.to_string_lossy())
                    {
                        warn!("failed to heal stale path for item {}: {e}", seg.id);
                    }
                }
                let mut info = seg.info.lock().unwrap();
                info.running = true;
                info.probe_path = Some(full.clone());
                info.src_fingerprint = file_fp(&full);
                return Some(Input::File(full));
            }
        }

        // 2) Live torrent: feed the FileStream of the chosen file.
        if let Some(handle) = self.engine.get_torrent(seg.id).await {
            let chosen = match seg.file.as_deref() {
                Some(rel) => self
                    .engine
                    .list_files(seg.id)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .find(|(_, path, _)| {
                        path == rel || crate::episodes::normalize_rel_path(path) == rel
                    }),
                None => self
                    .engine
                    .find_largest_video_file(seg.id)
                    .await
                    .map(|(id, name, _)| (id, name, 0)),
            };
            if let Some((file_id, name, _size)) = chosen {
                let probe_path = handle.output_folder().join(&name);
                let mut info = seg.info.lock().unwrap();
                info.running = true;
                info.probe_path = Some(probe_path.clone());
                return Some(Input::Stream { handle, file_id });
            }
        }
        None
    }

    /// Existing segmenter for an item+file, if one was ever created this run.
    pub async fn get(&self, id: i64, file: Option<String>) -> Option<Arc<Segmenter>> {
        self.map.lock().await.get(&(id, file)).cloned()
    }

    /// Kill every segmenter pipeline for an item (called on item delete).
    pub async fn remove(&self, id: i64) {
        let mut map = self.map.lock().await;
        let keys: Vec<(i64, Option<String>)> =
            map.keys().filter(|(i, _)| *i == id).cloned().collect();
        for key in keys {
            if let Some(seg) = map.remove(&key) {
                if let Some(task) = seg.task.lock().unwrap().take() {
                    task.abort();
                }
            }
        }
    }

    pub async fn stop_all(&self) {
        let ids: Vec<i64> = self.map.lock().await.keys().map(|(i, _)| *i).collect();
        for id in ids {
            self.remove(id).await;
        }
    }
}

async fn run_segmenter(seg: Arc<Segmenter>, input: Input) {
    let kind = match &input {
        Input::File(p) => format!("file {:?}", p.display()),
        Input::Stream { .. } => "torrent stream".to_string(),
    };
    info!("starting segmenter for item {} ({})", seg.id, kind);
    let result = run_segmenter_inner(&seg, &input).await;
    if let Err(e) = result {
        warn!("segmenter for item {} failed: {e:#}", seg.id);
        seg.set_error(format!("{e:#}"));
    }
}

async fn run_segmenter_inner(seg: &Arc<Segmenter>, input: &Input) -> anyhow::Result<()> {
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error"]);
    match input {
        Input::File(path) => {
            cmd.arg("-i").arg(path);
        }
        Input::Stream { .. } => {
            cmd.arg("-i").arg("pipe:0");
        }
    }
    cmd.args([
        "-map", "0:v:0?",
        "-map", "0:a:0?",
        "-c", "copy",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "-f", "mp4",
        "pipe:1",
    ])
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .kill_on_drop(true);

    let mut child = cmd.spawn()?;
    // Surface ffmpeg errors instead of swallowing them: a failing remux must
    // never look like a silent stall.
    let stderr = child.stderr.take();
    let err_task = tokio::spawn(async move {
        let Some(mut stderr) = stderr else { return String::new() };
        let mut buf = vec![0u8; 4096];
        let mut collected = Vec::new();
        loop {
            match stderr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    collected.extend_from_slice(&buf[..n]);
                    if collected.len() > 8192 {
                        collected.drain(..collected.len() - 8192);
                    }
                }
            }
        }
        String::from_utf8_lossy(&collected).trim().to_string()
    });

    // For live torrents: feed the FileStream to ffmpeg's stdin. The FileStream
    // returns Pending until the requested bytes are downloaded, so ffmpeg
    // idles at the download frontier — nothing is ever read that isn't there.
    let mut feed: Option<tokio::task::JoinHandle<()>> = None;
    if let Input::Stream { handle, file_id } = input {
        let mut file_stream = handle
            .clone()
            .stream(*file_id)
            .await
            .map_err(|e| anyhow::anyhow!("failed to open torrent stream: {e}"))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("no stdin on ffmpeg"))?;
        feed = Some(tokio::spawn(async move {
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                match file_stream.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if stdin.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = stdin.flush().await;
        }));
    }

    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("no stdout from ffmpeg"))?;

    let mut splitter = SegmentSplitter::new(seg.clone())?;
    let mut buf = vec![0u8; 256 * 1024];
    let read_result: anyhow::Result<()> = loop {
        match stdout.read(&mut buf).await {
            Ok(0) => break Ok(()),
            Ok(n) => {
                if let Err(e) = splitter.feed(&buf[..n]).await {
                    break Err(e);
                }
            }
            Err(e) => break Err(e.into()),
        }
    };

    // If the body/segmenter is dropped mid-flight, the abort of this task
    // kills ffmpeg (kill_on_drop) and the feed task.
    if let Some(feed) = feed {
        feed.abort();
    }

    read_result?;
    let status = child
        .wait()
        .await
        .map_err(|e| anyhow::anyhow!("waiting for ffmpeg: {e}"))?;
    let ffmpeg_err = err_task.await.unwrap_or_default();
    if !status.success() && ffmpeg_err.is_empty() {
        anyhow::bail!("ffmpeg exited with {status}");
    }
    if !status.success() {
        anyhow::bail!("ffmpeg failed: {ffmpeg_err}");
    }
    splitter.finish().await?;
    let mut info = seg.info.lock().unwrap();
    info.running = false;
    info.done = true;
    info!(
        "segmenter for item {} done: {} segments",
        seg.id, info.segments.len()
    );
    Ok(())
}

/// Consumes the raw fMP4 byte stream and writes init + segment files.
struct SegmentSplitter {
    seg: Arc<Segmenter>,
    acc: BoxStream,
    /// Boxes not yet assigned to a file (sidx/free/prft between fragments).
    pending: Vec<u8>,
    /// Accumulated bytes of the segment currently being built.
    current: Vec<u8>,
    init: Vec<u8>,
    init_done: bool,
    /// baseMediaDecodeTime of the first fragment in `current` (in track
    /// timescale ticks).
    seg_start: Option<u64>,
    /// baseMediaDecodeTime of the most recent fragment.
    last_tfdt: u64,
    /// Presentation time (tfdt + first composition offset) of the most recent
    /// fragment — matches the browser's buffered timeline, unlike the raw tfdt.
    last_pts: u64,
    timescale: u32,
    /// The video track's id — tfdt values are per-track in the track's own
    /// timescale, so we must read the video traf's tfdt, not just any.
    video_track_id: u32,
    next_n: usize,
}

impl SegmentSplitter {
    fn new(seg: Arc<Segmenter>) -> anyhow::Result<Self> {
        Ok(Self {
            seg,
            acc: BoxStream::default(),
            pending: Vec::new(),
            current: Vec::new(),
            init: Vec::new(),
            init_done: false,
            seg_start: None,
            last_tfdt: 0,
            last_pts: 0,
            timescale: 0,
            video_track_id: 0,
            next_n: 1,
        })
    }

    async fn feed(&mut self, chunk: &[u8]) -> anyhow::Result<()> {
        self.acc.feed(chunk);
        while let Some(box_) = self.acc.next_box() {
            self.handle_box(box_).await?;
        }
        Ok(())
    }

    async fn handle_box(&mut self, box_: Box) -> anyhow::Result<()> {
        if !self.init_done {
            self.init.extend_from_slice(&box_.raw);
            if box_.typ == *b"moov" {
                self.init_done = true;
                if let Some((track_id, timescale)) = parse_video_track(&self.init) {
                    self.video_track_id = track_id;
                    self.timescale = timescale;
                }
                write_atomic(&self.seg.dir, "init.mp4", &self.init).await?;
                self.init.clear();
            }
            return Ok(());
        }

        match &box_.typ {
            b"moof" => {
                self.pending.extend_from_slice(&box_.raw);
                if let Some((tfdt, pts0)) = parse_moof_video_start(box_.body(), self.video_track_id)
                {
                    self.last_tfdt = tfdt;
                    self.last_pts = pts0;
                }
            }
            b"mdat" => {
                self.current.extend_from_slice(&self.pending);
                self.pending.clear();
                self.current.extend_from_slice(&box_.raw);
                if self.timescale > 0 {
                    // Segment boundaries in PRESENTATION time: the manifest's
                    // `t` must match where the browser actually buffers the
                    // media, or the pump's coverage check misfires.
                    let start = *self.seg_start.get_or_insert(self.last_pts);
                    let span = (self.last_pts.saturating_sub(start)) as f64 / self.timescale as f64;
                    if span >= SEG_TARGET_SECS {
                        self.close_segment().await?;
                    }
                }
            }
            // sidx and friends belong to the fragment that follows them.
            _ => self.pending.extend_from_slice(&box_.raw),
        }
        Ok(())
    }

    async fn close_segment(&mut self) -> anyhow::Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        let n = self.next_n;
        self.next_n += 1;
        let t = self.seg_start.take().unwrap_or(0) as f64 / self.timescale.max(1) as f64;
        let data = std::mem::take(&mut self.current);
        write_atomic(&self.seg.dir, &format!("seg-{n}.m4s"), &data).await?;
        let mut info = self.seg.info.lock().unwrap();
        info.segments.push(SegMeta { n, t });
        Ok(())
    }

    async fn finish(&mut self) -> anyhow::Result<()> {
        self.close_segment().await
    }
}

async fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!("{name}.part"));
    let final_ = dir.join(name);
    tokio::fs::write(&tmp, data).await?;
    tokio::fs::rename(&tmp, &final_).await
}

// ---------- minimal MP4 box parsing ----------

#[derive(Debug)]
struct Box {
    typ: [u8; 4],
    /// Full box bytes, header included.
    raw: Vec<u8>,
    /// Offset into `raw` where the payload starts.
    body: usize,
}

impl Box {
    fn body(&self) -> &[u8] {
        &self.raw[self.body..]
    }
}

#[derive(Default)]
struct BoxStream {
    buf: Vec<u8>,
    consumed: usize,
}

impl BoxStream {
    fn feed(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// Next complete top-level box, or None if more input is needed.
    fn next_box(&mut self) -> Option<Box> {
        loop {
            let avail = self.buf.len() - self.consumed;
            if avail < 8 {
                return None;
            }
            let h = &self.buf[self.consumed..self.consumed + 8];
            let mut hdr = 8usize;
            let mut size = u32::from_be_bytes([h[0], h[1], h[2], h[3]]) as u64;
            if size == 1 {
                if avail < 16 {
                    return None;
                }
                hdr = 16;
                size = u64::from_be_bytes(
                    self.buf[self.consumed + 8..self.consumed + 16]
                        .try_into()
                        .unwrap(),
                );
            } else if size == 0 {
                // "to end of file" — only meaningful at EOF; wait for more.
                return None;
            }
            if size < hdr as u64 {
                // Corrupt length; skip forward to make progress.
                self.consumed += 8;
                continue;
            }
            if (avail as u64) < size {
                return None;
            }
            let end = self.consumed + size as usize;
            let raw = self.buf[self.consumed..end].to_vec();
            let typ = [h[4], h[5], h[6], h[7]];
            self.consumed = end;
            if self.consumed > 4 * 1024 * 1024 {
                self.buf.drain(..self.consumed);
                self.consumed = 0;
            }
            return Some(Box { typ, raw, body: hdr });
        }
    }
}

/// Parse all complete child boxes of a container payload.
fn child_boxes(body: &[u8]) -> Vec<([u8; 4], &[u8])> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 8 <= body.len() {
        let size = u32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
            as usize;
        let mut hdr = 8;
        let mut size = size;
        if size == 1 {
            if pos + 16 > body.len() {
                break;
            }
            hdr = 16;
            size = u64::from_be_bytes(body[pos + 8..pos + 16].try_into().unwrap()) as usize;
        } else if size == 0 {
            size = body.len() - pos;
        }
        if size < hdr || pos + size > body.len() {
            break;
        }
        out.push((body[pos + 4..pos + 8].try_into().unwrap(), &body[pos..pos + size]));
        pos += size;
    }
    out
}

/// (tfdt, first-sample composition time) of the video track's first traf in a
/// moof. The segment's presentation-time start is tfdt + cts0: the raw tfdt is
/// the first DECODE time, which sits below the first presentation time by the
/// B-frame reordering delay (x265: ~0.2s). Recording the raw tfdt made the
/// manifest's segment times systematically earlier than the browser's buffered
/// timeline, so the client's coverage check (`timeCovered(t + 0.05)`) sat
/// inside the gap and re-appended the same fragment every poll — Chrome
/// ignores duplicate fragments, Firefox wedges.
fn parse_moof_video_start(moof_body: &[u8], track_id: u32) -> Option<(u64, u64)> {
    for (typ, raw) in child_boxes(moof_body) {
        if typ != *b"traf" {
            continue;
        }
        // strip the traf header to get its payload
        let hdr = if u32::from_be_bytes(raw[0..4].try_into().unwrap()) == 1 {
            16
        } else {
            8
        };
        let mut this_traf_id: Option<u32> = None;
        let mut tfdt: Option<u64> = None;
        let mut cts0: Option<i64> = None;
        for (t2, r2) in child_boxes(&raw[hdr..]) {
            match t2 {
                x if x == *b"tfhd" => {
                    let body = &r2[8..];
                    if body.len() >= 8 {
                        this_traf_id = Some(u32::from_be_bytes(body[4..8].try_into().unwrap()));
                    }
                }
                x if x == *b"tfdt" => {
                    let body = &r2[8..];
                    if body.len() < 8 {
                        continue;
                    }
                    let version = body[0];
                    tfdt = Some(if version == 1 {
                        if body.len() < 12 {
                            continue;
                        }
                        u64::from_be_bytes(body[4..12].try_into().unwrap())
                    } else {
                        u32::from_be_bytes(body[4..8].try_into().unwrap()) as u64
                    });
                }
                x if x == *b"trun" => {
                    if cts0.is_none() {
                        cts0 = parse_trun_first_cts(&r2[8..]);
                    }
                }
                _ => {}
            }
        }
        if this_traf_id == Some(track_id) {
            let tfdt = tfdt?;
            let pts = tfdt as i64 + cts0.unwrap_or(0);
            return Some((tfdt, pts.max(0) as u64));
        }
    }
    None
}

/// composition_time_offset of the FIRST sample in a trun (0 when the trun
/// carries no cts field). trun per-sample columns are [duration][size]
/// [flags][cts]; the first sample's cts is the first entry of the cts column.
fn parse_trun_first_cts(body: &[u8]) -> Option<i64> {
    if body.len() < 8 {
        return None;
    }
    let version = body[0] & 1;
    let flags = u32::from_be_bytes([0, body[1], body[2], body[3]]);
    let count = u32::from_be_bytes([body[4], body[5], body[6], body[7]]) as usize;
    let mut p = 8usize;
    if flags & 0x000001 != 0 {
        p += 4; // data_offset
    }
    if flags & 0x000004 != 0 {
        p += 4; // first_sample_flags
    }
    if flags & 0x000800 == 0 {
        return Some(0); // no per-sample cts field
    }
    // ffmpeg emits the per-sample table ROW-major (each sample's fields
    // interleaved, verified against real output), so sample 0's cts sits
    // right after sample 0's duration/size/flags — no sample-count stride.
    let mut off = p;
    for f in [0x000100u32, 0x000200, 0x000400] {
        if flags & f != 0 {
            off += 4;
        }
    }
    if off + 4 > body.len() {
        return None;
    }
    let raw = u32::from_be_bytes(body[off..off + 4].try_into().unwrap());
    Some(if version == 1 {
        raw as i32 as i64
    } else {
        raw as i64
    })
}

/// (track_id, timescale) of the video track (or the first track) from an
/// init ftyp+moov byte blob.
fn parse_video_track(init: &[u8]) -> Option<(u32, u32)> {
    let mut first: Option<(u32, u32)> = None;
    for (typ, raw) in top_boxes(init) {
        if typ != *b"moov" {
            continue;
        }
        for (t2, r2) in moov_children(raw) {
            if t2 != *b"trak" {
                continue;
            }
            let info = trak_info(r2);
            if first.is_none() {
                first = info;
            }
            if trak_handler(r2) == Some(*b"vide") {
                return info;
            }
        }
        return first;
    }
    None
}

/// (track_id, timescale) of a trak.
fn trak_info(trak_full: &[u8]) -> Option<(u32, u32)> {
    let mut track_id: Option<u32> = None;
    let mut timescale: Option<u32> = None;
    for (typ, raw) in trak_children(trak_full) {
        if typ == *b"tkhd" {
            let body = &raw[8..];
            if body.len() >= 24 {
                let version = body[0];
                track_id = Some(if version == 1 {
                    u32::from_be_bytes(body[20..24].try_into().unwrap())
                } else {
                    u32::from_be_bytes(body[12..16].try_into().unwrap())
                });
            }
        } else if typ == *b"mdia" {
            for (t2, r2) in child_boxes(&raw[8..]) {
                if t2 != *b"mdhd" {
                    continue;
                }
                let body = &r2[8..];
                if body.len() < 24 {
                    continue;
                }
                let version = body[0];
                timescale = Some(if version == 1 {
                    u32::from_be_bytes(body[20..24].try_into().unwrap())
                } else {
                    u32::from_be_bytes(body[12..16].try_into().unwrap())
                });
            }
        }
    }
    Some((track_id?, timescale?))
}

// Container walking helpers: each returns (typ, FULL box bytes) pairs.

fn top_boxes(data: &[u8]) -> Vec<([u8; 4], &[u8])> {
    child_boxes(data)
}

fn moov_children(moov_full: &[u8]) -> Vec<([u8; 4], &[u8])> {
    let hdr = moov_header_len(moov_full);
    child_boxes(&moov_full[hdr..])
}

fn moov_header_len(full: &[u8]) -> usize {
    let size = u32::from_be_bytes(full[0..4].try_into().unwrap()) as usize;
    if size == 1 {
        16
    } else {
        8
    }
}

fn trak_children(trak_full: &[u8]) -> Vec<([u8; 4], &[u8])> {
    moov_children(trak_full)
}

/// Returns the handler_type (e.g. `vide`, `soun`) of a trak, if present.
fn trak_handler(trak_full: &[u8]) -> Option<[u8; 4]> {
    for (typ, raw) in trak_children(trak_full) {
        if typ != *b"mdia" {
            continue;
        }
        for (t2, r2) in child_boxes(&raw[8..]) {
            if t2 == *b"hdlr" && r2.len() >= 8 + 12 {
                return Some(r2[8 + 8..8 + 12].try_into().unwrap());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn box_bytes(typ: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(8 + payload.len() as u32).to_be_bytes());
        v.extend_from_slice(typ);
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn box_stream_splits_across_chunks() {
        let mut s = BoxStream::default();
        let a = box_bytes(b"ftyp", b"1234");
        let b = box_bytes(b"free", &[0u8; 3]);
        let all = [a, b].concat();
        // feed one byte at a time
        let mut got = Vec::new();
        for byte in &all {
            s.feed(std::slice::from_ref(byte));
            while let Some(bx) = s.next_box() {
                got.push(bx.typ);
            }
        }
        assert_eq!(got, vec![*b"ftyp", *b"free"]);
    }

    #[test]
    fn tfdt_parse_by_track() {
        let mk = |track: u32, t: u32, cts: i32| {
            let mut tfhd = vec![0u8]; // version 0
            tfhd.extend_from_slice(&[0, 0, 0]); // flags
            tfhd.extend_from_slice(&track.to_be_bytes());
            let mut tfdt = vec![0u8]; // version 0
            tfdt.extend_from_slice(&[0, 0, 0]); // flags
            tfdt.extend_from_slice(&t.to_be_bytes());
            // trun v0, flags 0x000A01 (data_offset + size + cts), ROW-major:
            // sample0 = [size][cts], sample1 = [size][cts]
            let mut trun = vec![0u8];
            trun.extend_from_slice(&0x000A01u32.to_be_bytes()[1..]);
            trun.extend_from_slice(&2u32.to_be_bytes()); // 2 samples
            trun.extend_from_slice(&100i32.to_be_bytes()); // data_offset
            trun.extend_from_slice(&50u32.to_be_bytes()); // sample 0 size
            trun.extend_from_slice(&(cts as i32).to_be_bytes()); // sample 0 cts
            trun.extend_from_slice(&60u32.to_be_bytes()); // sample 1 size
            trun.extend_from_slice(&0u32.to_be_bytes()); // sample 1 cts
            box_bytes(
                b"traf",
                &[box_bytes(b"tfhd", &tfhd), box_bytes(b"tfdt", &tfdt), box_bytes(b"trun", &trun)]
                    .concat(),
            )
        };
        let moof = box_bytes(
            b"moof",
            &[mk(1, 48000, 3344), mk(2, 2600, 0)].concat(),
        );
        // segment start = first sample PRESENTATION time (tfdt + cts), not the
        // raw decode time — the manifest must match the browser's timeline.
        assert_eq!(parse_moof_video_start(&moof[8..], 1), Some((48000, 51344)));
        assert_eq!(parse_moof_video_start(&moof[8..], 2), Some((2600, 2600)));
        assert_eq!(parse_moof_video_start(&moof[8..], 3), None);
    }

    #[test]
    fn timescale_parse() {
        // build moov > trak > mdia > mdhd(version0, timescale=24000), hdlr(vide)
        let mut tkhd = vec![0u8]; // version 0
        tkhd.extend_from_slice(&[0, 0, 0]); // flags
        tkhd.extend_from_slice(&[0; 4]); // creation
        tkhd.extend_from_slice(&[0; 4]); // modification
        tkhd.extend_from_slice(&7u32.to_be_bytes()); // track id
        tkhd.extend_from_slice(&[0; 12]); // rest (duration/layer/etc.)
        let tkhd = box_bytes(b"tkhd", &tkhd);

        let mut mdhd = vec![0u8];
        mdhd.extend_from_slice(&[0, 0, 0]); // flags
        mdhd.extend_from_slice(&[0; 4]); // creation
        mdhd.extend_from_slice(&[0; 4]); // modification
        mdhd.extend_from_slice(&24000u32.to_be_bytes());
        mdhd.extend_from_slice(&[0; 4]); // duration
        mdhd.extend_from_slice(&[0; 2]); // language
        mdhd.extend_from_slice(&[0; 2]); // pre_defined
        let mdhd = box_bytes(b"mdhd", &mdhd);

        let mut hdlr = vec![0u8];
        hdlr.extend_from_slice(&[0, 0, 0]);
        hdlr.extend_from_slice(&[0; 4]);
        hdlr.extend_from_slice(b"vide");
        let hdlr = box_bytes(b"hdlr", &hdlr);

        let mdia = box_bytes(b"mdia", &[mdhd, hdlr].concat());
        let trak = box_bytes(b"trak", &[tkhd, mdia].concat());
        let moov = box_bytes(b"moov", &trak);
        let init = [box_bytes(b"ftyp", b"isom"), moov].concat();
        assert_eq!(parse_video_track(&init), Some((7, 24000)));
    }
}
