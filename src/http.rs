use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tracing::info;
use tracing::warn;

use crate::db::LibraryItem;
use crate::error::{Error, Result};
use crate::library::{find_largest_video_in, LibraryManager};
use crate::metadata::TmdbClient;
use crate::remux::Remuxer;

#[derive(Deserialize)]
pub struct AddItemRequest {
    magnet: String,
    title: Option<String>,
    year: Option<i32>,
    poster_path: Option<String>,
    tmdb_id: Option<i64>,
    media_type: Option<String>,
    #[serde(default = "default_true")]
    sequential: bool,
}

#[derive(Deserialize)]
pub struct SetSequentialRequest {
    sequential: bool,
}

/// Query params for `GET /mse/{id}`: optional seek in seconds.
#[derive(Deserialize, Default)]
pub struct MseQuery {
    t: Option<f64>,
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
pub struct AddMovieRequest {
    title: String,
    year: Option<i32>,
    poster_path: Option<String>,
    tmdb_id: i64,
    media_type: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub library: Arc<LibraryManager>,
    pub index_html: String,
    pub tmdb: Option<Arc<TmdbClient>>,
}

pub fn build_router(state: AppState) -> Router {
    let root = state.index_html.clone();
    Router::new()
        .route("/", get(move || async move {
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                root.clone(),
            )
        }))
        .route("/api/library", post(add_item).get(list_items))
        .route("/api/movies", post(add_movie))
        .route("/api/search", get(search_tmdb))
        .route("/api/torrents", get(torrent_search))
        .route("/api/library/{id}", delete(delete_item))
        .route("/api/library/{id}/status", get(get_status))
        .route("/api/library/{id}/pieces", get(get_pieces))
        .route("/api/library/{id}/pause", post(pause_item))
        .route("/api/library/{id}/resume", post(resume_item))
        .route("/api/library/{id}/sequential", post(set_sequential))
        .route("/mse/{id}", get(mse_handler))
        .route("/api/library/{id}/codecs", get(mse_codecs))
        .route("/ws/torrent/{id}", get(ws_handler))
        .with_state(state)
}

async fn add_item(
    State(state): State<AppState>,
    Json(req): Json<AddItemRequest>,
) -> Result<impl IntoResponse> {
    if !req.magnet.starts_with("magnet:?") {
        return Err(Error::BadRequest("invalid magnet link".into()));
    }
    let id = state
        .library
        .add_item_with_sequential(&req.magnet, req.sequential)
        .await?;
    if req.title.is_some()
        || req.year.is_some()
        || req.poster_path.is_some()
        || req.tmdb_id.is_some()
    {
        state
            .library
            .db
            .update_metadata(
                id,
                req.title.as_deref(),
                req.year,
                req.poster_path.as_deref(),
                req.tmdb_id,
                req.media_type.as_deref(),
            )
            .map_err(|e| Error::Internal(e.to_string()))?;
    }
    Ok(Json(json!({ "id": id })))
}

async fn search_tmdb(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let query = params
        .get("q")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if query.is_empty() {
        return Ok(Json(json!([])));
    }
    let Some(tmdb) = state.tmdb.as_ref() else {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    };
    let results = tmdb
        .search(&query)
        .await
        .map_err(|e| Error::BadRequest(format!("search failed: {e}")))?;
    Ok(Json(serde_json::to_value(results).unwrap_or_else(|_| json!([]))))
}

async fn add_movie(
    State(state): State<AppState>,
    Json(req): Json<AddMovieRequest>,
) -> Result<impl IntoResponse> {
    if req.title.trim().is_empty() {
        return Err(Error::BadRequest("missing title".into()));
    }
    let id = state
        .library
        .add_movie(
            &req.title,
            req.year,
            req.poster_path.as_deref(),
            req.tmdb_id,
            req.media_type.as_deref().unwrap_or("movie"),
        )
        .await?;
    Ok(Json(json!({ "id": id })))
}

#[derive(Deserialize)]
struct ApibayItem {
    name: String,
    #[serde(rename = "info_hash")]
    info_hash: String,
    size: String,
    seeders: String,
    leechers: String,
}

async fn torrent_search(
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let query = params
        .get("q")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if query.is_empty() {
        return Err(Error::BadRequest("missing query param q (movie title)".into()));
    }
    let url = format!("https://apibay.org/q.php?q={}&cat=0", urlencoding(&query));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|e| Error::BadRequest(format!("torrent search failed: {e}")))?;

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut last_err: Option<reqwest::Error> = None;
    for attempt in 0..3 {
        let fetched: std::result::Result<Vec<ApibayItem>, reqwest::Error> = async {
            let resp = client.get(&url).send().await?.error_for_status()?;
            resp.json::<Vec<ApibayItem>>().await
        }
        .await;
        match fetched {
            Ok(items) => {
                results = items
                    .into_iter()
                    .filter_map(|it| {
                        let info_hash = it.info_hash.trim().to_string();
                        if info_hash.len() != 40 {
                            return None;
                        }
                        let magnet = format!(
                            "magnet:?xt=urn:btih:{info_hash}&dn={}",
                            urlencoding(&it.name)
                        );
                        Some(json!({
                            "name": it.name,
                            "size_bytes": it.size.parse::<u64>().ok(),
                            "seeders": it.seeders.parse::<u64>().ok().unwrap_or(0),
                            "leechers": it.leechers.parse::<u64>().ok().unwrap_or(0),
                            "magnet": magnet,
                        }))
                    })
                    .collect();
                if !results.is_empty() {
                    break;
                }
            }
            Err(e) => last_err = Some(e),
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
        }
    }
    if let Some(e) = last_err.as_ref() {
        warn!("all indexer attempts failed for '{query}': {e}");
    }
    if results.is_empty() && last_err.is_some() {
        return Err(Error::BadRequest(
            "torrent indexer is temporarily unreachable — try again".into(),
        ));
    }
    results.sort_by(|a, b| {
        b["seeders"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["seeders"].as_u64().unwrap_or(0))
    });
    Ok(Json(results))
}

fn urlencoding(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

async fn list_items(
    State(state): State<AppState>,
) -> Result<impl IntoResponse> {
    let items = state.library.list_items().await?;
    let with_flag = items
        .into_iter()
        .map(item_json_with_mse)
        .collect::<Vec<_>>();
    Ok(Json(with_flag))
}

/// Every playable item is served through the MSE endpoint (fragmented MP4
/// regardless of the source container). The flag is a convenience for the
/// frontend to pick the MSE player vs the plain `<video>` fallback.
fn item_json_with_mse(it: LibraryItem) -> serde_json::Value {
    let playable = matches!(
        it.state.as_str(),
        "ready" | "downloading" | "paused" | "stopped"
    );
    let mut v = serde_json::to_value(it).unwrap_or_else(|_| json!({}));
    v["mse"] = json!(playable);
    v
}

async fn delete_item(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let hard = params
        .get("hard")
        .and_then(|v| v.as_str())
        .map(|v| v == "1")
        .unwrap_or(false);
    if hard {
        state.library.remove_movie(id).await?;
    } else {
        state.library.remove_item(id).await?;
    }
    Ok(Json(json!({ "success": true })))
}

async fn get_status(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    let status = state.library.get_status(id).await?;
    Ok(Json(status))
}

async fn pause_item(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    state.library.pause_item(id).await?;
    Ok(Json(json!({ "success": true })))
}

async fn get_pieces(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    match state.library.get_pieces(id).await? {
        Some((have, total)) => Ok(Json(json!({ "total": total, "have": have }))),
        None => Ok(Json(json!({ "total": 0, "have": [] }))),
    }
}

async fn resume_item(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    state.library.resume_item(id).await?;
    Ok(Json(json!({ "success": true })))
}

async fn set_sequential(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<SetSequentialRequest>,
) -> Result<impl IntoResponse> {
    state.library.set_sequential(id, req.sequential).await?;
    Ok(Json(json!({ "success": true })))
}

/// Resolve the (file_id, size) of the largest video file inside a torrent
/// from its metadata. Mirrors `stream_handler`'s on-disk scan for the
/// engine-side stream.
fn best_video_file_id(
    handle: &librqbit::ManagedTorrent,
) -> Option<(usize, u64)> {
    let metadata = handle.with_metadata(|m| m.clone()).ok()?;
    let mut best: Option<(usize, u64)> = None;
    let mut best_size: u64 = 0;
    for (i, fi) in metadata.file_infos.iter().enumerate() {
        let name = fi.relative_filename.to_string_lossy();
        let ext = PathBuf::from(name.as_ref())
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let is_video = [
            "mp4", "m4v", "mkv", "avi", "webm", "mov", "ts",
            "flv", "wmv", "mpg", "mpeg", "vob",
        ]
        .contains(&ext.as_str());
        if is_video && fi.len > best_size {
            best_size = fi.len;
            best = Some((i, fi.len));
        }
    }
    best
}

/// Progressive streaming endpoint for the MSE blob player.
///
/// The server runs ffmpeg with `-c copy` (container only — it never decodes,
/// re-encodes, or reorders; the browser does all decoding). Output is
/// fragmented MP4 so the browser can `appendBuffer` it piece by piece.
///
/// Two backing sources:
///   - a finished file on disk (ffmpeg reads the path directly), or
///   - a still-downloading torrent's `FileStream` fed to ffmpeg's stdin
///     (ffmpeg consumes exactly the bytes that exist; when a piece isn't
///     downloaded yet nothing flows — there's no hang, no error, the client
///     just shows its buffering spinner until bytes arrive).
///
/// Buffering protocol is entirely driven by what ffmpeg is able to emit:
/// the moment the head of the file isn't available we return 503 with a short
/// retry window so the client reconnects after a moment instead of holding a
/// dead connection.
async fn mse_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<MseQuery>,
) -> Result<Response> {
    let item = state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

    // 1) Finished file on disk.
    if let Some(file_path) = item.file_path.as_ref() {
        if item.state == "ready" {
            let dir = PathBuf::from(file_path);
            if let Some(full) = find_largest_video_in(&dir) {
                return serve_mse_from_file(&full, params.t).await;
            }
        }
    }

    // 2) Live torrent stream.
    if let Some(handle) = state.library.engine.get_torrent(id).await {
        if let Some((file_id, _size)) = best_video_file_id(&handle) {
            // If even the first byte isn't there, say so immediately instead
            // of holding the connection: the client shows buffering and
            // retries in a moment.
            match handle.is_file_byte_ready(file_id, 0) {
                Ok(true) => {
                    return serve_mse_from_stream(handle, file_id, params.t).await;
                }
                Ok(false) => {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header(header::RETRY_AFTER, "2")
                        .body(Body::from("buffering"))
                        .unwrap());
                }
                Err(e) => {
                    warn!("is_file_byte_ready check failed: {e}");
                }
            }
        }
    }

    Err(Error::NotFound(format!(
        "no playable file found for item {id}"
    )))
}

/// Return the `SourceBuffer` MIME string the MSE player needs, e.g.
/// `video/mp4; codecs="avc1.640028,mp4a.40.2"`. Codec metadata comes from
/// ffprobe on the same source the `/mse` endpoint serves.
async fn mse_codecs(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    let item = state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

    // Finished file on disk.
    if let Some(file_path) = item.file_path.as_ref() {
        if item.state == "ready" {
            let dir = PathBuf::from(file_path);
            if let Some(full) = find_largest_video_in(&dir) {
                let mime = Remuxer::probe_mse_mime(&full.to_string_lossy()).await;
                let duration = Remuxer::probe_mse_duration(&full.to_string_lossy()).await;
                return Ok(Json(json!({ "mime": mime, "duration": duration })));
            }
        }
    }

    // Torrent stream: probe the on-disk partial file (may not have its head
    // yet — the client just retries; fallback MIME is a safe generic).
    if let Some(handle) = state.library.engine.get_torrent(id).await {
        if let Some((file_id, _size)) = best_video_file_id(&handle) {
            let name = handle
                .with_metadata(|m| {
                    m.file_infos
                        .get(file_id)
                        .map(|fi| fi.relative_filename.to_string_lossy().to_string())
                })
                .ok()
                .flatten();
            if let Some(fname) = name {
                let path = handle.output_folder().join(fname);
                if path.exists() {
                    let mime = Remuxer::probe_mse_mime(&path.to_string_lossy()).await;
                    let duration = Remuxer::probe_mse_duration(&path.to_string_lossy()).await;
                    return Ok(Json(json!({ "mime": mime, "duration": duration })));
                }
            }
        }
    }

    Ok(Json(json!({ "mime": "video/mp4; codecs=\"avc1.640028,mp4a.40.2\"", "duration": null })))
}

async fn serve_mse_from_file(path: &PathBuf, seek_sec: Option<f64>) -> Result<Response> {
    let input_str = path.to_string_lossy().to_string();
    let mut child = Remuxer::start_remux(&input_str, seek_sec)
        .await
        .map_err(|e| Error::Internal(format!("ffmpeg failed: {e}")))?;
    info!("MSE remux started for {:?}", path);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("no stdout from ffmpeg".into()))?;
    let task = RemuxTask { _child: child, reader: stdout };
    let stream = tokio_util::io::ReaderStream::with_capacity(task, 64 * 1024);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp4")
        .body(body)
        .unwrap())
}

/// Feed a live `FileStream` into ffmpeg's stdin so ffmpeg only ever sees the
/// bytes that actually exist. Attach the writer to the child so dropping the
/// body kills both the feed task and ffmpeg together.
async fn serve_mse_from_stream(
    handle: Arc<librqbit::ManagedTorrent>,
    file_id: usize,
    seek_sec: Option<f64>,
) -> Result<Response> {
    let name = handle.name().unwrap_or_else(|| "torrent".to_string());
    let mut child = Remuxer::start_remux_from_stream(&name, seek_sec)
        .await
        .map_err(|e| Error::Internal(format!("ffmpeg failed: {e}")))?;
    let writer = child
        .stdin
        .take()
        .ok_or_else(|| Error::Internal("no stdin on ffmpeg".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("no stdout from ffmpeg".into()))?;

    let mut file_stream = handle
        .clone()
        .stream(file_id)
        .await
        .map_err(|e| Error::Torrent(format!("failed to open stream: {e}")))?;

    // Copy FileStream -> ffmpeg stdin. FileStream returns Pending until the
    // requested bytes are downloaded, so ffmpeg naturally idles while a piece
    // is missing and resumes the moment it lands — never an error, never a hang.
    let feed = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut writer = writer;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            match file_stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if writer.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = writer.flush().await;
    });

    let task = StreamRemuxTask {
        _child: child,
        feed,
        reader: stdout,
    };
    let stream = tokio_util::io::ReaderStream::with_capacity(task, 64 * 1024);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp4")
        .body(body)
        .unwrap())
}

/// Wraps an ffmpeg child piped to a byte stream so both are dropped (and
/// ffmpeg killed) when the streaming body ends.
struct RemuxTask {
    _child: tokio::process::Child,
    reader: tokio::process::ChildStdout,
}

impl tokio::io::AsyncRead for RemuxTask {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

/// Like `RemuxTask` but also owns the stdin-feed task, so when the body is
/// dropped the feed is aborted and ffmpeg is killed.
struct StreamRemuxTask {
    _child: tokio::process::Child,
    feed: tokio::task::JoinHandle<()>,
    reader: tokio::process::ChildStdout,
}

impl Drop for StreamRemuxTask {
    fn drop(&mut self) {
        // Dropping a JoinHandle only detaches the task; the feed can be
        // stuck inside FileStream.read() (Pending) while holding a
        // BlockingSpawner semaphore permit.  Aborting the task cancels
        // it at the next await point, dropping the FileStream and
        // releasing the permit so the next `stream()` call can proceed.
        self.feed.abort();
    }
}

impl tokio::io::AsyncRead for StreamRemuxTask {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

// ---- WebSocket handler ----

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state, id))
}

async fn handle_ws(mut socket: WebSocket, state: AppState, id: i64) {
    info!("WS client connected for item {}", id);

    loop {
        let status = match state.library.get_status(id).await {
            Ok(s) => s,
            Err(e) => {
                let msg = json!({"type": "error", "message": e.to_string()});
                let _ = socket
                    .send(Message::Text(
                        serde_json::to_string(&msg)
                            .unwrap_or_default()
                            .into(),
                    ))
                    .await;
                break;
            }
        };

        let msg = serde_json::to_string(&status).unwrap_or_default();
        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }

        let tick = tokio::time::sleep(Duration::from_secs(1));
        let recv = socket.recv();
        tokio::select! {
            _ = tick => {},
            _ = recv => {},
        }
    }

    info!("WS client disconnected for item {}", id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_video_file_best_largest() {
        let dir = std::env::temp_dir()
            .join(format!("chadtorrent_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let small = dir.join("small.mp4");
        let large = dir.join("large.mp4");
        std::fs::write(&small, b"small").unwrap();
        std::fs::write(&large, std::vec![0u8; 1000]).unwrap();

        let result = find_largest_video_in(&dir);
        assert!(result.is_some());
        assert_eq!(result.unwrap().file_name().unwrap(), "large.mp4");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_find_video_file_none() {
        let dir = std::env::temp_dir()
            .join(format!("chadtorrent_test_none_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("notes.txt"), b"hello").unwrap();

        let result = find_largest_video_in(&dir);
        assert!(result.is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}