use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tracing::info;

use crate::error::{Error, Result};
use crate::library::{find_largest_video_in, LibraryManager};
use crate::metadata::TmdbClient;
use crate::remux::Remuxer;

#[allow(dead_code)]
const RANGE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

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
        .route("/stream/{id}", get(stream_handler))
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
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| Error::BadRequest(format!("torrent search failed: {e}")))?
        .error_for_status()
        .map_err(|e| Error::BadRequest(format!("torrent search failed: {e}")))?;
    let items: Vec<ApibayItem> = resp
        .json()
        .await
        .map_err(|e| Error::BadRequest(format!("torrent search failed: {e}")))?;

    let mut results: Vec<serde_json::Value> = items
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
    Ok(Json(items))
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

fn parse_range(range: Option<&str>, file_size: u64) -> (u64, u64) {
    let Some(range) = range else {
        return (0, file_size.saturating_sub(1));
    };
    let range = range.trim();
    if let Some(suffix) = range.strip_prefix("bytes=") {
        if let Some((start_str, end_str)) = suffix.split_once('-') {
            if start_str.is_empty() {
                if let Ok(suffix_len) = end_str.trim().parse::<u64>() {
                    if suffix_len == 0 {
                        return (0, file_size.saturating_sub(1));
                    }
                    let start = file_size.saturating_sub(suffix_len);
                    return (start, file_size.saturating_sub(1));
                }
                return (0, file_size.saturating_sub(1));
            }
            if let Ok(start) = start_str.trim().parse::<u64>() {
                if start >= file_size {
                    return (0, file_size.saturating_sub(1));
                }
                let end = if end_str.is_empty() {
                    file_size.saturating_sub(1)
                } else {
                    end_str
                        .trim()
                        .parse::<u64>()
                        .map(|e| e.min(file_size.saturating_sub(1)))
                        .unwrap_or(file_size.saturating_sub(1))
                };
                return (start, end);
            }
        }
    }
    (0, file_size.saturating_sub(1))
}

fn mime_for(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("mp4")
        .to_lowercase()
        .as_str()
    {
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "avi" => "video/x-msvideo",
        "ts" => "video/mp2t",
        "flv" => "video/x-flv",
        "wmv" => "video/x-ms-wmv",
        _ => "video/mp4",
    }
}

async fn stream_handler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Result<Response> {
    let item = state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

    // 1) If kept and local file exists, serve from disk directly
    if let Some(file_path) = item.file_path.as_ref() {
        if item.state == "ready" {
            let pb = PathBuf::from(file_path);
            if let Some(full) = find_largest_video_in(&pb) {
                let needs_remux = Remuxer::needs_remux(&full.to_string_lossy())
                    || Remuxer::probe_needs_remux(&full.to_string_lossy())
                        .await;
                if needs_remux {
                    return serve_remux(&full, range_header).await;
                }
                return serve_local_file(&full, range_header).await;
            }
        }
    }

    // 2) Try streaming from torrent engine (partial or complete)
    if let Some(handle) = state.library.engine.get_torrent(id).await {
        let output_folder = handle.output_folder().to_path_buf();
        if let Some(full_path) = find_largest_video_in(&output_folder) {
            let needs_remux = Remuxer::needs_remux(&full_path.to_string_lossy())
                || Remuxer::probe_needs_remux(&full_path.to_string_lossy())
                    .await;

            if needs_remux {
                return serve_remux(&full_path, range_header).await;
            }

            // Try to find the file_id in metadata for FileStream streaming
            if let Some(metadata) = state.library.engine.get_metadata(id).await {
                let mut best_file_id: Option<usize> = None;
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
                        best_file_id = Some(i);
                    }
                }

                if let Some(file_id) = best_file_id {
                    return serve_from_torrent_stream(
                        handle.clone(),
                        file_id,
                        best_size,
                        range_header,
                    )
                    .await;
                }
            }

            // Fallback: serve from the file on disk with wait-and-retry
            return serve_file_with_wait(&full_path, range_header).await;
        }
    }

    Err(Error::NotFound(format!(
        "no playable file found for item {id}"
    )))
}

async fn serve_local_file(
    path: &PathBuf,
    range_header: Option<&str>,
) -> Result<Response> {
    let file = tokio::fs::File::open(path).await?;
    let size = file.metadata().await?.len();
    let (start, end) = parse_range(range_header, size);
    let content_range = format!("bytes {start}-{end}/{size}");
    let content_length = end - start + 1;
    let mut file = file;
    use tokio::io::AsyncSeekExt;
    if start > 0 {
        file.seek(std::io::SeekFrom::Start(start)).await?;
    }
    let stream = tokio_util::io::ReaderStream::with_capacity(file, 128 * 1024);
    let body = Body::from_stream(stream);
    let mime = mime_for(path);
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_RANGE, content_range)
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::CONTENT_TYPE, mime)
        .header(header::ACCEPT_RANGES, "bytes")
        .body(body)
        .unwrap())
}

async fn serve_remux(
    input_path: &PathBuf,
    _range_header: Option<&str>,
) -> Result<Response> {
    let input_str = input_path.to_string_lossy().to_string();
    let mut child = Remuxer::start_remux(&input_str)
        .await
        .map_err(|e| Error::Internal(format!("ffmpeg failed: {e}")))?;
    info!("Remux started for {:?}", input_path);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("no stdout from ffmpeg".into()))?;
    // Keep the child alive for as long as we're reading from its stdout;
    // dropping it early (client disconnect / stream end) kills ffmpeg.
    let task = RemuxTask { _child: child, reader: stdout };
    let stream = tokio_util::io::ReaderStream::with_capacity(task, 64 * 1024);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp4")
        .body(body)
        .unwrap())
}

/// Wraps an ffmpeg child plus its piped stdout so the two are dropped
/// (and ffmpeg killed) together when the streaming body is finished.
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

/// Stream from librqbit's FileStream. If the piece at the requested start
/// offset is not yet downloaded, returns 503 immediately so the client can
/// show a buffering indicator and retry — instead of blocking the connection
/// forever (which freezes the browser).
async fn serve_from_torrent_stream(
    handle: Arc<librqbit::ManagedTorrent>,
    file_id: usize,
    file_size: u64,
    range_header: Option<&str>,
) -> Result<Response> {
    let (start, end) = parse_range(range_header, file_size);

    // Quick check: is the first byte of the requested range available?
    // If not, return 503 immediately so the client can show "buffering"
    // and retry — never block the HTTP connection.
    match handle.is_file_byte_ready(file_id, start) {
        Ok(false) => {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(header::RETRY_AFTER, "2")
                .body(Body::from("buffering"))
                .unwrap());
        }
        Err(e) => {
            tracing::warn!("is_file_byte_ready check failed: {e}");
        }
        _ => {}
    }

    let content_length = end - start + 1;
    let content_range = format!("bytes {start}-{end}/{file_size}");

    let mut file_stream = handle
        .clone()
        .stream(file_id)
        .await
        .map_err(|e| Error::Torrent(format!("failed to open stream: {e}")))?;

    use tokio::io::AsyncSeekExt;
    file_stream.seek(std::io::SeekFrom::Start(start)).await?;

    let limited = tokio::io::AsyncReadExt::take(file_stream, content_length);
    let stream = tokio_util::io::ReaderStream::with_capacity(limited, 128 * 1024);
    let body = Body::from_stream(stream);
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_RANGE, content_range)
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::CONTENT_TYPE, "video/mp4")
        .header(header::ACCEPT_RANGES, "bytes")
        .body(body)
        .unwrap())
}

/// For files on disk from a torrent (may be growing), wait for the data to
/// become available, with a bounded number of attempts.
async fn serve_file_with_wait(
    path: &PathBuf,
    range_header: Option<&str>,
) -> Result<Response> {
    for _ in 0..60 {
        if let Ok(file) = tokio::fs::File::open(path).await {
            if let Ok(size) = file.metadata().await.map(|m| m.len()) {
                if size == 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                let (_start, end) = parse_range(range_header, size);
                if end < size {
                    return serve_local_file(path, range_header).await;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    serve_local_file(path, range_header).await
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
    use std::path::PathBuf;

    #[test]
    fn test_parse_range_no_range() {
        assert_eq!(parse_range(None, 1000), (0, 999));
    }

    #[test]
    fn test_parse_range_from_start() {
        assert_eq!(parse_range(Some("bytes=0-"), 1000), (0, 999));
    }

    #[test]
    fn test_parse_range_suffix() {
        assert_eq!(parse_range(Some("bytes=-500"), 1000), (500, 999));
    }

    #[test]
    fn test_parse_range_specific() {
        assert_eq!(parse_range(Some("bytes=100-199"), 1000), (100, 199));
    }

    #[test]
    fn test_parse_range_invalid() {
        assert_eq!(parse_range(Some("bytes=abc"), 1000), (0, 999));
    }

    #[test]
    fn test_parse_range_start_past_end() {
        assert_eq!(parse_range(Some("bytes=5000-"), 1000), (0, 999));
    }

    #[test]
    fn test_parse_range_zero_suffix() {
        assert_eq!(parse_range(Some("bytes=-0"), 1000), (0, 999));
    }

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

    #[test]
    fn test_mime_for() {
        assert_eq!(mime_for(&PathBuf::from("x.mp4")), "video/mp4");
        assert_eq!(mime_for(&PathBuf::from("x.mkv")), "video/x-matroska");
        assert_eq!(mime_for(&PathBuf::from("x.xyz")), "video/mp4");
    }
}