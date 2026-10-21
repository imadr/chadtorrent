use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
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
use crate::library::LibraryManager;
use crate::metadata::{MetaCache, TmdbClient, TmdbMeta};
use crate::remux::Remuxer;
use crate::subtitles::{self, SubtitlesClient};

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

#[derive(Deserialize)]
pub struct SetPlayheadRequest {
    t: f64,
    /// Per-file playback position. Absent = the torrent's default file
    /// (mirrored to the legacy `library.playhead`).
    file: Option<String>,
}

#[derive(Deserialize)]
pub struct SetFileSelectedRequest {
    /// Normalized relative path inside the torrent.
    file: String,
    selected: bool,
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
    /// Synopsis stored on the row itself. Reading items (manga/comics) show
    /// this directly on their detail page; movie/show rows keep it as a
    /// fallback while the TMDB cache is cold.
    overview: Option<String>,
    /// The reading catalogue's native key (MangaDex UUID for manga rows).
    ext_key: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub library: Arc<LibraryManager>,
    pub index_html: String,
    pub tmdb: Option<Arc<TmdbClient>>,
    /// `tmdb_meta`-backed cache in front of `tmdb`. Present only when a TMDB
    /// key is configured.
    pub meta: Option<Arc<MetaCache>>,
    pub subs: Option<Arc<SubtitlesClient>>,
    pub party: Arc<crate::party::PartyManager>,
    /// Reading catalogues (manga: MangaDex keyless; comics: Comic Vine). Always
    /// present — the comics side just reports "not configured" without a key.
    pub reading: Arc<crate::reading::ReadingClient>,
    /// Reading sources: resolves bound series → chapter page images
    /// (MangaDex, Weeb Central) and proxies the bytes.
    pub reader: Arc<crate::reader::ReaderClient>,
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
        .route("/api/search/manga", get(search_manga))
        .route("/api/search/comics", get(search_comics))
        .route("/api/meta/{media_type}/{tmdb_id}", get(meta_detail))
        .route("/api/meta/{media_type}/{tmdb_id}/refresh", post(meta_refresh))
        .route("/api/meta/tv/{tmdb_id}/season/{season_number}", get(meta_episodes))
        .route("/api/torrents", get(torrent_search))
        .route("/api/library/{id}", delete(delete_item))
        .route("/api/library/{id}/status", get(get_status))
        .route("/api/library/{id}/pieces", get(get_pieces))
        .route("/api/library/{id}/pause", post(pause_item))
        .route("/api/library/{id}/resume", post(resume_item))
        .route("/api/library/{id}/sequential", post(set_sequential))
        .route("/api/library/{id}/files", get(files_list))
        .route("/api/library/{id}/file", post(set_file_selected))
        .route("/api/library/{id}/playhead", post(set_playhead))
        .route("/api/library/{id}/stream", get(stream_info))
        .route("/api/library/{id}/read/source", get(read_source_get).put(read_source_put))
        .route("/api/library/{id}/read/source/{source}/search", get(read_source_search))
        .route("/api/library/{id}/read/progress", get(read_progress))
        .route("/api/library/{id}/read/chapter/{label}/progress", post(read_chapter_progress))
        .route("/api/library/{id}/read/chapter/{label}", get(read_chapter))
        .route("/api/library/{id}/read/chapter/{label}/page/{n}", get(read_chapter_page))
        .route("/api/library/{id}/subs", get(subs_list))
        .route("/api/library/{id}/subs/search", get(subs_search))
        .route("/api/library/{id}/subs/download", post(subs_download))
        .route("/api/library/{id}/subs/{name}", delete(subs_delete))
        .route("/sub/{id}/{name}", get(sub_file))
        .route("/mse/{id}/init", get(seg_init))
        .route("/mse/{id}/seg/{n}", get(seg_n))
        .route("/ws/torrent/{id}", get(ws_handler))
        .route("/ws/library", get(library_ws_handler))
        .route("/api/party", post(create_party).get(get_party))
        .route("/api/party/join", post(join_party))
        .route("/api/party/leave", post(leave_party))
        .route("/ws/party", get(party_ws_handler))
        .fallback(get(spa_fallback))
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
        .add_item_with_title(&req.magnet, req.title.as_deref(), req.sequential)
        .await?;
    if req.year.is_some()
        || req.poster_path.is_some()
        || req.tmdb_id.is_some()
        || req.media_type.is_some()
    {
        state
            .library
            .db
            .update_metadata(
                id,
                None,
                req.year,
                req.poster_path.as_deref(),
                req.tmdb_id,
                req.media_type.as_deref(),
            )
            .map_err(|e| Error::Internal(e.to_string()))?;
    }
    Ok(Json(json!({ "id": id })))
}

/// TMDB detail for a library item's head section: the overview (description)
/// paragraph, the canonical TV-season list and every season's episode list —
/// all served from the `tmdb_meta` cache so opening a show paints its season
/// and episode buttons immediately instead of flickering them in per click.
///
/// A stale cache is returned as-is and re-checked in the background; only a
/// completely cold show blocks on TMDB, and it then comes back whole.
async fn meta_detail(
    State(state): State<AppState>,
    axum::extract::Path((media_type, tmdb_id)): axum::extract::Path<(String, i64)>,
) -> Result<impl IntoResponse> {
    // Movie/show meta needs a TMDB key; manga/comic sweeps are keyless.
    if matches!(media_type.as_str(), "movie" | "tv") && state.tmdb.is_none() {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    }
    let Some(cache) = state.meta.as_ref() else {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    };
    // Serve from the DB cache first — a page refresh must never re-hit TMDB.
    // `mvMetaByTmdb` on the client only covers the current session.
    if let Some(meta) = cache.get(&media_type, tmdb_id)? {
        if meta.is_stale() {
            cache.spawn_refresh(media_type.clone(), tmdb_id);
        }
        return Ok(Json(meta_json(&meta)));
    }
    // Cold cache: block until a sweep produces the payload so even the very
    // first open of a title is complete. A warm-up sweep (spawned at add
    // time) may still be in flight and lose the inflight race, so poll
    // briefly — answering an empty payload here would latch as "no data"
    // for the whole frontend session (the Gantz bug).
    let mut meta = None;
    for attempt in 0..6 {
        if let Err(e) = cache.refresh(&media_type, tmdb_id).await {
            warn!("meta refresh failed for {media_type}:{tmdb_id}: {e}");
        }
        meta = cache.get(&media_type, tmdb_id)?;
        if meta.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
        if attempt == 0 {
            info!("cold meta for {media_type}:{tmdb_id} not ready yet — waiting on the in-flight sweep");
        }
    }
    Ok(Json(meta_json(&meta.unwrap_or_default())))
}

fn meta_json(meta: &TmdbMeta) -> serde_json::Value {
    json!({
        "overview": meta.overview,
        "seasons": meta.seasons,
        "episodes": meta.episodes_by_season(),
    })
}

/// Explicit background re-check for newly released seasons/episodes. Answers
/// `{updated}` so the caller only repaints when something actually changed.
/// Costs two SQLite reads while the cached payload is still fresh — the TTL
/// lives server-side, so repeat views of a show never re-hit TMDB.
async fn meta_refresh(
    State(state): State<AppState>,
    axum::extract::Path((media_type, tmdb_id)): axum::extract::Path<(String, i64)>,
) -> Result<impl IntoResponse> {
    if matches!(media_type.as_str(), "movie" | "tv") && state.tmdb.is_none() {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    }
    let Some(cache) = state.meta.as_ref() else {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    };
    let updated = cache.refresh(&media_type, tmdb_id).await.unwrap_or(false);
    Ok(Json(json!({ "updated": updated })))
}

/// The ordered episode list of one TV season, for the show's episode buttons.
/// Normally served out of the show's cached bulk payload; the live endpoint is
/// only reached for a season the cache never got (failed sweep, or a season
/// that aired since the last one).
async fn meta_episodes(
    State(state): State<AppState>,
    axum::extract::Path((tmdb_id, season_number)): axum::extract::Path<(i64, i64)>,
) -> Result<impl IntoResponse> {
    let Some(cache) = state.meta.as_ref() else {
        return Err(Error::BadRequest(
            "Movie search is not configured on this server".into(),
        ));
    };
    if let Some(eps) = cache
        .get("tv", tmdb_id)?
        .and_then(|m| m.episodes_by_season().remove(&season_number))
    {
        return Ok(Json(json!({ "episodes": eps })));
    }
    let episodes = cache
        .tmdb()
        .ok_or_else(|| {
            Error::BadRequest("Movie search is not configured on this server".into())
        })?
        .tv_episodes(tmdb_id, season_number)
        .await
        .map_err(|e| Error::BadRequest(format!("search failed: {e}")))?;
    Ok(Json(json!({ "episodes": episodes })))
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

/// Manga search (Weeb Central catalogue, keyless — complete chapter lists,
/// unlike MangaDex which delists licensed titles). Same result shape as
/// `/api/search`, so the add modal renders every catalogue with one code path.
async fn search_manga(
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
    let results = state
        .reader
        .search_catalogue(&query)
        .await
        .map_err(|e| Error::BadRequest(format!("search failed: {e}")))?;
    Ok(Json(serde_json::to_value(results).unwrap_or_else(|_| json!([]))))
}

/// Comics search (Comic Vine). Without a key the error text is the setup
/// instructions, surfaced in the add modal as-is.
async fn search_comics(
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
    let results = state
        .reading
        .search_comics(&query)
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
            req.overview.as_deref(),
            req.ext_key.as_deref(),
        )
        .await?;
    // Warm the TMDB cache now (overview + seasons + every episode list) so the
    // detail view opens complete instead of filling in after a round-trip. The
    // TTL means this is a no-op when the show is already known.
    if let Some(cache) = state.meta.as_ref() {
        cache.spawn_refresh(
            req.media_type.clone().unwrap_or_else(|| "movie".into()),
            req.tmdb_id,
        );
    }
    Ok(Json(json!({ "id": id })))
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
    let provider = crate::indexer::Provider::parse(params.get("provider").and_then(|v| v.as_str()).unwrap_or("apibay"))?;
    Ok(Json(crate::indexer::search(provider, &query).await?))
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

/// Every playable item is served through the streaming endpoint (fragmented
/// MP4 regardless of the source container). The `mse` flag is a legacy
/// marker kept for API compatibility: it simply means "playable".
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

/// Every file of an item with its season/episode mapping, download state and
/// saved playhead — the data behind the TV-show episode UI.
async fn files_list(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    let files = state.library.list_item_files(id).await?;
    Ok(Json(json!({ "files": files })))
}

/// Toggle whether one torrent file should be downloaded (skip/unskip).
async fn set_file_selected(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<SetFileSelectedRequest>,
) -> Result<impl IntoResponse> {
    state
        .library
        .set_file_selected(id, &req.file, req.selected)
        .await?;
    Ok(Json(json!({ "success": true })))
}

/// Save the playback position for a torrent item. When `file` is present the
/// position is stored per file (mirrored to the legacy `library.playhead`
/// when it's the item's default stream file); otherwise it lands on
/// `library.playhead`. Called by the player on every seek and when the player
/// closes; the client resets it to 0 itself when a movie finishes.
async fn set_playhead(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<SetPlayheadRequest>,
) -> Result<impl IntoResponse> {
    state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    match req.file.as_deref() {
        Some(file) => {
            let file = crate::episodes::normalize_rel_path(file);
            state.library.db.set_file_playhead(id, &file, req.t)?;
            if state.library.default_video_path(id).await.as_deref() == Some(&file) {
                state.library.db.set_playhead(id, req.t)?;
            }
        }
        None => state.library.db.set_playhead(id, req.t)?,
    }
    Ok(Json(json!({ "success": true })))
}

// ---- reading (manga reader) ----

/// Resolve a library row to its reading group identity:
/// (media_type, catalogue id, catalogue ext_key, title).
async fn reading_group(
    state: &AppState,
    id: i64,
) -> Result<(String, i64, Option<String>, String)> {
    let item = state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    let media_type = item
        .media_type
        .clone()
        .filter(|mt| mt == "manga" || mt == "comic")
        .ok_or_else(|| Error::BadRequest("item is not a reading entry".into()))?;
    let tmdb_id = item
        .tmdb_id
        .ok_or_else(|| Error::BadRequest("reading entry has no catalogue id".into()))?;
    let (ext, group_title) = state
        .library
        .db
        .get_reading_source(&media_type, tmdb_id)?
        .unwrap_or((None, None));
    let title = group_title.or(item.title).unwrap_or_default();
    Ok((media_type, tmdb_id, ext, title))
}

/// The group's reading-source binding (where chapter pages come from) plus
/// the list of sources the picker can offer.
async fn read_source_get(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    let (media_type, tmdb_id, _, _) = reading_group(&state, id).await?;
    let binding = state.library.db.get_reader_source(&media_type, tmdb_id)?;
    Ok(Json(json!({
        "source": binding.as_ref().map(|b| b.source.clone()),
        "series_ref": binding.as_ref().map(|b| b.series_ref.clone()),
        "series_title": binding.as_ref().and_then(|b| b.series_title.clone()),
        "sources": crate::reader::sources().iter()
            .map(|(sid, label)| json!({ "id": sid, "label": label }))
            .collect::<Vec<_>>(),
    })))
}

#[derive(Deserialize)]
pub struct ReadSourcePutRequest {
    source: String,
    /// Manual series pick from the picker; absent = auto-match by title.
    series_ref: Option<String>,
    series_title: Option<String>,
}

/// Bind (or rebind) the group's reading source. The source is chosen once
/// per title, never per chapter; a chapter the source lacks surfaces as a
/// reader error with a "switch source" escape hatch, not a picker per row.
async fn read_source_put(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(req): Json<ReadSourcePutRequest>,
) -> Result<impl IntoResponse> {
    let (media_type, tmdb_id, ext_key, title) = reading_group(&state, id).await?;
    let label = crate::reader::source_label(&req.source)?.to_string();
    let (series_ref, series_title) = match req
        .series_ref
        .clone()
        .filter(|s| !s.trim().is_empty())
    {
        Some(r) => (r, req.series_title.clone()),
        None => {
            // Auto-match. The catalogue IS Weeb Central: a row whose ext_key
            // is already a Weeb Central series id binds exactly, zero
            // upstream requests. Otherwise match by title.
            if crate::reading::is_weebcentral_ref(ext_key.as_deref().unwrap_or("")) {
                (ext_key.clone().unwrap_or_default(), Some(title.clone()))
            } else {
                let matched = state.reader.auto_match(&req.source, &title).await;
                match matched {
                    Some(m) => (m.key, Some(m.title)),
                    None => {
                        return Err(Error::BadRequest(format!(
                            "no match for '{title}' on {label} — search and pick the series manually"
                        )))
                    }
                }
            }
        }
    };
    state.library.db.set_reader_source(
        &media_type,
        tmdb_id,
        &req.source,
        &series_ref,
        series_title.as_deref(),
    )?;
    Ok(Json(json!({
        "source": req.source,
        "series_ref": series_ref,
        "series_title": series_title,
    })))
}

/// Series search inside one source (the picker's manual-match mode).
async fn read_source_search(
    State(state): State<AppState>,
    Path((id, source)): Path<(i64, String)>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let _ = reading_group(&state, id).await?;
    let query = params
        .get("q")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if query.is_empty() {
        return Ok(Json(json!([])));
    }
    let hits = state
        .reader
        .search(&source, &query)
        .await
        .map_err(|e| Error::BadRequest(format!("{e}")))?;
    Ok(Json(serde_json::to_value(hits).unwrap_or_else(|_| json!([]))))
}

/// Resolve one chapter into its (proxied) page URLs via the bound source.
async fn read_chapter(
    State(state): State<AppState>,
    Path((id, label)): Path<(i64, String)>,
) -> Result<impl IntoResponse> {
    let (media_type, tmdb_id, _, _) = reading_group(&state, id).await?;
    let binding = state
        .library
        .db
        .get_reader_source(&media_type, tmdb_id)?
        .ok_or_else(|| {
            Error::BadRequest("no reading source picked yet — choose one first".into())
        })?;
    let pages = state
        .reader
        .chapter_pages(&binding.source, &binding.series_ref, &label)
        .await?;
    let urls: Vec<String> = (0..pages.len())
        .map(|n| format!("/api/library/{id}/read/chapter/{label}/page/{n}"))
        .collect();
    Ok(Json(json!({
        "source": crate::reader::source_label(&binding.source).unwrap_or(&binding.source),
        "series_title": binding.series_title,
        "count": pages.len(),
        "pages": urls,
    })))
}

/// One proxied page image: the server talks to the source (referer rules,
/// no CORS), the browser only ever sees this route.
async fn read_chapter_page(
    State(state): State<AppState>,
    Path((id, label, n)): Path<(i64, String, usize)>,
) -> Result<Response> {
    let (media_type, tmdb_id, _, _) = reading_group(&state, id).await?;
    let binding = state
        .library
        .db
        .get_reader_source(&media_type, tmdb_id)?
        .ok_or_else(|| {
            Error::BadRequest("no reading source picked yet — choose one first".into())
        })?;
    let (bytes, ctype) = state
        .reader
        .fetch_page(&binding.source, &binding.series_ref, &label, n)
        .await?;
    Ok((
        [
            (header::CONTENT_TYPE, ctype.as_str()),
            (header::CACHE_CONTROL, "private, max-age=86400"),
        ],
        bytes,
    )
        .into_response())
}

/// Saved per-chapter progress (read flags + last visible page) and the most
/// recently touched chapter — the detail page's read marks and the reader's
/// scroll restore.
async fn read_progress(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse> {
    let (media_type, tmdb_id, _, _) = reading_group(&state, id).await?;
    let rows = state.library.db.list_chapter_progress(&media_type, tmdb_id)?;
    let chapters: serde_json::Map<String, serde_json::Value> = rows
        .iter()
        .map(|r| (r.chapter.clone(), json!({ "read": r.read, "page": r.page })))
        .collect();
    let last = rows
        .iter()
        .filter(|r| r.read)
        .max_by_key(|r| r.updated_at)
        .or_else(|| rows.iter().max_by_key(|r| r.updated_at))
        .map(|r| r.chapter.clone());
    Ok(Json(json!({ "chapters": chapters, "last": last })))
}

#[derive(Deserialize)]
pub struct ReadProgressRequest {
    read: Option<bool>,
    page: Option<i64>,
}

/// Save one chapter's progress (called by the reader while scrolling and on
/// close; the read flag is sticky server-side).
async fn read_chapter_progress(
    State(state): State<AppState>,
    Path((id, label)): Path<(i64, String)>,
    Json(req): Json<ReadProgressRequest>,
) -> Result<impl IntoResponse> {
    let (media_type, tmdb_id, _, _) = reading_group(&state, id).await?;
    state
        .library
        .db
        .set_chapter_progress(&media_type, tmdb_id, &label, req.read, req.page)?;
    Ok(Json(json!({ "success": true })))
}

/// Read the optional `?file=` query param and normalize it. None = the
/// torrent's default (largest) video file.
fn query_file(params: &serde_json::Value) -> Option<String> {
    params
        .get("file")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(crate::episodes::normalize_rel_path)
}

/// Segment-stream manifest: tells the client the SourceBuffer MIME, total
/// duration, which segments exist, and whether segmentation is complete.
/// The client polls this; missing segments simply don't exist yet (either
/// ffmpeg hasn't cut them for a finished file, or the download frontier
/// hasn't reached them for a live torrent).
async fn stream_info(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;

    let file = query_file(&params);
    let seg = state.library.segments.ensure(id, file).await;
    let snap = seg.snapshot();
    if snap.mime.is_none() || snap.duration.is_none() {
        if let Some(p) = &snap.probe_path {
            if p.exists() {
                let mime = Remuxer::probe_mse_mime(&p.to_string_lossy()).await;
                let duration = Remuxer::probe_mse_duration(&p.to_string_lossy()).await;
                seg.set_probe(mime, duration);
            }
        }
    }
    let snap = seg.snapshot();
    let state = state
        .library
        .db
        .get_item(id)
        .ok()
        .flatten()
        .map(|i| i.state)
        .unwrap_or_default();
    Ok(Json(json!({
        "generation": snap.generation,
        "mime": snap.mime,
        "duration": snap.duration,
        "timing": snap.timing,
        "done": snap.done,
        "error": snap.error,
        "state": state,
        "segments": snap.segments,
    })))
}

/// SPA fallback: client-side routes (/library/...) all serve the app shell —
/// the frontend router picks the view from the path.
async fn spa_fallback() -> Response {
    match tokio::fs::read("static/index.html").await {
        Ok(bytes) => (
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            bytes,
        )
            .into_response(),
        Err(_) => match tokio::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/static/index.html")).await {
            Ok(bytes) => (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                bytes,
            )
                .into_response(),
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        },
    }
}

/// Serve `init.mp4` (ftyp+moov) for a segment session.
async fn seg_init(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Response {
    let file = query_file(&params);
    let Some(seg) = state.library.segments.get(id, file).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if params.get("g").and_then(|g| g.as_str()) != Some(seg.snapshot().generation.as_str()) {
        return StatusCode::GONE.into_response();
    }
    serve_seg_file(&seg.dir, "init.mp4").await
}

/// Serve one media segment (`seg-N.m4s`). A 404 means "not cut yet": the
/// client treats that as the frontier and keeps polling.
async fn seg_n(
    State(state): State<AppState>,
    Path((id, n)): Path<(i64, usize)>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Response {
    let file = query_file(&params);
    let Some(seg) = state.library.segments.get(id, file).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if params.get("g").and_then(|g| g.as_str()) != Some(seg.snapshot().generation.as_str()) {
        return StatusCode::GONE.into_response();
    }
    serve_seg_file(&seg.dir, &format!("seg-{n}.m4s")).await
}

async fn serve_seg_file(dir: &std::path::Path, name: &str) -> Response {
    match tokio::fs::read(dir.join(name)).await {
        Ok(bytes) => (
            [(header::CONTENT_TYPE, "video/mp4")],
            [(header::CACHE_CONTROL, "public, max-age=31536000, immutable")],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---- subtitles ----

fn subs_dirs(state: &AppState, id: i64) -> (PathBuf, PathBuf) {
    let item_dir = state.library.config.library_dir.join(id.to_string());
    let subs_dir = item_dir.join(".subs");
    (item_dir, subs_dir)
}

async fn video_info(state: &AppState, id: i64, file: Option<&str>) -> Option<(PathBuf, String)> {
    // (video path, query name) — file may be partial while downloading
    let handle = state.library.engine.get_torrent(id).await;
    if let Some(handle) = handle {
        let chosen = match file {
            Some(rel) => state
                .library
                .engine
                .list_files(id)
                .await
                .unwrap_or_default()
                .into_iter()
                .find(|(_, path, _)| {
                    path == rel || crate::episodes::normalize_rel_path(path) == rel
                })
                .map(|(_, name, _)| name),
            None => state
                .library
                .engine
                .find_largest_video_file(id)
                .await
                .map(|(_, name, _)| name),
        };
        if let Some(name) = chosen {
            let path = handle.output_folder().join(&name);
            let stem = std::path::Path::new(&name)
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or(name.clone());
            return Some((path, stem));
        }
    }
    let item = state.library.db.get_item(id).ok().flatten()?;
    let item_dir = state.library.config.library_dir.join(id.to_string());
    let dir = item
        .file_path
        .as_ref()
        .map(PathBuf::from)
        .filter(|p| p.exists())
        .unwrap_or(item_dir);
    let full = match file {
        Some(rel) => dir.join(rel).exists().then(|| dir.join(rel)),
        None => crate::library::find_largest_video_in(&dir),
    }?;
    let stem = full
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    Some((full, stem))
}

/// Select only an owned, fully downloaded media file for subtitle extraction.
/// Sparse partial files must not be probed/extracted as if their zeros were data.
async fn subtitle_video(
    state: &AppState, id: i64, file: Option<&str>,
) -> Result<Option<(PathBuf, bool)>> {
    let item = state.library.db.get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    let files = state.library.list_item_files(id).await?;
    let selected = files.iter().find(|f| {
        f.video && file.map(|p| {
            crate::episodes::normalize_rel_path(&f.path) == crate::episodes::normalize_rel_path(p)
        }).unwrap_or(f.default)
    });
    let Some(selected) = selected else { return Ok(None) };
    let verified = !selected.skipped && (item.state == "ready"
        || (state.library.engine.get_torrent(id).await.is_some()
            && selected.size > 0 && selected.have >= selected.size));
    let Some((path, _)) = video_info(state, id, Some(&selected.path)).await else {
        return Ok(None);
    };
    let path = if verified {
        tokio::fs::canonicalize(path).await?
    } else {
        path
    };
    Ok(Some((path, verified)))
}

/// Local sidecars, downloaded subtitles, and embedded text tracks for the
/// selected episode. Bitmap subtitles require a different renderer.
async fn subs_list(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    let (item_dir, subs_dir) = subs_dirs(&state, id);
    let mut local = subtitles::scan_local_subs(&item_dir);
    for mut s in subtitles::scan_local_subs(&subs_dir) {
        s.source = "downloaded".to_string();
        if !local.iter().any(|e| e.name == s.name) {
            local.push(s);
        }
    }
    let file = query_file(&params);
    let mut embedded_pending = false;
    let mut embedded_error = None;
    if let Some((path, verified)) = subtitle_video(&state, id, file.as_deref()).await? {
        if verified {
            match subtitles::embedded_subs(&path).await {
                Ok(tracks) => local.extend(tracks),
                Err(error) => {
                    warn!("embedded subtitles for {id}: {error:#}");
                    embedded_error = Some("Could not inspect embedded subtitles");
                }
            }
        } else {
            embedded_pending = true;
        }
    } else {
        embedded_pending = state.library.db.get_item(id)?.is_some_and(|i| i.state != "ready");
    }
    Ok(Json(json!({
        "configured": state.subs.is_some(),
        "local": local,
        "embedded_pending": embedded_pending,
        "embedded_error": embedded_error,
    })))
}

/// Search OpenSubtitles for the item's video (optionally a specific file).
async fn subs_search(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let Some(subs) = state.subs.clone() else {
        return Ok(Json(json!({ "configured": false, "results": [] })));
    };
    state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    let file = query_file(&params);
    let Some((video_path, stem)) = video_info(&state, id, file.as_deref()).await else {
        return Err(Error::NotFound("no video file found for item".into()));
    };
    let query = stem.replace('.', " ");
    // the hash only identifies the complete file; a partial file would hash
    // zeros and match garbage
    let hash = if video_path.exists() && video_path.metadata().map(|m| m.len()).unwrap_or(0) > 0 {
        let size = video_path.metadata().map(|m| m.len()).unwrap_or(0);
        let p = video_path.clone();
        tokio::task::spawn_blocking(move || subtitles::movie_hash(&p, size).ok())
            .await
            .unwrap_or(None)
    } else {
        None
    };
    let results = subs
        .search(
            &query,
            hash.as_ref()
                .map(|(h, size)| (h.as_str(), *size)),
        )
        .await
        .map_err(|e| Error::BadRequest(format!("subtitle search failed: {e:#}")))?;
    Ok(Json(json!({ "configured": true, "results": results })))
}

/// Download a subtitle from OpenSubtitles, convert to VTT, store in `.subs/`.
async fn subs_download(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<serde_json::Value>,
) -> Result<impl IntoResponse> {
    let Some(subs) = state.subs.clone() else {
        return Err(Error::BadRequest("opensubtitles is not configured".into()));
    };
    state
        .library
        .db
        .get_item(id)?
        .ok_or_else(|| Error::NotFound(format!("item {id} not found")))?;
    let file_id = body["file_id"]
        .as_str()
        .ok_or_else(|| Error::BadRequest("missing file_id".into()))?
        .to_string();
    let (saved_name, bytes) = subs
        .download(&file_id)
        .await
        .map_err(|e| Error::BadRequest(format!("subtitle download failed: {e:#}")))?;
    let vtt = subtitles::to_vtt(&saved_name, &bytes)
        .map_err(|e| Error::BadRequest(format!("subtitle conversion failed: {e:#}")))?;
    // prefer the language OpenSubtitles reported for this subtitle (the online
    // row's flag is derived from it), so the downloaded row matches the list.
    let lang = body["lang"]
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| subtitles::detect_lang(&saved_name, Some(&vtt)));
    let stem = std::path::Path::new(&saved_name)
        .file_stem()
        .map(|s| {
            s.to_string_lossy()
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect::<String>()
        })
        .unwrap_or_else(|| "subtitle".to_string());
    let fname = format!(
        "{}{}.vtt",
        lang.as_deref().map(|l| format!("{l}_")).unwrap_or_default(),
        stem
    );
    let (_item_dir, subs_dir) = subs_dirs(&state, id);
    tokio::fs::create_dir_all(&subs_dir)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    tokio::fs::write(subs_dir.join(&fname), vtt)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let flag = subtitles::flag_for(lang.as_deref());
    Ok(Json(json!({ "name": fname, "lang": lang, "flag": flag, "source": "downloaded" })))
}

/// Delete a downloaded subtitle from `.subs/`. Only files we downloaded are
/// deletable — a torrent-folder subtitle belongs to the torrent.
async fn subs_delete(
    State(state): State<AppState>,
    Path((id, name)): Path<(i64, String)>,
) -> Result<impl IntoResponse> {
    let (_item_dir, subs_dir) = subs_dirs(&state, id);
    let path = subtitles::resolve_sub_path(&subs_dir, &name)
        .ok_or_else(|| Error::NotFound(format!("subtitle {name} not found")))?;
    if !path.is_file() {
        return Err(Error::NotFound(format!("subtitle {name} not found")));
    }
    tokio::fs::remove_file(&path)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    Ok(Json(json!({ "ok": true })))
}

/// Serve a subtitle converted to WebVTT. `name` is matched against both the
/// `.subs/` store (already VTT) and the raw torrent folder (converted on the
/// fly, so the original .srt is never modified).
async fn sub_file(
    State(state): State<AppState>,
    Path((id, name)): Path<(i64, String)>,
    axum::extract::Query(params): axum::extract::Query<serde_json::Value>,
) -> Response {
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let (item_dir, subs_dir) = subs_dirs(&state, id);
    // Existing sidecars retain their names, including ones named embedded-*.
    if let Ok(bytes) = tokio::fs::read(subs_dir.join(&name)).await {
        return vtt_response(bytes);
    }
    if let Some(path) = subtitles::resolve_sub_path(&item_dir, &name) {
        if let Ok(bytes) = tokio::fs::read(&path).await {
            return match subtitles::to_vtt(&name, &bytes) {
                Ok(vtt) => vtt_response(vtt.into_bytes()),
                Err(e) => {
                    warn!("subtitle conversion failed for {name}: {e:#}");
                    StatusCode::UNPROCESSABLE_ENTITY.into_response()
                }
            };
        }
    }
    if name.starts_with("embedded-") {
        let file = query_file(&params);
        let (path, verified) = match subtitle_video(&state, id, file.as_deref()).await {
            Ok(Some(source)) => source,
            Ok(None) => return StatusCode::NOT_FOUND.into_response(),
            Err(e) => return e.into_response(),
        };
        if !verified {
            return (StatusCode::CONFLICT,
                "Embedded subtitles are available after this file finishes downloading").into_response();
        }
        let Some(index) = subtitles::embedded_index(&path, &name) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let tracks = match subtitles::embedded_subs(&path).await {
            Ok(tracks) => tracks,
            Err(error) => {
                warn!("embedded subtitle probe failed: {error:#}");
                return StatusCode::UNPROCESSABLE_ENTITY.into_response();
            }
        };
        if !tracks.iter().any(|t| t.name == name) {
            return StatusCode::NOT_FOUND.into_response();
        }
        let video_start = match Remuxer::probe_source_timing(&path.to_string_lossy()).await {
            Some(timing) => timing.video_start,
            None => return (StatusCode::UNPROCESSABLE_ENTITY,
                "Could not read subtitle timeline").into_response(),
        };
        let cache = state.library.config.temp_dir
            .join(id.to_string()).join("embedded-subs");
        return match subtitles::embedded_vtt(&path, index, video_start, &cache).await {
            Ok(bytes) => vtt_response(bytes),
            Err(error) => {
                warn!("embedded subtitle extraction failed: {error:#}");
                StatusCode::UNPROCESSABLE_ENTITY.into_response()
            }
        };
    }
    StatusCode::NOT_FOUND.into_response()
}

fn vtt_response(bytes: Vec<u8>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/vtt; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        bytes,
    )
        .into_response()
}

// ---- WebSocket handler ----

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state, id))
}

async fn library_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_library_ws(socket, state.library.clone()))
}

// ---- watch party ----

#[derive(Deserialize)]
pub struct CreatePartyRequest {
    item_id: i64,
    name: Option<String>,
    relay: Option<String>,
    /// Episode (torrent-relative file path) to scope a TV party to.
    file: Option<String>,
}

/// Create a watch party around a library item. Returns the paste-able
/// invite (which also stays available via GET /api/party and the panel).
async fn create_party(
    State(state): State<AppState>,
    Json(req): Json<CreatePartyRequest>,
) -> Result<impl IntoResponse> {
    let name = req
        .name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| state.party.default_name().to_string());
    let (invite, lan_only) = state
        .party
        .create(req.item_id, &name, req.relay, req.file)?;
    let party = state.party.snapshot_json()["party"].clone();
    Ok(Json(json!({ "invite": invite, "lan_only": lan_only, "party": party })))
}

#[derive(Deserialize)]
pub struct JoinPartyRequest {
    invite: String,
    name: Option<String>,
}

/// Join a watch party from an invite string. The invite's torrent is added
/// to the library first (deduped by infohash — same torrent, same swarm)
/// so the joiner downloads alongside everyone else.
async fn join_party(
    State(state): State<AppState>,
    Json(req): Json<JoinPartyRequest>,
) -> Result<impl IntoResponse> {
    let invite = crate::party::parse_invite(&req.invite)
        .ok_or_else(|| Error::BadRequest("invalid watch party invite".into()))?;

    let want = crate::party::magnet_infohash(&invite.magnet);
    let mut existing = None;
    if let Some(want) = want.as_deref() {
        for it in state.library.list_items().await? {
            if crate::party::magnet_infohash(&it.magnet).as_deref() == Some(want) {
                existing = Some(it);
                break;
            }
        }
    }

    let item_id = match existing {
        Some(it) => {
            // Already in the library: only fill in missing metadata.
            if it.tmdb_id.is_none() && invite.tmdb_id.is_some() {
                state.library.db.update_metadata(
                    it.id,
                    invite.title.as_deref(),
                    invite.year,
                    invite.poster_path.as_deref(),
                    invite.tmdb_id,
                    invite.media_type.as_deref(),
                )?;
            }
            it.id
        }
        None => {
            let id = state
                .library
                .add_item_with_sequential(&invite.magnet, true)
                .await?;
            state.library.db.update_metadata(
                id,
                invite.title.as_deref(),
                invite.year,
                invite.poster_path.as_deref(),
                invite.tmdb_id,
                invite.media_type.as_deref(),
            )?;
            id
        }
    };

    let name = req
        .name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| state.party.default_name().to_string());
    state.party.join(invite, &name, item_id)?;
    let party = state.party.snapshot_json()["party"].clone();
    Ok(Json(json!({ "item_id": item_id, "party": party })))
}

async fn get_party(State(state): State<AppState>) -> Result<impl IntoResponse> {
    Ok(Json(state.party.snapshot_json()["party"].clone()))
}

async fn leave_party(State(state): State<AppState>) -> Result<impl IntoResponse> {
    state.party.leave()?;
    Ok(Json(json!({ "success": true })))
}

/// Browser-facing party socket: roster/action events downstream, local
/// actions and player state upstream.
async fn party_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_party_ws(socket, state.party.clone()))
}

/// Library notification channel: streams `changed` / `progress` events to the
/// frontend so it never has to poll. Mirrors the party WS pattern (broadcast
/// subscribe + lagged catch-up) — and the frontend drives both its library
/// refresh and its per-torrent progress repaint from these events.
async fn handle_library_ws(
    socket: WebSocket,
    library: Arc<crate::library::LibraryManager>,
) {
    use futures_util::{SinkExt, StreamExt};
    let mut rx = library.events_tx.subscribe();
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let out = match ev {
                    Ok(ev) => serde_json::to_string(&ev).unwrap_or_default(),
                    // Fell behind (e.g. suspended tab): send a resync marker;
                    // the client re-fetches the library + progress once.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        "{\"type\":\"resync\"}".to_string()
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                if sink.send(Message::Text(out.into())).await.is_err() {
                    break;
                }
            }
            msg = stream.next() => match msg {
                Some(Ok(_)) => {} // client->server messages are ignored
                Some(Err(_)) | None => break,
            },
        }
    }
}

async fn handle_party_ws(mut socket: WebSocket, party: Arc<crate::party::PartyManager>) {
    use futures_util::{SinkExt, StreamExt};
    let mut events = party.subscribe();
    let snapshot = party.snapshot_json().to_string();
    if socket
        .send(Message::Text(snapshot.into()))
        .await
        .is_err()
    {
        return;
    }

    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            ev = events.recv() => {
                match ev {
                    Ok(msg) => {
                        if sink.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Fell behind (e.g. suspended tab): catch up with a
                        // fresh full snapshot.
                        let snap = party.snapshot_json().to_string();
                        if sink.send(Message::Text(snap.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else { continue };
                        match v["type"].as_str() {
                            Some("action") => {
                                if let Some(kind) = v["kind"].as_str() {
                                    let t = v["t"].as_f64().unwrap_or(0.0);
                                    let file = v["file"].as_str();
                                    party.send_action(kind, t, file);
                                }
                            }
                            Some("state") => {
                                party.set_local_state(
                                    v["ready"].as_bool().unwrap_or(false),
                                    v["pos"].as_f64().unwrap_or(0.0),
                                    v["playing"].as_bool().unwrap_or(false),
                                    v["buffering"].as_bool().unwrap_or(false),
                                    v["file"].as_str(),
                                );
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break,
                }
            }
        }
    }
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
    use crate::library::find_largest_video_in;

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
