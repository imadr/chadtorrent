use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use chadtorrent::config::Config;
use chadtorrent::http::{AppState, build_router};
use chadtorrent::library::LibraryManager;

fn make_pattern(size: usize) -> Vec<u8> {
    let mut state: u64 = 0x9E3779B97F4A7C15;
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push(state as u8);
    }
    out
}

fn config_for(tmp: &Path) -> Config {
    Config {
        port: 0,
        library_dir: tmp.join("library"),
        temp_dir: tmp.join("temp"),
        db_path: tmp.join("test.db"),
    }
}

async fn setup_app(tmp: &Path) -> Arc<LibraryManager> {
    LibraryManager::new(config_for(&tmp.join("app"))).await.unwrap()
}

async fn start_http(
    library: Arc<LibraryManager>,
) -> (String, String, tokio::task::AbortHandle) {
    let index_html = include_str!("../static/index.html").to_string();
    let router = build_router(AppState { library, index_html, tmdb: None, subs: None });
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let http = format!("http://{addr}");
    let ws = format!("ws://{addr}");
    (http, ws, handle.abort_handle())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_error_handling() {
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;
    let (http, _ws, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{http}/api/library"))
        .json(&serde_json::json!({ "magnet": "notamagnet" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = client
        .get(format!("{http}/api/library/999999/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = client
        .get(format!("{http}/api/library/999999/stream"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // unknown paths fall through to the SPA shell, not an error
    let resp = client.get(format!("{http}/mse/999999")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client
        .delete(format!("{http}/api/library/999999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    drop(srv);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_does_not_heuristically_mark_download_ready() {
    let tmp = TempDir::new().unwrap();

    // Pre-create a partial download on disk. The file is written to the
    // correct library path and has the right size_bytes so an older version
    // would have wrongly inferred completion.
    let db = chadtorrent::db::Database::new(tmp.path().join("test.db").to_str().unwrap()).unwrap();
    let library_dir = tmp.path().join("app").join("library");
    let temp_dir = tmp.path().join("app").join("temp");
    let id = db
        .insert_item("magnet:?xt=urn:btih:abc123")
        .unwrap();
    let full_contents = make_pattern(2_000_000);
    let partial_contents = make_pattern(200_000); // 10% of the file
    // Place the file under library_dir/{id} (where register_torrent writes).
    let out = library_dir.join(id.to_string());
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("movie.mp4"), &partial_contents).unwrap();
    db.update_title(id, "Partial Download").unwrap();
    db.update_size(id, full_contents.len() as i64).unwrap();
    db.update_state(id, "downloading").unwrap();
    // Also stash a temp file so the old code path that checked temp_dir
    // wouldn't accidentally mark it ready.
    let temp_out = temp_dir.join(id.to_string());
    std::fs::create_dir_all(&temp_out).unwrap();
    std::fs::write(temp_out.join("movie.mp4"), &partial_contents).unwrap();
    drop(db);

    let config = config_for(tmp.path());
    let library = LibraryManager::new(config).await.unwrap();

    let item = library.db.get_item(id).unwrap().unwrap();
    // The new code never infers completion from on-disk sizes — only the
    // torrent engine marks a download ready once every piece is verified.
    assert_ne!(item.state, "ready", "startup must not infer completion from file sizes");
    assert_eq!(item.state, "downloading");

    library.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_torrent_delete_removes_row() {
    let tmp = TempDir::new().unwrap();
    let config = config_for(tmp.path());
    let library = LibraryManager::new(config).await.unwrap();

    let id = library
        .db
        .insert_item("magnet:?xt=urn:btih:abc123")
        .unwrap();

    // A standalone torrent (no tmdb_id) is deleted outright, no placeholder.
    library.remove_item(id).await.unwrap();
    assert!(library.db.get_item(id).unwrap().is_none());

    library.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torrent_delete_removes_files_completely() {
    let tmp = TempDir::new().unwrap();
    let config = config_for(tmp.path());
    let library = LibraryManager::new(config.clone()).await.unwrap();

    let id = library
        .db
        .insert_item("magnet:?xt=urn:btih:abc123")
        .unwrap();
    let lib_dir = config.library_dir.join(id.to_string());
    std::fs::create_dir_all(&lib_dir).unwrap();
    std::fs::write(lib_dir.join("movie.mp4"), b"fake data").unwrap();
    library
        .db
        .update_file_path(id, &lib_dir.to_string_lossy())
        .unwrap();
    library.db.update_state(id, "ready").unwrap();

    // Soft delete: row + every file gone, no leftover in any dir.
    library.remove_item(id).await.unwrap();
    assert!(library.db.get_item(id).unwrap().is_none());
    assert!(!lib_dir.exists(), "library files must be deleted");
    assert!(
        !config.temp_dir.join(id.to_string()).exists(),
        "no leftover temp dir"
    );

    library.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn movie_delete_cascades_to_attached_torrents() {
    let tmp = TempDir::new().unwrap();
    let config = config_for(tmp.path());
    let library = LibraryManager::new(config.clone()).await.unwrap();

    let place = library
        .db
        .insert_movie("My Movie", Some(2024), None, 999, "movie")
        .unwrap();
    let t1 = library
        .db
        .insert_item("magnet:?xt=urn:btih:11111")
        .unwrap();
    let t2 = library
        .db
        .insert_item("magnet:?xt=urn:btih:22222")
        .unwrap();
    library
        .db
        .update_metadata(t1, None, None, None, Some(999), None)
        .unwrap();
    library
        .db
        .update_metadata(t2, None, None, None, Some(999), None)
        .unwrap();

    let mut dirs = Vec::new();
    for id in [t1, t2] {
        let dir = config.library_dir.join(id.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("movie.mp4"), b"fake data").unwrap();
        library.db.update_file_path(id, &dir.to_string_lossy()).unwrap();
        library.db.update_state(id, "ready").unwrap();
        dirs.push(dir);
    }

    // Hard delete of the placeholder cascades to every attached torrent.
    library.remove_movie(place).await.unwrap();
    assert!(library.db.get_item(place).unwrap().is_none());
    assert!(library.db.get_item(t1).unwrap().is_none());
    assert!(library.db.get_item(t2).unwrap().is_none());
    for dir in &dirs {
        assert!(!dir.exists(), "cascaded torrent files must be deleted");
    }

    library.stop().await;
}
/// Segment pipeline end-to-end: a finished (ready) item with a real (tiny)
/// MP4 gets segmented by the background ffmpeg job, the manifest reports
/// segments with sane media times, init/segments are served, unknown
/// segments 404, and a bogus item 404s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_stream_pipeline() {
    if which_ffmpeg().is_none() {
        eprintln!("ffmpeg not installed; skipping segment pipeline test");
        return;
    }
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;
    let (http, _ws, _srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();

    // 6s 64x64 test clip (video+audio) so at least one media segment exists.
    let id = library.db.insert_item("magnet:?xt=urn:btih:0000000000000000000000000000000000000000").unwrap();
    let dir = tmp.path().join("app").join("library").join(id.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("clip.mp4");
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-v", "error",
            "-f", "lavfi", "-i", "testsrc=duration=6:size=64x64:rate=10",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=6",
            "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-shortest",
            file.to_string_lossy().as_ref(),
        ])
        .status()
        .expect("ffmpeg should run");
    assert!(status.success());
    library.db.update_file_path(id, &dir.to_string_lossy()).unwrap();
    library.db.update_state(id, "ready").unwrap();

    // Manifest: the ensure() call inside kicks off segmentation; poll until done.
    let mut manifest = None;
    for _ in 0..100 {
        let resp = client
            .get(format!("{http}/api/library/{id}/stream"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let m: serde_json::Value = resp.json().await.unwrap();
        assert!(m["error"].is_null(), "segmenter error: {}", m["error"]);
        if m["done"].as_bool() == Some(true) && m["segments"].as_array().map(|s| !s.is_empty()).unwrap_or(false) {
            manifest = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let manifest = manifest.expect("segmenter should finish for a 6s clip");
    assert!(manifest["mime"].as_str().unwrap().contains("avc1"));
    assert!(manifest["duration"].as_f64().unwrap() > 5.0);
    let segs = manifest["segments"].as_array().unwrap();
    assert!(!segs.is_empty());
    assert_eq!(segs[0]["n"], 1);
    assert!(segs[0]["t"].as_f64().unwrap() < 1.0, "first segment starts at the head");

    // init + first segment serve as fMP4 with immutable caching.
    let resp = client.get(format!("{http}/mse/{id}/init")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-type"], "video/mp4");
    let init = resp.bytes().await.unwrap();
    assert!(&init[4..8] == b"ftyp");

    let resp = client.get(format!("{http}/mse/{id}/seg/1")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let seg = resp.bytes().await.unwrap();
    assert!(&seg[4..8] == b"moof" || &seg[4..8] == b"sidx" || &seg[4..8] == b"styp");

    // Unknown segment + unknown item -> 404.
    let resp = client.get(format!("{http}/mse/{id}/seg/99999")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    let resp = client.get(format!("{http}/api/library/999999/stream")).send().await.unwrap();
    assert_eq!(resp.status(), 404);

    library.stop().await;
}

fn which_ffmpeg() -> Option<std::path::PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|p| p.join("ffmpeg"))
            .find(|p| p.is_file())
    })
}
