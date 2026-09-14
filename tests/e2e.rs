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
        tmdb_api_key: None,
    }
}

async fn setup_app(tmp: &Path) -> Arc<LibraryManager> {
    Arc::new(LibraryManager::new(config_for(&tmp.join("app"))).await.unwrap())
}

async fn start_http(
    library: Arc<LibraryManager>,
) -> (String, String, tokio::task::AbortHandle) {
    let index_html = include_str!("../static/index.html").to_string();
    let router = build_router(AppState { library, index_html, tmdb: None });
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
        .get(format!("{http}/stream/999999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = client
        .delete(format!("{http}/api/library/999999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    drop(srv);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_marks_preexisting_completed_download_ready() {
    let tmp = TempDir::new().unwrap();

    // Simulate state from an older version: fully downloaded temp files on
    // disk, but the DB never recorded file_path or ready.
    let db = chadtorrent::db::Database::new(tmp.path().join("test.db").to_str().unwrap()).unwrap();
    let temp_dir = tmp.path().join("temp");
    let id = db
        .insert_item("magnet:?xt=urn:btih:abc123")
        .unwrap();
    let contents = make_pattern(1_000_000);
    let out = temp_dir.join(id.to_string());
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("movie.mp4"), &contents).unwrap();
    db.update_title(id, "Old Download").unwrap();
    db.update_size(id, contents.len() as i64).unwrap();
    db.update_state(id, "downloading").unwrap();
    drop(db);

    let config = config_for(tmp.path());
    let library = Arc::new(LibraryManager::new(config).await.unwrap());

    let item = library.db.get_item(id).unwrap().unwrap();
    assert_eq!(item.state, "ready", "startup reconciliation should mark it ready");
    assert!(item.file_path.is_some());

    let status = library.get_status(id).await.unwrap();
    assert!(status.is_local);
    assert_eq!(status.total_bytes, contents.len() as u64);

    // Deleting a reconciled temp item hard (movie delete) still cleans up its
    // files and removes the row.
    library.remove_movie(id).await.unwrap();
    assert!(!out.exists(), "reconciled temp files should be removed on delete");
    assert!(library.db.get_item(id).unwrap().is_none());

    library.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_torrent_delete_removes_row() {
    let tmp = TempDir::new().unwrap();
    let config = config_for(tmp.path());
    let library = Arc::new(LibraryManager::new(config).await.unwrap());

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
    let library = Arc::new(LibraryManager::new(config.clone()).await.unwrap());

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
    let library = Arc::new(LibraryManager::new(config.clone()).await.unwrap());

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