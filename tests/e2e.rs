use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use librqbit::{
    CreateTorrentOptions, ListenerMode, ListenerOptions, Session, SessionOptions,
};
use tempfile::TempDir;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use chadtorrent::config::Config;
use chadtorrent::http::{AppState, build_router};
use chadtorrent::library::LibraryManager;

// Serialize the heavyweight e2e tests (each spawns 2 librqbit sessions).
static E2E_LOCK: Semaphore = Semaphore::const_new(2);

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

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk_files(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

/// Create a session that seeds `contents` as a torrent named `file_name`,
/// returning (session, magnet, listen_addr). Internet-free peer discovery is
/// done via `initial_peers` on the client side.
async fn start_seeder(
    dir: &Path,
    file_name: &str,
    contents: &[u8],
) -> (Arc<Session>, String, SocketAddr) {
    std::fs::create_dir_all(dir).unwrap();
    let file_path = dir.join(file_name);
    tokio::fs::write(&file_path, contents).await.unwrap();

    let session = Session::new_with_opts(
        dir.join("rqbit-seeder"),
        SessionOptions {
            dht: None,
            persistence: None,
            fastresume: false,
            disable_trackers: true,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let (result, handle) = session
        .create_and_serve_torrent(
            &file_path,
            CreateTorrentOptions {
                name: None,
                trackers: vec![],
                piece_length: Some(512 * 1024),
            },
        )
        .await
        .expect("create_and_serve_torrent failed");

    timeout(Duration::from_secs(30), handle.wait_until_completed())
        .await
        .expect("seeder could not verify its own data")
        .expect("seeder verification error");

    let addr = session
        .listen_addr()
        .expect("seeder has no listen address");
    let magnet = result.as_magnet().to_string();
    (session, magnet, addr)
}

async fn setup_app(tmp: &Path) -> Arc<LibraryManager> {
    let config = Config {
        port: 0,
        library_dir: tmp.join("library"),
        temp_dir: tmp.join("temp"),
        db_path: tmp.join("test.db"),
        tmdb_api_key: None,
    };
    Arc::new(LibraryManager::new(config).await.unwrap())
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

async fn wait_complete(library: &LibraryManager, id: i64) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = library.get_status(id).await.unwrap();
        if status.total_bytes > 0 && status.contiguous_bytes >= status.total_bytes {
            return (status.contiguous_bytes, status.total_bytes);
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout waiting for item {id} to complete: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_false_full_flow_streaming_ws_delete() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(3_500_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;

    let library = setup_app(&tmp.path().join("app")).await;

    let id = library
        .add_item_with_initial_peers(&magnet, false, vec![peer])
        .await
        .unwrap();

    let item = library.db.get_item(id).unwrap().unwrap();
    assert!(!item.keep);
    assert!(item.file_path.is_none());
    assert!(
        item.state == "connecting" || item.state == "downloading",
        "unexpected initial state {:?}",
        item.state
    );

    // ---- HTTP layer (need it for mid-download streaming) ----
    let (http, wsur, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();

    // The key feature: stream the beginning of the file BEFORE the download
    // has finished. FileStream blocks on missing pieces, so the first bytes
    // arrive as soon as piece 0 is available.
    let resp = client
        .get(format!("{http}/stream/{id}"))
        .header(reqwest::header::RANGE, "bytes=0-2047")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let early = resp.bytes().await.unwrap();
    assert_eq!(early.as_ref(), &contents[..2048]);

    let (total, _c) = wait_complete(&library, id).await;
    assert_eq!(total, contents.len() as u64);

    // Files landed in the temp dir and match the seed bytes exactly.
    let temp_dir = library.config.temp_dir.join(id.to_string());
    let mut found = Vec::new();
    walk_files(&temp_dir, &mut found);
    assert!(!found.is_empty(), "expected downloaded files in temp dir");
    let matching = found
        .iter()
        .find(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) == contents.len() as u64)
        .expect("no file with matching size");
    assert_eq!(tokio::fs::read(matching).await.unwrap(), contents);

    let items: serde_json::Value = client
        .get(format!("{http}/api/library"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(items.as_array().unwrap().len(), 1);

    let status: serde_json::Value = client
        .get(format!("{http}/api/library/{id}/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["total_bytes"].as_u64().unwrap(), contents.len() as u64);
    assert_eq!(
        status["contiguous_bytes"].as_u64().unwrap(),
        contents.len() as u64
    );
    assert_eq!(status["keep"].as_bool(), Some(false));

    // Full stream (no range).
    let resp = client.get(format!("{http}/stream/{id}")).send().await.unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.headers()[reqwest::header::CONTENT_TYPE], "video/mp4");
    assert_eq!(
        resp.headers()[reqwest::header::ACCEPT_RANGES],
        "bytes"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), contents.as_slice());

    // Explicit range in the middle.
    let resp = client
        .get(format!("{http}/stream/{id}"))
        .header(reqwest::header::RANGE, "bytes=1048576-2097151")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(
        resp.headers()[reqwest::header::CONTENT_RANGE].to_str().unwrap(),
        format!("bytes 1048576-2097151/{}", contents.len())
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &contents[1048576..2097152]);

    // Suffix range.
    let resp = client
        .get(format!("{http}/stream/{id}"))
        .header(reqwest::header::RANGE, "bytes=-1024")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    let n = contents.len();
    assert_eq!(body.as_ref(), &contents[n - 1024..]);

    // ---- WebSocket ----
    let (mut socket, _resp) =
        tokio_tungstenite::connect_async(format!("{wsur}/ws/torrent/{id}"))
            .await
            .expect("ws connect");
    let msg = timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("ws timeout")
        .expect("ws closed early")
        .expect("ws error");
    let text = match msg {
        tokio_tungstenite::tungstenite::Message::Text(t) => t.to_string(),
        other => panic!("expected text message, got {other:?}"),
    };
    let s: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(s["id"].as_i64().unwrap(), id);
    assert_eq!(s["total_bytes"].as_u64().unwrap(), contents.len() as u64);
    socket.close(None).await.ok();
    drop(srv);

    // ---- Delete (hard: full movie delete via ?hard=1) ----
    let resp = client
        .delete(format!("{http}/api/library/{id}?hard=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(library.db.get_item(id).unwrap().is_none());
    assert!(!temp_dir.exists(), "temp dir should be cleaned up on delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keep_true_persists_locally_and_survives_delete() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(2_000_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;

    let library = setup_app(&tmp.path().join("app")).await;

    let id = library
        .add_item_with_initial_peers(&magnet, true, vec![peer])
        .await
        .unwrap();

    let item = library.db.get_item(id).unwrap().unwrap();
    assert!(item.keep);
    assert!(item.file_path.is_some());
    assert!(item.file_path.as_ref().unwrap().contains("library"));

    wait_complete(&library, id).await;

    // Status becomes "ready" + local once finished.
    let ready = loop {
        let s = library.get_status(id).await.unwrap();
        if s.state == "ready" && s.is_local {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(ready.progress_pct, 100.0);
    assert_eq!(ready.total_bytes, contents.len() as u64);

    // Files are in the library dir.
    let dir = library.config.library_dir.join(id.to_string());
    let mut found = Vec::new();
    walk_files(&dir, &mut found);
    assert!(!found.is_empty(), "kept file should be in library dir");
    let matching = found
        .iter()
        .find(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) == contents.len() as u64)
        .expect("no kept file with matching size");
    assert_eq!(tokio::fs::read(matching).await.unwrap(), contents);

    // Stream works from disk after completion.
    let (http, _ws, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{http}/stream/{id}"))
        .header(reqwest::header::RANGE, "bytes=0-499")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &contents[..500]);
    drop(srv);

    // Deleting a kept item (hard: full movie delete) removes it from the
    // library but keeps the files on disk.
    let resp = client
        .delete(format!("{http}/api/library/{id}?hard=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(library.db.get_item(id).unwrap().is_none());
    assert!(dir.exists(), "kept files must survive delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_temp_item_to_kept_moves_files() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(2_000_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;

    let library = setup_app(&tmp.path().join("app")).await;

    let id = library
        .add_item_with_initial_peers(&magnet, false, vec![peer])
        .await
        .unwrap();
    wait_complete(&library, id).await;

    let temp_dir = library.config.temp_dir.join(id.to_string());
    let lib_dir = library.config.library_dir.join(id.to_string());
    let mut found = Vec::new();
    walk_files(&temp_dir, &mut found);
    assert_eq!(found.len(), 1, "only one file expected in temp dir");
    assert_eq!(tokio::fs::read(&found[0]).await.unwrap(), contents);

    // Promote through the real PATCH endpoint.
    let (http, _ws, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();
    let resp = client
        .patch(format!("{http}/api/library/{id}"))
        .json(&serde_json::json!({ "keep": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let item = library.db.get_item(id).unwrap().unwrap();
    assert!(item.keep);
    assert!(item.file_path.is_some());

    // Files moved into library dir.
    let mut moved = Vec::new();
    walk_files(&lib_dir, &mut moved);
    assert!(!moved.is_empty(), "files should be moved into library dir");
    let matching = moved
        .iter()
        .find(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0) == contents.len() as u64)
        .expect("no moved file with matching size");
    assert_eq!(tokio::fs::read(matching).await.unwrap(), contents);
    assert_eq!(matching.file_name().unwrap(), "movie.mp4");

    // Stream still works after promotion (from disk now).
    let resp = client.get(format!("{http}/stream/{id}")).send().await.unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), contents.as_slice());
    drop(srv);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remux_ts_file_streams_playable_mp4() {
    match tokio::process::Command::new("ffmpeg").arg("-version").output().await {
        Ok(out) if out.status.success() => {}
        _ => {
            eprintln!("SKIP: ffmpeg not available");
            return;
        }
    }

    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let video_dir = tmp.path().join("video");
    tokio::fs::create_dir_all(&video_dir).await.unwrap();
    let ts_path = video_dir.join("clip.ts");
    let gen = tokio::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=160x120:rate=15:duration=4",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-f",
            "mpegts",
        ])
        .arg(&ts_path)
        .output()
        .await
        .unwrap();
    assert!(
        gen.status.success(),
        "ffmpeg generation failed: {}",
        String::from_utf8_lossy(&gen.stderr)
    );
    let contents = tokio::fs::read(&ts_path).await.unwrap();
    assert!(contents.len() > 1000);

    let (_seeder, magnet, peer) = start_seeder(&tmp.path().join("seed"), "clip.ts", &contents).await;
    let library = setup_app(&tmp.path().join("app")).await;
    let id = library
        .add_item_with_initial_peers(&magnet, false, vec![peer])
        .await
        .unwrap();
    wait_complete(&library, id).await;

    let (http, _ws, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();
    let resp = client.get(format!("{http}/stream/{id}")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "remux serves full 200");
    assert_eq!(resp.headers()[reqwest::header::CONTENT_TYPE], "video/mp4");
    let body = resp.bytes().await.unwrap();
    assert!(body.len() > 1000, "expected remuxed mp4 data");
    drop(srv);

    let out_file = tmp.path().join("out.mp4");
    let mut f = tokio::fs::File::create(&out_file).await.unwrap();
    f.write_all(&body).await.unwrap();
    f.flush().await.unwrap();

    // Verify the remuxed output is a playable MP4 (H.264 video stream).
    let probe = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "csv=p=0",
        ])
        .arg(&out_file)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "ffprobe failed: {}",
        String::from_utf8_lossy(&probe.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&probe.stdout).trim(), "h264");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_websocket_sends_recurring_updates() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(6_000_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;

    let library = setup_app(&tmp.path().join("app")).await;
    let id = library
        .add_item_with_initial_peers(&magnet, false, vec![peer])
        .await
        .unwrap();

    let (_http, wsur, srv) = start_http(library.clone()).await;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("{wsur}/ws/torrent/{id}"))
            .await
            .unwrap();

    let mut saw_downloading = false;
    let mut saw_complete = false;
    for _ in 0..30 {
        let msg = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("ws timeout")
            .expect("ws closed")
            .expect("ws error");
        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => t.to_string(),
            other => panic!("unexpected msg {other:?}"),
        };
        let s: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(s["id"].as_i64().unwrap(), id);
        let state = s["state"].as_str().unwrap();
        assert!(matches!(state, "connecting" | "downloading" | "ready"));
        if state == "downloading" {
            saw_downloading = true;
        }
        if s["contiguous_bytes"].as_u64().unwrap() == s["total_bytes"].as_u64().unwrap()
            && (state == "downloading" || state == "ready")
        {
            saw_complete = true;
            break;
        }
    }
    assert!(saw_downloading, "expected to observe downloading state");
    assert!(saw_complete, "expected to observe completion over WS");
    socket.close(None).await.ok();
    drop(srv);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_error_handling() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
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
async fn completed_temp_item_survives_restart() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(2_000_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;

    let config = Config {
        port: 0,
        library_dir: tmp.path().join("library"),
        temp_dir: tmp.path().join("temp"),
        db_path: tmp.path().join("test.db"),
        tmdb_api_key: None,
    };

    // First instance downloads the temp item to completion.
    {
        let library = Arc::new(LibraryManager::new(config.clone()).await.unwrap());
        let id = library
            .add_item_with_initial_peers(&magnet, false, vec![peer])
            .await
            .unwrap();
        wait_complete(&library, id).await;

        // Completion must persist the on-disk location + ready state so the
        // item is streamable even without the torrent engine.
        let item = library.db.get_item(id).unwrap().unwrap();
        assert_eq!(item.state, "ready");
        assert!(item.file_path.is_some(), "completed temp item should record its path");
        assert_eq!(
            item.file_path.as_ref().unwrap(),
            &config.temp_dir.join(id.to_string()).to_string_lossy().to_string()
        );

        library.stop().await;
    }

    // "Reboot": fresh instance, same db + dirs. The engine has no handle
    // anymore, but streaming must keep working from disk.
    let library2 = Arc::new(LibraryManager::new(config).await.unwrap());
    let item = library2.db.get_item(1).unwrap().unwrap();
    assert_eq!(item.state, "ready");

    let status = library2.get_status(1).await.unwrap();
    assert!(status.is_local, "reconnected item should be local");
    assert_eq!(status.progress_pct, 100.0);

    let (http, _ws, srv) = start_http(library2.clone()).await;
    let resp = reqwest::get(format!("{http}/stream/1")).await.unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.headers()[reqwest::header::CONTENT_TYPE], "video/mp4");
    assert_eq!(resp.bytes().await.unwrap().as_ref(), contents.as_slice());
    drop(srv);

    library2.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconcile_marks_preexisting_completed_download_ready() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();

    // Simulate state from an older version: fully downloaded temp files on
    // disk, but the DB never recorded file_path or ready.
    let db = chadtorrent::db::Database::new(tmp.path().join("test.db").to_str().unwrap()).unwrap();
    let temp_dir = tmp.path().join("temp");
    let library_dir = tmp.path().join("library");
    let id = db
        .insert_item("magnet:?xt=urn:btih:abc123", false)
        .unwrap();
    let contents = make_pattern(1_000_000);
    let out = temp_dir.join(id.to_string());
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("movie.mp4"), &contents).unwrap();
    db.update_title(id, "Old Download").unwrap();
    db.update_size(id, contents.len() as i64).unwrap();
    db.update_state(id, "downloading").unwrap();

    let config = Config {
        port: 0,
        library_dir: tmp.path().join("library"),
        temp_dir: tmp.path().join("temp"),
        db_path: tmp.path().join("test.db"),
        tmdb_api_key: None,
    };
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
async fn per_file_remove_keeps_movie_as_placeholder() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let config = Config {
        port: 0,
        library_dir: tmp.path().join("library"),
        temp_dir: tmp.path().join("temp"),
        db_path: tmp.path().join("test.db"),
        tmdb_api_key: None,
    };
    let library = Arc::new(LibraryManager::new(config).await.unwrap());

    let id = library
        .db
        .insert_item("magnet:?xt=urn:btih:abc123", true)
        .unwrap();
    library.db.update_title(id, "Legacy Movie").unwrap();

    // Per-file delete must NOT remove the movie: it converts the row into an
    // empty movie placeholder so the movie stays in the library.
    library.remove_item(id).await.unwrap();

    let item = library.db.get_item(id).unwrap().unwrap();
    assert_eq!(item.title.as_deref(), Some("Legacy Movie"), "title must survive per-file delete");
    assert_eq!(item.magnet, "");
    assert_eq!(item.state, "ready");
    assert!(item.file_path.is_none());
    assert!(item.keep);

    library.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_keeps_progress_visible() {
    let _permit = E2E_LOCK.acquire().await.unwrap();
    let tmp = TempDir::new().unwrap();
    let contents = make_pattern(250_000_000);
    let (_seeder, magnet, peer) = start_seeder(tmp.path(), "movie.mp4", &contents).await;
    let library = setup_app(&tmp.path().join("app")).await;

    let id = library
        .add_item_with_initial_peers(&magnet, false, vec![peer])
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let st = library.get_status(id).await.unwrap();
        if st.state == "downloading" && st.contiguous_bytes > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "torrent never started downloading: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    library.pause_item(id).await.unwrap();
    let p1 = library.get_status(id).await.unwrap();
    assert_eq!(p1.state, "paused");
    assert!(
        p1.progress_pct > 0.0,
        "progress must survive pause, got {}",
        p1.progress_pct
    );

    tokio::time::sleep(Duration::from_millis(600)).await;
    let p2 = library.get_status(id).await.unwrap();
    assert_eq!(p2.state, "paused");
    assert_eq!(p1.progress_pct, p2.progress_pct, "paused progress must not advance");

    library.resume_item(id).await.unwrap();
    let r1 = library.get_status(id).await.unwrap();
    assert_eq!(r1.state, "downloading");
    assert!(r1.progress_pct >= p1.progress_pct);

    wait_complete(&library, id).await;

    library.stop().await;
}