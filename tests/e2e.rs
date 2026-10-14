use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use chadtorrent::config::Config;
use chadtorrent::http::{AppState, build_router};
use chadtorrent::library::LibraryManager;
use chadtorrent::metadata::MetaCache;
use chadtorrent::party::PartyManager;
use chadtorrent::reading::ReadingClient;

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
        party_relay: None,
    }
}

async fn setup_app(tmp: &Path) -> Arc<LibraryManager> {
    LibraryManager::new(config_for(&tmp.join("app"))).await.unwrap()
}

async fn start_http(
    library: Arc<LibraryManager>,
) -> (String, String, Arc<PartyManager>, tokio::task::AbortHandle) {
    let index_html = include_str!("../static/index.html").to_string();
    let party = PartyManager::new(library.clone());
    let meta = MetaCache::new(
        library.db.clone(),
        None,
        Arc::new(ReadingClient::new(None)),
        Arc::new(chadtorrent::reader::ReaderClient::new()),
    );
    let router = build_router(AppState { library, index_html, tmdb: None, meta: Some(Arc::new(meta)), subs: None, party: party.clone(), reading: Arc::new(ReadingClient::new(None)), reader: Arc::new(chadtorrent::reader::ReaderClient::new()) });
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let http = format!("http://{addr}");
    let ws = format!("ws://{addr}");
    (http, ws, party, handle.abort_handle())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_error_handling() {
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;
    let (http, _ws, _party, srv) = start_http(library.clone()).await;
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

    // watch-party surface: no party yet, and garbage invites are rejected
    let resp = client.get(format!("{http}/api/party")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.json::<serde_json::Value>().await.unwrap()["party"].is_null());
    let resp = client
        .post(format!("{http}/api/party/join"))
        .json(&serde_json::json!({ "invite": "CTPARTY1:garbage!" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    drop(srv);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tmdb_meta_serves_seasons_and_episodes_from_cache() {
    use chadtorrent::metadata::{EpisodeInfo, MetaCache, SeasonEpisodes, SeasonInfo, TmdbClient};
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;

    // Seed the cache the way a completed background sweep would have, so no
    // TMDB request is made (the client key below is intentionally bogus).
    let seasons = vec![
        SeasonInfo {
            season_number: 1,
            name: Some("Season 1".into()),
            episode_count: Some(2),
            air_date: None,
        },
        SeasonInfo {
            season_number: 2,
            name: Some("Season 2".into()),
            episode_count: Some(1),
            air_date: None,
        },
    ];
    let episodes = vec![
        SeasonEpisodes {
            season_number: 1,
            episodes: (1..=2)
                .map(|n| EpisodeInfo {
                    episode_number: n,
                    name: Some(format!("Ep {n}")),
                    overview: None,
                    still_path: None,
                    air_date: None,
                })
                .collect(),
        },
        SeasonEpisodes {
            season_number: 2,
            episodes: vec![EpisodeInfo {
                episode_number: 1,
                name: Some("Premiere".into()),
                overview: None,
                still_path: None,
                air_date: None,
            }],
        },
    ];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    library
        .db
        .set_tmdb_meta("tv", 1396, Some("Seven noble families."), &seasons, &episodes, now)
        .unwrap();

    let meta = MetaCache::new(
        library.db.clone(),
        Some(Arc::new(TmdbClient::new("bogus-key".into()))),
        Arc::new(ReadingClient::new(None)),
        Arc::new(chadtorrent::reader::ReaderClient::new()),
    );
    let index_html = include_str!("../static/index.html").to_string();
    let party = PartyManager::new(library.clone());
    let router = build_router(AppState {
        library,
        index_html,
        tmdb: Some(Arc::new(TmdbClient::new("bogus-key".into()))),
        meta: Some(Arc::new(meta)),
        subs: None,
        party: party.clone(),
        reading: Arc::new(ReadingClient::new(None)),
        reader: Arc::new(chadtorrent::reader::ReaderClient::new()),
    });
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let http = format!("http://{addr}");
    let client = reqwest::Client::new();

    // The detail payload carries the episode map keyed by season, which is what
    // lets a season click paint its buttons with zero further requests.
    let detail: serde_json::Value = client
        .get(format!("{http}/api/meta/tv/1396"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["overview"], "Seven noble families.");
    assert_eq!(detail["seasons"][0]["season_number"], 1);
    assert_eq!(detail["episodes"]["1"].as_array().unwrap().len(), 2);
    assert_eq!(detail["episodes"]["1"][1]["name"], "Ep 2");
    assert_eq!(detail["episodes"]["2"][0]["name"], "Premiere");

    // The per-season endpoint is answered from the same cache, not TMDB.
    let season: serde_json::Value = client
        .get(format!("{http}/api/meta/tv/1396/season/2"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(season["episodes"][0]["name"], "Premiere");

    // A fresh cache short-circuits the background check: no TMDB traffic, and
    // the caller is told nothing changed.
    let refreshed: serde_json::Value = client
        .post(format!("{http}/api/meta/tv/1396/refresh"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(refreshed["updated"], false);

    srv.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tmdb_meta_routes_need_configured_key() {
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;
    let (http, _ws, _party, srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();

    for (method, path) in [
        ("GET", "/api/meta/tv/1396"),
        ("GET", "/api/meta/tv/1396/season/1"),
        ("POST", "/api/meta/tv/1396/refresh"),
        ("GET", "/api/meta/movie/157336"),
    ] {
        let resp = match method {
            "POST" => client.post(format!("{http}{path}")).send().await.unwrap(),
            _ => client.get(format!("{http}{path}")).send().await.unwrap(),
        };
        assert_eq!(resp.status(), 400, "{method} {path}");
    }

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
        .insert_movie("My Movie", Some(2024), None, 999, "movie", None, None)
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
/// Reading items (manga/comics): added as metadata-only rows via the movies
/// endpoint, listed with their synopsis, deleted like anything else. The
/// comics search reports its setup instructions while no key is configured,
/// and a hard delete never reaches across catalogues that merely share an
/// id (a ComicVine 123 and a TMDB 123 are different titles).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_item_lifecycle() {
    let tmp = TempDir::new().unwrap();
    let library = setup_app(&tmp.path().join("app")).await;
    let (http, _ws, _party, _srv) = start_http(library.clone()).await;
    let client = reqwest::Client::new();

    // Add a manga entry the way the search modal does.
    let resp = client
        .post(format!("{http}/api/movies"))
        .json(&serde_json::json!({
            "title": "Berserk",
            "year": 1989,
            "poster_path": "https://uploads.mangadex.org/covers/x/cover.jpg",
            "tmdb_id": 30002,
            "media_type": "manga",
            "overview": "Guts, a lone mercenary.",
            "ext_key": "801513ba-a712-498c-8f57-cae55b38cc92"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let manga_id: i64 = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_i64()
        .unwrap();

    // It shows up in the library list with its metadata attached.
    let items: serde_json::Value = client
        .get(format!("{http}/api/library"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = items
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"].as_i64() == Some(manga_id))
        .unwrap()
        .clone();
    assert_eq!(row["media_type"], "manga");
    assert_eq!(row["overview"], "Guts, a lone mercenary.");
    assert_eq!(
        row["poster_path"],
        "https://uploads.mangadex.org/covers/x/cover.jpg"
    );
    assert_eq!(row["ext_key"], "801513ba-a712-498c-8f57-cae55b38cc92");
    assert_eq!(row["state"], "ready");
    assert_eq!(row["magnet"], "");

    // Comics search without a configured key: a 400 whose body explains the fix.
    let resp = client
        .get(format!("{http}/api/search/comics?q=blake"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let text = resp.text().await.unwrap();
    assert!(text.contains("comicvine_api_key"), "helpful error, got: {text}");

    // An empty manga query is an empty result, not an upstream request.
    let resp = client
        .get(format!("{http}/api/search/manga?q="))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Chapter lists share the TV meta endpoint and cache row shape: seeded
    // here (no network), served as episodes keyed by season.
    use chadtorrent::metadata::{EpisodeInfo, SeasonEpisodes, SeasonInfo};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let seasons = vec![SeasonInfo {
        season_number: 1,
        name: Some("Chapters".into()),
        episode_count: Some(2),
        air_date: None,
    }];
    let chapters = vec![SeasonEpisodes {
        season_number: 1,
        episodes: (1..=2)
            .map(|n| EpisodeInfo {
                episode_number: n,
                name: None,
                overview: None,
                still_path: None,
                air_date: None,
            })
            .collect(),
    }];
    library
        .db
        .set_tmdb_meta("manga", 30002, Some("Guts, a lone mercenary."), &seasons, &chapters, now)
        .unwrap();
    let detail: serde_json::Value = client
        .get(format!("{http}/api/meta/manga/30002"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail["episodes"]["1"].as_array().unwrap().len(), 2);
    assert_eq!(detail["overview"], "Guts, a lone mercenary.");

    // Hard delete removes the manga; a same-id movie in another catalogue
    // is untouched.
    let movie_id = library
        .db
        .insert_movie("Same Number", Some(2001), None, 30002, "movie", None, None)
        .unwrap();
    let resp = client
        .delete(format!("{http}/api/library/{manga_id}?hard=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(library.db.get_item(manga_id).unwrap().is_none());
    assert!(library.db.get_item(movie_id).unwrap().is_some());

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
    let (http, _ws, _party, _srv) = start_http(library.clone()).await;
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

/* ---------- watch party ---------- */

/// Full two-instance party: A creates (its own instance hosts the punch
/// hub), B joins via the invite — torrent added + deduped by infohash —
/// and B's action reaches A's roster through the punched P2P pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_party_join_and_action_sync() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .with_test_writer()
        .try_init();
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let library_a = setup_app(&tmp_a.path().join("app")).await;
    let (_http_a, ws_a, party_a, srv_a) = start_http(library_a.clone()).await;
    start_punch_hub(&ws_a).await;

    // Creator's item: any state works for party creation (magnet is enough).
    let ida = library_a
        .db
        .insert_item("magnet:?xt=urn:btih:FEEDCAFEBABE0000000000000000000000000000&dn=test")
        .unwrap();
    library_a
        .db
        .update_metadata(ida, Some("Party Movie"), Some(1999), None, Some(777), Some("movie"))
        .unwrap();

    let (invite, _lan_only) = party_a
        .create(ida, "alice", Some(ws_a.clone()), None)
        .unwrap();
    assert!(invite.starts_with("CTPARTY1:"));

    // Alone in the room but hub reachable: already online.
    wait_for(&party_a, |s| s["party"]["conn"] == serde_json::json!("online")).await;

    // Joiner: its own instance, its own library.
    let library_b = setup_app(&tmp_b.path().join("app")).await;
    let (http_b, _ws_b, party_b, srv_b) = start_http(library_b.clone()).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{http_b}/api/party/join"))
        .json(&serde_json::json!({ "invite": invite, "name": "bob" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    let item_b = v["item_id"].as_i64().unwrap();

    // The torrent was added exactly once with the invite's metadata.
    let items = library_b.db.list_items().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, item_b);
    assert_eq!(items[0].tmdb_id, Some(777));
    assert_eq!(items[0].title.as_deref(), Some("Party Movie"));

    // Joining twice with the same invite dedupes by infohash.
    let resp = client
        .post(format!("{http_b}/api/party/join"))
        .json(&serde_json::json!({ "invite": invite, "name": "bob" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(library_b.db.list_items().unwrap().len(), 1);

    // B punches through A's hub; rosters converge on both sides.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let snap = party_b.snapshot_json();
        let conn = snap["party"]["conn"].as_str().unwrap_or("");
        if conn == "online" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "B should connect to the relay");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    wait_for(&party_a, |snap| {
        snap["party"]["peers"]
            .as_array()
            .is_some_and(|p| p.iter().any(|x| x["name"] == serde_json::json!("bob")))
    })
    .await;
    wait_for(&party_b, |snap| {
        snap["party"]["peers"]
            .as_array()
            .is_some_and(|p| p.iter().any(|x| x["name"] == serde_json::json!("alice")))
    })
    .await;

    // B plays: the action must land in A's roster (server-side ferry).
    party_b.send_action("play", 42.0, None);
    wait_for(&party_a, |snap| {
        snap["party"]["peers"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .any(|x| x["name"] == serde_json::json!("bob") && x["pos"].as_f64() == Some(42.0))
    })
    .await;

    // Leave cleans up: snapshot nulls out and the DB row is gone.
    party_b.leave().unwrap();
    assert!(library_b.db.get_party().unwrap().is_none());
    assert!(party_b.snapshot_json()["party"].is_null());

    drop(srv_a);
    drop(srv_b);
    library_a.stop().await;
    library_b.stop().await;
}

/// Poll a manager's snapshot until `pred` holds (or panic after ~10s).
async fn wait_for(
    party: &Arc<PartyManager>,
    pred: impl Fn(&serde_json::Value) -> bool,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let snap = party.snapshot_json();
        if pred(&snap) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "condition not met, last snapshot: {snap}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Start the NAT-punch UDP hub on the same port as the instance's HTTP
/// server (clients derive the UDP endpoint from the invite's relay URL).
async fn start_punch_hub(ws_base: &str) {
    let port: u16 = ws_base
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("ws base with port");
    let udp = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    tokio::spawn(chadtorrent::party::punch::run_punch_server(udp));
}

/// With a punch-capable relay, two members punch over loopback and sync
/// messages flow peer-to-peer (transport flips from "relay" to "udp").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_party_punches_into_peer_to_peer() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .with_test_writer()
        .try_init();
    let tmp_a = TempDir::new().unwrap();
    let tmp_b = TempDir::new().unwrap();
    let library_a = setup_app(&tmp_a.path().join("app")).await;
    let (_http_a, ws_a, party_a, srv_a) = start_http(library_a.clone()).await;
    start_punch_hub(&ws_a).await;

    let ida = library_a
        .db
        .insert_item("magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&dn=punch")
        .unwrap();
    let (invite, _lan) = party_a.create(ida, "alice", Some(ws_a.clone()), None).unwrap();

    let library_b = setup_app(&tmp_b.path().join("app")).await;
    let (http_b, _ws_b, party_b, srv_b) = start_http(library_b.clone()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{http_b}/api/party/join"))
        .json(&serde_json::json!({ "invite": invite, "name": "bob" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Both sides punch and come online (peer-to-peer only).
    wait_for(&party_a, |s| s["party"]["conn"] == serde_json::json!("online")).await;
    wait_for(&party_b, |s| s["party"]["conn"] == serde_json::json!("online")).await;

    // An action now travels the punched UDP path end-to-end — including the
    // acting member's file (episode scoping for TV parties).
    party_b.send_action("play", 7.5, Some("Show/S01E01.mkv"));
    wait_for(&party_a, |snap| {
        snap["party"]["peers"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .any(|x| x["name"] == serde_json::json!("bob") && x["pos"].as_f64() == Some(7.5))
    })
    .await;
    wait_for(&party_a, |snap| {
        snap["party"]["file"] == serde_json::json!("Show/S01E01.mkv")
            && snap["party"]["peers"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .any(|x| x["name"] == serde_json::json!("bob") && x["file"] == serde_json::json!("Show/S01E01.mkv"))
    })
    .await;

    drop(srv_a);
    drop(srv_b);
    library_a.stop().await;
    library_b.stop().await;
}
