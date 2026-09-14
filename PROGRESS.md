# Torrent Streamer - Implementation Progress

## Project Overview
Self-hosted service: paste magnet link → added to persistent library → sequential torrent download → immediate HTTP streaming to web client, with real-time status (progress, peers, speed, buffer health).

## Phase 0: Spike - Validate librqbit Capabilities
**Status**: COMPLETED

**Findings:**
- librqbit v9.0.1 API confirmed:
  - `Session::new(path)` creates async session with DHT + TCP listener
  - `session.add_torrent(AddTorrent::from_url(magnet), opts)` adds torrents
  - `ManagedTorrent::stream(file_id)` returns `FileStream` (implements `AsyncRead + AsyncSeek`)
  - `FileStream` blocks (`Poll::Pending`) until the piece at read position is downloaded — this is exactly what we need for streaming
  - `ManagedTorrent::stats()` returns `TorrentStats` with progress, speed, peers
  - `ManagedTorrent::with_metadata()` gives access to file info (names, sizes, IDs)
  - `ManagedTorrent::output_folder()` returns resolved on-disk path
  - Sequential downloading is the default mode
  - Multiple concurrent torrents supported natively
  - `AddTorrentOptions.output_folder: Option<String>` controls download directory
  - `AddTorrentResponse::into_handle()` returns `ManagedTorrentHandle` (which is `Arc<ManagedTorrent>`)
  - `TorrentStatsState` enum: `Initializing { paused }`, `Live`, `Paused`, `Error`
  - `TorrentStats` fields: `progress_bytes`, `total_bytes`, `finished`, `live: Option<LiveStats>`, `state`, `file_progress`
  - `LiveStats` has `download_speed: Speed` (`.as_bytes()` returns bytes/sec) and `snapshot.peer_stats.live` for peer count
  - `TorrentMetadata.file_infos: Vec<FileInfo>` with `relative_filename`, `len`, `offset_in_torrent`, `piece_range`

**Fallback needed**: None, librqbit works perfectly.

---

## Phase 1: Library DB + Core Torrent + File Serving
**Status**: COMPLETED

**Built:**
- SQLite database (`rusqlite`): `library(id, magnet, title, keep, state, added_at, file_path, size_bytes, rqbit_id)`
- All REST endpoints:
  - `POST /api/library` — add magnet (with optional keep flag)
  - `GET /api/library` — list all items
  - `PATCH /api/library/:id` — toggle keep flag (with promotion logic)
  - `DELETE /api/library/:id` — remove item + cleanup
  - `GET /api/library/:id/status` — one-shot status JSON
- `GET /stream/:id` — HTTP range serving with 206 Partial Content:
  - If kept + complete → serve directly from local file (fastest path)
  - If streaming → use librqbit's `FileStream` which blocks until pieces arrive
  - If remux needed → pipe through ffmpeg subprocess
  - If file growing → wait-and-retry with bounded attempts
- Library manager with keep toggle logic (promotes temp files to permanent storage)
- Torrent engine wrapping librqbit's `Session` + `ManagedTorrent` lifecycle
- Multiple concurrent torrents supported

**Tests:** 21 passing (10 HTTP range parsing + 11 DB operations)

---

## Phase 2: Remux for Browser Compatibility
**Status**: COMPLETED

**Built:**
- Container detection: checks extension and `ffprobe` codec/tag info
- `ffprobe` detection: identifies MKV, AVI, non-faststart MP4, and containers that need remuxing
- `ffmpeg -c copy -movflags frag_keyframe+empty_moov+faststart` remux pipeline
- Remuxes via `pipe:1` (stdout), streams directly as HTTP response body
- Zero re-encoding (`-c copy`), near-zero CPU

**Behavior:**
- MKV sources → remuxed to fragmented MP4 in real-time
- AVI/WMV/FLV/TS/etc → remuxed
- Faststart MP4 with H.264 → served directly
- Detection uses both extension heuristic and ffprobe

---

## Phase 3: WebSocket Status Stream
**Status**: COMPLETED

**Built:**
- `GET /ws/torrent/:id` — WebSocket endpoint
- Pushes JSON status every ~1 second
- Status includes: id, state, progress_pct, download_speed_bps, peers, contiguous_bytes, total_bytes, keep, is_local
- Handles client disconnect gracefully
- Error messages sent as JSON `{"type": "error", "message": "..."}`

**Logging (tracing):**
- INFO level: torrent added, metadata resolved, stream started
- Default env-filter: `info,tower_http=info`

---

## Phase 4: Reliability Hardening
**Status**: COMPLETED

**Implemented:**
- Graceful shutdown: SIGINT/SIGTERM → stops torrent engine cleanly
- Client disconnect mid-stream: handled via Drop on FileStream + connection close
- No orphaned ffmpeg processes: `kill_on_drop(true)` on child process
- DB operations under Mutex lock (no concurrent corruption)
- Error types properly map to HTTP status codes (404, 400, 500, 502)
- Library item deletion cleans up temp files and torrent engine state
- Server does not crash on invalid magnets, network errors, or DB errors

---

## Phase 5: Frontend
**Status**: COMPLETED

**Built:**
- Single `static/index.html`, inline JS, zero CSS, zero framework
- Add magnet form with keep checkbox
- Library list with state, progress, keep checkbox (PATCH on change), Play/Delete buttons
- Native `<video controls>` element
- Real-time status display via WebSocket
- Speed/peer/buffer health stats
- Refresh polling every 3 seconds
- Play button sets `video.src` for native range-request streaming

---

## Phase 6: Testing
**Status**: COMPLETED

**Unit tests:**
- `http::tests`: 10 tests — range header parsing (no range, bytes=0-, bytes=-500, specific, invalid, past-end, zero suffix) + video file selection + MIME type detection
- `db::tests`: 11 tests — insert/get, keep flag, state update, title update, list, delete, nonexistent, rqbit_id, file_path, find by rqbit_id

**Build:** Clean build with 0 warnings
**Tests:** 21 passing, 0 failing

---

## Phase 7: Real End-to-End Verification + Bug Fixes
**Status**: COMPLETED

Full-stack integration tests (`tests/e2e.rs`) exercising the app with a **local librqbit seeder** (no internet). 6 tests:

1. `keep_false_full_flow_streaming_ws_delete` — add temp torrent → **stream the first 2048 bytes BEFORE download completes** → wait for completion → verify downloaded bytes are byte-identical to the seed → full/range/suffix range requests over real HTTP (206 + correct Content-Range) → WebSocket status message → delete removes item + temp dir
2. `keep_true_persists_locally_and_survives_delete` — kept torrent lands in library_dir, status becomes `ready`/`is_local`, streams from disk after completion, delete removes from DB but **keeps files on disk**
3. `promote_temp_item_to_kept_moves_files` — add temp, complete, promote via real `PATCH /api/library/:id` → files moved temp→library, keep flag + file_path set, streaming still works from disk
4. `remux_ts_file_streams_playable_mp4` — real MPEG-TS seed → `/stream/:id` remuxes via ffmpeg → output verified **playable** (ffprobe reports h264 in mp4)
5. `status_websocket_sends_recurring_updates` — WS pushes recurring status messages, observes `downloading` state and completion
6. `http_error_handling` — invalid magnet→400, missing item status/stream/delete→404

**Real bugs found & fixed during verification:**

- **Server wouldn't start** — librqbit's default DHT persistence tried to load/write `~/Library/Caches/com.rqbit.dht/dht.json` and failed (`error initializing persistent DHT`). Fixed by constructing `SessionOptions` with DHT persistence and session persistence disabled (our own SQLite is the source of truth).
- **`/` returned literal HTML** — axum returns a bare `String` as `text/plain`; the root handler now sets `Content-Type: text/html; charset=utf-8`.
- **Ground-up integration test harness** — `LibraryManager::add_item_with_initial_peers` to deterministically reach a local seeder via `initial_peers` (no DHT/tracker race).
- **Dead stream on remux** — `serve_remux` moved `ChildStdout` out of `Child`, then dropped the `Child`, and `kill_on_drop(true)` killed ffmpeg before the response body was served (empty/garbage output). Fixed with a `RemuxTask` that owns the `Child` alongside its stdout so both live and die together with the streaming body.
- **Promoted items 404'd on stream** — after `PATCH keep=true` moved files to the library dir, the item stayed `downloading`, so `stream_handler` skipped the local-disk path and looked in the now-empty torrent folder. `set_keep` now marks finished items `ready` after promotion.
- **Kept `.ts` items served raw** — a completed kept `.ts` (needs remux) was served directly. `stream_handler` now remuxes local kept files that need it, same as the streaming path.
- **Contiguous bytes** — now reported from `stats.file_progress[<largest video file>]` instead of total `progress_bytes`, so progress bars reflect the actual streamed file.

**Also fixed:** orphaned server processes during manual smoke tests (backgrounding + kill targeting), stale release binary confusion, `chrono` unused dep removed, clippy warnings (0).

**Final state:** 21 unit + 6 integration tests, 0 failures, 0 warnings, clippy clean, graceful SIGINT/SIGTERM verified with no stray processes.

## Deviations from Plan

1. **MSE vs native streaming**: Used native `<video src>` instead of hand-rolled MSE. Native `src` works perfectly with HTTP range requests + 206 responses, handles seeking automatically, and is orders of magnitude simpler. The plan explicitly said "use native controls for v1 — skip custom control UI until pipeline is proven stable."

2. **FileStream instead of file polling**: Instead of manually polling file size for range serving, used librqbit's native `FileStream` which implements `AsyncRead + AsyncSeek` and blocks until pieces are downloaded. This is more correct, simpler, and handles piece prioritization automatically.

3. **Timeout on reads**: Removed the per-read timeout wrapper from streaming. The client handles timeouts via browser behavior (connection drop, seeking). The WebSocket status shows stalled state so the user knows what's happening. A timeout wrapper adds complexity and can cause premature stream termination.

4. **No moov-atom special casing needed**: librqbit's `iter_piece_priorities` already downloads first and last pieces first, which handles the moov-at-end case for MP4 files.

---

## Known Limitations (v1)

- No transcoding (only remux via `-c copy`)
- No search/indexing
- No auth/users
- No custom video player UI
- No adaptive bitrate
- No mobile-specific UI
- Resume support: files persist across restarts but torrent engine state does not
- Remuxed streams don't support range/seeking (full-body progressive)
- Contiguous bytes approximated via librqbit `file_progress` for the largest video file (accurate because pieces download sequentially)

---

## File Structure

```
chadtorrent/
├── Cargo.toml
├── PROGRESS.md
├── src/
│   ├── main.rs          — Entry point, graceful shutdown, logging setup
│   ├── lib.rs           — Module declarations
│   ├── config.rs        — CLI args (port, library_dir, temp_dir, db_path)
│   ├── db.rs            — SQLite library CRUD (11 unit tests)
│   ├── error.rs         — Error types + HTTP status code mapping
│   ├── torrent.rs       — librqbit Session/MannedTorrent wrapper
│   ├── library.rs       — Library manager (orchestrates DB + torrent + file paths)
│   ├── http.rs          — Axum server (REST + range serving + WS + remux task, 10 unit tests)
│   └── remux.rs         — ffmpeg subprocess for MKV/container remux
├── static/
│   └── index.html       — Single-file frontend (inline JS, no CSS/framework)
└── tests/
    └── e2e.rs           — 6 full-stack integration tests with local librqbit seeder
```
