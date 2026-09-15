# AGENTS.md

## What this is

Single-binary self-hosted torrent streamer: paste a magnet link (or search via indexer) → torrent downloads to `library_dir/{id}` → streams to browser via HTTP range requests. Built on vendored librqbit (local DHT-only, no trackers/DHT persistence; our SQLite is the source of truth).

## Build / test commands

```
cargo build          # fast incremental build
cargo test -- --test-threads=1   # full suite (~4s, all fast — no real torrent downloads)
cargo clippy --all-targets
```

Every edit to `static/index.html` requires `cargo build` to take effect — it is embedded via `include_str!`.

## Architecture

```
src/
  main.rs        Entry point, graceful SIGINT/SIGTERM, logging setup
  config.rs      CLI args: --port, --library-dir, --temp-dir, --db-path, --tmdb-api-key
  db.rs          SQLite CRUD via rusqlite (library items, metadata, state)
  torrent.rs     TorrentEngine: wraps librqbit Session + ManagedTorrent map
  library.rs     LibraryManager: orchestrates DB + TorrentEngine + file paths
  http.rs        Axum server: REST routes + WS status + streaming + remux
  remux.rs       ffmpeg MKV→MP4 remux via pipe (no re-encoding)
  metadata.rs    TMDB API client (search + detail fetch)
  error.rs       Error types mapping to HTTP status codes
  lib.rs         Module re-exports

static/
  index.html     Single-file frontend (no framework, all JS inline)

tests/
  e2e.rs         5 fast integration tests (no torrent downloads; use fake files + DB)
```

## Data flow

1. **Add torrent** (`POST /api/library`): inserts row immediately with state `connecting`, returns id, then spawns a background task that registers the torrent with librqbit (which may take seconds). The torrent's `tmdb_id`/metadata is applied immediately by the handler, so grouping is correct before the engine finishes.

2. **Download**: librqbit downloads sequentially to `library_dir/{id}/`. State flows `connecting → downloading → ready`. On failure: `error`.

3. **Stream** (`GET /stream/:id`): if file exists locally → serve from disk with range support. If downloading → use librqbit's `FileStream` (blocks until requested piece arrives). If container needs remux (MKV/AVI/etc) → pipe through ffmpeg.

4. **Delete** (`DELETE /api/library/:id`): removes the row, deletes `library_dir/{id}` and `temp_dir/{id}`, removes from engine. If `?hard=1`, cascades to all torrents sharing the same `tmdb_id` (movie delete).

## Frontend conventions

- **No framework**: all vanilla JS. Modules use global functions (no bundler).
- **Movie grouping**: items grouped by `media_type + ':' + tmdb_id` (or `'r' + id` for standalone torrents). The movie detail shows a placeholder row (tmdb_id, no magnet) plus all attached torrent rows.
- **Optimistic add**: clicking "Add" in torrent search pushes a synthetic row (`state: '_pending'`) into `items` instantly, shows "Adding…", then the server call completes in the background. The synthetic row is removed once the POST returns and `refreshLibrary` picks up the real row.

- **Row layout**: all torrent rows have identical structure (title + subline + controls) with `min-height: 68px` so state transitions don't cause layout shifts. Rows update in place (keyed by id) — never wipe and rebuild `#mv-torrents`.
- **In-place updates**: `renderMovie()` matches existing DOM nodes by `data-id` and updates them, only adding/removing nodes when the row set changes. This prevents flash when a row transitions between states.

## Key gotchas

- `static/index.html` is embedded at compile time. Any frontend edit requires `cargo build` before it shows up in the binary.
- Torrent registration is non-blocking (returns immediately). A row can sit in `connecting` state for seconds; the frontend shows "Adding…" during this time. The Play button is disabled until the state advances.

- `librqbit` is vendored locally at `vendor/librqbit/` with a Cargo path override in `Cargo.toml`.

## Database

SQLite via rusqlite. Schema lives in `db.rs`. Migration logic: `CREATE TABLE IF NOT EXISTS` (safe if column list changes across versions). `ON DELETE CASCADE` is NOT used — app-level cascading in `remove_item_inner` deletes related rows and files explicitly.

Key columns: `id, magnet, title, state, added_at, file_path, size_bytes, tmdb_id, media_type, poster_path, year`.

## Test suite

- **Unit tests** in `http.rs` (range parsing, MIME detection, video file selection) and `db.rs` (CRUD, migration).
- **E2e tests** in `tests/e2e.rs`: no torrent downloads. Use `db.insert_item` directly, create fake files on disk, verify delete/rollback/cascade behavior, HTTP error codes.
- Run with `cargo test -- --test-threads=1` (sequential to avoid SQLite lock contention).
- Tests that use real torrent downloads have been intentionally removed — iteration speed was prioritized over coverage.
