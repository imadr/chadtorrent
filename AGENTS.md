# AGENTS.md

Single-binary self-hosted torrent streamer: paste a magnet link (or search via indexer) → torrent downloads to `library_dir/{id}` → streams to browser via MediaSource Extensions. Built on vendored librqbit (local DHT-only, no trackers/DHT persistence; our SQLite is the source of truth). Nothing is ever re-encoded: ffmpeg only rewraps containers to fragmented MP4 (`-c copy`); the browser does all decoding.

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
  config.rs      CLI args: --port, --library-dir, --temp-dir, --db-path, --party-relay
  db.rs          SQLite CRUD via rusqlite (library items, metadata, state, active watch party)
  torrent.rs     TorrentEngine: wraps librqbit Session + ManagedTorrent map
  library.rs     LibraryManager: orchestrates DB + TorrentEngine + file paths
  http.rs        Axum server: REST routes + WS status + streaming + remux + party API
  remux.rs       ffprobe helpers: SourceBuffer MIME (AAC profile-aware) + duration
  segmenter.rs   segment pipeline: ffmpeg `-c copy` → init.mp4 + seg-N.m4s on disk (per item), manifest state
  metadata.rs    TMDB API client (search + detail fetch)
  subtitles.rs   OpenSubtitles client (search/download) + local subs scan
  party/         Watch parties (see below): protocol, relay, PartyManager
  bin/relay.rs   `chadtorrent-relay`: standalone stateless relay binary
  error.rs       Error types mapping to HTTP status codes
  lib.rs         Module re-exports

static/
  index.html     Single-file frontend (no framework, all JS inline)

tests/
  e2e.rs         Fast integration tests (no torrent downloads; use fake files + DB)
```

## Data flow

1. **Add torrent** (`POST /api/library`): inserts row immediately with state `connecting`, returns id, then spawns a background task that registers the torrent with librqbit (which may take seconds). The torrent's `tmdb_id`/metadata is applied immediately by the handler, so grouping is correct before the engine finishes.

2. **Download**: librqbit downloads sequentially to `library_dir/{id}/`. State flows `connecting → downloading → ready`. On failure: `error`.

3. **Stream** (segment pipeline, one path for every item state): `GET /api/library/:id/stream` returns the manifest `{mime, duration, done, error, state, segments:[{n,t}]}` and lazily starts a per-item **segmenter** (`src/segmenter.rs`): ffmpeg `-c copy` (no re-encode) produces an interleaved fragmented-MP4 byte stream which the splitter writes as `library_dir/{id}/.seg/init.mp4` + `seg-N.m4s` (~10-20s each, keyframe-aligned via `frag_keyframe`). Sources: finished file → ffmpeg reads the path (whole movie segmented in seconds, cached); downloading torrent → ffmpeg reads the librqbit `FileStream` via stdin, which blocks exactly at the download frontier, so segments appear as pieces land. The client (`static/index.html`) polls the manifest, appends `init` then segments into one MSE `SourceBuffer` (blob URL), keeping ~60s ahead / 30s behind the playhead, and calls `ms.endOfStream()` once everything is appended. There is exactly one session per playback — no reconnects, no 0-based seek sessions. Segment times are read from the **video track's** tfdt (per-track timescales differ — never take min across trafs). The declared MIME **must match the actual stream**: HE-AAC → `mp4a.40.5`, AAC-LC → `mp4a.40.2` — Firefox wedges on a mismatch (browser A/B test); `probe_mse_mime` returns `None` instead of guessing when ffprobe can't see the codecs yet. `ms.duration` is pinned from the manifest (fragmented streams have no embedded duration; without it Firefox clamps seeks to the buffered range).

4. **Seek**: `seekTo(t)` is just `v.currentTime = t` — instant when buffered (ready files: ~60-120ms to first frame, measured both directions); past the buffer the segment pump fetches the segment containing `t` (selection = segments *overlapping* the window, not just starting inside it) and playback resumes when it lands. While still downloading, a seek past the frontier simply waits for the sequential download to reach it — the overlay shows real download speed from the WS status; no piece-reprioritization tricks, no 503 path anymore.

5. **Delete** (`DELETE /api/library/:id`): removes the row, deletes `library_dir/{id}` and `temp_dir/{id}`, removes from engine. If `?hard=1`, cascades to all torrents sharing the same `tmdb_id` (movie delete).

## Watch parties

Each member runs their own instance; the BitTorrent swarm is the content distribution (everyone downloads locally — nothing is ever streamed between members). Only a tiny encrypted control channel (play/pause/seek/presence, bytes/sec) goes through a **relay**: a stateless room broadcaster.

- **Protocol** (`src/party/protocol.rs`): every frame is XChaCha20-Poly1305 encrypted with the party key — `seal`/`open` + serde `Message` enum (`Hello`, `Presence{peer}`, `Play/Pause/Seek{from,t}`). Room id + key exist only in the invite.
- **Invite**: `CTPARTY1:<base64url(JSON)>` carrying `{relay, room, key, magnet, tmdb metadata, host_name}`. The relay URL is inside the invite — joiners configure nothing. Joining dedupes by infohash (`magnet_infohash`) so the same torrent is never added twice.
- **Relay** (`src/party/relay.rs`): `GET /ws/party/relay/{room}` — broadcasts opaque frames between members, no persistence, idle timeout 75s, max 32 members/room. Mounted on every chadtorrent instance (so a LAN-reachable instance serves as its own relay) and built as the standalone `chadtorrent-relay` binary for internet-wide parties (`--party-relay` points invites at it; unset ⇒ invite uses the instance's LAN address, same-network only).
- **NAT punch** (`src/party/punch.rs`): the relay binary also runs a UDP "mailroom" hub on the same port (forward both TCP+UDP). Members bind, learn each other's public UDP endpoints, punch directly (simultaneous-open), and sync messages flow peer-to-peer with a small ack/retransmit layer; the server steps out. Strictly an optimization: any punch failure (symmetric NAT/CGNAT, ~15-25% of pairs), a new member joining mid-party, or a peer timing out silently falls back to the WS relay — payload encryption is identical in both modes (`transport` field in the party snapshot reports `udp` vs `relay`).
- **PartyManager** (`src/party/mod.rs`): one party per instance; outbound tokio-tungstenite connection with backoff reconnect, 4s presence heartbeat, 15s peer timeout, persisted in the `party` table (auto-rejoin on restart). Browser-facing surface: `POST /api/party` (create), `POST /api/party/join`, `GET /api/party`, `POST /api/party/leave`, `GET /ws/party` (events down / `{type:"action"|"state"}` up).
- **Sync model**: last-writer-wins actions, no host authority. The browser owns the `<video>`: local play/pause/seek broadcast; remote actions applied behind a `partyApply` guard counter (300ms) to prevent echo loops; drift correction re-seeks when >2.5s off the action anchor. Strict stalls: a member stalled >2s reports `buffering`, everyone auto-pauses with a "Waiting for X…" overlay, auto-resumes when clear. `ready` = ≥60s buffered ahead of the party position (or finished file).

## Frontend conventions

- **No framework**: all vanilla JS. Modules use global functions (no bundler).
- **Movie grouping**: items grouped by `media_type + ':' + tmdb_id` (or `'r' + id` for standalone torrents). The movie detail shows a placeholder row (tmdb_id, no magnet) plus all attached torrent rows.
- **Optimistic add**: clicking "Add" in torrent search pushes a synthetic row (`state: '_pending'`) into `items` instantly, shows "Adding…", then the server call completes in the background. The synthetic row is removed once the POST returns and `refreshLibrary` picks up the real row.

- **Row layout**: all torrent rows have identical structure (title + subline + controls) with `min-height: 68px` so state transitions don't cause layout shifts. Rows update in place (keyed by id) — never wipe and rebuild `#mv-torrents`.
- **In-place updates**: `renderMovie()` matches existing DOM nodes by `data-id` and updates them, only adding/removing nodes when the row set changes. This prevents flash when a row transitions between states.

## Key gotchas

- `static/index.html` is embedded at compile time. Any frontend edit requires `cargo build` before it shows up in the binary.
- Torrent registration is non-blocking (returns immediately). A row can sit in `connecting` state for seconds; the frontend shows "Adding…" during this time. The Play button is disabled until the state advances.
- **Browser quirks (verified with the MSE harness in `docs/browser-harness/`, served same-origin at `/__test`)**:
  - Firefox wedges or degrades when the SourceBuffer MIME's AAC profile doesn't match the stream's actual AudioSpecificConfig (HE-AAC must be declared `mp4a.40.5`). Chrome tolerates the mismatch.
  - Firefox clamps `v.currentTime` writes to the seekable range; with fragmented MP4 the duration is Infinity until `endOfStream()` — always pin `ms.duration` from the manifest before seeking.
  - Firefox coalesces fetch body chunks into ~16–32MB batches (Chrome delivers ~64KB slices) — never assume chunk size on the client.
  - Never `await v.play()` inside the pump loop — the promise can fail to settle (reproduced in headless Chrome) and would stall the pump forever. Fire-and-forget it.
- `librqbit` is vendored locally at `vendor/librqbit/` with a Cargo path override in `Cargo.toml`. Its `FileStream` implements `AsyncSeek` and each open stream drives piece priority via a 32MB lookahead window from the read position.

## Database

SQLite via rusqlite. Schema lives in `db.rs`. Migration logic: `CREATE TABLE IF NOT EXISTS` (safe if column list changes across versions). `ON DELETE CASCADE` is NOT used — app-level cascading in `remove_item_inner` deletes related rows and files explicitly.

Key columns: `id, magnet, title, state, added_at, file_path, size_bytes, tmdb_id, media_type, poster_path, year`.

## Test suite

- **Unit tests** in `db.rs` (CRUD, migration), `http.rs` (video file selection) and `segmenter.rs` (box parsing: tfdt/tkhd/mdhd).
- **E2e tests** in `tests/e2e.rs`: no torrent downloads. Use `db.insert_item` directly, create fake files on disk, verify delete/rollback/cascade behavior, HTTP error codes.
- Run with `cargo test -- --test-threads=1` (sequential to avoid SQLite lock contention).
- Tests that use real torrent downloads have been intentionally removed — iteration speed was prioritized over coverage.
