# AGENTS.md

Single-binary self-hosted torrent streamer: paste a magnet link (or search via indexer) → torrent downloads to `library_dir/{id}` → streams to browser via WebCodecs (canvas + WebAudio, no `<video>`/MediaSource). Built on vendored librqbit (local DHT-only, no trackers/DHT persistence; our SQLite is the source of truth). Nothing is ever re-encoded: ffmpeg only rewraps containers to fragmented MP4 (`-c copy`); the browser does all decoding.

## Build / test commands

```
cargo build          # fast incremental build
cargo test -- --test-threads=1   # full suite (~4s, all fast — no real torrent downloads)
cargo clippy --all-targets
node --test tests/player.test.cjs
# Browser regressions require Python playwright + Chrome (CHROME_BIN overrides it):
python tests/player_browser.py
# Installed Playwright Firefox:
PLAYER_BROWSER=firefox python tests/player_browser.py
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
  remux.rs       ffprobe helpers: stream MIME (AAC profile-aware) + duration
  segmenter.rs   segment pipeline: ffmpeg `-c copy` → init.mp4 + seg-N.m4s on disk (per item), manifest state
  metadata.rs    TMDB API client (search + detail fetch) + `MetaCache` (tmdb_meta-backed read cache, TTL'd background refresh)
  reading.rs     Reading catalogues (ground truth): MangaDex search + chapter sweep, Comic Vine issues, AniList covers
  reader.rs      Reading sources (chapter PAGES): MangaDex at-home + Weeb Central scraping, page-image proxy
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

3. **Stream** (segment pipeline, one path for every item state): `GET /api/library/:id/stream` returns the manifest `{mime, duration, done, error, state, segments:[{n,t}]}` and lazily starts a per-item **segmenter** (`src/segmenter.rs`): ffmpeg `-c copy` (no re-encode) produces an interleaved fragmented-MP4 byte stream which the splitter writes as `library_dir/{id}/.seg/init.mp4` + `seg-N.m4s` (~10-20s each, keyframe-aligned via `frag_keyframe`). Sources: finished file → ffmpeg reads the path (whole movie segmented in seconds, cached); downloading torrent → ffmpeg reads the librqbit `FileStream` via stdin, which blocks exactly at the download frontier, so segments appear as pieces land. The client (`static/index.html`) is a **WebCodecs player**: it demuxes the fMP4 fragments in JS (minimal ISO-BMFF reader: moov/trak/stsd → hvcC/avcC/esds; moof/tfhd/tfdt/trun → samples) and feeds samples to `VideoDecoder`/`AudioDecoder`; video frames paint to a `<canvas>`, audio is scheduled on a WebAudio timeline that doubles as the master clock (pause = `ctx.suspend()`, which freezes the clock for free). There is no browser media buffer at all — no SourceBuffer quota, no keyframe-group eviction — so any seek is just "reset decoders (recreate them: `reset()` leaves them `unconfigured`), fetch the segment containing the target, decode from its first sync sample". Backpressure: fetch ~45s ahead, cap decoded video frames (~30). A seek purges in-flight outputs via decoder identity (each decoder generation owns its callbacks — stale outputs are closed, never queued; scheduling stale audio re-anchors the clock at the old position). The manifest MIME's codec string is verified against the actual stsd sample entry (`hev1` vs `hvc1`), and `VideoDecoder.isConfigSupported` is NOT trusted (it hangs/false-negatives for HEVC on Firefox) — candidates are configured directly. If no WebCodecs configuration is accepted (e.g. Firefox ships no WebCodecs HEVC decoder), playback stops with a clear "no decoder in WebCodecs" overlay. The engine drives one shared facade (`v`) with `<video>`-like properties/events, so controls, subtitles, parties and playhead saving are engine-agnostic. **Clock model (mpv-style audio master):** the playhead interpolates from the last scheduled WebAudio buffer's anchor (each anchor carries the container timestamp minus the AAC decoder-delay trim — native players trim that priming, WebCodecs does not); at audio EOF with video remaining (credits outlasting the soundtrack — Django is +1.14s) or for video-only files, the wall clock takes over past the last anchor (also frozen by `ctx.suspend` on pause); `feedLoop` at EOF keeps draining the decode queues before exiting, and `audioEnded()`/`pendSeek`'s fullDur arm cover seeks landing past the last audio packet. Segment times are read from the **video track's** tfdt (per-track timescales differ — never take min across trafs). ffmpeg's trun carries NO per-sample durations here: `parseSeg` derives each track's implicit per-sample duration from consecutive traf tfdt bases (`next.base - this.base == count * dur`) — never trust the 1-tick fallback, it compresses intra-fragment audio timestamps to garbage. The AudioContext is created+resumed synchronously inside the row-click gesture (`ensureCtx` from `playTorrent`/`playerPlay`); Firefox silently rejects a `resume()` that lands outside the gesture window and the clock pins. Subtitles: browsers never load `<track>` elements outside a media element, so `applySubTrack` fetches the VTT and parses it in JS (`parseVtt`) into standalone `VTTCue`s on a synthetic track (`subTrack()`). The declared MIME **must match the actual stream**: HE-AAC → `mp4a.40.5`, AAC-LC → `mp4a.40.2`; the AudioSpecificConfig for `AudioDecoder` comes from the esds DecSpecificInfo (nested inside the ES/DecoderConfig descriptors). Implicit-SBR HE-AAC (esds ASC = plain LC at core rate, container timescale = doubled rate) is probed with an explicit-SBR ASC rewrite first (`ascExplicitSbr`: AOT 5 + extension rate; declared sampleRate = ASC core × 2, NOT container × 2); Firefox rejects every description-based config and falls to the description-less core-rate one, which decodes only the core band (one octave low — known limitation, timing still exact). 4. **Seek**: `seekTo(t)` resets the decoders (fresh decode generation), points the feeder at the segment containing `t`, and decodes from the segment's keyframe while *closing* every decoded output older than `t - 0.35s` (the seek floor — queueing pre-target outputs backpressure-throttles the decode to playback speed and pins the spinner for the whole segment tail; closing them makes the first kept output land at the target in ~250ms). Because there is no browser buffer, every seek is the same operation: instant once the segment exists on disk (~100-300ms to first frame, measured), a real wait with the spinner while the torrent downloads it — the overlay shows real download speed from the WS status; no piece-reprioritization tricks, no 503 path anymore.

5. **Delete** (`DELETE /api/library/:id`): removes the row, deletes `library_dir/{id}` and `temp_dir/{id}`, removes from engine. If `?hard=1`, cascades to all torrents sharing the same `tmdb_id` (movie delete).

## Current segment/player invariants

These supersede the historical timing/input details in data-flow item 3 above.

- Segments are keyframe-aligned, with a **4s target**, closed before the next fragment at the target boundary; long source GOPs still make longer segments. Caches live beneath `.seg/default/` or `.seg/<file-hash>/`, in generation-specific directories. The manifest carries `generation` and `timing`; init/segment URLs require `?g=<generation>` (plus `file` when applicable). A stale generation returns 410 and the browser rebuilds at its saved position while retaining its AudioContext and paused state.
- FFmpeg still uses **`-c copy`**. `negative_cts_offsets` prevents the first audio packet being stretched to cover video B-frame lead-in. Live torrents use a private loopback **HTTP range input backed by FileStream**, so FFmpeg/FFprobe can seek for container indexes without reading unverified bytes. Paused partial downloads stay on FileStream. `-xerror`, stderr, incomplete output, and empty output are terminal failures.
- Demux sample fields inherit **trun → tfhd → mvex/trex**. Missing durations fail; there is no inferred duration, one-tick video fallback, or universal AAC-frame-size assumption.
- **Source packet timestamps and skip-sample metadata** establish track offsets and priming. The server's `timing` maps fMP4 samples back to a common source timeline (video starts at zero). The browser trims actual PCM at the seek/priming floor and trims explicitly short packet tails. There is **no AAC delay lookup table** or global timestamp-step clock. Real PCM duration advances the clock 1:1; `getOutputTimestamp` / `outputLatency` accounts for device output timing; timestamp gaps remain silence.
- Seek decoding starts at the latest preceding sync sample within the fetched segment, with bounded audio preroll. Decoder output/dequeue and presentation wake feeding; timers only bound network retries and fetch lookahead. The seek floor remains for the decoder generation. Actual target output or drained EOF completes a seek; fetched EOF and the requested clock position cannot establish readiness.
- Fetched EOF, decoder EOF, and presentation EOF are distinct. Each decoder is **flushed independently** after all of its chunks have been submitted; audio-drained EOF enables the video-tail clock handoff. Stale startup/output generations cannot mutate the active session, and decoder/manifest errors surface instead of blind watchdog re-seeks.
- Startup checks codecs as soon as MIME/init metadata is available, without waiting for completed segmentation or the first media segment. Candidate configuration uses an **empty `flush()` control-queue barrier**, not fixed sleeps; synchronous rejection advances immediately. Video-only clocks stay at zero until the first decoded frame arrives. Optional `PLAYER_HEVC=1` browser tests cover Main10 HEVC support and fast rejection.
- `tests/player.test.cjs` exercises the shipped inline engine. `tests/player_browser.py` runs an isolated server with generated H.264/AAC files (no torrent downloads), checks the normalized timeline against original FFprobe packets, and verifies accurate/paused seeks, video-only files longer than the 45s fetch window, cache rebuilds and server restarts, delayed audio, B-frame EOF, audio-EOF video tails, and paused EOF seeks. Optional `PLAYER_HEAAC=1` uses macOS AudioToolbox; unsupported browser decoders must produce a terminal error, not a spinner.

## TMDB metadata is cached whole, refreshed by TTL

`GET /api/meta/{media_type}/{tmdb_id}` returns `{overview, seasons, episodes}` — the synopsis, the canonical season list **and every season's episode list** — all from one `tmdb_meta` row (`episodes_json` holds `Vec<SeasonEpisodes>` keyed by season, `fetched_at` is the age stamp). Reading it is a single SQLite row and never touches the network, so opening a TV show paints its season chips *and* every season's episode buttons in one pass: clicking a season is a synchronous map lookup (`episodesOf(rep, season)`) with **no request at all** — previously each click re-hit TMDB and first rendered the *previous* season's buttons, which read as a flicker. `GET /api/meta/tv/{id}/season/{n}` survives only as the fallback for a season the bulk payload lacks (a failed sweep, or a season that aired since the last one); the frontend falls back to it via `ensureTvEpisodes`, and only when `episodes[season]` is absent.

Staleness is decided **server-side** (`MetaCache::refresh`, `META_TTL_SECS` = 12h) so it doubles as the background check for newly released seasons/episodes: repeat views of a show cost two SQLite reads and zero upstream requests. Three triggers, all no-ops while the row is fresh: `POST /api/movies` spawns a warm-up (the cache is ready before the user clicks the new card), `meta_detail` re-checks a stale row in the background while serving the stale payload immediately, and the client fires `POST /api/meta/{mt}/{id}/refresh` from `openMovie` — answered `{updated: bool}` (fingerprint-differs, so "nothing new" costs nothing and skips the reload), with a 30-min client-side throttle. A failed episode sweep is stored but leaves `fetched_at` untouched so the next view retries instead of trusting a half-filled cache for a day. Concurrent refreshes of one show collapse into a single sweep (async-mutex handoff, not `Notify::notify_waiters` — a waiter that hasn't registered yet would hang the request forever).

## Library updates are notification-driven (no polling)

The frontend never polls the library. `/ws/library` streams two events from `LibraryEvent` (`src/library.rs`): `{"type":"changed"}` broadcast synchronously at every mutation point (`notify_changed()`) and `{"type":"progress","id":N}` emitted by the lazy 1s `spawn_progress_watcher` aggregator (filters to real changes via `piece_fingerprint` FNV-1a over the piece map in `src/torrent.rs`; scans only while subscribers exist). The client only refetches/repaints what those events imply: `changed` → `refreshLibrary()` (JSON-fingerprint-guarded, defers grid paint to `gridDirty` when the library view is hidden), `progress` → one status+pieces GET for that id through a coalescing `progQueue` pump — never a full sweep. `Lagged`/reconnect sends `{"type":"resync"}`. A 5s fallback refresh runs only while the WS is down.

## Manga reader: one catalogue, one source

- **Catalogue = Weeb Central** (`src/reader.rs` owns the client; `src/reading.rs` shapes rows). MangaDex is gone as catalogue AND source: it delists licensed chapters (Gantz 0/383, One Piece 4/1100+ english) so its feed was never "every chapter that exists" — Weeb Central keeps complete lists (verified 383/383 Gantz, 386/386 Berserk) keyless: `POST /search/simple?location=main`, `GET /series/{id}` (title/synopsis), `GET /series/{id}/full-chapter-list`, `GET /chapters/{hid}/images?style=list`. Comic Vine (keyed) still catalogues comics; AniList still supplies manga cover art.
- **Chapter NAMES are a MangaDex-only enrichment**, merged into the sweep by chapter number (`reading::chapter_titles` + `apply_chapter_titles`): search MangaDex by title, only accept an exact normalized-title match (never glue another series' names on), en titles first with an any-language fallback for gaps. Weeb Central lists carry no titles and every other complete keyless source strips them; series MangaDex wiped entirely (Gantz) stay numbered-only — often correct, those chapters were never named. Sub-chapter rows keep the `Ch. <raw> · <title>` name convention so the raw label stays extractable.
- **Binding is structural**: the library row's `ext_key` IS the Weeb Central series id, so a group's reading source (reader_source table) binds **exactly, silently, zero requests** on first read — no picker for native rows. The picker (Source chip / manual search) remains for legacy rows (MangaDex-era UUID ext_keys), which the sweep heals onto Weeb Central ids via `db.rekey_reading_group` (drags meta/source/progress rows along).
- **API** (`src/http.rs`): `GET/PUT /api/library/{id}/read/source`, `GET …/read/source/{src}/search?q=`, `GET …/read/chapter/{label}` (resolve → proxied page URLs, 10-min in-memory cache so per-image requests don't re-resolve), `GET …/read/chapter/{label}/page/{n}` (image proxy — the browser never talks to the source), `GET/POST …/read/…/progress` (per-chapter read flag + last page; `read` sticky via `MAX(read, excluded.read)`).
- **Reader UX**: plain vertical continuous scroll (`loading=lazy`, min-height placeholders), the player's exact `pback` button (same class/CSS, `.on` = always visible), back button + Escape close (`KCTX_READER` keybind context). **URL routing**: `/library/<movie>/read/<label>` — pushed on open, replaced on chapter switch, popped by back/Escape (popstate → handleRoute closes); a refresh on a read route re-opens the same chapter and restores the **exact saved page** silently (no badges/buttons show progress; finishing a chapter tags it read, the row keeps its ✓). Scroll saves progress throttled 1.5s + on close; the bottom marks read and offers Next chapter (from the ground-truth list; upgrades itself when meta lands after a deep link).
- **Chapter labels** are raw number strings ("386", "37.5") — `chapterLabelOf` extracts them from ground-truth rows; labels compare numerically (`label_matches`) against source chapter lists.
- **JS-safe ids**: catalogue ids cross the JSON/JS boundary, so they must stay under 2^53 — Weeb Central ids hash via `ext_id_hash` (FNV-1a masked to 52 bits); `mangadex_id_hash` survives only for the one-time `manga_id_52bit` migration. The `weebcentral_catalogue` migration drops MangaDex-era meta + bindings once.
- **Cold-open race**: a meta read that lands while the add-time warm-up sweep is still running must NOT answer an empty payload — `meta_detail` polls up to ~4s for the row to appear (an empty answer latches as "no chapters" for the whole frontend session; the Gantz bug).
- **Frontend lazy-loader gotcha**: `rdEnsureSource`/`rdEnsureProgress` dedupe concurrent callers via an in-flight **promise** map — a repaint that lands mid-load and re-enters the loader must get the same promise, never a fresh `Promise.resolve(null)`; a resolve-null → repaint → reload chain is an endless microtask loop that starves the very fetch it waits for (main thread wedges, CDP can't even pause in — hit and fixed once already).

## Watch parties

Each member runs their own instance; the BitTorrent swarm is the content distribution (everyone downloads locally — nothing is ever streamed between members). Sync traffic (play/pause/seek/presence, bytes/sec) flows **peer-to-peer over punched UDP** — there is deliberately no server relay fallback.
- **Protocol** (`src/party/protocol.rs`): every frame is XChaCha20-Poly1305 encrypted with the party key — `seal`/`open` + serde `Message` enum (`Hello`, `Presence{peer}`, `Play/Pause/Seek{from,t,file}`). Room id + key exist only in the invite. Actions carry the acting member's current file (`file`, optional — absent for single-file items), so a TV-pack party always agrees on *which episode* the position `t` refers to; `PeerState`/`Invite`/the `party` DB row carry it too, making a party episode-scoped for TV (an action whose file differs from the receiver's implies "switch to that episode, then apply").

- **Invite**: `CTPARTY1:<base64url(JSON)>` carrying `{relay, room, key, magnet, tmdb metadata, host_name, file?}` (the `relay` field is the coordination hub's address; `file` pins the episode for TV parties). Joiners configure nothing. Joining dedupes by infohash (`magnet_infohash`) so the same torrent is never added twice.
- **Punch hub** (`src/party/punch.rs`, server half): a stateless UDP "mailroom" on one port. Members bind, learn each other's public UDP endpoints, punch directly (simultaneous-open) and the hub steps out — it never carries party traffic. Run via the standalone `chadtorrent-relay` binary (UDP-only, `--party-relay` points invites at it; unset ⇒ invite uses the instance's LAN address — every chadtorrent instance hosts a hub on its own port for LAN parties).
- **Transport** (`src/party/punch.rs`, client half — `run_transport`): owns the party's UDP socket, per-peer reliability (seq/ack/retransmit/keepalive) and punch lifecycle. Party traffic flows only when every member is punched; a new member joining or a silent peer drops the party back to "connecting/reconnecting" until re-punched. Peer ids are per-instance (stable across re-joins) so the hub's room table never accumulates dead identities.
- **PartyManager** (`src/party/mod.rs`): one party per instance, persisted in the `party` table (auto-rejoin on restart). Browser-facing surface: `POST /api/party` (create), `POST /api/party/join`, `GET /api/party`, `POST /api/party/leave`, `GET /ws/party` (events down / `{type:"action"|"state"}` up).
- **Sync model**: last-writer-wins actions, no host authority. The browser owns the `<video>`: local play/pause/seek broadcast; remote actions applied behind a `partyApply` guard counter (300ms) to prevent echo loops; drift correction re-seeks when >2.5s off the action anchor. Strict stalls: a member stalled >2s reports `buffering`, everyone auto-pauses with a "Waiting for X…" overlay, auto-resumes when clear. `ready` = ≥60s buffered ahead of the party position (or finished file).

## Frontend conventions

- **No framework**: all vanilla JS. Modules use global functions (no bundler).
- **Movie grouping**: items grouped by `media_type + ':' + tmdb_id` (or `'r' + id` for standalone torrents). The movie detail shows a placeholder row (tmdb_id, no magnet) plus all attached torrent rows.
- **Optimistic add**: clicking "Add" in torrent search pushes a synthetic row (`state: '_pending'`) into `items` instantly, shows "Adding…", then the server call completes in the background. The synthetic row is removed once the POST returns and `refreshLibrary` picks up the real row.

- **Row layout**: all torrent rows have identical structure (title + subline + controls) with `min-height: 68px` so state transitions don't cause layout shifts.
- **Party button placement**: the watch-party button sits on playable units only — torrent rows for movies, episode rows (`.ep`) for TV. Whole-show/season pack header rows carry no party button (a pack is not a playable unit); creating from an episode passes its `file` so the party is episode-scoped.
- **Add-in-place**: a torrent added from search results is pushed into the downloaded-content list immediately (synthetic `_pending` row) and that same row then updates with the real information as the background add completes and the state progresses (`connecting → downloading → ready`), keyed by id so the optimistic row matures into the real one without re-adding it.

## Key gotchas

- `static/index.html` is embedded at compile time. Any frontend edit requires `cargo build` before it shows up in the binary.
- Torrent registration is non-blocking (returns immediately). A row can sit in `connecting` state for seconds; the frontend shows "Adding…" during this time. The Play button is disabled until the state advances.
- **Browser quirks (previously verified with a browser harness; keep them in mind when touching the player)**:
  - The declared MIME's AAC profile must match the stream's actual AudioSpecificConfig (HE-AAC → `mp4a.40.5`, AAC-LC → `mp4a.40.2`); mismatches historically wedged or degraded Firefox, while Chrome tolerated them.
  - Firefox clamps `v.currentTime` writes to the seekable range; with fragmented MP4 the duration is Infinity until `endOfStream()` — always pin `ms.duration` from the manifest before seeking.
  - Firefox coalesces fetch body chunks into ~16–32MB batches (Chrome delivers ~64KB slices) — never assume chunk size on the client.
  - Never `await v.play()` inside the pump loop — the promise can fail to settle (reproduced in headless Chrome) and would stall the pump forever. Fire-and-forget it.
- `librqbit` is vendored locally at `vendor/librqbit/` with a Cargo path override in `Cargo.toml`. Its `FileStream` implements `AsyncSeek` and each open stream drives piece priority via a 32MB lookahead window from the read position.

## Database

SQLite via rusqlite. Schema lives in `db.rs`. Migration logic: `CREATE TABLE IF NOT EXISTS` (safe if column list changes across versions). `ON DELETE CASCADE` is NOT used — app-level cascading in `remove_item_inner` deletes related rows and files explicitly.

Key columns: `id, magnet, title, state, added_at, file_path, size_bytes, tmdb_id, media_type, poster_path, year`.

## Test suite

- **Unit tests** in `db.rs` (CRUD, migration, `tmdb_meta` roundtrip + migration, reader-source binding + chapter progress + reading-group rekey), `metadata.rs` (payload shape contract, staleness, fingerprint), `reading.rs` (numbered chapter rows: dedupe/asc/sub-chapter labels, id hashes), `reader.rs` (Weeb Central fragment parsers: search cards, series page, chapter-list rows, page-image strip; chapter-label matching), `http.rs` (video file selection), `segmenter.rs` (box parsing: tfdt/tkhd/mdhd) and `remux.rs` (codec strings: HEVC level must stay decimal — `L93`, not hex `L05D`; AVC level byte stays hex; profile mapping; bit-depth; unknown codecs named as `None`).
- **E2e tests** in `tests/e2e.rs`: no torrent downloads. Use `db.insert_item` directly, create fake files on disk, verify delete/rollback/cascade behavior, HTTP error codes, and the TMDB meta surface (cache-only, seeded `tmdb_meta` + a bogus API key, so no test ever hits TMDB).
- Run with `cargo test -- --test-threads=1` (sequential to avoid SQLite lock contention).
- Tests that use real torrent downloads have been intentionally removed — iteration speed was prioritized over coverage.
