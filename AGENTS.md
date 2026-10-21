# AGENTS.md

Self-hosted torrent streamer: add a magnet or search result, download locally, and play in the browser. Rust/Axum backend with a vanilla-JS frontend embedded in a single binary. The repository also includes a manga/comic reader and peer-to-peer watch parties.

## Build and verification

```sh
cargo build
cargo test -- --test-threads=1
cargo clippy --all-targets
node --test tests/player.test.cjs
node --test tests/torrent_search.test.cjs
# Requires Python Playwright, FFmpeg/FFprobe, and Chrome (CHROME_BIN overrides it):
python tests/player_browser.py
PLAYER_BROWSER=firefox python tests/player_browser.py
```

`static/index.html` is embedded via `include_str!`; rebuild the binary for frontend changes to take effect.

Use the checks relevant to the change. Rust integration tests use temporary databases/files and no real torrent downloads. Browser tests run an isolated server with generated media. `PLAYER_SOURCE=/path/to/file` optionally exercises existing media read-only. Additional codec options are documented in `tests/player_browser.py`.

## Code map

| Path | Responsibility |
| --- | --- |
| `src/main.rs`, `src/config.rs` | Startup, shutdown, CLI configuration |
| `src/http.rs` | Axum routes, WebSockets, streaming and subtitle endpoints |
| `src/db.rs` | SQLite schema, migrations, persistence |
| `src/library.rs` | Library operations coordinating the DB, torrent engine, and files |
| `src/torrent.rs` | Wrapper around vendored librqbit |
| `src/indexer.rs` | Torrent search adapters: ApiBay, Knaben, Nyaa, YTS |
| `src/remux.rs`, `src/segmenter.rs` | Media probing and fragmented-MP4 segment production |
| `src/metadata.rs` | TMDB client and metadata cache |
| `src/reading.rs`, `src/reader.rs` | Reading catalogues, chapter sources, image proxy |
| `src/subtitles.rs` | Local/embedded subtitles and OpenSubtitles integration |
| `src/party/`, `src/bin/relay.rs` | Party protocol, UDP transport, coordination hub |
| `static/index.html` | Entire frontend: inline JS/CSS, no framework or bundler |
| `tests/` | HTTP integration, frontend unit, and browser tests |

## Storage and torrent lifecycle

- SQLite is the source of truth. librqbit is vendored under `vendor/librqbit/` and configured for local DHT-only operation, without trackers or DHT persistence.
- Torrent content lives under `library_dir/{id}`, with segment caches in `.seg/`. `temp_dir` holds engine working data and other temporary caches. Configuration is in `src/config.rs`.
- Adding a torrent inserts its row and metadata immediately, then registers it with the engine asynchronously. States normally progress `connecting → downloading → ready`, with failure represented as `error`.
- Incomplete torrents must be read through librqbit's verified `FileStream`, not directly from sparse files. The stream supports seeking and drives download priority from its read position.
- Deletion cascades are implemented at the application level, not through SQLite `ON DELETE CASCADE`; coordinate related rows, engine state, and files through the library layer.

## Streaming and player

- Audio/video is never re-encoded. FFmpeg uses `-c copy` to produce keyframe-aligned fragmented-MP4 segments with a 4s target; source GOPs can make them longer.
- Ready files are read directly. Incomplete torrents use a private loopback HTTP range input backed by `FileStream`, allowing FFmpeg/FFprobe to seek for container indexes.
- Segment caches are per item/file and generation. The manifest exposes `generation` and `timing`; init/segment requests include `g` and, when applicable, `file`. HTTP 410 triggers a client rebuild at the saved position.
- The browser demuxes fragments and decodes through WebCodecs, painting video to canvas and scheduling audio with WebAudio. There is no `<video>`/MediaSource playback path. Controls, subtitles, and parties use the shared `v` player facade.
- Source timestamps and skip-sample metadata define the common timeline, with video starting at zero. Real decoded PCM duration drives the audio-master clock; video-only playback and video tails use a wall clock. Demux sample defaults inherit `trun → tfhd → mvex/trex`.
- Decoder work is generation-scoped: a seek/rebuild must discard stale outputs and callbacks. Fetch completion, decoder drain, and presentation completion are separate stages. Startup configures decoder candidates directly; `VideoDecoder.isConfigSupported` is unreliable for HEVC on Firefox.
- Embedded text subtitles are extracted on demand to cached WebVTT and normalized to the same source timeline. Probe/extract only complete, non-skipped files or fully verified active-torrent files. Bitmap subtitles do not use the text renderer.

## Metadata and frontend data flow

- TMDB metadata is cached in SQLite as a whole-show payload, including seasons and episode lists. Reads serve cached data; TTL refresh happens in the background. Season selection normally uses the in-memory payload rather than making another request.
- Library updates come from `/ws/library`: `changed` refreshes the library, `progress` refreshes the affected item's status/pieces, and `resync` handles missed events. Periodic fallback refresh runs only while the socket is down.
- Frontend items group by `media_type + ':' + tmdb_id`, with standalone torrents keyed by item ID. TV playback and related requests also identify the selected file.
- Torrent search providers share one response shape. Keep the frontend provider list aligned with `indexer::Provider` when adding adapters.

## Reading

- Weeb Central supplies the manga catalogue and chapter pages. MangaDex only enriches chapter titles; Comic Vine catalogues comics, and AniList supplies manga cover art.
- Native manga rows use the Weeb Central series ID as `ext_key`, which binds the source directly. Legacy rows may require source resolution.
- Chapter pages are proxied through the backend. Reading progress is stored per item/chapter; reader routes are `/library/<movie>/read/<label>`.
- Chapter labels are raw numeric strings, including decimals. Catalogue IDs cross JSON/JS boundaries and must remain below `2^53`.

## Watch parties

- Each member downloads locally; the swarm distributes content. Party actions and presence use encrypted peer-to-peer UDP. The coordination hub exchanges endpoints for hole punching and does not relay party traffic.
- `PartyManager` maintains one persisted party per instance. Invites contain the hub address, room/key, torrent metadata, and optional episode file.
- Synchronization is last-writer-wins, with no host authority. Actions are episode-scoped when a file is present; remote actions are guarded against rebroadcast loops.
