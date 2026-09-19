# Stream-to-client rework plan

Status: **implemented (2026-09-19)** — see "What shipped" at the bottom. The plan
below was drafted after a measured investigation of playback micro-stuttering
(Pulp Fiction 1080p, fully downloaded, local playback) and one indefinite-buffering
incident; the segment architecture from Phase 2 was then built in a simplified
form (no piece-reprioritization, per the product decision).

## Measured facts (this machine, this repo)

- Server → client pipe is not the bottleneck: `/mse/:id` pours ~280MB/s (whole
  1.5GB remux in ~3s) with zero gaps >50ms between 64KB chunks.
- The remuxed fMP4 is clean: 222k video packets, zero PTS anomalies; audio
  decode rate matches container timing 1:1 (the "2× HE-AAC timescale" suspicion
  was checked and ruled out — audio sounds normal, confirmed by the user).
- Source file is faststart (moov at byte 24) — pipe streaming works for this
  file. Non-faststart MP4s would NOT stream over a pipe at all (ffmpeg cannot
  seek back on stdin).
- Audio is HE-AAC (mp4a.40.5). The codecs endpoint used to declare
  `mp4a.40.2` unconditionally. Browser A/B test (MSE harness, see below):
  - Chrome: plays fine with both MIMEs (0 dropped frames / 1449 rendered).
  - Firefox: correct `mp4a.40.5` → smooth (frame-gap jitter p95 ≈ 1.7ms).
    Wrong `mp4a.40.2` → degraded jitter (p95 ≈ 8.4ms) and hard MSE wedges
    (two runs hung after 1–2 appends, no frame ever rendered).
- Ready-mode seek (fresh `?t=` session): first frame ~190ms at t=1800. This is
  already "instant" — keep it.
- The frontend's effective forward buffer is ~60s, not 120s: `MSE_MAX_AHEAD +
  MSE_KEEP_BEHIND = 120s` span cap includes the 60s kept *behind* the playhead.
- Firefox delivers fetch body chunks in ~16–32MB coalesced batches; Chrome in
  ~64KB slices. Chunk-size assumptions are unsafe.
- `await v.play()` can hang forever in some environments (reproduced in
  headless Chrome). The app already fire-and-forgets it — keep it that way.
- The frontend has **no retry** for the server's 503 (byte 0 not ready) despite
  `Retry-After`: it throws "Playback error: MSE stream returned 503".
- Streaming-mode seek is read-and-discard from byte 0: `serve_mse_from_stream`
  opens the `FileStream` at position 0 and lets ffmpeg consume everything up to
  `t`. The vendored librqbit `FileStream` implements `AsyncSeek` — unused.
- ffmpeg runs with `-loglevel error` and stderr → null: remux failures are
  silent (manifest as mysterious EOF or endless watchdog reconnects).
- The "indefinite buffering" incident is consistent with a stalled swarm
  (DHT-only, no trackers in magnet) + the frontend watchdog reconnecting every
  ~6–15s while nothing arrives, showing "Buffering…" forever with no surfacing
  of peers/speed.

## Test harness

`docs/browser-harness/` — self-contained, no build step:

```
cd docs/browser-harness
python3 harness.py            # serves the page on :8972, proxies /mse + /codecs to :8971
                              # (edit UP/PORT constants if the server runs elsewhere)
python3 ffrun.py 'http://127.0.0.1:8972/?mime=he405&secs=10'      # Firefox (geckodriver)
python3 crrun.py 'http://127.0.0.1:8972/?mime=server&secs=10'     # Chrome (playwright)
# params: mime=server|he405, t=<seek target>, secs=<measure window>
```

The page replicates the app's pump exactly (64KB appends, 120s span cap, 60s
behind-eviction, watchdog omitted) and reports: first-frame latency, frame-gap
jitter, waiting/stalled events *with buffer state at stall time*, dropped
frames, append counts. Use it as the acceptance gate for every phase below.

## Goals for "flawless"

1. Local (ready) playback: never buffers, seek always ≤ ~200ms. (Already true
   today; must not regress.)
2. Streaming (downloading) playback: plays at the download frontier without
   wedging; seek lands within the downloaded region in ≤ ~200ms; seeking past
   the frontier shows *real* progress ("downloading 4.2/12MB at 1.1MB/s"),
   never a silent infinite spinner.
3. No session can wedge silently: every stall is either self-healed with
   evidence or surfaced to the user with cause.
4. Browser matrix: Chrome and Firefox behave identically (Safari best-effort).

## Phase 0 — quick fixes (applied / trivial)

- [x] **Audio MIME profile fix** (`src/remux.rs`): `aac` + ffprobe profile
  `HE-AAC`/`HE-AAC v2` → `mp4a.40.5`, else `mp4a.40.2`. This alone removes the
  Firefox wedge/degradation class and is the most likely fix for the reported
  micro-buffering (user watches in Firefox).
- [x] Correct the false claims in AGENTS.md (503 retry that never existed,
  "instant seek" claims, browser quirks).
- [ ] Frontend fallback MIME (used only if `addSourceBuffer` throws) should try
  a small candidate list (`40.5` before `40.2`) instead of hardcoding `40.2`.
- [ ] Forward ffmpeg stderr to `tracing` (stderr → pipe, log non-empty lines at
  warn). Cost: a few lines in `remux.rs` + a reader task. Diagnosability win
  across everything else.

## Phase 1 — correctness (streaming mode)

1. **Seek via `FileStream::start_seek`** (http.rs, `serve_mse_from_stream`):
   convert `?t=` to a byte offset (ffprobe avg bitrate × t is not exact; use
   per-file `duration` from moov → bytes ≈ `file_len × t / duration`, then let
   ffmpeg's input-side `-ss` on a *seekable* wrapper do exact alignment). The
   clean way: wrap the `FileStream` in a `tokio::io::BufReader` that implements
   `AsyncSeek` passthrough (it already does), pass it to ffmpeg via
   `Command::stdin(AsyncApplicationBuilder)`… tokio's `ChildStdin` can't be an
   arbitrary `AsyncRead`, so instead run the copy task with
   `tokio::io::copy(&mut seeked_stream, &mut child_stdin)` after issuing
   `start_seek(Start(offset))` + `poll_complete` before the copy loop. Result:
   no read-and-discard of the whole prefix; the stream's 32MB priority window
   also jumps to the target.
2. **Seek-target readiness gate**: `is_file_byte_ready(file_id, byte_offset(t))`
   before answering; if not ready → `503 + Retry-After` (same as byte 0 today).
3. **Frontend 503 retry loop**: on 503, wait `Retry-After` (cap 5s) and refetch,
   with the stall overlay showing server-provided context (`X-Stall-Reason`:
   `head-not-downloaded` vs `seek-target-not-downloaded`, plus contiguous
   bytes/total). Never surface a hard error for a transient 503.
4. **Stall surfacing instead of blind heal**: keep the watchdog, but before any
   reconnect require `download_speed == 0 && no buffer growth for N seconds`
   (WS status already carries speed/peers); show "waiting for peers — 0.0 KB/s,
   N peers" in the overlay. This converts "indefinite buffering" into a
   diagnosable state.
5. **Segment-cache for ready files (optional here, required for Phase 2)**:
   on first play of a `ready` item, run `-c copy` segmentation to
   `library_dir/{id}/.dash/` (see Phase 2) in the background; serve the pipe
   until the cache is warm, then switch. Keeps Phase 1 independent of Phase 2.

Acceptance: harness runs (Firefox + Chrome) with `t=0`, `t=1800`,
`t=<near-frontier>` on a downloading torrent: no wedges, no silent stalls,
ready-mode TTFF ≤ 250ms.

## Phase 2 — target architecture: HTTP segment protocol

Replace the single infinite remux response with discrete segments. This is what
removes the whole class of "session" problems (watchdog, reconnect, MSE session
state, backpressure pump).

**Server**

- `POST`-time or first-play background job per item: segment with one process,
  no re-encode:
  `ffmpeg -i <src> -c copy -movflags +faststart -f dash -seg_duration 4
   -use_template 0 -use_timeline 0 <dir>/manifest.mpd` (or the mp4 muxer in a
   loop — dash is fine since we parse the mpd server-side).
  Output: `init.mp4` + `seg-N.m4s` (+ mpd with exact segment→time table) under
  `library_dir/{id}/.dash/`. For the 1.5GB test file this takes ~5-10s of CPU.
- New routes:
  - `GET /api/library/:id/manifest` → JSON: `{ mime, duration, segments:
    [{t, bytes, file}] }` (server-derived from the mpd; no client mpd parsing).
  - `GET /mse/:id/init`, `GET /mse/:id/seg/:n` → static file serve; 404+retry
    marker for segments past the download frontier (streaming mode), 200
    otherwise.
- Streaming mode: run the same dash muxer with input from the `FileStream`
  (stdin): segments materialize as bytes arrive; the manifest endpoint reports
  the *available* segment prefix + frontier time. Because segment k exists only
  when fully downloaded, "is segment k ready" becomes trivial and exact — no
  more byte-guessing.
- Non-faststart MP4s and MKVs stop being special cases: ffmpeg reads them fine
  when segmenting to a file (seekable input), which the pipe path could never
  do. (MKV stays remux-only; browsers still get fMP4.)

**Client**

- Delete: backpressure pump, byte-budget heuristics, watchdog heal, session
  restart machinery, `mseOff` bookkeeping.
- Keep: SourceBuffer MIME + eviction behind playhead, seekbar mapping.
- New loop: `while (ahead < TARGET) { seg = await fetchSegment(n++, {retry:
  503/404→backoff}); await appendBuffer(seg); }` — TARGET ~45s ahead. Seek =
  look up segment for t in the manifest; if unavailable (past frontier), show
  download progress and start pulling at the frontier segment instead.
- Natural speed adaptivity: if segments arrive slower than realtime, the
  overlay shows real numbers; no reconnect churn ever.

**Migration strategy**

- Ship segment protocol behind `?v=2` on `/mse/:id` (pipe = v1 fallback) and
  keep the pipe path for one release.
- A/B with the harness on both browsers across: t=0 / mid / past-frontier /
  network-throttled (simulate by pausing the torrent).
- Delete v1 once the harness matrix is green for a week of real use.

## Risks / notes

- DASH muxer writes an mpd we must parse — small, stable subset (single
  representation, fixed seg_duration). Alternatively `ffmpeg -f mp4` piped
  through a tiny in-process fMP4 splitter (moof box walk) — more code, zero
  format surprises; decide at implementation time.
- Disk cost: segment cache ≈ file size again for ready items → make it
  `.dash/` inside the item dir and delete on item delete (already covered by
  `remove_files_of` since it deletes the whole dir).
- Still-downloading + segmentation from stdin: if ffmpeg's dash muxer buffers
  more than one segment before writing, frontier reporting gets coarse —
  verify early with the 1.5GB file; the `-c copy` muxer is stream-friendly.
- Firefox fetch batching (~16-32MB) is irrelevant in the segment world (each
  segment ≈ 700KB at this bitrate).

## What shipped

- `src/segmenter.rs`: per-item segmenter (ffmpeg `-c copy` → interleaved fMP4
  split in-process into `init.mp4` + `seg-N.m4s`, keyframe-aligned, cached under
  `library_dir/{id}/.seg/`). Works from a file (finished) or the librqbit
  `FileStream` (downloading; throttles exactly at the frontier). ffmpeg stderr
  is captured and surfaced through the manifest.
- Routes: `GET /api/library/:id/stream` (manifest + lazy segmenter start),
  `GET /mse/:id/init`, `GET /mse/:id/seg/:n` (404 = not cut yet), dev harness
  at `/__test`.
- Frontend: one MSE session per playback, manifest-driven pump (~60s ahead /
  30s behind), `seekTo(t)` = native `v.currentTime` write, `ms.duration` pinned
  from the manifest, endOfStream at the end, real download numbers in the
  buffering overlay, clear error messages (undecodable codecs, ffmpeg failures).
  The old pipe protocol, watchdog self-healing, session restarts and 0-based
  seek bookkeeping are deleted.
- MIME probing is exact or absent: `probe_mse_mime` returns `None` when ffprobe
  can't see the codecs yet (no more guessed fallback that broke Firefox).
- Verified with `docs/browser-harness/page.html` served same-origin at
  `/__test` (real Firefox window + Chromium): first frame 91-258ms, mid-movie
  seek to first frame 63-124ms (both directions), zero dropped frames,
  jitter p95 ≤ 25ms. Rust e2e `segment_stream_pipeline` covers the server side.

Deliberately NOT built (kept simple on purpose): piece-reprioritization for
out-of-order seeks past the download frontier (a seek there waits for the
sequential download, with real progress shown), and client-side wasm decoding
for codecs browsers can't play (x265/HEVC in Chrome/Firefox) — those surface a
clear error instead.
