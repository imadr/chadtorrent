"""Offline WebCodecs regressions using generated media and the real application.

Requires ffmpeg, ffprobe, Python playwright, and Chrome (or CHROME_BIN).
Fixtures contain silent audio; browser/player output is muted, including PLAYER_SOURCE.
Run after cargo build: python tests/player_browser.py
Set PLAYER_BROWSER=firefox to exercise Playwright's installed Firefox.
Set PLAYER_HEAAC=1 on macOS to include an AudioToolbox HE-AAC fixture.
Set PLAYER_HEVC=1 to include a Main10 HEVC capability/rejection latency check.
Set PLAYER_FIREFOX_HEVC=1 to enable experimental HEVC in the isolated Firefox profile.
PLAYER_SOURCE=/path/to/episode.mkv adds repeated-seek checks against an existing file (read-only).
"""
import asyncio
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request

from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]


def generate(folder, duration, audio_duration=None, audio_delay=0, he_aac=False, hevc=False):
    folder.mkdir(parents=True)
    args = ["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
            f"testsrc2=duration={duration}:size=640x360:rate=24"]
    if audio_duration is not None:
        args += ["-itsoffset", str(audio_delay), "-f", "lavfi", "-i",
                 f"anullsrc=sample_rate=48000:channel_layout=mono:duration={audio_duration}"]
    args += ["-c:v", "libx265" if hevc else "libx264", "-preset", "fast", "-g", "144",
             "-keyint_min", "144", "-bf", "3"]
    if hevc:
        args += ["-pix_fmt", "yuv420p10le", "-x265-params", "level-idc=4:pools=1:log-level=error"]
    else:
        args += ["-sc_threshold", "0"]
    if audio_duration is not None:
        args += ["-c:a", "aac_at" if he_aac else "aac"]
        if he_aac:
            args += ["-profile:a", "4", "-b:a", "64000"]  # FF_PROFILE_AAC_HE
    subprocess.run(args + [str(folder / "clip.mp4")], check=True)


async def exercise(url, library, he_aac, hevc, restart_server, source=None):
    chrome = os.environ.get("CHROME_BIN") or shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome and Path("/Applications/Google Chrome.app").exists():
        chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    async with async_playwright() as p:
        browser_name = os.environ.get("PLAYER_BROWSER", "chromium")
        if browser_name == "firefox":
            prefs = {"media.volume_scale": "0.0"}
            if os.environ.get("PLAYER_FIREFOX_HEVC") == "1":
                prefs.update({"dom.media.webcodecs.h265.enabled": True, "media.hevc.enabled": True})
            browser = await p.firefox.launch(headless=True, firefox_user_prefs=prefs)
        else:
            browser = await p.chromium.launch(executable_path=chrome, headless=True,
                                             args=["--autoplay-policy=no-user-gesture-required", "--mute-audio"])
        try:
            page = await browser.new_page()
            errors = []
            messages = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("console", lambda m: messages.append(m.text) if m.type in ("error", "warning") else None)
            await page.goto(url)
            await page.wait_for_function("n=>items.length === n", arg=6 + int(he_aac) + int(hevc) + int(source is not None))
            await page.evaluate("""() => {
                // Keep decoding/scheduling audio for clock regressions, with
                // zero output gain in both browsers (also for real episodes).
                v.muted=true;
                window.testRecords=[];window.testEnds=[];
                window.testProbes=[];
                const configure=configureCandidate;
                configureCandidate=async(dec,cfg)=>{
                    const at=performance.now(),ok=await configure(dec,cfg);
                    testProbes.push({at,end:performance.now(),ok});return ok;
                };
                const original=paint;
                paint=f=>{testRecords.push({ts:f.timestamp/1e6,at:performance.now(),gen:segGen});original(f)};
                v.addEventListener('ended',()=>testEnds.push(testRecords.slice()));
                const b=document.createElement('button');b.id='test-start';b.textContent='Start';
                b.style='position:fixed;top:0;right:0;z-index:99999';
                b.onclick=()=>{v.muted=true;testRecords=[];testEnds=[];testProbes=[];window.testClickedAt=performance.now();const item=items.find(i=>i.id===testId);item.playhead=window.testPosition||0;playTorrent(item,window.testFile||null)};
                document.body.appendChild(b);
            }""")

            async def start(item_id):
                await page.evaluate("window.testFile=null")
                await page.evaluate("id=>window.testId=id", item_id)
                await page.click("#test-start")
                await page.wait_for_function("testRecords.length > 0", timeout=12000)
                assert await page.evaluate("v.muted && (!P.gain || P.gain.gain.value===0)")

            async def check_subtitles(item_id, file, expected=None):
                await page.evaluate("({id,file})=>{window.testId=id;window.testFile=file}", {"id": item_id, "file": file})
                await page.click("#test-start")
                await page.wait_for_function("testRecords.length>0 || P.err", timeout=20000)
                assert not await page.evaluate("P.err && P.err.message")
                assert await page.evaluate("v.muted && (!P.gain || P.gain.gain.value===0)")
                await page.evaluate("toggleSubsPanel(true)")
                tracks = await page.evaluate("subsLocal.filter(s=>s.source==='embedded')")
                if not tracks and expected is None:
                    return 0
                assert tracks, "No embedded subtitles listed"
                track = next((s for s in tracks if s["lang"] == "en" and "forced" not in s["label"]), tracks[0])
                row = page.locator('#subpanel .subrow[data-local][data-name="' + track["name"] + '"]')
                assert track["label"] in await row.inner_text()
                if expected == "Embedded episode one":
                    async def fail_subtitle(route):
                        await route.fulfill(status=503, body="temporary subtitle failure")
                    await page.route("**/sub/9/**", fail_subtitle)
                    await row.click(force=True)
                    await page.wait_for_function("subLoadError.length>0")
                    assert "Select it again to retry" in await page.locator("#subpanel").inner_text()
                    await page.unroute("**/sub/9/**", fail_subtitle)
                await row.click(force=True)
                await page.wait_for_function("subTrack() && subTrack().cues.length>0", timeout=10000)
                if expected:
                    assert await page.evaluate("subTrack().cues[0].text") == expected
                    assert abs(await page.evaluate("subTrack().cues[0].startTime") - 1) < 0.01
                await page.evaluate("()=>{toggleSubsPanel(false);v.pause();const c=subTrack().cues[0];seekTo((c.startTime+c.endTime)/2)}")
                await page.wait_for_function("pendSeek===null && !document.getElementById('suboverlay').hidden", timeout=10000)
                assert await page.evaluate("document.querySelector('#suboverlay span').textContent.includes(subTrack().cues[0].getCueAsHTML().textContent)")
                assert await page.evaluate("v.paused")
                return len(tracks)

            await check_subtitles(9, "one.mkv", "Embedded episode one")
            await check_subtitles(9, "two.mkv", "Embedded episode two")
            # Reopening the first file restores its own selection, never the
            # second episode's track; cached VTT must also render while paused.
            await page.evaluate("()=>{window.testId=9;window.testFile='one.mkv'}")
            await page.click("#test-start")
            await page.wait_for_function("subTrack() && subTrack().cues.length>0", timeout=10000)
            assert await page.evaluate("subTrack().cues[0].text") == "Embedded episode one"

            async def stress_seeks(targets):
                for target in targets:
                    await page.evaluate("t=>{testRecords=[];seekTo(t)}", target)
                    await page.wait_for_function("(testRecords.length>0 && pendSeek===null) || P.err", timeout=10000)
                    state = await page.evaluate("__seg()")
                    assert not state["error"], state
                    assert state["recoveryAttempts"] == 0, (target, state)
                    first = await page.evaluate("testRecords[0].ts")
                    assert abs(first - target) < 0.08, (target, first, state)
                await page.evaluate("""async()=>{
                    testRecords=[];
                    for(let i=0;i<40;i++){seekTo(10+(i%20)/10);await new Promise(r=>setTimeout(r,5));}
                    seekTo(12.3);
                }""")
                await page.wait_for_function("(testRecords.length>0 && pendSeek===null) || P.err", timeout=10000)
                assert not await page.evaluate("P.err && P.err.message")
                assert abs(await page.evaluate("testRecords[0].ts") - 12.3) < 0.08

            async def inject_decoder_failure(which, paused, target):
                if paused:
                    await page.evaluate("v.pause()")
                await page.evaluate("""({which,target})=>{
                    window.recoveryContext=P.ctx;testRecords=[];window.injected=false;
                    const key=which==='video'?'VideoDecoder':'AudioDecoder',Native=window[key];
                    window[key]=function(init){
                        const dec=new Native(init),decode=dec.decode.bind(dec);
                        dec.decode=chunk=>{
                            if(!window.injected){
                                window.injected=true;window[key]=Native;dec.close();
                                init.error(new DOMException('The given encoding is not supported.','NotSupportedError'));
                                return;
                            }decode(chunk);
                        };return dec;
                    };seekTo(target);
                }""", {"which": which, "target": target})
                await page.wait_for_function("(injected && testRecords.length>0 && pendSeek===null) || P.err", timeout=10000)
                state = await page.evaluate("__seg()")
                assert not state["error"], state
                assert state["paused"] == paused, state
                assert state["recoveryAttempts"] == 1, state
                assert await page.evaluate("P.ctx===recoveryContext")
                assert abs(await page.evaluate("testRecords[0].ts") - target) < 0.08

            await start(1)
            await stress_seeks([23.3, 61.3, 3.1, 35.7, 12.3] * 4)
            await inject_decoder_failure("video", False, 20.3)
            await inject_decoder_failure("audio", True, 8.3)
            await start(1)
            await page.evaluate("""()=>{
                window.flushContext=P.ctx;window.flushInjected=false;testRecords=[];
                const Native=window.AudioDecoder;
                window.AudioDecoder=function(init){
                    const dec=new Native(init),flush=dec.flush.bind(dec);
                    dec.flush=()=>{
                        if(!flushInjected){
                            flushInjected=true;window.failedFlushGen=segGen;
                            window.failedFlushPosition=pendSeek??mediaNow();window.AudioDecoder=Native;
                            return Promise.reject(new DOMException('Injected native flush failure','EncodingError'));
                        }return flush();
                    };return dec;
                };seekTo(61.3);
            }""")
            await page.wait_for_function("(flushInjected && testRecords.some(f=>f.gen>failedFlushGen) && pendSeek===null) || P.err", timeout=10000)
            assert await page.evaluate("!P.err && P.ctx===flushContext && P.recoveryAttempts===1")
            await page.evaluate("v.pause();testRecords=[];window.streamContext=P.ctx")
            failed_request = False

            async def fail_segment_once(route):
                nonlocal failed_request
                if not failed_request:
                    failed_request = True
                    await route.fulfill(status=503, body="temporary source failure")
                else:
                    await route.continue_()

            await page.route("**/mse/1/seg/**", fail_segment_once)
            await page.evaluate("seekTo(13.3)")
            await page.wait_for_function("P.err", timeout=6000)
            assert await page.evaluate("P.id===1 && P.ctx===streamContext && P.paused")
            await page.unroute("**/mse/1/seg/**")
            await page.evaluate("seekTo(13.3)")
            await page.wait_for_function("testRecords.length>0 && pendSeek===null && !P.err", timeout=6000)
            assert await page.evaluate("P.id===1 && P.ctx===streamContext && P.paused")
            await page.evaluate("""()=>{
                window.retryContext=P.ctx;testRecords=[];
                const Native=window.VideoDecoder;window.restoreVideoDecoder=()=>window.VideoDecoder=Native;
                window.VideoDecoder=function(init){
                    const dec=new Native(init);
                    dec.decode=()=>{throw new DOMException('Injected persistent decoder failure','EncodingError')};
                    return dec;
                };seekTo(13.3);
            }""")
            await page.wait_for_function("P.err", timeout=6000)
            assert await page.evaluate("P.id===1 && P.ctx===retryContext && P.paused")
            assert await page.evaluate("P.recoveryAttempts===4")
            await page.evaluate("()=>{restoreVideoDecoder();testRecords=[]}")
            await page.evaluate("playerPlay()")
            await page.wait_for_function("testRecords.length>0 && pendSeek===null && !P.err", timeout=6000)
            assert await page.evaluate("P.id===1 && P.ctx===retryContext && !P.paused")
            assert abs(await page.evaluate("testRecords[0].ts") - 13.3) < 0.08

            latencies = []
            for item_id in (1, 2):
                await start(item_id)
                await page.wait_for_function("__seg().clock>0.1 || P.err", timeout=3000)
                state = await page.evaluate("__seg()")
                assert state["clock"] > 0.1, state
                if item_id == 1:
                    # Compare the shipped demuxer's complete normalized timeline
                    # against original source packets, not against its own clock.
                    source_packets = json.loads(subprocess.check_output([
                        "ffprobe", "-v", "error", "-show_packets", "-show_entries",
                        "packet=stream_index,pts_time", "-of", "json",
                        str(library / "1" / "clip.mp4")]))["packets"]
                    parsed = await page.evaluate("""async()=>{
                        const m=await api('/api/library/1/stream');const out={video:[],audio:[]};
                        for(const seg of m.segments){
                            const buf=await fetchSegBuf(1,'seg/'+seg.n);
                            for(const [tid,samples] of parseSeg(buf,P.tracks)){
                                const tr=P.tracks.get(tid),offset=tr.kind==='video'?m.timing.video_offset:m.timing.audio_offset;
                                out[tr.kind].push(...samples.map(s=>s.pts/tr.timescale-offset));
                            }
                        }return out;
                    }""")
                    for tid, kind in ((0, "video"), (1, "audio")):
                        expected = [float(p["pts_time"]) for p in source_packets if p["stream_index"] == tid]
                        assert len(parsed[kind]) == len(expected), kind
                        assert max(abs(a - b) for a, b in zip(parsed[kind], expected)) < 0.000002, kind
                for target in (23.3, 61.3, 3.1):
                    await page.evaluate("t=>{testRecords=[];window.seekStarted=performance.now();seekTo(t)}", target)
                    await page.wait_for_function("testRecords.length > 0 && pendSeek === null", timeout=6000)
                    record = await page.evaluate("()=>({...testRecords[0],delay:testRecords[0].at-seekStarted})")
                    assert abs(record["ts"] - target) < 0.06, (target, record)
                    assert record["delay"] < 1000, record
                    latencies.append(round(record["delay"], 1))
                await page.evaluate("v.pause();testRecords=[];seekTo(8.3)")
                await page.wait_for_function("testRecords.length > 0 && pendSeek === null")
                await asyncio.sleep(0.3)
                state = await page.evaluate("__seg()")
                assert state["paused"] and state["ctx"] == "suspended", state
                assert abs(state["clock"] - 8.3) < 0.03, state

            # Recut while paused: stale immutable URLs must rebuild the engine
            # without changing playback position or resuming the AudioContext.
            previous = await page.evaluate("P.generation")
            await page.evaluate("window.savedAudioContext=P.ctx;testRecords=[]")
            with (library / "2" / "clip.mp4").open("ab") as file:
                file.write(b"\0\0\0\x08free")
            await page.evaluate("refreshManifest(segGen,P.id)")
            await page.wait_for_function("g=>P.generation!==g && testRecords.length>0 && pendSeek===null", arg=previous)
            state = await page.evaluate("__seg()")
            assert state["paused"] and abs(state["clock"] - 8.3) < 0.03, state
            assert await page.evaluate("P.ctx===savedAudioContext")

            # A process restart first returns 404 for the old URL. The client
            # must refresh the manifest, recreate the cache, and retain pause.
            await page.wait_for_function("P.fetchN>=10 && P.segs.has(10)", timeout=6000)
            previous = await page.evaluate("P.generation")
            # Routing disables the browser HTTP cache, exercising an uncached
            # old URL even though the seek tests already fetched this tiny file.
            await page.route("**/mse/**", lambda route: route.continue_())
            await restart_server()
            assert await page.evaluate("P.id===2"), await page.evaluate("__seg()")
            await page.evaluate("testRecords=[];seekTo(8.3)")
            await page.wait_for_function("g=>P.generation!==g && testRecords.length>0 && pendSeek===null", arg=previous, timeout=12000)
            state = await page.evaluate("__seg()")
            assert state["paused"] and abs(state["clock"] - 8.3) < 0.03, state
            assert await page.evaluate("P.ctx===savedAudioContext")
            await page.unroute("**/mse/**")

            # Delayed audio must retain the source's silence, not start at t=0.
            await start(3)
            state = await page.evaluate("__seg()")
            assert state["clock"] < 0.5, state
            schedules = await page.evaluate("P.sched")
            assert not schedules or schedules[0]["ts"] > 1.9, schedules

            # Flush B-frame tails and preserve video after audio EOF.
            for item_id, last_pts in ((4, 0.375), (5, 5.95)):
                await start(item_id)
                await page.wait_for_function("testEnds.length > 0", timeout=10000)
                end = await page.evaluate("testEnds[0]")
                assert max(f["ts"] for f in end) >= last_pts - 0.001, end
            await page.evaluate("v.pause();testRecords=[];seekTo(6)")
            await page.wait_for_function("testRecords.length>0 && pendSeek===null", timeout=6000)
            await asyncio.sleep(0.5)
            state = await page.evaluate("__seg()")
            assert state["paused"] and abs(state["clock"] - 6) < 0.03, state
            he_aac_result = "not requested"
            if he_aac:
                await page.evaluate("window.testId=6")
                await page.click("#test-start")
                await page.wait_for_function("(testRecords.length>0 && mediaNow()>0.15) || ((P.id===null || P.err) && document.getElementById('buf').innerText.includes('Playback error'))", timeout=12000)
                if await page.evaluate("P.id===null || !!P.err"):
                    error = await page.evaluate("document.getElementById('buf').innerText")
                    assert "encoding is not supported" in error or "No WebCodecs audio decoder" in error, error
                    assert await page.evaluate("document.getElementById('buf').querySelector('.spin').hidden")
                    he_aac_result = "unsupported decoder; terminal error verified"
                else:
                    await page.evaluate("testRecords=[];seekTo(3.1)")
                    await page.wait_for_function("testRecords.length > 0 && pendSeek===null", timeout=6000)
                    await asyncio.sleep(0.3)
                    state = await page.evaluate("__seg()")
                    assert abs(state["adrift"] or 0) < 0.002, state
                    assert state["clock"] > 3.1 and state["ctx"] == "running", state
                    he_aac_result = "played and sought"
            hevc_result = "not requested"
            if hevc:
                await page.evaluate("window.testId=7")
                await page.click("#test-start")
                await page.wait_for_function("testRecords.length > 0 || (P.id===null && document.getElementById('buf').innerText.toLowerCase().includes('decoder'))", timeout=6000)
                probes = await page.evaluate("testProbes")
                probe_ms = probes[-1]["end"] - probes[0]["at"]
                if await page.evaluate("P.id===null"):
                    error = await page.evaluate("document.getElementById('buf').innerText")
                    assert "no hvc1.2.4.L120.B0 decoder in WebCodecs" in error, error
                    assert probe_ms < 500, probes
                    assert await page.evaluate("document.getElementById('buf').querySelector('.spin').hidden")
                    hevc_result = {"unsupported_decoder_probe_ms": round(probe_ms, 1),
                                   "click_to_error_ms": await page.evaluate("Math.round(performance.now()-testClickedAt)")}
                else:
                    hevc_result = {"played_decoder_probe_ms": round(probe_ms, 1)}
            if source:
                await page.evaluate("file=>{window.testId=8;window.testFile=file;window.testPosition=279.9033785238093}", source.name)
                await page.click("#test-start")
                await page.wait_for_function("(testRecords.length>0 && pendSeek===null) || P.err", timeout=20000)
                assert not await page.evaluate("P.err && P.err.message")
                assert await page.evaluate("P.recoveryAttempts===0"), await page.evaluate("__seg()")
                assert abs(await page.evaluate("testRecords[0].ts") - 279.9033785238093) < 0.08
                await page.evaluate("window.testPosition=0")
                await stress_seeks([279.9033785238093, 295.07, 1043.41, 1013.2,
                                    1055.51, 1109.95, 1306.53, 1438.093641]
                                   + [114.046, 500.3, 25.1, 113.9, 900.3, 114.04] * 4)
                await inject_decoder_failure("video", False, 114.04)
                await inject_decoder_failure("audio", True, 114.04)
                embedded_tracks = await check_subtitles(8, source.name)
                print(json.dumps({"existing_source_embedded_tracks": embedded_tracks}))
            assert not errors, errors
            print(json.dumps({"browser": browser_name, "version": browser.version, "seek_ms": latencies,
                              "checks": ["accurate seeks", "video-only", "paused seek", "cache rebuild",
                                         "server restart", "delayed audio", "short B-frame EOF", "audio EOF video tail", "paused EOF seek",
                                         "repeated/rapid seeks", "video decoder recovery", "paused audio decoder recovery", "flush recovery",
                                         "stream error recovery by seeking", "bounded recovery and normal play control",
                                         "embedded subtitle menu, normalized timing and paused rendering",
                                         "subtitle error retry", "per-episode subtitle persistence"],
                              "he_aac": he_aac_result,
                              "hevc": hevc_result,
                              "existing_source": str(source) if source else None,
                              "page_errors": errors}))
        except Exception:
            print("PLAYER STATE", await page.evaluate("()=>({state:__seg(),overlay:document.getElementById('buf').innerText,records:testRecords.slice(0,3)})"))
            print("BROWSER MESSAGES", messages)
            raise
        finally:
            await browser.close()


async def main():
    with tempfile.TemporaryDirectory(prefix="chadtorrent-player-") as tmp:
        tmp = Path(tmp)
        library = tmp / "library"
        generate(library / "1", 70, 70)
        generate(library / "2", 70)
        generate(library / "3", 6, 4, 2)
        generate(library / "4", 0.4, 0.4)
        generate(library / "5", 6, 3)
        (library / "9").mkdir()
        for file, text in [("one.mkv", "Embedded episode one"), ("two.mkv", "Embedded episode two")]:
            subtitle = tmp / "input.srt"
            subtitle.write_text(f"1\n00:00:01,000 --> 00:00:03,000\n{text}\n")
            subprocess.run(["ffmpeg", "-v", "error", "-i", str(library / "3" / "clip.mp4"), "-i", str(subtitle),
                            "-map", "0", "-map", "1:0", "-c", "copy", "-c:s", "srt",
                            "-metadata:s:s:0", "language=eng", "-metadata:s:s:0", "title=English",
                            "-output_ts_offset", "5", str(library / "9" / file)], check=True)
        he_aac = os.environ.get("PLAYER_HEAAC") == "1"
        if he_aac:
            generate(library / "6", 6, 6, he_aac=True)
        hevc = os.environ.get("PLAYER_HEVC") == "1"
        if hevc:
            generate(library / "7", 2, hevc=True)
        source = Path(os.environ["PLAYER_SOURCE"]).resolve() if os.environ.get("PLAYER_SOURCE") else None
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        with (tmp / "server.log").open("w+") as log:
            def launch():
                return subprocess.Popen([str(ROOT / "target/debug/chadtorrent"), "--port", str(port),
                                         "--library-dir", str(library), "--temp-dir", str(tmp / "temp"),
                                         "--db-path", str(tmp / "test.db")], cwd=tmp, stdout=log, stderr=log)

            async def wait_ready():
                deadline = time.monotonic() + 15
                while True:
                    try:
                        with urllib.request.urlopen(url + "/api/library", timeout=1):
                            break
                    except OSError:
                        if time.monotonic() > deadline or server.poll() is not None:
                            raise RuntimeError("test server did not start")
                        await asyncio.sleep(0.1)

            async def restart_server():
                nonlocal server
                server.terminate()
                server.wait(timeout=10)
                with sqlite3.connect(tmp / "test.db") as db:
                    # Fixtures have no real torrent; prevent startup from
                    # attempting to register their synthetic magnet strings.
                    db.execute("UPDATE library SET magnet=''")
                server = launch()
                await wait_ready()

            server = launch()
            try:
                await wait_ready()
                with sqlite3.connect(tmp / "test.db") as db:
                    fixture_ids = list(range(1, 6)) + [9] + ([6] if he_aac else []) + ([7] if hevc else []) + ([8] if source else [])
                    for item_id in fixture_ids:
                        db.execute("INSERT INTO library (id,magnet,title,state,file_path,added_at) "
                                   "VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
                                   (item_id, "audit", f"fixture-{item_id}", "ready", str(source.parent if item_id == 8 else library / str(item_id))))
                await exercise(url, library, he_aac, hevc, restart_server, source)
            except Exception:
                log.flush(); log.seek(0)
                print(log.read())
                raise
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill(); server.wait()


if __name__ == "__main__":
    asyncio.run(main())
