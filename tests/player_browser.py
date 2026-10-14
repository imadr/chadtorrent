"""Offline WebCodecs regressions using generated media and the real application.

Requires ffmpeg, ffprobe, Python playwright, and Chrome (or CHROME_BIN).
Run after cargo build: python tests/player_browser.py
Set PLAYER_BROWSER=firefox to exercise Playwright's installed Firefox.
Set PLAYER_HEAAC=1 on macOS to include an AudioToolbox HE-AAC fixture.
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


def generate(folder, duration, audio_duration=None, audio_delay=0, he_aac=False):
    folder.mkdir(parents=True)
    args = ["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
            f"testsrc2=duration={duration}:size=640x360:rate=24"]
    if audio_duration is not None:
        args += ["-itsoffset", str(audio_delay), "-f", "lavfi", "-i",
                 f"sine=frequency=440:duration={audio_duration}:sample_rate=48000"]
    args += ["-c:v", "libx264", "-preset", "fast", "-g", "144",
             "-keyint_min", "144", "-sc_threshold", "0", "-bf", "3"]
    if audio_duration is not None:
        args += ["-c:a", "aac_at" if he_aac else "aac"]
        if he_aac:
            args += ["-profile:a", "4", "-b:a", "64000"]  # FF_PROFILE_AAC_HE
    subprocess.run(args + [str(folder / "clip.mp4")], check=True)


async def exercise(url, library, he_aac, restart_server):
    chrome = os.environ.get("CHROME_BIN") or shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome and Path("/Applications/Google Chrome.app").exists():
        chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    async with async_playwright() as p:
        browser_name = os.environ.get("PLAYER_BROWSER", "chromium")
        if browser_name == "firefox":
            browser = await p.firefox.launch(headless=True)
        else:
            browser = await p.chromium.launch(executable_path=chrome, headless=True,
                                             args=["--autoplay-policy=no-user-gesture-required"])
        try:
            page = await browser.new_page()
            errors = []
            messages = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("console", lambda m: messages.append(m.text) if m.type in ("error", "warning") else None)
            await page.goto(url)
            await page.wait_for_function("n=>items.length === n", arg=6 if he_aac else 5)
            await page.evaluate("""() => {
                window.testRecords=[];window.testEnds=[];
                const original=paint;
                paint=f=>{testRecords.push({ts:f.timestamp/1e6,at:performance.now()});original(f)};
                v.addEventListener('ended',()=>testEnds.push(testRecords.slice()));
                const b=document.createElement('button');b.id='test-start';b.textContent='Start';
                b.style='position:fixed;top:0;right:0;z-index:99999';
                b.onclick=()=>{testRecords=[];testEnds=[];const item=items.find(i=>i.id===testId);item.playhead=0;playTorrent(item)};
                document.body.appendChild(b);
            }""")

            async def start(item_id):
                await page.evaluate("id=>window.testId=id", item_id)
                await page.click("#test-start")
                await page.wait_for_function("testRecords.length > 0", timeout=12000)

            latencies = []
            for item_id in (1, 2):
                await start(item_id)
                await asyncio.sleep(0.3)
                state = await page.evaluate("__seg()")
                assert state["clock"] > 0.1, state
                if item_id == 1:
                    # Compare the shipped demuxer's complete normalized timeline
                    # against original source packets, not against its own clock.
                    source = json.loads(subprocess.check_output([
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
                        expected = [float(p["pts_time"]) for p in source if p["stream_index"] == tid]
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
            await page.evaluate("window.savedAudioContext=P.ctx")
            with (library / "2" / "clip.mp4").open("ab") as file:
                file.write(b"\0\0\0\x08free")
            await page.evaluate("refreshManifest(segGen,P.id)")
            await page.wait_for_function("g=>P.generation!==g && P.tracks && pendSeek===null", arg=previous)
            state = await page.evaluate("__seg()")
            assert state["paused"] and abs(state["clock"] - 8.3) < 0.03, state
            assert await page.evaluate("P.ctx===savedAudioContext")

            # A process restart first returns 404 for the old URL. The client
            # must refresh the manifest, recreate the cache, and retain pause.
            previous = await page.evaluate("P.generation")
            # Routing disables the browser HTTP cache, exercising an uncached
            # old URL even though the seek tests already fetched this tiny file.
            await page.route("**/mse/**", lambda route: route.continue_())
            await restart_server()
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
                await page.wait_for_function("testRecords.length > 0 || (P.id===null && document.getElementById('buf').innerText.includes('Playback error'))", timeout=12000)
                if await page.evaluate("P.id===null"):
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
            assert not errors, errors
            print(json.dumps({"browser": browser_name, "version": browser.version, "seek_ms": latencies,
                              "checks": ["accurate seeks", "video-only", "paused seek", "cache rebuild",
                                         "server restart", "delayed audio", "short B-frame EOF", "audio EOF video tail", "paused EOF seek"],
                              "he_aac": he_aac_result,
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
        he_aac = os.environ.get("PLAYER_HEAAC") == "1"
        if he_aac:
            generate(library / "6", 6, 6, he_aac=True)
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
                    for item_id in range(1, 7 if he_aac else 6):
                        db.execute("INSERT INTO library (id,magnet,title,state,file_path,added_at) "
                                   "VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)",
                                   (item_id, "audit", f"fixture-{item_id}", "ready", str(library / str(item_id))))
                await exercise(url, library, he_aac, restart_server)
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
