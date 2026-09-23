#!/usr/bin/env python3
"""Repro: does a past-buffer seek on a ready file bounce back to the session
start? Boots only user1 (port 3001), plays item 56, then seeks +200s and
samples __seg()/seekable/fullDur to see what the browser actually does.
"""
import glob, json, os, signal, subprocess, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent.parent
LOG = Path("/tmp/ct_seekrepro")
LOG.mkdir(exist_ok=True)
PORT = 3001

p = subprocess.Popen(
    [str(ROOT / "target/debug/chadtorrent"), "--port", str(PORT),
     "--library-dir", "./local/library", "--temp-dir", "./local/temp",
     "--db-path", "./local/chadtorrent.db"],
    cwd=str(ROOT), stdout=open(LOG / "u1.log", "wb"), stderr=subprocess.STDOUT,
    env=dict(os.environ, RUST_LOG="info"),)
try:
    while True:
        try:
            import socket
            s = socket.socket(); s.bind(("127.0.0.1", PORT)); s.close(); time.sleep(0.3)
        except OSError:
            break
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=True)
        pg = b.new_page()
        errs = []
        pg.on("pageerror", lambda e: errs.append(str(e)))
        pg.goto(f"http://127.0.0.1:{PORT}/", wait_until="domcontentloaded")
        pg.wait_for_function("typeof playTorrent === 'function' && typeof seekTo === 'function'", timeout=20000)

        def find():
            for _ in range(60):
                try:
                    r = pg.evaluate("""() => {
                        const m = items.filter(i => i.magnet && i.magnet.indexOf('3F8F219568B8B229') !== -1);
                        return m.length ? { id: m[0].id, state: m[0].state } : null;
                    }""")
                    if r and r["state"] != "_pending": return r
                except Exception: pass
                time.sleep(0.5)
            return None

        it = find()
        print("item:", it)
        pg.evaluate("playTorrent(items.find(i => i.id === %d))" % it["id"])
        # wait for playback to start
        t0 = time.time()
        while time.time() - t0 < 90:
            d = pg.evaluate("""() => {
                const o = { vT: v.currentTime || 0, paused: v.paused, readyState: v.readyState,
                            fullDur, pump: window.__pump ? (window.__pump.stage + ' n=' + window.__pump.n) : null,
                            segEos: segEosDone };
                try { o.seekable = ranges ? ranges(v.seekable) : 'nope'; } catch(e) { o.seekable = 'err ' + e; }
                try { o.vBuf = ranges ? ranges(v.buffered) : 'nope'; } catch(e) { o.vBuf = 'err ' + e; }
                return o;
            }""")
            print("pre ", d)
            if (d.get("vT") or 0) > 5: break
            time.sleep(0.4)

        target = round((pg.evaluate("v.currentTime") or 0) + 200, 3)
        print("seekTo:", target)
        pg.evaluate(f"seekTo({target})")
        for i in range(30):
            d = pg.evaluate("""() => {
                const o = { vT: v.currentTime || 0, paused: v.paused, readyState: v.readyState,
                            fullDur, pendSeek, pump: window.__pump ? (window.__pump.stage + ' n=' + window.__pump.n) : null,
                            lastAppendMs: lastAppendAt ? Date.now()-lastAppendAt : null, segEos: segEosDone };
                try { o.seekable = ranges ? ranges(v.seekable) : 'nope'; } catch(e) { o.seekable = 'err'; }
                try { o.vBuf = ranges ? ranges(v.buffered) : 'nope'; } catch(e) { o.vBuf = 'err'; }
                return o;
            }""")
            print("%3ds " % (i*0.5), d)
            time.sleep(0.5)
        print("pageerrors:", errs[:5])
finally:
    p.send_signal(signal.SIGTERM)
    time.sleep(0.5)
    p.kill()