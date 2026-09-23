#!/usr/bin/env python3
"""End-to-end watch-party sync test.

Topology (all on loopback):
  3000  chadtorrent-relay  (UDP punch hub)
  3001  chadtorrent user1  -> ./local/library  + ./local/chadtorrent.db
  3002  chadtorrent user2  -> ./local/library2 + ./local/chadtorrent2.db

Both instances run with --party-relay ws://127.0.0.1:3000. The same Pulp
Fiction torrent exists on both sides (same infohash), so the join dedupes
to the existing item and no swarm download is needed.

Drives two real Chromium tabs through the app's own JS (the exact functions
the buttons call) and asserts the user's scenario:
  create -> play (user1) -> late join (user2) catches position unpaused
  seek / pause / play propagate user1 -> user2 and user2 -> user1
  leave + rejoin keeps sync

Usage:
  python3 party_test.py            # headless
  HEADLESS=0 python3 party_test.py # visible windows, see what it does
"""

import glob
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent.parent
HEADLESS = os.environ.get("HEADLESS", "1") == "1"
LOG_DIR = Path("/tmp/ct_party_test")
LOG_DIR.mkdir(exist_ok=True)

RELAY_PORT, U1_PORT, U2_PORT = 3000, 3001, 3002
RELAY_URL = f"ws://127.0.0.1:{RELAY_PORT}"
INFOHASH_HINT = "3F8F219568B8B229"

BIN = ROOT / "target" / "debug"
procs = []


def clear_party_rows():
    for db in ("local/chadtorrent.db", "local/chadtorrent2.db"):
        con = sqlite3.connect(ROOT / db)
        con.execute("DELETE FROM party")
        con.commit()
        con.close()


def spawn(name, args, level="info"):
    f = open(LOG_DIR / f"{name}.log", "wb")
    env = dict(os.environ, RUST_LOG=level, RUST_BACKTRACE="1")
    p = subprocess.Popen(args, cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT, env=env)
    procs.append(p)
    return p


def free_port(port):
    import socket
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def wait_http(port, timeout=30):
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/party", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


class Client:
    def __init__(self, page, name):
        self.page = page
        self.name = name
        self.console = []
        self.errors = []
        self.ws = []
        self.timeline = []
        page.on("console", lambda m: self.console.append((time.time(), m.type, m.text)))
        page.on("pageerror", lambda e: self.errors.append((time.time(), str(e))))
        page.on("websocket", self._on_ws)

    def _on_ws(self, ws):
        import re as _re
        def redact(f):
            s = _re.sub(r'"invite":"[^"]*"', '"invite":"…"', str(f)[:4000])
            return s
        self.ws.append((time.time(), "ws-open"))
        ws.on("framesent", lambda f: self.ws.append((time.time(), "send", redact(f))))
        ws.on("framereceived", lambda f: self.ws.append((time.time(), "recv", redact(f))))
        ws.on("close", lambda: self.ws.append((time.time(), "ws-close")))

    def ev(self, js, *args):
        return self.page.evaluate(js, *args)

    def sample(self):
        d = {}
        try:
            raw = self.page.evaluate("""() => {
                const o = { t: Date.now() };
                try { o.vT = v.currentTime || 0; } catch(e) { o.vT = -1; }
                try { o.paused = v.paused; } catch(e) { o.paused = null; }
                try { o.src = !!v.src; } catch(e) { o.src = false; }
                try { o.cur = currentId; } catch(e) { o.cur = null; }
                try { o.conn = party ? party.conn : null; } catch(e) { o.conn = null; }
                try { o.roster = party ? (party.peers || []).length : -1; } catch(e) { o.roster = -1; }
                try { o.mePos = party && party.me ? party.me.pos : null; } catch(e) { o.mePos = null; }
                try { o.mePlay = party && party.me ? party.me.playing : null; } catch(e) { o.mePlay = null; }
                try { o.meBuf = party && party.me ? party.me.buffering : null; } catch(e) { o.meBuf = null; }
                try { o.anchor = partyAnchor ? partyAnchor.t : null; } catch(e) { o.anchor = null; }
                try { o.anchorPlay = partyAnchor ? partyAnchor.playing : null; } catch(e) { o.anchorPlay = null; }
                try { o.apply = partyApply; } catch(e) { o.apply = -1; }
                try { o.auto = partyAutoPaused; } catch(e) { o.auto = null; }
                try { o.hold = partyHoldPlay; } catch(e) { o.hold = null; }
                try { o.bufN = segSb ? segSb.buffered.length : -1; } catch(e) { o.bufN = -1; }
                try { o.ws = (partyWs && partyWs.readyState) || -2; } catch(e) { o.ws = -3; }
                return o;
            }""")
            if isinstance(raw, dict):
                d.update(raw)
        except Exception as e:
            d["eval_error"] = str(e)
        d["wall"] = time.time()
        self.timeline.append(d)
        return d


class Test:
    def __init__(self, c1, c2):
        self.c1, self.c2 = c1, c2
        self.passes, self.fails = [], []
        self.steps = 0

    def step(self, name):
        self.steps += 1
        print(f"\n== {self.steps}. {name} ==")

    def ok(self, name, detail=None):
        self.passes.append(name)
        print(f"  PASS  {name}" + (f"  ({detail})" if detail else ""))

    def fail(self, name, detail):
        self.fails.append(name)
        print(f"  FAIL  {name}  -> {detail}")

    def wait(self, c, pred, timeout, what):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = c.sample()
            if pred(last):
                return last
            time.sleep(0.2)
        return None

    def wait_playing(self, c, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = c.sample()
            if cur.get("vT", 0) and cur.get("vT", 0) > 0.5:
                return cur
            time.sleep(0.25)
        return None

    def wait_conn(self, c, state, timeout=30):
        return self.wait(c, lambda d: d.get("conn") == state, timeout, f"conn={state}")

    def wait_roster(self, c, n, timeout=30):
        return self.wait(c, lambda d: d.get("roster") == n, timeout, f"roster={n}")


def wait_both_close(c1, c2, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        a, b = c1.sample(), c2.sample()
        if a.get("vT", 0) > 1 and b.get("vT", 0) > 1 and abs(a.get("vT", 0) - b.get("vT", 0)) < 4:
            return True
        time.sleep(0.2)
    return False


def main():
    global procs
    deadline = time.time() + 15
    while True:
        busy = [p for p in (RELAY_PORT, U1_PORT, U2_PORT) if not free_port(p)]
        if not busy:
            break
        if time.time() > deadline:
            assert False, f"ports still busy: {busy}"
        time.sleep(0.5)

    clear_party_rows()

    relay_p = spawn("relay", [str(BIN / "chadtorrent-relay"), "--port", str(RELAY_PORT)], level="debug")
    spawn("u1", [
        str(BIN / "chadtorrent"),
        "--port", str(U1_PORT),
        "--library-dir", "./local/library",
        "--temp-dir", "./local/temp",
        "--db-path", "./local/chadtorrent.db",
        "--party-relay", RELAY_URL,
    ])
    spawn("u2", [
        str(BIN / "chadtorrent"),
        "--port", str(U2_PORT),
        "--library-dir", "./local/library2",
        "--temp-dir", "./local/temp2",
        "--db-path", "./local/chadtorrent2.db",
        "--party-relay", RELAY_URL,
    ])

    try:
        assert wait_http(U1_PORT), "user1 HTTP not up"
        assert wait_http(U2_PORT), "user2 HTTP not up"
        print("servers up: relay:3000 u1:3001 u2:3002")
    except Exception:
        dump_logs(relay_p)
        raise

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx1, ctx2 = browser.new_context(), browser.new_context()
        pg1, pg2 = ctx1.new_page(), ctx2.new_page()
        c1, c2 = Client(pg1, "u1"), Client(pg2, "u2")
        t = Test(c1, c2)

        pg1.goto(f"http://127.0.0.1:{U1_PORT}/", wait_until="domcontentloaded")
        pg2.goto(f"http://127.0.0.1:{U2_PORT}/", wait_until="domcontentloaded")
        pg1.wait_for_function("typeof partyCreate === 'function' && typeof refreshLibrary === 'function'", timeout=20000)
        pg2.wait_for_function("typeof partyJoin === 'function' && typeof refreshLibrary === 'function'", timeout=20000)

        def find_item(c, hint=INFOHASH_HINT):
            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    res = c.ev("""(h) => {
                        const arr = (typeof items !== 'undefined' && items) ? items : [];
                        const m = arr.filter(i => i.state !== '_pending' && i.magnet && i.magnet.indexOf(h) !== -1);
                        return m.length ? { id: m[0].id, state: m[0].state } : null;
                    }""", hint)
                    if res:
                        return res
                except Exception:
                    pass
                time.sleep(0.5)
            return None

        # ----- app ready -----
        t.step("both apps loaded, Pulp Fiction present")
        i1 = find_item(c1)
        i2 = find_item(c2)
        assert i1 and i2, f"item not found: u1={i1} u2={i2}"
        t.ok("items loaded", f"u1 item={i1} u2 item={i2}")

        # ----- user1 creates -----
        t.step("user1 creates a party on Pulp Fiction")
        c1.ev(f"partyCreate({i1['id']})")
        assert t.wait(c1, lambda d: d.get("conn") is not None, 30, "party set"), "user1 party never materialised"
        invite = c1.ev("party ? party.invite : null")
        assert invite and str(invite).startswith("CTPARTY1:"), f"bad invite: {invite!r}"
        t.ok("u1 created", f"invite={invite[:24]}…")
        t.wait_conn(c1, "online", 30) or t.fail("u1 online", "never online")
        if t.fails:
            dump_all(t, c1, c2, "create")
            sys.exit(1)

        # ----- user1 plays -----
        t.step("user1 starts playing; lets the movie climb past 6s")
        c1.ev("playTorrent(items.find(i => i.id === %d))" % i1["id"])
        t.wait_playing(c1, timeout=120) or t.fail("u1 playing", "never started")
        t.wait(c1, lambda d: d.get("vT", 0) > 6, 60, "u1 past 6s") or t.fail("u1 advances", "stalled")
        if t.fails:
            dump_all(t, c1, c2, "u1-play")
            sys.exit(1)
        print(f"  u1 at {c1.sample().get('vT')}s")

        # ----- user2 joins -----
        t.step("user2 joins via invite")
        c2.ev(f"partyJoin({json.dumps(invite)})")
        t.wait_conn(c2, "online", 40) or t.fail("u2 online", "never punched online")
        t.wait_roster(c2, 1, 40) or t.fail("u2 roster", "other member missing")
        t.wait_roster(c1, 1, 40) or t.fail("u1 roster", "u2 not in u1 roster")
        t.ok("rosters converged both sides", f"u1={c1.sample().get('roster')} u2={c2.sample().get('roster')}")
        if t.fails:
            dump_all(t, c1, c2, "join")
            sys.exit(1)

        # ----- user2 watches, catches position unpaused -----
        t.step("user2 watches: picks up where u1 is, unpaused")
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                has = c2.ev("items.some(i => i.id === %d)" % i2["id"])
                if has:
                    break
            except Exception:
                pass
            time.sleep(0.3)
        c2.ev("playTorrent(items.find(i => i.id === %d))" % i2["id"])
        deadline = time.time() + 120
        caught = None
        while time.time() < deadline:
            a, b = c1.sample(), c2.sample()
            d = abs(b.get("vT", 0) - a.get("vT", 0))
            if not b.get("paused") and a.get("vT", 0) > 8 and d < 5:
                caught = (a.get("vT"), b.get("vT"))
                break
            time.sleep(0.2)
        if caught:
            t.ok("u2 caught up unpaused", f"|Δ|={abs(caught[0]-caught[1]):.1f}s @ {caught[0]:.1f}s")
        else:
            dump_all(t, c1, c2, "join-catchup")
            sys.exit(1)

        # ----- u1 seek -> u2 -----
        t.step("user1 seeks +200s -> user2 follows")
        target = c1.sample().get("vT", 10) + 200
        c1.ev(f"seekTo({target})")
        if wait_both_close(c1, c2, 40):
            t.ok("u2 followed u1 seek", f"to {target:.0f}s")
        else:
            t.fail("u2 seek-follow", "u2 never converged after u1 seek")

        # ----- u1 pause / play -> u2 -----
        t.step("user1 pause -> user2 pauses")
        # A pause click is only meaningful while actually playing. Right after
        # a seek the strict-stall machinery may have auto-paused this very
        # player ("Waiting for X…"); pressing pause then is a no-op and would
        # verify nothing. Wait for a settled, playing player first.
        if not t.wait(c1, lambda d: d.get("paused") is False and not d.get("auto"), 25, "u1 settled playing"):
            t.fail("u1 playing", "u1 not playing/auto-paused at pause time")
        c1.ev("v.pause()")
        if t.wait(c2, lambda d: d.get("paused") is True, 8, "u2 paused"):
            t.ok("u2 paused after u1 pause")
        else:
            t.fail("u2 pause-sync", "u2 did not pause")

        t.step("user1 play -> user2 plays")
        c1.ev("v.play()")
        if t.wait(c2, lambda d: d.get("paused") is False, 8, "u2 playing"):
            t.ok("u2 resumed after u1 play")
        else:
            t.fail("u2 play-sync", "u2 did not resume")
        time.sleep(2)

        # ----- reverse: u2 -> u1 -----
        t.step("user2 seeks +120s -> user1 follows")
        tgt2 = c2.sample().get("vT", 10) + 120
        c2.ev(f"seekTo({tgt2})")
        if wait_both_close(c1, c2, 40):
            t.ok("u1 followed u2 seek", f"to {tgt2:.0f}s")
        else:
            t.fail("u1 seek-follow", "u1 never converged after u2 seek")

        t.step("user2 pause -> user1 pauses")
        if not t.wait(c2, lambda d: d.get("paused") is False and not d.get("auto"), 25, "u2 settled playing"):
            t.fail("u2 playing", "u2 not playing/auto-paused at pause time")
        c2.ev("v.pause()")
        if t.wait(c1, lambda d: d.get("paused") is True, 8, "u1 paused"):
            t.ok("u1 paused after u2 pause")
        else:
            t.fail("u1 pause-sync", "u1 did not pause")

        t.step("user2 play -> user1 plays")
        c2.ev("v.play()")
        if t.wait(c1, lambda d: d.get("paused") is False, 8, "u1 playing"):
            t.ok("u1 resumed after u2 play")
        else:
            t.fail("u1 play-sync", "u1 did not resume")
        time.sleep(2)

        # ----- steady sync -----
        t.step("steady playback ~10s stays within 5s both ways")
        maxg = 0.0
        for _ in range(50):
            a, b = c1.sample(), c2.sample()
            maxg = max(maxg, abs(b.get("vT", 0) - a.get("vT", 0)))
            time.sleep(0.2)
        if maxg < 5:
            t.ok("steady sync holds", f"max |Δ|={maxg:.2f}s")
        else:
            t.fail("steady sync", f"drift {maxg:.2f}s")

        # ----- quit & rejoin -----
        t.step("user2 leaves, then rejoins, sync still works")
        c2.ev("partyLeave()")
        time.sleep(0.3)
        c2.ev("partyLeave()")
        t.wait(c2, lambda d: d.get("conn") is None, 20, "u2 left") or t.fail("u2 leave", "party not null")
        t.ok("u2 left", "")
        c2.ev(f"partyJoin({json.dumps(invite)})")
        t.wait_conn(c2, "online", 40) or t.fail("u2 rejoined online", "never online")
        t.wait_roster(c2, 1, 40) or t.fail("u2 rejoin roster", "roster missing")
        t.ok("u2 rejoined online, roster back", "")
        time.sleep(4)

        t.step("post-rejoin: u1 seek -> u2 follows")
        c1.ev("v.play()")
        tgt3 = c1.sample().get("vT", 10) + 150
        c1.ev(f"seekTo({tgt3})")
        if wait_both_close(c1, c2, 45):
            t.ok("u2 followed seek after rejoin", f"to {tgt3:.0f}s")
        else:
            t.fail("rejoin seek-follow", "u2 did not converge post-rejoin")

        print("\n" + "=" * 60)
        print(f"PASS: {len(t.passes)}   FAIL: {len(t.fails)}")
        for n in t.fails:
            print("  UNRESOLVED:", n)
        print("=" * 60)

        save_report(t, c1, c2)
        if t.fails:
            dump_all(t, c1, c2, "final")
            sys.exit(1)
        sys.exit(0)


def save_report(t, c1, c2):
    report = {
        "passes": t.passes,
        "fails": t.fails,
        "timeline_u1": c1.timeline,
        "timeline_u2": c2.timeline,
        "console_u1": c1.console,
        "console_u2": c2.console,
        "errors_u1": c1.errors,
        "errors_u2": c2.errors,
        "ws_u1": c1.ws,
        "ws_u2": c2.ws,
    }
    (LOG_DIR / "report.json").write_text(json.dumps(report, indent=1, default=str))


def dump_all(t, c1, c2, tag):
    save_report(t, c1, c2)
    try:
        c1.page.screenshot(path=str(LOG_DIR / f"{tag}-u1.png"))
        c2.page.screenshot(path=str(LOG_DIR / f"{tag}-u2.png"))
    except Exception:
        pass
    tail = 12
    print(f"\n--- last {tail} telemetry samples ---")
    for c in (c1, c2):
        print(f"[{c.name}]")
        for d in c.timeline[-tail:]:
            print("  ", {k: (round(v, 2) if isinstance(v, float) else v) for k, v in d.items() if k != "wall"})
    for c in (c1, c2):
        for line in c.console[-8:]:
            print(f"[{c.name}] console {line}")
        for e in c.errors[-8:]:
            print(f"[{c.name}!] pageerror {e}")
    print(f"--- last WS frames ({tag}) ---")
    for c in (c1, c2):
        for f in c.ws[-8:]:
            print(f"[{c.name}] ws {f}")
    print(f"report: {LOG_DIR}/report.json")


def dump_logs(relay_p=None):
    for log in sorted(glob.glob(str(LOG_DIR / "*.log"))):
        try:
            out = Path(log).read_text(errors="replace")
            print(f"--- {Path(log).name} tail ---\n" + "\n".join(out.splitlines()[-25:]))
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    finally:
        for p in list(procs):
            try:
                p.send_signal(signal.SIGTERM)
            except Exception:
                pass
        time.sleep(0.5)
        for p in list(procs):
            try:
                p.kill()
            except Exception:
                pass