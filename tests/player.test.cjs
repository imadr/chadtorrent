// Exercise the shipped inline engine rather than a copy of its implementation.
// Run: node --test tests/player.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function engine() {
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  const timers = [];
  const c = vm.createContext({
    console, Uint8Array, DataView, Map, Event, EventTarget, AbortController,
    performance, setTimeout, clearTimeout,
    URLSearchParams,
    setInterval: fn => timers.push(fn), requestAnimationFrame: () => 1,
    window: {}, v: new EventTarget(), fullDur: 100,
    $: () => ({ getContext: () => ({ drawImage() {} }) }),
  });
  vm.runInContext(html.slice(html.indexOf('const SEG_AHEAD'), html.indexOf('/* Persist the playback position')), c);
  c.P = vm.runInContext('P', c);
  c.timers = timers;
  return c;
}
function u32(...values) {
  const b = Buffer.alloc(values.length * 4);
  values.forEach((v, i) => b.writeUInt32BE(v >>> 0, i * 4));
  return b;
}
function box(type, ...parts) {
  const data = Buffer.concat(parts);
  return Buffer.concat([u32(data.length + 8), Buffer.from(type), data]);
}
function arrayBuffer(b) { return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); }

function wakeEngine(request) {
  const c = engine();
  c.document = new EventTarget(); c.document.visibilityState = 'visible';
  c.navigator = { wakeLock: { request } };
  Object.assign(c.P, { id: 1, paused: false });
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  vm.runInContext(html.slice(html.indexOf("for (const event of ['play', 'playing'"), html.indexOf('const ppic =')), c);
  return c;
}
function wakeSentinel() {
  const lock = new EventTarget();
  lock.released = false;
  lock.release = async () => { lock.released = true; lock.dispatchEvent(new Event('release')); };
  return lock;
}

test('screen wake lock follows playback, visibility, EOF and stop without duplicate requests', async () => {
  const locks = [];
  const c = wakeEngine(async type => {
    assert.equal(type, 'screen');
    const lock = wakeSentinel(); locks.push(lock); return lock;
  });
  c.v.dispatchEvent(new Event('play'));
  c.v.dispatchEvent(new Event('playing'));
  c.v.dispatchEvent(new Event('canplay'));
  await c.syncScreenWakeLock();
  assert.equal(locks.length, 1);
  assert.equal(locks[0].released, false);

  c.P.paused = true; c.v.dispatchEvent(new Event('pause'));
  assert.equal(locks[0].released, true);
  c.P.paused = false; c.v.dispatchEvent(new Event('play'));
  await c.syncScreenWakeLock();
  assert.equal(locks.length, 2);
  c.document.visibilityState = 'hidden'; c.document.dispatchEvent(new Event('visibilitychange'));
  assert.equal(locks[1].released, true);
  c.document.visibilityState = 'visible'; c.document.dispatchEvent(new Event('visibilitychange'));
  await c.syncScreenWakeLock();
  assert.equal(locks.length, 3);

  c.P.endedFired = true; c.v.dispatchEvent(new Event('ended'));
  assert.equal(locks[2].released, true);
  c.P.endedFired = false; c.v.dispatchEvent(new Event('seeked'));
  await c.syncScreenWakeLock();
  assert.equal(locks.length, 4);
  c.P.id = null; await c.syncScreenWakeLock();
  assert.equal(locks[3].released, true);
});

test('a wake lock requested before pause or close is released when acquisition finishes', async () => {
  for (const stop of [c => { c.P.paused = true; }, c => { c.P.id = null; }]) {
    let resolve;
    const c = wakeEngine(() => new Promise(r => { resolve = r; }));
    const pending = c.syncScreenWakeLock();
    stop(c); await c.syncScreenWakeLock();
    const lock = wakeSentinel(); resolve(lock); await pending;
    assert.equal(lock.released, true);
    assert.equal(vm.runInContext('screenWakeLock', c), null);
  }
});

test('browser-released wake locks are reacquired on return to the visible tab', async () => {
  const locks = [];
  const c = wakeEngine(async () => { const lock = wakeSentinel(); locks.push(lock); return lock; });
  await c.syncScreenWakeLock();
  await locks[0].release();
  assert.equal(vm.runInContext('screenWakeLock', c), null);
  c.document.visibilityState = 'hidden'; c.document.dispatchEvent(new Event('visibilitychange'));
  c.document.visibilityState = 'visible'; c.document.dispatchEvent(new Event('visibilitychange'));
  await c.syncScreenWakeLock();
  assert.equal(locks.length, 2);
  assert.equal(locks[1].released, false);
});

test('wake-lock denial and unsupported browsers leave playback alone', async () => {
  let requests = 0;
  const c = wakeEngine(async () => { requests++; throw new Error('denied'); });
  await c.syncScreenWakeLock();
  assert.equal(requests, 1);
  assert.equal(c.P.err, null);
  assert.equal(c.P.paused, false);
  assert.equal(vm.runInContext('screenWakePending', c), false);
  c.navigator = {};
  await c.syncScreenWakeLock();
  assert.equal(requests, 1);
});

function fragment({ defaults = [], tfhdFlags = 0, runFlags = 0, rows = [], count = 3 } = {}) {
  const tfhd = box('tfhd', u32(0x020000 | tfhdFlags, 1, ...defaults));
  const tfdt = box('tfdt', u32(0, 100));
  const make = offset => box('moof', box('traf', tfhd, tfdt, box('trun', u32(1 | runFlags, count, offset, ...rows))));
  const moof = make(0);
  return arrayBuffer(Buffer.concat([make(moof.length + 8), box('mdat', Buffer.alloc(count * 4))]));
}
function tracks(defaults = {}) {
  return new Map([[1, { kind: 'video', timescale: 1000, trex: { dur: 0, size: 0, flags: 0, ...defaults } }]]);
}
async function startVideoOnly(c, beforeStart = () => {}) {
  c.api = async () => ({ done: true, mime: 'video/mp4; codecs="avc1.64001E"', timing: { video_offset: 0, audio_offset: null, audio_skip_until: 0 }, duration: 90, segments: [{ n: 1, t: 0 }] });
  c.items = []; c.segFileQ = () => '';
  c.fetchSegBuf = async () => new ArrayBuffer(0);
  c.parseInit = () => new Map([[1, { kind: 'video', w: 64, h: 64, codec4cc: 'avc1' }]]);
  c.ensureCtx = () => c.P.ctx = { currentTime: 0, state: 'running' };
  c.VideoDecoder = class { constructor() { this.state = 'unconfigured'; } configure() { this.state = 'configured'; } async flush() {} };
  c.feedLoop = async () => {};
  c.setTimeout = fn => { queueMicrotask(fn); return 0; };
  c.P.id = 1;
  beforeStart();
  await c.startEngine();
}

test('audio startup rejects configurations that accept configure but cannot decode source packets', async () => {
  const c = engine();
  let closedOutputs = 0;
  c.api = async () => ({ done: true, generation: 'one', mime: 'video/mp4; codecs="avc1.64001E,mp4a.40.5"',
    timing: { video_offset: 0, audio_offset: 0, audio_skip_until: 0 }, segments: [{ n: 1, t: 0 }] });
  c.segFileQ = () => ''; c.fetchSegBuf = async () => new ArrayBuffer(0);
  c.parseInit = () => new Map([
    [1, { kind: 'video', codec4cc: 'avc1' }],
    [2, { kind: 'audio', rate: 48000, ch: 2, desc: new Uint8Array([0x13, 0x10]) }],
  ]);
  c.ensureCtx = () => c.P.ctx = { currentTime: 0, state: 'running' };
  c.VideoDecoder = class {
    constructor() { this.state = 'unconfigured'; }
    configure() { this.state = 'configured'; }
    async flush() {}
  };
  c.AudioDecoder = class {
    constructor(callbacks) { this.callbacks = callbacks; this.state = 'unconfigured'; this.inputs = 0; }
    configure(config) { this.config = config; this.state = 'configured'; }
    decode() {
      if (++this.inputs > 16 && this.config.description) {
        this.state = 'closed'; this.callbacks.error(new Error('The given encoding is not supported.'));
        throw new Error('decoder closed');
      }
      this.callbacks.output({ timestamp: 0, duration: 42667, numberOfFrames: 1024,
        sampleRate: 24000, numberOfChannels: 2, close() { closedOutputs++; } });
    }
    async flush() {}
    close() { this.state = 'closed'; }
  };
  c.audioProbeChunks = async () => Array.from({ length: 32 }, () => ({}));
  c.feedLoop = async () => {};
  c.P.id = 1;
  await c.startEngine();
  assert.equal(c.P.acfgIdx, 2);
  assert.equal(c.P.recoveryAttempts, 0);
  assert.equal(c.P.aq.length, 0, 'probe PCM is discarded, never played twice');
  assert.equal(closedOutputs, 64);
  c.P.adec.decode({});
  assert.equal(c.P.aq.length, 1, 'accepted decoder delivers real playback output');
});

test('audio probing finishes when a completed stream has no audio packets', async () => {
  const c = engine();
  Object.assign(c.P, { id: 1, generation: 'same', segMaxN: 1 });
  c.fetchSegBuf = async () => new ArrayBuffer(0);
  c.parseSeg = () => new Map(); c.segFileQ = () => '';
  c.api = async () => ({ generation: 'same', done: true, segments: [{ n: 1, t: 0 }] });
  const chunks = await c.audioProbeChunks(0, 1);
  assert.equal(chunks.length, 0);
});

test('HEVC NAL parsing identifies RASL pictures behind in-band parameter sets', () => {
  const c = engine();
  const nal = (type, n) => Buffer.concat([Buffer.alloc(n - 1), Buffer.from([2, type * 2, 1])]);
  for (const n of [1, 2, 4]) {
    assert.equal(c.hevcPictureType(Buffer.concat([nal(32, n), nal(33, n), nal(21, n)]), n), 21);
    assert.equal(c.hevcPictureType(nal(8, n), n), 8);
    assert.equal(c.hevcPictureType(nal(9, n), n), 9);
  }
  assert.equal(c.hevcPictureType(Buffer.from([0, 0, 0, 20, 16, 1]), 4), null);
});
test('HEVC seeks discard only leading RASL pictures whose references precede the access point', () => {
  const c = engine();
  const V = [
    { ts: 1000000, key: true },
    { ts: 800000, rasl: true }, { ts: 900000, rasl: true },
    { ts: 1100000 }, { ts: 2000000, key: true },
    { ts: 1900000, rasl: true },
  ];
  const selected = c.seekChunks({ V, A: [] }, 1.2);
  assert.deepEqual(Array.from(selected.V, v => v.ts), [1000000, 1100000, 2000000, 1900000]);
  // Ordinary sequential decoding must retain leading pictures of later GOPs.
  const data = Buffer.from([0, 0, 0, 2, 16, 1]);
  const desc = new Uint8Array(22); desc[21] = 3;
  c.P.tracks = new Map([[1, { kind: 'video', timescale: 1000, codec4cc: 'hev1', desc }]]);
  const parsed = new Map([[1, [{ pts: 800, dts: 1000, dur: 40, off: 0, size: 6, key: false }]]]);
  assert.equal(c.segToChunks(arrayBuffer(data), parsed).V[0].rasl, true);
});

test('single-fragment tails use tfhd timing without neighboring fragments', () => {
  const c = engine();
  const samples = c.parseSeg(fragment({ tfhdFlags: 0x38, defaults: [40, 4, 0x10000] }), tracks()).get(1);
  assert.deepEqual(Array.from(samples, s => s.dts), [100, 140, 180]);
  assert.equal(samples[1].key, false);
});
test('initialization reads trex inside mvex rather than directly under moov', () => {
  const c = engine();
  const tkhd = Buffer.alloc(84); tkhd.writeUInt32BE(1, 12);
  const mdhd = Buffer.alloc(24); mdhd.writeUInt32BE(1000, 12);
  const hdlr = Buffer.alloc(24); hdlr.write('vide', 8);
  const entry = Buffer.alloc(86); entry.writeUInt32BE(86, 0); entry.write('avc1', 4);
  const init = box('moov', box('mvex', box('trex', u32(0, 1, 1, 40, 4, 0x10000))),
    box('trak', box('tkhd', tkhd), box('mdia', box('mdhd', mdhd), box('hdlr', hdlr),
      box('minf', box('stbl', box('stsd', u32(0, 1), entry))))));
  const parsed = c.parseInit(arrayBuffer(init)).get(1);
  assert.equal(parsed.trex.dur, 40);
  assert.equal(parsed.trex.size, 4);
  assert.equal(parsed.trex.flags, 0x10000);
});
test('trun explicit zero flags override tfhd defaults and durations can vary', () => {
  const c = engine();
  const samples = c.parseSeg(fragment({ tfhdFlags: 0x38, defaults: [40, 4, 0x10000], runFlags: 0x500, rows: [30, 0, 50, 0x10000, 20, 0x10000] }), tracks()).get(1);
  assert.deepEqual(Array.from(samples, s => s.dts), [100, 130, 180]);
  assert.equal(samples[0].key, true);
});
test('trex defaults are inherited and missing durations fail instead of inventing timestamps', () => {
  const c = engine();
  const samples = c.parseSeg(fragment(), tracks({ dur: 40, size: 4 })).get(1);
  assert.equal(samples[2].dts, 180);
  assert.throws(() => c.parseSeg(fragment(), tracks({ size: 4 })), /duration/);
});

test('fetched EOF cannot complete a seek before target decoder output', () => {
  const c = engine();
  Object.assign(c.P, { id: 1, eof: true, lastT: 71.3, seekFloor: 71.3, noAudio: true });
  vm.runInContext('pendSeek = 71.3', c);
  c.finishSeek();
  assert.equal(vm.runInContext('pendSeek', c), 71.3);
});
test('seek completion keeps rejecting in-flight preroll and waits for both tracks', () => {
  const c = engine();
  c.VideoDecoder = class { constructor(callbacks) { this.callbacks = callbacks; } };
  c.AudioDecoder = c.VideoDecoder;
  Object.assign(c.P, { id: 1, seekFloor: 10 });
  vm.runInContext('pendSeek = 10', c);
  c.P.vdec = c.mkVdec(); c.P.adec = c.mkAdec();
  const output = (decoder, ts, dur) => {
    const f = { timestamp: ts * 1e6, duration: dur * 1e6, close() { this.closed = true; } };
    decoder.callbacks.output(f); return f;
  };
  const preroll = output(c.P.vdec, 5, 0.04);
  output(c.P.vdec, 6, 0.04);
  assert.equal(preroll.closed, true);
  output(c.P.vdec, 9.99, 0.04);
  assert.equal(vm.runInContext('pendSeek', c), 10);
  output(c.P.adec, 9.99, 0.02);
  assert.equal(vm.runInContext('pendSeek', c), null);
  assert.equal(output(c.P.vdec, 6, 0.04).closed, true);
  assert.equal(c.P.seekFloor, 10);
});
test('video-only startup selects a wall clock without EOF and clears previous audio config', async () => {
  const c = engine();
  c.P.acfgs = [{ codec: 'mp4a.40.2' }];
  await startVideoOnly(c);
  assert.equal(c.P.noAudio, true);
  assert.equal(c.P.acfgs, null);
  c.P.ctx.currentTime = 5;
  assert.equal(c.mediaNow(), 0); // early codec checks must not skip unavailable video
  c.queueVideoFrame(c.P.vdec, { timestamp: 0, duration: 40000 });
  c.P.ctx.currentTime = 10;
  assert.equal(c.mediaNow(), 5);
  assert.equal(c.P.eof, false);
});
test('explicit SBR ASC carries both object types and the original core configuration', () => {
  const c = engine();
  const result = c.ascExplicitSbr(new Uint8Array([0x13, 0x90])); // LC, 22050Hz, stereo
  const bits = Array.from(result.asc, b => b.toString(2).padStart(8, '0')).join('');
  assert.equal(parseInt(bits.slice(0, 5), 2), 5);
  assert.equal(parseInt(bits.slice(5, 9), 2), 7);
  assert.equal(parseInt(bits.slice(9, 13), 2), 2);
  assert.equal(parseInt(bits.slice(13, 17), 2), 4);
  assert.equal(parseInt(bits.slice(17, 22), 2), 2);
  assert.equal(result.outRate, 44100);
  assert.equal(c.ascExplicitSbr(result.asc), null);
});
test('EOF waits for asynchronous decoder flush output before completing playback', async () => {
  const c = engine();
  let release;
  Object.assign(c.P, { id: 1, noAudio: true, eof: true, lastT: 100, paused: false });
  c.P.vdec = { flush: () => new Promise(r => release = r) };
  let ended = 0;
  c.v.addEventListener('ended', () => ended++);
  const draining = c.flushTail('V', 0);
  await Promise.resolve(); // flush control message runs after its owner is registered
  c.timers[0]();
  assert.equal(ended, 0);
  c.P.vq.push({ timestamp: 99e6, close() {} });
  c.P.videoEnd = 100;
  release(); await draining;
  c.timers[0]();
  assert.equal(ended, 0);
  c.P.vq.length = 0;
  c.timers[0]();
  assert.equal(ended, 1);
});
test('audio EOF is determined by flush completion, not decodeQueueSize', async () => {
  const c = engine();
  c.P.adec = { decodeQueueSize: 0, flush: async () => {} };
  c.P.eof = true;
  assert.equal(c.audioEnded(), false);
  await c.flushTail('A', 0);
  assert.equal(c.audioEnded(), true);
});
test('seek starts at the nearest preceding sync sample with bounded audio preroll', () => {
  const c = engine();
  const V = [0, 1, 2, 3, 4, 5].map(t => ({ ts: t * 1e6, key: t % 2 === 0 }));
  const A = [2.7, 2.9, 3, 3.1].map(t => ({ ts: t * 1e6, durUs: 20000 }));
  const selected = c.seekChunks({ V, A }, 3.1);
  assert.equal(selected.V[0].ts, 2e6);
  assert.equal(selected.A[0].ts, 2.9e6);
});
test('decoder progress wakes feeding without waiting for a polling timer', async () => {
  const c = engine();
  const waiting = c.waitFeed(10000);
  c.wakeFeed();
  await waiting;
  assert.equal(vm.runInContext('feedWaiters.size', c), 0);
});
test('manifest errors after playback starts are surfaced, not retried forever', async () => {
  const c = engine();
  c.P.id = 1; c.segFileQ = () => '';
  c.api = async () => ({ error: 'demux failed', segments: [] });
  await assert.rejects(c.refreshManifest(0, 1), /demux failed/);
  c.api = async () => ({ done: true, segments: [] });
  await assert.rejects(c.refreshManifest(0, 1), /no playable segments/);
  c.api = async () => { throw new Error('server unreachable'); };
  await assert.rejects(c.refreshManifest(0, 1), /server unreachable/);
});
test('a new segment generation rebuilds playback instead of retaining stale manifest entries', async () => {
  const c = engine();
  c.P.id = 1; c.P.generation = 'old'; c.segFileQ = () => '';
  c.api = async () => ({ generation: 'new', segments: [] });
  let rebuilt = false;
  c.reloadStream = async () => { rebuilt = true; };
  await c.refreshManifest(0, 1);
  assert.equal(rebuilt, true);
});

test('a far seek relocates when the growing manifest finally indexes the target', async () => {
  const c = engine();
  let selected = null;
  Object.assign(c.P, { id: 1, generation: 'same', fetchN: 2, segs: new Map([[1, 0], [2, 10]]) });
  vm.runInContext('pendSeek = 35', c);
  c.segFileQ = () => '';
  c.api = async () => ({ generation: 'same', done: true,
    segments: [{ n: 1, t: 0 }, { n: 2, t: 10 }, { n: 3, t: 30 }, { n: 4, t: 40 }] });
  c.seekTo = (target, origin, options) => { selected = { target, options }; };
  await c.refreshManifest(0, 1);
  assert.equal(selected.target, 35);
  assert.equal(selected.options.recovery, true);
  assert.equal(c.segContaining(35), 3);
});

test('audio trim removes real PCM samples at a seek boundary', () => {
  const c = engine();
  let copied, closed = false;
  c.P.ctx = { createBuffer: (chs, n, rate) => ({ length: n, duration: n / rate, copyToChannel() {} }) };
  const ad = { numberOfFrames: 1024, numberOfChannels: 1, sampleRate: 48000,
    copyTo: (_dest, options) => copied = options, close: () => closed = true };
  const buf = c.audioBufferOf(ad, 512);
  assert.equal(buf.length, 512);
  assert.equal(copied.frameOffset, 512);
  assert.equal(copied.frameCount, 512);
  assert.equal(closed, true);
});
test('audio scheduling preserves timestamp gaps and uses actual PCM duration', () => {
  const c = engine();
  const starts = [];
  c.P.ctx = { currentTime: 0, state: 'running', createBufferSource: () => ({ connect() {}, start: t => starts.push(t) }) };
  c.P.paused = false; c.P.adec = {}; c.P.gain = {};
  c.audioBufferOf = () => ({ duration: 0.02 });
  c.P.aq = [0, 0.1].map(t => ({ timestamp: t * 1e6, numberOfFrames: 960, sampleRate: 48000 }));
  c.pumpAudio();
  assert.ok(Math.abs(starts[1] - starts[0] - 0.1) < 1e-9);
  assert.ok(Math.abs(c.P.anchor.media - 0.12) < 1e-9);
});
test('the audio clock accounts for device output timing instead of scheduled-ahead context time', () => {
  const c = engine();
  c.P.ctx = { currentTime: 1, state: 'running', outputLatency: 0.08 };
  c.P.anchor = { ctx: 2, media: 2 };
  assert.ok(Math.abs(c.mediaNow() - 0.92) < 1e-9);
});
test('PCM padding is trimmed at the known audio track end, not individual packet durations', () => {
  const c = engine();
  const ad = { timestamp: 1e6, numberOfFrames: 1024, sampleRate: 48000 };
  c.P.audioEnd = 1 + 640 / 48000;
  assert.equal(c.audioFrameLimit(ad), 1024);
  c.P.eof = true;
  assert.equal(c.audioFrameLimit(ad), 640);
});
test('the accepted startup decoder routes later errors to recovery', async () => {
  const c = engine();
  let failure;
  c.recoverDecoder = (which, e) => failure = { which, message: e.message };
  await startVideoOnly(c, () => {
    c.VideoDecoder = class {
      constructor(callbacks) { this.callbacks = callbacks; this.state = 'unconfigured'; }
      configure() { this.state = 'configured'; }
      async flush() {}
    };
  });
  c.P.vdec.callbacks.error(new Error('hardware decoder failed'));
  assert.equal(failure.which, 'video');
  assert.match(failure.message, /hardware decoder failed/);
});
test('a superseded startup cannot install its codec into the new session', async () => {
  const c = engine();
  await startVideoOnly(c, () => {
    c.VideoDecoder = class {
      constructor() { this.state = 'unconfigured'; }
      configure() { this.state = 'configured'; }
      async flush() { vm.runInContext('segGen++', c); c.P.id = 2; }
      close() { this.state = 'closed'; }
    };
  });
  assert.equal(c.P.id, 2);
  assert.equal(c.P.vcfg, undefined);
});
test('asynchronous configuration failures advance to the next decoder candidate', async () => {
  const c = engine();
  let attempts = 0;
  await startVideoOnly(c, () => {
    c.VideoDecoder = class {
      constructor(callbacks) { this.callbacks = callbacks; this.state = 'unconfigured'; }
      configure() {
        this.state = 'configured';
        if (++attempts === 1) queueMicrotask(() => this.callbacks.error(new Error('unsupported hardware config')));
      }
      async flush() {}
      close() { this.state = 'closed'; }
    };
  });
  assert.equal(attempts, 2);
  assert.equal(c.P.vdec.state, 'configured');
});
test('missing known segments refresh the manifest and recover after a server restart', async () => {
  const c = engine();
  Object.assign(c.P, { id: 1, generation: 'old', done: true, segs: new Map([[1, 0]]) });
  c.segFileQ = () => '';
  let fetches = 0, rebuilds = 0;
  c.fetchSegBuf = async () => { if (++fetches > 3) throw new Error('repeated 404 without recovery'); return null; };
  c.api = async () => ({ generation: 'new', segments: [{ n: 1, t: 0 }] });
  c.reloadStream = async () => { rebuilds++; vm.runInContext('segGen++', c); };
  await c.feedLoop(0, 1);
  assert.equal(rebuilds, 1);
  assert.equal(fetches, 1);
});
test('a paused seek to EOF does not trigger the autoplay loop', () => {
  const c = engine();
  Object.assign(c.P, { id: 1, noAudio: true, vDrained: true, aDrained: true, videoEnd: 100, lastT: 100, paused: true });
  let ended = 0;
  c.v.addEventListener('ended', () => ended++);
  c.timers[0]();
  assert.equal(ended, 0);
  assert.equal(c.P.paused, true);
});
test('unsupported HEVC candidates fail without scheduling per-candidate timers', async () => {
  const c = engine();
  let attempts = 0, timers = 0, flushes = 0;
  await assert.rejects(startVideoOnly(c, () => {
    c.api = async () => ({ mime: 'video/mp4; codecs="hvc1.2.4.L120.B0"', done: false, segments: [] });
    c.parseInit = () => new Map([[1, { kind: 'video', w: 64, h: 64, codec4cc: 'hev1', desc: new Uint8Array([1]) }]]);
    c.setTimeout = fn => { timers++; queueMicrotask(fn); return 0; };
    c.VideoDecoder = class {
      constructor() { this.state = 'unconfigured'; }
      configure() { attempts++; throw new Error('unsupported codec'); }
      flush() { flushes++; throw new Error('must not flush rejected config'); }
      close() { this.state = 'closed'; }
    };
  }), /no WebCodecs decoder/);
  assert.equal(attempts, 8);
  assert.equal(flushes, 0);
  assert.equal(timers, 0);
});
test('codec checks do not wait for ready-file segmentation to finish', async () => {
  const c = engine();
  let requests = 0;
  await startVideoOnly(c, () => {
    c.items = [{ id: 1, state: 'ready' }];
    c.api = async () => {
      if (++requests > 1) throw new Error('unnecessary manifest poll');
      return { mime: 'video/mp4; codecs="avc1.64001E"', done: false, segments: [] };
    };
  });
  assert.equal(c.P.vdec.state, 'configured');
  assert.equal(c.P.done, false);
  assert.equal(requests, 1);
});

test('the reported 42667us PCM / 44833us packet difference is not a codec failure', async () => {
  const c = engine();
  const ts = 114046167;
  let failures = 0, closed = false;
  c.onAudioDecErr = () => failures++;
  c.AudioDecoder = class {
    constructor(callbacks) { this.callbacks = callbacks; this.state = 'configured'; this.decodeQueueSize = 0; }
    decode(chunk) {
      this.callbacks.output({ timestamp: chunk.timestamp, duration: 42667, numberOfFrames: 2048,
        sampleRate: 48000, close() { closed = true; } });
    }
    async flush() {}
  };
  Object.assign(c.P, { id: 1, done: true, acfgIdx: 0, adec: c.mkAdec(),
    chunksA: [{ timestamp: ts, duration: 44833 }], audioEnd: (ts + 44833) / 1e6 });
  await c.feedLoop(0, 1);
  assert.equal(failures, 0);
  assert.equal(c.P.acfgIdx, 0);
  assert.equal(c.P.aq.length, 1);
  assert.equal(c.audioFrameLimit(c.P.aq[0]), 2048);
  assert.equal(closed, false);
});

function recoveryEngine(paused = false) {
  const c = engine(), jobs = new Map();
  let next = 0, videoConfigs = 0;
  c.console = { ...console, warn() {} };
  c.setTimeout = (fn, delay) => { const id = ++next; jobs.set(id, { fn, delay }); return id; };
  c.clearTimeout = id => jobs.delete(id);
  c.showBuf = () => {};
  c.showBufProgress = () => c.showBuf();
  c.P.ctx = { currentTime: 10, state: paused ? 'suspended' : 'running',
    createGain: () => ({ gain: {}, connect() {}, disconnect() {} }) };
  Object.assign(c.P, { id: 1, paused, lastT: 18.25, vcfg: { codec: 'avc1.64001E', hardwareAcceleration: 'prefer-hardware' },
    acfgs: [{ codec: 'mp4a.40.2' }, { codec: 'mp4a.40.2' }], acfgIdx: 0 });
  c.VideoDecoder = class {
    constructor(callbacks) { this.callbacks = callbacks; this.state = 'unconfigured'; }
    configure() { this.state = 'configured'; videoConfigs++; }
    close() { this.state = 'closed'; }
  };
  c.AudioDecoder = c.VideoDecoder;
  c.feedLoop = async () => {};
  function run() {
    const [id, job] = jobs.entries().next().value;
    jobs.delete(id); job.fn(); return job;
  }
  return { c, jobs, run, configurations: () => videoConfigs };
}

test('decoder recovery retains the session, target, paused state and audio context', () => {
  const { c, jobs, run } = recoveryEngine(true);
  const ctx = c.P.ctx;
  vm.runInContext('pendSeek = 35.5', c);
  c.recoverDecoder('video', new Error('The given encoding is not supported.'));
  assert.equal(c.P.id, 1);
  assert.equal(c.P.ctx, ctx);
  assert.equal(c.P.paused, true);
  assert.equal(c.P.lastT, 35.5);
  assert.equal(c.P.recoveryAttempts, 1);
  assert.equal(jobs.values().next().value.delay, 100);
  run();
  assert.equal(c.P.vdec.state, 'configured');
  assert.equal(c.P.adec.state, 'configured');
});

test('rapid seeks coalesce and supersede queued decoder recoveries', () => {
  const { c, jobs, run, configurations } = recoveryEngine();
  c.recoverDecoder('video', new Error('temporary failure'));
  const staleJob = jobs.values().next().value.fn;
  for (const t of [10, 20, 30, 40]) c.seekTo(t);
  assert.equal(jobs.size, 1);
  assert.equal(configurations(), 0);
  staleJob();
  assert.equal(configurations(), 0);
  run();
  assert.equal(configurations(), 2); // one video and one audio decoder
  assert.equal(c.P.lastT, 40);
  assert.equal(c.P.recoveryAttempts, 0);
});

test('persistent decoder failure stops after bounded retries and normal play can resume', () => {
  const { c, jobs, run } = recoveryEngine();
  const ctx = c.P.ctx;
  for (let i = 0; i < 3; i++) { c.recoverDecoder('video', new Error('persistent failure')); run(); }
  c.recoverDecoder('video', new Error('persistent failure'));
  assert.equal(jobs.size, 0);
  assert.ok(c.P.err);
  assert.equal(c.P.id, 1);
  assert.equal(c.P.ctx, ctx);
  c.playerPlay();
  assert.equal(c.P.err, null);
  assert.equal(c.P.recoveryAttempts, 0);
  run();
  assert.equal(c.P.vdec.state, 'configured');
});

test('stale decoder error callbacks cannot restart a newer seek', () => {
  const { c, run } = recoveryEngine();
  c.seekTo(10); run();
  const stale = c.P.vdec;
  c.seekTo(40); run();
  stale.callbacks.error(new Error('late error'));
  assert.equal(c.P.lastT, 40);
  assert.equal(c.P.recoveryAttempts, 0);
});
test('old decoder failures do not consume the retry budget for the whole session', () => {
  const { c, run } = recoveryEngine();
  let now = 0;
  c.performance = { now: () => now };
  for (let i = 0; i < 3; i++) { c.recoverDecoder('video', new Error('temporary failure')); run(); }
  now = 31000;
  c.recoverDecoder('video', new Error('later unrelated failure'));
  assert.equal(c.P.recoveryAttempts, 1);
  assert.equal(c.P.err, null);
  run();
  assert.equal(c.P.vdec.state, 'configured');
});
test('output latency holds the resumed clock at the seek floor instead of moving backwards', () => {
  const c = engine();
  Object.assign(c.P, { seekFloor: 20, lastT: 20, anchor: { ctx: 1.5, media: 20.5 },
    ctx: { state: 'running', currentTime: 1, outputLatency: 0.2 } });
  assert.equal(c.mediaNow(), 20);
  c.P.ctx.currentTime = 1.4;
  assert.ok(Math.abs(c.mediaNow() - 20.2) < 1e-9);
});
test('one decoded audio buffer can span multiple input packets', async () => {
  const c = engine();
  let inputs = 0, failures = 0;
  c.onAudioDecErr = () => failures++;
  c.AudioDecoder = class {
    constructor(callbacks) { this.callbacks = callbacks; this.state = 'configured'; this.decodeQueueSize = 0; }
    decode() {
      if (++inputs === 2) this.callbacks.output({ timestamp: 0, duration: 42667,
        sampleRate: 48000, numberOfFrames: 2048, close() {} });
    }
    async flush() {}
  };
  Object.assign(c.P, { id: 1, done: true, adec: c.mkAdec(), audioEnd: 0.042667,
    chunksA: [{ timestamp: 0, duration: 21333 }, { timestamp: 21333, duration: 21334 }] });
  await c.feedLoop(0, 1);
  assert.equal(failures, 0);
  assert.equal(c.P.aq.length, 1);
  assert.equal(c.audioFrameLimit(c.P.aq[0]), 2048);
});
test('synchronous flush errors recover without tearing down the active session', async () => {
  const { c, jobs } = recoveryEngine();
  c.P.vdec = { flush() { throw new Error('flush failed'); }, close() {} };
  const gen = vm.runInContext('segGen', c);
  await c.flushTail('V', gen).catch(e => c.handleFeedError(e, gen, 1));
  assert.equal(c.P.id, 1);
  assert.equal(c.P.recoveryAttempts, 1);
  assert.equal(jobs.size, 1);
});
test('a flush scheduled by a cancelled seek cannot flush or drain the new decoder', async () => {
  const { c, run } = recoveryEngine();
  let flushed = 0;
  c.P.vdec = { flush: async () => flushed++, close() {} };
  const pending = c.flushTail('V', vm.runInContext('segGen', c));
  c.seekTo(40); run();
  await pending;
  assert.equal(flushed, 0);
  assert.equal(c.P.vDrained, false);
  assert.equal(c.P.flushV, null);
});
test('stream errors after startup preserve the player and normal seeking can resume', () => {
  const { c, jobs } = recoveryEngine(true);
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  vm.runInContext(html.slice(html.indexOf('function segFatal'), html.indexOf('function teardownWC')), c);
  const ctx = c.P.ctx;
  c.segFatal(new Error('temporary server failure'));
  assert.equal(c.P.id, 1);
  assert.equal(c.P.ctx, ctx);
  assert.equal(c.P.paused, true);
  c.seekTo(20);
  assert.equal(c.P.err, null);
  assert.equal(jobs.size, 1);
});
test('a flush callback that starts recovery cannot install stale flush state into the new generation', async () => {
  const { c } = recoveryEngine();
  c.P.vdec = { close() {}, flush() {
    c.recoverDecoder('video', new Error('native flush error'));
    return Promise.resolve();
  } };
  await c.flushTail('V', vm.runInContext('segGen', c));
  assert.equal(c.P.flushV, null);
  assert.equal(c.P.vDrained, false);
  assert.equal(c.P.recoveryAttempts, 1);
});
test('closing playback cancels pending recovery and stale scheduled work cannot recreate decoders', () => {
  const { c, jobs, configurations } = recoveryEngine();
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  vm.runInContext(html.slice(html.indexOf('function teardownWC'), html.indexOf('// Debug hook')), c);
  c.subRenderStop = () => {};
  c.P.ctx.close = async () => {};
  c.$ = () => ({ firstElementChild: { textContent: '' }, getContext: () => ({ clearRect() {} }) });
  c.recoverDecoder('video', new Error('temporary failure'));
  const stale = jobs.values().next().value.fn;
  c.teardownWC();
  assert.equal(jobs.size, 0);
  stale();
  assert.equal(configurations(), 0);
  assert.equal(c.P.id, null);
});
