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
  c.VideoDecoder = class { constructor() { this.state = 'unconfigured'; } configure() { this.state = 'configured'; } };
  c.feedLoop = async () => {};
  c.setTimeout = fn => { queueMicrotask(fn); return 0; };
  c.P.id = 1;
  beforeStart();
  await c.startEngine();
}

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
  Object.assign(c.P, { id: 1, noAudio: true, eof: true, lastT: 100 });
  c.P.vdec = { flush: () => new Promise(r => release = r) };
  let ended = 0;
  c.v.addEventListener('ended', () => ended++);
  const draining = c.flushTail('V', 0);
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
test('partial final AAC packets trim decoder padding instead of triggering a false config failure', () => {
  const c = engine();
  c.AudioDecoder = class { constructor(callbacks) { this.callbacks = callbacks; } };
  c.P.adec = c.mkAdec();
  c.P.audioNominalUs = 21333;
  c.P.audioDurations.set(1e6, 13333);
  c.P.adec.callbacks.output({ timestamp: 1e6, duration: 21333, numberOfFrames: 1024, sampleRate: 48000 });
  assert.equal(c.P.aq.length, 1);
  assert.equal(c.P.audioLengths.get(1e6), 640);
});
test('the accepted startup decoder surfaces later decode errors', async () => {
  const c = engine();
  let failure;
  c.segFatal = e => failure = e.message;
  await startVideoOnly(c, () => {
    c.VideoDecoder = class {
      constructor(callbacks) { this.callbacks = callbacks; this.state = 'unconfigured'; }
      configure() { this.state = 'configured'; }
    };
  });
  c.P.vdec.callbacks.error(new Error('hardware decoder failed'));
  assert.match(failure, /hardware decoder failed/);
});
test('a superseded startup cannot install its codec into the new session', async () => {
  const c = engine();
  await startVideoOnly(c, () => {
    c.setTimeout = fn => {
      vm.runInContext('segGen++', c);
      c.P.id = 2;
      queueMicrotask(fn); return 0;
    };
    c.VideoDecoder = class {
      constructor() { this.state = 'unconfigured'; }
      configure() { this.state = 'configured'; }
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
