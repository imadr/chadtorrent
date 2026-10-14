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
async function startVideoOnly(c) {
  c.api = async () => ({ done: true, mime: 'video/mp4; codecs="avc1.64001E"', duration: 90, segments: [{ n: 1, t: 0 }] });
  c.items = []; c.segFileQ = () => '';
  c.fetchSegBuf = async () => new ArrayBuffer(0);
  c.parseInit = () => new Map([[1, { kind: 'video', w: 64, h: 64, codec4cc: 'avc1' }]]);
  c.ensureCtx = () => c.P.ctx = { currentTime: 0, state: 'running' };
  c.VideoDecoder = class { constructor() { this.state = 'unconfigured'; } configure() { this.state = 'configured'; } };
  c.feedLoop = async () => {};
  c.setTimeout = fn => { queueMicrotask(fn); return 0; };
  c.P.id = 1;
  await c.startEngine();
}

test('single-fragment tails use tfhd timing without neighboring fragments', () => {
  const c = engine();
  const samples = c.parseSeg(fragment({ tfhdFlags: 0x38, defaults: [40, 4, 0x10000] }), tracks()).get(1);
  assert.deepEqual(Array.from(samples, s => s.dts), [100, 140, 180]);
  assert.equal(samples[1].key, false);
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
