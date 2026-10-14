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
  assert.equal(output(c.P.vdec, 5, 0.04).closed, true);
  output(c.P.vdec, 9.99, 0.04);
  assert.equal(vm.runInContext('pendSeek', c), 10);
  output(c.P.adec, 9.99, 0.02);
  assert.equal(vm.runInContext('pendSeek', c), null);
  assert.equal(output(c.P.vdec, 6, 0.04).closed, true);
  assert.equal(c.P.seekFloor, 10);
});
