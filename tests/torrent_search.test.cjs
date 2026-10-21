const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function search(saved) {
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  const requests = [], storage = new Map();
  if (saved) storage.set('chadtorrent.torrentProvider', saved);
  const box = { children: [], set innerHTML(value) { this.children = []; }, appendChild(button) { this.children.push(button); } };
  const rep = { id: 1, tmdb_id: 10, title: "JoJo's Bizarre Adventure", media_type: 'tv' };
  const c = vm.createContext({
    AbortController, currentMovie: { rows: [rep] }, movieRep: rows => rows[0], isReading: () => false,
    searchResults: [], curRep: null, $: () => box,
    localStorage: { getItem: key => storage.get(key), setItem: (key, value) => storage.set(key, value) },
    document: { createElement: () => ({ dataset: {}, attributes: {}, setAttribute(key, value) { this.attributes[key] = value; } }) },
    api: (url, options) => new Promise((resolve, reject) => requests.push({ url, options, resolve, reject })),
    renderSearch: () => {},
  });
  vm.runInContext(html.slice(html.indexOf('const TORRENT_PROVIDERS'), html.indexOf('/* Torrent search results ordering')), c);
  c.renderSearch = () => c.renderTorrentProviders();
  return { c, requests, storage, rep, box };
}

test('provider buttons persist the selection and cancel stale searches', async () => {
  const { c, requests, storage, rep, box } = search();
  const old = c.findTorrents(rep);
  assert.match(requests[0].url, /provider=knaben/);
  assert.deepEqual(box.children.map(b => b.textContent), ['Knaben', 'ApiBay', 'Nyaa', 'YTS']);
  box.children.find(b => b.dataset.provider === 'nyaa').onclick();
  assert.equal(requests[0].options.signal.aborted, true);
  assert.match(requests[1].url, /provider=nyaa/);
  assert.equal(storage.get('chadtorrent.torrentProvider'), 'nyaa');
  requests[1].resolve([{ name: 'Nyaa result' }]);
  await new Promise(resolve => setImmediate(resolve));
  requests[0].resolve([{ name: 'Late Knaben result' }]);
  await old;
  assert.equal(c.searchResults[0].name, 'Nyaa result');
  assert.equal(box.children.filter(b => b.attributes['aria-pressed'] === 'true')[0].dataset.provider, 'nyaa');
});

test('provider failures keep the switcher usable and a new search clears old error/results', async () => {
  const { c, requests, rep, box } = search('apibay');
  const failing = c.findTorrents(rep);
  requests[0].reject(new Error('ApiBay search is unavailable (HTTP 521).'));
  await failing;
  assert.equal(c.findState, 'error');
  assert.match(c.findError, /ApiBay/);
  assert.equal(box.children.length, 4);
  c.selectTorrentProvider('knaben');
  assert.equal(c.findState, 'loading');
  assert.equal(c.findError, '');
  assert.equal(c.searchResults.length, 0);
  requests[1].resolve([]);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(c.findState, 'ready');
});

test('only known persisted providers are restored', () => {
  assert.equal(search('yts').c.torrentProvider, 'yts');
  assert.equal(search('unknown').c.torrentProvider, 'knaben');
});

test('name filtering is local, case-insensitive, and combines multiple terms without changing results', () => {
  const { c, requests } = search();
  const rows = [{ name: '[Judas] JoJo 1080p HEVC' }, { name: 'JoJo 720p HEVC' }, { name: 'JoJo 1080p AVC' }];
  const filtered = c.filterTorrentResults(rows, '  1080P   hevc ');
  assert.deepEqual(Array.from(filtered, row => row.name), ['[Judas] JoJo 1080p HEVC']);
  assert.equal(c.filterTorrentResults(rows, '[judas]').length, 1);
  assert.equal(c.filterTorrentResults(rows, 'missing').length, 0);
  assert.equal(c.filterTorrentResults(rows, '  ').length, 3);
  assert.equal(rows.length, 3);
  assert.equal(requests.length, 0);
});

test('adding a search result sends the exact release title used by the optimistic row', async () => {
  const { c, rep } = search();
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  vm.runInContext(html.slice(html.indexOf('async function addTorrentFromSearch'), html.indexOf('/* ---------- add modal')), c);
  c.curRep = rep; c.items = []; c.searchPending = new Map(); c.pendingRows = new Map();
  c.infoHash = () => '0123456789abcdef0123456789abcdef01234567';
  c.syncMovie = () => {}; c.refreshLibrary = () => {};
  let payload;
  c.api = async (_url, options) => { payload = JSON.parse(options.body); return { id: 42 }; };
  const name = "[Judas] JoJo's Bizarre Adventure - S06E04 [1080p][HEVC x265 10bit][Dual-Audio]";
  await c.addTorrentFromSearch({ name, magnet: 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567' });
  assert.equal(payload.title, name);
  assert.equal(c.items[0].title, name);
  assert.equal(payload.tmdb_id, rep.tmdb_id);
});

test('episode sources display the saved release title while retaining the file only for playback', () => {
  const { c } = search();
  const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
  vm.runInContext(html.slice(html.indexOf('function renderEpFileRow'), html.indexOf('/* Lowest-numbered playable episode')), c);
  c.esc = String; c.fmtBytes = n => `${n} B`;
  const release = "[Judas] JoJo's Bizarre Adventure - S06E04 [1080p][HEVC x265 10bit][Dual-Audio]";
  const t = { id: 1, title: release, state: 'ready', magnet: 'magnet:?xt=urn:btih:fake' };
  const f = { path: 'pack/short-file.mkv', have: 100, size: 100 };
  const row = { classList: { toggle() {} }, appendChild() {} };
  let played;
  c.playTorrent = (torrent, file) => played = { torrent, file };
  c.renderEpFileRow(row, t, f);
  assert.ok(row.innerHTML.includes(release));
  assert.ok(!row.innerHTML.includes('short-file.mkv'));
  assert.ok(!row.innerHTML.includes('magnet:?'));
  row.onclick({ target: { closest: () => false } });
  assert.equal(played.file, f.path);
  assert.equal(played.torrent, t);
});
