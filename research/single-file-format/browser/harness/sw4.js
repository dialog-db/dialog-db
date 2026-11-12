// Round four's service worker, in the place tonk's runs dialog today: it
// answers every `/api/op` fetch. In design A it is today's dialog instance:
// it replays the operation's block and cell events on IndexedDB itself. In
// design E it forwards the request to the dialog worker a tab hosts, over a
// port that tab gave it, and answers with the reply; while no tab hosts, it
// keeps the request and sends it to the next host, and a read-only request
// can be served from the file through `getFile()` (no host, below).

importScripts('common.js', 'store.js', 'replay.js');

self.addEventListener('install', (e) => e.waitUntil(self.skipWaiting()));
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

const started = now();
// This instance of the service worker: a stopped one starts again as another.
const instance = Math.random().toString(36).slice(2, 8);
const counts = { forwarded: 0, resent: 0, late: 0, answered: 0, noHost: 0, asked: 0 };
let host = null;
const pending = new Map();
let a = null;
let noHost = null;

async function loadTrace(url) {
  const t = now();
  const buf = await (await fetch(url)).arrayBuffer();
  return { trace: parseTrace(buf), ms: now() - t };
}

// Design A: today's dialog instance, started on the first request it gets,
// after a restart too.
function instanceA(space, url) {
  if (a && a.space === space) return a.ready;
  const me = { space };
  me.ready = (async () => {
    const loaded = await loadTrace(url);
    const t = now();
    const replay = await BlockReplay.open(space, loaded.trace);
    me.trace = loaded.trace;
    me.replay = replay;
    me.init = { traceMs: loaded.ms, openMs: now() - t, sinceStartMs: now() - started };
    return me;
  })();
  a = me;
  return me.ready;
}

async function opA(p) {
  const me = await instanceA(p.s, p.t);
  const t = now();
  await me.replay.run(me.trace.ops[p.i], p.ro);
  return { ms: now() - t, init: me.init, instance };
}

// The file read through `getFile()`, a fresh `File` for each operation (a
// `File` dies at the file's next write), behind the same page cache. The
// shape of `IdbSlices` that `ByteReplay` reads through.
class FileSlices {
  constructor(handles, slice) {
    this.handles = handles;
    this.slice = slice;
    this.fetches = 0;
    this.txs = 0;
    this.files = [];
  }
  async refresh() {
    this.files = await Promise.all(this.handles.map((h) => h.getFile()));
  }
  async fetch(f, first, last) {
    this.fetches++;
    const S = this.slice;
    const from = first * S;
    const to = Math.min((last + 1) * S, this.files[f].size);
    const bytes = new Uint8Array(await this.files[f].slice(from, to).arrayBuffer());
    const out = [];
    for (let n = first; n * S < from + bytes.length; n++) {
      const s = new Uint8Array(S);
      s.set(bytes.subarray((n - first) * S, Math.min((n - first + 1) * S, bytes.length)));
      out.push([n, s]);
    }
    return out;
  }
  reach(f) {
    return this.files[f].size;
  }
}

async function openNoHost(space, url, cacheBytes) {
  const loaded = await loadTrace(url);
  const root = await navigator.storage.getDirectory();
  const handles = [];
  for (let f = 0; f < loaded.trace.files.length; f++) handles.push(await root.getFileHandle(`${space}-f${f}.bin`));
  const slices = new FileSlices(handles, 4096);
  const replay = new ByteReplay(loaded.trace, { medium: 'idb', slices, slice: 4096, cacheBytes, locks: null });
  noHost = { trace: loaded.trace, slices, replay };
  return { traceMs: loaded.ms };
}

async function opNoHost(p) {
  const t = now();
  await noHost.slices.refresh();
  if (p.c) noHost.replay.dropCache();
  await noHost.replay.refreshEnd();
  await noHost.replay.run(noHost.trace.ops[p.i], true);
  counts.noHost++;
  return { ms: now() - t, noHost: true, instance };
}

// Asks the windows for the host's port again: after this service worker
// started, it has none.
// Asked once, and again only if no port came within half a second, so
// the host does not announce itself (and get every pending request) twice.
let askedAt = -Infinity;
function askForHost() {
  if (now() - askedAt < 500) return;
  askedAt = now();
  counts.asked++;
  self.clients.matchAll({ type: 'window' }).then((all) => {
    for (const c of all) c.postMessage({ type: 'needHost' });
  });
}

function setHost(port, epoch) {
  host = { port, epoch };
  port.onmessage = (event) => {
    const r = event.data || {};
    const q = pending.get(r.re);
    if (!q) {
      counts.late++;
      return;
    }
    pending.delete(r.re);
    counts.answered++;
    q.resolve({ epoch: r.epoch, hostMs: r.ms, resent: q.sent > 1, instance });
  };
  for (const q of pending.values()) {
    q.sent++;
    counts.resent++;
    port.postMessage(q.msg);
  }
}

function opE(p) {
  if (!host && p.ro && noHost) return opNoHost(p);
  return new Promise((resolve) => {
    const msg = { id: p.id, i: p.i, ro: p.ro, cold: p.c };
    const q = { msg, resolve, sent: 0 };
    pending.set(p.id, q);
    if (host) {
      q.sent++;
      counts.forwarded++;
      host.port.postMessage(msg);
    } else askForHost();
  });
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (url.pathname !== '/api/op') return;
  const q = url.searchParams;
  const p = { m: q.get('m'), i: Number(q.get('i')), ro: q.get('ro') === '1', c: q.get('c') === '1', id: q.get('id'), s: q.get('s'), t: q.get('t') };
  const answer = (p.m === 'A' ? opA(p) : opE(p)).then(
    (v) => new Response(JSON.stringify(v), { headers: { 'content-type': 'application/json' } }),
    (e) => new Response(JSON.stringify({ error: errorName(e) }), { status: 500, headers: { 'content-type': 'application/json' } }),
  );
  event.respondWith(answer);
});

// Messages from windows: each carries a port to answer on first, then any
// port it hands over.
self.addEventListener('message', (event) => {
  const d = event.data || {};
  const reply = event.ports[0];
  const done = (v) => reply && reply.postMessage(v);
  const fail = (e) => reply && reply.postMessage({ error: errorName(e) });
  switch (d.type) {
    case 'host':
      setHost(event.ports[1], d.epoch);
      done({ ok: true, pending: pending.size });
      break;
    case 'nohost':
      if (host && host.epoch === d.epoch) host = null;
      done({ ok: true });
      break;
    case 'connect':
      if (!host) return fail(new Error('no host'));
      host.port.postMessage({ attach: true }, [event.ports[1]]);
      done({ ok: true });
      break;
    case 'initA':
      instanceA(d.space, d.traceUrl).then((me) => done({ ok: true, init: me.init, instance }), fail);
      break;
    case 'openNoHost':
      openNoHost(d.space, d.traceUrl, d.cacheBytes).then(done, fail);
      break;
    case 'stats':
      done({ instance, counts, pending: pending.size, host: host ? host.epoch : null, startedMsAgo: now() - started, noHost: noHost ? noHost.replay.stats() : null });
      break;
    default:
      fail(new Error(`no message ${d.type}`));
  }
});
