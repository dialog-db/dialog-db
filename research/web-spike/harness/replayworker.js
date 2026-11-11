// Round three's dedicated worker: replays the native archive's trace
// (replay.js) against one design, as a tab's own worker, or serves as the
// owner (design D) for a tab's worker in another page.

importScripts('common.js', 'store.js', 'replay.js');

let trace = null;
let replay = null;
let design = null;
let owner = null;
let ownerReplay = null;
let segments = null;
let stopReaders = false;
let setupCfg = {};
const peers = {};
let shared = null;

// Yields to the event loop without setTimeout's clamping, so a loop of
// synchronous replays still sees messages.
const yieldNow = () =>
  new Promise((r) => {
    const c = new MessageChannel();
    c.port1.onmessage = () => r();
    c.port2.postMessage(0);
  });

async function openHandles(cfg, mode) {
  const root = await navigator.storage.getDirectory();
  const handles = [];
  for (let f = 0; f < trace.files.length; f++) {
    const fh = await root.getFileHandle(`${cfg.space}-f${f}.bin`, { create: true });
    if (mode) handles.push(await fh.createSyncAccessHandle({ mode }));
    else handles.push((await openExclusive(`${cfg.space}-f${f}.bin`)).handle);
  }
  return handles;
}

const isCold = (op) => op.name && op.name.endsWith('/cold');

const handlers = {
  hello: () => ({ isolated: self.crossOriginIsolated }),
  attachShared: (args, e) => {
    shared = endpoint(e.ports[0], handlers);
    return true;
  },
  peer: ({ tag }, e) => {
    peers[tag] = endpoint(e.ports[0], handlers);
    return true;
  },
  async load({ url }) {
    const t = now();
    const buf = await (await fetch(url)).arrayBuffer();
    const fetched = now() - t;
    trace = parseTrace(buf);
    return { bytes: buf.byteLength, fetchMs: fetched, parseMs: now() - t - fetched, ...traceSummary(trace) };
  },
  async setup(cfg) {
    design = cfg.design;
    setupCfg = cfg;
    if (design === 'A') replay = await BlockReplay.open(cfg.space, trace);
    else if (design === 'B') {
      const slices = await IdbSlices.open(cfg.space, trace.files, { slice: cfg.slice, durability: 'relaxed' });
      replay = new ByteReplay(trace, { medium: 'idb', slices, slice: cfg.slice, cacheBytes: cfg.cacheBytes, locks: new Locks() });
    } else if (design === 'C') {
      const handles = await openHandles(cfg, 'readwrite-unsafe');
      replay = new ByteReplay(trace, { medium: 'opfs', handles, slice: cfg.slice, cacheBytes: cfg.cacheBytes, locks: new Locks() });
    } else if (design === 'D') {
      owner = peers.owner;
      if (!owner) throw new Error('no owner port');
      segments = trace.ops.map(ownerSegments);
    } else throw new Error(`no design ${design}`);
    return true;
  },
  // The owner's side of design D.
  async ownerSetup(cfg) {
    const handles = await openHandles(cfg, null);
    ownerReplay = new ByteReplay(trace, { medium: 'opfs', handles, slice: cfg.slice, cacheBytes: cfg.cacheBytes, locks: null });
    segments = trace.ops.map(ownerSegments);
    return true;
  },
  async seg({ op, seg, payload, readOnly }) {
    const s = segments[op][seg];
    await ownerReplay.run({ ev: s.ev }, !!readOnly);
    if (s.kind === 'G') {
      const size = trace.sizes.get(s.key) || 0;
      const out = new Uint8Array(size);
      return withTransfer(out, [out.buffer]);
    }
    return payload ? payload.byteLength : 0;
  },
  ownerDrop() {
    ownerReplay.dropCache();
    return true;
  },
  async runOp(i, readOnly) {
    const op = trace.ops[i];
    if (design === 'D') {
      // The owner's cache is shared by every tab: a new tab's cold read finds
      // it warm unless the owner itself is new (keepOwnerCache says which).
      if (isCold(op) && !setupCfg.keepOwnerCache) await owner('ownerDrop', {});
      const segs = segments[i];
      for (let j = 0; j < segs.length; j++) {
        const s = segs[j];
        if (readOnly && (s.kind === 'B' || s.kind === 'M')) continue;
        const payload = s.send ? new Uint8Array(s.send) : null;
        await owner('seg', { op: i, seg: j, payload, readOnly }, payload ? [payload.buffer] : []);
      }
      return;
    }
    if (isCold(op)) replay.dropCache();
    // A reader learns how far the file reaches before it reads, as the
    // native reader reads the slot.
    if (readOnly && replay.refreshEnd) await replay.refreshEnd();
    await replay.run(op, readOnly);
  },
  // Replays operations from..to, each timed; ops without a name are
  // replayed and not timed.
  async replay({ from = 0, to = trace.ops.length }) {
    const times = {};
    const t0 = now();
    for (let i = from; i < Math.min(to, trace.ops.length); i++) {
      const op = trace.ops[i];
      const t = now();
      await handlers.runOp(i, false);
      const ms = now() - t;
      if (op.name) (times[op.name] = times[op.name] || []).push(ms);
      if (i % 50 === 0) await yieldNow();
    }
    return { times, totalMs: now() - t0 };
  },
  firstOf({ names }) {
    return trace.ops.findIndex((o) => names.includes(o.name));
  },
  // A reader tab: replays the reads of the warm views and polls over and
  // over until stopped.
  async readers({ names }) {
    stopReaders = false;
    const which = [];
    trace.ops.forEach((o, i) => {
      if (names.includes(o.name)) which.push(i);
    });
    const times = {};
    let k = 0;
    while (!stopReaders) {
      const i = which[k++ % which.length];
      const t = now();
      await handlers.runOp(i, true);
      const ms = now() - t;
      (times[trace.ops[i].name] = times[trace.ops[i].name] || []).push(ms);
      await yieldNow();
    }
    return times;
  },
  stop() {
    stopReaders = true;
    return true;
  },
  async checksum() {
    const r = ownerReplay || replay;
    if (!r || !r.checksum) return null;
    const out = [];
    for (let f = 0; f < trace.files.length; f++) out.push(await r.checksum(f));
    return out;
  },
  ownerChecksum: () => handlers.checksum(),
  stats() {
    const r = design === 'D' ? null : replay;
    return r ? r.stats() : ownerReplay ? ownerReplay.stats() : null;
  },
  ownerStats: () => (ownerReplay ? ownerReplay.stats() : null),
  close() {
    for (const r of [replay, ownerReplay]) {
      if (!r) continue;
      if (r.locks) r.locks.releaseAll();
      if (r.handles) for (const h of r.handles) h.close();
      if (r.slices) r.slices.close();
      if (r.close) r.close();
    }
    replay = null;
    ownerReplay = null;
    return true;
  },
};
handlers.ownerStatsRemote = () => owner('ownerStats', {});
handlers.ownerChecksumRemote = () => owner('ownerChecksum', {});

endpoint(self, handlers);
