// Round three: a trace of the native file archive (see
// rust/dialog-chronicle/src/trace.rs) replayed against each design.
// Loaded with importScripts after common.js and store.js (for base58,
// sum32, idbOpen, idbRequest, idbDone, openExclusive).
//
// The trace is parsed into operations, each its name, how long it took
// natively, and its events in order:
//   ['R', f, off, len]      a read of the file: a run of adjacent mapping
//                           pages first read one after another, or a
//                           positioned read
//   ['W', f, off, bytes]    a write
//   ['L', f, len]           the file's length set
//   ['S', f]                a sync
//   ['K', f, lock, exclusive, wait, taken]   a lock taken (or tried)
//   ['U', f, lock]          a lock let go
//   ['G', f, key, found]    a block read by its hash (a Get, a node read)
//   ['B', f, key, bytes]    a block written
//   ['C', f, cell]          a cell resolved
//   ['M', f, cell, bytes]   a cell published
// R to K are the file's pattern (designs B, C, D); G to M are the same
// operations as blocks and cells (design A, today's store).

const TRACE_PAGE = 4096;
const LOCK_BASE = 2 ** 62;

function lockName(f, byte) {
  const d = byte - LOCK_BASE;
  let name;
  if (d === 0) name = 'turn';
  else if (d === -1) name = 'present';
  else if (d === -2) name = 'recovery';
  else if (d === -3) name = 'sync';
  else if (d > 0) name = `owner:${d - 1}`;
  else name = `byte:${byte}`;
  return `f${f}/${name}`;
}

function parseTrace(buffer) {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
  const magic = String.fromCharCode(...bytes.subarray(0, 8));
  if (magic !== 'DTRACE01') throw new Error(`not a trace: ${magic}`);
  const dec = new TextDecoder();
  let at = 8;
  const u8 = () => bytes[at++];
  const u16 = () => {
    const v = view.getUint16(at, true);
    at += 2;
    return v;
  };
  const u32 = () => {
    const v = view.getUint32(at, true);
    at += 4;
    return v;
  };
  const u64 = () => {
    const v = Number(view.getBigUint64(at, true));
    at += 8;
    return v;
  };
  const text = () => {
    const n = u16();
    const s = dec.decode(bytes.subarray(at, at + n));
    at += n;
    return s;
  };
  const hash = () => {
    const h = bytes.subarray(at, at + 32);
    at += 32;
    return base58(h);
  };
  const files = [];
  const ops = [];
  let op = { name: null, ns: 0, ev: [] };
  let lastPage = null;
  let pages = 0;
  const push = (e) => {
    op.ev.push(e);
    lastPage = null;
  };
  while (at < bytes.length) {
    const tag = u8();
    switch (tag) {
      case 1: {
        if (op.ev.length) ops.push(op);
        op = { name: text(), ns: 0, ev: [] };
        lastPage = null;
        break;
      }
      case 2: {
        op.ns = u64();
        ops.push(op);
        op = { name: null, ns: 0, ev: [] };
        lastPage = null;
        break;
      }
      case 3: {
        const f = u8();
        const page = u64();
        pages++;
        if (lastPage && lastPage[1] === f && lastPage.next === page) {
          lastPage[3] += TRACE_PAGE;
          lastPage.next = page + 1;
        } else {
          const e = ['R', f, page * TRACE_PAGE, TRACE_PAGE];
          e.next = page + 1;
          op.ev.push(e);
          lastPage = e;
        }
        break;
      }
      case 4: {
        const f = u8();
        const off = u64();
        push(['R', f, off, u32()]);
        break;
      }
      case 5: {
        const f = u8();
        const off = u64();
        const n = u32();
        push(['W', f, off, bytes.subarray(at, at + n)]);
        at += n;
        break;
      }
      case 6: {
        const f = u8();
        push(['L', f, u64()]);
        break;
      }
      case 7:
        push(['S', u8()]);
        break;
      case 8: {
        const f = u8();
        const byte = u64();
        const flags = u8();
        push(['K', f, lockName(f, byte), !!(flags & 1), !!(flags & 2), !!(flags & 4)]);
        break;
      }
      case 9: {
        const f = u8();
        push(['U', f, lockName(f, u64())]);
        break;
      }
      case 10: {
        const f = u8();
        const key = hash();
        push(['G', f, key, !!u8()]);
        break;
      }
      case 11: {
        const f = u8();
        const key = hash();
        const n = u32();
        push(['B', f, key, bytes.subarray(at, at + n)]);
        at += n;
        break;
      }
      case 12: {
        const f = u8();
        push(['C', f, text()]);
        break;
      }
      case 13: {
        const f = u8();
        const cell = text();
        const n = u32();
        push(['M', f, cell, bytes.subarray(at, at + n)]);
        at += n;
        break;
      }
      case 14: {
        const f = u8();
        files[f] = text();
        break;
      }
      default:
        throw new Error(`unknown tag ${tag} at ${at - 1}`);
    }
  }
  if (op.ev.length) ops.push(op);
  // Block sizes by key, for the owner's answers.
  const sizes = new Map();
  for (const o of ops) for (const e of o.ev) if (e[0] === 'B') sizes.set(e[2], e[3].length);
  return { files, ops, pages, sizes };
}

// What a trace holds, by operation name.
function traceSummary(trace) {
  const by = {};
  for (const o of trace.ops) {
    const k = o.name || '(none)';
    const s = (by[k] = by[k] || { n: 0, reads: 0, readBytes: 0, writes: 0, writeBytes: 0, syncs: 0, locks: 0, gets: 0, puts: 0, putBytes: 0, cells: 0, nativeMs: [] });
    s.n++;
    if (o.name) s.nativeMs.push(o.ns / 1e6);
    for (const e of o.ev) {
      if (e[0] === 'R') {
        s.reads++;
        s.readBytes += e[3];
      } else if (e[0] === 'W') {
        s.writes++;
        s.writeBytes += e[3].length;
      } else if (e[0] === 'S') s.syncs++;
      else if (e[0] === 'K') s.locks++;
      else if (e[0] === 'G') s.gets++;
      else if (e[0] === 'B') {
        s.puts++;
        s.putBytes += e[3].length;
      } else if (e[0] === 'C' || e[0] === 'M') s.cells++;
    }
  }
  for (const s of Object.values(by)) s.nativeMs = summarize(s.nativeMs);
  return { files: trace.files, ops: trace.ops.length, pages: trace.pages, by };
}

// A page cache of the file in the worker that holds its bytes: slices of
// `size` bytes, least recently used dropped past `capacity` bytes.
class SliceCache {
  constructor(size, capacity) {
    this.size = size;
    this.max = Math.max(1, Math.floor(capacity / size));
    this.map = new Map();
    this.hits = 0;
    this.misses = 0;
  }
  key(f, n) {
    return f * 2 ** 40 + n;
  }
  get(f, n) {
    const k = this.key(f, n);
    const s = this.map.get(k);
    if (s) {
      this.map.delete(k);
      this.map.set(k, s);
    }
    return s;
  }
  set(f, n, slice, pinned) {
    const k = this.key(f, n);
    this.map.delete(k);
    this.map.set(k, slice);
    if (this.map.size > this.max) {
      for (const old of this.map.keys()) {
        if (this.map.size <= this.max) break;
        if (!pinned(old)) this.map.delete(old);
      }
    }
  }
  drop(f, n) {
    this.map.delete(this.key(f, n));
  }
  clear(pinned) {
    for (const k of [...this.map.keys()]) if (!pinned(k)) this.map.delete(k);
  }
}

// Web Locks held in the shape of the native byte locks. Native locks are the
// process's and taking one held again converts it; a Web Lock is not
// re-entrant, so taking one held lets the old one go first.
class Locks {
  constructor() {
    this.held = new Map();
    this.waited = 0;
    this.count = 0;
  }
  async take(name, exclusive, wait) {
    const old = this.held.get(name);
    if (old) {
      old.release();
      this.held.delete(name);
    }
    const t = now();
    const got = await hold(name, { mode: exclusive ? 'exclusive' : 'shared', ifAvailable: !wait });
    this.waited += now() - t;
    this.count++;
    if (got) this.held.set(name, got);
    return !!got;
  }
  let(name) {
    const h = this.held.get(name);
    if (h) {
      h.release();
      this.held.delete(name);
    }
  }
  releaseAll() {
    for (const h of this.held.values()) h.release();
    this.held.clear();
  }
}

// The file's bytes in one IndexedDB database, an object store per traced
// file (`f<n>`), a record per slice keyed by the slice's start offset (a
// number key: IndexedDB orders numbers numerically, so a range of the file
// is one getAll over a key range).
class IdbSlices {
  static async open(name, files, { slice, durability }) {
    const m = new IdbSlices();
    m.db = await idbOpen(`${name}.slices`, files.map((_, f) => `f${f}`));
    m.slice = slice;
    m.durability = durability;
    m.fetches = 0;
    m.txs = 0;
    return m;
  }
  // The slices first..last that the store holds, as [slice number, bytes]
  // pairs: a file grown by setting its length has slices never written,
  // which getAll skips, so the keys come with them.
  fetch(f, first, last) {
    this.fetches++;
    const store = this.db.transaction([`f${f}`], 'readonly').objectStore(`f${f}`);
    const range = IDBKeyRange.bound(first * this.slice, last * this.slice);
    const keys = idbRequest(store.getAllKeys(range));
    const values = idbRequest(store.getAll(range));
    return Promise.all([keys, values]).then(([k, v]) => k.map((key, i) => [key / this.slice, v[i]]));
  }
  // The end of the last slice file f holds.
  reach(f) {
    const store = this.db.transaction([`f${f}`], 'readonly').objectStore(`f${f}`);
    return idbRequest(store.openKeyCursor(null, 'prev')).then((c) => (c ? c.key + this.slice : 0));
  }
  // Puts every dirty slice in one transaction across the files' stores.
  commit(dirty, cuts, strict) {
    const names = new Set();
    for (const [f] of dirty) names.add(`f${f}`);
    for (const [f] of cuts) names.add(`f${f}`);
    if (!names.size) return null;
    this.txs++;
    const durability = strict ? 'strict' : this.durability;
    const tx = this.db.transaction([...names], 'readwrite', { durability });
    // Requests in a transaction run in order: the cut first, then the
    // slices written after it.
    for (const [f, from] of cuts) tx.objectStore(`f${f}`).delete(IDBKeyRange.lowerBound(from * this.slice));
    for (const [f, slices] of dirty) {
      const store = tx.objectStore(`f${f}`);
      for (const [n, s] of slices) store.put(s, n * this.slice);
    }
    return idbDone(tx);
  }
  close() {
    this.db.close();
  }
}

// The byte-level replayer: designs B (IdbSlices, reads awaited) and C (an
// OPFS sync access handle per file), each behind the same page cache.
class ByteReplay {
  // medium: 'idb' or 'opfs'; handles: OPFS handles by file, for 'opfs'.
  constructor(trace, { medium, slices, handles, slice, cacheBytes, locks }) {
    this.trace = trace;
    this.medium = medium;
    this.slices = slices;
    this.handles = handles;
    this.S = slice;
    this.cache = new SliceCache(slice, cacheBytes);
    this.cacheBytes = cacheBytes;
    this.locks = locks;
    this.end = trace.files.map(() => 0);
    this.dirty = new Map();
    this.cuts = new Map();
    this.strict = false;
    this.mediumReads = 0;
    this.mediumBytes = 0;
  }
  pinned(k) {
    const f = Math.floor(k / 2 ** 40);
    const n = k - f * 2 ** 40;
    const d = this.dirty.get(f);
    return !!(d && d.has(n));
  }
  dropCache() {
    this.cache.clear((k) => this.pinned(k));
  }
  // How far each file reaches as the medium holds it, for a reader that
  // did not write it.
  async refreshEnd() {
    for (let f = 0; f < this.end.length; f++) {
      if (this.medium === 'opfs') this.end[f] = this.handles[f].getSize();
      else this.end[f] = await this.slices.reach(f);
    }
  }
  // Slices first..last of file f, into the cache; a promise for 'idb'.
  load(f, first, last) {
    const want = [];
    for (let n = first; n <= last; n++) {
      if (n * this.S >= this.end[f]) break;
      if (!this.cache.get(f, n)) want.push(n);
    }
    if (!want.length) {
      this.cache.hits++;
      return null;
    }
    this.cache.misses++;
    const lo = want[0];
    const hi = want[want.length - 1];
    this.mediumReads++;
    if (this.medium === 'opfs') {
      const len = (hi - lo + 1) * this.S;
      const buf = new Uint8Array(len);
      this.handles[f].read(buf, { at: lo * this.S });
      this.mediumBytes += len;
      for (let n = lo; n <= hi; n++) {
        if (!this.cache.get(f, n)) this.cache.set(f, n, buf.slice((n - lo) * this.S, (n - lo + 1) * this.S), (k) => this.pinned(k));
      }
      return null;
    }
    return this.slices.fetch(f, lo, hi).then((got) => {
      const have = new Set();
      for (const [n, slice] of got) {
        have.add(n);
        this.mediumBytes += slice.length;
        if (!this.cache.get(f, n)) this.cache.set(f, n, slice, (k) => this.pinned(k));
      }
      // A slice never written reads as zeros, as a hole in a file does.
      for (const n of want) if (!have.has(n) && !this.cache.get(f, n)) this.cache.set(f, n, new Uint8Array(this.S), (k) => this.pinned(k));
    });
  }
  async read(f, off, len) {
    const first = Math.floor(off / this.S);
    const last = Math.floor((off + len - 1) / this.S);
    const p = this.load(f, first, last);
    if (p) await p;
  }
  async write(f, off, bytes) {
    if (this.medium === 'opfs') {
      this.handles[f].write(bytes, { at: off });
    }
    let at = 0;
    while (at < bytes.length) {
      const n = Math.floor((off + at) / this.S);
      const inSlice = (off + at) % this.S;
      const k = Math.min(this.S - inSlice, bytes.length - at);
      let s = this.cache.get(f, n);
      if (!s && this.medium === 'idb') {
        // A partial write over a slice not cached reads it first.
        if (n * this.S < this.end[f] && (inSlice !== 0 || k !== this.S)) {
          await this.load(f, n, n);
          s = this.cache.get(f, n);
        }
        if (!s) s = new Uint8Array(this.S);
      }
      if (s) {
        s.set(bytes.subarray(at, at + k), inSlice);
        if (this.medium === 'idb') {
          let d = this.dirty.get(f);
          if (!d) this.dirty.set(f, (d = new Map()));
          d.set(n, s);
        }
        this.cache.set(f, n, s, (key) => this.pinned(key));
      }
      at += k;
    }
    this.end[f] = Math.max(this.end[f], off + bytes.length);
  }
  async setLen(f, len) {
    if (this.medium === 'opfs') this.handles[f].truncate(len);
    if (len < this.end[f]) {
      const from = Math.ceil(len / this.S);
      for (let n = from; n * this.S < this.end[f]; n++) {
        this.cache.drop(f, n);
        const d = this.dirty.get(f);
        if (d) d.delete(n);
      }
      // What the cut leaves of its last slice reads as zeros past the end
      // if the file grows again, as a file's would.
      const n = Math.floor(len / this.S);
      if (len % this.S) {
        if (this.medium === 'idb' && !this.cache.get(f, n)) await this.load(f, n, n);
        const s = this.cache.get(f, n);
        if (s) {
          s.fill(0, len % this.S);
          if (this.medium === 'idb') {
            let d = this.dirty.get(f);
            if (!d) this.dirty.set(f, (d = new Map()));
            d.set(n, s);
          }
        }
      }
      if (this.medium === 'idb') {
        const cut = this.cuts.get(f);
        this.cuts.set(f, cut === undefined ? from : Math.min(cut, from));
      }
    }
    this.end[f] = len;
  }
  sync(f) {
    if (this.medium === 'opfs') this.handles[f].flush();
    else this.strict = true;
  }
  async finishOp() {
    if (this.medium !== 'idb') return;
    const done = this.slices.commit(this.dirty, this.cuts, this.strict);
    this.dirty = new Map();
    this.cuts = new Map();
    this.strict = false;
    if (done) await done;
  }
  async run(op, readOnly = false) {
    for (const e of op.ev) {
      switch (e[0]) {
        case 'R':
          await this.read(e[1], e[2], e[3]);
          break;
        case 'W':
          if (!readOnly) await this.write(e[1], e[2], e[3]);
          break;
        case 'L':
          if (!readOnly) await this.setLen(e[1], e[2]);
          break;
        case 'S':
          if (!readOnly) this.sync(e[1]);
          break;
        case 'K':
          if (!readOnly && this.locks) await this.locks.take(e[2], e[3], e[4]);
          break;
        case 'U':
          if (!readOnly && this.locks) this.locks.let(e[2]);
          break;
        default:
      }
    }
    if (!readOnly) await this.finishOp();
  }
  // The file's bytes as the medium holds them, summed, to check against the
  // native file.
  async checksum(f) {
    const len = this.end[f];
    const all = new Uint8Array(len);
    if (this.medium === 'opfs') this.handles[f].read(all, { at: 0 });
    else {
      const last = Math.floor((len - 1) / this.S);
      for (let lo = 0; lo <= last; lo += 256) {
        const got = await this.slices.fetch(f, lo, Math.min(last, lo + 255));
        for (const [n, s] of got) all.set(s.subarray(0, Math.min(this.S, len - n * this.S)), n * this.S);
      }
    }
    return { len, sum: sum32(all) };
  }
  stats() {
    return {
      medium: this.medium,
      slice: this.S,
      cacheBytes: this.cacheBytes,
      hits: this.cache.hits,
      misses: this.cache.misses,
      mediumReads: this.mediumReads,
      mediumBytes: this.mediumBytes,
      fetches: this.slices ? this.slices.fetches : 0,
      txs: this.slices ? this.slices.txs : 0,
      locks: this.locks ? this.locks.count : 0,
      lockMs: this.locks ? this.locks.waited : 0,
    };
  }
}

// Design A: today's store, one database per traced file (dialog keeps one
// per space): blocks by base58 hash in `archive/index`, gets awaited one
// by one, each run of block writes one import transaction awaiting each
// put, cells in `memory` resolved and published (get, check, put).
class BlockReplay {
  static async open(name, trace) {
    const r = new BlockReplay();
    r.dbs = [];
    for (let f = 0; f < trace.files.length; f++) r.dbs.push(await idbOpen(`${name}.blocks.${f}`, ['archive/index', 'memory']));
    r.gets = 0;
    r.puts = 0;
    r.txs = 0;
    return r;
  }
  get(f, key) {
    this.gets++;
    const store = this.dbs[f].transaction(['archive/index'], 'readonly').objectStore('archive/index');
    return idbRequest(store.get(key));
  }
  async import(f, blocks) {
    this.txs++;
    const tx = this.dbs[f].transaction(['archive/index'], 'readwrite');
    const done = idbDone(tx);
    const store = tx.objectStore('archive/index');
    for (const [key, bytes] of blocks) {
      this.puts++;
      await idbRequest(store.put(bytes.slice(), key));
    }
    await done;
  }
  resolve(f, cell) {
    const store = this.dbs[f].transaction(['memory'], 'readonly').objectStore('memory');
    return idbRequest(store.get(cell));
  }
  async publish(f, cell, bytes) {
    this.txs++;
    const tx = this.dbs[f].transaction(['memory'], 'readwrite');
    const done = idbDone(tx);
    const store = tx.objectStore('memory');
    await idbRequest(store.get(cell));
    await idbRequest(store.put(bytes.slice(), cell));
    await done;
  }
  async run(op, readOnly = false) {
    const ev = op.ev;
    for (let i = 0; i < ev.length; i++) {
      const e = ev[i];
      if (e[0] === 'G') await this.get(e[1], e[2]);
      else if (e[0] === 'C') await this.resolve(e[1], e[2]);
      else if (readOnly) continue;
      else if (e[0] === 'B') {
        const f = e[1];
        const blocks = [[e[2], e[3]]];
        while (i + 1 < ev.length && (ev[i + 1][0] === 'B' || (ev[i + 1][0] !== 'G' && ev[i + 1][0] !== 'C' && ev[i + 1][0] !== 'M')) && (ev[i + 1][0] !== 'B' || ev[i + 1][1] === f)) {
          i++;
          if (ev[i][0] === 'B') blocks.push([ev[i][2], ev[i][3]]);
        }
        await this.import(f, blocks);
      } else if (e[0] === 'M') await this.publish(e[1], e[2], e[3]);
    }
  }
  dropCache() {}
  stats() {
    return { gets: this.gets, puts: this.puts, txs: this.txs };
  }
  close() {
    for (const db of this.dbs) db.close();
  }
}

// Design D: the owner. The tab sends one message per block-level operation
// (a block read, a run of block writes, a cell resolved or published) and
// the owner, which holds the file exclusively behind its own page cache,
// replays the file's events from that one to the next. A block read is
// answered with as many bytes as the block holds; a block write carries
// its bytes.
function ownerSegments(op) {
  const segs = [];
  let cur = { kind: 'lead', send: 0, answer: 0, ev: [] };
  const isBlock = (e) => e[0] === 'G' || e[0] === 'B' || e[0] === 'C' || e[0] === 'M';
  for (const e of op.ev) {
    if (isBlock(e)) {
      if (e[0] === 'B' && cur.kind === 'B') {
        cur.send += e[3].length;
        continue;
      }
      segs.push(cur);
      cur = { kind: e[0], key: e[2], send: e[0] === 'B' || e[0] === 'M' ? e[3].length : 0, answer: 0, ev: [] };
    } else cur.ev.push(e);
  }
  segs.push(cur);
  // A segment with no block operation before it (lead) has no message of
  // its own: its events go with the first message.
  if (segs.length > 1 && segs[0].kind === 'lead') {
    segs[1].ev = segs[0].ev.concat(segs[1].ev);
    segs.shift();
  }
  // An operation that touched no storage sends nothing.
  if (segs.length === 1 && segs[0].kind === 'lead' && !segs[0].ev.length) return [];
  return segs;
}
