// The stores round two compares, behind one interface, and the workload's
// blocks. Loaded with importScripts by the host (a dedicated worker or the
// shared worker) and by the owner worker, after common.js.
//
// Every store answers get(key), getMany(keys), import(blocks),
// resolve(cell), publish(cell, content, expected) and walk(root, depth). A
// method returns its value, or a promise of it when the store has to wait:
// the OPFS archive never waits, so a walk over it is one synchronous loop.
//
// - IdbArchive: dialog's IndexedDB store as it is today (see
//   rust/dialog-storage/src/storage/provider/indexeddb*.rs).
// - LogArchive over OpfsFile: blocks appended to one OPFS file held by a
//   sync access handle, two rolling slots naming the log's end and the
//   memory cells, an index in memory.
// - LogArchive over PagedIdbFile: the same file, its bytes kept as
//   fixed-size page records in IndexedDB with a page cache in memory, in
//   the style of absurd-sql and wa-sqlite's IDBBatchAtomicVFS.
// - RemoteArchive: a client of an owner worker that holds the OPFS file,
//   over a MessagePort, which retries what it had in flight when it is
//   pointed at a new owner.

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

// Base58 of bytes, the alphabet dialog keys IndexedDB blocks with.
function base58(bytes) {
  const digits = [0];
  for (const byte of bytes) {
    let carry = byte;
    for (let i = 0; i < digits.length; i++) {
      carry += digits[i] << 8;
      digits[i] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }
  let out = '';
  for (const byte of bytes) {
    if (byte) break;
    out += '1';
  }
  for (let i = digits.length - 1; i >= 0; i--) out += B58[digits[i]];
  return out;
}

// Fills bytes from a xorshift generator seeded by `seed`: incompressible,
// and reproducible, so a block's expected bytes can be rebuilt to check it.
function fill(bytes, seed) {
  const words = new Uint32Array(bytes.buffer, bytes.byteOffset, bytes.length >> 2);
  let x = seed | 1;
  let y = 0x9e3779b9 ^ seed;
  for (let i = 0; i < words.length; i++) {
    const t = x ^ (x << 11);
    x = y;
    y = y ^ (y >>> 19) ^ (t ^ (t >>> 8));
    words[i] = y;
  }
  for (let i = words.length << 2; i < bytes.length; i++) bytes[i] = (seed >>> (i & 24)) & 0xff;
}

// FNV-1a over 32-bit words: the records' checksum. A table CRC-32 in
// JavaScript runs at about 300 MiB/s here, this at 2.5 GiB/s, which is
// nearer what Wasm with a hardware CRC would pay.
function sum32(bytes, start = 0, end = bytes.length) {
  let h = 0x811c9dc5 | 0;
  let i = start;
  if (((bytes.byteOffset + start) & 3) === 0) {
    const n = (end - start) >> 2;
    const words = new Uint32Array(bytes.buffer, bytes.byteOffset + start, n);
    for (let j = 0; j < n; j++) h = Math.imul(h ^ words[j], 0x01000193);
    i = start + (n << 2);
  }
  for (; i < end; i++) h = Math.imul(h ^ bytes[i], 0x01000193);
  return h >>> 0;
}

function strSum(s) {
  let h = 0x811c9dc5 | 0;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 0x01000193);
  return h >>> 0;
}

// A cell's version, from its content, as dialog versions a cell by the
// hash of what it holds.
const version = (content) => `${strSum(content).toString(16)}.${content.length}`;

// A block's first LINK bytes name the block it points at (the key in ASCII,
// padded with spaces), so a chain of blocks is a chain of dependent reads,
// as a search tree's nodes are.
const LINK = 48;

function writeLink(bytes, next) {
  for (let i = 0; i < LINK; i++) bytes[i] = next && i < next.length ? next.charCodeAt(i) : 32;
}

function readLink(bytes) {
  let s = '';
  for (let i = 0; i < LINK; i++) {
    const c = bytes[i];
    if (c === 32) break;
    s += String.fromCharCode(c);
  }
  return s || null;
}

function keyOf(seed) {
  const b = new Uint8Array(32);
  fill(b, Math.imul(seed, 0x5bd1e995) ^ 0x2545f491);
  return base58(b);
}

// A block of `size` bytes from `seed`, pointing at `next` (or nothing).
// Content addressed in spirit: the key is a function of the seed, and the
// bytes are a function of seed, size and link, and the workload never
// reuses a seed.
function makeBlock(seed, size, next = null) {
  const bytes = new Uint8Array(size);
  fill(bytes, seed);
  writeLink(bytes, next);
  return { key: keyOf(seed), bytes };
}

// A seeded generator for everything that decides what is written (sizes,
// how many blocks a commit has), so every store is given the same bytes.
function rngFrom(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Most blocks 1 to 8 KiB, one in ten 64 KiB: the shape of tonk's blocks
// (tree nodes, and the odd large value).
function blockSize(rand = Math.random) {
  if (rand() < 0.1) return 65536;
  return 1024 + 4 * Math.floor(rand() * ((7 * 1024) / 4));
}

// What a compact format would store for a block of `raw` bytes: `ratio` of
// it, for every block, so the stored total is `ratio` of the raw bytes. An
// emulation: the payload is incompressible bytes of that length with the
// block's link kept readable, so the stores are given the sizes a format
// would hand them, not the format's CPU. (The native file keeps blocks that
// are not tree nodes whole; which of tonk's blocks are large values is not
// known here, so this does not try to tell them apart.)
function storedSize(raw, ratio = 1) {
  if (ratio >= 1) return raw;
  return Math.max(LINK + 16, 4 * Math.round((raw * ratio) / 4));
}

function mismatch(message) {
  const e = new Error(`VersionMismatch: ${message}`);
  e.name = 'VersionMismatch';
  return e;
}

// Dialog's compare-and-swap (indexeddb/memory.rs): a cell already holding
// what is published succeeds without writing, which is what makes a retried
// publish idempotent; otherwise `expected` must name what it holds, or be
// null for an empty cell. Returns true when the cell must be written.
function casCheck(current, content, expected) {
  if (current !== undefined && current !== null && current === content) return false;
  if (expected === null || expected === undefined) {
    if (current !== undefined && current !== null) throw mismatch('cell is not empty');
    return true;
  }
  if (current === undefined || current === null) throw mismatch('cell is empty');
  if (version(current) !== expected) throw mismatch(`expected ${expected}, holds ${version(current)}`);
  return true;
}

const andThen = (x, fn) => (x && typeof x.then === 'function' ? x.then(fn) : fn(x));

function idbRequest(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function idbDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onabort = () => reject(tx.error || new Error('transaction aborted'));
    tx.onerror = () => {};
  });
}

function idbOpen(name, stores) {
  return new Promise((resolve, reject) => {
    const q = indexedDB.open(name, 1);
    q.onupgradeneeded = () => {
      for (const s of stores) if (!q.result.objectStoreNames.contains(s)) q.result.createObjectStore(s);
    };
    q.onsuccess = () => resolve(q.result);
    q.onerror = () => reject(q.error);
    q.onblocked = () => reject(new Error('open blocked'));
  });
}

// Dialog's IndexedDB store. Mirrored: one database per space, object stores
// `archive/index` (key: base58 of the block's digest, value: a Uint8Array
// copy of its bytes) and `memory` (key: `space/cell`, value: the content's
// bytes); one connection per context, shared; a get is its own read-only
// transaction answered when its request succeeds; an import is one
// read-write transaction that awaits each put's request before issuing the
// next and is answered on `complete`; a publish is one read-write
// transaction that gets the cell, checks it and puts it; no durability is
// passed, so Chromium's default applies. Simplified: both stores are made
// at open, not by a version upgrade on first use; there is no schema mutex;
// a version is a 32-bit hash, not BLAKE3; content is a string here,
// stored as its UTF-16 code units' low bytes.
//
// `pipeline` issues every put of an import before waiting, which dialog
// does not do; it is measured as the cheapest change to the baseline.
class IdbArchive {
  static async open(name, { durability = 'default', pipeline = false } = {}) {
    const a = new IdbArchive();
    a.db = await idbOpen(name, ['archive/index', 'memory']);
    a.durability = durability;
    a.pipeline = pipeline;
    a.name = name;
    return a;
  }

  txOptions() {
    return this.durability === 'default' ? undefined : { durability: this.durability };
  }

  get(key) {
    const store = this.db.transaction(['archive/index'], 'readonly').objectStore('archive/index');
    return idbRequest(store.get(key)).then((v) => (v === undefined ? null : v));
  }

  getMany(keys) {
    return Promise.all(keys.map((k) => this.get(k)));
  }

  async import(blocks) {
    if (!blocks.length) return 0;
    const tx = this.db.transaction(['archive/index'], 'readwrite', this.txOptions());
    const done = idbDone(tx);
    const store = tx.objectStore('archive/index');
    if (this.pipeline) {
      for (const b of blocks) store.put(b.bytes.slice(), b.key);
    } else {
      for (const b of blocks) await idbRequest(store.put(b.bytes.slice(), b.key));
    }
    await done;
    return blocks.length;
  }

  async resolve(cell) {
    const store = this.db.transaction(['memory'], 'readonly').objectStore('memory');
    const v = await idbRequest(store.get(`local/${cell}`));
    if (v === undefined) return null;
    const content = String.fromCharCode(...v);
    return { content, version: version(content) };
  }

  async publish(cell, content, expected) {
    const tx = this.db.transaction(['memory'], 'readwrite', this.txOptions());
    const done = idbDone(tx);
    const store = tx.objectStore('memory');
    const key = `local/${cell}`;
    const v = await idbRequest(store.get(key));
    const current = v === undefined ? null : String.fromCharCode(...v);
    let write;
    try {
      write = casCheck(current, content, expected);
    } catch (e) {
      tx.abort();
      await done.catch(() => {});
      throw e;
    }
    if (write) await idbRequest(store.put(Uint8Array.from(content, (c) => c.charCodeAt(0)), key));
    await done;
    return version(content);
  }

  async walk(root, depth) {
    let key = root;
    let hops = 0;
    let bytes = 0;
    let last = null;
    while (hops < depth && key) {
      const b = await this.get(key);
      if (!b) throw new Error(`missing block ${key}`);
      hops++;
      bytes += b.length;
      last = key;
      key = readLink(b);
    }
    return { hops, bytes, last };
  }

  dropCache() {}

  stats() {
    return { design: 'idb', durability: this.durability, pipeline: this.pipeline };
  }

  close() {
    this.db.close();
  }
}

// The file: two rolling slots of SLOT bytes at 0, the log from LOG.
const SLOT = 2048;
const LOG = 4096;
const REC = 64;
const REC_MAGIC = 0x4b4c4244;
const SLOT_MAGIC = 0x544f4c53;

// A sync access handle as a file. `flush` says whether the archive's
// acknowledgements wait for `flush()` (fdatasync on Linux).
class OpfsFile {
  constructor(handle, flush) {
    this.h = handle;
    this.flushes = flush;
    this.reads = 0;
  }
  read(off, len) {
    const b = new Uint8Array(len);
    this.h.read(b, { at: off });
    this.reads++;
    return b;
  }
  write(off, bytes) {
    this.h.write(bytes, { at: off });
  }
  size() {
    return this.h.getSize();
  }
  // Between the records and the slot naming them.
  barrier() {
    if (this.flushes) this.h.flush();
  }
  // After the slot: what an acknowledgement waits for.
  commit() {
    if (this.flushes) this.h.flush();
  }
  dropCache() {}
  close() {
    this.h.flush();
    this.h.close();
  }
}

// The file's bytes as page records in IndexedDB (database `<name>.paged`,
// object store `pages`, key: the page number, value: a Uint8Array of
// `pageSize` bytes). Writes go to cached pages and are marked dirty; commit
// puts every dirty page in one read-write transaction, which is atomic, so
// records and slot land together (no barrier is needed, as
// IDBBatchAtomicVFS needs no journal). Reads are answered from the cache
// synchronously; a miss fetches the pages it spans with one getAll over
// their key range. Page 0 (the slots) and the tail page stay cached, so a
// write never has to read first.
class PagedIdbFile {
  static async open(name, { pageSize = 65536, durability = 'relaxed', cachePages = Infinity } = {}) {
    const f = new PagedIdbFile();
    f.db = await idbOpen(`${name}.paged`, ['pages']);
    f.P = pageSize;
    f.durability = durability;
    f.cachePages = cachePages;
    f.cache = new Map();
    f.dirty = new Set();
    f.fetches = 0;
    f.hits = 0;
    f.misses = 0;
    f.end = 0;
    const store = f.db.transaction(['pages'], 'readonly').objectStore('pages');
    const count = await idbRequest(store.count());
    f.end = count * f.P;
    if (count === 0) f.cache.set(0, new Uint8Array(f.P));
    else {
      await f.fetch(0, 0);
      await f.fetch(count - 1, count - 1);
    }
    return f;
  }

  pinned(n) {
    return n === 0 || n === Math.floor((this.end - 1) / this.P) || this.dirty.has(n);
  }

  touch(n, page) {
    this.cache.delete(n);
    this.cache.set(n, page);
    if (this.cache.size > this.cachePages) {
      for (const k of this.cache.keys()) {
        if (this.cache.size <= this.cachePages) break;
        if (!this.pinned(k)) this.cache.delete(k);
      }
    }
  }

  async fetch(first, last) {
    this.fetches++;
    const store = this.db.transaction(['pages'], 'readonly').objectStore('pages');
    const pages = await idbRequest(store.getAll(IDBKeyRange.bound(first, last)));
    for (let i = 0; i < pages.length; i++) if (!this.cache.has(first + i)) this.touch(first + i, pages[i]);
  }

  copyOut(off, len) {
    const out = new Uint8Array(len);
    let at = 0;
    while (at < len) {
      const n = Math.floor((off + at) / this.P);
      const inPage = (off + at) % this.P;
      const k = Math.min(this.P - inPage, len - at);
      const page = this.cache.get(n);
      out.set(page.subarray(inPage, inPage + k), at);
      at += k;
    }
    return out;
  }

  read(off, len) {
    const first = Math.floor(off / this.P);
    const last = Math.floor((off + len - 1) / this.P);
    let missing = false;
    for (let n = first; n <= last; n++) {
      const page = this.cache.get(n);
      if (!page) missing = true;
      else this.touch(n, page);
    }
    if (!missing) {
      this.hits++;
      return this.copyOut(off, len);
    }
    this.misses++;
    return this.fetch(first, last).then(() => this.copyOut(off, len));
  }

  write(off, bytes) {
    let at = 0;
    while (at < bytes.length) {
      const n = Math.floor((off + at) / this.P);
      const inPage = (off + at) % this.P;
      const k = Math.min(this.P - inPage, bytes.length - at);
      let page = this.cache.get(n);
      if (!page) {
        if (n * this.P < this.end) throw new Error(`write to uncached page ${n}`);
        page = new Uint8Array(this.P);
      }
      page.set(bytes.subarray(at, at + k), inPage);
      this.dirty.add(n);
      this.touch(n, page);
      at += k;
    }
    this.end = Math.max(this.end, off + bytes.length);
  }

  size() {
    return this.end;
  }

  barrier() {}

  commit() {
    const opts = this.durability === 'default' ? undefined : { durability: this.durability };
    const tx = this.db.transaction(['pages'], 'readwrite', opts);
    const store = tx.objectStore('pages');
    // put() clones the page now, so later writes to it are not in this
    // transaction.
    for (const n of this.dirty) store.put(this.cache.get(n), n);
    this.dirty = new Set();
    return idbDone(tx);
  }

  dropCache() {
    for (const k of [...this.cache.keys()]) if (!this.pinned(k)) this.cache.delete(k);
  }

  close() {
    this.db.close();
  }
}

// The archive over either file: blocks appended as records (REC bytes of
// header: magic, length, checksum, key length, the key in ASCII; then the
// bytes), the newer of two slots naming the log's end and the cells (as
// JSON), and an index from key to record in memory. An import appends the
// new blocks in one write, passes the barrier, writes the next slot and
// commits; a publish writes the next slot and commits. Recovery reads the
// slots and walks the record headers up to the end the newer slot names.
// Simplified against the native file: the index is rebuilt by that walk,
// not read from index pages, and there are no extents (one writer).
class LogArchive {
  static open(file, design) {
    const a = new LogArchive();
    a.file = file;
    a.design = design;
    a.index = new Map();
    a.cells = {};
    a.seq = 0;
    a.end = LOG;
    a.imports = 0;
    a.publishes = 0;
    a.gets = 0;
    const t = now();
    return andThen(a.recover(), (records) => {
      a.recovery = { records, logEnd: a.end, slotSeq: a.seq, ms: now() - t };
      return a;
    });
  }

  readSlot(bytes) {
    const v = new DataView(bytes.buffer, bytes.byteOffset, bytes.length);
    if (v.getUint32(0, true) !== SLOT_MAGIC) return null;
    if (v.getUint32(SLOT - 4, true) !== sum32(bytes, 0, SLOT - 4)) return null;
    const len = v.getUint32(16, true);
    const json = String.fromCharCode(...bytes.subarray(20, 20 + len));
    return { seq: v.getUint32(4, true), end: v.getUint32(8, true) + v.getUint32(12, true) * 2 ** 32, cells: JSON.parse(json) };
  }

  recover() {
    return andThen(this.file.size() >= LOG ? this.file.read(0, LOG) : null, (head) => {
      if (!head) return 0;
      const slots = [this.readSlot(head.subarray(0, SLOT)), this.readSlot(head.subarray(SLOT, LOG))].filter(Boolean);
      if (!slots.length) return 0;
      const s = slots.sort((a, b) => b.seq - a.seq)[0];
      this.seq = s.seq;
      this.end = s.end;
      this.cells = s.cells;
      return this.scan(LOG, 0);
    });
  }

  scan(at, records) {
    while (at < this.end) {
      const r = this.file.read(at, REC);
      if (r && typeof r.then === 'function') return r.then((h) => this.scanOne(at, h, records)).then(([a, n]) => this.scan(a, n));
      [at, records] = this.scanOne(at, r, records);
    }
    return records;
  }

  scanOne(at, h, records) {
    const v = new DataView(h.buffer, h.byteOffset, REC);
    if (v.getUint32(0, true) !== REC_MAGIC) throw new Error(`no record at ${at}`);
    const len = v.getUint32(4, true);
    const key = String.fromCharCode(...h.subarray(16, 16 + v.getUint32(12, true)));
    this.index.set(key, { at: at + REC, len, sum: v.getUint32(8, true) });
    return [at + REC + len, records + 1];
  }

  writeSlot() {
    this.seq++;
    const b = new Uint8Array(SLOT);
    const v = new DataView(b.buffer);
    const json = JSON.stringify(this.cells);
    if (20 + json.length > SLOT - 4) throw new Error('cells do not fit a slot');
    v.setUint32(0, SLOT_MAGIC, true);
    v.setUint32(4, this.seq, true);
    v.setUint32(8, this.end % 2 ** 32, true);
    v.setUint32(12, Math.floor(this.end / 2 ** 32), true);
    v.setUint32(16, json.length, true);
    for (let i = 0; i < json.length; i++) b[20 + i] = json.charCodeAt(i);
    v.setUint32(SLOT - 4, sum32(b, 0, SLOT - 4), true);
    this.file.write((this.seq % 2) * SLOT, b);
  }

  get(key) {
    this.gets++;
    const r = this.index.get(key);
    if (!r) return null;
    return this.file.read(r.at, r.len);
  }

  getMany(keys) {
    const out = keys.map((k) => this.get(k));
    return out.some((x) => x && typeof x.then === 'function') ? Promise.all(out) : out;
  }

  // Blocks already held are skipped: blocks are content addressed, so a
  // retried import writes nothing twice.
  import(blocks) {
    const fresh = [];
    const seen = new Set();
    let total = 0;
    for (const b of blocks) {
      if (this.index.has(b.key) || seen.has(b.key)) continue;
      seen.add(b.key);
      fresh.push(b);
      total += REC + b.bytes.length;
    }
    if (!fresh.length) return 0;
    const buf = new Uint8Array(total);
    const v = new DataView(buf.buffer);
    const placed = [];
    let at = 0;
    for (const b of fresh) {
      v.setUint32(at, REC_MAGIC, true);
      v.setUint32(at + 4, b.bytes.length, true);
      const sum = sum32(b.bytes);
      v.setUint32(at + 8, sum, true);
      v.setUint32(at + 12, b.key.length, true);
      for (let i = 0; i < b.key.length; i++) buf[at + 16 + i] = b.key.charCodeAt(i);
      buf.set(b.bytes, at + REC);
      placed.push([b.key, { at: this.end + at + REC, len: b.bytes.length, sum }]);
      at += REC + b.bytes.length;
    }
    this.file.write(this.end, buf);
    this.file.barrier();
    this.end += total;
    this.writeSlot();
    return andThen(this.file.commit(), () => {
      for (const [k, r] of placed) this.index.set(k, r);
      this.imports++;
      return fresh.length;
    });
  }

  resolve(cell) {
    const content = this.cells[cell];
    return content === undefined ? null : { content, version: version(content) };
  }

  publish(cell, content, expected) {
    if (!casCheck(this.cells[cell], content, expected)) return version(content);
    this.cells[cell] = content;
    this.writeSlot();
    return andThen(this.file.commit(), () => {
      this.publishes++;
      return version(content);
    });
  }

  // The chain walked where the file is: synchronous on OPFS, synchronous on
  // the paged file while its pages are cached.
  walk(root, depth) {
    let key = root;
    let hops = 0;
    let bytes = 0;
    let last = null;
    const step = () => {
      while (hops < depth && key) {
        const b = this.get(key);
        if (!b) throw new Error(`missing block ${key}`);
        if (typeof b.then === 'function') {
          return b.then((x) => {
            take(x);
            return step();
          });
        }
        take(b);
      }
      return { hops, bytes, last };
    };
    const take = (b) => {
      hops++;
      bytes += b.length;
      last = key;
      key = readLink(b);
    };
    return step();
  }

  dropCache() {
    this.file.dropCache();
  }

  stats() {
    const s = { design: this.design, records: this.index.size, logEnd: this.end, imports: this.imports, publishes: this.publishes, gets: this.gets, recovery: this.recovery };
    if (this.file.fetches !== undefined) Object.assign(s, { fetches: this.file.fetches, hits: this.file.hits, misses: this.file.misses, cachedPages: this.file.cache.size });
    return s;
  }

  close() {
    this.file.close();
  }
}

// Opens `file` in OPFS with the exclusive handle, retrying while another
// handle still holds it (a dead owner's handle closes some milliseconds
// after its worker goes). Returns the handle and how many attempts it took.
async function openExclusive(file, timeoutMs = 10000) {
  const root = await navigator.storage.getDirectory();
  const fh = await root.getFileHandle(file, { create: true });
  const t = now();
  let attempts = 0;
  for (;;) {
    attempts++;
    try {
      return { handle: await fh.createSyncAccessHandle(), attempts };
    } catch (e) {
      if (e.name !== 'NoModificationAllowedError' || now() - t > timeoutMs) throw e;
      await sleep(1);
    }
  }
}

// A client of an owner worker. Requests are {id, cmd, args}; answers
// {re, ok, value | error, epoch}. Every request stays pending until it is
// answered; `retarget(port)` points the link at a new owner and sends it
// every request still pending, so a request the old owner took and never
// answered is done again by the new one (imports skip what they hold,
// publishes find the cell already holding their content). An answer to a
// request that is not pending (answered already) is counted, not used.
class OwnerLink {
  constructor() {
    this.port = null;
    this.next = 0;
    this.pending = new Map();
    this.counts = { sent: 0, resent: 0, answered: 0, unexpectedAnswers: 0, errors: 0 };
    this.onAnswer = null;
  }

  retarget(port, epoch) {
    this.port = port;
    this.epoch = epoch;
    port.onmessage = (e) => this.answer(e.data);
    for (const p of this.pending.values()) {
      if (p.sentTo !== undefined) this.counts.resent++;
      this.send(p);
    }
  }

  send(p) {
    p.sentTo = this.epoch;
    this.counts.sent++;
    this.port.postMessage({ id: p.id, cmd: p.cmd, args: p.args });
  }

  answer(data) {
    if (data.re === undefined) {
      if (this.onEvent) this.onEvent(data);
      return;
    }
    const p = this.pending.get(data.re);
    if (!p) {
      this.counts.unexpectedAnswers++;
      return;
    }
    this.pending.delete(data.re);
    this.counts.answered++;
    if (this.onAnswer) this.onAnswer(data, p);
    if (data.ok) p.resolve(data.value);
    else {
      this.counts.errors++;
      p.reject(new Error(data.error));
    }
  }

  call(cmd, args) {
    return new Promise((resolve, reject) => {
      const p = { id: this.next++, cmd, args, resolve, reject, sentTo: undefined };
      this.pending.set(p.id, p);
      if (this.port) this.send(p);
    });
  }
}

class RemoteArchive {
  constructor(link) {
    this.link = link;
  }
  get(key) {
    return this.link.call('get', { key });
  }
  getMany(keys) {
    return this.link.call('getMany', { keys });
  }
  import(blocks) {
    return this.link.call('import', { blocks });
  }
  resolve(cell) {
    return this.link.call('resolve', { cell });
  }
  publish(cell, content, expected) {
    return this.link.call('publish', { cell, content, expected });
  }
  walk(root, depth) {
    return this.link.call('walk', { root, depth });
  }
  dropCache() {}
  stats() {
    return this.link.call('stats', {}).then((s) => ({ ...s, link: this.link.counts }));
  }
  close() {}
}

// Serves a LogArchive on a port: each request is answered in the task that
// received it (the OPFS archive never waits), so requests from every port
// are serialized, which is the writer's turn. Bytes read are transferred
// back, not copied.
function serveArchive(port, archive, epoch, onServed) {
  port.onmessage = (e) => {
    const { id, cmd, args } = e.data || {};
    // Another port to serve, passed along this one (the shared worker
    // brokering a tab's worker to the owner).
    if (cmd === 'port' && e.ports[0]) return serveArchive(e.ports[0], archive, epoch, onServed);
    if (id === undefined) return;
    let value;
    try {
      if (cmd === 'get') value = archive.get(args.key);
      else if (cmd === 'getMany') value = archive.getMany(args.keys);
      else if (cmd === 'import') value = archive.import(args.blocks);
      else if (cmd === 'resolve') value = archive.resolve(args.cell);
      else if (cmd === 'publish') value = archive.publish(args.cell, args.content, args.expected);
      else if (cmd === 'walk') value = archive.walk(args.root, args.depth);
      else if (cmd === 'stats') value = archive.stats();
      else throw new Error(`no command ${cmd}`);
    } catch (err) {
      port.postMessage({ re: id, ok: false, error: errorName(err), epoch });
      return;
    }
    const transfer = [];
    if (value instanceof Uint8Array) transfer.push(value.buffer);
    else if (Array.isArray(value)) for (const v of value) if (v instanceof Uint8Array) transfer.push(v.buffer);
    port.postMessage({ re: id, ok: true, value, epoch }, transfer);
    if (onServed) onServed(cmd);
  };
}
