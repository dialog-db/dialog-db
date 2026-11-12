// A dedicated worker for questions 1 to 4: sync access handle costs, message
// round trips, Web Locks, and handles shared between workers.

importScripts('common.js');

const KiB = 1024;
const MiB = 1024 * KiB;
const handles = {};
const peers = {};
let shared = null;
let myName = '';

// Times `fn` per call, and in batches of `batch` calls divided by `batch`,
// since one call can be shorter than the timer's resolution.
function measure(fn, n, batch = 20) {
  for (let i = 0; i < Math.min(50, n); i++) fn(i);
  const single = [];
  for (let i = 0; i < n; i++) {
    const t = now();
    fn(i);
    single.push(now() - t);
  }
  const batched = [];
  for (let j = 0; j < Math.max(1, Math.floor(n / batch)); j++) {
    const t = now();
    for (let k = 0; k < batch; k++) fn(j * batch + k);
    batched.push((now() - t) / batch);
  }
  return { perCall: us(summarize(single)), perBatch: us(summarize(batched)), batch };
}

// The smallest step performance.now() takes here.
function timerResolution() {
  let smallest = Infinity;
  for (let i = 0; i < 2000; i++) {
    const a = now();
    let b = now();
    while (b === a) b = now();
    smallest = Math.min(smallest, b - a);
  }
  return Math.round(smallest * 1e4) / 10;
}

function rand(n) {
  return Math.floor(Math.random() * n);
}

async function openHandle(file, mode) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file, { create: true });
  return mode ? fh.createSyncAccessHandle({ mode }) : fh.createSyncAccessHandle();
}

async function q1({ file = 'q1.bin', sizeMiB = 32, n = 2000, flushN = 200 }) {
  const h = await openHandle(file);
  const size = sizeMiB * MiB;
  h.truncate(0);
  const chunk = new Uint8Array(MiB);
  crypto.getRandomValues(chunk.subarray(0, 65536));
  for (let i = 1; i < 16; i++) chunk.copyWithin(i * 65536, 0, 65536);
  for (let at = 0; at < size; at += MiB) h.write(chunk, { at });
  h.flush();
  const b4 = new Uint8Array(4 * KiB);
  const b100 = new Uint8Array(100 * KiB);
  crypto.getRandomValues(b4);
  crypto.getRandomValues(b100.subarray(0, 65536));
  const pages4 = size / (4 * KiB);
  const out = {};
  out.read4k = measure(() => h.read(b4, { at: rand(pages4) * 4 * KiB }), n);
  out.read100k = measure(() => h.read(b100, { at: rand(pages4 - 25) * 4 * KiB }), n);
  out.write4k = measure(() => h.write(b4, { at: rand(pages4) * 4 * KiB }), n);
  out.write100k = measure(() => h.write(b100, { at: rand(pages4 - 25) * 4 * KiB }), n);
  // Appends grow the file; it is cut back (untimed) before it passes 2x.
  let end = h.getSize();
  out.append4k = measure(() => {
    h.write(b4, { at: end });
    end += b4.length;
  }, n);
  h.truncate(size);
  end = size;
  out.append100k = measure(() => {
    if (end > 2 * size) {
      h.truncate(size);
      end = size;
    }
    h.write(b100, { at: end });
    end += b100.length;
  }, n);
  h.truncate(size);
  out.getSize = measure(() => h.getSize(), n);
  h.flush();
  out.flushClean = measure(() => h.flush(), flushN, 5);
  const flushAfter = (bytes) => {
    const samples = [];
    for (let i = 0; i < flushN; i++) {
      h.write(bytes, { at: rand(pages4 - 25) * 4 * KiB });
      const t = now();
      h.flush();
      samples.push(now() - t);
    }
    return us(summarize(samples));
  };
  out.flushAfter4k = flushAfter(b4);
  out.flushAfter100k = flushAfter(b100);
  const big = new Uint8Array(MiB);
  out.flushAfter1M = flushAfter(big);
  // write + flush as one commit of 4 KiB appended.
  const commits = [];
  end = h.getSize();
  for (let i = 0; i < flushN; i++) {
    const t = now();
    h.write(b4, { at: end });
    end += b4.length;
    h.flush();
    commits.push(now() - t);
  }
  out.append4kFlush = us(summarize(commits));
  h.close();
  return out;
}

// Flush alone, by mode and by what was written before it, on a fresh file.
async function flushProbe({ n = 100, mode, growMiB = 0 }) {
  const file = `flush-${mode || 'readwrite'}-${growMiB}.bin`;
  const h = await openHandle(file, mode);
  h.truncate(0);
  const b4 = new Uint8Array(4 * KiB);
  if (growMiB) {
    const chunk = new Uint8Array(MiB);
    for (let i = 0; i < growMiB; i++) h.write(chunk, { at: i * MiB });
  }
  h.write(b4, { at: 0 });
  h.flush();
  const time = (fn) => {
    const s = [];
    for (let i = 0; i < n; i++) {
      const t = now();
      fn(i);
      s.push(now() - t);
    }
    return us(summarize(s));
  };
  const out = { mode: mode || 'readwrite', growMiB };
  out.clean = time(() => h.flush());
  out.overwrite4kThenFlush = time(() => {
    h.write(b4, { at: 0 });
    h.flush();
  });
  let end = h.getSize();
  out.append4kThenFlush = time(() => {
    h.write(b4, { at: end });
    end += b4.length;
    h.flush();
  });
  out.cleanAfterAppends = time(() => h.flush());
  h.truncate(4 * KiB);
  h.flush();
  out.cleanAfterTruncate = time(() => h.flush());
  h.close();
  await removeFile(file);
  return out;
}

async function q1open({ file = 'q1.bin', n = 300, mode }) {
  const root = await opfsRoot();
  const fileHandle = [];
  const create = [];
  const close = [];
  const both = [];
  for (let i = 0; i < n + 20; i++) {
    let t = now();
    const fh = await root.getFileHandle(file, { create: true });
    const tf = now() - t;
    t = now();
    const h = mode ? await fh.createSyncAccessHandle({ mode }) : await fh.createSyncAccessHandle();
    const tc = now() - t;
    const t2 = now();
    h.close();
    const tx = now() - t2;
    if (i >= 20) {
      fileHandle.push(tf);
      create.push(tc);
      close.push(tx);
      both.push(tc + tx);
    }
  }
  return {
    mode: mode || 'readwrite (default)',
    getFileHandle: us(summarize(fileHandle)),
    createSyncAccessHandle: us(summarize(create)),
    close: us(summarize(close)),
    openPlusClose: us(summarize(both)),
  };
}

async function locksUncontended({ n = 2000, prefix = 'bench' }) {
  const L = navigator.locks;
  const cycle = async (name, options) => {
    const samples = [];
    for (let i = 0; i < n + 100; i++) {
      const t = now();
      await L.request(name, options, async () => {});
      if (i >= 100) samples.push(now() - t);
    }
    return us(summarize(samples));
  };
  const out = {};
  out.exclusive = await cycle(`${prefix}/x`, { mode: 'exclusive' });
  out.shared = await cycle(`${prefix}/s`, { mode: 'shared' });
  out.ifAvailableFree = await cycle(`${prefix}/t`, { ifAvailable: true });
  const held = await hold(`${prefix}/held`);
  const busy = [];
  let nulls = 0;
  for (let i = 0; i < n + 100; i++) {
    const t = now();
    await L.request(`${prefix}/held`, { ifAvailable: true }, async (lock) => {
      if (!lock) nulls++;
    });
    if (i >= 100) busy.push(now() - t);
  }
  held.release();
  out.ifAvailableHeld = { ...us(summarize(busy)), refused: nulls };
  // The native shape: acquire, do something, release, with acquire and
  // release as two calls.
  const acquire = [];
  const release = [];
  for (let i = 0; i < n + 100; i++) {
    let t = now();
    const h = await hold(`${prefix}/pair`);
    const ta = now() - t;
    t = now();
    h.release();
    await h.done;
    const tr = now() - t;
    if (i >= 100) {
      acquire.push(ta);
      release.push(tr);
    }
  }
  out.holdAcquire = us(summarize(acquire));
  out.holdReleaseSettled = us(summarize(release));
  const q = [];
  for (let i = 0; i < 300; i++) {
    const t = now();
    await L.query();
    q.push(now() - t);
  }
  out.query = us(summarize(q));
  return out;
}

async function contend({ name, rounds = 500, holdMs = 0, who }) {
  const log = [];
  for (let i = 0; i < rounds; i++) {
    const entry = { who, req: abs() };
    await navigator.locks.request(name, async () => {
      entry.acq = abs();
      if (holdMs) spin(holdMs);
      entry.rel = abs();
    });
    log.push(entry);
  }
  return log;
}

const heldLocks = {};
async function holdLock({ name, mode = 'exclusive', key = name }) {
  const state = { aborted: null };
  heldLocks[key] = state;
  const got = await hold(name, { mode });
  state.handle = got;
  got.done.then((r) => {
    state.aborted = r instanceof Error ? errorName(r) : 'released';
  });
  return true;
}

async function waitLock({ name, steal = false, mode = 'exclusive' }) {
  const options = steal ? { steal: true } : { mode };
  return navigator.locks.request(name, options, async () => Date.now());
}

let heartbeatTimer = null;
function heartbeat({ channel = 'heartbeat', ms = 10 }) {
  const bc = new BroadcastChannel(channel);
  let i = 0;
  heartbeatTimer = setInterval(() => bc.postMessage({ from: myName, i: i++, t: Date.now() }), ms);
  return true;
}

const heard = {};
function listen({ channel = 'heartbeat' }) {
  const bc = new BroadcastChannel(channel);
  heard[channel] = [];
  bc.onmessage = (e) => heard[channel].push(e.data.t);
  return true;
}

async function presentRace({ name, key }) {
  const state = {};
  heldLocks[key] = state;
  state.handle = await hold(name, { mode: 'shared' });
  return true;
}

// Leaving: let the shared hold go, then try the lock exclusive. With
// `turn`, inside the writer's turn, as the native close does.
async function presentLeave({ key, name, turn }) {
  const leave = async () => {
    heldLocks[key].handle.release();
    await heldLocks[key].handle.done;
    return navigator.locks.request(name, { ifAvailable: true }, async (l) => !!l);
  };
  return turn ? navigator.locks.request(turn, leave) : leave();
}

async function tryOpen({ file, mode, keep }) {
  try {
    const h = await openHandle(file, mode);
    if (keep) handles[keep] = h;
    else h.close();
    return 'opened';
  } catch (e) {
    return errorName(e);
  }
}

async function tryOthers({ file }) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file, { create: true });
  const out = {};
  try {
    const f = await fh.getFile();
    const buf = await f.slice(0, 16).arrayBuffer();
    out.getFile = `read ${buf.byteLength} of ${f.size} bytes`;
  } catch (e) {
    out.getFile = errorName(e);
  }
  try {
    const w = await fh.createWritable({ keepExistingData: true });
    await w.abort();
    out.createWritable = 'opened';
  } catch (e) {
    out.createWritable = errorName(e);
  }
  return out;
}

function closeHandle({ key }) {
  if (handles[key]) {
    handles[key].close();
    delete handles[key];
  }
  return true;
}

function fill(buf, value) {
  buf.fill(value & 0xff);
  new DataView(buf.buffer, buf.byteOffset).setUint32(0, value >>> 0, true);
}

function uniform(buf) {
  const v = buf[4];
  for (let i = 5; i < buf.length; i++) if (buf[i] !== v) return false;
  return true;
}

// One writes, tells the other over a direct port, the other reads at once.
async function visWriter({ key, tag, n = 2000, size = 4 * KiB, check = 'visCheck', file }) {
  const h = handles[key];
  const buf = new Uint8Array(size);
  let wrong = 0;
  const rtt = [];
  for (let i = 1; i <= n; i++) {
    const at = (i % 64) * size;
    fill(buf, i);
    h.write(buf, { at });
    const t = now();
    const ok = await peers[tag](check, { key: `${key}:peer`, i, at, size, file });
    rtt.push(now() - t);
    if (!ok) wrong++;
  }
  return { n, wrong, notifyAndCheck: us(summarize(rtt)) };
}

function visCheck({ key, i, at, size }) {
  const h = handles[key];
  const buf = new Uint8Array(size);
  h.read(buf, { at });
  const dv = new DataView(buf.buffer);
  return dv.getUint32(0, true) === i && buf[size - 1] === (i & 0xff);
}

// The same check through getFile(), which takes no lock: a File is a
// snapshot, so a fresh one is taken for every read.
async function visCheckFile({ file, i, at, size }) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file);
  const f = await fh.getFile();
  const buf = new Uint8Array(await f.slice(at, at + size).arrayBuffer());
  if (buf.length < size) return false;
  return new DataView(buf.buffer).getUint32(0, true) === i && buf[size - 1] === (i & 0xff);
}

// Reads through getFile() while another worker may hold the file: the cost
// of a read, and what a File taken before a write does when read after it.
async function fileReads({ file, n = 500, size = 4 * KiB, writerKey }) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file);
  const getFile = [];
  const sliceRead = [];
  for (let i = 0; i < n; i++) {
    let t = now();
    const f = await fh.getFile();
    getFile.push(now() - t);
    const at = rand(Math.max(1, Math.floor(f.size / size) - 1)) * size;
    t = now();
    await f.slice(at, at + size).arrayBuffer();
    sliceRead.push(now() - t);
  }
  return { getFile: us(summarize(getFile)), sliceRead: us(summarize(sliceRead)) };
}

const staleFiles = {};
async function takeFile({ file, key }) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file);
  staleFiles[key] = await fh.getFile();
  return staleFiles[key].size;
}

async function readTakenFile({ key }) {
  try {
    const buf = await staleFiles[key].slice(0, 16).arrayBuffer();
    return `read ${buf.byteLength} bytes`;
  } catch (e) {
    return errorName(e);
  }
}

function tornWriter({ key, ms, size, at = 0, base = 0 }) {
  const h = handles[key];
  const bufs = [new Uint8Array(size), new Uint8Array(size)];
  const end = now() + ms;
  let writes = 0;
  while (now() < end) {
    const b = bufs[writes & 1];
    fill(b, base + writes);
    h.write(b, { at });
    writes++;
  }
  return { writes };
}

function tornReader({ key, ms, size, at = 0 }) {
  const h = handles[key];
  const buf = new Uint8Array(size);
  const end = now() + ms;
  let reads = 0;
  let mixed = 0;
  let headerMismatch = 0;
  while (now() < end) {
    h.read(buf, { at });
    reads++;
    if (!uniform(buf)) mixed++;
    else if ((new DataView(buf.buffer).getUint32(0, true) & 0xff) !== buf[4]) headerMismatch++;
  }
  return { reads, mixed, headerMismatch };
}

function checkUniform({ key, at = 0, size }) {
  const buf = new Uint8Array(size);
  handles[key].read(buf, { at });
  const counts = {};
  for (let i = 4; i < size; i++) counts[buf[i]] = (counts[buf[i]] || 0) + 1;
  return counts;
}

function writeDirty({ key, bytes, at = 0 }) {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf.subarray(0, 65536));
  const t = now();
  handles[key].write(buf, { at });
  return now() - t;
}

function timeFlush({ key }) {
  const t = now();
  handles[key].flush();
  return now() - t;
}

// Turns under a Web Lock. With `perTurn`, each turn opens the file
// exclusively and closes it; otherwise it uses a handle already open.
async function turns({ file, ms, lock, perTurn, key, flush, size = 4 * KiB }) {
  const buf = new Uint8Array(size);
  crypto.getRandomValues(buf);
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file, { create: true });
  const turn = [];
  const open = [];
  const close = [];
  const waited = [];
  let count = 0;
  let errors = 0;
  let lastError = null;
  const end = now() + ms;
  while (now() < end) {
    const tw = now();
    await navigator.locks.request(lock, async () => {
      const t0 = now();
      waited.push(t0 - tw);
      let h;
      try {
        if (perTurn) {
          h = await fh.createSyncAccessHandle();
          open.push(now() - t0);
        } else {
          h = handles[key];
        }
        h.write(buf, { at: h.getSize() });
        if (flush) h.flush();
        if (perTurn) {
          const tc = now();
          h.close();
          close.push(now() - tc);
        }
        count++;
      } catch (e) {
        errors++;
        lastError = errorName(e);
      }
      turn.push(now() - t0);
    });
  }
  return {
    count,
    errors,
    lastError,
    turn: us(summarize(turn)),
    waited: us(summarize(waited)),
    open: us(summarize(open)),
    close: us(summarize(close)),
  };
}

async function openRetry({ file, mode, keep, timeoutMs = 10000 }) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(file, { create: true });
  const end = Date.now() + timeoutMs;
  let attempts = 0;
  let lastError = null;
  while (Date.now() < end) {
    attempts++;
    try {
      const h = mode ? await fh.createSyncAccessHandle({ mode }) : await fh.createSyncAccessHandle();
      if (keep) handles[keep] = h;
      else h.close();
      return { at: Date.now(), attempts };
    } catch (e) {
      lastError = errorName(e);
    }
  }
  return { at: null, attempts, lastError };
}

const handlers = {
  hello: ({ name }) => {
    myName = name;
    return { name, crossOriginIsolated: self.crossOriginIsolated };
  },
  echo: (x) => x,
  attachShared: (args, e) => {
    shared = endpoint(e.ports[0], handlers);
    return true;
  },
  peer: ({ tag }, e) => {
    peers[tag] = endpoint(e.ports[0], handlers);
    return true;
  },
  pingPeer: async ({ tag, n = 2000, warm = 200 }) => {
    const samples = [];
    for (let i = 0; i < n + warm; i++) {
      const t = now();
      await peers[tag]('echo', i);
      if (i >= warm) samples.push(now() - t);
    }
    return us(summarize(samples));
  },
  q1,
  q1open,
  flushProbe,
  locksUncontended,
  contend,
  holdLock,
  releaseLock: ({ key }) => {
    heldLocks[key].handle.release();
    return true;
  },
  lockState: ({ key }) => ({ aborted: heldLocks[key] && heldLocks[key].aborted }),
  waitLock,
  heartbeat,
  listen,
  heard: ({ channel = 'heartbeat' }) => heard[channel] || [],
  presentRace,
  presentLeave,
  tryOpen,
  tryOthers,
  closeHandle,
  visWriter,
  visCheck,
  visCheckFile,
  fileReads,
  takeFile,
  readTakenFile,
  tornWriter,
  tornReader,
  checkUniform,
  writeDirty,
  timeFlush,
  turns,
  openRetry,
  remove: ({ file }) => removeFile(file),
  locksQuery: () => navigator.locks.query(),
  timer: () => timerResolution(),
};

endpoint(self, handlers);
