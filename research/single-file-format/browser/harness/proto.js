// Question 5: a space in one OPFS file, written by several workers at once.
//
// The file: two rolling slots (offsets 0 and 4096) that name how many records
// each extent has committed; memory cells from 8192; extents from 64 KiB, each
// a run of fixed-size checksummed records appended by the worker that owns it.
//
// Two designs, one layout:
// - "unsafe": every worker opens the file with readwrite-unsafe. A writer owns
//   its extent by holding the Web Lock space/<id>/extent/<n> for its life,
//   appends without any other lock, then takes space/<id>/turn to publish a
//   slot and space/<id>/cell/<i> to update a cell. Readers take no lock.
// - "owner": one worker holds the file's exclusive handle and does every
//   read and write; writers (who still own their extent by its Web Lock) and
//   readers send it requests over a MessagePort the shared worker connected.

importScripts('common.js');

const SLOT = 4096;
const SLOT_MAGIC = 0x534c4f54;
const MAXEXT = 16;
const SLOT_BYTES = 12 + MAXEXT * 4 + 4;
const CELL_BASE = 8192;
const CELL = 64;
const CELL_MAGIC = 0x43454c4c;
const EXTENT_BASE = 64 * 1024;
const REC = 4096;
const REC_MAGIC = 0x52454344;

let config = { space: 's', extentSize: 128 * 1024 * 1024 };
const lockName = (what) => `space/${config.space}/${what}`;
const extentAt = (e, seq) => EXTENT_BASE + e * config.extentSize + seq * REC;

function decodeSlot(buf) {
  const dv = new DataView(buf.buffer, buf.byteOffset, SLOT_BYTES);
  if (dv.getUint32(0, true) !== SLOT_MAGIC) return null;
  if (crc32(buf, 0, SLOT_BYTES - 4) !== dv.getUint32(SLOT_BYTES - 4, true)) return null;
  const records = [];
  for (let e = 0; e < MAXEXT; e++) records.push(dv.getUint32(12 + e * 4, true));
  return { seq: dv.getUint32(4, true), records };
}

function encodeSlot(slot) {
  const buf = new Uint8Array(SLOT_BYTES);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, SLOT_MAGIC, true);
  dv.setUint32(4, slot.seq, true);
  dv.setUint32(8, MAXEXT, true);
  for (let e = 0; e < MAXEXT; e++) dv.setUint32(12 + e * 4, slot.records[e], true);
  dv.setUint32(SLOT_BYTES - 4, crc32(buf, 0, SLOT_BYTES - 4), true);
  return buf;
}

const slotBufs = [new Uint8Array(SLOT_BYTES), new Uint8Array(SLOT_BYTES)];
// The newest whole slot, and how many of the two were whole.
function readSlots(h) {
  let best = null;
  let whole = 0;
  for (let i = 0; i < 2; i++) {
    h.read(slotBufs[i], { at: i * SLOT });
    const s = decodeSlot(slotBufs[i]);
    if (s) {
      whole++;
      if (!best || s.seq > best.seq) best = s;
    }
  }
  return { slot: best || { seq: 0, records: new Array(MAXEXT).fill(0) }, whole };
}

function writeSlot(h, slot) {
  h.write(encodeSlot(slot), { at: (slot.seq % 2) * SLOT });
}

const cellBuf = new Uint8Array(24);
function readCell(h, i) {
  h.read(cellBuf, { at: CELL_BASE + i * CELL });
  const dv = new DataView(cellBuf.buffer);
  if (dv.getUint32(0, true) !== CELL_MAGIC) return { version: 0, value: 0, whole: dv.getUint32(0, true) === 0 };
  if (crc32(cellBuf, 0, 20) !== dv.getUint32(20, true)) return { whole: false };
  return { version: dv.getUint32(4, true), value: dv.getFloat64(8, true), whole: true };
}

function writeCell(h, i, cell, writer) {
  const buf = new Uint8Array(24);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, CELL_MAGIC, true);
  dv.setUint32(4, cell.version, true);
  dv.setFloat64(8, cell.value, true);
  dv.setUint32(16, writer, true);
  dv.setUint32(20, crc32(buf, 0, 20), true);
  h.write(buf, { at: CELL_BASE + i * CELL });
}

// A record: magic, extent, seq, writer, crc of bytes 20.., then extent and
// seq again under the crc, then payload.
function makeRecord(template, e, seq, writer) {
  const buf = new Uint8Array(REC);
  buf.set(template);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, REC_MAGIC, true);
  dv.setUint32(4, e, true);
  dv.setUint32(8, seq, true);
  dv.setUint32(12, writer, true);
  dv.setUint32(20, e, true);
  dv.setUint32(24, seq, true);
  dv.setFloat64(28, Math.random(), true);
  dv.setUint32(16, crc32(buf, 20, REC), true);
  return buf;
}

function checkRecord(buf, e, seq) {
  const dv = new DataView(buf.buffer, buf.byteOffset, REC);
  if (dv.getUint32(0, true) !== REC_MAGIC) return 'magic';
  if (dv.getUint32(4, true) !== e || dv.getUint32(20, true) !== e) return 'extent';
  if (dv.getUint32(8, true) !== seq || dv.getUint32(24, true) !== seq) return 'seq';
  if (crc32(buf, 20, REC) !== dv.getUint32(16, true)) return 'crc';
  return null;
}

// Yields to the event loop without setTimeout's clamping.
const yieldChannel = new MessageChannel();
const yieldQueue = [];
yieldChannel.port1.onmessage = () => yieldQueue.shift()();
const yieldTask = () =>
  new Promise((r) => {
    yieldQueue.push(r);
    yieldChannel.port2.postMessage(0);
  });

async function openFile(mode) {
  const root = await opfsRoot();
  const fh = await root.getFileHandle(config.file, { create: true });
  return mode ? fh.createSyncAccessHandle({ mode }) : fh.createSyncAccessHandle();
}

async function claimExtent() {
  for (let e = 0; e < MAXEXT; e++) {
    const held = await hold(lockName(`extent/${e}`), { ifAvailable: true });
    if (held) return { e, held };
  }
  throw new Error('no extent free');
}

let peers = {};
// Every port a peer was connected on, so none is collected while in use.
const peerPorts = [];
let shared = null;
let report = null;
let role = null;
let stopping = false;
let stopped = null;

async function writer({ design, sync }) {
  const id = (Math.random() * 2 ** 32) >>> 0;
  const present = await hold(lockName('present'), { mode: 'shared' });
  const { e, held } = await claimExtent();
  const template = new Uint8Array(REC);
  crypto.getRandomValues(template);
  let h = null;
  let owner = null;
  let committed;
  if (design === 'unsafe') {
    h = await openFile('readwrite-unsafe');
    committed = readSlots(h).slot.records[e];
  } else {
    owner = peers.owner;
    committed = (await owner('state')).records[e];
  }
  const adoptedAt = committed;
  const latency = [];
  const turnWait = [];
  const cellWait = [];
  let errors = 0;
  let lastError = null;
  let full = false;
  let lastReport = now();
  const t0 = now();
  while (!stopping) {
    if ((committed + 1) * REC > config.extentSize) {
      full = true;
      break;
    }
    const tc = now();
    const rec = makeRecord(template, e, committed, id);
    if (design === 'unsafe') {
      h.write(rec, { at: extentAt(e, committed) });
      if (sync) h.flush();
      const tw = now();
      let ok = true;
      await navigator.locks.request(lockName('turn'), async () => {
        turnWait.push(now() - tw);
        const { slot } = readSlots(h);
        if (slot.records[e] !== committed) {
          ok = false;
          errors++;
          lastError = `slot says ${slot.records[e]} for extent ${e}, writer has ${committed}`;
          return;
        }
        slot.records[e] = committed + 1;
        slot.seq++;
        writeSlot(h, slot);
        if (sync) h.flush();
      });
      if (!ok) break;
      const tl = now();
      await navigator.locks.request(lockName('cell/0'), async () => {
        cellWait.push(now() - tl);
        const cell = readCell(h, 0);
        writeCell(h, 0, { version: (cell.version || 0) + 1, value: (cell.value || 0) + 1 }, id);
      });
    } else {
      try {
        await owner('commit', { e, seq: committed, rec, writer: id }, [rec.buffer]);
      } catch (err) {
        errors++;
        lastError = String(err);
        break;
      }
    }
    committed++;
    latency.push(now() - tc);
    if (now() - lastReport > 100) {
      lastReport = now();
      report({ kind: 'progress', e, committed });
    }
  }
  const elapsed = now() - t0;
  if (h) h.close();
  held.release();
  present.release();
  return {
    role: 'writer',
    design,
    e,
    adoptedAt,
    committed,
    commits: committed - adoptedAt,
    elapsedMs: elapsed,
    full,
    errors,
    lastError,
    commit: us(summarize(latency)),
    turnWait: us(summarize(turnWait)),
    cellWait: us(summarize(cellWait)),
  };
}

async function reader({ design }) {
  let h = null;
  let owner = null;
  if (design === 'unsafe') h = await openFile('readwrite-unsafe');
  else owner = peers.owner;
  const buf = new Uint8Array(REC);
  const slotLat = [];
  const readLat = [];
  const cellLat = [];
  const verified = new Array(MAXEXT).fill(0);
  let reads = 0;
  let bad = 0;
  let badKinds = {};
  let slotsTorn = 0;
  let cellTorn = 0;
  let cellBackwards = 0;
  let lastCell = 0;
  let iter = 0;
  while (!stopping) {
    let t = now();
    let slot;
    if (h) {
      const r = readSlots(h);
      slot = r.slot;
      if (r.whole < 2 && slot.seq > 1) slotsTorn++;
    } else {
      slot = await owner('slot');
    }
    slotLat.push(now() - t);
    const live = [];
    for (let e = 0; e < MAXEXT; e++) if (slot.records[e] > 0) live.push(e);
    if (live.length) {
      const e = live[Math.floor(Math.random() * live.length)];
      const seq = Math.floor(Math.random() * slot.records[e]);
      t = now();
      let rec;
      if (h) {
        h.read(buf, { at: extentAt(e, seq) });
        rec = buf;
      } else {
        rec = new Uint8Array(await owner('read', { e, seq }));
      }
      const wrong = checkRecord(rec, e, seq);
      readLat.push(now() - t);
      reads++;
      if (wrong) {
        bad++;
        badKinds[wrong] = (badKinds[wrong] || 0) + 1;
      }
      // Walk the records committed since the last look, a few at a time.
      if (h) {
        for (const e2 of live) {
          let k = 0;
          while (verified[e2] < slot.records[e2] && k++ < 4) {
            h.read(buf, { at: extentAt(e2, verified[e2]) });
            const w = checkRecord(buf, e2, verified[e2]);
            if (w) {
              bad++;
              badKinds[`walk:${w}`] = (badKinds[`walk:${w}`] || 0) + 1;
            }
            verified[e2]++;
          }
        }
      }
    }
    t = now();
    const cell = h ? readCell(h, 0) : await owner('cell');
    cellLat.push(now() - t);
    if (!cell.whole) cellTorn++;
    else {
      if (cell.value < lastCell) cellBackwards++;
      lastCell = cell.value;
    }
    if (h && ++iter % 16 === 0) await yieldTask();
  }
  if (h) h.close();
  return {
    role: 'reader',
    design,
    reads,
    bad,
    badKinds,
    slotsTorn,
    cellTorn,
    cellBackwards,
    walked: verified.reduce((a, b) => a + b, 0),
    slotRead: us(summarize(slotLat)),
    recordRead: us(summarize(readLat)),
    cellRead: us(summarize(cellLat)),
  };
}

// The owner: the file's only handle, exclusive; requests run one at a time
// (each handler is synchronous), which is the turn.
let ownerState = null;
async function startOwner({ sync }) {
  const leader = await hold(lockName('owner'), { ifAvailable: true });
  if (!leader) throw new Error('another worker owns the space');
  const h = await openFile();
  const { slot } = readSlots(h);
  const cell = readCell(h, 0);
  ownerState = { h, slot, cell, sync, leader, commits: 0, reads: 0, errors: 0, lastError: null };
  return { records: slot.records };
}

const ownerHandlers = {
  state: () => ({ records: ownerState.slot.records }),
  commit: ({ e, seq, rec, writer }) => {
    const o = ownerState;
    if (o.slot.records[e] !== seq) {
      o.errors++;
      o.lastError = `slot says ${o.slot.records[e]} for extent ${e}, writer has ${seq}`;
      throw new Error(o.lastError);
    }
    o.h.write(rec instanceof Uint8Array ? rec : new Uint8Array(rec), { at: extentAt(e, seq) });
    if (o.sync) o.h.flush();
    o.slot.records[e] = seq + 1;
    o.slot.seq++;
    writeSlot(o.h, o.slot);
    if (o.sync) o.h.flush();
    o.cell = { version: (o.cell.version || 0) + 1, value: (o.cell.value || 0) + 1 };
    writeCell(o.h, 0, o.cell, writer);
    o.commits++;
    return seq + 1;
  },
  slot: () => readSlots(ownerState.h).slot,
  read: ({ e, seq }) => {
    const buf = new Uint8Array(REC);
    ownerState.h.read(buf, { at: extentAt(e, seq) });
    ownerState.reads++;
    return withTransfer(buf.buffer, [buf.buffer]);
  },
  cell: () => readCell(ownerState.h, 0),
};

function stopOwner() {
  const o = ownerState;
  o.h.flush();
  o.h.close();
  o.leader.release();
  return { role: 'owner', commits: o.commits, reads: o.reads, errors: o.errors, lastError: o.lastError };
}

async function verify() {
  const h = await openFile();
  const { slot, whole } = readSlots(h);
  const buf = new Uint8Array(REC);
  const extents = [];
  for (let e = 0; e < MAXEXT; e++) {
    const n = slot.records[e];
    if (!n) continue;
    const bad = {};
    let firstBad = null;
    for (let seq = 0; seq < n; seq++) {
      h.read(buf, { at: extentAt(e, seq) });
      const w = checkRecord(buf, e, seq);
      if (w) {
        bad[w] = (bad[w] || 0) + 1;
        if (firstBad === null) firstBad = seq;
      }
    }
    // What lies past the committed end: a record a writer appended and
    // never published, which is allowed, whole or torn.
    buf.fill(0);
    const got = h.read(buf, { at: extentAt(e, n) });
    const past = got === 0 ? 'end of file' : checkRecord(buf, e, n) || 'whole record';
    extents.push({ e, records: n, bad, firstBad, pastEnd: past === 'magic' ? 'zeros' : past });
  }
  const cell = readCell(h, 0);
  const size = h.getSize();
  h.close();
  return { slotSeq: slot.seq, slotsWhole: whole, extents, cell, size };
}

const handlers = {
  hello: () => true,
  echo: (x) => x,
  configure: (c) => {
    config = { ...config, ...c };
    return config;
  },
  attachShared: (args, e) => {
    shared = endpoint(e.ports[0], handlers);
    return true;
  },
  peer: ({ tag }, e) => {
    peerPorts.push(e.ports[0]);
    peers[tag] = endpoint(e.ports[0], { ...handlers, ...(role === 'owner' ? ownerHandlers : {}) });
    return true;
  },
  run: (args) => {
    stopping = false;
    role = args.role;
    stopped = (args.role === 'writer' ? writer(args) : reader(args)).catch((e) => ({
      role: args.role,
      failed: errorName(e),
    }));
    return true;
  },
  owner: (args) => {
    role = 'owner';
    return startOwner(args);
  },
  stopOwner,
  stop: async () => {
    stopping = true;
    return stopped;
  },
  verify,
  remove: ({ file }) => removeFile(file),
};

report = endpoint(self, handlers).bind(null, 'report');
