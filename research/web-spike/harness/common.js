// Helpers shared by every context of the harness: page, dedicated workers and
// the shared worker. Loaded with importScripts in workers and a script tag in
// the page, so it defines globals.

const now = () => performance.now();
// An absolute time in milliseconds, comparable across contexts: each context
// has its own time origin, and the sum is on the wall clock.
const abs = () => performance.timeOrigin + performance.now();

function summarize(samples) {
  if (!samples.length) return { n: 0 };
  const s = Float64Array.from(samples).sort();
  const at = (q) => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  let sum = 0;
  for (const x of s) sum += x;
  return {
    n: s.length,
    min: s[0],
    median: at(0.5),
    p95: at(0.95),
    p99: at(0.99),
    max: s[s.length - 1],
    mean: sum / s.length,
  };
}

// Microseconds, rounded to tenths, for the JSON.
function us(summary) {
  const out = { n: summary.n };
  for (const k of ['min', 'median', 'p95', 'p99', 'max', 'mean']) {
    if (k in summary) out[k] = Math.round(summary[k] * 10000) / 10;
  }
  return out;
}

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

function crc32(bytes, start = 0, end = bytes.length) {
  let c = 0xffffffff;
  for (let i = start; i < end; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function errorName(e) {
  return e && e.name ? `${e.name}: ${e.message}` : String(e);
}

// One end of a request/response channel over a port (a MessagePort, or a
// worker's global scope). Requests are {id, cmd, args}, answered with
// {re: id, ok, value} or {re: id, ok: false, error}; both ends may ask. A
// handler returns `withTransfer(value, buffers)` to transfer buffers back.
// Returns `call(cmd, args, transfer)`.
function endpoint(port, handlers = {}) {
  let next = 0;
  const pending = new Map();
  port.onmessage = async (event) => {
    const data = event.data || {};
    if (data.re !== undefined) {
      const p = pending.get(data.re);
      if (!p) return;
      pending.delete(data.re);
      data.ok ? p.resolve(data.value) : p.reject(new Error(data.error));
      return;
    }
    if (data.id === undefined) return;
    const handler = handlers[data.cmd];
    try {
      if (!handler) throw new Error(`no handler ${data.cmd}`);
      const value = await handler(data.args, event);
      if (value && value.__transfer) {
        port.postMessage({ re: data.id, ok: true, value: value.value }, value.__transfer);
      } else {
        port.postMessage({ re: data.id, ok: true, value });
      }
    } catch (e) {
      port.postMessage({ re: data.id, ok: false, error: errorName(e) });
    }
  };
  return (cmd, args, transfer = []) =>
    new Promise((resolve, reject) => {
      const id = next++;
      pending.set(id, { resolve, reject });
      port.postMessage({ id, cmd, args }, transfer);
    });
}

function withTransfer(value, transfer) {
  return { __transfer: transfer, value };
}

// A Web Lock held until `release` is called, the shape of a native
// lock/unlock pair. Resolves once the lock is granted, or with null when
// `ifAvailable` finds it taken.
function hold(name, options = {}) {
  return new Promise((granted, failed) => {
    let release;
    const released = new Promise((r) => (release = r));
    const done = navigator.locks
      .request(name, options, (lock) => {
        if (!lock) {
          granted(null);
          return;
        }
        granted({ release, done });
        return released;
      })
      .catch((e) => {
        failed(e);
        return e;
      });
  });
}

async function opfsRoot() {
  return navigator.storage.getDirectory();
}

async function removeFile(name) {
  const root = await opfsRoot();
  try {
    await root.removeEntry(name);
    return true;
  } catch (e) {
    return errorName(e);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function spin(ms) {
  const end = now() + ms;
  while (now() < end) {}
}
