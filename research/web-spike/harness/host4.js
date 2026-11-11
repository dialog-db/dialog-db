// Round four's dialog worker (design E): a dedicated worker in the tab that
// holds the host lock. It holds every traced file with the exclusive
// `readwrite` sync handle and replays each operation it is asked for on it,
// behind round three's page cache, one operation at a time. The service
// worker sends it requests over a port; a tab may get a port of its own
// (design Ed).

importScripts('common.js', 'store.js', 'replay.js');

let trace = null;
let replay = null;
let cfg = null;
let epoch = 0;
let chain = Promise.resolve();
let draining = false;
let served = 0;
const timings = {};

async function loadTrace(url) {
  const t = now();
  const buf = await (await fetch(url)).arrayBuffer();
  return { parsed: parseTrace(buf), ms: now() - t };
}

// Serves requests {id, i, ro, cold} on `port`, answering {re, epoch}.
function serve(port) {
  port.onmessage = (event) => {
    const r = event.data || {};
    // A tab's own port to this worker (design Ed), brokered by the
    // service worker.
    if (r.attach && event.ports[0]) {
      serve(event.ports[0]);
      return;
    }
    if (r.id === undefined) return;
    if (draining) return; // the service worker sends it to the next host
    chain = chain.then(async () => {
      if (draining) return;
      const t = now();
      const op = trace.ops[r.i];
      if (r.cold && cfg.dropCold) replay.dropCache();
      await replay.run(op, !!r.ro);
      served++;
      port.postMessage({ re: r.id, epoch, ms: now() - t });
    });
  };
}

const handlers = {
  hello: () => true,
  // Takes the host's role: loads the trace (timed apart: a real worker
  // loads dialog's Wasm instead), opens every file exclusively, and, when
  // the files hold something, replays what the native archive reads to
  // open them (the `open` trace, reads only).
  async start(args) {
    cfg = args;
    epoch = args.epoch;
    const loaded = await loadTrace(args.traceUrl);
    trace = loaded.parsed;
    timings.traceMs = loaded.ms;
    let t = now();
    const handles = [];
    let attempts = 0;
    for (let f = 0; f < trace.files.length; f++) {
      const got = await openExclusive(`${args.space}-f${f}.bin`);
      attempts += got.attempts;
      handles.push(got.handle);
    }
    timings.openMs = now() - t;
    timings.openAttempts = attempts;
    replay = new ByteReplay(trace, { medium: 'opfs', handles, slice: 4096, cacheBytes: args.cacheBytes, locks: null });
    replay.end = handles.map((h) => h.getSize());
    timings.recoverMs = 0;
    if (replay.end.some((n) => n > 0) && args.openTraceUrl) {
      const open = (await loadTrace(args.openTraceUrl)).parsed;
      t = now();
      for (const op of open.ops) await replay.run(op, true);
      timings.recoverMs = now() - t;
    }
    return { ...timings, sizes: replay.end };
  },
  // A port the service worker or a tab sends requests on.
  attach(_, e) {
    serve(e.ports[0]);
    return true;
  },
  // Stops taking requests, finishes the one running, flushes and closes
  // the files: the voluntary hand-off.
  async drain() {
    draining = true;
    const t = now();
    await chain;
    for (const h of replay.handles) {
      h.flush();
      h.close();
    }
    return { drainMs: now() - t, served };
  },
  async checksum() {
    await chain;
    const out = [];
    for (let f = 0; f < trace.files.length; f++) out.push(await replay.checksum(f));
    return out;
  },
  stats: () => ({ ...replay.stats(), served, epoch }),
  async close() {
    await chain;
    for (const h of replay.handles) h.close();
    return true;
  },
};

endpoint(self, handlers);
