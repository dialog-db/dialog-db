// Round four: one dialog worker hosted by a tab (design E) against today's
// dialog in the service worker on IndexedDB (A), on round three's trace,
// every request timed in the tab that makes it. Every configuration in one
// sitting, a fresh profile per run, interleaved round by round; then the
// failovers. See the journal's round four.
//
//   SPIKE_TRACES=dir node round4.js <trace> <open-trace> <native-file>... [--rounds N] [--only A,E,...]
//
// `<trace>` and `<open-trace>` are files in `dir`: the trace of the
// workload, and the trace of the native archive opening the files it left
// (`cargo run -p dialog-chronicle --features trace --example trace_open`).
// Each `<native-file>` is a traced file as the native run left it, to check
// the replay rebuilt it byte for byte. Writes
// results/<date>T<time>-chromium-<major>-round4.json.

const { chromium } = require('playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const vm = require('vm');
const { start } = require('./server');

const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf(name);
  if (i < 0) return dflt;
  const v = args[i + 1];
  args.splice(i, 2);
  return v;
};
const rounds = Number(flag('--rounds', '3'));
const only = flag('--only', null);
const compactWait = Number(flag('--compact-wait', '20000'));
const passes = Number(flag('--passes', '5'));
const [traceName, openName, ...natives] = args;
if (!openName) throw new Error('usage: SPIKE_TRACES=dir node round4.js <trace> <open-trace> <native-file>...');

const MiB = 2 ** 20;
const CACHE = 64 * MiB;
const CONFIGS = [
  { id: 'A', mode: 'A', tabs: 1 },
  { id: 'E', mode: 'E', tabs: 1, dropCold: true },
  { id: 'Ek', mode: 'E', tabs: 1, dropCold: false },
  { id: 'Ed', mode: 'E', tabs: 1, dropCold: true, direct: true },
  { id: 'A3', mode: 'A', tabs: 3 },
  { id: 'E3', mode: 'E', tabs: 3, dropCold: true },
  { id: 'Ek3', mode: 'E', tabs: 3, dropCold: false },
  { id: 'Ed3', mode: 'E', tabs: 3, dropCold: true, direct: true },
  { id: 'FA-sw', mode: 'A', tabs: 3, kill: 'sw' },
  { id: 'FE-sw', mode: 'E', tabs: 3, dropCold: true, kill: 'sw' },
  { id: 'FE-close', mode: 'E', tabs: 3, dropCold: true, kill: 'close' },
  { id: 'FE-crash', mode: 'E', tabs: 3, dropCold: true, kill: 'crash' },
  { id: 'FE-hand', mode: 'E', tabs: 3, dropCold: true, kill: 'hand' },
];
const chosen = only ? CONFIGS.filter((c) => only.split(',').includes(c.id)) : CONFIGS;

// The trace's operations by name, parsed with the harness's own parser.
function traceOps(file) {
  const ctx = { console, TextDecoder, performance, navigator: {}, setTimeout };
  ctx.self = ctx;
  vm.createContext(ctx);
  for (const f of ['common.js', 'store.js', 'replay.js']) vm.runInContext(fs.readFileSync(path.join(__dirname, 'harness', f), 'utf8'), ctx, { filename: f });
  const b = fs.readFileSync(file);
  const t = ctx.parseTrace(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
  return { names: t.ops.map((o) => o.name || '(none)'), summary: ctx.traceSummary(t) };
}

function sum32(bytes) {
  let h = 0x811c9dc5 | 0;
  const n = bytes.length >> 2;
  const words = new Uint32Array(bytes.buffer, bytes.byteOffset, n);
  for (let j = 0; j < n; j++) h = Math.imul(h ^ words[j], 0x01000193);
  for (let i = n << 2; i < bytes.length; i++) h = Math.imul(h ^ bytes[i], 0x01000193);
  return h >>> 0;
}

function dirBytes(dir) {
  let n = 0;
  if (!fs.existsSync(dir)) return 0;
  for (const f of fs.readdirSync(dir)) {
    const p = path.join(dir, f);
    const st = fs.statSync(p);
    n += st.isDirectory() ? dirBytes(p) : st.size;
  }
  return n;
}

function profileSizes(profile) {
  const d = path.join(profile, 'Default');
  const out = { indexedDB: dirBytes(path.join(d, 'IndexedDB')), fileSystem: dirBytes(path.join(d, 'File System')) };
  out.total = out.indexedDB + out.fileSystem;
  return out;
}

let base = '';

async function newPage(ctx) {
  const page = await ctx.newPage();
  page.on('pageerror', (e) => console.error('  pageerror:', e.message));
  page.on('console', (m) => {
    if (m.type() === 'error') console.error('  console:', m.text());
  });
  await page.goto(`${base}/page4.html`);
  await page.waitForFunction(() => typeof spike4 !== 'undefined');
  const r = await page.evaluate(() => spike4.init());
  if (!r.isolated || !r.controlled) throw new Error(`page not isolated or not controlled: ${JSON.stringify(r)}`);
  return page;
}

const ev = (page, fn, arg) => page.evaluate(fn, arg);
const waitFor = async (check, ms = 30000) => {
  const t = Date.now();
  for (;;) {
    if (await check()) return;
    if (Date.now() - t > ms) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
};

async function runConfig(cfgIn, names, nativeSums) {
  console.log(`round 4: ${cfgIn.id}`);
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'web-spike4-'));
  const ctx = await chromium.launchPersistentContext(profile);
  for (const p of ctx.pages()) await p.close().catch(() => {});
  const space = `s${Date.now().toString(36)}`;
  const cfg = { ...cfgIn, space, traceUrl: `/trace/${traceName}`, openTraceUrl: `/trace/${openName}`, cacheBytes: CACHE };
  const out = { cfg: cfgIn };
  const all = names.map((name, i) => [i, name, false, name.endsWith('/cold')]);
  const week = names.findIndex((n) => ['pull', 'push', 'command'].includes(n));
  const reads = all.filter(([, n]) => n === 'query/cold' || n === 'poll').map(([i, n, , c]) => [i, n, true, c]);
  try {
    // Tabs open in the order they offer to host: the first hosts, the
    // writer offers last, so at three tabs it never hosts.
    const pages = [];
    for (let k = 0; k < cfg.tabs; k++) pages.push(await newPage(ctx));
    const writer = pages[pages.length - 1];
    const readers = pages.slice(0, -1);
    if (cfg.mode === 'A') {
      out.initA = await ev(writer, (c) => spike4.sw({ type: 'initA', space: c.space, traceUrl: c.traceUrl }), cfg);
    } else {
      for (const p of pages) await ev(p, (c) => spike4.candidate(c), cfg);
      await waitFor(() => ev(pages[0], () => spike4.isHost));
      await waitFor(async () => (await ev(writer, () => spike4.sw({ type: 'stats' }))).host != null);
      if (cfg.direct) for (const p of pages) await ev(p, () => spike4.connect());
    }
    const t = Date.now();
    await ev(writer, ([c, l]) => spike4.replay(c, l), [cfg, all.slice(0, week)]);
    for (const p of readers) await ev(p, ([c, l]) => spike4.readers(c, l), [cfg, reads]);
    if (!cfg.kill) {
      await ev(writer, ([c, l]) => spike4.replay(c, l), [cfg, all.slice(week)]);
    } else {
      await ev(writer, ([c, l]) => spike4.startReplay(c, l), [cfg, all.slice(week)]);
      const killAt = week + Math.floor((names.length - week) * 0.4);
      await waitFor(() => ev(writer, (k) => spike4.done >= k, killAt), 120000);
      const host = pages[0];
      out.killAt = Date.now();
      if (cfg.kill === 'close') host.close().catch(() => {});
      else if (cfg.kill === 'crash') {
        const cdp = await ctx.newCDPSession(host);
        cdp.send('Page.crash').catch(() => {});
      } else if (cfg.kill === 'hand') ev(host, () => spike4.hidden()).catch(() => {});
      else if (cfg.kill === 'sw') {
        const cdp = await ctx.newCDPSession(writer);
        await cdp.send('ServiceWorker.enable');
        await cdp.send('ServiceWorker.stopAllWorkers');
      }
      out.finished = await ev(writer, () => spike4.finished());
      if (cfg.kill === 'close' || cfg.kill === 'crash') readers.shift();
    }
    for (const p of readers) await ev(p, () => spike4.stop());
    out.wallMs = Date.now() - t;
    const live = cfg.kill === 'close' || cfg.kill === 'crash' ? pages.slice(1) : pages;
    out.hostLogs = [];
    for (const p of live) out.hostLogs.push(await ev(p, () => spike4.hostLog));
    if (cfg.mode === 'E') {
      for (const p of live) {
        const sums = await ev(p, () => spike4.hostCall('checksum'));
        if (sums) {
          out.checksums = sums;
          out.matchesNative = nativeSums.length ? sums.every((s, f) => nativeSums[f] && s.sum === nativeSums[f].sum && s.len === nativeSums[f].len) : null;
          out.stats = await ev(p, () => spike4.hostCall('stats'));
        }
      }
    }
    out.sw = await ev(writer, () => spike4.sw({ type: 'stats' }));
    // At one tab: the cold view reads and polls again, read only, with the
    // host; then, for E, with no host, served by the service worker
    // through getFile().
    if (cfg.tabs === 1 && !cfg.kill) {
      const pass = [];
      for (let k = 0; k < passes; k++) pass.push(...reads);
      await ev(writer, ([c, l]) => spike4.replay(c, l, 'post/'), [cfg, pass]);
      if (cfg.mode === 'E' && !cfg.direct) {
        out.handoffToNone = await ev(writer, () => spike4.hidden());
        out.openNoHost = await ev(writer, (c) => spike4.sw({ type: 'openNoHost', space: c.space, traceUrl: c.traceUrl, cacheBytes: c.cacheBytes }), cfg);
        await ev(writer, ([c, l]) => spike4.replay(c, l, 'nohost/'), [cfg, pass]);
        out.swAfter = await ev(writer, () => spike4.sw({ type: 'stats' }));
      }
    }
    out.records = await ev(writer, () => spike4.take());
    out.readerRecords = [];
    for (const p of readers) out.readerRecords.push(await ev(p, () => spike4.take()));
  } catch (e) {
    out.error = String(e && e.stack ? e.stack : e);
    console.error('  error:', out.error);
  }
  await ctx.close().catch(() => {});
  if (!cfg.kill) {
    out.disk = profileSizes(profile);
    if (cfg.mode === 'A') {
      // LevelDB keeps what was rewritten until it compacts: reopen, open
      // the databases, idle, measure again (as round three).
      const again = await chromium.launchPersistentContext(profile);
      const page = await again.newPage();
      await page.goto(`${base}/`);
      await page.evaluate(async () => {
        const names = (await indexedDB.databases()).map((d) => d.name);
        self.__dbs = await Promise.all(names.map((n) => new Promise((res, rej) => {
          const q = indexedDB.open(n);
          q.onsuccess = () => res(q.result);
          q.onerror = () => rej(q.error);
        })));
      });
      await new Promise((r) => setTimeout(r, compactWait));
      await again.close().catch(() => {});
      out.diskAfterIdle = profileSizes(profile);
    }
  }
  fs.rmSync(profile, { recursive: true, force: true });
  const disk = out.disk ? `, disk ${(out.disk.total / MiB).toFixed(1)} MiB` : '';
  console.log(`  ${out.error ? 'failed' : 'done'} in ${((out.wallMs || 0) / 1000).toFixed(1)} s${disk}${out.matchesNative === false ? ', DOES NOT MATCH NATIVE' : ''}`);
  return out;
}

(async () => {
  const dir = process.env.SPIKE_TRACES;
  const { names, summary } = traceOps(path.join(dir, traceName));
  const nativeSums = natives.map((p) => {
    const b = fs.readFileSync(p);
    return { len: b.length, sum: sum32(new Uint8Array(b.buffer, b.byteOffset, b.length)) };
  });
  const server = await start(0);
  base = `http://localhost:${server.address().port}`;
  const probe = await chromium.launch();
  const version = probe.version();
  await probe.close();
  const runs = [];
  for (let r = 0; r < rounds; r++) {
    for (const cfg of chosen) runs.push({ round: r, ...(await runConfig(cfg, names, nativeSums)) });
  }
  const d = new Date().toISOString();
  const stamp = `${d.slice(0, 10)}T${d.slice(11, 13)}${d.slice(14, 16)}`;
  const file = path.join(__dirname, 'results', `${stamp}-chromium-${version.split('.')[0]}-round4.json`);
  fs.writeFileSync(file, JSON.stringify({ chromium: version, trace: traceName, openTrace: openName, natives: nativeSums, cpus: os.cpus().length, rounds, passes, summary: { ops: summary.ops, pages: summary.pages }, runs }, null, 1));
  console.log(`wrote ${file}`);
  server.close();
})();
