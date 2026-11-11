// Round three: the native file archive's trace (rust/dialog-archive-file's
// `trace` feature, written by tonk_bench) replayed against each design in
// headless Chromium, every design in one sitting, a fresh profile per run,
// the designs interleaved round by round. See the journal's round three.
//
//   SPIKE_TRACES=dir node round3.js <trace> <native-file>... [--rounds N] [--only A,B16,...]
//
// `<trace>` is a file in `dir`; each `<native-file>` is a traced file as
// the native run left it, in the trace's file order, to check that the
// replay rebuilt it byte for byte. Writes results/<date>-chromium-<major>-round3.json.

const { chromium } = require('playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { start } = require('./server');

const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf(name);
  if (i < 0) return dflt;
  const v = args[i + 1];
  args.splice(i, 2);
  return v;
};
const rounds = Number(flag('--rounds', '2'));
const only = flag('--only', null);
const compactWait = Number(flag('--compact-wait', '20000'));
const [traceName, ...natives] = args;
if (!traceName) throw new Error('usage: SPIKE_TRACES=dir node round3.js <trace> <native-file>...');

const MiB = 2 ** 20;
// Every design's configurations, cheapest first. Page caches are the same
// size for B, C and D.
const CACHE = 64 * MiB;
const CONFIGS = [
  { id: 'A', design: 'A', tabs: 1 },
  { id: 'B16', design: 'B', slice: 16384, cacheBytes: CACHE, tabs: 1 },
  { id: 'C', design: 'C', slice: 4096, cacheBytes: CACHE, tabs: 1 },
  { id: 'D', design: 'D', slice: 4096, cacheBytes: CACHE, tabs: 1 },
  { id: 'A3', design: 'A', tabs: 3 },
  { id: 'B16x3', design: 'B', slice: 16384, cacheBytes: CACHE, tabs: 3 },
  { id: 'C3', design: 'C', slice: 4096, cacheBytes: CACHE, tabs: 3 },
  { id: 'D3', design: 'D', slice: 4096, cacheBytes: CACHE, tabs: 3 },
  { id: 'Dk', design: 'D', slice: 4096, cacheBytes: CACHE, tabs: 1, keepOwnerCache: true },
  { id: 'Dk3', design: 'D', slice: 4096, cacheBytes: CACHE, tabs: 3, keepOwnerCache: true },
  { id: 'B64', design: 'B', slice: 65536, cacheBytes: CACHE, tabs: 1 },
  { id: 'C0', design: 'C', slice: 4096, cacheBytes: 0, tabs: 1 },
];
const chosen = only ? CONFIGS.filter((c) => only.split(',').includes(c.id)) : CONFIGS.filter((c) => !c.keepOwnerCache);

// The ops the week starts with, where reader tabs join, and the ops they
// read over and over.
const WEEK = ['pull', 'push', 'command'];
const READS = ['query/cold', 'poll'];

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
  const idb = path.join(d, 'IndexedDB');
  const out = { indexedDB: dirBytes(idb), fileSystem: dirBytes(path.join(d, 'File System')), leveldb: 0, blobs: 0 };
  if (fs.existsSync(idb)) {
    for (const f of fs.readdirSync(idb)) {
      const p = path.join(idb, f);
      if (f.endsWith('.leveldb')) out.leveldb += dirBytes(p);
      else if (f.endsWith('.blob')) out.blobs += dirBytes(p);
    }
  }
  out.total = out.indexedDB + out.fileSystem;
  return out;
}

let base = '';

async function newPage(ctx) {
  const page = await ctx.newPage();
  page.on('pageerror', (e) => console.error('  pageerror:', e.message));
  page.on('crash', () => console.error('  page crashed'));
  page.on('console', (m) => {
    if (m.type() === 'error') console.error('  console:', m.text());
  });
  await page.goto(`${base}/`);
  await page.waitForFunction(() => self.spike);
  await page.evaluate(() => spike.spawn('r', 'replayworker.js'));
  return page;
}

const call = (page, cmd, a = {}) => page.evaluate(([c, x]) => spike.call('r', c, x), [cmd, a]);

async function link(writer, ownerPage, tag) {
  for (const [p, id] of [[writer, `w-${tag}`], [ownerPage, 'owner']]) {
    await p.evaluate(() => spike.connectShared());
    await p.evaluate(([i]) => spike.register('r', i), [id]);
  }
  await writer.evaluate(([a, b]) => spike.shareCall('connect', { a, b, tag: 'owner' }), [`w-${tag}`, 'owner']);
}

async function runConfig(cfg, nativeSums) {
  console.log(`round 3: ${cfg.id}`);
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'web-spike3-'));
  const ctx = await chromium.launchPersistentContext(profile);
  for (const p of ctx.pages()) await p.close().catch(() => {});
  const space = `s${Date.now().toString(36)}`;
  const url = `/trace/${traceName}`;
  const out = { cfg };
  try {
    const writer = await newPage(ctx);
    out.trace = await call(writer, 'load', { url });
    let ownerPage = null;
    if (cfg.design === 'D') {
      ownerPage = await newPage(ctx);
      await call(ownerPage, 'load', { url });
      await call(ownerPage, 'ownerSetup', { ...cfg, space });
      await link(writer, ownerPage, 'w');
    }
    await call(writer, 'setup', { ...cfg, space });
    const readers = [];
    if (cfg.tabs > 1) {
      for (let i = 1; i < cfg.tabs; i++) {
        const page = await newPage(ctx);
        await call(page, 'load', { url });
        if (cfg.design === 'D') await link(page, ownerPage, `r${i}`);
        readers.push(page);
      }
    }
    const week = await call(writer, 'firstOf', { names: WEEK });
    const t = Date.now();
    const first = await call(writer, 'replay', { from: 0, to: week });
    // Readers set up once the space is in (their caches start empty).
    for (const page of readers) await call(page, 'setup', { ...cfg, space });
    const reading = readers.map((page) => call(page, 'readers', { names: READS }));
    const rest = await call(writer, 'replay', { from: week });
    for (const page of readers) await call(page, 'stop');
    out.readers = await Promise.all(reading);
    out.wallMs = Date.now() - t;
    out.times = { ...first.times };
    for (const [k, v] of Object.entries(rest.times)) out.times[k] = (out.times[k] || []).concat(v);
    out.stats = cfg.design === 'D' ? await call(writer, 'ownerStatsRemote') : await call(writer, 'stats');
    if (cfg.design !== 'A') {
      const sums = cfg.design === 'D' ? await call(writer, 'ownerChecksumRemote') : await call(writer, 'checksum');
      out.checksums = sums;
      out.matchesNative = nativeSums.length ? sums.every((s, f) => nativeSums[f] && s.sum === nativeSums[f].sum && s.len === nativeSums[f].len) : null;
    }
    for (const page of readers) await call(page, 'close');
    await call(writer, 'close');
    if (ownerPage) await call(ownerPage, 'close');
  } catch (e) {
    out.error = String(e && e.stack ? e.stack : e);
    console.error('  error:', out.error);
  }
  await ctx.close().catch(() => {});
  out.disk = profileSizes(profile);
  if (cfg.design === 'A' || cfg.design === 'B') {
    // LevelDB keeps what was rewritten until it compacts: open the profile
    // again, open the databases and leave them idle, then measure again.
    const again = await chromium.launchPersistentContext(profile);
    const page = await again.newPage();
    await page.goto(`${base}/`);
    await page.evaluate(async () => {
      const names = (await indexedDB.databases()).map((d) => d.name);
      const dbs = await Promise.all(
        names.map(
          (n) =>
            new Promise((res, rej) => {
              const q = indexedDB.open(n);
              q.onsuccess = () => res(q.result);
              q.onerror = () => rej(q.error);
            }),
        ),
      );
      self.__dbs = dbs;
    });
    await new Promise((r) => setTimeout(r, compactWait));
    await again.close().catch(() => {});
    out.diskAfterIdle = profileSizes(profile);
  }
  fs.rmSync(profile, { recursive: true, force: true });
  console.log(`  ${out.error ? 'failed' : 'done'} in ${((out.wallMs || 0) / 1000).toFixed(1)} s, disk ${(out.disk.total / MiB).toFixed(1)} MiB${out.matchesNative === false ? ', DOES NOT MATCH NATIVE' : ''}`);
  return out;
}

(async () => {
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
    for (const cfg of chosen) runs.push({ round: r, ...(await runConfig(cfg, nativeSums)) });
  }
  const stamp = new Date().toISOString().slice(0, 16).replace(/[-:]/g, '').replace('T', 'T');
  const file = path.join(__dirname, 'results', `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}T${stamp.slice(9)}-chromium-${version.split('.')[0]}-round3.json`);
  const json = { chromium: version, trace: traceName, natives: nativeSums, cpus: os.cpus().length, rounds, runs };
  fs.writeFileSync(file, JSON.stringify(json, null, 1));
  console.log(`wrote ${file}`);
  server.close();
})();
