// The workload, run where dialog would run: in a dedicated worker
// (hostworker.js) or in the shared worker (dialogsw.js). It keeps a ledger of
// every block it was told was stored and every cell it published, so it can
// check what it reads and what survives.
//
// The workload is tonk-shaped and the same for every store:
// - a commit is an import of 10 to 50 new blocks (blockSize in store.js)
//   followed by a publish of the committer's head cell, compare-and-swap on
//   the version it last published;
// - a chain read follows links from a chain's root through `depth` blocks,
//   each read naming the next (a search tree's descent);
// - a batch reads 16 independent blocks at once.
// Loaded after common.js and store.js; `makeHost(openStore)` returns the
// handlers.

function makeHost(openStore) {
  const host = {
    store: null,
    salt: (Math.random() * 2 ** 32) >>> 0,
    seq: 0,
    blocks: [],
    known: new Map(),
    chains: [],
    cells: {},
    genMs: [],
    bad: [],
  };

  // Set by setup: `salt` decides every seed and size, `ratio` the compact
  // format's payload (storedSize in store.js).
  host.rand = Math.random;
  host.ratio = 1;
  const size = () => storedSize(blockSize(host.rand), host.ratio);
  const nextSeed = () => (host.salt + host.seq++ * 2654435761) >>> 0;

  function remember(b, seed, next, raw) {
    const entry = { key: b.key, seed, size: b.bytes.length, raw, next, sum: sum32(b.bytes) };
    host.blocks.push(entry);
    host.known.set(b.key, entry);
  }

  function genCommit(n = 10 + Math.floor(host.rand() * 41)) {
    const t = now();
    const out = [];
    for (let i = 0; i < n; i++) {
      const seed = nextSeed();
      const raw = blockSize(host.rand);
      out.push({ seed, raw, block: makeBlock(seed, storedSize(raw, host.ratio)) });
    }
    host.genMs.push(now() - t);
    return out;
  }

  function check(key, bytes, where) {
    const e = host.known.get(key);
    if (!bytes) {
      host.bad.push({ where, key, why: 'missing' });
      return false;
    }
    if (e && (bytes.length !== e.size || sum32(bytes) !== e.sum)) {
      host.bad.push({ where, key, why: `wrong bytes (${bytes.length} of ${e.size})` });
      return false;
    }
    return true;
  }

  async function commit(cell) {
    const gen = genCommit();
    const blocks = gen.map((g) => g.block);
    const t0 = now();
    await host.store.import(blocks);
    const t1 = now();
    for (const g of gen) remember(g.block, g.seed, null, g.raw);
    const c = host.cells[cell] || { n: 0, version: null };
    const content = `${blocks[0].key}:${c.n + 1}`;
    const v = await host.store.publish(cell, content, c.version);
    const t2 = now();
    host.cells[cell] = { n: c.n + 1, version: v, content };
    let bytes = 0;
    for (const b of blocks) bytes += b.bytes.length;
    return { importMs: t1 - t0, publishMs: t2 - t1, totalMs: t2 - t0, blocks: blocks.length, bytes };
  }

  // A chain read from a random chain's root, `depth` hops: `hop` awaits the
  // store's get per hop; `walk` asks the store to walk it (synchronously
  // where the store can, in one request for a remote owner).
  async function chain(depth, mode, cold) {
    const c = host.chains[Math.floor(Math.random() * host.chains.length)];
    if (cold) host.store.dropCache();
    const t0 = now();
    let last = null;
    if (mode === 'walk') {
      const r = await host.store.walk(c[0], depth);
      last = r.last;
    } else {
      let key = c[0];
      for (let i = 0; i < depth; i++) {
        const b = await host.store.get(key);
        if (!check(key, b, `chain hop ${i}`)) break;
        last = key;
        key = readLink(b);
      }
    }
    const ms = now() - t0;
    if (last !== c[depth - 1]) host.bad.push({ where: `chain depth ${depth}`, why: `ended at ${last}, expected ${c[depth - 1]}` });
    return ms;
  }

  async function batch(n = 16, cold) {
    const keys = [];
    for (let i = 0; i < n; i++) keys.push(host.blocks[Math.floor(Math.random() * host.blocks.length)].key);
    if (cold) host.store.dropCache();
    const t0 = now();
    const got = await Promise.all(keys.map((k) => host.store.get(k)));
    const ms = now() - t0;
    got.forEach((b, i) => check(keys[i], b, 'batch'));
    return ms;
  }

  const ms2us = (xs) => us(summarize(xs));

  const handlers = {
    setup: async (cfg) => {
      host.cfg = cfg;
      if (cfg.salt !== undefined) {
        host.salt = cfg.salt >>> 0;
        host.rand = rngFrom(cfg.salt);
      }
      host.ratio = cfg.ratio || 1;
      host.store = await openStore(cfg);
      return true;
    },

    // Chains of 8 blocks (each naming the next), then other blocks up to
    // `blocks`, imported 50 at a time with no publish.
    preload: async ({ blocks = 3000, chains = 150 }) => {
      const t0 = now();
      let pending = [];
      let bytes = 0;
      const flush = async () => {
        if (!pending.length) return;
        await host.store.import(pending.map((p) => p.block));
        for (const p of pending) remember(p.block, p.seed, p.next, p.raw);
        pending = [];
      };
      for (let c = 0; c < chains; c++) {
        const keys = [];
        let next = null;
        const made = [];
        for (let i = 0; i < 8; i++) {
          const seed = nextSeed();
          const raw = blockSize(host.rand);
          const b = makeBlock(seed, storedSize(raw, host.ratio), next);
          made.push({ seed, block: b, next, raw });
          keys.unshift(b.key);
          next = b.key;
          bytes += raw;
        }
        pending.push(...made);
        host.chains.push(keys);
        if (pending.length >= 50) await flush();
      }
      while (host.blocks.length + pending.length < blocks) {
        const seed = nextSeed();
        const raw = blockSize(host.rand);
        const b = makeBlock(seed, storedSize(raw, host.ratio));
        pending.push({ seed, block: b, next: null, raw });
        bytes += raw;
        if (pending.length >= 50) await flush();
      }
      await flush();
      const ms = now() - t0;
      return { blocks: host.blocks.length, bytes, ms, mibPerSecond: bytes / 2 ** 20 / (ms / 1000) };
    },

    // What another host needs to read the same data.
    ledger: () => ({ blocks: host.blocks, chains: host.chains }),
    loadLedger: ({ blocks, chains }) => {
      host.blocks = blocks;
      host.chains = chains;
      for (const b of blocks) host.known.set(b.key, b);
      return blocks.length;
    },

    commits: async ({ n = 100, cell = 'head' }) => {
      const imp = [];
      const pub = [];
      const tot = [];
      let bytes = 0;
      let blocks = 0;
      const t0 = now();
      for (let i = 0; i < n; i++) {
        const r = await commit(cell);
        imp.push(r.importMs);
        pub.push(r.publishMs);
        tot.push(r.totalMs);
        bytes += r.bytes;
        blocks += r.blocks;
      }
      const s = (now() - t0) / 1000;
      return {
        n,
        importUs: ms2us(imp),
        publishUs: ms2us(pub),
        commitUs: ms2us(tot),
        commitsPerSecond: n / s,
        blocksPerCommit: blocks / n,
        mibPerSecond: bytes / 2 ** 20 / s,
      };
    },

    chains: async ({ depth, n = 200, mode = 'hop', cold = false }) => {
      const xs = [];
      for (let i = 0; i < n; i++) xs.push(await chain(depth, mode, cold));
      return { depth, mode, cold, us: ms2us(xs), meanUs: (xs.reduce((a, b) => a + b, 0) / xs.length) * 1000 };
    },

    // Single gets of random blocks, one at a time, by size: the 64 KiB
    // blocks apart from the rest.
    gets: async ({ n = 400 }) => {
      const small = [];
      const large = [];
      for (let i = 0; i < n; i++) {
        const e = host.blocks[Math.floor(Math.random() * host.blocks.length)];
        const t0 = now();
        const b = await host.store.get(e.key);
        ((e.raw || e.size) >= 65536 ? large : small).push(now() - t0);
        check(e.key, b, 'get');
      }
      return { smallUs: ms2us(small), largeUs: ms2us(large) };
    },

    // A get of a block already stored, issued just after an import is
    // started and before it is acknowledged: does the read wait for the
    // write? (An IndexedDB read-only transaction created after a read-write
    // one over the same store waits for it to finish.)
    contend: async ({ n = 30 }) => {
      const during = [];
      const imports = [];
      let readFirst = 0;
      for (let i = 0; i < n; i++) {
        const gen = genCommit();
        const e = host.blocks[Math.floor(Math.random() * host.blocks.length)];
        const t0 = now();
        let importDone = null;
        const p = Promise.resolve(host.store.import(gen.map((g) => g.block))).then(() => (importDone = now()));
        const b = await host.store.get(e.key);
        const tg = now();
        await p;
        for (const g of gen) remember(g.block, g.seed, null, g.raw);
        check(e.key, b, 'contend');
        during.push(tg - t0);
        imports.push(importDone - t0);
        if (tg < importDone) readFirst++;
      }
      return { getDuringImportUs: ms2us(during), importUs: ms2us(imports), readAnsweredBeforeImport: readFirst, n };
    },

    batches: async ({ n = 100, size = 16, cold = false }) => {
      const xs = [];
      for (let i = 0; i < n; i++) xs.push(await batch(size, cold));
      return { size, cold, us: ms2us(xs) };
    },

    // One commit, then four chain reads (depth 1, 4, 8 and a random one)
    // and a batch, in a loop for `ms`.
    mixed: async ({ ms = 3000, cell = 'head', mode = 'hop' }) => {
      const lat = { commit: [], chain1: [], chain4: [], chain8: [], batch: [] };
      let commits = 0;
      const t0 = now();
      while (now() - t0 < ms) {
        lat.commit.push((await commit(cell)).totalMs);
        commits++;
        for (const d of [1, 4, 8, [1, 4, 8][Math.floor(Math.random() * 3)]]) lat[`chain${d}`].push(await chain(d, mode, false));
        lat.batch.push(await batch(16, false));
      }
      const s = (now() - t0) / 1000;
      const out = { seconds: s, commits, commitsPerSecond: commits / s };
      for (const [k, v] of Object.entries(lat)) out[k] = ms2us(v);
      return out;
    },

    // One operation, for a caller that times it from outside (a page
    // driving the shared worker). Returns the time inside, too.
    op: async ({ kind, depth, mode = 'hop', cell = 'head' }) => {
      const t0 = now();
      if (kind === 'commit') await commit(cell);
      else if (kind === 'chain') await chain(depth, mode, false);
      else if (kind === 'batch') await batch(16, false);
      else if (kind === 'echo') return 0;
      else throw new Error(`no op ${kind}`);
      return now() - t0;
    },

    // Every block in the ledger read back and checked, 16 at a time, and
    // every cell published here resolved.
    verify: async () => {
      const before = host.bad.length;
      let read = 0;
      for (let i = 0; i < host.blocks.length; i += 16) {
        const keys = host.blocks.slice(i, i + 16).map((b) => b.key);
        const got = await host.store.getMany(keys);
        got.forEach((b, j) => check(keys[j], b, 'verify'));
        read += keys.length;
      }
      const cells = {};
      for (const [cell, c] of Object.entries(host.cells)) {
        const r = await host.store.resolve(cell);
        cells[cell] = r && r.content === c.content ? 'ok' : `holds ${r && r.content}, last acknowledged ${c.content}`;
      }
      return { read, bad: host.bad.slice(before), cells };
    },

    status: async () => ({
      blocks: host.blocks.length,
      ledgerBytes: host.blocks.reduce((a, b) => a + (b.raw || b.size), 0),
      storedBytes: host.blocks.reduce((a, b) => a + b.size, 0),
      bad: host.bad.length,
      badSample: host.bad.slice(0, 5),
      genUs: ms2us(host.genMs),
      store: await host.store.stats(),
    }),

    estimate: () => navigator.storage.estimate(),
    close: () => {
      host.store.close();
      return true;
    },
  };
  return { host, handlers };
}
