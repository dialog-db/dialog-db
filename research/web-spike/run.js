// Drives the harness in headless Chromium through Playwright and writes what
// it measured to results/<date>-chromium-<version>.json.
//
//   node run.js                 every question
//   node run.js q1 q3           some of them
//   node run.js --quick         fewer iterations, shorter runs (a smoke test)
//
// A persistent context (a profile on disk) is used throughout, since a
// Playwright context made with browser.newContext() is off the record and
// Chromium keeps its OPFS in memory there; question 1 runs in both to show
// the difference.

const { chromium } = require('playwright');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { start } = require('./server');

const args = process.argv.slice(2);
const quick = args.includes('--quick');
const only = args.filter((a) => /^q\d$/.test(a));
const wanted = (q) => only.length === 0 || only.includes(q);
const Q = (full, small) => (quick ? small : full);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let base = '';
let onReport = () => {};

async function newPage(ctx) {
  const page = await ctx.newPage();
  page.on('pageerror', (e) => console.error('  pageerror:', e.message));
  page.on('crash', () => console.error('  page crashed'));
  await page.goto(`${base}/`);
  await page.waitForFunction(() => self.spike);
  return page;
}

async function withWorker(ctx, script = 'bench.js', name = 'w') {
  const page = await newPage(ctx);
  await page.evaluate(([n, s]) => spike.spawn(n, s), [name, script]);
  return page;
}

const call = (page, cmd, a, name = 'w') =>
  page.evaluate(([n, c, x]) => spike.call(n, c, x), [name, cmd, a]);
const startCall = (page, key, cmd, a, name = 'w') =>
  page.evaluate(([k, n, c, x]) => spike.start(k, n, c, x), [key, name, cmd, a]);
const result = (page, key) => page.evaluate((k) => spike.result(k), key);

async function register(page, id, name = 'w') {
  await page.evaluate(() => spike.connectShared());
  return page.evaluate(([n, i]) => spike.register(n, i), [name, id]);
}

const shared = (page, cmd, a) => page.evaluate(([c, x]) => spike.shareCall(c, x), [cmd, a]);

function stats(samples) {
  const s = [...samples].sort((a, b) => a - b);
  if (!s.length) return { n: 0 };
  const at = (q) => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  const r = (x) => Math.round(x * 10) / 10;
  return { n: s.length, min: r(s[0]), median: r(at(0.5)), p95: r(at(0.95)), p99: r(at(0.99)), max: r(s[s.length - 1]) };
}

async function closeAll(pages) {
  for (const p of pages) await p.close().catch(() => {});
}

async function q1(ctx, incognito) {
  const out = {};
  for (const [label, context] of [['persistent', ctx], ['incognito', incognito]]) {
    console.log(`q1: sync access handle costs, ${label} profile`);
    const page = await withWorker(context);
    const r = {};
    r.timerResolutionUs = await call(page, 'timer');
    r.calls = await call(page, 'q1', { n: Q(2000, 200), flushN: Q(200, 20) });
    r.flushProbe = [];
    for (const mode of [undefined, 'readwrite-unsafe']) {
      for (const growMiB of [0, 32]) r.flushProbe.push(await call(page, 'flushProbe', { n: Q(100, 10), mode, growMiB }));
    }
    r.open = {};
    for (const mode of [undefined, 'readwrite-unsafe', 'read-only']) {
      r.open[mode || 'readwrite'] = await call(page, 'q1open', { n: Q(300, 30), mode });
    }
    await call(page, 'remove', { file: 'q1.bin' });
    await page.close();
    out[label] = r;
  }
  return out;
}

async function q2(ctx) {
  console.log('q2: shared worker, nested workers, round trips');
  const out = {};
  const a = await withWorker(ctx);
  await a.evaluate(() => spike.spawn('w2', 'bench.js'));
  const b = await withWorker(ctx);
  await register(a, 'A');
  await register(a, 'A2', 'w2');
  await register(b, 'B');
  out.sharedWorkerProbe = await shared(a, 'probe');
  const n = Q(2000, 200);
  out.rtt = {
    pageToOwnWorker: await a.evaluate(([n]) => spike.rtt({ target: 'worker', via: 'w', n }), [n]),
    pageToSharedWorker: await a.evaluate(([n]) => spike.rtt({ target: 'shared', n }), [n]),
    pageToSharedToSamePageWorker: await a.evaluate(([n]) => spike.rtt({ target: 'relay', via: 'A', n }), [n]),
    pageToSharedToOtherPageWorker: await a.evaluate(([n]) => spike.rtt({ target: 'relay', via: 'B', n }), [n]),
  };
  await shared(a, 'connect', { a: 'A', b: 'A2', tag: 'same' });
  await shared(a, 'connect', { a: 'A', b: 'B', tag: 'cross' });
  out.rtt.workerToWorkerSamePage = await call(a, 'pingPeer', { tag: 'same', n });
  out.rtt.workerToWorkerOtherPage = await call(a, 'pingPeer', { tag: 'cross', n });
  // The port A holds to B keeps working with the shared worker gone from the
  // path: ask the shared worker to forget both, then ping again.
  await shared(a, 'forget', { id: 'B' });
  out.rtt.workerToWorkerOtherPageAfterForget = await call(a, 'pingPeer', { tag: 'cross', n: Q(500, 50), warm: 0 });
  await closeAll([a, b]);
  return out;
}

function contention(logs) {
  const all = logs.flat().sort((x, y) => x.acq - y.acq);
  const handoff = [];
  let fifoViolations = 0;
  let repeats = 0;
  for (let i = 1; i < all.length; i++) {
    handoff.push((all[i].acq - all[i - 1].rel) * 1000);
    if (all[i].who === all[i - 1].who) repeats++;
    // Among requests waiting when the lock was released, the oldest should
    // be granted. Allow 50 us for clocks of different contexts.
    const rel = all[i - 1].rel;
    for (let j = i + 1; j < all.length; j++) {
      if (all[j].req < rel && all[j].req < all[i].req - 0.05) {
        fifoViolations++;
        break;
      }
    }
  }
  return { grants: all.length, handoffUs: stats(handoff), sameHolderTwiceInARow: repeats, fifoViolations };
}

async function releaseTest(ctx, action) {
  const name = `release/${action}/${Date.now()}`;
  const holder = await withWorker(ctx);
  const waiter = await withWorker(ctx);
  if (action === 'pageThreadClose') await holder.evaluate((n) => spike.holdInPage(n), name);
  else await call(holder, 'holdLock', { name });
  await startCall(waiter, 'wait', 'waitLock', { name });
  await sleep(300);
  const t0 = Date.now();
  if (action === 'close' || action === 'pageThreadClose') await holder.close();
  else if (action === 'terminate') await holder.evaluate(() => spike.terminate('w'));
  else if (action === 'navigate') await holder.goto('about:blank');
  else if (action === 'crash') {
    const cdp = await ctx.newCDPSession(holder);
    cdp.send('Page.crash').catch(() => {});
  } else if (action === 'release') await call(holder, 'releaseLock', { key: name });
  const got = await Promise.race([result(waiter, 'wait'), sleep(10000).then(() => null)]);
  const out = got && got.ok ? { releasedAfterMs: got.value - t0 } : { releasedAfterMs: null, note: 'not granted in 10 s' };
  await closeAll([holder, waiter]);
  return out;
}

async function freezeTest(ctx) {
  const name = `freeze/${Date.now()}`;
  const holder = await withWorker(ctx);
  const waiter = await withWorker(ctx);
  const stealer = await withWorker(ctx);
  await call(holder, 'holdLock', { name });
  await call(holder, 'heartbeat', { channel: name, ms: 10 });
  await call(waiter, 'listen', { channel: name });
  await startCall(waiter, 'wait', 'waitLock', { name });
  await sleep(300);
  const cdp = await ctx.newCDPSession(holder);
  const frozenAt = Date.now();
  await cdp.send('Page.setWebLifecycleState', { state: 'frozen' });
  await sleep(1500);
  const waiterGotWhileFrozen = await Promise.race([result(waiter, 'wait'), sleep(10).then(() => null)]);
  const stealT0 = Date.now();
  const stolen = await call(stealer, 'waitLock', { name, steal: true });
  const beatsDuringFreeze = (await call(waiter, 'heard', { channel: name })).filter(
    (t) => t > frozenAt + 50 && t < stealT0,
  ).length;
  await sleep(200);
  await cdp.send('Page.setWebLifecycleState', { state: 'active' });
  const activeAt = Date.now();
  await sleep(500);
  const holderView = await call(holder, 'lockState', { key: name });
  const beatsAfter = (await call(waiter, 'heard', { channel: name })).filter((t) => t > activeAt).length;
  const waiterFinal = await Promise.race([result(waiter, 'wait'), sleep(2000).then(() => null)]);
  await closeAll([holder, waiter, stealer]);
  return {
    waiterGrantedWhileHolderFrozen: !!waiterGotWhileFrozen,
    heartbeatsFromFrozenPagesWorkerIn1_5s: beatsDuringFreeze,
    stealGrantedAfterMs: stolen - stealT0,
    holderSawAfterUnfreeze: holderView.aborted,
    heartbeatsIn500msAfterUnfreeze: beatsAfter,
    waiterGrantedEventually: !!(waiterFinal && waiterFinal.ok),
  };
}

async function q3(ctx) {
  console.log('q3: Web Locks');
  const out = {};
  const a = await withWorker(ctx);
  out.uncontendedInWorker = await call(a, 'locksUncontended', { n: Q(2000, 200) });
  out.uncontendedInPage = await a.evaluate(async (n) => {
    const s = [];
    for (let i = 0; i < n; i++) {
      const t = now();
      await navigator.locks.request('page/x', async () => {});
      s.push(now() - t);
    }
    return us(summarize(s));
  }, Q(2000, 200));
  await a.close();
  out.contention = {};
  for (const [pages, holdMs] of [[2, 0], [3, 0], [3, 0.2], [4, 0]]) {
    const ps = [];
    for (let i = 0; i < pages; i++) ps.push(await withWorker(ctx));
    const name = `contend/${pages}/${holdMs}/${Date.now()}`;
    const rounds = Q(1000, 100);
    await Promise.all(ps.map((p, i) => startCall(p, 'c', 'contend', { name, rounds, holdMs, who: i })));
    const logs = await Promise.all(ps.map((p) => result(p, 'c').then((r) => r.value)));
    const totalMs = Math.max(...logs.flat().map((e) => e.rel)) - Math.min(...logs.flat().map((e) => e.req));
    out.contention[`${pages}pages_hold${holdMs}ms`] = { ...contention(logs), grantsPerSecond: Math.round((pages * rounds) / (totalMs / 1000)) };
    await closeAll(ps);
  }
  out.release = {};
  for (const action of ['release', 'close', 'pageThreadClose', 'terminate', 'navigate', 'crash']) {
    const trials = [];
    for (let i = 0; i < Q(5, 2); i++) trials.push((await releaseTest(ctx, action)).releasedAfterMs);
    out.release[action] = { releasedAfterMs: trials };
  }
  out.freeze = await freezeTest(ctx);
  // The present lock: every worker holds it shared; one that leaves lets it
  // go and tries it exclusive, and finding it free means it was the last.
  // Without the turn, and inside it as the native close does.
  const ps = [];
  for (let i = 0; i < 4; i++) ps.push(await withWorker(ctx));
  for (const inTurn of [false, true]) {
    const race = [];
    for (let t = 0; t < Q(20, 3); t++) {
      const name = `present/${inTurn}/${t}/${Date.now()}`;
      const turn = inTurn ? `${name}/turn` : undefined;
      await Promise.all(ps.map((p, i) => call(p, 'presentRace', { name, key: `${name}/${i}` })));
      const lasts = await Promise.all(ps.map((p, i) => call(p, 'presentLeave', { name, key: `${name}/${i}`, turn })));
      race.push(lasts.filter(Boolean).length);
    }
    const hist = {};
    for (const r of race) hist[r] = (hist[r] || 0) + 1;
    out[inTurn ? 'presentLeaveTogetherInTurn' : 'presentLeaveTogether'] = { workers: 4, trials: race.length, workersThatThoughtTheyWereLast: hist };
  }
  await closeAll(ps);
  return out;
}

async function q4(ctx) {
  console.log('q4: handles shared between workers');
  const out = {};
  const [a, b, c] = [await withWorker(ctx), await withWorker(ctx), await withWorker(ctx)];
  const modes = ['readwrite', 'readwrite-unsafe', 'read-only'];
  const m = (x) => (x === 'readwrite' ? undefined : x);
  await call(a, 'tryOpen', { file: 'q4.bin', keep: 'init' });
  await call(a, 'writeDirty', { key: 'init', bytes: 65536 });
  await call(a, 'closeHandle', { key: 'init' });
  out.matrix = {};
  out.othersWhileHeld = {};
  for (const held of modes) {
    out.matrix[held] = {};
    const r = await call(a, 'tryOpen', { file: 'q4.bin', mode: m(held), keep: 'm' });
    if (r !== 'opened') {
      out.matrix[held] = `first open failed: ${r}`;
      continue;
    }
    for (const second of modes) {
      out.matrix[held][second] = {
        otherPage: await call(b, 'tryOpen', { file: 'q4.bin', mode: m(second) }),
        sameWorker: await call(a, 'tryOpen', { file: 'q4.bin', mode: m(second) }),
      };
    }
    out.othersWhileHeld[held] = await call(b, 'tryOthers', { file: 'q4.bin' });
    await call(a, 'closeHandle', { key: 'm' });
  }
  // Reading through getFile() while another page holds the exclusive handle.
  await call(a, 'tryOpen', { file: 'q4.bin', keep: 'm' });
  await call(a, 'writeDirty', { key: 'm', bytes: 4 << 20 });
  out.getFileWhileHeld = await call(b, 'fileReads', { file: 'q4.bin', n: Q(500, 50) });
  await call(b, 'takeFile', { file: 'q4.bin', key: 'snap' });
  await call(a, 'writeDirty', { key: 'm', bytes: 4096, at: 0 });
  out.getFileWhileHeld.fileTakenBeforeAWriteReadAfterIt = await call(b, 'readTakenFile', { key: 'snap' });
  await register(a, 'A');
  await register(b, 'B');
  await shared(a, 'connect', { a: 'A', b: 'B', tag: 'gf' });
  out.getFileWhileHeld.visibility = await call(a, 'visWriter', { key: 'm', tag: 'gf', n: Q(500, 50), size: 4096, check: 'visCheckFile', file: 'q4.bin' });
  await call(a, 'closeHandle', { key: 'm' });
  out.threeUnsafeAtOnce = [];
  for (const p of [a, b, c]) out.threeUnsafeAtOnce.push(await call(p, 'tryOpen', { file: 'q4.bin', mode: 'readwrite-unsafe', keep: 'u' }));
  for (const p of [a, b, c]) await call(p, 'closeHandle', { key: 'u' });

  // Visibility: a writes, tells b over a direct port, b reads at once.
  await call(a, 'tryOpen', { file: 'vis.bin', mode: 'readwrite-unsafe', keep: 'vis' });
  await call(b, 'tryOpen', { file: 'vis.bin', mode: 'readwrite-unsafe', keep: 'vis:peer' });
  await call(c, 'tryOpen', { file: 'vis.bin', mode: 'readwrite-unsafe', keep: 'vis' });
  await shared(a, 'connect', { a: 'A', b: 'B', tag: 'vis' });
  out.visibility = {};
  for (const size of [4096, 102400]) {
    out.visibility[size] = await call(a, 'visWriter', { key: 'vis', tag: 'vis', n: Q(2000, 200), size });
  }
  // Torn reads: a rewrites one region with alternating contents while b reads it.
  out.tornReads = {};
  const ms = Q(1500, 300);
  for (const size of [4096, 102400, 1048576]) {
    await startCall(a, 'tw', 'tornWriter', { key: 'vis', ms, size, at: 1 << 24 });
    await startCall(b, 'tr', 'tornReader', { key: 'vis:peer', ms, size, at: 1 << 24 });
    out.tornReads[size] = { writer: (await result(a, 'tw')).value, reader: (await result(b, 'tr')).value };
  }
  // Two writers on one region at once, a reader watching, then what is left.
  out.twoWriters = {};
  for (const size of [102400, 1048576]) {
    const trials = [];
    for (let t = 0; t < Q(5, 2); t++) {
      const at = (1 << 25) + t * (1 << 21);
      await startCall(a, 'w1', 'tornWriter', { key: 'vis', ms: 300, size, at, base: 0x10 });
      await startCall(c, 'w2', 'tornWriter', { key: 'vis', ms: 300, size, at, base: 0x81 });
      await startCall(b, 'r', 'tornReader', { key: 'vis:peer', ms: 300, size, at });
      const w1 = (await result(a, 'w1')).value;
      const w2 = (await result(c, 'w2')).value;
      const r = (await result(b, 'r')).value;
      const left = await call(b, 'checkUniform', { key: 'vis:peer', at, size });
      trials.push({ writes: [w1.writes, w2.writes], reads: r.reads, mixedReads: r.mixed, distinctBytesLeft: Object.keys(left).length });
    }
    out.twoWriters[size] = trials;
  }
  // Does b's flush write out what a wrote? Time it against a clean flush.
  const flush = { cleanFlushB: [], flushBAfterAWrote16M: [], flushAAfterAWrote16M: [], writeA16M: [] };
  for (let t = 0; t < Q(10, 2); t++) {
    await call(b, 'timeFlush', { key: 'vis:peer' });
    flush.cleanFlushB.push(await call(b, 'timeFlush', { key: 'vis:peer' }));
    flush.writeA16M.push(await call(a, 'writeDirty', { key: 'vis', bytes: 16 << 20, at: 1 << 26 }));
    flush.flushBAfterAWrote16M.push(await call(b, 'timeFlush', { key: 'vis:peer' }));
    await call(a, 'writeDirty', { key: 'vis', bytes: 16 << 20, at: 1 << 26 });
    flush.flushAAfterAWrote16M.push(await call(a, 'timeFlush', { key: 'vis' }));
  }
  out.flushCoverageMs = Object.fromEntries(Object.entries(flush).map(([k, v]) => [k, stats(v)]));
  for (const [p, k] of [[a, 'vis'], [b, 'vis:peer'], [c, 'vis']]) await call(p, 'closeHandle', { key: k });
  await call(a, 'remove', { file: 'vis.bin' });

  // Turns: two pages take a Web Lock in turn and append 4 KiB, opening and
  // closing the file each turn, or through a held readwrite-unsafe handle.
  out.turns = {};
  const tms = Q(2000, 300);
  for (const flushEach of [false, true]) {
    for (const perTurn of [true, false]) {
      for (const writers of [1, 2]) {
        const file = `turns-${Date.now()}.bin`;
        const ps = [a, b].slice(0, writers);
        if (!perTurn) for (const p of ps) await call(p, 'tryOpen', { file, mode: 'readwrite-unsafe', keep: 't' });
        await Promise.all(ps.map((p) => startCall(p, 'turns', 'turns', { file, ms: tms, lock: `turn/${file}`, perTurn, key: 't', flush: flushEach })));
        const rs = await Promise.all(ps.map((p) => result(p, 'turns').then((r) => r.value)));
        if (!perTurn) for (const p of ps) await call(p, 'closeHandle', { key: 't' });
        await call(a, 'remove', { file });
        const label = `${perTurn ? 'openClosePerTurn' : 'heldUnsafeHandle'}_${writers}writer${writers > 1 ? 's' : ''}${flushEach ? '_flush' : ''}`;
        out.turns[label] = {
          turnsPerSecond: Math.round(rs.reduce((s, r) => s + r.count, 0) / (tms / 1000)),
          errors: rs.reduce((s, r) => s + r.errors, 0),
          lastError: rs.map((r) => r.lastError).find(Boolean) || null,
          perWorker: rs.map((r) => ({ count: r.count, turn: r.turn, open: r.open, close: r.close, waited: r.waited })),
        };
      }
    }
  }
  await closeAll([a, b, c]);

  // Failover: how soon another page can open a file whose exclusive holder
  // went away.
  out.failover = {};
  for (const action of ['closeHandle', 'close', 'terminate', 'crash']) {
    const trials = [];
    for (let t = 0; t < Q(5, 2); t++) {
      const holder = await withWorker(ctx);
      const taker = await withWorker(ctx);
      const file = `failover-${Date.now()}.bin`;
      await call(holder, 'tryOpen', { file, keep: 'x' });
      await startCall(taker, 'o', 'openRetry', { file, timeoutMs: 10000 });
      await sleep(200);
      const t0 = Date.now();
      if (action === 'closeHandle') await call(holder, 'closeHandle', { key: 'x' });
      else if (action === 'close') await holder.close();
      else if (action === 'terminate') await holder.evaluate(() => spike.terminate('w'));
      else {
        const cdp = await ctx.newCDPSession(holder);
        cdp.send('Page.crash').catch(() => {});
      }
      const r = (await result(taker, 'o')).value;
      trials.push(r.at ? { openedAfterMs: r.at - t0, attempts: r.attempts } : { openedAfterMs: null, lastError: r.lastError });
      await call(taker, 'remove', { file });
      await closeAll([holder, taker]);
    }
    out.failover[action] = trials;
  }
  return out;
}

// Question 5: N writer pages, one reader page and, for the owner design, an
// owner page; a run of `ms`; optionally one writer page closed or crashed
// halfway and a new one started in its place.
async function proto(ctx, { design, writers, sync, ms, kill }) {
  const space = `${design}-${writers}-${sync ? 's' : 'n'}-${kill || 'none'}-${Date.now()}`;
  const file = `space-${space}.bin`;
  const progress = {};
  onReport = (r) => {
    if (r.kind === 'progress') progress[r.e] = Math.max(progress[r.e] || 0, r.committed);
  };
  const setup = async (id) => {
    const page = await withWorker(ctx, 'proto.js', 'p');
    await call(page, 'configure', { space, file }, 'p');
    await register(page, id, 'p');
    return page;
  };
  const pages = [];
  let owner = null;
  if (design === 'owner') {
    owner = await setup('owner');
    pages.push(owner);
    await call(owner, 'owner', { sync }, 'p');
  }
  const readerPage = await setup('reader');
  pages.push(readerPage);
  const writerPages = [];
  for (let i = 0; i < writers; i++) {
    const p = await setup(`writer${i}`);
    writerPages.push(p);
    pages.push(p);
  }
  if (owner) {
    await shared(owner, 'connect', { a: 'owner', b: 'reader', tag: 'owner' });
    for (let i = 0; i < writers; i++) await shared(owner, 'connect', { a: 'owner', b: `writer${i}`, tag: 'owner' });
  }
  await call(readerPage, 'run', { role: 'reader', design }, 'p');
  for (const p of writerPages) await call(p, 'run', { role: 'writer', design, sync }, 'p');
  const t0 = Date.now();
  let killed = null;
  if (kill) {
    await sleep(ms / 2);
    const victim = writerPages[writers - 1];
    const t = Date.now();
    if (kill === 'crash') {
      const cdp = await ctx.newCDPSession(victim);
      cdp.send('Page.crash').catch(() => {});
      await sleep(50);
    }
    await victim.close().catch(() => {});
    writerPages.pop();
    killed = { at: t - t0, progressBefore: { ...progress } };
    await sleep(100);
    const replacement = await setup(`writer${writers}`);
    if (owner) await shared(owner, 'connect', { a: 'owner', b: `writer${writers}`, tag: 'owner' });
    await call(replacement, 'run', { role: 'writer', design, sync }, 'p');
    writerPages.push(replacement);
    pages.push(replacement);
    await sleep(ms - (Date.now() - t0));
  } else {
    await sleep(ms);
  }
  const elapsed = Date.now() - t0;
  const writerResults = [];
  for (const p of writerPages) writerResults.push(await call(p, 'stop', {}, 'p'));
  const readerResult = await call(readerPage, 'stop', {}, 'p');
  const ownerResult = owner ? await call(owner, 'stopOwner', {}, 'p') : null;
  const verify = await call(readerPage, 'verify', {}, 'p');
  await call(readerPage, 'remove', { file }, 'p');
  await closeAll(pages);

  const committed = verify.extents.reduce((s, e) => s + e.records, 0);
  const badCommitted = verify.extents.reduce((s, e) => s + Object.values(e.bad).reduce((a, b) => a + b, 0), 0);
  const lost = [];
  for (const [e, acked] of Object.entries(progress)) {
    const ext = verify.extents.find((x) => x.e === Number(e));
    if (!ext || ext.records < acked) lost.push({ e: Number(e), acked, committed: ext ? ext.records : 0 });
  }
  const commits = writerResults.reduce((s, r) => s + (r.commits || 0), 0);
  const summary = {
    design,
    writers,
    sync,
    kill: kill || null,
    elapsedMs: elapsed,
    committedRecords: committed,
    commitsPerSecond: Math.round(committed / (elapsed / 1000)),
    commitLatencyUs: writerResults.map((r) => r.commit && { median: r.commit.median, p95: r.commit.p95 }),
    turnWaitUs: writerResults.map((r) => r.turnWait && { median: r.turnWait.median, p95: r.turnWait.p95 }),
    readerReads: readerResult.reads,
    readsPerSecond: Math.round(readerResult.reads / (elapsed / 1000)),
    recordReadUs: readerResult.recordRead,
    slotReadUs: readerResult.slotRead,
    check: {
      badCommittedRecords: badCommitted,
      readerBad: readerResult.bad,
      readerBadKinds: readerResult.badKinds,
      readerCellTorn: readerResult.cellTorn,
      readerCellBackwards: readerResult.cellBackwards,
      readerWalked: readerResult.walked,
      ackedButLost: lost,
      writerErrors: writerResults.reduce((s, r) => s + (r.errors || 0), 0),
      writerFailures: writerResults.filter((r) => r.failed).map((r) => r.failed),
      fullExtents: writerResults.filter((r) => r.full).length,
      cellValue: verify.cell.value,
      cellShortOfRecords: committed - verify.cell.value,
      pastEnd: verify.extents.map((e) => e.pastEnd),
      writerCommitsCounted: commits,
    },
    killed,
    adopted: writerResults.filter((r) => r.adoptedAt > 0).map((r) => ({ e: r.e, adoptedAt: r.adoptedAt })),
    owner: ownerResult,
    fileBytes: verify.size,
  };
  return summary;
}

async function q5(ctx) {
  const out = { runs: [] };
  const ms = Q(3000, 800);
  for (const sync of [false, true]) {
    for (const design of ['unsafe', 'owner']) {
      for (const writers of [1, 2, 4]) {
        console.log(`q5: ${design}, ${writers} writer(s), ${sync ? 'flush per commit' : 'no flush'}`);
        out.runs.push(await proto(ctx, { design, writers, sync, ms }));
      }
    }
  }
  for (const design of ['unsafe', 'owner']) {
    for (const kill of ['close', 'crash']) {
      console.log(`q5: ${design}, 4 writers, one page ${kill}d halfway`);
      out.runs.push(await proto(ctx, { design, writers: 4, sync: false, ms, kill }));
    }
  }
  return out;
}

// Round two. Every configuration gets a fresh persistent context (a new
// profile on disk), so no store sees another's files or caches, and the
// profile's IndexedDB and OPFS directories can be measured afterwards.
async function fresh() {
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'web-spike2-'));
  const ctx = await chromium.launchPersistentContext(profile);
  for (const p of ctx.pages()) await p.close().catch(() => {});
  const done = async () => {
    await ctx.close().catch(() => {});
    const sizes = profileSizes(profile);
    fs.rmSync(profile, { recursive: true, force: true });
    return sizes;
  };
  return { ctx, done };
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

// Bytes Chromium keeps for the origin: IndexedDB (LevelDB files and blob
// files) and the origin's file system (OPFS), after the context is closed.
function profileSizes(profile) {
  const d = path.join(profile, 'Default');
  const idb = path.join(d, 'IndexedDB');
  const out = { indexedDB: dirBytes(idb), fileSystem: dirBytes(path.join(d, 'File System')), leveldb: 0, blobs: 0, blobFiles: 0 };
  if (fs.existsSync(idb)) {
    for (const f of fs.readdirSync(idb)) {
      const p = path.join(idb, f);
      if (f.endsWith('.leveldb')) out.leveldb += dirBytes(p);
      else if (f.endsWith('.blob')) {
        out.blobs += dirBytes(p);
        const count = (q) => fs.readdirSync(q).reduce((n, g) => n + (fs.statSync(path.join(q, g)).isDirectory() ? count(path.join(q, g)) : 1), 0);
        out.blobFiles += count(p);
      } else out.other = (out.other || 0) + dirBytes(p);
    }
  }
  return out;
}

const dpage = async (ctx, swName) => {
  const page = await newPage(ctx);
  const id = await page.evaluate((n) => spike.dialog(n), swName);
  return { page, id };
};
const d = (page, cmd, a) => page.evaluate(([c, x]) => spike.d(c, x), [cmd, a]);
const hc = (page, cmd, a) => call(page, cmd, a, 'h');

function label(cfg) {
  const dur = cfg.design === 'idb' || cfg.design === 'pidb' ? cfg.durability : cfg.flush ? 'flush' : 'no flush';
  return `${cfg.ratio ? `format 1/${Math.round(1 / cfg.ratio)}, ` : ''}${cfg.design}${cfg.pipeline ? ' (pipelined)' : ''}${cfg.pageSize ? ` ${cfg.pageSize / 1024} KiB pages` : ''}${cfg.cachePages ? `, cache ${(cfg.cachePages * cfg.pageSize) / 2 ** 20} MiB` : ''}, ${dur}, ${cfg.pages} page(s)`;
}

const PRELOAD = () => ({ blocks: Q(3000, 600), chains: Q(150, 30) });

// Question 6: the workload in each tab's own dedicated worker, against each
// store, timed in that worker.
async function q6run(cfg) {
  console.log(`q6/q9: ${label(cfg)}`);
  const { ctx, done } = await fresh();
  const space = `s${Date.now().toString(36)}`;
  const file = `${space}.bin`;
  const swName = `q6-${space}`;
  const out = { cfg };
  const all = [];
  try {
    if (cfg.design === 'owner') {
      const o = await dpage(ctx, swName);
      all.push(o.page);
      await d(o.page, 'arrangeOwner', { space, file, flush: !!cfg.flush, mode: 'none' });
    }
    const hosts = [];
    for (let i = 0; i < cfg.pages; i++) {
      const page = await newPage(ctx);
      all.push(page);
      await page.evaluate(() => spike.spawn('h', 'hostworker.js'));
      if (cfg.design === 'owner') {
        await page.evaluate((n) => spike.dialog(n), swName);
        await page.evaluate(() => spike.linkHostToOwner('h'));
      }
      await hc(page, 'setup', { ...cfg, space, file, salt: 1000 + i });
      hosts.push(page);
    }
    out.preload = await hc(hosts[0], 'preload', PRELOAD());
    if (cfg.pages === 1) {
      const h = hosts[0];
      out.commits = await hc(h, 'commits', { n: Q(100, 20) });
      out.chains = [];
      const modes = cfg.design === 'idb' ? ['hop'] : ['hop', 'walk'];
      const colds = cfg.design === 'pidb' ? [false, true] : [false];
      for (const cold of colds) {
        for (const mode of modes) {
          for (const depth of [1, 4, 8]) out.chains.push(await hc(h, 'chains', { depth, mode, cold, n: Q(200, 30) }));
        }
      }
      out.gets = await hc(h, 'gets', { n: Q(400, 60) });
      out.contend = await hc(h, 'contend', { n: Q(30, 10) });
      out.batches = [];
      for (const cold of colds) out.batches.push(await hc(h, 'batches', { n: Q(100, 20), cold }));
    } else {
      const ledger = await hc(hosts[0], 'ledger', {});
      for (const h of hosts.slice(1)) await hc(h, 'loadLedger', ledger);
      out.mixed = await Promise.all(hosts.map((h, i) => hc(h, 'mixed', { ms: Q(3000, 800), cell: `head-${i}` })));
    }
    out.verify = [];
    for (const h of hosts) out.verify.push(await hc(h, 'verify', {}));
    out.status = await hc(hosts[0], 'status', {});
    out.logicalBytes = 0;
    out.storedBytes = 0;
    for (const h of hosts) {
      const st = await hc(h, 'status', {});
      out.logicalBytes += st.ledgerBytes;
      out.storedBytes += st.storedBytes;
    }
    const pre = out.status.storedBytes / out.status.ledgerBytes;
    out.logicalBytes -= (hosts.length - 1) * out.preload.bytes;
    out.storedBytes -= (hosts.length - 1) * out.preload.bytes * pre;
    out.estimate = await hc(hosts[0], 'estimate', {});
  } catch (e) {
    out.error = String(e);
    console.error('  ', out.error);
  }
  await closeAll(all);
  out.disk = await done();
  return out;
}

async function q6() {
  const runs = [];
  const one = [
    { design: 'idb', durability: 'default' },
    { design: 'idb', durability: 'relaxed' },
    { design: 'idb', durability: 'strict' },
    { design: 'idb', durability: 'relaxed', pipeline: true },
    { design: 'pidb', durability: 'relaxed', pageSize: 4096 },
    { design: 'pidb', durability: 'relaxed', pageSize: 32768 },
    { design: 'pidb', durability: 'strict', pageSize: 32768 },
    { design: 'pidb', durability: 'relaxed', pageSize: 65536 },
    { design: 'opfs', flush: false },
    { design: 'opfs', flush: true },
    { design: 'owner', flush: false },
    { design: 'owner', flush: true },
  ];
  for (const c of one) runs.push(await q6run({ ...c, pages: 1 }));
  const many = [
    { design: 'idb', durability: 'relaxed' },
    { design: 'idb', durability: 'strict' },
    { design: 'owner', flush: false },
    { design: 'owner', flush: true },
  ];
  for (const c of many) runs.push(await q6run({ ...c, pages: 3 }));
  return { runs };
}

// Question 9: format against medium. The same written dataset (same seeds,
// sizes and commits) stored as today's raw blocks and as a compact format's
// payload (storedSize in store.js, at a third and a twentieth of a node's
// bytes), in each medium: records per block in IndexedDB, pages in
// IndexedDB, an OPFS file in the tab's worker, an OPFS file behind an owner
// in another tab. Acknowledgements without a flush throughout (IndexedDB's
// default, which is relaxed in Chromium; OPFS unflushed).
async function q9() {
  const runs = [];
  for (const ratio of [1, 1 / 3, 1 / 20]) {
    for (const m of [
      { design: 'idb', durability: 'default' },
      { design: 'pidb', durability: 'relaxed', pageSize: 32768 },
      { design: 'opfs', flush: false },
      { design: 'owner', flush: false },
    ]) {
      runs.push(await q6run({ ...m, ratio, pages: 1 }));
    }
  }
  return { runs };
}

// Question 7: dialog in the shared worker, driven by 1 or 3 tabs, each
// operation timed in the tab (which adds the tab-to-shared-worker round
// trip, measured as `echo`) and, coarsely, inside the shared worker.
async function q7run(cfg) {
  console.log(`q7: ${label(cfg)}`);
  const { ctx, done } = await fresh();
  const space = `s${Date.now().toString(36)}`;
  const file = `${space}.bin`;
  const swName = `q7-${space}`;
  const out = { cfg };
  const tabs = [];
  try {
    for (let i = 0; i < cfg.pages; i++) tabs.push(await dpage(ctx, swName));
    const p0 = tabs[0].page;
    out.swEnv = await d(p0, 'swEnv', {});
    await d(p0, 'setup', { ...cfg, space, file, mode: 'none', salt: 1000 });
    out.preload = await d(p0, 'preload', PRELOAD());
    out.echo = await p0.evaluate((n) => spike.dEcho(n), Q(500, 50));
    if (cfg.pages === 1) {
      out.inner = {
        commits: await d(p0, 'commits', { n: Q(100, 20) }),
        chains: [],
      };
      for (const mode of cfg.design === 'idb' ? ['hop'] : ['hop', 'walk']) {
        for (const depth of [1, 4, 8]) out.inner.chains.push(await d(p0, 'chains', { depth, mode, n: Q(200, 30) }));
      }
    }
    out.drive = {};
    for (const mode of cfg.design === 'idb' ? ['hop'] : ['hop', 'walk']) {
      for (const t of tabs) await t.page.evaluate((a) => spike.driveStart(a), { ms: Q(3000, 800), mode });
      out.drive[mode] = [];
      for (const t of tabs) out.drive[mode].push(await t.page.evaluate(() => spike.driveResult()));
    }
    out.verify = await d(p0, 'verify', {});
    out.status = await d(p0, 'status', {});
    out.logicalBytes = out.status.ledgerBytes;
    out.storedBytes = out.status.storedBytes;
    out.estimate = await d(p0, 'estimate', {});
  } catch (e) {
    out.error = String(e);
    console.error('  ', out.error);
  }
  await closeAll(tabs.map((t) => t.page));
  out.disk = await done();
  return out;
}

async function q7() {
  const runs = [];
  const cfgs = [
    { design: 'idb', durability: 'relaxed' },
    { design: 'idb', durability: 'strict' },
    { design: 'pidb', durability: 'relaxed', pageSize: 32768 },
    { design: 'pidb', durability: 'strict', pageSize: 32768 },
    { design: 'pidb', durability: 'relaxed', pageSize: 32768, cachePages: 256 },
    { design: 'owner', flush: false },
    { design: 'owner', flush: true },
    { design: 'idb', durability: 'default', ratio: 1 / 20 },
    { design: 'pidb', durability: 'relaxed', pageSize: 32768, ratio: 1 / 20 },
    { design: 'owner', flush: false, ratio: 1 / 20 },
  ];
  for (const pages of [1, 3]) for (const c of cfgs) runs.push(await q7run({ ...c, pages }));
  return { runs };
}

// Question 8: the owner's tab goes while three tabs drive the shared
// worker. Times are from the runner's kill (wall clock, milliseconds).
async function q8run(cfg) {
  console.log(`q8: ${cfg.mode}, ${cfg.kill}${cfg.flush ? ', flush' : ''}`);
  const { ctx, done } = await fresh();
  const space = `s${Date.now().toString(36)}`;
  const file = `${space}.bin`;
  const swName = `q8-${space}`;
  const out = { cfg };
  const tabs = [];
  try {
    for (let i = 0; i < 3; i++) tabs.push(await dpage(ctx, swName));
    const p0 = tabs[0].page;
    await d(p0, 'setup', { design: 'owner', space, file, flush: !!cfg.flush, mode: cfg.mode, salt: 1000 });
    out.preload = await d(p0, 'preload', PRELOAD());
    if (cfg.mode === 'standby') await sleep(300);
    const ms = Q(6000, 3000);
    for (const t of tabs) await t.page.evaluate((a) => spike.driveStart(a), { ms, mode: 'hop' });
    await sleep(Q(2000, 1000));
    const before = await d(p0, 'failover', {});
    const ownerPage = before.events.filter((e) => e.kind === 'ready').pop().page;
    const victim = tabs.find((t) => t.id === ownerPage);
    out.ownerTab = tabs.indexOf(victim);
    const killAt = Date.now();
    if (cfg.kill === 'close') await victim.page.close();
    else if (cfg.kill === 'crash') {
      const cdp = await ctx.newCDPSession(victim.page);
      cdp.send('Page.crash').catch(() => {});
    } else await victim.page.evaluate(() => spike.terminateOwners());
    const survivors = cfg.kill === 'terminate' ? tabs : tabs.filter((t) => t !== victim);
    out.drive = [];
    for (const t of survivors) out.drive.push(await t.page.evaluate(() => spike.driveResult()));
    const ps = survivors[0].page;
    const f = await d(ps, 'failover', {});
    out.events = f.events.map((e) => ({ ...e, at: e.at - killAt }));
    out.link = f.link;
    out.pendingAfter = f.pending;
    out.verify = await d(ps, 'verify', {});
    out.status = await d(ps, 'status', {});
    const after = f.events.filter((e) => e.at > killAt);
    const at = (kind) => {
      const e = after.find((x) => x.kind === kind);
      return e ? e.at - killAt : null;
    };
    const ready = after.find((e) => e.kind === 'ready');
    out.failover = {
      detectedMs: cfg.mode === 'cold' ? at('detected') : null,
      spawnAskedMs: at('spawnAsked'),
      readyMs: at('ready'),
      firstServedMs: at('firstServed'),
      owner: ready && {
        lockedMs: ready.owner.locked - killAt,
        openedMs: ready.owner.opened - killAt,
        recoveredMs: ready.owner.recovered - killAt,
        scriptStartedMs: ready.owner.script - killAt,
        openAttempts: ready.attempts,
        recovery: ready.recovery,
      },
      clientGapMs: out.drive.map((r) => (r.gaps[0] ? r.gaps[0].ms : null)),
      clientGapSpansKill: out.drive.map((r) => (r.gaps[0] ? r.gaps[0].from <= killAt + 5 && r.gaps[0].from + r.gaps[0].ms >= killAt : null)),
    };
  } catch (e) {
    out.error = String(e);
    console.error('  ', out.error);
  }
  await closeAll(tabs.map((t) => t.page));
  out.disk = await done();
  return out;
}

async function q8() {
  const runs = [];
  const trials = Q(3, 1);
  for (const mode of ['cold', 'standby']) {
    for (const kill of ['close', 'crash', 'terminate']) {
      for (let i = 0; i < trials; i++) runs.push(await q8run({ mode, kill }));
    }
  }
  for (let i = 0; i < trials; i++) runs.push(await q8run({ mode: 'cold', kill: 'close', flush: true }));
  return { runs };
}

(async () => {
  const server = await start();
  base = `http://localhost:${server.address().port}`;
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'web-spike-profile-'));
  const ctx = await chromium.launchPersistentContext(profile);
  await ctx.exposeBinding('reportToRunner', (source, r) => onReport(r));
  const browser = await chromium.launch();
  const incognito = await browser.newContext();
  const probe = await newPage(ctx);
  const env = await probe.evaluate(() => spike.env());
  await probe.close();
  const results = {
    date: new Date().toISOString(),
    quick,
    browser: ctx.browser() ? ctx.browser().version() : browser.version(),
    env,
    machine: { platform: os.platform(), release: os.release(), cpus: os.cpus().length, cpuModel: os.cpus()[0].model, memGiB: Math.round(os.totalmem() / 2 ** 30) },
    profileDir: 'a fresh temporary directory under os.tmpdir()',
  };
  const t = Date.now();
  try {
    if (wanted('q1')) results.q1 = await q1(ctx, incognito);
    if (wanted('q2')) results.q2 = await q2(ctx);
    if (wanted('q3')) results.q3 = await q3(ctx);
    if (wanted('q4')) results.q4 = await q4(ctx);
    if (wanted('q5')) results.q5 = await q5(ctx);
    if (wanted('q6')) results.q6 = await q6();
    if (wanted('q7')) results.q7 = await q7();
    if (wanted('q8')) results.q8 = await q8();
    if (wanted('q9')) results.q9 = await q9();
  } finally {
    results.wallSeconds = Math.round((Date.now() - t) / 1000);
    const dir = path.join(__dirname, 'results');
    fs.mkdirSync(dir, { recursive: true });
    const stamp = new Date().toISOString().slice(0, 16).replace(':', '');
    const name = `${stamp}-chromium-${results.browser.split('.')[0]}${quick ? '-quick' : ''}${only.length ? '-' + only.join('-') : ''}.json`;
    fs.writeFileSync(path.join(dir, name), JSON.stringify(results, null, 2) + '\n');
    console.log(`wrote results/${name}`);
    await ctx.close();
    await browser.close();
    server.close();
    fs.rmSync(profile, { recursive: true, force: true });
  }
})();
