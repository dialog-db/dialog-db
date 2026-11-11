// Prints the tables of a results file as Markdown.
//
//   node report.js results/2026-11-11-chromium-153.json

const fs = require('fs');

const d = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const out = [];
const p = (s = '') => out.push(s);
const table = (head, rows) => {
  p(`| ${head.join(' | ')} |`);
  p(`|${head.map((h, i) => (i ? '---:' : '---')).join('|')}|`);
  for (const r of rows) p(`| ${r.join(' | ')} |`);
  p();
};
const f = (x) => (x === undefined || x === null ? '' : typeof x === 'number' ? (x >= 100 ? Math.round(x) : Math.round(x * 10) / 10) : x);
const mp = (s) => (s ? [f(s.median), f(s.p95)] : ['', '']);

p(`Chromium ${d.browser}, ${d.machine.cpus} CPUs (${d.machine.cpuModel}), ${d.machine.platform} ${d.machine.release}, ${d.date}${d.quick ? ', quick' : ''}.`);
p();

if (d.q1) {
  p('### 1. Calls on a held sync access handle (microseconds)');
  p();
  p(`Timer step: ${d.q1.persistent.timerResolutionUs} us. "batch" is the time of 20 calls (5 for flush) divided by 20.`);
  p();
  const ops = Object.keys(d.q1.persistent.calls);
  const rows = ops.map((op) => {
    const row = [op];
    for (const prof of ['persistent', 'incognito']) {
      const c = d.q1[prof].calls[op];
      if (c.perCall) row.push(...mp(c.perCall), f(c.perBatch.median));
      else row.push(...mp(c), '');
    }
    return row;
  });
  table(['call', 'disk median', 'disk p95', 'disk batch median', 'incognito median', 'incognito p95', 'incognito batch median'], rows);
  if (d.q1.persistent.flushProbe) {
    p('Flush alone on a fresh file, disk profile (microseconds, median / p95):');
    p();
    const fp = d.q1.persistent.flushProbe;
    const keys = ['clean', 'overwrite4kThenFlush', 'append4kThenFlush', 'cleanAfterAppends', 'cleanAfterTruncate'];
    table(['mode, file', ...keys], fp.map((r) => [`${r.mode}, ${r.growMiB ? r.growMiB + ' MiB' : '4 KiB'}`, ...keys.map((k) => `${f(r[k].median)} / ${f(r[k].p95)}`)]));
  }
  p('Open and close (microseconds):');
  p();
  const orows = [];
  for (const prof of ['persistent', 'incognito']) {
    for (const [mode, r] of Object.entries(d.q1[prof].open)) {
      orows.push([`${prof}, ${mode}`, ...mp(r.getFileHandle), ...mp(r.createSyncAccessHandle), ...mp(r.close), ...mp(r.openPlusClose)]);
    }
  }
  table(['profile, mode', 'getFileHandle med', 'p95', 'createSyncAccessHandle med', 'p95', 'close med', 'p95', 'open+close med', 'p95'], orows);
}

if (d.q2) {
  p('### 2. Shared worker and round trips (microseconds)');
  p();
  p('```');
  p(JSON.stringify(d.q2.sharedWorkerProbe));
  p('```');
  p();
  table(['round trip', 'median', 'p95', 'p99'], Object.entries(d.q2.rtt).map(([k, v]) => [k, f(v.median), f(v.p95), f(v.p99)]));
}

if (d.q3) {
  const q = d.q3;
  p('### 3. Web Locks (microseconds)');
  p();
  table(
    ['lock call, in a worker', 'median', 'p95', 'p99'],
    Object.entries(q.uncontendedInWorker).map(([k, v]) => [k, f(v.median), f(v.p95), f(v.p99)]).concat([['exclusive, in the page', f(q.uncontendedInPage.median), f(q.uncontendedInPage.p95), f(q.uncontendedInPage.p99)]]),
  );
  table(
    ['contention', 'grants/s', 'hand-off median', 'p95', 'p99', 'same holder twice', 'FIFO violations'],
    Object.entries(q.contention).map(([k, v]) => [k, v.grantsPerSecond, f(v.handoffUs.median), f(v.handoffUs.p95), f(v.handoffUs.p99), v.sameHolderTwiceInARow, v.fifoViolations]),
  );
  table(['holder goes by', 'waiter granted after (ms), per trial'], Object.entries(q.release).map(([k, v]) => [k, v.releasedAfterMs.join(', ')]));
  p('Freeze (CDP `Page.setWebLifecycleState`):');
  p();
  p('```');
  p(JSON.stringify(q.freeze, null, 1));
  p('```');
  p();
  for (const k of ['presentLeaveTogether', 'presentLeaveTogetherInTurn']) {
    if (!q[k]) continue;
    p(`Present lock, 4 workers leaving at once${k.endsWith('InTurn') ? ', each in the turn' : ''}: ${JSON.stringify(q[k].workersThatThoughtTheyWereLast)} (workers that found the exclusive lock free: trials), ${q[k].trials} trials.`);
    p();
  }
}

if (d.q4) {
  const q = d.q4;
  p('### 4. Handles shared between workers');
  p();
  const short = (s) => (s === 'opened' ? 'opens' : s.split(':')[0]);
  table(
    ['held \\ second open', 'readwrite', 'readwrite-unsafe', 'read-only', 'getFile', 'createWritable'],
    Object.entries(q.matrix).map(([held, row]) => [
      held,
      ...['readwrite', 'readwrite-unsafe', 'read-only'].map((m) => `${short(row[m].otherPage)} (same worker: ${short(row[m].sameWorker)})`),
      q.othersWhileHeld[held].getFile,
      short(q.othersWhileHeld[held].createWritable),
    ]),
  );
  if (q.getFileWhileHeld) {
    const g = q.getFileWhileHeld;
    table(
      ['getFile() in another page while the exclusive handle is held', 'median (us)', 'p95'],
      [['getFile()', f(g.getFile.median), f(g.getFile.p95)], ['slice(4 KiB).arrayBuffer()', f(g.sliceRead.median), f(g.sliceRead.p95)], ['write, notify, getFile read', f(g.visibility.notifyAndCheck.median), f(g.visibility.notifyAndCheck.p95)]],
    );
    p(`Through getFile(): ${g.visibility.wrong} of ${g.visibility.n} reads after a write missed it; a File taken before a write and read after it: ${g.fileTakenBeforeAWriteReadAfterIt}.`);
    p();
  }
  p(`Three pages open readwrite-unsafe at once: ${q.threeUnsafeAtOnce.join(', ')}.`);
  p();
  table(['visibility, write then read in another page', 'writes', 'read wrong', 'notify+read median (us)', 'p95'], Object.entries(q.visibility).map(([k, v]) => [`${k} B`, v.n, v.wrong, f(v.notifyAndCheck.median), f(v.notifyAndCheck.p95)]));
  table(['torn reads, one writer rewriting', 'writes', 'reads', 'mixed reads', '%'], Object.entries(q.tornReads).map(([k, v]) => [`${k} B`, v.writer.writes, v.reader.reads, v.reader.mixed, f((100 * v.reader.mixed) / v.reader.reads)]));
  table(
    ['two writers on one region, 300 ms', 'writes (a, b)', 'reads', 'mixed reads', 'distinct bytes left'],
    Object.entries(q.twoWriters).flatMap(([k, trials]) => trials.map((t, i) => [`${k} B, trial ${i + 1}`, t.writes.join(', '), t.reads, t.mixedReads, t.distinctBytesLeft])),
  );
  table(['flush (ms)', 'median', 'p95'], Object.entries(q.flushCoverageMs).map(([k, v]) => [k, f(v.median), f(v.p95)]));
  table(
    ['turns, 4 KiB append each', 'turns/s', 'turn median (us)', 'turn p95', 'open median', 'close median', 'wait median'],
    Object.entries(q.turns).map(([k, v]) => [k, v.turnsPerSecond, f(v.perWorker[0].turn.median), f(v.perWorker[0].turn.p95), f(v.perWorker[0].open.median), f(v.perWorker[0].close.median), f(v.perWorker[0].waited.median)]),
  );
  table(['exclusive holder goes by', 'another page opens after (ms), per trial'], Object.entries(q.failover).map(([k, v]) => [k, v.map((t) => t.openedAfterMs).join(', ')]));
}

if (d.q5) {
  p('### 5. Prototype');
  p();
  table(
    ['design', 'writers', 'flush', 'kill', 'commits/s', 'commit median (us)', 'commit p95', 'reads/s', 'read median (us)', 'read p95', 'bad / lost / errors', 'cell short'],
    d.q5.runs.map((r) => {
      const c = r.check;
      const meds = r.commitLatencyUs.filter(Boolean);
      const med = meds.map((x) => x.median).sort((a, b) => a - b)[Math.floor(meds.length / 2)];
      const p95 = Math.max(...meds.map((x) => x.p95));
      return [
        r.design,
        r.writers,
        r.sync ? 'per commit' : 'none',
        r.kill || '',
        r.commitsPerSecond,
        f(med),
        f(p95),
        r.readsPerSecond,
        f(r.recordReadUs.median),
        f(r.recordReadUs.p95),
        `${c.badCommittedRecords + c.readerBad} / ${c.ackedButLost.length} / ${c.writerErrors + c.writerFailures.length}`,
        c.cellShortOfRecords,
      ];
    }),
  );
}

// Round two. Latencies are stored in microseconds; tables give milliseconds.
const ms = (x) => (x === undefined || x === null ? '' : x >= 10000 ? (x / 1000).toFixed(1) : (x / 1000).toFixed(2));
const mm = (s) => (s && s.n ? `${ms(s.median)} / ${ms(s.p95)}` : '');
const mb = (x) => (x / 1e6).toFixed(1);
const lbl = (c) => {
  const dur = c.design === 'idb' || c.design === 'pidb' ? c.durability : c.flush ? 'flush' : 'no flush';
  return `${c.ratio && c.ratio < 1 ? `1/${Math.round(1 / c.ratio)} ` : ''}${c.design}${c.pipeline ? ' pipelined' : ''}${c.pageSize ? ` ${c.pageSize / 1024}K` : ''}${c.cachePages ? ` cache ${(c.cachePages * c.pageSize) / 2 ** 20}M` : ''}, ${dur}`;
};
const diskCell = (r) => {
  if (!r.disk) return '';
  const total = r.disk.indexedDB + r.disk.fileSystem;
  const ratio = r.logicalBytes ? ` (${(total / r.logicalBytes).toFixed(2)}x)` : '';
  const stored = r.storedBytes && r.cfg.ratio && r.cfg.ratio < 1 ? `, payload ${mb(r.storedBytes)}` : '';
  return `${mb(total)} of ${r.logicalBytes ? mb(r.logicalBytes) : '?'}${ratio}${stored}${r.disk.blobFiles ? `, ${r.disk.blobFiles} blob files ${mb(r.disk.blobs)}` : ''}`;
};
const checks = (r) => {
  const bad = (r.verify ? [].concat(r.verify) : []).reduce((n, v) => n + v.bad.length, 0) + (r.status ? r.status.bad : 0);
  const read = (r.verify ? [].concat(r.verify) : []).reduce((n, v) => n + v.read, 0);
  const cells = (r.verify ? [].concat(r.verify) : []).flatMap((v) => Object.values(v.cells)).filter((x) => x !== 'ok').length;
  return `${read} read, ${bad} bad, ${cells} cells wrong${r.error ? `, error: ${r.error}` : ''}`;
};

if (d.q6) {
  const one = d.q6.runs.filter((r) => r.cfg.pages === 1);
  const many = d.q6.runs.filter((r) => r.cfg.pages > 1);
  p('### 6. The workload in a tab\'s own worker (ms, median / p95)');
  p();
  table(
    ['store', 'commit', 'import', 'publish', 'commits/s', 'MiB/s', 'preload MiB/s', 'on disk, MB of logical MB', 'checks'],
    one.map((r) => [lbl(r.cfg), mm(r.commits && r.commits.commitUs), mm(r.commits && r.commits.importUs), mm(r.commits && r.commits.publishUs), r.commits ? Math.round(r.commits.commitsPerSecond) : '', r.commits ? r.commits.mibPerSecond.toFixed(1) : '', r.preload ? r.preload.mibPerSecond.toFixed(1) : '', diskCell(r), checks(r)]),
  );
  table(
    ['store', 'get 1-8 KiB', 'get 64 KiB', '16 gets at once', '16 at once, cold', 'get during an import', 'that import', 'get answered first'],
    one.map((r) => [
      lbl(r.cfg),
      mm(r.gets && r.gets.smallUs),
      mm(r.gets && r.gets.largeUs),
      mm(r.batches && r.batches[0].us),
      r.batches && r.batches[1] ? mm(r.batches[1].us) : '',
      mm(r.contend && r.contend.getDuringImportUs),
      mm(r.contend && r.contend.importUs),
      r.contend ? `${r.contend.readAnsweredBeforeImport} of ${r.contend.n}` : '',
    ]),
  );
  const rows = [];
  for (const r of one) {
    if (!r.chains) continue;
    const groups = {};
    for (const c of r.chains) (groups[`${c.mode}${c.cold ? ', cold' : ''}`] = groups[`${c.mode}${c.cold ? ', cold' : ''}`] || {})[c.depth] = c;
    for (const [g, byDepth] of Object.entries(groups)) rows.push([lbl(r.cfg), g, ...[1, 4, 8].map((k) => mm(byDepth[k] && byDepth[k].us)), byDepth[8] ? ms(byDepth[8].meanUs) : '']);
  }
  table(['store', 'chain read', 'depth 1', 'depth 4', 'depth 8', 'depth 8 mean'], rows);
  if (many.length) {
    p('Three tabs at once, each its own worker running the mixed loop (a commit, chain reads of depth 1, 4, 8 and one random, a batch of 16):');
    p();
    table(
      ['store', 'commits/s, all tabs', 'commit, per tab', 'chain 1', 'chain 8', 'batch 16', 'on disk', 'checks'],
      many.map((r) => [
        lbl(r.cfg),
        r.mixed ? Math.round(r.mixed.reduce((a, m) => a + m.commitsPerSecond, 0)) : '',
        r.mixed ? r.mixed.map((m) => mm(m.commit)).join('; ') : '',
        r.mixed ? r.mixed.map((m) => ms(m.chain1.median)).join('; ') : '',
        r.mixed ? r.mixed.map((m) => ms(m.chain8.median)).join('; ') : '',
        r.mixed ? r.mixed.map((m) => ms(m.batch.median)).join('; ') : '',
        diskCell(r),
        checks(r),
      ]),
    );
  }
}

if (d.q7) {
  const one = d.q7.runs.filter((r) => r.cfg.pages === 1);
  const many = d.q7.runs.filter((r) => r.cfg.pages > 1);
  p('### 7. Dialog in the shared worker (ms, median / p95, timed in the tab)');
  p();
  if (one[0] && one[0].swEnv) p(`Shared worker: crossOriginIsolated ${one[0].swEnv.crossOriginIsolated}, timer step ${one[0].swEnv.timerStepUs} us, so "inside" numbers are coarse.`);
  p();
  const drows = [];
  for (const r of one) {
    for (const [mode, arr] of Object.entries(r.drive || {})) {
      const x = arr[0];
      drows.push([lbl(r.cfg), mode, ms(r.echo && r.echo.median), mm(x.lat.commit), mm(x.lat.chain1), mm(x.lat.chain4), mm(x.lat.chain8), mm(x.lat.batch), Math.round(x.lat.commit.n / x.seconds), x.errors]);
    }
  }
  table(['store', 'chain read', 'echo', 'commit', 'chain 1', 'chain 4', 'chain 8', 'batch 16', 'commits/s', 'errors'], drows);
  const irows = [];
  for (const r of one) {
    if (!r.inner) continue;
    const ch = {};
    for (const c of r.inner.chains) ch[`${c.mode}${c.depth}`] = c;
    irows.push([lbl(r.cfg), mm(r.inner.commits.commitUs), ms(r.inner.commits.commitUs.mean), Math.round(r.inner.commits.commitsPerSecond), ...['hop1', 'hop4', 'hop8', 'walk1', 'walk4', 'walk8'].map((k) => (ch[k] ? ms(ch[k].meanUs) : ''))]);
  }
  p('Inside the shared worker, one operation at a time (commit median / p95 and mean; chain means, from 100 us steps):');
  p();
  table(['store', 'commit', 'commit mean', 'commits/s', 'hop 1', 'hop 4', 'hop 8', 'walk 1', 'walk 4', 'walk 8'], irows);
  const mrows = [];
  for (const r of many) {
    for (const [mode, arr] of Object.entries(r.drive || {})) {
      const total = arr.reduce((a, x) => a + x.lat.commit.n / x.seconds, 0);
      mrows.push([lbl(r.cfg), mode, Math.round(total), arr.map((x) => ms(x.lat.commit.median)).join('; '), arr.map((x) => ms(x.lat.chain1.median)).join('; '), arr.map((x) => ms(x.lat.chain8.median)).join('; '), arr.map((x) => ms(x.lat.batch.median)).join('; '), arr.reduce((a, x) => a + x.errors, 0)]);
    }
  }
  p('Three tabs driving the shared worker at once (per tab: commit, chain 1, chain 8, batch medians):');
  p();
  table(['store', 'chain read', 'commits/s, all tabs', 'commit', 'chain 1', 'chain 8', 'batch 16', 'errors'], mrows);
  table(['store', 'tabs', 'on disk, MB of logical MB', 'checks'], d.q7.runs.map((r) => [lbl(r.cfg), r.cfg.pages, diskCell(r), checks(r)]));
}

if (d.q8) {
  p('### 8. Failover of the owner (ms after the runner closed, crashed or terminated it)');
  p();
  table(
    ['standby', 'owner goes by', 'flush', 'detected', 'new owner: script', 'lock', 'open (tries)', 'recovered (records, MB, scan ms)', 'ready', 'first served', 'longest gap per tab', 'resent / answered twice / errors', 'checks'],
    d.q8.runs.map((r) => {
      const f8 = r.failover || {};
      const o = f8.owner || {};
      const rec = o.recovery || {};
      return [
        r.cfg.mode,
        r.cfg.kill,
        r.cfg.flush ? 'yes' : 'no',
        f8.detectedMs !== null && f8.detectedMs !== undefined ? f(f8.detectedMs) : '',
        r.cfg.mode === 'standby' ? 'waiting' : f(o.scriptStartedMs),
        f(o.lockedMs),
        `${f(o.openedMs)} (${o.openAttempts})`,
        `${f(o.recoveredMs)} (${rec.records}, ${rec.logEnd ? mb(rec.logEnd) : ''}, ${f(rec.ms)})`,
        f(f8.readyMs),
        f(f8.firstServedMs),
        (f8.clientGapMs || []).map(f).join('; '),
        r.link ? `${r.link.resent} / ${r.link.unexpectedAnswers} / ${r.link.errors + (r.drive || []).reduce((a, x) => a + x.errors, 0)}` : '',
        checks(r),
      ];
    }),
  );
}

if (d.q9) {
  const runs = d.q9.runs;
  const base = runs.find((r) => r.cfg.design === 'idb' && (!r.cfg.ratio || r.cfg.ratio === 1));
  const disk = (r) => (r.disk ? r.disk.indexedDB + r.disk.fileSystem : NaN);
  const ch = (r, mode, depth) => r.chains && r.chains.find((c) => c.mode === mode && c.depth === depth && !c.cold);
  const best = (r, depth) => ch(r, 'walk', depth) || ch(r, 'hop', depth);
  const x = (a, b) => (a && b ? (a / b).toFixed(2) : '');
  p('### 9. Format against medium: one written dataset, today\'s layout = 1');
  p();
  p(`Today's layout (dialog's IndexedDB records, raw blocks): ${mb(disk(base))} MB on disk for ${mb(base.logicalBytes)} MB of blocks; estimate() ${mb(base.estimate.usage)} MB.`);
  p();
  table(
    ['format', 'medium', 'on disk MB', 'estimate MB', 'size x', 'commit p50 / p95 ms', 'x p50 / p95', 'chain 8 p50 / p95 ms', 'x p50 / p95', 'get 1-8 KiB p50 / p95', 'smaller and not slower'],
    runs.map((r) => {
      const c = r.commits ? r.commits.commitUs : null;
      const b = best(r, 8);
      const bb = best(base, 8);
      const sx = disk(r) / disk(base);
      const ok =
        c && b && sx <= 0.8 && c.median <= 1.1 * base.commits.commitUs.median && c.p95 <= 1.1 * base.commits.commitUs.p95 && b.us.median <= 1.1 * bb.us.median && b.us.p95 <= 1.1 * bb.us.p95;
      return [
        r.cfg.ratio && r.cfg.ratio < 1 ? `1/${Math.round(1 / r.cfg.ratio)}` : 'raw',
        `${r.cfg.design}${b && b.mode === 'walk' ? ' (walk)' : ''}`,
        mb(disk(r)),
        r.estimate ? mb(r.estimate.usage) : '',
        sx.toFixed(2),
        mm(c),
        c ? `${x(c.median, base.commits.commitUs.median)} / ${x(c.p95, base.commits.commitUs.p95)}` : '',
        mm(b && b.us),
        b ? `${x(b.us.median, bb.us.median)} / ${x(b.us.p95, bb.us.p95)}` : '',
        mm(r.gets && r.gets.smallUs),
        ok ? 'yes' : 'no',
      ];
    }),
  );
  table(['format', 'medium', 'chain 8 per hop p50 / p95', 'batch 16 p50 / p95', 'get during import', 'checks'], runs.map((r) => [r.cfg.ratio && r.cfg.ratio < 1 ? `1/${Math.round(1 / r.cfg.ratio)}` : 'raw', r.cfg.design, mm(ch(r, 'hop', 8) && ch(r, 'hop', 8).us), mm(r.batches && r.batches[0].us), mm(r.contend && r.contend.getDuringImportUs), checks(r)]));
}

console.log(out.join('\n'));
