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

console.log(out.join('\n'));
