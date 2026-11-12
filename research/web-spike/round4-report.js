// Prints a round-four results file as Markdown: every measure by round,
// E against A with the kill criteria the journal set before measuring, the
// no-host reads against A's, and the failovers.
//
//   node round4-report.js results/<file>-round4.json

const fs = require('fs');

const data = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

function stats(xs) {
  const s = [...(xs || [])].sort((a, b) => a - b);
  if (!s.length) return null;
  const at = (q) => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  return { n: s.length, p50: at(0.5), p95: at(0.95), p99: at(0.99), max: s[s.length - 1] };
}
const fmt = (x) => (x == null ? '–' : x >= 100 ? x.toFixed(0) : x >= 10 ? x.toFixed(1) : x.toFixed(2));
const MiB = 2 ** 20;

const named = (recs, names) => (recs || []).filter((r) => names.includes(r.name)).map((r) => r.ms);
const readers = (r, names) => (r.readerRecords || []).flatMap((recs) => named(recs, names.map((n) => `reader/${n}`)));
const measures = {
  commit: (r) => named(r.records, ['command']),
  'cold read': (r) => named(r.records, ['query/cold']),
  poll: (r) => named(r.records, ['poll']),
  'warm read (no storage: the hops alone)': (r) => named(r.records, ['query/warm']),
  pull: (r) => named(r.records, ['pull']),
  push: (r) => named(r.records, ['push']),
  'reader tabs, cold read': (r) => readers(r, ['query/cold']),
  'reader tabs, poll': (r) => readers(r, ['poll']),
  'read-only pass, cold read': (r) => named(r.records, ['post/query/cold']),
  'read-only pass, poll': (r) => named(r.records, ['post/poll']),
  'no host (getFile in the service worker), cold read': (r) => named(r.records, ['nohost/query/cold']),
  'no host (getFile in the service worker), poll': (r) => named(r.records, ['nohost/poll']),
};

const ok = data.runs.filter((r) => !r.error);
const steady = ok.filter((r) => !r.cfg.kill);
const ids = [...new Set(steady.map((r) => r.cfg.id))];
const rounds = [...new Set(ok.map((r) => r.round))];
const run = (id, round) => ok.find((r) => r.cfg.id === id && r.round === round);

console.log(`Chromium ${data.chromium}, trace ${data.trace}, ${data.cpus} CPUs, ${rounds.length} round(s), ${data.passes} read-only passes.\n`);
for (const r of data.runs.filter((x) => x.error)) console.log(`- ${r.cfg.id} round ${r.round + 1} failed: ${r.error.split('\n')[0]}`);

for (const [name, get] of Object.entries(measures)) {
  const rows = [];
  for (const id of ids) {
    const cells = rounds.map((round) => {
      const r = run(id, round);
      const s = r && stats(get(r));
      return s ? `${fmt(s.p50)} / ${fmt(s.p95)} / ${fmt(s.p99)}` : '–';
    });
    if (cells.every((c) => c === '–')) continue;
    rows.push(`| ${id} | ${cells.join(' | ')} |`);
  }
  if (!rows.length) continue;
  console.log(`\n**${name}**, ms in the requesting tab, p50 / p95 / p99, by round:\n`);
  console.log(`| design | ${rounds.map((r) => `round ${r + 1}`).join(' | ')} |`);
  console.log(`|---|${rounds.map(() => '---').join('|')}|`);
  console.log(rows.join('\n'));
}

console.log('\n**Bytes on disk** (MiB; after closing, then after reopening and idling):\n');
console.log(`| design | ${rounds.map((r) => `round ${r + 1}`).join(' | ')} | rebuilds the native files |`);
console.log(`|---|${rounds.map(() => '---').join('|')}|---|`);
for (const id of ids) {
  const cells = rounds.map((round) => {
    const r = run(id, round);
    if (!r || !r.disk) return '–';
    const after = r.diskAfterIdle ? ` → ${fmt(r.diskAfterIdle.total / MiB)}` : '';
    return `${fmt(r.disk.total / MiB)}${after}`;
  });
  const m = ok.filter((r) => r.cfg.id === id).map((r) => r.matchesNative);
  console.log(`| ${id} | ${cells.join(' | ')} | ${m.every((x) => x == null) ? 'n/a' : m.every(Boolean) ? 'yes' : 'NO'} |`);
}

// The kill criteria, against A at the same tabs and round. E and E3 are
// judged; the others are shown against the same bars.
const JUDGED = ['E', 'E3'];
const checks1 = ['commit', 'cold read', 'poll'];
const checks3 = [...checks1, 'reader tabs, cold read', 'reader tabs, poll'];
console.log('\n**Against A, same round and tabs** (ratio to A; kill: disk > 0.8x, or any p50 or p95 > 1.1x; E and E3 judged, the rest shown):\n');
console.log(`| design | round | disk | ${checks3.map((c) => `${c} p50 / p95`).join(' | ')} | commit p50 ≤ 5 ms | verdict |`);
console.log(`|---|---|---|${checks3.map(() => '---').join('|')}|---|---|`);
for (const id of ids) {
  for (const round of rounds) {
    const r = run(id, round);
    if (!r || r.cfg.mode === 'A') continue;
    const a = run(r.cfg.tabs > 1 ? 'A3' : 'A', round);
    if (!a) continue;
    const fail = [];
    const disk = r.disk.total / a.disk.total;
    if (disk > 0.8) fail.push('disk');
    const checks = r.cfg.tabs > 1 ? checks3 : checks1;
    const cells = checks3.map((c) => {
      if (!checks.includes(c)) return '–';
      const s = stats(measures[c](r));
      const b = stats(measures[c](a));
      if (!s || !b) return '–';
      const p50 = s.p50 / b.p50;
      const p95 = s.p95 / b.p95;
      if (p50 > 1.1 || p95 > 1.1) fail.push(c);
      return `${p50.toFixed(2)} / ${p95.toFixed(2)}`;
    });
    const c = stats(measures.commit(r));
    const verdict = fail.length ? `fails: ${fail.join(', ')}` : 'survives';
    console.log(`| ${id}${JUDGED.includes(id) ? '' : ' (shown)'} | ${round + 1} | ${disk.toFixed(2)} | ${cells.join(' | ')} | ${c ? (c.p50 <= 5 ? 'yes' : 'no') : '–'} | ${verdict} |`);
  }
}

console.log('\n**No host** (the service worker reading the file through getFile, a fresh File an operation, cache dropped at each cold read) against A\'s same read-only pass, same round:\n');
console.log('| round | cold read p50 / p95, ms (A) | ratio | poll p50 / p95, ms (A) | ratio |');
console.log('|---|---|---|---|---|');
for (const round of rounds) {
  const e = run('E', round);
  const a = run('A', round);
  if (!e || !a) continue;
  const row = [['no host (getFile in the service worker), cold read', 'read-only pass, cold read'], ['no host (getFile in the service worker), poll', 'read-only pass, poll']].map(([m, base]) => {
    const s = stats(measures[m](e));
    const b = stats(measures[base](a));
    if (!s || !b) return '– | –';
    return `${fmt(s.p50)} / ${fmt(s.p95)} (${fmt(b.p50)} / ${fmt(b.p95)}) | ${(s.p50 / b.p50).toFixed(2)} / ${(s.p95 / b.p95).toFixed(2)}`;
  });
  console.log(`| ${round + 1} | ${row.join(' | ')} |`);
}

// The failovers: what the writer tab saw from the loss on. The first
// answer that the replacement gave (a new host's epoch; after the service
// worker restarted, the first answer it gave), from the loss; the longest
// any request waited across it; and the parts of the replacement's start.
console.log('\n**Failovers** (writer tab, which never hosts; ms from the loss; "trace excluded" takes out the harness loading the trace, where a real worker loads dialog\'s Wasm):\n');
console.log('| run | round | first answer from the replacement | trace excluded | longest wait, trace excluded | lock granted | worker spawned | files opened (tries) | recovered | next 5 answers, ms | resent / late / fetch retries | service worker restarted | rebuilds native |');
console.log('|---|---|---|---|---|---|---|---|---|---|---|---|---|');
const failover = [];
for (const r of ok.filter((x) => x.cfg.kill)) {
  const kill = r.killAt;
  const recs = (r.records || []).slice().sort((x, y) => x.end - y.end);
  const before = recs.filter((x) => x.end <= kill);
  const oldEpoch = before.length ? before[before.length - 1].epoch : null;
  const after = recs.filter((x) => x.end > kill);
  let first = null;
  let traceMs = 0;
  let parts = ['–', '–', '–', '–'];
  const elected = (r.hostLogs || []).flat().filter((l) => l.granted && l.granted > kill);
  if (elected.length) {
    const h = elected[0];
    traceMs = h.start.traceMs;
    first = after.find((x) => x.epoch && x.epoch !== oldEpoch);
    parts = [fmt(h.granted - kill), fmt(h.spawnMs), `${fmt(h.start.openMs)} (${h.start.openAttempts})`, fmt(h.start.recoverMs)];
  } else if (r.cfg.mode === 'A') {
    const oldInstance = before.length ? before[before.length - 1].instance : null;
    first = after.find((x) => x.instance && x.instance !== oldInstance);
    if (first) {
      traceMs = first.init.traceMs;
      parts = ['–', '–', `IndexedDB ${fmt(first.init.openMs)}`, '–'];
    }
  } else {
    const re = (r.hostLogs || []).flat().filter((l) => l.announced === 'needHost' && l.at > kill);
    if (re.length) {
      const oldInstance = before.length ? before[before.length - 1].instance : null;
      first = after.find((x) => x.instance && x.instance !== oldInstance);
      parts = [`port again at ${fmt(re[0].at - kill)}`, '–', '–', '–'];
    }
  }
  if (!first) {
    console.log(`| ${r.cfg.id} | ${r.round + 1} | (no answer from a replacement found) |`);
    continue;
  }
  const at = first.end - kill;
  const window = after.filter((x) => x.end <= first.end + 3000);
  const longest = Math.max(...window.map((x) => x.end - Math.max(x.start, kill))) - traceMs;
  const k = after.indexOf(first);
  const next = after.slice(k + 1, k + 6).map((x) => fmt(x.ms)).join(', ');
  const c = (r.sw && r.sw.counts) || {};
  const retries = (r.records || []).reduce((n, x) => n + (x.retries || 0), 0);
  failover.push({ id: r.cfg.id, at: at - traceMs, longest });
  const lastInstance = before.length ? before[before.length - 1].instance : null;
  const restarted = after.some((x) => x.instance && x.instance !== lastInstance) ? 'yes' : 'no';
  console.log(`| ${r.cfg.id} | ${r.round + 1} | ${fmt(at)} | ${fmt(at - traceMs)} | ${fmt(longest)} | ${parts.join(' | ')} | ${next} | ${c.resent || 0} / ${c.late || 0} / ${retries} | ${restarted} | ${r.matchesNative == null ? 'n/a' : r.matchesNative ? 'yes' : 'NO'} |`);
}
const byId = {};
for (const f of failover) (byId[f.id] = byId[f.id] || []).push(f);
console.log('\n**Failover, trace excluded, range over rounds** (ms: first answer from the replacement; longest wait):\n');
for (const [id, xs] of Object.entries(byId)) {
  const a = xs.map((x) => x.at);
  const l = xs.map((x) => x.longest);
  console.log(`- ${id}: ${fmt(Math.min(...a))}–${fmt(Math.max(...a))}; ${fmt(Math.min(...l))}–${fmt(Math.max(...l))}`);
}

console.log('\n**What the replay did** (first round):\n');
for (const id of ids) {
  const r = run(id, rounds[0]);
  if (r && r.stats) console.log(`- ${id}: ${JSON.stringify(r.stats)}`);
  if (r && r.swAfter && r.swAfter.noHost) console.log(`- ${id}, no host: ${JSON.stringify(r.swAfter.noHost)}`);
}
