// Prints a round-three results file as Markdown tables, every design
// against A (today's store) in the same round, and applies the kill
// criteria the journal set before measuring.
//
//   node round3-report.js results/<file>-round3.json

const fs = require('fs');

const file = process.argv[2];
const data = JSON.parse(fs.readFileSync(file, 'utf8'));

function stats(xs) {
  const s = [...(xs || [])].sort((a, b) => a - b);
  if (!s.length) return null;
  const at = (q) => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  return { n: s.length, p50: at(0.5), p90: at(0.9), p95: at(0.95), p99: at(0.99), max: s[s.length - 1] };
}

const COMMIT = ['command', 'command/steady'];
const pick = (times, names) => names.flatMap((n) => times[n] || []);
const measures = {
  commit: (r) => pick(r.times, COMMIT),
  'cold read': (r) => pick(r.times, ['query/cold']),
  'warm read': (r) => pick(r.times, ['query/warm']),
  poll: (r) => pick(r.times, ['poll']),
  pull: (r) => pick(r.times, ['pull']),
  push: (r) => pick(r.times, ['push']),
  join: (r) => pick(r.times, ['pull/join']),
  'reader tabs, cold read': (r) => (r.readers || []).flatMap((t) => t['query/cold'] || []),
  'reader tabs, poll': (r) => (r.readers || []).flatMap((t) => t.poll || []),
};

const fmt = (x) => (x == null ? '–' : x >= 100 ? x.toFixed(0) : x >= 10 ? x.toFixed(1) : x.toFixed(2));
const MiB = 2 ** 20;
const ok = data.runs.filter((r) => !r.error);
const ids = [...new Set(ok.map((r) => r.cfg.id))];
const rounds = [...new Set(ok.map((r) => r.round))];
const run = (id, round) => ok.find((r) => r.cfg.id === id && r.round === round);
const baseOf = (cfg) => (cfg.tabs > 1 ? 'A3' : 'A');

console.log(`Chromium ${data.chromium}, trace ${data.trace}, ${data.cpus} CPUs, ${rounds.length} round(s).\n`);
for (const r of data.runs.filter((x) => x.error)) console.log(`- ${r.cfg.id} round ${r.round} failed: ${r.error.split('\n')[0]}`);

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
  console.log(`\n**${name}**, ms, p50 / p95 / p99, by round:\n`);
  console.log(`| design | ${rounds.map((r) => `round ${r + 1}`).join(' | ')} |`);
  console.log(`|---|${rounds.map(() => '---').join('|')}|`);
  console.log(rows.join('\n'));
}

console.log('\n**Bytes on disk** (MiB; after closing, then after reopening and idling):\n');
console.log(`| design | ${rounds.map((r) => `round ${r + 1}`).join(' | ')} | matches native |`);
console.log(`|---|${rounds.map(() => '---').join('|')}|---|`);
for (const id of ids) {
  const cells = rounds.map((round) => {
    const r = run(id, round);
    if (!r) return '–';
    const after = r.diskAfterIdle ? ` → ${fmt(r.diskAfterIdle.total / MiB)}` : '';
    return `${fmt(r.disk.total / MiB)}${after}`;
  });
  const m = ok.filter((r) => r.cfg.id === id).map((r) => r.matchesNative);
  console.log(`| ${id} | ${cells.join(' | ')} | ${m.every((x) => x == null) ? 'n/a' : m.every(Boolean) ? 'yes' : 'NO'} |`);
}

// The kill criteria, round by round, against A at the same number of tabs.
console.log('\n**Against A, same round and tabs** (ratio to A; kill: disk > 0.8x, or any of commit, cold read, poll p50 or p95 > 1.1x; the warm view reads touch no storage, see the journal):\n');
const checks = ['commit', 'cold read', 'poll'];
console.log(`| design | round | disk | ${checks.map((c) => `${c} p50 / p95`).join(' | ')} | commit p50 ≤ 5 ms | verdict |`);
console.log(`|---|---|---|${checks.map(() => '---').join('|')}|---|---|`);
for (const id of ids) {
  for (const round of rounds) {
    const r = run(id, round);
    if (!r || r.cfg.design === 'A') continue;
    const a = run(baseOf(r.cfg), round);
    if (!a) continue;
    const fail = [];
    const disk = r.disk.total / a.disk.total;
    if (disk > 0.8) fail.push('disk');
    const cells = checks.map((c) => {
      const s = stats(measures[c](r));
      const b = stats(measures[c](a));
      if (!s || !b) return '–';
      const p50 = s.p50 / b.p50;
      const p95 = s.p95 / b.p95;
      if (p50 > 1.1 || p95 > 1.1) fail.push(c);
      return `${p50.toFixed(2)} / ${p95.toFixed(2)}`;
    });
    const c = stats(measures.commit(r));
    console.log(`| ${id} | ${round + 1} | ${disk.toFixed(2)} | ${cells.join(' | ')} | ${c ? (c.p50 <= 5 ? 'yes' : 'no') : '–'} | ${fail.length ? `fails: ${fail.join(', ')}` : 'survives'} |`);
  }
}

// What each design did, from its stats, the first round.
console.log('\n**What the replay did** (first round):\n');
for (const id of ids) {
  const r = run(id, rounds[0]);
  if (r && r.stats) console.log(`- ${id}: ${JSON.stringify(r.stats)}`);
}
const t = ok[0] && ok[0].trace;
if (t) {
  console.log(`\n**The trace**: ${t.ops} operations, ${t.pages} pages caught, files ${JSON.stringify(t.files)}; ${(t.bytes / MiB).toFixed(1)} MiB.\n`);
  console.log('| operation | n | native p50 / p95 ms | file reads | KiB read | writes | KiB written | syncs | locks | block gets | block puts | KiB put | cell ops |');
  console.log('|---|---|---|---|---|---|---|---|---|---|---|---|---|');
  for (const [k, s] of Object.entries(t.by)) {
    const nm = s.nativeMs && s.nativeMs.n ? `${fmt(s.nativeMs.median)} / ${fmt(s.nativeMs.p95)}` : '–';
    const per = (x) => fmt(x / s.n);
    console.log(`| ${k} | ${s.n} | ${nm} | ${per(s.reads)} | ${per(s.readBytes / 1024)} | ${per(s.writes)} | ${per(s.writeBytes / 1024)} | ${per(s.syncs)} | ${per(s.locks)} | ${per(s.gets)} | ${per(s.puts)} | ${per(s.putBytes / 1024)} | ${per(s.cells)} |`);
  }
  console.log('\n(per operation, means)');
}
