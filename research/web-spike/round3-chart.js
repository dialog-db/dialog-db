// Draws round three's headline numbers as an SVG (and a PNG of it):
// commit p50, cold read p50, poll p50 and bytes on disk for each design at
// one tab, the median over a results file's rounds.
//
//   node round3-chart.js results/<file>-round3.json [more files] --out results/<name>

const fs = require('fs');
const args = process.argv.slice(2);
const outAt = args.indexOf('--out');
const out = args[outAt + 1];
const files = args.filter((_, i) => i !== outAt && i !== outAt + 1);
const runs = files.flatMap((f) => JSON.parse(fs.readFileSync(f, 'utf8')).runs.filter((r) => !r.error && r.cfg.tabs === 1));
const p50 = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
};
const ORDER = ['A', 'B16', 'B64', 'C', 'C0', 'D', 'Dk'];
const LABEL = { A: 'A today', B16: 'B 16K', B64: 'B 64K', C: 'C OPFS', C0: 'C no cache', D: 'D owner', Dk: 'D warm' };
const measures = [
  ['commit p50, ms', (r) => p50([...(r.times.command || []), ...(r.times['command/steady'] || [])])],
  ['cold read p50, ms', (r) => p50(r.times['query/cold'] || [])],
  ['poll p50, ms', (r) => p50(r.times.poll || [])],
  ['on disk, MiB', (r) => r.disk.total / 2 ** 20],
];
const ids = ORDER.filter((id) => runs.some((r) => r.cfg.id === id));
const value = (id, m) => p50(runs.filter((r) => r.cfg.id === id).map(m));
const W = 900;
const panelH = 190;
const H = measures.length * panelH + 40;
let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" font-family="sans-serif" font-size="12">`;
svg += `<rect width="${W}" height="${H}" fill="white"/>`;
svg += `<text x="10" y="20" font-size="14" font-weight="bold">Round three, one tab, median over rounds (trace tonk-1x-steady, Chromium 153)</text>`;
measures.forEach(([title, m], p) => {
  const top = 40 + p * panelH;
  const vals = ids.map((id) => value(id, m));
  const max = Math.max(...vals) * 1.15;
  const bw = (W - 160) / ids.length;
  svg += `<text x="10" y="${top + 14}" font-weight="bold">${title}</text>`;
  const base = top + panelH - 40;
  svg += `<line x1="120" y1="${base}" x2="${W - 20}" y2="${base}" stroke="#444"/>`;
  ids.forEach((id, i) => {
    const v = vals[i];
    const h = (v / max) * (panelH - 70);
    const x = 130 + i * bw;
    svg += `<rect x="${x}" y="${base - h}" width="${bw * 0.7}" height="${h}" fill="${id === 'A' ? '#888' : id.startsWith('C') ? '#2a7' : id.startsWith('B') ? '#37c' : '#c73'}"/>`;
    svg += `<text x="${x + bw * 0.35}" y="${base - h - 4}" text-anchor="middle">${v >= 10 ? v.toFixed(1) : v.toFixed(2)}</text>`;
    svg += `<text x="${x + bw * 0.35}" y="${base + 14}" text-anchor="middle">${LABEL[id] || id}</text>`;
  });
});
svg += '</svg>';
fs.writeFileSync(`${out}.svg`, svg);
(async () => {
  const { chromium } = require('playwright');
  const b = await chromium.launch();
  const page = await b.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: 2 });
  await page.setContent(`<html><body style="margin:0">${svg}</body></html>`);
  await page.screenshot({ path: `${out}.png` });
  await b.close();
  console.log(`wrote ${out}.svg and ${out}.png`);
})();
