const { chromium } = require('playwright');
(async () => {
  const [, , file, outdir, theme, width] = process.argv;
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: +width, height: 900 }, deviceScaleFactor: 1 });
  await page.addInitScript(t => { localStorage.setItem('mdbook-theme', t); }, theme);
  await page.goto('file://' + file);
  await page.waitForTimeout(500);
  const report = await page.evaluate(() => {
    const out = [];
    document.querySelectorAll('figure.dg svg').forEach((svg, i) => {
      const vb = svg.viewBox.baseVal;
      const label = (svg.getAttribute('aria-label') || '').slice(0, 40);
      const texts = [...svg.querySelectorAll('text')].filter(t => t.textContent.trim());
      const boxes = texts.map(t => ({ t: t.textContent, b: t.getBBox() }));
      for (const { t, b } of boxes) {
        if (b.x < -0.5 || b.y < -0.5 || b.x + b.width > vb.width + 0.5 || b.y + b.height > vb.height + 0.5) {
          out.push(`fig ${i} (${label}): clipped "${t.slice(0, 50)}" x ${b.x.toFixed(0)}..${(b.x + b.width).toFixed(0)} of ${vb.width}, y ${b.y.toFixed(0)}..${(b.y + b.height).toFixed(0)} of ${vb.height}`);
        }
      }
      for (let a = 0; a < boxes.length; a++) {
        for (let c = a + 1; c < boxes.length; c++) {
          const p = boxes[a].b, q = boxes[c].b;
          const ix = Math.min(p.x + p.width, q.x + q.width) - Math.max(p.x, q.x);
          const iy = Math.min(p.y + p.height, q.y + q.height) - Math.max(p.y, q.y);
          if (ix > 1.5 && iy > 2) out.push(`fig ${i}: overlap "${boxes[a].t.slice(0, 30)}" / "${boxes[c].t.slice(0, 30)}" (${ix.toFixed(0)}x${iy.toFixed(0)})`);
        }
      }
      const rects = [...svg.querySelectorAll('rect')].map(r => r.getBBox());
      for (const { t, b } of boxes) {
        const cx = b.x + b.width / 2, cy = b.y + b.height / 2;
        const inside = rects.filter(r => cx > r.x && cx < r.x + r.width && cy > r.y && cy < r.y + r.height);
        if (!inside.length) continue;
        const r = inside.reduce((m, r) => (r.width * r.height < m.width * m.height ? r : m));
        if (b.x < r.x - 1 || b.x + b.width > r.x + r.width + 1) out.push(`fig ${i}: "${t.slice(0, 40)}" wider than its box (${b.width.toFixed(0)} > ${r.width.toFixed(0)})`);
      }
    });
    out.push('page scrollWidth ' + document.documentElement.scrollWidth + ' vs ' + window.innerWidth);
    return out;
  });
  console.log(report.join('\n'));
  const figs = await page.$$('figure.dg');
  for (let i = 0; i < figs.length; i++) {
    await figs[i].screenshot({ path: `${outdir}/${theme}-${width}-fig${String(i).padStart(2, '0')}.png` });
  }
  await browser.close();
})();
