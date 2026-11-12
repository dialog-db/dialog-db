// Renders a report's SVG charts to PNG, for showing them where Markdown
// does not draw charts (a conversation, a terminal).
//
//   node research/single-file-format/bench/render.js <charts dir> [<out dir>]
//
// Every `<name>.svg` in <charts dir> becomes `<name>.png` in <out dir>
// (default: <charts dir>), at twice its size. Needs Playwright and a
// Chromium it can launch.
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

(async () => {
  const [charts, out = charts] = process.argv.slice(2);
  if (!charts) {
    console.error('usage: render.js <charts dir> [<out dir>]');
    process.exit(2);
  }
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch();
  const page = await browser.newPage({ deviceScaleFactor: 2 });
  for (const name of fs.readdirSync(charts).filter((n) => n.endsWith('.svg'))) {
    const svg = fs.readFileSync(path.join(charts, name), 'utf8');
    const box = /viewBox="0 0 (\d+) (\d+)"/.exec(svg);
    const [width, height] = box ? [Number(box[1]), Number(box[2])] : [960, 460];
    await page.setViewportSize({ width, height });
    await page.setContent(`<body style="margin:0">${svg}</body>`);
    const target = path.join(out, name.replace(/\.svg$/, '.png'));
    await page.screenshot({ path: target });
    console.log(target);
  }
  await browser.close();
})();
