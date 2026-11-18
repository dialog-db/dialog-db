const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ deviceScaleFactor: 2 });
  for (const svg of process.argv.slice(2)) {
    await page.goto('file://' + svg);
    const el = await page.$('svg');
    await el.screenshot({ path: svg.replace(/\.svg$/, '.png') });
  }
  await browser.close();
})();
