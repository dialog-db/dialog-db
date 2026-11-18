// Runs bench.html in headless Chromium over the frames and JSON the
// `answers` harness dumped into ./data, and prints the timings as JSON.
//
//   node run.mjs stuff-2000:name,role:name,role issue-3000:title,status:this,ident,...
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");
const root = new URL(".", import.meta.url).pathname;
const types = { ".html": "text/html", ".js": "text/javascript" };

const server = createServer(async (request, response) => {
  try {
    const path = join(root, decodeURIComponent(new URL(request.url, "http://x").pathname));
    const body = await readFile(path);
    response.writeHead(200, { "content-type": types[extname(path)] ?? "application/octet-stream" });
    response.end(body);
  } catch {
    response.writeHead(404);
    response.end();
  }
}).listen(0, "127.0.0.1");
await new Promise((resolve) => server.once("listening", resolve));
const { port } = server.address();

const browser = await chromium.launch();
const page = await browser.newPage();
await page.goto(`http://127.0.0.1:${port}/bench.html`);
await page.waitForFunction(() => window.ready);
const results = {};
for (const spec of process.argv.slice(2)) {
  const [name, shown, fields] = spec.split(":");
  results[name] = await page.evaluate(
    ([name, shown, fields]) => window.measure(name, shown, fields),
    [name, shown.split(","), fields.split(",")],
  );
}
console.log(JSON.stringify(results, null, 2));
await browser.close();
server.close();
