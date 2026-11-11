// A static server for the harness on http://localhost, a secure context, so
// OPFS and Web Locks are available. Every response carries COOP and COEP, so
// pages and workers are cross-origin isolated and `performance.now()` has its
// fine resolution (5 microseconds in Chromium, against 100 without). Paths
// under /plain/ are served without them, to check what isolation changes.

const http = require('http');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, 'harness');
const TYPES = { '.html': 'text/html', '.js': 'text/javascript' };

function start(port = 0) {
  const server = http.createServer((req, res) => {
    let url = new URL(req.url, 'http://localhost').pathname;
    let isolated = true;
    if (url.startsWith('/plain/')) {
      isolated = false;
      url = url.slice('/plain'.length);
    }
    if (url === '/') url = '/index.html';
    // Round three's traces, from the directory SPIKE_TRACES names (they are
    // made outside the repository).
    let root = ROOT;
    if (url.startsWith('/trace/') && process.env.SPIKE_TRACES) {
      root = path.resolve(process.env.SPIKE_TRACES);
      url = url.slice('/trace'.length);
    }
    const file = path.join(root, path.normalize(url));
    if (!file.startsWith(root) || !fs.existsSync(file)) {
      res.writeHead(404);
      res.end();
      return;
    }
    const headers = {
      'content-type': TYPES[path.extname(file)] || 'application/octet-stream',
      'cache-control': 'no-store',
    };
    if (isolated) {
      headers['cross-origin-opener-policy'] = 'same-origin';
      headers['cross-origin-embedder-policy'] = 'require-corp';
    }
    res.writeHead(200, headers);
    fs.createReadStream(file).pipe(res);
  });
  return new Promise((resolve) => {
    server.listen(port, '127.0.0.1', () => resolve(server));
  });
}

module.exports = { start };

if (require.main === module) {
  start(Number(process.argv[2] || 8123)).then((s) =>
    console.log(`http://localhost:${s.address().port}/`),
  );
}
