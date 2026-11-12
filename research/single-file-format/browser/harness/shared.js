// The shared worker: one per origin (per name), a registry of the dedicated
// workers the pages hand it ports to. It relays calls to them and connects
// two of them with a MessageChannel of their own, so they can talk without
// passing through it.

importScripts('common.js');

const workers = {};
const pages = [];

async function probe() {
  const out = {
    crossOriginIsolated: self.crossOriginIsolated,
    hasWorker: typeof Worker,
    hasLocks: typeof navigator.locks,
    hasStorage: typeof navigator.storage,
  };
  if (typeof Worker === 'function') {
    try {
      const w = new Worker('bench.js', { name: 'nested' });
      const call = endpoint(w, {});
      out.nested = await Promise.race([
        call('echo', 'nested').then((v) => `answered ${v}`),
        sleep(3000).then(() => 'no answer in 3 s'),
      ]);
      w.terminate();
    } catch (e) {
      out.nested = errorName(e);
    }
  } else {
    out.nested = 'Worker is not defined in SharedWorkerGlobalScope';
  }
  try {
    const root = await navigator.storage.getDirectory();
    const fh = await root.getFileHandle('shared-probe.bin', { create: true });
    out.hasCreateSyncAccessHandle = typeof fh.createSyncAccessHandle;
    if (typeof fh.createSyncAccessHandle === 'function') {
      try {
        const h = await fh.createSyncAccessHandle();
        h.close();
        out.syncHandle = 'opened';
      } catch (e) {
        out.syncHandle = errorName(e);
      }
    }
    const f = await fh.getFile();
    out.getFile = `ok, ${f.size} bytes`;
    await root.removeEntry('shared-probe.bin');
  } catch (e) {
    out.opfs = errorName(e);
  }
  try {
    out.lockCycle = await navigator.locks.request('probe/shared', async () => 'granted');
  } catch (e) {
    out.lockCycle = errorName(e);
  }
  return out;
}

function connect({ a, b, tag }) {
  if (!workers[a] || !workers[b]) throw new Error(`unknown worker ${a} or ${b}`);
  const channel = new MessageChannel();
  return Promise.all([
    workers[a]('peer', { tag, id: b }, [channel.port1]),
    workers[b]('peer', { tag, id: a }, [channel.port2]),
  ]);
}

const common = {
  hello: () => ({ pages: pages.length, workers: Object.keys(workers) }),
  echo: (x) => x,
  connect,
  registry: () => Object.keys(workers),
};

self.onconnect = (event) => {
  const port = event.ports[0];
  pages.push(port);
  endpoint(port, {
    ...common,
    probe,
    register: ({ id }, e) => {
      workers[id] = endpoint(e.ports[0], common);
      return Object.keys(workers);
    },
    relay: ({ to, args }) => workers[to]('echo', args),
    forget: ({ id }) => {
      delete workers[id];
      return true;
    },
  });
};
