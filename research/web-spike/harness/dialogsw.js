// The shared worker where dialog would run: one per origin, reached by every
// tab. It runs the workload (host.js) against one store:
// - `idb` and `pidb` open IndexedDB here, in the shared worker itself;
// - `owner` cannot (a shared worker has no sync access handles and cannot
//   make workers, round one), so it asks a tab to create the owner worker
//   (owner.js), which hands it a port, and calls the archive over it.
//
// Failover, for `owner`: the owner holds `space/<id>/owner` for its life.
// mode `cold`: once the owner is ready this worker queues on that lock; when
// it is granted the owner is gone, so it lets go of the lock at once and
// asks a live tab (one holding its `page/<id>` lock, preferring another than
// the dead owner's) to create a new owner. mode `standby`: a second owner
// worker in another tab waits on the lock already loaded, and takes over by
// itself when it is granted; this worker then asks for a new standby.
// Either way requests in flight stay pending in the OwnerLink and are sent
// again to the new owner when it says it is ready.

importScripts('common.js', 'store.js', 'host.js');

const pages = new Map();
const link = new OwnerLink();
const sw = { cfg: null, epoch: 0, ownerPage: null, events: [], lastEpochServed: -1, spawns: 0 };
let firstReady = null;
let firstReadyPromise = new Promise((r) => (firstReady = r));

link.onAnswer = (data) => {
  if (data.epoch !== sw.lastEpochServed) {
    sw.lastEpochServed = data.epoch;
    sw.events.push({ kind: 'firstServed', epoch: data.epoch, at: abs() });
  }
};

async function alivePages() {
  const q = await navigator.locks.query();
  const held = new Set(q.held.map((l) => l.name));
  return [...pages.keys()].filter((id) => held.has(`page/${id}`));
}

async function spawn(epoch, standby, avoid) {
  const alive = await alivePages();
  const choice = alive.find((id) => id !== avoid) || alive[0];
  if (!choice) {
    sw.events.push({ kind: 'noPage', epoch, at: abs() });
    return;
  }
  sw.spawns++;
  sw.events.push({ kind: 'spawnAsked', epoch, standby, page: choice, at: abs() });
  const c = sw.cfg;
  try {
    await pages.get(choice)('spawnOwner', { space: c.space, file: c.file, flush: !!c.flush, epoch, standby });
  } catch (e) {
    sw.events.push({ kind: 'spawnFailed', epoch, page: choice, error: errorName(e), at: abs() });
  }
}

function watch(epoch, page) {
  navigator.locks.request(`space/${sw.cfg.space}/owner`, () => {
    sw.events.push({ kind: 'detected', epoch, at: abs() });
    link.port = null;
    spawn(epoch + 1, false, page);
  });
}

function onReady(port, m, page) {
  sw.events.push({ kind: 'ready', epoch: m.epoch, standby: m.standby, page, at: abs(), owner: m.t, attempts: m.attempts, recovery: m.recovery });
  sw.epoch = m.epoch;
  sw.ownerPage = page;
  link.retarget(port, m.epoch);
  firstReady();
  if (sw.cfg.mode === 'cold') watch(m.epoch, page);
  else if (sw.cfg.mode === 'standby') spawn(m.epoch + 1, true, page);
}

async function openStore(cfg) {
  sw.cfg = cfg;
  if (cfg.design === 'idb') return IdbArchive.open(cfg.space, cfg);
  if (cfg.design === 'pidb') return LogArchive.open(await PagedIdbFile.open(cfg.space, cfg), 'pidb');
  if (cfg.design === 'owner') {
    await spawn(0, false, null);
    await firstReadyPromise;
    return new RemoteArchive(link);
  }
  throw new Error(`no design ${cfg.design}`);
}

const { handlers } = makeHost(openStore);

Object.assign(handlers, {
  hello: ({ pageId }, e) => {
    pages.set(pageId, e.target.__call);
    return pages.size;
  },
  // A port from a page's new owner worker: it says when it is ready.
  ownerLink: ({ epoch, standby, pageId }, e) => {
    const port = e.ports[0];
    sw.events.push({ kind: 'linked', epoch, standby, page: pageId, at: abs() });
    port.onmessage = (ev) => {
      const m = ev.data || {};
      if (m.event === 'ready') onReady(port, m, pageId);
      else if (m.event === 'failed') sw.events.push({ kind: 'ownerFailed', epoch, error: m.error, at: abs() });
    };
    return true;
  },
  // Arranges an owner without running the workload here (question 6, where
  // the workload runs in the tabs' own workers).
  arrangeOwner: async (cfg) => {
    sw.cfg = cfg;
    await spawn(0, false, null);
    await firstReadyPromise;
    return true;
  },
  forwardToOwner: (args, e) => {
    link.port.postMessage({ cmd: 'port' }, [e.ports[0]]);
    return true;
  },
  failover: () => ({ events: sw.events, link: link.counts, pending: link.pending.size, epoch: sw.epoch, spawns: sw.spawns }),
  swEnv: () => ({ crossOriginIsolated: self.crossOriginIsolated, timerStepUs: timerStep() }),
});

function timerStep() {
  let smallest = Infinity;
  for (let i = 0; i < 200; i++) {
    const a = now();
    let b = now();
    while (b === a) b = now();
    smallest = Math.min(smallest, b - a);
  }
  return Math.round(smallest * 1e4) / 10;
}

self.onconnect = (event) => {
  const port = event.ports[0];
  port.__call = endpoint(port, handlers);
};
