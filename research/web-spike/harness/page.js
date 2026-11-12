// The page side: it starts dedicated workers and the shared worker, wires
// ports between them, and is what run.js drives through page.evaluate.

const spike = {
  workers: {},
  calls: {},
  shared: null,
  sharedCall: null,
  pageId: Math.random().toString(36).slice(2, 8),
  log: [],
};

spike.spawn = (name, script = 'bench.js') => {
  const worker = new Worker(script, { name });
  spike.workers[name] = worker;
  spike.calls[name] = endpoint(worker, {
    report: (r) => {
      spike.log.push(r);
      if (self.reportToRunner) self.reportToRunner(r);
      return true;
    },
  });
  return spike.calls[name]('hello', { name });
};

spike.call = (name, cmd, args) => spike.calls[name](cmd, args);
const rawCall = (name, cmd, args, transfer) => spike.calls[name](cmd, args, transfer);

// Starts a call without waiting for it; `result` collects it later.
spike.pending = {};
spike.start = (key, name, cmd, args) => {
  spike.pending[key] = spike.calls[name](cmd, args).then(
    (value) => ({ ok: true, value }),
    (e) => ({ ok: false, error: String(e) }),
  );
  return true;
};
spike.result = (key) => spike.pending[key];

spike.terminate = (name) => {
  spike.workers[name].terminate();
  delete spike.workers[name];
  return true;
};

spike.connectShared = () => {
  if (spike.shared) return true;
  spike.shared = new SharedWorker('shared.js', { name: 'spike' });
  spike.shared.onerror = (e) => spike.log.push({ sharedError: String(e.message || e) });
  spike.sharedCall = endpoint(spike.shared.port, {});
  spike.shared.port.start();
  return spike.sharedCall('hello', { page: spike.pageId });
};

spike.shareCall = (cmd, args) => spike.sharedCall(cmd, args);

// Gives the shared worker a port to this page's worker `name`, registered
// under `id`.
spike.register = async (name, id) => {
  const channel = new MessageChannel();
  await rawCall(name, 'attachShared', { id }, [channel.port1]);
  return spike.sharedCall('register', { id }, [channel.port2]);
};

// Round trips from the page: to its own worker, to the shared worker, and
// through the shared worker to a registered worker.
spike.rtt = async ({ target, n = 2000, warm = 200, via }) => {
  const samples = [];
  for (let i = 0; i < n + warm; i++) {
    const t = now();
    if (target === 'worker') await spike.calls[via]('echo', i);
    else if (target === 'shared') await spike.sharedCall('echo', i);
    else if (target === 'relay') await spike.sharedCall('relay', { to: via, args: i });
    if (i >= warm) samples.push(now() - t);
  }
  return us(summarize(samples));
};

// A lock held by the page's own thread.
spike.holdInPage = async (name) => {
  const held = await hold(name);
  spike.pageLock = held;
  return !!held;
};

spike.env = () => ({
  crossOriginIsolated: self.crossOriginIsolated,
  isSecureContext: self.isSecureContext,
  userAgent: navigator.userAgent,
  hardwareConcurrency: navigator.hardwareConcurrency,
  hasSharedWorker: typeof SharedWorker,
  hasLocks: typeof navigator.locks,
});

self.spike = spike;

// Round two: the dialog shared worker (dialogsw.js), owner workers this page
// creates when it asks, and a load loop that drives the shared worker and
// times each operation here, where the timer is fine.
spike.owners = [];

spike.dialog = async (swName) => {
  spike.dsw = new SharedWorker('dialogsw.js', { name: swName });
  spike.dsw.onerror = (e) => spike.log.push({ dialogError: String(e.message || e) });
  spike.dcall = endpoint(spike.dsw.port, {
    spawnOwner: (args) => spike.spawnOwner(args),
  });
  spike.dsw.port.start();
  // Held for the page's life: the shared worker reads which pages are alive
  // from the held locks.
  spike.pageLock = await hold(`page/${spike.pageId}`);
  await spike.dcall('hello', { pageId: spike.pageId });
  return spike.pageId;
};

spike.d = (cmd, args) => spike.dcall(cmd, args);

spike.spawnOwner = async (args) => {
  const w = new Worker('owner.js', { name: `owner-${args.epoch}` });
  spike.owners.push(w);
  const ch = new MessageChannel();
  w.postMessage({ cmd: 'start', ...args }, [ch.port1]);
  await spike.dcall('ownerLink', { epoch: args.epoch, standby: args.standby, pageId: spike.pageId }, [ch.port2]);
  return true;
};

spike.terminateOwners = () => {
  for (const w of spike.owners) w.terminate();
  const n = spike.owners.length;
  spike.owners = [];
  return n;
};

// Gives this page's host worker `name` a port to the owner, through the
// shared worker.
spike.linkHostToOwner = async (name) => {
  const ch = new MessageChannel();
  await rawCall(name, 'ownerPort', {}, [ch.port1]);
  await spike.dcall('forwardToOwner', {}, [ch.port2]);
  return true;
};

// A closed loop against the shared worker: a commit, chain reads of depth
// 1, 4 and 8 and one more of a random depth, and a batch of 16 gets. Each
// is timed here (`lat`) and inside the shared worker (`inner`). Runs until
// `ms` passes or driveStop is called; records the longest gaps between
// completions, which is what a tab sees of a failover.
spike.driveStart = ({ ms = 3000, mode = 'hop', cell = `head-${spike.pageId}` } = {}) => {
  const r = { lat: {}, inner: {}, errors: 0, errorSample: [], done: 0 };
  spike.driving = { stop: false, r };
  const kinds = [
    { kind: 'commit', key: 'commit' },
    { kind: 'chain', depth: 1, key: 'chain1' },
    { kind: 'chain', depth: 4, key: 'chain4' },
    { kind: 'chain', depth: 8, key: 'chain8' },
    { kind: 'chain', depth: 0, key: 'chainR' },
    { kind: 'batch', key: 'batch' },
  ];
  const gaps = [];
  spike.driving.done = (async () => {
    const t0 = now();
    let last = abs();
    r.start = last;
    while (!spike.driving.stop && now() - t0 < ms) {
      for (const k of kinds) {
        const depth = k.depth === 0 ? [1, 4, 8][Math.floor(Math.random() * 3)] : k.depth;
        const t = now();
        try {
          const inner = await spike.dcall('op', { kind: k.kind, depth, mode, cell });
          (r.lat[k.key] = r.lat[k.key] || []).push(now() - t);
          (r.inner[k.key] = r.inner[k.key] || []).push(inner);
          r.done++;
        } catch (e) {
          r.errors++;
          if (r.errorSample.length < 5) r.errorSample.push(String(e));
        }
        const at = abs();
        gaps.push([at - last, last]);
        last = at;
      }
    }
    r.seconds = (now() - t0) / 1000;
    r.end = abs();
    gaps.sort((a, b) => b[0] - a[0]);
    r.gaps = gaps.slice(0, 3).map(([ms, from]) => ({ ms, from }));
    for (const key of Object.keys(r.lat)) {
      r.lat[key] = us(summarize(r.lat[key]));
      r.inner[key] = us(summarize(r.inner[key]));
    }
    return r;
  })();
  return true;
};

spike.driveStop = async () => {
  spike.driving.stop = true;
  return spike.driving.done;
};

spike.driveResult = () => spike.driving.done;

// Round trips page to shared worker and back, as the floor under every
// operation the loop times.
spike.dEcho = async (n = 500) => {
  const xs = [];
  for (let i = 0; i < n + 50; i++) {
    const t = now();
    await spike.dcall('op', { kind: 'echo' });
    if (i >= 50) xs.push(now() - t);
  }
  return us(summarize(xs));
};
