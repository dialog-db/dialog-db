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
