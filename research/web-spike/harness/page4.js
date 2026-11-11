// Round four's tab: it registers the service worker, offers to host the
// dialog worker (design E) by waiting on the host lock, and makes the
// trace's requests, each one `fetch` of `/api/op` (or, design Ed, a message
// on a port to the host), timed here, in the tab that asks.

const spike4 = {
  tab: Math.random().toString(36).slice(2, 8),
  isHost: false,
  worker: null,
  call: null,
  epoch: null,
  release: null,
  hostLog: [],
  records: [],
  done: 0,
  stopReaders: false,
  direct: null,
};

spike4.init = async () => {
  await navigator.serviceWorker.register('/sw4.js', { scope: '/' });
  await navigator.serviceWorker.ready;
  if (!navigator.serviceWorker.controller) {
    await new Promise((r) => navigator.serviceWorker.addEventListener('controllerchange', r, { once: true }));
  }
  navigator.serviceWorker.addEventListener('message', (e) => {
    if (e.data && e.data.type === 'needHost' && spike4.isHost) spike4.announce('needHost');
  });
  return { isolated: self.crossOriginIsolated, controlled: !!navigator.serviceWorker.controller };
};

// A message to the service worker, answered on a port of its own.
spike4.sw = (msg, ports = []) =>
  new Promise((resolve, reject) => {
    const ch = new MessageChannel();
    ch.port1.onmessage = (e) => (e.data && e.data.error ? reject(new Error(e.data.error)) : resolve(e.data));
    const sw = navigator.serviceWorker.controller;
    if (!sw) return reject(new Error('not controlled'));
    sw.postMessage(msg, [ch.port2, ...ports]);
  });

// Gives the service worker a new port to this tab's dialog worker.
spike4.announce = async (why) => {
  const ch = new MessageChannel();
  await spike4.call('attach', {}, [ch.port1]);
  const t = abs();
  const r = await spike4.sw({ type: 'host', epoch: spike4.epoch }, [ch.port2]);
  spike4.hostLog.push({ announced: why, at: t, pendingThen: r.pending });
};

// Waits on the host lock; once granted, spawns the dialog worker, has it
// open (and recover) the files, and hands the service worker its port. The
// lock is held until the tab goes or hands the role off.
spike4.candidate = (cfg) => {
  navigator.locks.request('dialog-host', async () => {
    const granted = abs();
    let t = now();
    spike4.worker = new Worker('host4.js', { name: 'dialog' });
    spike4.call = endpoint(spike4.worker, {});
    await spike4.call('hello', {});
    const spawnMs = now() - t;
    spike4.epoch = `${spike4.tab}.${Date.now()}`;
    t = now();
    const start = await spike4.call('start', { ...cfg, epoch: spike4.epoch });
    const startMs = now() - t;
    spike4.isHost = true;
    await spike4.announce('elected');
    spike4.hostLog.push({ granted, spawnMs, startMs, start, ready: abs() });
    await new Promise((r) => (spike4.release = r));
  });
  return true;
};

// The tab is hidden with another visible: the host drains, closes its
// files and lets the lock go, and does not offer again. Headless Chromium
// cannot hide a tab, so the harness calls this as the `visibilitychange`
// handler would.
spike4.hidden = async () => {
  if (!spike4.isHost) return null;
  const t = now();
  const at = abs();
  const drained = await spike4.call('drain', {});
  spike4.worker.terminate();
  spike4.isHost = false;
  await spike4.sw({ type: 'nohost', epoch: spike4.epoch });
  spike4.release();
  const out = { at, handoffMs: now() - t, drained };
  spike4.hostLog.push({ handedOff: out });
  return out;
};
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') spike4.hidden();
});

spike4.hostCall = (cmd) => (spike4.isHost ? spike4.call(cmd, {}) : null);

// Design Ed: a port of this tab's own straight to the host.
spike4.connect = async () => {
  const ch = new MessageChannel();
  await spike4.sw({ type: 'connect' }, [ch.port2]);
  const waiting = new Map();
  ch.port1.onmessage = (e) => {
    const w = waiting.get(e.data.re);
    if (w) {
      waiting.delete(e.data.re);
      w(e.data);
    }
  };
  spike4.direct = (msg) =>
    new Promise((r) => {
      waiting.set(msg.id, r);
      ch.port1.postMessage(msg);
    });
  return true;
};

// Yields to the event loop without setTimeout's clamping.
const yieldNow = () =>
  new Promise((r) => {
    const c = new MessageChannel();
    c.port1.onmessage = () => r();
    c.port2.postMessage(0);
  });

let seq = 0;
// One request, retried when the fetch fails (the service worker stopped
// under it).
spike4.request = async (cfg, i, ro, cold) => {
  const id = `${spike4.tab}.${seq++}`;
  if (cfg.direct && spike4.direct) {
    const v = await spike4.direct({ id, i, ro, cold });
    return { v, retries: 0 };
  }
  const url = `/api/op?m=${cfg.mode}&i=${i}&ro=${ro ? 1 : 0}&c=${cold ? 1 : 0}&id=${id}&s=${cfg.space}&t=${encodeURIComponent(cfg.traceUrl)}`;
  let retries = 0;
  for (;;) {
    try {
      const res = await fetch(url);
      const v = await res.json();
      if (v.error) throw new Error(v.error);
      return { v, retries };
    } catch (e) {
      retries++;
      if (retries > 100) throw e;
      await sleep(1);
    }
  }
};

// Makes each request of `list` ([op, name, ro, cold]) in turn, timed.
spike4.replay = async (cfg, list, tag = '') => {
  for (const [i, name, ro, cold] of list) {
    const start = abs();
    const t = now();
    const { v, retries } = await spike4.request(cfg, i, ro, cold);
    const ms = now() - t;
    spike4.records.push({ i, name: tag + name, ms, start, end: abs(), epoch: v.epoch, instance: v.instance, init: v.init, noHost: v.noHost, retries, resent: v.resent });
    spike4.done++;
    if (spike4.done % 50 === 0) await yieldNow();
  }
  return spike4.done;
};

// Starts a replay without waiting for it (the failover runs watch `done`).
spike4.startReplay = (cfg, list) => {
  spike4.running = spike4.replay(cfg, list).then(
    () => ({ ok: true }),
    (e) => ({ ok: false, error: errorName(e) }),
  );
  return true;
};
spike4.finished = () => spike4.running;

// A reader tab: the list over and over until stopped.
spike4.readers = (cfg, list) => {
  spike4.stopReaders = false;
  spike4.reading = (async () => {
    let k = 0;
    while (!spike4.stopReaders) {
      const [i, name, ro, cold] = list[k++ % list.length];
      const start = abs();
      const t = now();
      const { v, retries } = await spike4.request(cfg, i, ro, cold);
      spike4.records.push({ i, name: `reader/${name}`, ms: now() - t, start, end: abs(), epoch: v.epoch, retries });
      await yieldNow();
    }
    return true;
  })().catch((e) => errorName(e));
  return true;
};
spike4.stop = async () => {
  spike4.stopReaders = true;
  return spike4.reading ? spike4.reading : true;
};

spike4.take = () => {
  const r = spike4.records;
  spike4.records = [];
  return r;
};
