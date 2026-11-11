// The owner: a dedicated worker some page creates, holding the space's OPFS
// file with the exclusive sync access handle and serving the archive on
// every port it is given. It holds the Web Lock `space/<id>/owner` for its
// life, taken before it opens the file, so at most one owner opens it and
// whoever queues on the lock learns when the owner is gone.
//
// Messages to it: {cmd: 'start', space, file, flush, epoch, standby} with a
// port, and {cmd: 'port'} with another port to serve. On 'start' it queues
// on the lock (a standby waits there until the owner before it goes), opens
// the file (retrying while the dead owner's handle is still closing),
// recovers the archive from it, and posts {event: 'ready', timings} on its
// first port.

importScripts('common.js', 'store.js');

let archive = null;
const waiting = [];
let cfg = null;
let served = 0;
const t = { script: abs() };

function serve(port) {
  if (!archive) {
    waiting.push(port);
    return;
  }
  serveArchive(port, archive, cfg.epoch, () => {
    if (served++ === 0) t.firstServed = abs();
  });
}

self.onmessage = async (e) => {
  const m = e.data || {};
  if (m.cmd === 'port') return serve(e.ports[0]);
  if (m.cmd !== 'start') return;
  cfg = m;
  const control = e.ports[0];
  t.start = abs();
  try {
    await new Promise((locked) => {
      navigator.locks.request(`space/${cfg.space}/owner`, () => {
        locked();
        return new Promise(() => {});
      });
    });
    t.locked = abs();
    const { handle, attempts } = await openExclusive(cfg.file);
    t.opened = abs();
    archive = LogArchive.open(new OpfsFile(handle, !!cfg.flush), 'owner');
    t.recovered = abs();
    serve(control);
    for (const p of waiting.splice(0)) serve(p);
    control.postMessage({ event: 'ready', epoch: cfg.epoch, standby: !!cfg.standby, attempts, t, recovery: archive.recovery });
  } catch (err) {
    control.postMessage({ event: 'failed', epoch: cfg.epoch, error: errorName(err) });
  }
};
