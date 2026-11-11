// A dedicated worker running the workload (host.js) against one store: the
// placement where dialog runs in each tab's own worker, as it can today.
// Designs: `idb` (dialog's IndexedDB store), `pidb` (the paged file in
// IndexedDB), `opfs` (the OPFS file held by this worker's own sync handle:
// dialog in the owner) and `owner` (the OPFS file held by an owner worker
// in another page, reached over a port the shared worker passes on).

importScripts('common.js', 'store.js', 'host.js');

const link = new OwnerLink();
let linked = null;
const linkedReady = new Promise((r) => (linked = r));

async function openStore(cfg) {
  if (cfg.design === 'idb') return IdbArchive.open(cfg.space, cfg);
  if (cfg.design === 'pidb') return LogArchive.open(await PagedIdbFile.open(cfg.space, cfg), 'pidb');
  if (cfg.design === 'opfs') {
    const { handle } = await openExclusive(cfg.file);
    return LogArchive.open(new OpfsFile(handle, !!cfg.flush), 'opfs');
  }
  if (cfg.design === 'owner') {
    await linkedReady;
    return new RemoteArchive(link);
  }
  throw new Error(`no design ${cfg.design}`);
}

const { handlers } = makeHost(openStore);
handlers.hello = () => true;
handlers.ownerPort = (args, e) => {
  link.retarget(e.ports[0], 0);
  linked();
  return true;
};
endpoint(self, handlers);
