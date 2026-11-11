# Web spike: a space file on OPFS, many workers

A measured spike of the browser side of the file archive. Round one: what a
sync access handle, a MessagePort and a Web Lock cost in Chromium, whether
one shared worker can supervise dedicated workers, and whether the native
design's byte-range locks (the turn, extent owners, cells, presence) carry
over as Web Locks. Round two: dialog's IndexedDB store, a paged file in
IndexedDB and the OPFS file measured against each other on one tonk-shaped
workload, what synchronous reads are worth to a chain of dependent
lookups, and dialog in a shared worker served by an owner worker that is
replaced when its tab goes. The questions, numbers and conclusions are in
[journal.md](journal.md); the background is
[research/single-file-format/web-opfs.md](../single-file-format/web-opfs.md)
and [design/file-writers.md](../../design/file-writers.md).

## Running it

Needs Node and Playwright with its Chromium (`require('playwright')` must
resolve; nothing here installs it).

```sh
cd research/web-spike
node run.js                 # every question, about ten minutes
node run.js q1 q4           # some questions
node run.js q6 q7 q8 q9     # round two only, about seven minutes
node run.js --quick         # few iterations, short runs: a smoke test
node report.js results/<file>.json   # the tables of a run, as Markdown
```

`run.js` starts `server.js` on an ephemeral port of `localhost` (a secure
context, so OPFS and Web Locks are available), launches Chromium with a
fresh profile directory under the system temporary directory, runs the
questions, writes `results/<date>T<time>-chromium-<major>[-quick][-qN].json`
and deletes the profile; questions 6 to 8 launch a fresh profile for every
configuration and measure its IndexedDB and File System directories after
closing it. Each experiment deletes the OPFS files it made;
question 5 writes up to about 430 MB into one file per run before deleting
it, and a round-two configuration up to about 400 MB into its profile, so
keep a gigabyte free.

Every response carries COOP and COEP, so pages and dedicated workers are
cross-origin isolated and `performance.now()` steps by 5 microseconds
instead of 100. Paths under `/plain/` are served without them. The shared
worker is never isolated (see the journal), so nothing is timed there.

Chromium runs from a persistent context (`launchPersistentContext`), a
profile on disk. A context from `browser.newContext()` is off the record:
each sync-handle call there costs two orders of magnitude more and
`flush()` is free, which fits Chromium keeping an off-the-record OPFS in
memory behind the browser process (inferred, not traced). Question 1 runs in both to show it; nothing else uses the
off-the-record context.

## Layout

| file | what |
|---|---|
| `run.js` | drives Playwright: one function per question, results as JSON |
| `report.js` | prints a results file's tables as Markdown |
| `server.js` | the static server |
| `harness/common.js` | stats, CRC-32, a request/response endpoint over any port, a Web Lock held until released |
| `harness/page.js` | the page: spawns workers and the shared worker, wires ports, is what `run.js` calls |
| `harness/shared.js` | the shared worker: a registry of dedicated workers, relays calls, connects two workers by a MessageChannel |
| `harness/bench.js` | dedicated worker for questions 1 to 4 |
| `harness/proto.js` | dedicated worker for question 5: writer, reader, owner and verifier of a space file |
| `harness/store.js` | round two's stores behind one interface: dialog's IndexedDB layout, the log archive over an OPFS handle or over pages in IndexedDB, the owner client that retries across a failover, and the owner's server loop |
| `harness/host.js` | round two's workload and its ledger and checks, run where dialog would run |
| `harness/hostworker.js` | a tab's dedicated worker running the workload (question 6) |
| `harness/dialogsw.js` | the shared worker running the workload (questions 7 and 8), arranging owners and failing over |
| `harness/owner.js` | the owner worker: takes the owner lock, opens the OPFS file exclusively, recovers it, serves the archive |
| `results/` | the runs the journal cites |

## The questions

1. Per-call cost on a held `FileSystemSyncAccessHandle`; open latency.
2. Nested workers in a shared worker; round trips over MessagePorts.
3. Web Locks: cost, hand-off under contention, release when a holder dies,
   freezing, stealing, and the "present" lock's last-closer test.
4. Handles shared between pages: the mode matrix, visibility, torn reads,
   flush coverage, hand-off per turn, failover, `getFile()` alongside.
5. The prototype: N writer pages, a reader page and (for the owner design)
   an owner page on one space file; commits/s, read latency, and a check
   that no committed record is lost or torn, with a page closed or crashed
   halfway.
6. Round two, the stores in a tab's own worker: dialog's IndexedDB store,
   the paged file in IndexedDB, the OPFS file held by that worker, and an
   owner worker in another tab; commits, single gets, batches, chains of
   depth 1, 4 and 8 read per hop or walked where the file is, a read during
   a write, three tabs at once, and bytes on disk.
7. The same with dialog in the shared worker, driven by one or three tabs.
8. Failover: the owner's tab closed, crashed or its worker terminated while
   three tabs drive the shared worker, with a cold replacement or a standby
   waiting on the owner lock; every acknowledged write checked afterwards.
9. Format against medium: one written dataset stored as today's raw
   blocks and as an emulated compact format's payload (1/3 and 1/20 of the
   bytes), as IndexedDB records, as pages in IndexedDB and in an OPFS
   file; size and latency against today's layout.

What a run here cannot tell: Firefox and WebKit were not available, so
nothing is known from this spike about them; headless Chromium on a Linux
VM says nothing about real disks, macOS's `F_BARRIERFSYNC`, Windows,
mobile, or a background tab that the browser freezes or discards on its
own (the freeze here is the DevTools protocol's).
