# Web spike: a space file on OPFS, many workers

A measured spike of the browser side of the file archive: what a sync
access handle, a MessagePort and a Web Lock cost in Chromium, whether one
shared worker can supervise dedicated workers, and whether the native
design's byte-range locks (the turn, extent owners, cells, presence) carry
over as Web Locks. The questions, numbers and conclusions are in
[journal.md](journal.md); the background is
[research/single-file-format/web-opfs.md](../single-file-format/web-opfs.md)
and [design/file-writers.md](../../design/file-writers.md).

## Running it

Needs Node and Playwright with its Chromium (`require('playwright')` must
resolve; nothing here installs it).

```sh
cd research/web-spike
node run.js                 # every question, about three minutes
node run.js q1 q4           # some questions
node run.js --quick         # few iterations, short runs: a smoke test
node report.js results/<file>.json   # the tables of a run, as Markdown
```

`run.js` starts `server.js` on an ephemeral port of `localhost` (a secure
context, so OPFS and Web Locks are available), launches Chromium with a
fresh profile directory under the system temporary directory, runs the
questions, writes `results/<date>T<time>-chromium-<major>[-quick][-qN].json`
and deletes the profile. Each experiment deletes the OPFS files it made;
question 5 writes up to about 430 MB into one file per run before deleting
it, so keep a gigabyte free.

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

What a run here cannot tell: Firefox and WebKit were not available, so
nothing is known from this spike about them; headless Chromium on a Linux
VM says nothing about real disks, macOS's `F_BARRIERFSYNC`, Windows,
mobile, or a background tab that the browser freezes or discards on its
own (the freeze here is the DevTools protocol's).
