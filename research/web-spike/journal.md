# Web spike: journal

What was tried, measured and decided, newest last. The questions come from
the "What applies to us" and "Open questions" sections of
[web-opfs.md](../single-file-format/web-opfs.md); the native design they
port is [design/file-writers.md](../../design/file-writers.md) and the end
of its [journal](../../design/file-writers/journal.md). How to run the
harness: [README.md](README.md). Raw numbers: `results/`, rendered with
`node report.js <file>`.

## Where things stand

**Status: Exploring** (a spike; nothing here is built into the archive).

- **One browser.** Headless Chromium 153.0.8010.12 on a 4-CPU Linux VM
  (Xeon 2.10 GHz, 16 GiB, ext4 on a virtio disk with online discard).
  Firefox and WebKit were not available: nothing below says anything
  about them, and I do not guess.
- **Sync access handle calls are cheap** on a disk-backed profile: 1–2 µs
  to read 4 KiB, 7–9 µs for 100 KiB, 12–40 µs to write 4 KiB. Opening a
  handle and closing it costs 0.7–0.9 ms with the file handle's lookup.
- **A shared worker cannot create workers or open sync handles** in
  Chromium: it can only broker ports. Each tab makes its own dedicated
  worker and hands the shared worker a port; once two workers have a
  direct port, a call between pages costs about 110 µs round trip.
- **Web Locks carry the native locks' semantics** (first in, first out;
  released 3–13 ms after a worker or page dies; a try that refuses) **but
  cost 120–155 µs a lock and unlock**, against about 1 µs for the native
  `fcntl` byte lock on the same machine.
- **`readwrite-unsafe` works as the native file does**: several pages hold
  the file at once, a write is visible to a read in another page as soon
  as it returns (0 misses in 4,000), and reads tear mid-write, so the
  checksums stay load-bearing.
- **The prototype lost nothing**: 64 runs, 492,809 committed records
  verified, a page closed or crashed mid-run in 16 of them, no record
  lost or torn.
- **Neither design wins outright.** Without flushes, one owner worker
  serving the others commits 1.7–3.6x faster than `readwrite-unsafe`
  workers taking Web Locks; `readwrite-unsafe` readers read 12–29x faster
  (15–20 µs against 230–440 µs) and do not queue behind commits.
- **Flush numbers here describe this VM, not Chromium**: `fdatasync` on
  its disk is sometimes 40–300 µs and sometimes 4 ms flat, natively too.

## 2026-11-11, the setup and what a Playwright benchmark gets wrong

**Question.** Can headless Chromium under Playwright measure OPFS at all,
and with what timer?

**Measured.** `performance.now()` steps by 100 µs in a page that is not
cross-origin isolated, 5 µs in one that is. The harness serves COOP and
COEP on every response, so pages and dedicated workers are isolated
(`crossOriginIsolated: true`, timer step measured at 5 µs). The shared
worker reports `crossOriginIsolated: false` under the same headers, so
nothing is timed inside it. Calls shorter than the step are also timed
in batches of 20 and divided.

**Surprise.** A context from Playwright's `browser.newContext()` is off
the record, and there the same calls cost 100–300 times more (table
below) and `flush()` costs nothing. Its OPFS is not on disk. A benchmark
of OPFS written the usual Playwright way measures that path. The harness
uses `launchPersistentContext` with a fresh profile directory for every
number except that comparison.

## 2026-11-11, question 1: calls on a held sync access handle

**Question.** What do `read`, `write`, `getSize` and `flush` cost on a
handle held in a dedicated worker, for 4 KiB pages and 100 KiB appends,
and what does `createSyncAccessHandle` plus `close` cost?

**Measured.** A 32 MiB file of random bytes, held open in a dedicated
worker; 2,000 calls of each kind at random 4 KiB-aligned offsets (the file
is in the page cache), appends at the end, 200 flushes. Two full runs
(`T1026`, `T1029`); the table gives both where they differ. "Batch" is
20 calls timed together and divided, which is the honest number below
the 5 µs step.

| call (µs) | disk, median | disk, p95 | disk, batch median | off the record, median | off the record, p95 |
|---|---:|---:|---:|---:|---:|
| read 4 KiB | 0 | 5 | 1.2–2.0 | 260–285 | 340–435 |
| read 100 KiB | 10 | 20 | 6.5–9.0 | 1035–1255 | 1325–1865 |
| write 4 KiB (overwrite) | 15–25 | 30–40 | 12.7–24.2 | 620–750 | 890–1130 |
| write 100 KiB (overwrite) | 25–35 | 45–55 | 26–44 | 925–1010 | 1150–1440 |
| append 4 KiB | 5–30 | 20–40 | 6.8–8.0 | 695–810 | 915–1875 |
| append 100 KiB | 35–45 | 65–70 | 41–58 | 920–1060 | 1255–1965 |
| getSize | 0 | 5 | 0.3–0.5 | 215–240 | 265–350 |
| flush, nothing written | 35–45 | 165–3995 | | 0 | 5 |
| write 4 KiB then flush | 245–560 | 430–3945 | | 0–5 | 5–10 |
| append 4 KiB and flush | 315–345 | 425–525 | | 680–705 | 850–1060 |

Opening (µs, median / p95, disk profile):

| mode | `getFileHandle` | `createSyncAccessHandle` | `close` | open + close |
|---|---:|---:|---:|---:|
| readwrite (exclusive) | 345–415 / 425–840 | 420–515 / 500–1185 | 275–355 / 340–685 | 695–875 / 825–2210 |
| readwrite-unsafe | 360–385 / 435–580 | 440–475 / 525–770 | 290–315 / 355–500 | 720–800 / 870–1220 |
| read-only | 360–385 / 480 | 445–475 / 580–635 | 295–310 / 400–470 | 745–790 / 950–1110 |

**Flush is `fdatasync`, and its cost here is the VM's.** `strace` on the
renderer shows the `DedicatedWorker` thread calling `fdatasync(fd)`
itself, once per `flush()`, so on Linux the flush is a direct system
call in the renderer, not a request to the browser process. Its time
on this VM is bimodal: in some runs 40 µs clean and 250–550 µs after a
4 KiB write; in others 4.0 ms flat (`strace` shows 3.5–3.9 ms inside
the call), once 4 ms in every flush of a run, and once commits stalling
for hundreds of milliseconds (question 5). Node calling `fdatasync` on
the same disk reproduces both modes (38–270 µs in one sitting, 4.0 ms
flat in another), and the kernel's tick is 250 Hz (4 ms). So the 4 ms is
the machine, not Chromium, and no flush number from this spike says
what a laptop's disk does.

**What it means.** A read through a held handle costs about what a
`memcpy` from the page cache costs plus a microsecond, so the absence of
`mmap` is a copy, not a round trip: a Wasm-side cache need not be large
for speed's sake. Opening per turn costs 0.7–0.9 ms, 400–700 reads' worth,
which is why handing an exclusive handle from worker to worker per turn
loses (question 4). Off-the-record windows are a different, slower and
non-durable store and should be treated as one.

## 2026-11-11, question 2: one shared worker and many dedicated workers

**Question.** Can a shared worker create dedicated workers in Chromium?
If not, does the pattern of each tab making its worker and handing the
shared worker a port work, and what do the round trips cost?

**Measured.** The shared worker's own probe:

| in `SharedWorkerGlobalScope` | result |
|---|---|
| `typeof Worker` | `undefined`: nested workers cannot be made |
| `createSyncAccessHandle` on a file handle | not defined: no sync access there |
| `getFile()` | works |
| `navigator.locks` | works |
| `crossOriginIsolated` | `false`, though its script is served with COOP and COEP |

Round trips of a small message (µs; two runs, 2,000 each after 200 to
warm):

| path | median | p95 | p99 |
|---|---:|---:|---:|
| page to its own dedicated worker | 45–55 | 75–85 | 130 |
| page to the shared worker | 105–120 | 150–195 | 245–335 |
| page to shared worker to a worker of the same page, and back | 210–260 | 345–510 | 530–810 |
| page to shared worker to a worker of another page, and back | 245–270 | 420–565 | 655–1130 |
| worker to worker, same page, over a port the shared worker made | 50–55 | 85–95 | 150–405 |
| worker to worker, two pages, same | 110 | 155–160 | 270–280 |
| the same after the shared worker forgot both | 110–115 | 155 | 220–245 |

**Inferred, from the crash tests below:** each page runs in a renderer of
its own (crashing one page with `Page.crash` left the reader and the
other writers running), so the two-page numbers cross processes.

**What it means.** "One SharedWorker plus dedicated workers" is viable
only as a broker: the shared worker is where workers find each other and
get a `MessageChannel` between them, and then it is out of the path (the
port keeps working after it forgets them). It cannot own the file, so
the owner of an exclusive handle is some tab's dedicated worker, and the
shared worker cannot respawn it when that tab goes; another tab's worker
has to take over, elected by a Web Lock. Relaying every call through the
shared worker doubles a round trip (about 250 µs against 110 µs), so it
should not sit on the data path. Not checked: whether Android Chrome and
Safari have `SharedWorker` (nothing here tests them); Web Locks and
`BroadcastChannel` could do the brokering without one, which this spike
did not try.

## 2026-11-11, question 3: Web Locks as byte-range locks

**Question.** What does a Web Lock cost uncontended (exclusive, shared,
and an `ifAvailable` try, the native non-blocking `F_SETLK`), how fast and
how fairly does it pass between workers in different pages, and is it
released when its holder's page closes or worker ends?

**Measured, uncontended** (µs, in a dedicated worker; five runs):

| call | median | p95 |
|---|---:|---:|
| `request(exclusive)`, callback returns at once | 120–155 | 165–205 |
| `request(shared)` | 120–150 | 165–205 |
| `ifAvailable`, lock free (granted) | 120–135 | 160–195 |
| `ifAvailable`, lock held (refused) | 95–115 | 130–175 |
| held as a pair: acquire | 120–130 | 160–190 |
| held as a pair: release until its request settles | 15–20 | 30–40 |
| `query()` | 85–100 | 135–165 |
| `request(exclusive)` in the page's main thread | 120–145 | 185–205 |
| native `fcntl(F_SETLK)` lock and unlock of one byte, same VM (Python) | 0.99 | 1.14 |

**Contention**: workers in 2–4 pages each take one exclusive lock in a
loop (1,000 rounds each); hand-off is the time from one holder's release
to the next holder's callback, across contexts on the wall clock (five
runs):

| pages, hold | grants/s | hand-off median (µs) | p95 | p99 | same holder twice in a row | grants out of request order |
|---|---:|---:|---:|---:|---:|---:|
| 2, 0 ms | 6,000–7,000 | 135–160 | 200–255 | 270–675 | 0–13 of 2,000 | 0–8 |
| 3, 0 ms | 5,600–6,700 | 135–160 | 200–255 | 280–600 | 0–2 of 3,000 | 1–6 |
| 3, 0.2 ms | 2,000–2,100 | 230–275 | 370–425 | 520–1,580 | 0 | 0–2 |
| 4, 0 ms | 5,400–6,500 | 140–165 | 205–280 | 275–415 | 0–5 of 4,000 | 1–31 |

"Out of order" counts a grant made while an older request (by more than
50 µs on the two contexts' clocks) was waiting; at most 0.8% of grants,
which I cannot tell from clock error between contexts. The holders take
turns round robin: the lock behaves first in, first out.

**Release when the holder goes** (ms from the runner's action to the
waiter's grant, 25 trials each over five runs; the runner's own call
costs 2–5 ms, the "release" row):

| holder goes by | granted after (ms) |
|---|---|
| releasing (baseline) | 2–5 |
| `worker.terminate()` | 2–4 |
| closing its page (lock held in the worker) | 7–13 |
| closing its page (lock held by the page's main thread) | 7–15 |
| renderer crash (`Page.crash`) | 6–13 |
| navigating its page away | 27–51 |

**Freezing and stealing.** With a worker holding a lock and sending a
heartbeat every 10 ms, the page was frozen with the DevTools protocol
(`Page.setWebLifecycleState: frozen`) for 1.5 s (the same in five runs):

| | result |
|---|---|
| a waiter granted while the holder's page was frozen | no |
| heartbeats from the frozen page's worker in 1.5 s | 146–147: the worker kept running |
| `steal: true` from a third page | granted in 3–4 ms |
| what the holder saw on unfreezing | its request rejected, `AbortError: Lock broken by another request with the 'steal' option.` |
| the original waiter | granted after the stealer finished |

**The present lock.** Four workers hold `present` shared; each leaves by
releasing it and trying it exclusive with `ifAvailable`, all at once. A
worker that is granted believes it is last.

| | trials where one worker was last | trials where two were |
|---|---:|---:|
| leaving without the turn (five runs) | 97 of 100 | 3 of 100 (all in one run) |
| leaving inside the turn, as the native close does (three runs) | 60 of 60 | 0 |

**Surprises.** A lock and unlock costs about 130 µs, 130 times the
native byte lock; I infer it is a round trip to the browser process,
where Chromium keeps the lock manager, but did not trace it. And a frozen page's
dedicated worker kept running under the protocol's freeze, so what was
frozen was the page, not its writer; how Chrome's own background-tab
freezing and discarding treat workers is not something headless can show.

**What it means.** Semantically the locks map: exclusive and shared
modes, a try that refuses, first-in-first-out hand-off, and release when
the worker, the page or the renderer dies, which is what the native
design relies on for a dead writer's extent. The native close order
(release `present`, try it exclusive, inside the turn) gives exactly one
last closer with Web Locks too; without the turn, two closers can each
see themselves last, one after the other, so the turn has to stay
around that step. What does not map is the cost: at 130 µs a lock, a
design that takes a lock per commit or per cell write is bounded near
3,000–7,000 operations a second by the locks alone, where native pays a
microsecond. And `steal` is not death: the robbed holder learns of it
only through a rejected promise and can go on writing through its
handle, so stealing a lock from a frozen tab is safe only if the stolen
holder checks before every write, which the native design never needed.

## 2026-11-11, question 4: one file, several handles

**Question.** With `readwrite-unsafe`, can workers in several pages hold
one file; is one handle's write visible to another's read as soon as it
returns; does one handle's flush cover another's writes? In the default
mode, does a second open fail, and what does handing the file over per
turn cost?

**The mode matrix**: a worker in page A holds the file in the row's mode;
page B (and A again) opens it in the column's mode, and B tries
`getFile()` and `createWritable()`:

| held \ opened | readwrite | readwrite-unsafe | read-only | `getFile()` | `createWritable()` |
|---|---|---|---|---|---|
| readwrite | refused | refused | refused | reads | refused |
| readwrite-unsafe | refused | opens | refused | reads | refused |
| read-only | refused | refused | opens | reads | refused |

"Refused" is `NoModificationAllowedError` ("Access Handles cannot be
created if there is another open Access Handle or Writable stream
associated with the same file"), the same in the holder's own worker.
Three pages held the file `readwrite-unsafe` at once. So readers cannot
use `read-only` beside `readwrite-unsafe` writers: they open
`readwrite-unsafe` too.

**Visibility.** A writes a block whose every byte names the round, then
tells B over a direct port; B reads at once and checks:

| block | rounds | B read something else | write, tell, read (µs, median / p95) |
|---|---:|---:|---:|
| 4 KiB | 2,000 ×2 runs | 0 | 120–140 / 195–220 |
| 100 KiB | 2,000 ×2 runs | 0 | 210–220 / 305–380 |

**Torn reads.** A rewrites one region as fast as it can, alternating
contents; B reads it as fast as it can and counts reads that hold both:

| region | A's writes | B's reads | reads mixing two writes |
|---|---:|---:|---:|
| 4 KiB | 126,000–259,000 | 96,000–195,000 | 0.5–2.2% |
| 100 KiB | 53,000–92,000 | 7,800–12,300 | 6.7–39% |
| 1 MiB | 7,500–10,700 | 720–880 | 1.7–29% |

With two writers rewriting one region at once and a reader watching (10
trials per size, 300 ms each), the reader saw mixed contents in every
trial, and what was left afterwards was always one writer's whole block
(one distinct byte value in 20 of 20).

**Flush coverage** (ms; ten trials, two runs):

| | median | p95 |
|---|---:|---:|
| B's flush, nothing written | 0.1–0.2 | 0.2–0.3 |
| A writes 16 MiB (no flush) | 10.7–14.7 | 17–21 |
| then B's flush | 18.2–25.1 | 20–27 |
| A writes 16 MiB, then A's own flush | 18.0–24.7 | 19.6–26.7 |

**Turns, exclusive against held.** One or two pages take a Web Lock in
turn and append 4 KiB in each, either opening the file exclusively and
closing it every turn, or through a `readwrite-unsafe` handle each keeps
open (2 s each; two runs):

| | turns/s | turn median (µs) | open median | close median |
|---|---:|---:|---:|---:|
| open and close per turn, 1 page | 620–800 | 1,040–1,100 | 445–515 | 295–305 |
| open and close per turn, 2 pages | 615–720 | 1,150–1,175 | 495–545 | 320–340 |
| held handle, 1 page | 3,100–6,100 | 10–20 | | |
| held handle, 2 pages | 3,100–5,400 | 20–30 | | |
| open and close per turn, flushed, 2 pages | 480–535 | 1,610–1,620 | 485–540 | 350–390 |
| held handle, flushed, 2 pages | 295–1,550 | 355–3,680 | | |

**Failover.** A page holds the file exclusively; another page's worker
retries `createSyncAccessHandle` in a loop. Time from the holder's
going to the other's open (ms, five trials, two runs):

| holder goes by | other page opens after (ms) |
|---|---|
| `close()` on the handle | 3–5 |
| `worker.terminate()` | 4–7 |
| closing its page | 10–29 |
| renderer crash | 10–15 |

**`getFile()` beside an exclusive handle.** While A holds the file
exclusively, B reads it through `getFile()` (µs, two runs):

| | median | p95 |
|---|---:|---:|
| `getFile()` | 305–445 | 420–555 |
| `slice(4 KiB).arrayBuffer()` | 930–1,190 | 1,435–3,615 |
| A writes, tells B, B reads through a fresh `getFile()` | 2,330–2,745 | 2,770–3,760 |

B saw every write (0 of 500 wrong). A `File` taken before A's write and
read after it fails with `NotReadableError`: a `File` is a snapshot that
dies at the next write, so each read takes a fresh one.

**What I measured and what I infer.** Measured: shared handles, immediate
visibility, torn reads, one writer's block left whole, B's flush taking as
long as A's own after A's writes. Inferred, not shown: that B's flush made
A's writes durable. It is consistent with both handles being one inode
and `fdatasync` writing back the inode's dirty pages whoever made them,
but nothing here cuts the power. Whether two writers' blocks can ever be
left interleaved is not settled by 20 trials; Linux serializes writes to
one inode, the specification promises nothing.

**What it means.** On Chromium, `readwrite-unsafe` behaves like the
native file: processes share it, see each other's writes at once, and
see torn writes in progress, which the native records' checksums and the
two rolling slots already handle. Handing an exclusive handle over per
turn costs a millisecond a turn and caps turns near 700 a second, five
times fewer than a held handle under the same Web Lock. `getFile()` is a
lock-free read path that works beside an exclusive owner, at about
1–3 ms a read: too slow for tree nodes, possibly enough for a cold open
or a sealed extent.

## 2026-11-11, question 5: a space file written by several pages

**Question.** One space file and N pages in one browser context, each with
a dedicated worker. Each writer appends checksummed records to its own
extent under that extent's Web Lock, publishes a slot under a short turn
lock, and updates a cell under its cell lock; a reader reads at once.
Commits per second and read latency for N = 1, 2, 4, for the
`readwrite-unsafe` single file and for one owner worker serving the
others; and is any record lost or torn, including when a page is closed
mid-run?

**The prototype** (`harness/proto.js`). The file holds two rolling slots
(each naming how many records every extent has committed, with a CRC),
memory cells from 8 KiB, and extents of 128 MiB from 64 KiB. A record is
4 KiB: magic, extent, sequence, writer, and a CRC-32 over the rest, which
repeats extent and sequence.

- **unsafe**: every worker opens the file `readwrite-unsafe` and holds
  `space/<id>/present` shared. A writer claims the first extent whose
  `space/<id>/extent/<n>` it can take with `ifAvailable` and holds it for
  its life; adopting an extent, it reads the newest slot and continues
  from that extent's committed count (overwriting anything a dead owner
  left unpublished). A commit appends one record (no lock), takes
  `space/<id>/turn`, reads the slots, checks the slot still names its own
  count, writes the other slot, and then takes `space/<id>/cell/0` to
  increment a counter cell. The reader takes no lock: it reads the
  slots, a random committed record, the counter cell, and walks every
  newly committed record.
- **owner**: one worker holds the exclusive handle (elected by
  `space/<id>/owner`) and does every read and write; writers keep claiming
  extents by Web Lock and send each record over a port the shared worker
  connected; the owner's commit handler is synchronous, so requests are
  the turn. The reader asks the owner for slots, records and the cell.
- **flush**: none, or per commit (after the record and after the slot,
  in both designs).
- **kill**: halfway through a 3 s run, the last writer's page is closed,
  or its renderer crashed, and a new page is started; its writer finds
  the dead page's extent free and adopts it.

After each run every worker stops, and a verifier opens the file
exclusively and checks every record the newest slot names, and that every
commit a writer had reported (every 100 ms, to the runner) is still named.

**Numbers**: four runs of each configuration (`T1026`, `T1029`, `T1031`,
`T1032`), each 3 s with one reader page.

| design | writers | flush | kill | commits/s, median (min–max) | commit median (µs) | reads/s, median (min–max) | read median (µs) |
|---|---:|---|---|---:|---:|---:|---:|
| unsafe | 1 | none | | 1,799 (1,243–1,964) | 490 | 23,913 (14,367–40,567) | 20 |
| unsafe | 2 | none | | 2,147 (1,586–2,575) | 792 | 35,133 (15,982–39,369) | 17.5 |
| unsafe | 4 | none | | 2,093 (1,684–2,827) | 1,597 | 27,826 (13,864–39,770) | 15 |
| owner | 1 | none | | 3,044 (2,446–3,714) | 287 | 1,434 (1,052–1,645) | 233 |
| owner | 2 | none | | 5,467 (3,629–6,138) | 323 | 1,127 (703–1,224) | 295 |
| owner | 4 | none | | 7,608 (6,103–8,904) | 460 | 721 (591–880) | 438 |
| unsafe | 1 | per commit | | 455 (292–790) | 1,292 | 35,158 (29,147–41,834) | 17.5 |
| unsafe | 2 | per commit | | 369 (287–1,074) | 4,212 | 34,983 (30,613–39,495) | 15 |
| unsafe | 4 | per commit | | 841 (623–990) | 3,982 | 37,165 (28,215–40,642) | 15 |
| owner | 1 | per commit | | 422 (280–842) | 1,687 | 206 (127–456) | 935 |
| owner | 2 | per commit | | 562 (133–1,119) | 2,272 | 129 (34–255) | 1,570 |
| owner | 4 | per commit | | 379 (7–1,319) | 7,565 | 39 (2–129) | 3,575 |
| unsafe | 4 | none | close | 2,109 (1,869–2,621) | 1,316 | 19,971 (15,965–36,848) | 20 |
| unsafe | 4 | none | crash | 1,831 (1,495–2,606) | 1,526 | 24,872 (18,886–31,860) | 17.5 |
| owner | 4 | none | close | 5,412 (4,531–8,099) | 583 | 526 (474–794) | 555 |
| owner | 4 | none | crash | 5,641 (4,170–7,911) | 581 | 531 (385–779) | 570 |

In the unsafe design a writer waited for the turn 145 µs (median) alone,
255 µs with two and 550 µs with four; the slot read takes 5 µs, against
165–335 µs through the owner.

**Checks**, over all 64 runs:

| check | result |
|---|---:|
| committed records verified after the runs | 492,809 |
| records read back wrong (by the verifier or the reader during runs) | 0 |
| random reads during runs | 2,925,604 |
| committed records the unsafe reader walked as they were committed | 143,507 |
| commits reported to the runner and later missing | 0 |
| a slot found naming another count for a writer's own extent | 0 |
| torn cell reads, or a cell read going backwards | 0 |
| counter cell short of the records committed | by 1, in 2 of 8 crash runs (unsafe) |
| extents adopted from a closed or crashed page | 16 of 16, continuing at the committed count |

The cell short by one is a writer crashed between publishing its slot
and updating the cell: the cell is not part of the commit, here as in the
native design.

**What surprised me.** The owner wins on writes, by 1.7x with one writer
and 3.6x with four, because an unsafe commit takes two Web Locks (about
2 × 140 µs before any waiting) where an owner commit takes one round trip
(110–250 µs), and the owner serializes for free. And the unsafe design
does not scale with writers: its commits are bound by the turn's Web Lock
hand-off (150–170 µs), not by the disk. The four-writer runs also put
five or six busy threads on four CPUs, which is in the spread.

**What it means.** On writes without flushes, an owner worker beats Web
Locks taken per commit. On reads, `readwrite-unsafe` is 12–29x faster and
readers never wait for writers, while the owner's reads slow as writers
are added (233 to 438 µs median, 1,434 to 721 reads a second) since they
queue behind commits. With a flush per commit, the flush decides both
and this VM's flush is not a measurement (one owner run fell to 7 commits
a second on stalled flushes). Batching would change the unsafe numbers:
taking the turn once per batch of records, as the native design takes it
once per integration rather than per commit, removes most of the lock
cost; this spike did not measure that.

## 2026-11-11, what it means for porting the native design

**Which locks map onto Web Locks.**

| native lock | Web Lock | maps? |
|---|---|---|
| the writer's turn (exclusive byte) | `space/<id>/turn`, exclusive | yes: FIFO, released on death in 3–13 ms; 130 µs a take, so per integration, not per commit |
| an extent's owner (exclusive byte, held while it is one's own; a try finds a dead owner's) | `space/<id>/extent/<n>`, exclusive, held for the worker's life; `ifAvailable` to find a free or dead one | yes, cleanly: the case Web Locks do best; adoption from closed and crashed pages worked in every run |
| a reader's shared lock on an extent it reads | the same name, shared | yes, at 130 µs a take |
| a cell's lock (per-cell byte, compare-and-swap) | `space/<id>/cell/<name>`, exclusive | in meaning; at 130 µs per cell write it is the cost that bounds a commit |
| present (shared while open; exclusive try at the last close) | `space/<id>/present`, shared, then `ifAvailable` exclusive | only inside the turn, as native does it: there 60 of 60 trials gave exactly one last closer; outside it, two |
| a process's locks dying with it | a worker's or page's locks dying with it | yes, but `steal` is not death: a robbed holder keeps its handle and must check before it writes |

What has no counterpart: the native locks are free next to an I/O, Web
Locks cost about one 4 KiB read per 100. And the native design's locks
belong to a process; a Web Lock belongs to the worker or page that asked,
so two workers of one tab exclude each other, which is simpler than POSIX
(no in-process mutexes needed for that).

**Is "one SharedWorker plus dedicated workers" viable?** Yes, as a
broker and nothing more. In Chromium 153 a shared worker cannot create
workers or open a sync access handle, so each tab creates its dedicated
worker and gives the shared worker a port; the shared worker connects
workers to one another and to an owner with `MessageChannel`s and then
stays off the data path (a direct cross-page round trip is about 110 µs,
one through the shared worker about 250 µs). Readers can run concurrently
in several workers either way: with `readwrite-unsafe` each reads the file
itself; with an owner they can read through `getFile()` at 1–3 ms, or ask
the owner at 0.2–0.5 ms.

**Does the `readwrite-unsafe` single file beat an owner worker?** Not
outright:

- Reads: `readwrite-unsafe` by 12–29x, and readers never queue behind
  writers.
- Writes taking Web Locks per commit: the owner, by 1.7–3.6x.
- Portability: the owner works with the standard's exclusive handle;
  `readwrite-unsafe` is Chromium only.

My reading, an inference from these numbers: the portable design is an
owner worker elected by a Web Lock and reached over ports a shared worker
brokers, which is the native writer with an in-process turn. On Chromium,
`readwrite-unsafe` can be added behind a feature check for readers (every
tab reads the file itself, as native readers do) and for appends to a
tab's own extent, provided the turn is taken per integration, not per
commit, as the native design already does. Measuring that hybrid, and
batched turns, is the next step.

**What this spike cannot tell.**

- Firefox and Safari: not available here. Outside Chromium the exclusive
  handle is the only mode (per web-opfs.md), and none of these numbers
  carry over.
- Real disks: flush times here are a virtual disk's, bimodal and stalling
  natively too; macOS (`F_BARRIERFSYNC`), Windows and mobile flash are
  not measured.
- Durability: nothing cut the power; "B's flush covers A's writes" is
  inferred from timing.
- Background tabs: the freeze here is the DevTools protocol's, which left
  the worker running. Chrome's own freezing of hidden tabs, tab discarding
  and memory-pressure kills, and iOS backgrounding, were not observed.
- Headless rendering and four CPUs: the four-writer runs oversubscribe
  the machine; on more cores the unsafe readers and the owner may both do
  better.
- Wasm: the prototype is plain JavaScript; the archive in Wasm would add
  copies into linear memory and its own CPU cost per commit.
