# A single-file space on the web: what SQLite on OPFS has taught

Sources read on 2026-11-11. The question: could one OPFS file per space
replace the IndexedDB block store on the web, the way `archive.dialog`
holds a space natively, and what does the community's experience with
SQLite (and similar engines) in the browser say about how?

The short answer is that every working design so far treats the browser's
file lock as the hard constraint and builds around it in one of three
ways: one dedicated worker owns the file and everyone else talks to it;
each connection opens and closes the file in turn; or, on Chromium only,
every connection opens the file at once with `readwrite-unsafe` and does
its own locking with Web Locks. The fastest designs that allow concurrent
readers and a writer (wa-sqlite's OPFSWriteAheadVFS, and before it
OPFSPermutedVFS) look a lot like our file: writes go to new places, a
reader holds a snapshot, and no shared memory is used. The measured
numbers are thin, mostly from two authors, and some of them conflict in
ways that a change in Chromium's flush on macOS probably explains.

## The platform: OPFS, sync access handles and their locks

**The standard.** The WHATWG File System Living Standard (last updated
19 October 2026, <https://fs.spec.whatwg.org/>) defines
`FileSystemSyncAccessHandle` with `read`, `write`, `truncate`, `getSize`,
`flush` and `close`, all synchronous, and `createSyncAccessHandle()`,
which is asynchronous and takes an **exclusive** lock on the file entry; a
second open fails with `NoModificationAllowedError`. The standard has no
mode option: it still says only exclusive. `read` and `write` take an
`AllowSharedBufferSource`, so a view of a shared WebAssembly memory is
accepted. Two notes in the text matter to a file format:

- `write` leaves "the write order and the results of partial writes"
  unspecified, because implementations are expected to issue direct
  writes to the host OS. A write can be torn and nothing orders two
  writes before a flush.
- `flush` is "attempt to transfer all cached modifications of the file's
  content to the file system's underlying storage device", and may be a
  no-op. `close` does not flush.

Sync access handles exist only in dedicated workers (MDN,
<https://developer.mozilla.org/en-US/docs/Web/API/FileSystemFileHandle/createSyncAccessHandle>),
not in the main thread, shared workers or service workers. Browser
support for the handle itself: Chrome 102, Firefox 111, Safari 15.2
(MDN browser-compat-data, read 2026-11-11). Chrome 108, Firefox 111 and
Safari 16.4 are where its methods became synchronous; earlier versions
returned promises (header comment of SQLite's
`sqlite3-opfs-async-proxy.js`, 2022-11-27 note,
<https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.46.1-build1/sqlite-wasm/jswasm/sqlite3-opfs-async-proxy.js>).

**Locking modes (`readwrite-unsafe`, `read-only`).** Chromium's "Multiple
Readers and Writers in File System Access API" adds a `mode` to
`createSyncAccessHandle`: `readwrite` (default, exclusive), `read-only`
(many handles, only `read`, `getSize`, `close`) and `readwrite-unsafe`
(many handles, all methods; "writes can be racy if performed from
multiple tabs, and sites would need to provide their own locking
scheme"). It is a draft explainer in whatwg/fs
(<https://github.com/whatwg/fs/blob/main/proposals/MultipleReadersWriters.md>,
issue #34) that says nothing about whether one handle's writes are
visible to another immediately, about ordering between handles, or about
flush. Chromium shipped it in Chrome 121 (intent to ship, 2023-11-27,
<https://groups.google.com/a/chromium.org/g/blink-dev/c/c-9erLfX7Ec>);
that thread records Gecko as positive and WebKit as no signal, and notes
the spec PR was blocked waiting on a second implementer. MDN's compat
data (read 2026-11-11) marks `mode` experimental and non-standard,
supported in Chrome/Edge 121 and **not** in Firefox or Safari. The
intent thread's remark that Firefox "had already implemented" it is not
borne out by any shipped Firefox release I could find. So as of today:
both the shared-read and the shared-write modes are Chromium only.

**Web Locks.** `navigator.locks.request(name, {mode: "exclusive" |
"shared", ifAvailable, steal, signal}, callback)` is scoped to the origin,
works across tabs and workers, and has been baseline since March 2022
(MDN, <https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API>).
A lock is held until the callback's promise settles; Notion and PGlite
rely on a lock being released when the holding tab dies to notice that it
closed. `query()` gives a snapshot of held and waiting locks. This is the
nearest thing the web has to a POSIX lock that dies with its process.

**Flush and durability.** Chromium's `base::File::Flush` (main branch,
`base/files/file_posix.cc`, read 2026-11-11,
<https://chromium.googlesource.com/chromium/src/+/main/base/files/file_posix.cc>)
uses `fdatasync` on Linux, Android and ChromeOS, and `F_BARRIERFSYNC` on
macOS and iOS. Its comment says the default "used to be `F_FULLFSYNC`"
and that the switch "was extensively tried via experiment and showed no
detectable sign of increased corruption". An older revision of the same
file has that experiment, `MacEfficientFileFlush`, disabled by default
("not ready to enable by default as-is",
<https://chromium.googlesource.com/chromium/src/+/6fa11a1f9cc953883145d76f82787877c5e901b6/base/files/file_posix.cc>).
whatwg/fs issue #156 (opened 2024-01-18, still open,
<https://github.com/whatwg/fs/issues/156>) is the complaint that
`F_FULLFSYNC` made `flush()` slow on Mac (crbug 1440778); it says Safari
chose not to use `F_FULLFSYNC` long ago. I did not confirm that the OPFS
flush path goes through `base::File::Flush`, nor in which Chrome release
the default flipped; both are likely but unverified.

IndexedDB moved the other way on durability: since Chrome 121 a
`readwrite` transaction defaults to `relaxed`, its `complete` firing
before the data is flushed, to match Firefox and Safari, which already
behaved this way (Thomas Steiner, Chrome blog, undated, published for Chrome 121,
<https://developer.chrome.com/blog/indexeddb-durability-mode-now-defaults-to-relaxed/>).
Any "OPFS against IndexedDB" number that flushes OPFS and not IndexedDB
compares two durabilities, not two stores.

**Quota and eviction.** OPFS counts against the origin's quota and is
evicted with the rest of its storage. WebKit's policy (Safari 17,
2023-08-10, <https://webkit.org/blog/14403/updates-to-storage-policy/>)
gives a browser origin up to 60% of the disk, evicts least recently used
origins under pressure or after long inactivity, and grants
`persist()` by heuristics such as being a home-screen app. OPFS is not
available in Safari's private windows or Firefox's private browsing, and
Chrome's incognito caps a database at about 100 MB (PowerSync, below;
drift docs, <https://drift.simonbinder.eu/platforms/web/>). A web build
keeps a non-OPFS fallback regardless.

## The official SQLite WASM build: "opfs", "opfs-wl" and "opfs-sahpool"

Source: "Persistent Storage Options", SQLite WASM docs (trunk, read
2026-11-11, <https://sqlite.org/wasm/doc/trunk/persistence.md>), and the
async proxy's source.

**"opfs".** SQLite's VFS has to be synchronous and OPFS opening and
locking is asynchronous, so the VFS runs in SQLite's worker and forwards
each call to a second worker, the async proxy, through a
`SharedArrayBuffer`, blocking on `Atomics.wait` until the proxy answers.
Because of `SharedArrayBuffer`, the page must be cross-origin isolated
(`Cross-Origin-Opener-Policy: same-origin` and
`Cross-Origin-Embedder-Policy: require-corp` or `credentialless`);
without the headers the VFS does not load. Safari before 17 cannot use it
(WebKit bug 255458). Each file's sync access handle is acquired lazily as
an "implicit lock" and released once the VFS is idle for under half a
second; when `createSyncAccessHandle` fails with
`NoModificationAllowedError` the proxy waits and retries several times,
then reports `SQLITE_BUSY`. Reads lock too, so there are no concurrent
readers: one tab works at a time and the others wait their turn. The
docs say the safe number of connections is unknown; older docs said 3,
and testing in 2026-03 ran 8 to 10 workers for long periods if clients
keep transactions short and handle `SQLITE_BUSY`. Releasing the handle
after every operation (`opfs-unlock-asap=1`) costs "up to 400% run time
increase in I/O-heavy benchmarks", the only number on the page, and it
measures the cost of reacquiring the handle. WAL mode needs
`locking_mode=exclusive`, which removes all concurrency.

**"opfs-wl"** (SQLite 3.53.0) is the same VFS with Web Locks in place of
its own retry protocol. It needs `Atomics.waitAsync`, which is in
widespread use only "since 2025". The docs call its speed "on par" with
"opfs", and note that Web Locks queue strictly first in, first out, where
"opfs" could starve a connection.

**"opfs-sahpool"** (3.43) opens a fixed pool of sync access handles at
start (default six) and keeps them open; databases are mapped onto pool
files under internal names, so the directory is not readable as SQLite
files. No `SharedArrayBuffer` and no COOP/COEP. The docs call it "easily
the highest OPFS performance", "starkly clear in batch operations", with
no figures. It has no multi-connection support: installing it fails if
another context in the origin holds the same directory, and 3.50 added
`pauseVfs()`/`unpauseVfs()` so cooperating tabs can hand the pool over.
Same-thread multi-connection use had a locking bug before 3.54 that "can
lead to corruption"; a failed initialisation wiped storage before
`preserveOnInitFailure` (3.54). Stephan Beal, the VFS's author, says in a
2024 forum design thread that keeping handles open is "extremely
performant but eliminates all concurrency options".

Older measurement (wa-sqlite discussion #86, 2023-05-03, Chrome 112, 2014
Mac mini, <https://github.com/rhashimoto/wa-sqlite/discussions/86>): with
one connection and the cache holding the whole database, AccessHandlePool
(the same design as sahpool) was even with "opfs" on some tests and "a
bit faster" on statement-heavy ones; Roy Hashimoto attributes the gap to
proxying over `SharedArrayBuffer`, which SQLite's own analysis put at
about 40%, and says he is "not fully confident" in that. The timings are
in a screenshot, not text. His call counts show SQLite issues four
`xSync` per write transaction under a rollback journal, and that the
read-looking tests measure the page cache, not the file.

## wa-sqlite (Roy Hashimoto)

Sources: the examples README
(<https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/README.md>,
read 2026-11-11), the repository's discussions, and its benchmark page
(<https://rhashimoto.github.io/wa-sqlite/demo/benchmarks/>), which runs
in the reader's browser and publishes no fixed results.

wa-sqlite ships a synchronous build and asynchronous builds (Asyncify, and
JSPI), and a set of example VFSes. Asyncify roughly doubles the binary
and costs 2 to 5 times in speed (PowerSync, below); the JSPI build is
602,830 bytes against 1,223,567 for Asyncify and 595,826 synchronous
(discussion #115, 2024-07-04). JSPI's JS-to-Wasm calls were "hundreds of
times slower" in early 2024, fixed in Chrome Canary by mid-2024; it is
stable in Chrome 137+, behind a flag in Firefox, being implemented in
WebKit since 2025-10, and an Interop 2026 focus area (#115,
<https://github.com/rhashimoto/wa-sqlite/discussions/115>). The examples:

- **IDBBatchAtomicVFS** stores pages in IndexedDB and uses SQLite's batch
  atomic write so no journal is written; the journal lives in the page
  cache. Works in every context. PowerSync reports it degrades past
  100 MB and hit `RangeError: Maximum call stack size exceeded` in Safari
  on large queries. **IDBMirrorVFS** keeps the files in memory and
  persists to IndexedDB: much faster, bounded by memory.
- **AccessHandlePoolVFS** pre-opens a pool of handles, all methods
  synchronous, one connection, so `locking_mode=exclusive` and WAL are
  safe. Not filesystem transparent.
- **OPFSAdaptiveVFS** maps handles to VFS calls, closes them lazily when
  another connection asks, and where the browser allows many handles on a
  file (`readwrite-unsafe`) uses them for overlapping reads and writes.
  Needs an async build.
- **OPFSAnyContextVFS** uses `File` and `FileSystemWritableFileStream`, so
  it runs outside a dedicated worker. Reads are acceptable; writes are
  "very slow" and slow down as the file grows.
- **OPFSCoopSyncVFS** is synchronous, transparent and allows many
  connections, one transaction at a time. It keeps a pool of handles for
  the temporary files and closes the main database's handle lazily. When
  it needs something asynchronous (a lock, an open) a VFS method returns
  an error; the wrapper waits for the async work and repeats the call.
  The README warns this "retry hack" can expose races between
  connections, and PowerSync found it surfaces `SQLITE_BUSY` inside
  transactions, which extensions must handle. No transaction may span two
  databases.
- **FLOOR** ("Fragmented Logging On OPFS Realized", discussion #116,
  2023-10-12 to 2024-05-21,
  <https://github.com/rhashimoto/wa-sqlite/discussions/116>) kept page
  data in an OPFS file and the log's metadata in IndexedDB, wrote log
  pages anywhere free (no fixed frame positions), tracked other
  connections' views with Web Locks, and needed no shared memory:
  "readers should never block a writer, and a writer should never block
  a reader" except at full checkpoints. It relied on IndexedDB's ordering,
  not its durability. It had a race under stress and was removed in
  favour of OPFSPermutedVFS. A tester's Test 1 (1000 autocommit inserts)
  took about 9 s on a Pixel 7 and about 40 s on an M1 MacBook Pro.
- **OPFSPermutedVFS** (2024, deprecated 2026-04-12, discussion #317)
  wrote pages directly into the database file at new positions, so the
  file holds several versions of a page in no particular order; IndexedDB
  holds which version is where; read transactions never touch
  IndexedDB; Web Locks track views; it needs `readwrite-unsafe`. It wrote
  each page once, where FLOOR logged then copied.
- **OPFSWriteAheadVFS** (discussion #315, 2026-04-11,
  <https://github.com/rhashimoto/wa-sqlite/discussions/315>, developed
  with PowerSync) replaces it. A write-ahead log inside the VFS, closer to
  WAL2: two log files, switching when one passes a threshold (1000 pages)
  and the other is fully checkpointed. No shared memory: each connection
  keeps its own lookup of pages in the log, and connections "broadcast
  transaction and checkpoint messages" with fallbacks if a message is
  lost. Separate reader and writer locks; SQLite 3.53's experimental write
  hint picks which; write transactions must be `BEGIN IMMEDIATE`. Readers
  "are never blocked and writers are only blocked by other writers". By
  default the log is not durable (recent transactions can be lost to a
  crash or power failure) because flushing is "by far the most expensive
  file operation"; `synchronous=FULL` restores durability at a write
  cost. Checkpoints are partial and never block. Needs `readwrite-unsafe`,
  so Chromium only. Roy calls the code "new, lengthy (2000+ LoC), and
  moderately complicated".

Roy's numbers for it (Chrome, Mac mini M2, single connection): 1000
autocommit inserts take 0.132 s against OPFSCoopSyncVFS's 0.748 s;
25,000 inserts in one transaction 0.067 s against 0.065 s; most other
tests within 0.01 s. Under contention (three readers, one writer, ten
seconds) OPFSCoopSyncVFS manages about 2,675 iterations per reader and
2,677 writes, the WAL VFS about 197,000 per reader and 29,887 writes,
"~75x reads and ~10x writes". These are one author's runs, one machine,
one browser, with a non-durable default on the faster side.

The README's comparison table marks OPFSWriteAheadVFS filesystem
transparent; PowerSync's table marks it not transparent. Two files of log
beside the database make "transparent" a matter of definition.

## PowerSync and the Sync Conf talk

**The talk.** The Sync Conf talk on this subject is **Conrad Hofmeyr
(co-founder, PowerSync), "SQLite persistence on the web"**, Sync Conf
2025, San Francisco, 12 November 2025, 14:45 local, 20 minutes, main
stage (schedule,
<https://schedule.2025.syncconf.dev/schedule/talks/sqlite-persistence>;
recording, Local-First Conf channel,
<https://www.youtube.com/watch?v=7yNG1aj7-Aw>). It was one of four talks
chosen from 34 CFP submissions (newsletter, 2025-10-07,
<https://buttondown.com/local-first/archive/cfp-talks-for-sync-conf/>).
Its abstract: IndexedDB-based and OPFS-based persistence, wa-sqlite
against the official build, performance and concurrency trade-offs,
multiple tabs and web workers. Hofmeyr's own roadmap post (2026-01-05,
<https://powersync.com/blog/2025-powersync-roadmap-update>) says the talk
"is based on Ralf's blog post", below. No other Sync Conf 2025 talk was
about browser storage (the full list is in the schedule), I found no
Sync Conf 2026, and I could not read the video itself, so its claims here
are those of the post it is based on. If the "much faster" OPFS SQLite
the user remembers is a specific design, the best match is
OPFSWriteAheadVFS (PowerSync and Hashimoto, April 2026), which came
after the talk, or OPFSCoopSyncVFS, which the post recommended at the
time.

Local-First Conf did not have a SQLite-on-OPFS talk in 2024, 2025 or
2026. The nearest is **Jari Jalkanen (Mozilla), "Filesystem in the
Browser"**, Local-First Conf 2026, Berlin, 13 July 2026 (schedule,
<https://app-2026.localfirstconf.com/schedule/day-2/1600-filesystem-in-the-browser>),
on the tension between file access and privacy and "what actually works
today"; its abstract makes no performance claim. Sync Conf 2025 also had
Carl Sverre (Graft, SQLSync) on physical replication, page-level rather
than CRDT sync, which is about replication, not browser storage.

**The post.** Ralf Kistner, "The Current State Of SQLite Persistence On
The Web" (first 2023-07-19, updated 2024-02, 2025-11-11 and 2026-05-15,
<https://powersync.com/blog/sqlite-persistence-on-the-web>). It is the
most complete survey and is careful: it has no benchmark charts, says
its browser versions were not tested, and says none of the projects
claim "production level stability" for persistence. Its points:

- Ways to bridge synchronous SQLite and asynchronous storage: Asyncify
  (2x size, 2 to 5x slower), `SharedArrayBuffer` plus Atomics (a second
  worker and COOP/COEP), JSPI, wa-sqlite's `SQLITE_BUSY` retry, and sync
  access handles (sync reads and writes, async open, exclusive by
  default).
- Recommends OPFSCoopSyncVFS as the general default ("excellent
  performance", databases over 1 GB, no Safari stack overflow),
  OPFSWriteAheadVFS for read-heavy work on Chromium, IDBBatchAtomicVFS as
  the fallback, and not the official "opfs" yet, for COOP/COEP.
- Multi-tab: sync access handles are not available in shared workers;
  Chrome and Edge suspend tabs, which made a shared worker that depended
  on one tab's dedicated worker unresponsive, worked around with a Web
  Lock; Firefox can start dedicated workers from a shared worker, Chrome
  and Safari cannot. Under Ionic Capacitor, handles close when the app is
  backgrounded.
- `PRAGMA synchronous=NORMAL` or `OFF` plus exclusive locking raises
  small-transaction throughput "much", with no number.
- Its table and body disagree on whether AccessHandlePoolVFS is
  transparent and allows concurrent reads (it was rewritten in 2024).

**The SDK.** PowerSync's web SDK docs
(<https://docs.powersync.com/client-sdk-references/javascript-web>, read
2026-11-11) still default to IDBBatchAtomicVFS "for backwards
compatibility", recommend OPFSCoopSyncVFS for multi-tab and Safari, keep
AccessHandlePoolVFS for single-tab apps, offer OPFSWriteAheadVFS on
Chromium, and a `vfs: 'auto'` that tries OPFS and falls back to memory.
Multi-tab sync runs in a shared worker that uses the most recently opened
tab; without shared workers (disabled by default on Android, iOS and
Safari) tabs fall back to broadcast channels, where "updates may not reach
every tab".

## Other production users

**Notion** (Carlo Francisco, "How we sped up Notion in the browser with
WASM SQLite", 2024-07-10,
<https://www.notion.com/blog/how-we-sped-up-notion-in-the-browser-with-wasm-sqlite>).
The official build with **opfs-sahpool**, because the "opfs" VFS needs
cross-origin isolation and Notion loads many third-party scripts. They
first tried "opfs" with Web Locks and writes only from the focused tab and
still saw corruption: wrong data on pages, duplicate row ids. The design
that shipped: each tab has a dedicated worker, a SharedWorker picks one
"active tab" whose worker alone opens the database, all tabs' queries go
through the SharedWorker to it, and a Web Lock per tab tells the
SharedWorker when the active tab dies so it can pick another. Results:
page navigation 20% faster in all modern browsers (28% Australia, 31%
China, 33% India); the library's load first made initial load slower
until loaded asynchronously, and the 95th percentile got worse on slow
Android devices until SQLite was raced against the network. No
corruption attributed to SQLite after the change. The win is a cache in
front of the network, so it says nothing about OPFS against IndexedDB.

**PGlite / ElectricSQL** (<https://pglite.dev/docs/filesystems>,
<https://pglite.dev/docs/multi-tab-worker>, read 2026-11-11). Postgres
in Wasm has the same synchronous problem. Its OPFS "AHP" filesystem opens
a pool of handles with random names and keeps a state file with the
directory tree and metadata, so OPFS shows a pool, not a Postgres data
directory. It does not work in Safari, which "appears to have a limit of
252 open sync access handles" against Postgres's 300+ files. Multi-tab:
every tab starts a worker, they elect a leader that alone runs PGlite,
and a new election runs when the leader's tab closes. The IndexedDB
filesystem loads every file into memory and writes whole files back
after each query ("a few milliseconds"), with a `relaxedDurability`
option that answers before flushing.

**cr-sqlite / vlcn** (Matt Wonlaw). The browser package
`@vlcn.io/crsqlite-wasm` 0.16.0 imports wa-sqlite's IDBBatchAtomicVFS and
carries an experimental access-handle-pool worker (package source on
jsDelivr, read 2026-11-11). No published storage benchmark.

**absurd-sql** (James Long, 2021,
<https://github.com/jlongster/absurd-sql>) put sql.js's pages in
IndexedDB as blocks, synchronous through `SharedArrayBuffer` and Atomics
with COOP/COEP, with a fallback in which only one tab may write. It
claimed up to 10x over plain IndexedDB on a 2015 MacBook Pro for its own
read and bulk-insert benchmarks, and is unmaintained. This is the design
closest to our current web store (a record per block). **Riffle**
(Schiefer, Litt, Schickling, Jackson, March 2022,
<https://riffle.systems/essays/prelude/>) used sql.js with absurd-sql in
the browser and native SQLite under Tauri, and reports query times, not
storage times.

**Turso in the browser** (Nikita Sivukhin and Jamie Barton, 2025-10-08,
<https://turso.tech/blog/introducing-turso-in-the-browser>). Their
SQLite rewrite runs queries on the main thread and does file I/O in a
worker through OPFS's synchronous interface, sharing buffers over
`SharedArrayBuffer` (so COOP/COEP) to avoid copies; they found moving
query execution into the worker cost "pretty significant" message
overhead. The handle locks the database to one worker, so tabs cannot
share a database. No numbers.

**drift / Dart `sqlite3`** (Simon Binder,
<https://drift.simonbinder.eu/platforms/web/>,
<https://pub.dev/documentation/sqlite3/3.4.0/wasm/SimpleOpfsFileSystem-class.html>).
Drift ranks OPFS in a shared worker (only Firefox can start the needed
dedicated worker from a shared one), OPFS with two workers and
`Atomics.wait` (needs COOP/COEP), IndexedDB chunks in a shared worker,
then unsafe IndexedDB and memory. On Chrome for Android without shared
workers "there unfortunately is no way to prevent data races between
tabs". Its `SimpleOpfsFileSystem` stores exactly two files,
`/database` and `/database-journal`, opened up front because opening is
the only asynchronous step, and takes a `readWriteUnsafe` flag.

**RxDB** (vendor material; its OPFS storage is a paid add-on). "LocalStorage
vs. IndexedDB vs. Cookies vs. OPFS vs. WASM-SQLite" (dated 2024-05-11 but
measured on Chrome 128,
<https://rxdb.info/articles/localstorage-indexeddb-cookies-opfs-sqlite-wasm.html>)
stores each JSON document as its own OPFS file, so its OPFS times include
opening a file per operation; there IndexedDB beats OPFS on small reads
and writes. Its OPFS storage page (<https://rxdb.info/rx-storage-opfs.html>)
claims OPFS is "3x-4x faster compared to IndexedDB" with a table that has
no hardware, browser or method. Neither is evidence about a single file
held open.

**wcpos** (point-of-sale app, issue #2143, measured 2026-09-18,
<https://github.com/wcpos/monorepo/issues/2143>). opfs-sahpool behind a
worker RPC against their IndexedDB-backed engine, on their own query
shapes: SQLite wins indexed queries by one to two orders of magnitude and
loses whole-document reads and bulk reseeding; a three-document write was
20 / 36 / 61 ms on IndexedDB against 6.0 / 3.1 / 31 ms on SQLite (Chrome
Mac / Firefox Mac / Chrome Windows runner). Playwright WebKit failed the
first 1,000-row OPFS write on both OPFS engines. It measures engines, not
stores, and its own summary multipliers do not match its table.

## Measured numbers

Times as reported. "Commit" means a write transaction that flushes.

| Measurement | Number | Source, date | Conditions |
|---|---|---|---|
| 1000 autocommit INSERTs, OPFSCoopSyncVFS | 0.748 s (≈0.75 ms per commit) | wa-sqlite #315, 2026-04-11 | Chrome, Mac mini M2, one connection, durable |
| 1000 autocommit INSERTs, OPFSWriteAheadVFS | 0.132 s | same | same, log not flushed by default |
| 25,000 INSERTs in one transaction, Coop / WAL VFS | 0.065 s / 0.067 s | same | same |
| 3 readers + 1 writer for 10 s, Coop | ≈2,675 reads each, 2,677 writes | same | same |
| 3 readers + 1 writer for 10 s, WAL VFS | ≈197,000 reads each, 29,887 writes | same | needs `readwrite-unsafe` |
| Insert one small row, wa-sqlite OPFS AHP / IndexedDB / memory | 19.8 / 2.9 / 0.08 ms | PGlite benchmarks, undated (2024) | M2 MacBook Air, browser not stated |
| Select one small row, wa-sqlite OPFS AHP / IndexedDB | 0.03 / 0.67 ms | same | same |
| Insert one small row, PGlite OPFS AHP / PGlite IndexedDB | 3.9 / 21.0 ms | same | same; PGlite IDB relaxed: 0.085 ms |
| 1000 autocommit INSERTs, wa-sqlite OPFS AHP / IDB batch atomic | 24.67 s / 1.094 s | same | same |
| 25,000 INSERTs in a transaction, wa-sqlite OPFS AHP / IDB batch atomic / memory | 0.137 / 0.15 / 0.077 s | same | same |
| 1000 autocommit INSERTs (FLOOR) | ≈9 s Pixel 7, ≈40 s M1 MacBook Pro | wa-sqlite #116, 2023-11 | Chrome Canary, community tester, unmodified benchmark |
| Releasing the OPFS handle after every operation | up to +400% run time | SQLite WASM docs, undated | "I/O-heavy benchmarks" |
| Proxy over `SharedArrayBuffer` ("opfs") | ≈40% overhead, author unsure | wa-sqlite #86, 2023-05-03 | Chrome 112, 2014 Mac mini |
| IndexedDB, 1000 one-item transactions, default / relaxed | 10,456 / 631 ms | Nolan Lawson, 2021-08-22 | Chrome 92, 2015 MacBook Pro (strict was then the default) |
| IndexedDB, 1000 items in one transaction, Chrome / Firefox / Safari | 126 / 53 / 28 ms | same | same, Firefox 91, Safari 14.1 |
| Small write, IndexedDB / OPFS in worker (file per document) | 0.17 / 1.54 ms | RxDB, 2024 | Chrome 128, machine not stated |
| Small read, IndexedDB / OPFS in worker (file per document) | 0.1 / 1.41 ms | same | same |
| Notion page navigation with SQLite cache | −20% (−28 to −33% by region) | Notion, 2024-07-10 | production A/B |
| IndexedDB relaxed default, real-world | "between a factor of 3 and 30" | Chrome blog, for Chrome 121 | claim, no method |
| Asyncify cost | ≈2x size, 2–5x slower | PowerSync survey | claim |

What these say, and where they conflict:

- **The cost of a commit on OPFS is the cost of `flush()`**, and on
  macOS it moved by more than an order of magnitude. The PGlite page has
  wa-sqlite on OPFS at about 20 ms a commit (SQLite flushes four times
  per transaction, so about 5 ms a flush; PGlite flushes once, about
  4 ms), and FLOOR's tester saw an M1 Mac four times slower than a Pixel
  7. Roy's 2026 run has a durable commit at 0.75 ms on an M2. The likely
  reason is Chromium replacing `F_FULLFSYNC` with `F_BARRIERFSYNC`; that
  is an inference, not something anyone measured side by side. A barrier
  orders writes but does not force them to the medium, so the faster
  number is also a weaker promise.
- **"OPFS is faster than IndexedDB"** is true for reads of a file kept
  open (0.03 against 0.67 ms per select above) and for large
  transactions, and false for small commits wherever OPFS flushes and
  IndexedDB does not (Chrome since 121, and Firefox and Safari before
  that). wa-sqlite #86 in 2023 found IDBBatchAtomicVFS
  "dominates" the commit-heavy test and loses the rest. RxDB's two pages
  give opposite verdicts because one opens a file per document.
- **Nothing published measures** a single `read` or `write` call on a
  sync access handle, `createSyncAccessHandle` latency, `flush` on
  Firefox or Safari, Windows, or a mobile Safari. Every concurrency
  number is from one author on Chrome on a Mac.

## What applies to us

**One file per space, one owner at a time.** Outside Chromium a sync
access handle is exclusive, even for reading. So the file can be open in
one dedicated worker at a time, and the choice is the one every project
above made: one owner worker that the other tabs RPC to (Notion,
PGlite, AccessHandlePool, opfs-sahpool), or handing the file from worker
to worker per transaction (OPFSCoopSyncVFS, SQLite "opfs"). The second
pays an asynchronous open per turn (SQLite's "up to 400%" is that cost),
cannot let a reader in while another tab writes, and is the design Notion
saw corrupt data under; the retry it needs is what PowerSync found
leaking into transactions. The first is what the production apps
converged on. For us it means: the worker that owns a space's file runs
the archive, holds the handle for its lifetime, and other tabs send it
`Get`/`Put`/`Import` and node loads over a `MessagePort`. Our native
design (one writer at a time, readers reading a whole slot) already
assumes a single writer per file, so the owner worker's writer is just
the native writer with an in-process turn.

**Web Locks as the turn, and as leader election.** The native turn byte
maps onto an exclusive Web Lock named for the space; native "process
died, its lock went with it" maps onto a lock released when its worker
or tab ends. The same lock elects the owner: the worker that holds it
opens the handle; the others queue on it and take over when it drops,
reopening the file and running recovery from the slots, which the native
design already does after a crash. Web Locks are first in, first out
(SQLite's "opfs-wl" relies on this), so a waiting tab is not starved.
Shared mode could stand for readers, as FLOOR and OPFSPermutedVFS used it
to track views; we do not need that while one worker owns the file.

**Pooling OPFS files as extents.** A writer's extent maps naturally onto a
file of its own: an extent is appended to by one owner, which is exactly
what an exclusive handle allows, and pools of pre-opened handles are the
proven fast path (sahpool, AccessHandlePool, PGlite). So a space could be
a directory: a head file (slots, durable slot, memory cells, index pages)
and extent files, each held by the worker that appends to it, with the
turn (a Web Lock) guarding the head. Two limits: whoever holds the head
must also read every extent, and outside Chromium it cannot open an
extent another worker holds, so extent owners would have to serve reads
of their own extents or close an extent once it is sealed; and Safari's
limit of about 252 open handles bounds the pool. This trades "one file"
for parallel appends, which only pays if tabs really write at the same
time; with turns of a few hundred KB per commit, one owner probably
suffices.

**`readwrite-unsafe` for many writers.** On Chromium every tab's worker
could open the one file with `readwrite-unsafe` and the native design
would carry over almost whole: the turn is a Web Lock, an extent is
claimed by a Web Lock instead of a byte lock, readers read a slot they
trust and the records it names, which never change. OPFSWriteAheadVFS
shows the pattern works and pays (about 75x the reads of the exclusive
design under contention) with no shared memory, which matches our
readers taking no lock. But it is Chromium only, its explainer does not
say when one handle sees another's writes or how writes from two handles
order, and it is a draft that WebKit has not engaged with. It can be an
optimisation behind a feature check, never the only path.

**Our format suits OPFS better than SQLite's does.** The designs that
needed many connections all ended up writing new data to new places
under a snapshot (FLOOR, OPFSPermutedVFS, OPFSWriteAheadVFS), which is
what our file does without being asked: records are appended and never
change, a slot names a whole state, index pages are rewritten only after
a sync point. SQLite's rollback journal is the reason the cooperative
designs serialize readers behind writers; we have no journal. And the
spec's warning that writes can tear and reorder before a flush is the
case our rolling slots and per-record and per-page checksums already
handle natively.

**What blocks us, or costs.**

- **No mmap.** Native readers read tokens in place through a mapping. In
  a worker every read is a `read(view, {at})` copy into Wasm memory
  (one copy, into a `Uint8Array` over linear memory, since the method
  accepts shared buffers). Token and frame reads need a cache in Wasm
  memory sized for the working set; the measured reads (0.03 ms a row,
  page cache included) suggest the call itself is cheap, but no one has
  published its per-call cost.
- **The owner worker must run the archive synchronously** on the handle,
  or proxy each call over `postMessage`. Our Rust is async at the
  archive boundary, so a sync handle inside the worker that runs the
  archive is fine and needs no COOP/COEP; a design where the archive
  runs elsewhere and blocks on the worker needs `SharedArrayBuffer` and
  cross-origin isolation, which Notion found impractical.
- **Exclusive handles mean a frozen owner blocks everyone.** Chrome
  freezes and discards background tabs. PowerSync hit a shared worker
  hung on a suspended tab. The owner must be a worker whose tab is
  likely to stay alive, and the lock must be given up (or stolen with
  `steal`) when a tab is hidden for long; what happens to a handle in a
  discarded tab needs testing.
- **`flush()` is the durability point and its meaning varies.**
  `fdatasync` on Linux Chrome, `F_BARRIERFSYNC` on Apple Chrome (ordering,
  not media durability), unknown on Firefox and Safari, and possibly a
  no-op by spec. Our sync points (records, sync, durable slot, sync) need
  ordering more than media durability, which a barrier gives. But the
  native recovery's fast path relies on `boot_id` to know the OS kept
  what a dead process wrote; the web has nothing like it, so every open
  after a crash should take the slow path (check what was written since
  the durable slot), or the open should trust a flag the owner clears
  only after a clean close.
- **The IndexedDB comparison must be fair.** Our IndexedDB store, on
  Chrome since 121 and on Firefox and Safari always, does not flush per
  transaction. An OPFS archive that flushes only at sync points, as
  native does, is the like-for-like comparison; one that flushes per
  commit will lose on small commits and the published numbers will look
  like OPFS is slow.
- **A fallback stays.** Private windows in Safari and Firefox have no
  OPFS, Chrome incognito caps it near 100 MB, and older Safari has no
  `createWritable`. The IndexedDB store remains for those.
- **The file is not portable as-is.** Our web fs provider already writes
  through OPFS with whole-file `createWritable` (see
  `rust/dialog-storage/src/storage/provider/fs/web.rs`); a sync-access
  archive is a different path. If we split a space into a head file and
  extents, it is no longer the native single file and needs an export
  step, as sahpool and PGlite's pool do.

## Open questions

- What a single `read`, `write`, `getSize` and `flush` cost on a held
  handle in Chrome, Firefox and Safari, on macOS, Windows, Linux, Android
  and iOS, for 4 KiB pages and 100 KiB appends; and what
  `createSyncAccessHandle` costs, which decides between an owner worker
  and handing the file over per turn. Nothing published answers this;
  it is a small benchmark to write.
- Whether OPFS `flush()` in Chrome goes through `base::File::Flush`, in
  which release Apple's default became `F_BARRIERFSYNC`, and what Firefox
  and Safari do. The 20 ms and 0.75 ms commits above cannot both be
  right for the same browser.
- Under `readwrite-unsafe`, whether a write through one handle is visible
  to a `read` through another as soon as it returns, and whether writes
  from two handles can interleave within a record. The explainer is
  silent; Chromium's behaviour (shared OS file, so probably the page
  cache) needs a test.
- Whether `getFile()` (an asynchronous `File` read, no lock in the spec)
  works on a file another worker holds an exclusive sync handle on, in
  each browser. If it does, sealed extents and the head could be read by
  non-owners without RPC, slowly.
- What happens to a held handle, and to its Web Lock, when Chrome freezes
  or discards the owner's tab, and when iOS backgrounds the page.
- How large our working set is, to size the Wasm-side cache that stands
  in for the mapping.
- Whether turns are ever contended in real use (several tabs writing at
  once). If not, extents on the web are not worth their complexity.
- Whether Firefox will ship the locking modes; the intent thread called
  Gecko positive in 2023 and nothing has shipped since.
