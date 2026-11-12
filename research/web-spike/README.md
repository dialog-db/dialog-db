# Web spike: a space file in the browser

**Status: Exploring** (a spike in four rounds; nothing here is built into
the archive).

Whether the single-file archive ([`research/single-file-format`](../single-file-format/journal.md),
[`design/file-writers.md`](../../design/file-writers.md)) can serve dialog
in a browser, and whether that is worth it against what dialog uses there
today, an IndexedDB store: "space savings without becoming slow". The
background is [web-opfs.md](../single-file-format/web-opfs.md). What was
tried, measured and decided, with every number, is in the
[journal](journal.md); this page is its outcome.

## Report

- **Round one, the platform (Chromium 153).** A sync access handle reads
  4 KiB in 1–2 µs. A shared worker can only broker ports: each tab makes
  its own dedicated worker. Web Locks carry the native byte locks'
  semantics at 120–155 µs a lock, against about 1 µs natively.
  `readwrite-unsafe` behaves as the native file does, and a prototype on
  it lost nothing in 64 runs with tabs closed and crashed mid-run.
- **Round two, the medium against today's store.** The space comes from
  the format, not the medium: raw blocks take today's bytes in every
  medium, a compact format a third or less in IndexedDB and OPFS alike.
  OPFS is much faster, not smaller.
- **Round three, the native format's own trace, as a go/no-go.** Of the
  file as IndexedDB slices, an owner worker, and OPFS in every tab's
  worker, only the last passed every bar (a tenth of the bytes, commits
  at 0.12–0.17x), and it is Chromium-only.
- **Round four, one dialog worker hosted by a tab** (holding the file with
  the exclusive handle, behind the service worker): **kept**. At one tab
  and three it takes 0.14x today's bytes, commits at 0.15–0.20x, polls at
  0.29–0.40x and reads cold views at 0.66–0.96x; losing its tab costs what
  losing today's dialog instance does; it needs nothing Chromium-only.
  With no tab open the service worker can only read the file.

What it does not show: Firefox and WebKit were not available, and the
format's own CPU in Wasm is inferred, not measured. Next, if taken up:
the archive's reads over a page cache in Wasm, tabs talking to the host
over ports rather than the service worker's `fetch` (about 1.5 ms of every
request today), and the hosting rules.

## Supporting materials

The harness, the run, report and chart scripts, the runbook and every
run's raw JSON are on the
[`research-data`](https://github.com/dialog-db/dialog-db/tree/research-data/research/web-spike)
branch, under `research/web-spike/`. The charts the journal shows are
here, in `results/`.
