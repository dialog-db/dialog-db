# SQLite baseline: first captured numbers

Produced by `rust/dialog-baseline` (see its crate docs for methodology):

```sh
cargo bench -p dialog-baseline --bench sqlite_vs_dialog -- --warm-up-time 1 --measurement-time 3
DIALOG_SE_CSV=<retro-facts.csv> cargo bench -p dialog-baseline --bench se_replay -- --warm-up-time 1 --measurement-time 3
```

Environment for the numbers below: 4-core x86_64 Linux container,
2026-07-28, criterion medians, branch `claude/perf-1-sqlite-baseline`
(commit before any optimization work). Absolute numbers are
machine-specific; the *ratios* are the signal, and the same commands
re-run on any machine reproduce them.

SQLite is configured as a faithful model of the dialog information model:
one `facts` table with the EAV ordering as `PRIMARY KEY (of, the, val)
WITHOUT ROWID`, plus `(the, of, val)` (AEV) and `(val, the, of)` (VAE)
secondary indexes — the same three orderings `dialog-artifacts` maintains.
`sqlite_disk` is WAL + `synchronous=NORMAL` (the production bar);
`sqlite_disk_nosync` is `synchronous=OFF`, which matches the durability
dialog's fsync-free filesystem backend actually provides today.

## Synthetic `stuff` workload (mirrors dialog-query's `seed_stuff`)

| workload | sqlite_mem | sqlite_disk | sqlite_disk_nosync | dialog_mem | dialog_disk |
|---|---|---|---|---|---|
| write_small_txns (100 entities, 1 txn each) | 0.719 ms | 2.80 ms | 2.58 ms | 13.7 ms | 96.1 ms |
| write_batch (1000 entities, 1 txn) | 4.66 ms | 6.27 ms | 5.07 ms | 1.833 s | 1.867 s |
| point_get (of 1000 entities) | 0.82 µs | 2.26 µs | 2.16 µs | 23.6 µs | 23.2 µs |
| attr_scan (1000 rows) | 163 µs | 168 µs | 171 µs | 1.118 ms | 1.162 ms |
| join (1000 rows, storage-layer hash join) | 542 µs | 577 µs | 562 µs | 2.673 ms | 2.580 ms |

### What the gaps say

- **Small commits: 19× (memory) / 34× (disk, with *weaker* durability than
  SQLite's NORMAL).** ~137 µs per 2-fact commit in memory is commit-path
  CPU (audit findings: per-instruction value encodes, canonical rebuild
  path instead of the buffered one); the additional ~10× on disk
  (~960 µs/commit) is the file-per-block backend (multiple files + syscalls
  per commit, no batching).
- **Batch writes: ~390×, and superlinear.** 1000 entities in one commit
  cost 1.83 ms *per entity* in memory — 13× worse per entity than the
  small-commit path at 100 entities. A single big commit degrades as the
  transient tree grows (per-instruction descents ping-ponging across the
  EAV/AEV/VAE regions, linear `child_for` routing). This is the
  bulk-import finding made measurable.
- **Point get: 10–29×, all CPU.** dialog_mem and dialog_disk are
  identical (23 µs) — the OS cache hides the disk, so the whole gap is
  per-read validation + linear leaf decode (rkyv bytecheck on every
  `body()`, front-coded linear `find`, `Entity`/URL reconstruction per
  row).
- **Scan and join: 5–7×.** The smallest gaps — the streaming scan path is
  the best-optimized part of the read side — but per-row `Entity::parse` +
  blake3 and per-row allocations still cost ~1 µs/row where SQLite pays
  ~0.16 µs/row.

## Stack Exchange replay (real data: retrocomputing.stackexchange.com)

Transformed with `scripts/se-transform.py` (117,236 facts / 50,553
transactions; see `notes/benchmark-dataset.md`). The replay commits one
transaction per commit — real commit boundaries, real supersession
(cardinality-one writes are `Instruction::Replace` in dialog, delete+insert
in SQLite), real long-tailed value sizes crossing the 4096-byte spill
boundary.

Replay of the first 500 real transactions (1,289 facts), fresh store per
iteration; reads against a store seeded with the first 2,000 transactions:

| workload | sqlite_mem | sqlite_disk | sqlite_disk_nosync | dialog_mem | dialog_disk |
|---|---|---|---|---|---|
| replay 500 txns | 10.2 ms | 93.6 ms | 23.4 ms | 444 ms | 1.61 s |
| per commit | 20 µs | 187 µs | 47 µs | 888 µs | 3.2 ms |
| kind lookup (value-indexed, ~700 rows) | 34.9 µs | 40.5 µs | 35.5 µs | 261 µs | 244 µs |
| title point get (superseded pair) | 0.97 µs | 6.07 µs | 2.68 µs | 9.34 µs | 9.41 µs |

### What the real workload adds

- **Small real commits: 43× (memory), 17× against SQLite's *durable*
  NORMAL config, 69× against the durability-equivalent nosync config.**
  Real commits average 2.6 facts and include `Replace` supersession
  (prior scan + retract + assert), which costs more than the synthetic
  pure-assert path — the realistic per-commit price is ~0.9 ms CPU plus
  ~2.3 ms of file-per-block I/O.
- **Value-indexed lookups: 7×** — consistent with the synthetic scan gap;
  per-row reconstruction costs dominate.
- **Point gets: ~1.5× vs cold-ish sqlite_disk, ~10× vs sqlite_mem.** The
  dialog store again shows memory ≡ disk (9.4 µs both): pure CPU.

## Reading the numbers

The write-side gaps are the priority: they are 1-2 orders of magnitude and
they are the local-first interactive case (many small commits). The
audit's phase-1/phase-2 items target exactly these constants; each
improvement group PR should re-run both benches above and quote the deltas
against this file, then update it.

## Group 1: trust-once validation (branch `claude/perf-2-trust-once`)

Changes: bytecheck validation memoized per buffer (`TypeId`-keyed, then
`access_unchecked`); point `get` binary-searches the memoized flat key
decode from a leaf's second touch; `Delta::get` takes the read lock.

Measured deltas (criterion vs the baselines above; SQLite rows are the
noise controls):

| benchmark | dialog_mem | dialog_disk | control drift |
|---|---|---|---|
| point_get | **-36.6%** (23.6 → 14.4 µs) | **-33.5%** (23.2 → 15.8 µs) | ±3% |
| se_title_get | **-21.3%** (9.3 → 7.3 µs) | **-17.2%** (9.3 → 7.7 µs) | ±2% |
| se_kind_lookup | **-15.1%** (250 → 212 µs) | **-12.4%** (249 → 218 µs) | ±5% |
| write_small_txns | -3.0% | -8.5% | sqlite_disk moved -7.4% → treat as noise |
| write_batch / scans / join | within noise | within noise | ±5-10% |

Reads got the predicted win; the remaining point-get gap vs SQLite
(~15 µs vs 2.2 µs) is per-query pipeline setup, walker descent, and
per-row `Entity` reconstruction — later groups. Write benches did not
move outside noise, as expected: the write path's costs are dominated by
encode/hash work, not validation.
