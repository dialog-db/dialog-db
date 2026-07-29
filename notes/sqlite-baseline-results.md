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

## Group 2: leaf weight caching (branch `claude/perf-3-weight-cache`)

Changes, in the order the profile forced them:

1. **Leaf weight cache.** `TransientSegment` now carries its exact total
   entry weight (`Option<usize>`, computed lazily, maintained
   incrementally by the segment's own `upsert`/`delete`, seeded from the
   regroup's already-computed per-entry weights when a group is sealed,
   and invalidated by any wholesale mutation). The edit path's
   frame-ceiling gate reads it in O(1) instead of re-summing the whole
   leaf (`Entry::weight` over every entry) on every membership-changing
   edit. The segment's `entries` field went private so the compiler
   forces every mutation through the cache-maintaining methods; exactness
   is additionally pinned by test-build asserts.
2. **First measurement was a negative result.** The cache removed the
   weight complex at small N (callgrind, dev profile, `profile_commit`:
   N=300 fell 100.3M → 55.5M instructions, `payload_weight` 22.1% → 0.3%)
   but `write_batch/1000` moved only ~-5%. Re-profiling at N=1000 showed
   why: per-entity instructions jump ~45× between N=300 and N=1000
   (17.7B total) because frames cross the 192 KiB ceiling and enter the
   force-split regime, where every membership edit re-merges the frame
   and re-runs the whole-frame anchor election — `cap::frame_cut_positions`
   alone was 62.7% of ALL instructions, split between the blake3 memo's
   SipHash table lookups (~27%), the per-seam candidacy test that
   allocated a 513-byte padded separator just to compare it (~28%), and
   byte-wise `lcp` scans (~11%). The weight sums were a minor term there.
3. **Election constants, shape-safely.** Three fixes, none touching any
   decision value: `is_frame_candidate` decides candidacy without
   materializing the padded separator (pinned to
   `frame_separator(..).is_some()` by an exhaustive equivalence test plus
   a test-build assert on every call); the blake3 memo's table hasher is
   now FxHash instead of SipHash (the memo compares full keys, so hash
   quality affects probes, never values); `lcp` compares word-at-a-time.

Measured deltas (criterion medians; SQLite rows are the noise controls):

| benchmark | dialog_mem | dialog_disk | control drift |
|---|---|---|---|
| write_batch (1000, 1 txn) | **-55.5%** (1.833 s → 815 ms) | **-55.8%** (1.867 s → 826 ms) | sqlite ±5% |
| write_small_txns | within noise (13.4 ms) | -34% (96 → 59 ms), but likely file-I/O variance, not a claimed win | ±5% |
| se_replay_write (500 real txns) | within noise (437 ms) | -17% printed, but sqlite_disk also -12% → I/O noise | sqlite_mem +8.8% |
| se_title_get / se_kind_lookup | within control drift (controls swung ±20% between two runs an hour apart; a quiet re-run measured kind_lookup flat) | same | ±20% |

Profile attribution (callgrind instruction counts, `profile_commit`,
dev profile):

| N | before | after | per entity |
|---|---|---|---|
| 100 | 22.8M Ir | 15.4M Ir (-33%) | 228K → 154K |
| 300 | 100.3M Ir | 52.0M Ir (-48%) | 334K → 173K |
| 1000 | 17.70B Ir | 7.73B Ir (-56%) | 17.7M → 7.7M |

Wall (dev profile): `profile_commit 1000` 2.07 s → ~1.05 s.

Interpretation: the batch-commit constant halved, but the shape of the
curve stands — per-entity cost still jumps ~45× from N=300 to N=1000,
because the frame-ceiling regime re-merges and re-elects over the whole
frame on every edit, and that is architectural, not a constant. What
remains at N=1000: memcpy 28% (entry moves and clones through
`Vec::insert` and the merge/regroup concatenations), the memoized-hash
table lookups ~18%, election bookkeeping ~8%. Group 3 candidates, in
expected order of value: route batch commits through the buffered
(hitchhiker) path instead of one canonical edit per key; memoize frame
election results per unchanged frame; cut per-edit entry clones. Small
real-world commits (se_replay, 2.6 facts/commit) never touch the ceiling
regime and were expectedly unmoved — their costs are the Group 1 note's
per-commit encode/persist constants, untouched here.

## Group 3: commit path (branch `claude/perf-4-commit-path`)

Changes:

1. **Single value encode per instruction** (`dialog-artifacts`).
   `EncodedValue` computes the key payload and, for a spilling value, the
   raw block bytes plus their 32-byte reference in ONE pass;
   `write_instructions` threads it through key building and the spill
   store. Previously each instruction re-encoded its value in the key
   builder, again in the spill check, and (spilled values) re-hashed and
   re-serialized twice more in the spill store. A provable lower bound on
   the encoded length short-circuits the spill decision without the full
   encode (byte-string encodings are strictly longer than raw; numerics
   are fixed-width; symbols never short-circuit). Same keys, same blocks.
2. **`Artifacts::commit` goes through the buffered (hitchhiker) path and
   seals WITHOUT canonicalizing.** This is a policy decision from the
   project owner, not just an optimization: commit roots are now the
   buffered form — valid, publishable, content-exact (a node's hash covers
   its buffered ops) but NOT canonical. Canonicalization is an explicit,
   separate operation: `Artifacts::canonicalize()` (mirroring the
   repository layer's pre-existing `commit(..).canonicalize()` builder).
   The policy across the stack: ordinary commits seal buffered; sync and
   publish do NOT canonicalize (buffered novelty sits near the root, so
   replicas differ in a few top blocks and the diff exchanges FEWER
   blocks than fully-canonicalized trees whose differences smear across
   leaf paths); bulk loads canonicalize once at the end —
   `Artifacts::import` now does. Tests pinning canonical-root equality
   (insertion-order independence, assert+retract cancellation, CSV
   round-trip) now canonicalize explicitly before comparing; the
   buffered-vs-direct root-equality pin in `buffered.rs` is unchanged.
3. **Repository-layer benchmark rows** (`repo_mem` / `repo_disk`): the
   same workloads through dialog-repository's `Branch::commit` — the
   surface applications actually write through — via a lean
   operator/repository/branch stack in `dialog-baseline::repo`, branch
   handle held across commits.

Measurement notes, before the numbers: the container restarted between
Group 2's runs and these, and the machine's run-to-run drift measured up
to ±30% on identical code (a write-only rerun of the same binary moved
`write_batch/dialog_mem` +30% while its SQLite controls moved +13-28%).
The stored criterion baselines were therefore stale, so Group 2's branch
(`claude/perf-3-weight-cache`) was RE-BENCHED on this container
back-to-back with this branch, and the deltas below are that same-machine
A/B. Deterministic callgrind instruction counts back up the wall-clock
claims; anything inside ±30% without a callgrind confirmation is labeled
accordingly.

Criterion deltas (same-machine A/B; SQLite rows are the controls):

| benchmark | dialog_mem | dialog_disk | control drift |
|---|---|---|---|
| se_replay_write (500 real txns) | **-64.6%** (442 → 156 ms; 885 → 313 µs/commit) | **-49.8%** (2.41 → 1.21 s) | sqlite ±5% |
| write_small_txns (100 × 2 facts) | **-20.5%** (13.9 → 11.1 ms) | -21% printed, but disk runs swung 53↔161 ms across reruns → file-I/O noise, no claim | sqlite mixed ±7% |
| write_batch (1000, 1 txn) | -18.3% printed (924 → 755 ms), but a same-code rerun moved +30% → within machine noise; callgrind says the true single-batch delta is ~-3% | same | sqlite -4 to -18% |
| point_get | +4.9% (18.4 → 18.7 µs) | +13.4% (16.8 → 19.0 µs) | sqlite -9 to -15% → relative ~+15-25%, see below |
| attr_scan / join | -10% / -13% | -5% / -7% | sqlite ±8% |
| se_kind_lookup | +13.6% (263 → 291 µs) | +3.3% | sqlite_mem +4.9% |
| se_title_get | +0.5% (8.13 → 8.17 µs) | -5.9% | sqlite ±4% |

Callgrind instruction counts (deterministic, dev profile,
`profile_commit`, `Artifacts::commit` only):

| shape | Group 2 | Group 3 | delta |
|---|---|---|---|
| one batch, N=100 | 15.36M Ir | 51.48M Ir | **+235%** |
| one batch, N=300 | 52.06M Ir | 112.5M Ir | +116% |
| one batch, N=1000 | 7.769B Ir | 7.496B Ir | -3.5% |
| 100 per-row txns (`profile_commit 100 small`, new mode) | 190.1M Ir | 152.2M Ir | **-19.9%** |
| 500 per-row txns | 6.091B Ir | 1.645B Ir | **-73.0%** |

Repository layer (`Branch::commit`), measured on this branch in the same
session — the practical write budget an application pays:

| workload | sqlite_mem | dialog_mem (raw store) | repo_mem | repo_disk |
|---|---|---|---|---|
| write_small_txns (100) | 0.83 ms | 11.0 ms | 33.7 ms (337 µs/commit) | 149 ms |
| write_batch (1000) | 5.2 ms | 1.05 s | 1.54 s | 1.65 s |
| se_replay_write (500 real txns) | 11.9 ms | 153 ms | 434 ms (868 µs/commit) | 1.30 s |

Interpretation:

- **The sequential-commit shape is the win, and it grows with the tree.**
  One canonical edit per commit re-reshaped the touched leaves every
  time; the buffered path appends to bounded node buffers and reshapes
  only on overflow. Deterministically -20% at 100 commits, -73% at 500,
  and -65% wall on the real 500-txn replay (controls flat). The
  remaining se_replay gap vs sqlite_mem is 13× (was 43× at baseline,
  after Groups 1-3 combined).
- **A single one-shot batch does NOT benefit**: the hitchhiker layer
  costs extra per-op bookkeeping (+235% Ir at N=100 — cheap in absolute
  terms, ~36M Ir ≈ ms-scale in dev profile) and at N=1000 the batch
  cascades into the same ceiling-regime merge machinery Group 2
  profiled, so the superlinear write_batch curve stands (~-3%). The
  audit's finding-8 batch entry point and the transient merge memcpy
  complex (Group 2's "what remains") are untouched — that was this
  group's optional item 3, skipped in favor of the repository rows.
- **Reads pay a small, real price for buffered roots.** Point reads must
  merge node buffers over stored entries on the descent: point_get and
  the value-indexed kind_lookup drift +5-15% against controls moving the
  other way. Scans moved -5 to -13% (probably tree-shape luck; within
  machine noise). This is the deliberate trade of the non-canonical
  policy; `canonicalize()` restores the compact form when a caller wants
  it (e.g. after bulk load — `import` now does exactly that).
- **The repository layer costs ~2.8-3.1× the raw store per commit**
  (337 vs 110 µs synthetic small commits; 868 vs 306 µs real replay,
  in-memory). The overhead is version tagging + history claims + the
  signed revision record + head publication per commit. This row is the
  honest "what an app pays" number: 36× sqlite_mem on the real replay.
- **Roots are now path-dependent by default.** Two stores committing the
  same facts in different batches converge on the same root only after
  both canonicalize. Anything comparing commit-produced roots across
  replicas must either canonicalize first or compare content.

What to try next, in expected order of value:

1. The repository layer's ~3× per-commit overhead: profile
   `Branch::commit` — candidates are the per-commit Ed25519 signing, the
   revision-record encode (a large CBOR value that usually spills), and
   the head-publish round trip; some of these batch or cache naturally.
2. The single-batch ceiling regime (audit #8, Group 2's memcpy 28% /
   memo-table 18%): per-edit entry clones and the whole-frame re-merge in
   `dialog-search-tree` `tree/transient.rs` — unchanged by this group,
   still the write_batch wall.
3. Read-side: teach point descent to skip empty buffers cheaply (or
   background-canonicalize idle trees) to claw back the +5-15%.
4. The buffered path's own per-op constant (+36M Ir per 100-key batch at
   small N) — likely the per-write node clone in the hitchhiker layer.
