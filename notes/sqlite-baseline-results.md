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

## Group 4: read path and repository overhead (branch claude/perf-5-read-repo)

Targets: (a) the read-side price of buffered roots that Group 3 introduced
(point_get / se_kind_lookup drifted +5-15% against controls), and (b) the
repository layer's ~2.8x per-commit overhead over the raw store.

Changes that LANDED:

1. **Range-restricted, slot-tracked buffered-read resolution**
   (`dialog-search-tree`). The walker's `pending_for_leaf` previously
   decoded every key of every ancestor buffer on the path (root buffers
   hold up to 256 ops for the whole subtree), decoded values for every
   run winner, deduplicated across levels with an O(k^2) membership scan,
   and re-ran the O(n) buffer validation plus an O(at) polarity re-scan
   inside every `op_at`. It now collects only ops inside the walk's own
   range (an out-of-range op can never yield — for a point read that is
   ~1 op instead of ~256), tracks each winner's value-table slot while
   streaming (new `op_with_slot`; validation runs once per buffer in
   `keys()`), merges the per-level winner lists with a linear sorted
   merge, and decodes values only for ops that survive narrowing and
   shadowing. `pending_for_key` resolves through a new one-pass
   `ArchivedNoveltyBuffer::resolve`; the owned `NoveltyBuffer::resolve`
   and the transient sealed-buffer range collection got the same slot
   treatment. Resolution semantics (last-op-in-run wins within a buffer,
   root-most layer wins across the path) are unchanged and remain pinned
   by the existing buffered-vs-canonical equivalence sweeps.
2. **Commit identity memoized per branch** (`dialog-repository`).
   `branch_of` + `origin_of` (blake3, base58 render, URI parse) are pure
   functions of (subject, name, profile, issuer) but ran on every
   `Branch::commit`; a keyed memo on the branch handle now serves them.
3. **History-record fold without redundant clones/keys**
   (`dialog-artifacts`). `buffer_record` cloned the whole claim per
   instruction just to compute its history key, and the flush loop
   rebuilt the same key a second time via `into_entry`. `Record::key`
   now borrows, and the fold map's own key is reused for the final write
   (`Record::into_datum`).
4. Profile targets `profile_read` and `profile_repo_commit` added to
   `dialog-baseline` (callgrind without criterion in the way).

Change that was tried and REVERTED (negative result, kept for the
record): batching the commit path's key fan-out (3 index keys per
instruction, folded history entries, the revision-record pair) into a
single hitchhiker enqueue per group. Dev-profile callgrind loved it
(-26% on 200 branch commits) — but that was mostly debug-assert work.
Release callgrind told the truth: batching moves where the overflow
cascade fires, and the resulting buffer shapes were consistently worse
(repo 200 commits +11.3% Ir, raw 1000-commit seed +5.3% Ir; more
regroup/memcpy/hash-memo work downstream). The `ArtifactWriter::write_all`
convenience stayed, deliberately implemented as sequential writes, with
the negative result documented on it.

Measurement notes. Wall-clock on this container drifts: the base branch
was RE-RUN in this session and the deltas below are that same-machine
A/B (absolute medians, base run vs final run ~2h apart); sqlite rows are
the drift controls, and `sqlite_disk` swung +54% between the two se runs,
so no disk-row claims are made. Release-build callgrind (deterministic,
no debug asserts — the dev-profile numbers earlier groups used overstate
buffer-path costs via a debug re-encode assert in `into_buffers`) backs
the wall numbers.

Criterion medians (same-machine A/B; sqlite rows are the controls):

| benchmark | dialog_mem | dialog_disk | control drift |
|---|---|---|---|
| attr_scan | **-7.2%** (1.299 -> 1.205 ms) | -0.7% | sqlite -2.0% / +2.7% |
| join | **-5.3%** (3.081 -> 2.918 ms) | -1.7% | sqlite ~0% |
| se_title_get | -1.5% (7.76 -> 7.64 us), ~-5% against a +4.3% control | -3.4% | sqlite_mem +4.3% |
| point_get | -1.3% (18.43 -> 18.19 us) — flat in wall; **-12.2% instructions** (release callgrind: 175.8K -> 154.4K Ir/get, buffered store) | +1.3% | sqlite_mem -3.5% |
| se_kind_lookup | +0.9% (flat; its cost is per-row Entity reconstruction — audit #5, untouched) | +5.7% (history says this row swings ±20%) | sqlite ±1% |
| write_small_txns | +0.5% abs vs a +7.3% control (~-6% relative, but within drift) | noisy | sqlite_mem +7.3% |
| write_batch | +3.5% abs vs a -2.8% control — same code on this path (batching was reverted), so this brackets the session's intra-run noise at ~±6% | +0.9% | sqlite mixed |
| se_replay_write | -2.3% (145.9 -> 142.5 ms) | +3.7% | sqlite_mem -3.1%, sqlite_disk +54% (!) |
| repo_mem (se replay) | +2.3% (813 -> 832 us/commit; CI -15%..+11%, p=0.69 — flat) | -2.4% | same |

Release callgrind (deterministic; `profile_repo_commit 200 small`,
`profile_read 1000 x 500 point gets`):

| metric | base | batched attempt | final |
|---|---|---|---|
| point get, Ir/get | 175.8K | 154.6K | **154.4K (-12.2%)** |
| repo commit, 200 small | 622.3M | 692.6M (+11.3%) | **604.7M (-2.8%)** |
| raw commit, 200 small | 185.5M | 169.8M (-8.4%) | 185.0M (-0.3%) |
| raw per-row seed, 1000 | 7,037M | 7,410M (+5.3%) | 7,014M (-0.3%) |

Interpretation, honestly:

- **The read regression is partially recovered, and where it lives is
  now clear.** Scans and joins (which cross buffered ancestors per leaf)
  got their -5..-7%; the point read halved its buffered-path
  instructions (pending_for_leaf 29.6K -> 6.7K Ir/get, whole read -12%
  Ir) but wall stayed flat — the remaining point-read wall is memory
  stalls in search/pipeline setup, not the buffer merge. se_kind_lookup
  never moved because its per-row cost is `Entity`/URL reconstruction
  (audit finding 5), which this group did not touch.
- **The repository overhead target (below 2x) was NOT reached: the
  honest result is ~3% instruction reduction** (identity memo + record
  fold fixes), with repo_mem wall flat within its noisy CI. The
  remaining overhead decomposes (release Ir per commit, synthetic
  2-assert shape): versioned instruction semantics inside
  `write_instructions` (+0.82M — the standing-claim read probes and the
  history/coverage writes riding the same tree), TWO Ed25519 signs
  (0.50M — the record signature and the head signature; that alone is
  54% of a whole raw commit), the revision-record append (0.22M), the
  proportionally larger seal (0.21M), and skip/context/publish
  bookkeeping (~0.35M). Under this group's constraints — recorded
  history bytes and signature semantics unchanged — those are semantic
  costs, not removable constants; the redundant work (claim clones,
  double key builds, per-commit identity derivation) is what this group
  removed, and it was small.
- **The batching negative result is worth keeping in mind**: enqueue
  granularity changes buffered tree shapes, and shape effects (where
  cascades fire, what regroup touches) can swamp per-call savings in
  either direction. Anything that changes write granularity on the
  buffered path needs a release-build shape-sensitive measurement, not
  a dev-profile one.

What to try next for the repo gap, in expected order of value: cut the
versioned probe/scan constants (they ride the same buffered read path,
so audit findings 5 and 16 apply); stream sealed link buffers into the
encoder at persist instead of decode-then-re-encode on lift (the
memcpy complex both layers share, largest where record values sit in
root buffers); and revisit whether the head signature can cover the
record signature's payload (one sign per commit) — a semantic change
needing an owner decision.

## Group 5 spike: DCAA single-file archive (branch `claude/perf-6-dcaa-spike`)

What was built: the DCAA v1 single-file archive from `notes/dcaa.md`,
implemented as a capability provider — another member of the archive
provider family, a peer of the file-per-blob `FileSystem` archive on
native and the IndexedDB/OPFS archive providers on the web, NOT a
`StorageBackend` implementation. `dialog_storage::provider::dcaa::Dcaa`
serves `archive::{Get, Put, Import}` from one append-only `.dialog` file
per catalog, opens from a `Location` via `Resource` like the other
providers, and a `Space` selects it by using it as its archive field
(the bench harness composes `Space<TempDcaa, TempFileSystem, ...>`). It
lives as a module inside dialog-storage rather than a new crate because
that is where every other provider lives and where the
`Resource`/`Space` composition machinery it plugs into is defined; the
module is cfg'd off wasm32 (the engine needs file offsets, `set_len`,
fdatasync).

Both review-note amendments are implemented:

1. **Per-commit index deltas** with a periodic fold (threshold 32 by
   default, `DIALOG_DCAA_FOLD` overrides, 0 = merged index every commit).
2. **Outboard policy**: `outboard_len = 0` at or below 64 KiB
   (whole-blob BLAKE3 verification — every tree node in these workloads),
   full BAO outboard above it (`bao-tree` 0.16 builds and verifies fine
   in this environment; the plain-BLAKE3 fallback was not needed).

Two fsync modes, so durability is a measured variable instead of an
excuse: the default fsyncs once per commit (records + index delta +
footer ride one buffered write, then one fdatasync);
`Dcaa::configured(root, fold, durable=false)` — or `DIALOG_DCAA_NOSYNC=1`
for `Location`-opened providers — skips the per-commit fdatasync.
Relaxed mode writes the identical byte format and recovers through the
identical footer scan; it only stops promising that a commit survives a
crash the moment `commit` returns, which is exactly the (absent)
guarantee the file-per-block archive provides, making
`repo_dcaa_nosync` vs `repo_disk` apples-to-apples.

Spec deviations, documented in the module docs: the footer is 72 bytes
(delta-chain fields: base index location, previous footer offset, chain
length), and the footer checksum covers the commit's whole appended
payload, not just the footer bytes. The latter is what makes a SINGLE
fsync per commit crash-safe: with no write barrier between payload and
footer, the kernel may persist the footer page before the payload pages,
and a footer-only checksum would validate that torn state. Covering the
payload costs nothing at commit time (the bytes are in hand) and O(tail
commit) at recovery. Tests cover torn-tail truncation, a
footer-persisted/payload-torn commit, media corruption of durable
commits (BAO and whole-blob paths), redaction semantics, dedup no-ops
(a fully-duplicate transaction appends zero bytes), lookup correctness
across an unfolded delta chain after reopen, and relaxed-mode
round-trip + recovery.

Commit granularity maps 1:1 onto effect granularity: the repository
layer sends one `Import` per branch commit, and each `Import` is one
DCAA transaction — so durable mode is one fsync per branch commit.

### Measured: the durability triple (same run, criterion medians)

This container's disk is shared and NOISY: identical code moved up to
2.4× between runs an hour apart (dialog_disk measured 937 ms and then
387 ms on consecutive runs of this same branch), so only same-run
comparisons are quoted, and differences under ~±15% on disk rows are
not claims. The triple that matters — repo_disk (file-per-block, never
fsyncs) vs repo_dcaa_nosync (DCAA, fsync skipped: same durability as
repo_disk) vs repo_dcaa (DCAA, fdatasync every commit) — comes from one
run with its SQLite controls.

SE replay, 500 real transactions (1,289 facts), fresh store per
iteration:

| config | median | per commit | durable? |
|---|---|---|---|
| sqlite_mem | 10.9 ms | 22 µs | no |
| sqlite_disk (WAL, NORMAL) | 90.1 ms | 180 µs | checkpoint-deferred |
| sqlite_disk_nosync | 20.6 ms | 41 µs | no |
| dialog_mem (raw store) | 143 ms | 286 µs | no |
| dialog_disk (raw store, file-per-block) | 387 ms | 774 µs | no |
| repo_mem | 408 ms | 817 µs | no |
| repo_disk (file-per-block) | 1.69 s | 3.4 ms | no |
| **repo_dcaa_nosync** | **933 ms** | **1.9 ms** | no |
| **repo_dcaa** | **2.74 s** | **5.5 ms** | **yes, per commit** |

Synthetic writes (same run):

| config | write_small_txns (100 × 1 txn) | write_batch (1000, 1 txn) |
|---|---|---|
| sqlite_mem | 0.74 ms | 4.94 ms |
| sqlite_disk | 2.37 ms | 5.37 ms |
| sqlite_disk_nosync | 2.31 ms | 5.53 ms |
| dialog_mem | 10.5 ms | 1.15 s |
| dialog_disk | 124 ms | 1.17 s |
| repo_mem | 32.0 ms | 1.49 s |
| repo_disk | 179 ms | 1.54 s |
| **repo_dcaa_nosync** | **191 ms** | **1.77 s** |
| **repo_dcaa** | **358 ms** | **1.60 s** |

Reading the triple:

- **The owner's observation was right and the original framing was
  wrong-way-round: DCAA's machinery is not slower than file-per-block —
  the fsync is.** At equal durability, DCAA is 1.8× FASTER than
  file-per-block on the real replay (933 ms vs 1.69 s, far outside this
  host's noise): one sequential append per commit replaces several
  temp-file+rename creations, and node reads come off one open fd plus
  an in-memory index instead of per-block `open`/`read`/`close`.
- On the synthetic shapes the two no-fsync rows are statistically
  indistinguishable: small_txns 191 vs 179 ms (+7%, inside noise;
  commits here are 2 fresh facts — minimal blocks, no supersession
  reads, so there is little file-per-block overhead to beat) and the
  batch rows overlap outright (1.77 vs 1.54 s ranges touch; the durable
  row even printed FASTER than nosync, which is mechanically impossible
  and confirms these deltas are noise around a CPU-bound 1.5 s). No
  profiling of "DCAA overhead" is warranted: at equal durability there
  is no measured overhead to profile, only a large win on the workload
  with real commit widths and read traffic.
- **The price of actual durability is the fsync and nothing else**:
  repo_dcaa minus repo_dcaa_nosync is ~3.6 ms/commit on the real replay
  and ~1.7 ms/commit synthetic — this host's fdatasync latency. When
  commits batch it collapses (write_batch: durable within noise of
  everything else — one commit, one fsync).

### Index amplification (review amendment 1, measured)

`cargo run -p dialog-baseline --release --example dcaa_amplification`
replays the SE log through the repo harness with the delta chain ON
(fold 32) and OFF (`DIALOG_DCAA_FOLD=0`, a complete merged index every
commit — the pre-amendment spec), plus the file-per-block control. The
DCAA file is strictly append-only, so final file size == total bytes
written, and byte counts (unlike wall times) are deterministic:

| SE txns | fold 32 | fold 0 (merged every commit) | file-per-block control | total ratio | index-bytes ratio |
|---|---|---|---|---|---|
| 500 (835 blocks) | 53.7 MiB | 61.8 MiB | 51.5 MiB / 835 files | 1.15× | ~4.7× (2.2 vs 10.3 MiB) |
| 2000 (5,043 blocks) | 403.0 MiB | 568.9 MiB | 378.7 MiB / 5,043 files | 1.41× | ~7.9× (24 vs 190 MiB) |

(Index bytes = total minus the record bytes, which are identical across
the two configs.) The amendment is validated and the effect grows with
entry count exactly as predicted — at 835 entries the merged index is
only 33 KiB so rewriting it per commit barely shows; at 5k entries the
merged-per-commit config already spends a third of all bytes on index
churn, and the spec's 100k-entry projection (~4 MB of index per 1-fact
commit) makes the chain non-optional at scale.

The bigger number in that table is neither config's index: ~379 MiB of
RECORD bytes for 4,675 facts. The append-only prolly tree supersedes
spine/leaf blocks on every commit and nothing ever reclaims them —
file-per-block leaves the same dead bytes as 5,043 loose files. Any
production adoption of DCAA needs the spec's compaction section
implemented (and gets it much cheaper than a directory walk: one
sequential rewrite + rename).

### Verdict (revised after the durability control)

Should DCAA replace file-per-block as the native store? **Yes, on these
measurements — and the earlier hedged verdict understated it.** At
equal durability DCAA is faster than file-per-block on the realistic
workload (1.8× on the SE replay) and at parity on the synthetics, while
also giving: one file instead of thousands, dedup no-op writes,
self-verifying reads (whole-blob BLAKE3 under 64 KiB, BAO above),
redaction, tested crash recovery, and an OPTIONAL true-durability mode
(fdatasync per commit, ~2-4 ms/commit on this host) that file-per-block
cannot offer at any reasonable price (it would need an fsync per block
file plus directory fsyncs). The fsync default is a policy question,
not an architecture question — the provider exposes both modes, and
relaxed mode equals today's durability while being faster.

Remaining gaps before flipping the default archive, none of which the
numbers argue against: compaction (the append-only file grows without
bound; dead tree blocks dominate it — 403 MiB for 4.7k facts — same
bytes file-per-block leaves behind, but visible as one growing file),
a concurrency story beyond the current single-process per-catalog
mutex, and wiring a `NativeSpace` variant + real (non-temp) `Location`
opening through app configuration.


## Deferred decisions (owner-reviewed)

- **Batch-signing commits** (2026-07-28): approved direction for the
  two-Ed25519-per-commit cost (one signature over a small Merkle root of
  the record + revision payloads; both stay standalone-verifiable via a
  1-hash inclusion proof). Deliberately deferred until the larger costs
  (storage I/O, DCAA outcome) are optimized away; revisit once the
  repo-commit profile is no longer dominated by them.
