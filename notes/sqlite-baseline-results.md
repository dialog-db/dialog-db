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


## Group 6: in-memory commit decomposition (branch `claude/perf-7-oplog-spike`)

Question answered first (owner): do we have numbers showing that
removing signing and disk writes alone reaches the desired profile? Yes
— and they show it does NOT. `dialog_mem` (500 real SE txns, no
signing, no history, no disk at all) measures 286 us/commit against
sqlite_disk's 180 us durable WAL commits. The in-memory buffered commit
itself is the remaining wall. The owner also rejected the operational
log/memtable split (see `notes/operational-log-architecture.md`):
commits must keep a stable content address and the per-commit push
contract; the target is making the buffered commit cost what the
root-novelty design intends.

Decomposition of that 286 us (new `profile_se_replay` example, release
callgrind, 500 real SE txns, 2.03M Ir/commit; wall re-measured at
260-282 us/commit):

| term (exclusive Ir) | share | per commit |
|---|---|---|
| memcpy | 29.3% | ~601K Ir |
| blake3 kernels (AVX2 + SSE4.1 + drivers) | ~17-20% | ~350-400K Ir |
| allocator (malloc/free/realloc family) | ~10.7% | ~215K Ir |
| varkey scans (`value_payload_len` 5.5%, `split_components` 2.2%, parse/build ~1.6%) | ~9.3% | ~190K Ir |
| memcmp | 2.8% | ~57K Ir |

By phase (inclusive): `BufferedBatch::apply` (enqueue into the transient
root) 56.9%; `BufferedBatch::seal` (persist: encode + hash + set) 35.9%.

Byte volume (new `measure_se_replay` example, `MeasuredStorage` over the
memory backend, 500 real SE txns averaging 2.6 facts / ~2 KB novelty per
commit):

| commits | bytes written per commit (window) |
|---|---|
| 1-50 | 28 KB |
| 201-250 | 78 KB |
| 451-500 | 91 KB |
| average | **64 KB/commit, ~3.2 sets, ~30x byte amplification** |

Reads mirror writes (31 MB read over the replay): every commit round-trips
the full root frame persistent -> transient (clone all entries + buffered
ops) -> apply -> re-encode -> re-hash -> set.

Conclusion: per-commit cost is O(root frame bytes), not O(novelty
bytes), because the novelty buffer is embedded inline in the root frame
— appending 2 KB of ops forces the whole ~64-90 KB frame to be decoded,
cloned, re-encoded, re-copied (memcpy 29%), and re-hashed (blake3 ~20%)
every commit. rkyv is NOT the primary gap; swapping the encoding library
would leave the O(frame) shape intact. Two directions follow:

- **Copy elimination (no format change)**: remove the per-commit
  persistent->transient->persistent round trip of untouched entries
  (owner: cloning is waste). Caps at the full-frame hash, ~O(frame)
  floor of roughly 30-60 us/commit.
- **Chained novelty deltas (small format change, LMDB-style
  touch-only-what-changed)**: the root frame stores its entry section
  plus a digest link to this commit's small delta block (which links the
  previous). Commit then hashes ~2 KB of new ops plus a small root
  re-encode: O(novelty) floor, single-digit us. Bonus: a per-commit push
  ships the ~2 KB delta block instead of the whole rewritten ~90 KB root
  frame — a direct replication-bandwidth win consistent with
  novelty-near-root sync. Flush folds the chain into children exactly as
  the buffer flushes today.

### SIMD experiment (2026-07-30)

Question (owner): can SIMD speed up the tree ops? Measured answer for
the current commit path:

- ~50% of commit instructions already run in SIMD kernels: memcpy 29%
  (glibc AVX2/ERMS), blake3 ~17-20% (hand-written asm with runtime
  dispatch; the binary carries the AVX-512 kernels and this host has
  full AVX-512, so real runs hash at maximum width — callgrind showed
  the AVX2 kernel only because valgrind masks AVX-512 from CPUID),
  memcmp 2.8% (AVX2).
- Rebuilding everything at `-C target-cpu=x86-64-v3` (lets rustc
  autovectorize with AVX2/BMI2): collected Ir 1,028.6M -> 1,008.1M
  (**-2.0%**), wall clock indistinguishable. The scalar half of the
  commit is allocator traffic, varint decoding, and per-entry control
  flow — sequential dependence, not vectorizable loops.

Conclusion: the SIMD-friendly work (bulk copy + bulk hash) is exactly
the O(frame) waste the group-6 work is about to eliminate; making the
waste faster is not the lever. SIMD items worth revisiting AFTER the
O(novelty) restructuring shifts the profile:

- ed25519 signing (repo layer): curve25519-dalek's AVX-512/IFMA backend
  — this host has `avx512ifma` — is a compile-time backend choice, part
  of the already-deferred x86-64 baseline decision.
- SIMD-decodable varints / columnar scans (stream-vbyte style) if
  varint decode surfaces as a top term post-restructuring (today ~1.2%).
- Cross-node parallelism (cores, not lanes): hashing/encoding
  independent nodes fans out during flush, import, canonicalize, and
  checkpointing (blake3 also parallelizes internally over large
  inputs). It cannot help single-commit latency — one root node, hash
  chain dependency — which is another argument for keeping the commit
  itself O(novelty).

### Group 6A results: copy elimination landed (2026-07-30)

Changes on `claude/perf-7-oplog-spike` (all persisted bytes pinned
bit-identical by tests at the tree and store layers):

1. **Live spine across commits** — `HitchhikerTree::persist_mut` (a
   non-consuming persist), `LinkNovelty::Cached` (a persisted open link
   keeps its decoded ops AND its encoding: appends skip the
   accumulated-buffer decode, untouched links re-embed verbatim),
   `BufferedBatch::apply_reusing` + `SpineSlot` on `Artifacts` and on
   repository `Branch` (keyed by root hash, so out-of-band root changes
   miss safely).
2. **Persist seeds the node cache** with the frame it just produced —
   the next read otherwise re-fetched it from storage and re-verified
   the blake3 of bytes this process just hashed (point queries drop one
   backend read; the pinned read-count tests moved 2 -> 1).
3. **`terminated_len` via memchr** — the 0x00-terminator scan crossed
   every value on every key parse/split byte-by-byte; now it leaps
   between zeros with SIMD memchr (-8.4% alone). This is the measured
   answer to "can SIMD help": not by compiling wider (-2%), but by
   restructuring scalar scans around vector primitives.
4. `Manifest::default()` reads its env overrides once per process
   (was 4 getenv per persist), and the repo commit path reads through
   the branch's shared node cache (`Index::from_hash_with_cache`)
   instead of a fresh empty cache per commit.

Instruction counts (callgrind, 500 real SE txns, collected Ir):

| step | Ir | vs baseline |
|---|---|---|
| post-group-4 baseline | 1,028.6M | — |
| + live spine | 1,057.4M | +2.8% (open-side copies became persist-side clones) |
| + cache seeding | 930.1M | -9.6% |
| + Cached links | 814.0M | -20.9% |
| + memchr scan | 745.7M | **-27.5%** |

Criterion scoreboard (same-run, 500 real SE txns; reads on a 2000-txn
seed):

| config | replay | per commit |
|---|---|---|
| sqlite_mem | 10.9 ms | 22 us |
| sqlite_disk (WAL+NORMAL, durable) | 106.2 ms | 212 us |
| sqlite_disk_nosync | 23.0 ms | 46 us |
| dialog_mem | **103.9 ms** | **208 us** (was 286) |
| dialog_disk | 508 ms | 1.02 ms (file-per-block I/O) |
| repo_mem | 325.6 ms | 651 us (was 817) |
| repo_dcaa_nosync | 932 ms | 1.9 ms |
| repo_dcaa (fsync/commit) | 2.62 s | 5.2 ms |

**dialog_mem now matches fully durable SQLite on the real workload**
(103.9 vs 106.2 ms) while producing a stable content-addressed root and
a pushable novelty-near-root block per commit. Remaining gap to
sqlite_disk_nosync (46 us, the equal-durability comparison): 4.5x,
almost entirely the O(frame) persist (memcpy 29.5% + blake3 13.8% of
the remaining profile — ~3 passes plus one hash over a 28-91 KB frame
per commit) — that is what the group-B chained-novelty format targets.
Reads improved too: se_title_get 8.1 -> 7.3 us, se_kind_lookup
291 -> 258 us (dialog_mem, cache-seeded root).

Repo layer decomposition (callgrind, 200 synthetic per-row commits
through `Branch::commit`, volatile space): **Ed25519 signing is now the
largest single term at 26.6%** (two signs per commit via `Attest`,
~250K Ir each); record writes ~27%, data instructions ~24%, persist
~22%. The deferred batch-signing decision's trigger condition — "the
profile is no longer dominated by storage I/O" — has arrived for the
in-memory profile; one Merkle-pair signature per commit would cut the
repo overhead by roughly an eighth immediately, and the
curve25519-dalek IFMA backend (this class of host has avx512ifma) cuts
the per-sign cost further.

### Group 6A addendum: deferred flush (owner-designed, 2026-07-30)

Owner design: "allow insert novelty without bothering with encode and
once we are done inserting we apply flush policy — this way we avoid
several flushes into the same node." Implemented as
`HitchhikerTree::insert_deferred`/`delete_deferred` (route only, no
trigger evaluation) + `HitchhikerTree::settle` (one top-down flush-policy
pass), wired so a `BufferedBatch` defers through the whole instruction
stream and settles once in `seal`. Reads are novelty-aware wherever ops
sit, so the batch's own supersession scans are unaffected; buffered
roots were already batching-dependent, so the (different, still valid)
shapes this produces break no contract. All 643 tree/artifacts/repo
tests pass, including the flush-policy equivalence oracle and both
spine byte-identity pins.

Measured (same machine, same run):

| workload | before | after |
|---|---|---|
| write_batch dialog_mem (1000 entities, 1 txn) | ~755-820 ms | **377-387 ms** (-50%; campaign start: 1.833 s, -79% cumulative) |
| write_batch sqlite_mem / sqlite_disk (controls) | 4.7 / 6.3 ms | 5.0 / 5.8 ms (flat) |
| se_replay dialog_mem (small commits, rarely overflow) | 204-213 us/txn | 209-223 us/txn (unchanged) |

The batch regime's remaining 66x vs sqlite is now dominated by the
canonical leaf path the deferred ops land through (`replay_ops` ->
per-edit reshape), which is exactly the layer the owner suggests the
hitchhiker flush could subsume ("hitchhiker design could probably
replace transients if we avoid multiple child rebuilds") — recorded in
`notes/tree-research-2026-07.md` as a follow-up direction alongside the
g-tree and bijoux findings.

### Buffer-capacity sweep (2026-07-30): 256 is at the optimum

New `DIALOG_TREE_OP_BUF` override (read once, native only) sweeping the
op-buffer capacity on the 500-txn real SE replay (dialog_mem, two runs
per point):

| capacity | per commit |
|---|---|
| 16 | 463 us |
| 32 | 331-352 us |
| 64 | 245-250 us |
| 128 | 213-224 us |
| **256 (default)** | **212-234 us** |
| 512 | 243-254 us |

The default sits at the measured minimum: smaller buffers make commits
cheaper per frame but flush so much more often that the amortized cost
rises steeply; larger buffers grow the frame past the flush savings.
Conclusion: the ~210 us in-memory commit is the design's amortization
EQUILIBRIUM, not a mistuned constant — further reduction requires
either fewer byte-passes per persist (engineering), cheaper repo-layer
semantics (batch-signing), or a node-format change designed WITH the
sync contract (the chained-delta form of that idea is rejected — see
`notes/operational-log-architecture.md`).

### Node-size sweep (2026-07-30): the ~50 KB target is not optimal, and small frames crash

Owner: "my general goal was to have nodes that are roughly 50kb for
optimal network reads but we never measured anything." New
`node_size_sweep` example: per `DIALOG_TREE_MAX_SEGMENT` setting it
builds the real SE dataset (buffered replay + one canonicalize),
censuses the block-size distribution, and measures cold-read fetch
profiles (freshly opened store, empty cache — the partial-replication
shape) for a point get, an entity load, and a VAE lookup.

2000 real txns (4675 facts), point get (24 queries averaged):

| max_segment | write us/txn | cold point get | cumulative bytes written |
|---|---|---|---|
| 8 KiB | 292 | 3.0 fetches / 34 KB | 112 MiB |
| 16 KiB | 514 | 2.0 / 64 KB | 161 MiB |
| 32 KiB | 541 | 2.0 / 76 KB | 164 MiB |
| 48 KiB (the stated goal) | 444 | 2.0 / 121 KB | 166 MiB |
| 64 KiB (default) | 533 | 2.0 / 179 KB | 172 MiB |
| 128 KiB | 607 | 2.0 / 272 KB | 176 MiB |
| 256 KiB | 1932 | 2.0 / 763 KB | 193 MiB |

Modeled network cost per cold point get (fetches x RTT + bytes / BW):
broadband (30 ms, 20 Mbit): 16K 85 ms < 32K 90 < 8K 104 < 48K 108 <
64K 132 < 128K 169. Mobile (80 ms, 5 Mbit): 16K 262 < 32K 281 < 8K
295 < 48K 353 < 64K 447. 10,000-txn confirmation (24K facts): 32K
beats the 64K default on every axis (write 1895 vs 2043 us/txn, point
get 110 vs 136 KB at equal 2.1 fetches, canonicalize 25 vs 32 ms,
cumulative bytes 1513 vs 1655 MiB).

Also visible in the census: real block sizes run FAR past the pacing
target (64K setting: p50 18.5 KB, p90 92 KB, p99 209 KB — the p99 is
3.2x the target), so "roughly 50 KB nodes" is not what the current
default produces anyway.

**Bug found (pre-existing, exposed at small segments + volume):**
`DIALOG_TREE_MAX_SEGMENT=16384` with 10,000 real txns fails during
replay with "Re-shape path child index out of range"; 8192 fails during
canonicalize with "Re-shape path descended into a node that was not
lifted". Bisect: clean at `claude/perf-3-weight-cache` and at
`f89cd32~1`; first failing commit is `f89cd32` ("commit through the
buffered path", group 3) — i.e. the switch to buffered-by-default
commits EXPOSED a latent reshape/buffered-flush interaction bug rather
than any of the group-6A changes (HEAD, pre-deferred-flush `8065f34`,
and pre-6A `b10807a` all reproduce identically). The default 64 KiB
survives the full 50,553-txn log and 32 KiB survives 25,000 txns, so
the default is not observed to hit it — but the invariant violation is
shape-dependent, so this gates ANY move to smaller segments and needs a
real fix in the reshape path.

Also worth flagging from these runs: per-commit cost GROWS with
dataset size (210 us at 500 txns -> ~1.9 ms at 10K -> 3.6 ms at 25K ->
4.5 ms at 50K, default settings) — the scaling curve, not the small-DB
constant, is the next performance question after the bug.

Recommendation once the reshape bug is fixed: move the default toward
32 KiB (dominates 64 KiB on writes, cold-read bytes, canonicalize, and
write amplification at both measured scales), and revisit 16 KiB —
best in every network model at small scale — only with the bug fixed
and a deeper-tree fetch-count check at larger scales.

### Live-tree census and scale curve (2026-07-30): three honest corrections

Owner challenges: (1) the frame ceiling was believed to clamp blocks at
3x — does it?; (2) 32 vs 64 KiB is unclear without the depth-growth
scale; (3) are we anywhere near SQLite at scale? New tools:
`live_census` (walks ONLY blocks reachable from the live root, split by
node kind — the earlier sweep censused the whole backend graveyard,
dead superseded roots included, which was misleading) and `scale_curve`
(windowed per-commit cost as the dataset grows, sqlite_mem vs
dialog_mem).

**1. The clamp does NOT hold.** Canonical (post-canonicalize) leaf
segments at 10,000 real txns, in bytes:

| max_segment | p50 | p99 | max | share over 3x byte ceiling |
|---|---|---|---|---|
| 8 KiB | 7.5 KB | 35 KB (4.3x) | 47 KB (5.7x) | 5% |
| 32 KiB | 32 KB | 145 KB (4.4x) | 171 KB (5.2x) | 6% |
| 64 KiB | 60 KB | 268 KB (4.1x) | 365 KB (5.6x) | 7% |

These are live canonical leaves, not graveyard: real blocks drift to
~4-6x the pacing target, ceiling notwithstanding. (The ceiling clamps
WEIGHT; encoded bytes exceed 3x max_segment far beyond any plausible
weight/byte accounting slack, so either the weight accounting diverges
badly from bytes or a write path skips the over-ceiling gate — the
buffered flush's batch landings are the prime suspect. Diagnosis
needed: extend the census to compute each fat leaf's WEIGHT and
compare against the ceiling directly.) The owner's conclusion stands:
with arbitrary block sizes, size-sensitive measurements (network
models, fanout, pacing sweeps) sit on sand until this is pinned.

**2. The tree is going FLAT, and that is the scaling story.** The
census shows the canonical tree at 10K txns is ONE root index node
holding 284 links (64 KiB setting) or 549 links (32 KiB) directly over
the leaves. The geometric coin (m=256) simply failed to cut (P(no cut
in 549 seams) ~ 12%), and the index-level frame ceiling in weight
terms permits thousands of links before force-splitting. Every commit
rewrites that root frame — links PLUS the novelty buffer — so the
per-commit cost grows with the dataset (the root frame is
O(leaf count) until the ceiling finally splits it). The 32-vs-64
depth question is therefore not the binding issue; unbounded index
fanout variance is.

**3. Not close to SQLite at scale.** Windowed per-commit cost, real SE
replay, both stores in memory:

| txns | sqlite us/txn | dialog us/txn | ratio |
|---|---|---|---|
| 2,500 | 33 | 593 | 18x |
| 10,000 | 59 | 1,941 | 33x |
| 25,000 | 141 | 4,796 | 34x |

SQLite grows 4.3x over the range (its own tree depth); dialog grows
~8x and holds a ~30-40x ratio. The 500-txn parity with durable
sqlite_disk was a small-database artifact. The growth term is the flat
root above; fixing index fanout bounds is the prerequisite for every
other number to mean anything at scale.

Strategy implications, in order: (a) diagnose the leaf weight-vs-bytes
clamp gap; (b) bound index fanout deterministically (count AND byte
caps at every level — the g-tree critique made concrete by our own
census; this changes canonical form and needs owner sign-off); (c)
re-run every size/scale measurement after (a)+(b), since current
numbers reflect pathological shapes.

### Boundary-policy review (2026-07-30): why the pathology persists

Reviewed `notes/boundary-policy-experiment.md` (steps 1-5, all landed,
plus #399's weight-paced index ladder) against the census. Answer to
the owner's question — and one correction to the previous entry.

**What landed vs what is remembered.** The remembered design — "apply
pressure once past a threshold until the boundary is forced" — landed
as the step-4 WEIGHT BANK, but deliberately scoped: the bank
accumulates only across VETOED (provably uncuttable) seams and resets
at every accepted seam. Dolt-style pressure ("weight since the last
CUT", probability rising until forced) was explicitly rejected in the
step-4 note of record because a cut-outcome-fed coin cascades
downstream boundary decisions (edit locality lost). Natural runs are
therefore memoryless per-key renewal (expected leaf = S, tail e^-W/S)
with the 3x frame ceiling as the only hard bound. Pressure-until-
forced exists for vetoed atoms; it never applied to natural runs.

**Correction: the flat root is DESIGNED, not a coin failure.** The
previous entry blamed the 549-link root on the geometric coin failing
to cut. Wrong: #399's `weight_paced_seam_rank` paces index fanout at
`max_segment / link weight` — ~550-600 links at the defaults is the
INTENDED fanout, and the root frame is byte-bounded at ~S as the tree
grows. Consequence for the scale curve: the per-commit root rewrite
grows toward ~S and then plateaus; the equilibrium commit rewrites
roughly (root ~S) + (novelty buffer bytes) per commit — a structural
~(S + buffer)/commit-size ratio vs SQLite, which is the properly
attributed scaling term.

**Why blocks still exceed the clamp — four scope gaps, no broken
mechanism:**

1. **The bound unit is WEIGHT, not BYTES.** The ceiling holds in
   weight terms; encoded bytes drift ~1.9x past it (census max 5.6xS
   vs the 3xS weight ceiling — matching step 5's own caveat that "the
   byte tail lands near 2x the weight-implied figure"). Weight is a
   calibrated estimate (`cap::entry_weight` = key+32 for the coin;
   `Entry::weight` = key + `payload_weight` for budgets). For the
   network goal — bytes on the wire — the bound that exists is
   therefore ~6xS bytes (~390 KB at defaults), which does invalidate
   +-50 KB block sizing, exactly the owner's concern.
2. **The natural-run tail is a soft cap by design**: P(leaf > 2S) =
   e^-2 = 13.5%, hard-stopped only at the 3x weight ceiling. Factor 2
   exists as the tighter knob (measured 27-70% replay CPU in the
   experiment).
3. **The novelty buffer is byte-unbounded.** Buffers cap at 256 OPS
   with no byte cap; the census shows buffered index frames at 222 KB
   (8 KiB setting!) — buffer bytes ride every operational root block
   and every per-commit rewrite entirely outside the pacing policy,
   which predates buffered-by-default commits (group 3). For the
   per-commit push payload and the commit cost, buffer bytes DOMINATE.
4. **History rides the same tree**: each fact contributes multiple
   value-carrying keys across orderings and history records, so
   value-heavy bands recur across key regions.

**Follow-ups in value order** (owner sign-off wanted on 2-3):

1. Extend `live_census` to sum per-leaf `Entry::weight` and report
   weight-vs-ceiling next to bytes-vs-ceiling: separates estimate
   drift from enforcement in one run (expected: enforcement clean,
   drift ~1.7-2x).
2. **Byte-cap the novelty buffer** (trigger = min(op cap, byte cap)):
   bounds operational block sizes, per-commit rewrite cost, and push
   payloads. Local, cheap, and NO canonical-form change — buffers do
   not affect canonical shape. Likely the single highest-value item
   for both the size story and the scale curve.
3. Re-base the ceiling (or the weight calibration) on encoded bytes —
   persist knows exact bytes; a byte-informed calibration pass (or a
   persist-time byte check feeding the next regroup) closes the
   weight-to-byte drift without abandoning pure-function pacing.
4. Only then re-tune (S, factor) and re-run the scale curve and
   network models on bounded shapes.

### Veto census (2026-07-30): the veto is inert on the real workload

Owner question: does the vetoing cause issues? New `veto_census`
example walks the canonical tree in key order and evaluates the actual
`Geometric::vetoes` rule on every adjacent-key seam (10,000 real SE
txns, shipped defaults: max_separator 512, max_segment 64 KiB):

- **Zero vetoed seams out of 57,503** — across all three fact
  orderings. The veto never fires on this data at all. The Phase-A
  pathology the veto replaced came from the old DEMOTION rule (any key
  longer than max_separator ranked 0 unconditionally — every body key);
  the veto is pairwise (neighbors must SHARE a >512-byte prefix), and
  under real supersession churn near-duplicate large values do not
  coexist — the old revision is retracted before the new one lands. So
  vetoed stretches, the weight bank, and stretch anchors all idle in
  production shapes; they are insurance for coexisting near-duplicates
  (multi-valued attributes with similar large values, import shapes
  that assert revisions side by side).
- **The 20 over-byte-ceiling leaves have 0.0% veto share** — they are
  pure natural accepted-seam frames, confirming the oversize cause is
  the weight-to-byte drift (previous entry), not vetoed stretches.
- 13 forced links storing 7.6 KB of separators: the step-5 natural-
  frame ceiling firing rarely, as designed.

Issues assessment: the veto RULE causes no measurable shape, size, or
edit-cost issues on real data — it is not the pathology and not a
contributor. The veto/forced-run MACHINERY (window widenings, stretch
merges, anchor elections) is a different matter: it is the complexity
locus where the stale-path reshape bug lived, and real workloads
exercise it only through the rare ceiling anchors (the field crash at
max_segment 16 KiB came via ceiling-forced runs, not vetoes — smaller
targets mint many more forced anchors). Standing complexity with
near-zero production exercise argues for keeping the small-frame
repro fixture as the permanent exerciser (done, on the fix branch) and
for treating any future simplification of the widening machinery as
low-risk to real shapes. Caveat: this census is the artifacts-level
tree (no version-prefixed history region); repo trees add history keys
whose 40-byte shared version prefixes still sit far under the 512
bound, so the zero-veto finding should generalize, but a repo-level
census would confirm.

### Weight enforcement + ceiling-factor sweep (2026-07-30): variance is priced, not broken

Extended `live_census` decodes every leaf's entries and sums the exact
`Entry::weight` the ceiling meters, next to encoded bytes; ceiling
factor swept via `DIALOG_TREE_CEILING_FACTOR` on current (group-6A)
code, 10K real txns, S=64K:

| factor | replay us/txn | leaves | byte p50 / p99 / max | max weight vs ceiling | over weight ceiling |
|---|---|---|---|---|---|
| 3 (default) | 2183 | 284 | 60K / 268K / 365K | 0.98x | **0** |
| 2 | 2579 (+18%) | 333 | 57K / 208K / 216K | 0.9996x | **0** |
| 1 | 3735 (+71%) | 568 | 36K / 106K / 125K | 0.9994x | **0** |

**Enforcement is exact.** Zero leaves exceed the weight ceiling at any
factor; the max sits at 0.98-0.9996x of it. The whole "clamp not
holding" impression was DRIFT: encoded bytes / metered weight runs
p50 1.15, p90 1.85, max 2.1-4.4 (the high ratios are value-heavy or
tiny leaves). So the coin's variance is already hard-bounded — in
weight — and the bytes bound is (factor x S x drift).

**Tighter ceilings got cheap.** The boundary experiment measured
factor 2 at 27-70% replay CPU; on current code it costs +18% (and
factor 1 +71%) — the group-6A work (live spine, deferred flush,
memchr) changed the regroup economics. Factor 1 at S=64K yields a
125 KB hard byte max with 36 KB p50 today, no mechanism change.

Conclusion for the owner's "rethink the coin?" question: the renewal
coin does NOT need rethinking for the upper bound — determinism there
comes from the ceiling, which provably holds; the lever is (a) close
the weight-to-byte drift (recalibrate `payload_weight` against real
rkyv encodings and charge column overhead, so the ceiling denominates
in effective bytes), (b) choose (S, factor) for the byte target —
e.g. S=32K x factor 2 bounds blocks at ~64K weight = ~120K bytes
today, tighter after (a), and (c) the buffer byte-cap for operational
blocks (unchanged, still #1). A dolt-style pressure coin remains the
known alternative ONLY if a narrow two-sided size distribution ever
becomes a goal in itself (uniform fetch sizes, dedup chunking); it
buys nothing for the upper bound we actually need and costs edit
locality. If factor-1/2 regroup cost matters at scale, the
experiment's own noted lever — incremental segment-weight bookkeeping
on the edit path — is the follow-up, not a new coin.

### Buffer byte-cap + weight calibration landed, impact measured (2026-07-30)

Owner sign-off on items 2-3 with "measure impact". Both landed on
`claude/perf-7-oplog-spike`; all suites green (292/159/193), wasm32
clean.

**Item 2 — novelty buffer byte cap.** `Novelty` now carries exact
weight bookkeeping (lazy for spines opened from stored buffers — the
sealed columns stream once — exact under every mutator after), and the
flush trigger fires on `ops > op_cap OR weight > byte_cap`. Knobs:
`HitchhikerTree::with_op_buf_bytes` / `DIALOG_TREE_OP_BUF_BYTES`;
DEFAULT OFF pending the owner's choice. Measured (10K real txns,
replay us/txn; the cap bounds the operational root block, which
otherwise sawtooths to ~200 KB+ of buffered ops riding every commit
rewrite and every push):

| byte cap | replay | operational root at snapshot |
|---|---|---|
| off | 1765 | 52 KB (sawtooths unbounded) |
| 64 KiB | 2068 (+17%) | <= cap + one commit |
| 32 KiB | 2618 (+48%) | <= cap + one commit |
| 16 KiB | 3159 (+79%) | <= cap + one commit |

Recommendation: 64 KiB (~S) as default — bounded push payloads and
commit rewrites for +17%; workloads that prize commit latency can
raise it.

**Item 3 — weight-to-byte calibration.** Per-leaf component dump +
least-squares fit found the drift is a flat ~64-72 bytes/entry of
encoding overhead (columnar offsets, dictionary/value-table framing)
that `Entry::weight` did not charge; key bytes track 1.01x and payload
~flat. Landed as `ENTRY_ENCODING_OVERHEAD = 64` charged by
`Entry::weight` (and the buffer cap's metering). Measured:

| config | max leaf bytes vs byte ceiling | replay |
|---|---|---|
| factor 3, uncalibrated | 365 KB = 1.86x | ~1.8-2.2 ms/txn |
| **factor 3, calibrated** | **198 KB = 1.007x** | 1735 us/txn (no cost) |
| **factor 2, calibrated** | **134 KB = 1.03x** | 2052 (+18%) |

The frame ceiling now denominates in effective bytes: blocks are hard-
bounded at ~factor x S x 1.02. Combined with the byte-capped buffer,
"blocks come out arbitrary sizes" is closed at both the canonical and
operational layers; the (S, factor) pair is now an honest byte knob
(e.g. factor 2 x S=32K ~ 66 KB hard bound for the ~50 KB network
goal). CAVEAT: the calibration changes canonical shapes (boundary
decisions move), so trees built before/after diverge byte-wise —
fine pre-ship per the boundary note's "nothing has shipped", but it
is a coordination point. One fixture updated for the new metering
(`it_anchors_frames_only_at_accepted_seams`: its cluster crossed the
stretch target under the calibrated weight, which made the stretch
backstop fire — correctly — inside it).

### Op cap vs byte cap (2026-07-30): the byte cap subsumes the count cap

Owner question: any point keeping `ops > cap` next to the byte cap?
Measured (byte cap 64 KiB fixed, op cap swept):

- Small-op synthetic (2000 per-row commits): op cap 256 / 384 / 512 /
  1024 / 4096 / 1,000,000 all replay in 1.25-1.31 s — identical. The
  byte cap binds at ~380 small ops and nothing downstream cares about
  the count.
- Real SE (10K txns): op cap 256 vs 1,000,000: 1553 vs 1629 us/txn —
  within noise (the byte cap binds at ~70 of SE's ~900-byte ops, far
  under 256 anyway).
- A first A/B run showed a 6.4x slowdown and an OOM kill for the
  no-op-cap arm; a clean-state rerun could not reproduce either — it
  was machine-state contamination (the killed 10K run poisoned the
  timings that followed). Recorded as a methodology reminder: A/B
  arms interleaved on a dirty machine are not measurements.

Analytically the subsumption is structural: the metering charges every
op at least ENTRY_ENCODING_OVERHEAD (64) + key bytes, so a byte cap of
B implies a hard count bound of ~B/64 (about 1000 at 64 KiB, typically
far fewer). Recommendation: once the byte cap is the default, the byte
cap is THE knob; keep the count check itself (one integer compare, and
dozens of tests plus the wasm path — where the env plumbing does not
exist — drive cascades through `with_op_buf_size`), but it stops being
a tuning parameter.

### Post-calibration scoreboard check (2026-07-30): held or improved

Owner question: did the calibration (canonical shapes moved) improve
or regress the numbers? Same-machine re-run vs the recorded post-6A
figures:

| metric | before | after calibration |
|---|---|---|
| SE replay wall (500 txns) | 204-213 us/txn | 202-210 us/txn |
| SE replay callgrind Ir (500) | 745.7M | 751.0M (+0.7%) |
| criterion dialog_mem write (500) | 103.9 ms | 99.5 ms (criterion self-delta -3.4%, p<0.05) |
| se_kind_lookup dialog_mem | 258 us | 236 us (-8.5%; sqlite control also drifted -4-8%) |
| se_title_get dialog_mem | 7.3 us | 7.8 us (wide CI, noise) |
| 10K-txn replay (census tool) | 1765-2183 us/txn | 1735 us/txn |

Verdict: flat to slightly improved everywhere — the byte-bounded
shapes (198 KB max leaf at factor 3, was 365 KB) came at zero
performance cost, and the value-indexed scan got a bit faster from
the smaller leaves. SQLite control rows drifted with the machine, so
the relative standing (dialog_mem ~ parity with durable sqlite_disk at
small scale) is unchanged.

### Byte cap adopted as the default (2026-07-30)

Owner call: "sounds like we should adopt it and continue chasing
things to improve performance." `DEFAULT_OP_BUF_BYTES = 64 KiB` is now
what trees open under (`DIALOG_TREE_OP_BUF_BYTES` still overrides;
explicit 0 disables the byte trigger). With the default on, the
500-txn SE replay lands at 241-246 us/txn — the expected ~+17% over
the uncapped 202-210, buying a bounded operational root block (the
thing every commit rewrites and every push ships).

What the cap does NOT do: flatten the scale curve. `scale_curve 25000
2500` with the default on still climbs 703 -> ~4,100-5,400 us/txn
(27-53x sqlite_mem), same shape as uncapped. The earlier attribution
of the scaling term to buffer bytes was wrong; the windowed byte-volume
measurement (measure_se_replay, now reporting per-window reads and
wall time too) isolates the real signal — bytes moved per commit grows
~3.5x across the run:

| window (25K txns, 2.5K window) | sets/commit | write B/commit | gets/commit | read B/commit | us/commit |
|---|---|---|---|---|---|
| first | 3.8 | 100K | 0.8 | 63K | 730 |
| last | 5.5 | 346K | 2.4 | 223K | 4,400-5,600 |

Per-commit time tracks bytes-moved almost linearly. The growth is in
block sizes and touch counts, not buffer size — candidate mechanisms
(unattributed as of this entry): flush write-amp into ceiling-sized
leaves at depth, supersession-scan read volume, root frame growing
toward S. Attribution is the active thread.

## Deferred decisions (owner-reviewed)

- **Batch-signing commits** (2026-07-28): approved direction for the
  two-Ed25519-per-commit cost (one signature over a small Merkle root of
  the record + revision payloads; both stay standalone-verifiable via a
  1-hash inclusion proof). Deliberately deferred until the larger costs
  (storage I/O, DCAA outcome) are optimized away; revisit once the
  repo-commit profile is no longer dominated by them.
