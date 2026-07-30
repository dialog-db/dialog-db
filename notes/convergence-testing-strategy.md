# Convergence testing strategy

Twice now a convergence bug has shipped past a large, carefully written test
suite and surfaced only in the field: the search-tree ranking/deletion fix
(#332), and the stale reshape path after a forced-run widening fixed on
`claude/fix-buffered-reshape`, which required replaying the real Stack
Exchange fact log with `DIALOG_TREE_MAX_SEGMENT=16384` for a few thousand
buffered commits before it fired. A third is under investigation as this is
written. This note diagnoses why bugs of this class escape the current suite
and lays out a strategy for finding them before the field does.

## The property, stated precisely

Convergence (history independence) is the load-bearing correctness property
of the whole replication story: two replicas that arrive at the same logical
fact set must hold byte-identical trees, or structural sync, diffing, and
content-addressed sharing all silently degrade or break.

It is worth pinning the exact statement, because tests can only check a
property that has been stated:

> For a fixed `Manifest`, the persisted canonical tree is a pure function of
> the net entry set. Any program of inserts and deletes — in any order, any
> batching, through any API surface (`TransientTree` sequential or batched,
> `HitchhikerTree` under any flush policy followed by `canonicalize`, stitch
> or carve reassembly, replica reconciliation), interleaved with any number
> of persist/reopen cycles — whose net effect is entry set `E` must produce
> the same root hash, byte for byte, as the simplest possible reference
> build of `E`.

Corollaries that deserve their own tests: reads through any surface
(buffered gets, range streams over unflushed novelty, differential over a
buffered spine) must agree with reads over the canonicalized tree; and a
persisted-then-reopened tree must behave identically to the never-persisted
one under every subsequent program.

## Why the current suite misses these bugs

The suite's *oracles* are already right. `tree/transient.rs` and
`hitchhiker.rs` are full of exactly the correct equivalence assertions:
batched-vs-sequential, insert-order permutations, incremental-deletes vs
scratch rebuild of survivors, buffered-vs-canonical. The escaped bugs are
not oracle failures; they are *input search* failures. Three axes of the
input space are under-sampled, and the bugs live in their intersection:

1. **Key shape.** The randomized tests draw uniform keys from small domains.
   The field bug needed long-shared-prefix clusters straddling the separator
   bound (so in-cluster seams are vetoed), with churn concentrated on
   cluster head keys (whose short separators are where index cuts punch, so
   deleting one dissolves a cut and re-derives the seam). Uniform sampling
   essentially never generates this; the fix commit had to hand-construct it
   by scaling down the observed field shape.

2. **Manifest parameters.** Tests overwhelmingly run the default manifest.
   The field failures appeared at `max_segment` 16384 and 8192 — tight
   ceilings that make forced runs, frame splits, and their widenings common
   instead of rare. Shape-rule interactions that need three mechanisms to
   fire in one edit are reachable in minutes under a tight manifest and
   perhaps never under the default at test sizes.

3. **Lifecycle / horizon.** The randomized loops run one batch over one
   fresh tree. The field bug needed the buffered commit lifecycle — open
   over a persisted root, a few ops, persist, reopen — repeated thousands of
   times, so that stale state laid down by one edit is tripped over by a
   later one. Single-shot tests structurally cannot see cross-edit staleness.

Two further structural gaps:

4. **No canonical-form validator for real trees.** The `helpers::validate()`
   that exists checks hand-written `tree_spec!` descriptors, not trees the
   code actually built. Convergence checks today are end-to-end comparisons
   against a rebuild, which detect divergence only at the final root and say
   nothing about where or when it crept in.

5. **No automated search or shrinking.** The 200-seed loops use a hand-rolled
   xorshift with no coverage feedback and no shrinker. Notably, both field
   bugs were *detected* by internal assertions ("Re-shape path descended
   into a node that was not lifted") rather than by wrong output — the
   detectors exist; nothing at test time drives execution into the corners
   where they fire.

## Strategy

The plan is layered. Each layer is independently useful; together they turn
"hope the field finds it" into a standing search process.

### 1. A single convergence harness (the foundation)

Build one reusable harness in `dialog-search-tree` (behind the existing
`helpers` feature) that everything else — property tests, fuzz targets,
soaks, regression tests — drives through:

- **`Program`**: a serializable value describing one experiment: a
  `Manifest`, a sequence of ops (insert/delete with generated keys/values),
  and lifecycle markers (batch boundaries, persist/reopen points, flush /
  canonicalize points, checkpoints).
- **Executors**: functions that run a `Program` through each surface —
  sequential canonical edits, batched `TransientTree` edits, `HitchhikerTree`
  under each flush policy, persist-every-k-ops with reopen, stitch/carve
  reassembly, and (for pairs of programs) two-replica reconcile.
- **Oracles**, checked at every checkpoint, not just at the end:
  - root equality across all executors and against the reference build;
  - read equivalence against a `BTreeMap` model (point gets, range streams);
  - the canonical-form validator (layer 2 below);
  - all internal `debug_assert!`s live, since the harness runs in dev builds.

The regression test on `claude/fix-buffered-reshape` is precisely one
hand-compiled instance of this harness. The harness makes that class of test
the cheap default instead of a per-bug heroic effort.

### 2. A canonical-form validator for real trees

The shape rules are history-independent, which means they are *locally
re-derivable*: given a persisted tree and its manifest, a walker can recompute
every seam decision (distribution coin from the key, separator-bound veto,
frame-ceiling force, anchor placement) and every node-level bound from the
keys alone, and confirm the tree is **the** canonical tree for its contents —
not merely *a* well-formed tree.

This is the highest-leverage single artifact to build:

- It converts every existing test, soak, and fuzz run into a convergence
  check without needing a reference rebuild (O(n) walk instead of O(n log n)
  rebuild), so it can run after *every batch* in long-horizon runs.
- It localizes failures: "node at height 2, child 4 violates the seam rule
  for key K" instead of "root hash differs after 3,000 commits".
- It doubles as a field/ops diagnostic (`dialog-diagnose` could grow a
  `verify` command over any stored root).

A buffered (hitchhiker) variant checks the weaker invariants that hold with
novelty in place: routing correctness of every buffered op, buffer bounds,
and that the underlying spine is canonical for its flushed contents.

### 3. An adversarial workload generator

Replace uniform sampling with a generator whose vocabulary is the known
structure-stressing patterns, composed randomly:

- long-shared-prefix key clusters, sized to straddle the separator bound;
- keys just under / at / over the separator-length bound (veto pressure);
- churn concentrated on cluster heads and on current node boundaries
  (delete-the-minimum, delete-the-separator-owner);
- dense monotone runs (frame-ceiling pressure) and sparse scatters;
- insert/delete/reinsert of the same keys (cut dissolve and re-punch);
- value-size mixes crossing the inline/spill threshold.

Crucially, the generator also draws the *manifest* (tight `max_segment`,
small branch factors, tiny novelty capacities) and the *lifecycle* (batch
sizes from 1 to thousands, persist/reopen frequency, flush policy). The bug
that escaped was a conjunction across all three axes; the generator's job is
to sample conjunctions, not marginals.

Every future field bug feeds back here: the diagnosis work that scaled the
Stack Exchange shape down into forty vetoed clusters is exactly a new
generator pattern — institutionalize that step in the bug-fix checklist
(fix + regression test + generator vocabulary + any new internal assertion).

### 4. Coverage-guided fuzzing over the harness

Wire `Program` to `arbitrary` and add a `cargo-fuzz` target that decodes a
`Program`, runs the executors, and asserts the oracles. Coverage guidance is
qualitatively different from seeded random loops: it is rewarded for reaching
new branches, and the interesting bugs live behind branch conjunctions
("punched cut dissolves" AND "left spine fuses in a shared ancestor" AND
"forced-run widening merges left of the main path"). The existing internal
assertions become free fuzzing oracles.

Practical notes: the tree API is async but the harness can `block_on` a
current-thread runtime; keep per-input programs modest (hundreds of ops) and
let the corpus, not one giant input, carry the diversity. Check the corpus
plus shrunken crashers into the repo so CI replays them as regression tests.

For `cargo test` ergonomics, mirror the same generator through `proptest`
(already a workspace dependency via `dialog-ucan-core`) to get automatic
shrinking on failures — the manual scale-down work in the fix commit is what
a shrinker automates.

### 5. Long-horizon soak and field-replay corpus in CI

Both escaped bugs were found by replay; make replay a standing CI asset
rather than an ad-hoc diagnostic:

- A nightly job replaying the Stack Exchange log (via `scripts/se-transform.py`
  and the `profile_replay`-style driver) through the buffered commit path
  across a matrix of `DIALOG_TREE_MAX_SEGMENT` values (e.g. 4096, 8192,
  16384, 65536) and flush policies, running the canonical-form validator at
  checkpoints and a full canonicalize-vs-rebuild comparison at the end.
- A synthetic long-horizon soak from the adversarial generator (fresh seed
  nightly, seed logged for reproduction) with the same checks.
- Time-box these to fit a nightly budget; the PR-gating suite stays fast.

### 6. First-divergence bisection tooling

When a soak or replay fails at commit N of thousands, the expensive step is
turning that into a small test. Build a diagnostic mode into the harness:
re-run the program checking the validator (or a reference rebuild at coarse
intervals, then binary-search) to find the *first* divergent edit, then use
the `differential` module to diff the divergent tree against the canonical
one and print the offending subtree. This is what turns "nightly soak red"
into "unit test by lunchtime".

### 7. Replica-level convergence (the property end-to-end)

The same discipline one level up, where the property actually matters to
users: N simulated replicas over in-memory storage, each applying a
generated fact stream with random partitions, sync points, and reconcile
orders (`it_reconciles_*` in `hitchhiker.rs` are the seed of this), asserting
all replicas converge to byte-identical roots — and, once the revision-DAG /
observed-remove merge work (#389) lands, that merge outcomes are order- and
grouping-independent. Everything stays deterministic (seeded scheduling, no
wall-clock), so failures replay exactly.

### 8. Measure the suite: mutation testing

Run `cargo-mutants` over `dialog-search-tree` (nightly or weekly, not
PR-gating) scoped to the shaping logic (`tree/transient.rs`, `hitchhiker.rs`,
`distribution.rs`). Surviving mutants in seam/veto/widening code are precise,
actionable evidence of assertion gaps — a direct answer to "where else are
we blind?", which is the question that motivates this note.

## Suggested order of attack

| Step | Artifact | Why first |
|---|---|---|
| 1 | Canonical-form validator (layer 2) | Biggest single win; upgrades every existing and future test; helps the investigation in flight *today* |
| 2 | Harness + adversarial generator (1, 3) | Turns the fix-branch test pattern into infrastructure |
| 3 | Proptest integration + fuzz target (4) | Automated search with shrinking over the harness |
| 4 | Nightly replay/soak + bisection (5, 6) | Institutionalizes what actually caught both bugs |
| 5 | Replica-level simulation (7) | End-to-end property, grows with #389 |
| 6 | Mutation testing (8) | Ongoing measurement of suite strength |

The theme across all of it: keep the oracles the suite already has, state
the property once, and replace human imagination of inputs with generated,
coverage-guided, long-horizon search — plus enough internal assertion
density that divergence is caught at the edit that causes it, not at the
root hash three thousand commits later.
