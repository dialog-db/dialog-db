# What an ephemeral-only stack commit costs

Measured on `claude/state-layers` at fb7db24, with the ungated scenarios this
change adds to `dialog-perf` (`rust/dialog-perf/src/scenario/stack.rs`). The
question is whether a stack can carry a UI's per-frame events and session state.

## Setup, and where it departs from the brief

- **The stack is `[space, application, top]`, not `[space, application]`.** A
  scope name is bound only by a link, and a layer cannot link itself, so the
  layer that `memory:application` names cannot be the top. `top` is a second
  `Ephemeral` that links `application` under `memory:application`, and
  `application` links the space under `memory:space`. Tonk's reactor has the
  same shape. That makes one more layer to refresh per commit, which the
  numbers below include.
- **Placements and the rule are committed to the space's tree** in setup, not
  asserted into `branch.overlay()` as tonk does. `ui/click` is
  `TransientAttribute`. `ui/click`, `ui/count` and `ui/cursor` are placed on
  `memory:application`.
- **Setup makes one stack commit off the record**, which seeds the counters
  (or the cursor) at 0. The first stack commit induces over the bottom's whole
  lag, which here is the seed of `size` entities. Without the warm-up commit,
  that one-time cost would land in the first measured round.
- **The `main` counterparts (`branch-cursor`, `branch-subscribe`) run on this
  branch, in the same binary**, not on `main`. They need no API that `main`
  lacks, and one binary removes toolchain and build differences from the ratio.
  `branch.overlay().apply(..)` is a write into the session store with no
  transaction, induction or settlement. It is the floor, not like-for-like with
  a stack commit.
- **`stack-frame` puts each click on its own counter** (`click-f-i` → `counter-i`,
  with 100 counters seeded at 0). With one shared counter, the rule's 100
  conclusions would collapse into one.
- **Instruction counts come from this container's `rustc`**, not the nix
  toolchain the baselines use. The README puts that gap at 2 to 6%. Ratios are
  unaffected.

### Gating

`compare` fails a scenario that has no baseline file (`compare.rs:384`,
"a scenario the sweep ran with no baseline is ungated, and ... both fail").
So the new scenarios are **not** in `catalog()`. They are in a separate
`ungated()` list:

- `perf list` prints them under an `ungated` heading.
- `perf run <name>` runs them.
- `perf sweep --only <name>` runs them.
- A full sweep (the gate's) never runs them.

Nothing under `perf/baseline/`, `compare.rs`, any tolerance, or
`.github/workflows/test.yml` changed. Whether they become gated is the
maintainer's call.

Reproduce one with:

```sh
valgrind --tool=callgrind --collect-atstart=no --toggle-collect=dialog_perf_measured \
  --cache-sim=no --callgrind-out-file=out.callgrind \
  target/release/perf run stack-cursor --size 20000
```

## A correctness bug found first: a rule's `last` head accumulates in an ephemeral layer

`stack-event` should end with one `ui/count` of 200. Instead the application
layer holds **201 counts, 0 through 200** (`counted` = 20100 = 0 + 1 + … + 200).
`stack-frame` holds 21 per counter (`counted` = 21000). Asserting `ui/cursor`
under `last` directly, from the transaction, holds 1 (`cursors.held` = 1). So
only the conclusions of rules are affected.

The cause:

1. Induction settles a head's succession with
   `succession::resolve_against`. `Cell::write` (`succession.rs:607-648`)
   retracts the elected value only when it is a *claim*.
2. The row of a standalone ephemeral layer is not a claim, by design, so
   nothing is retracted, and the write is rewritten as
   `Change::Assert(value, Pick::All)`.
3. The layer then inserts the value beside the old one.
4. The transaction's own writes avoid this because `StackCommit::perform`
   exempts them from settlement through the `stores` set
   (`stack.rs:2931-2945`), so they reach `Ephemeral::apply` with their pick.
   Induced heads have no such exemption.

The effect on the numbers: a counter's cell grows by a row per commit, and
the rule body reads it, so `stack-event` is quadratic in the number of commits.
Fitting runs of 50, 100 and 200 rounds (scratch build, not committed):

| rounds | instructions | per commit |
|---:|---:|---:|
| 50 | 166.2 M | 3.32 M |
| 100 | 379.7 M | 3.80 M |
| 200 | 949.9 M | 4.75 M |

That fits `2.85 M + 18.9 k × (rows in the cell)` per commit.

A second inductive rule, `retract! ui/count{prev} when click, count prev`,
holds the cell at one row. With it, the cost is linear: 177.4 M for 50 rounds
and 711.4 M for 200, **3.55 M per commit**, two rules' evaluation included. So
the steady-state cost of the event loop is about 2.9 to 3.5 M instructions per
commit. The 4.75 M in the table below is an average inflated by the leak.

For a UI the leak matters more than the cost: a `last` resource that a rule
updates every frame grows without bound for as long as the process lives.

## Results

Every count is callgrind's over the measured phase. "Per op" divides by the
number of commits, or by 2000 clicks for `stack-frame`.

| scenario | size | instructions | per op | outcome |
|---|---:|---:|---:|---|
| stack-event | 2000 | 949,883,393 | 4.75 M / commit | counted 20100, clicks held 0 |
| stack-event | 20000 | 949,151,492 | 4.75 M / commit | same |
| stack-cursor | 2000 | 236,273,590 | 1.18 M / commit (1.13 M commit + read share) | position 200, rows 1, held 1 |
| stack-cursor | 20000 | 235,776,855 | 1.18 M / commit | same |
| stack-frame | 2000 | 2,012,167,891 | 100.6 M / frame, 1.01 M / click | counted 21000, clicks held 0 |
| stack-subscribe | 2000 | 476,611,358 | 2.38 M / round (1.165 M commit + 1.175 M poll) | deltas 200, maintenances 200, recomputes 0 |
| stack-subscribe | 20000 | 475,491,962 | 2.38 M / round | same |
| branch-cursor | 2000 | 5,774,878 | 28.9 k / round (21.5 k write) | position 200, rows 1, held 1 |
| branch-cursor | 20000 | 5,963,891 | 29.8 k / round | same |
| branch-subscribe | 2000 | 41,164,236 | 206 k / round (33.7 k write + 167 k poll) | deltas 200, maintenances 200, recomputes 0 |

### Counters

All scenarios, at both sizes:

| counter | stack-* | branch-* |
|---|---:|---:|
| `archive.get` / `.bytes` | 0 / 0 | 0 / 0 |
| `archive.put` / `.bytes` | 0 / 0 | 0 / 0 |
| `span.commit`, `span.write_instructions`, `span.seal_batch`, `span.buffer_records`, `span.apply_batch_reusing` | 0 | 0 |
| `span.settle_cell` | 0 | 0 |
| `span.settle` (the commit's succession pass) | 1 per commit (200; 20 for `stack-frame`) | 0 |
| `span.resolve_bundle`, `span.resolve_rules` | 1, 1 | 1, 1 |

No stack scenario reads or writes an archive block, and none commits a tree.
The bottom's induction cell, a memory cell outside the archive meter, is
resolved twice per commit (`Resolve<Revision>`, once in the commit and once in
`catch_up_induction`). It is never published in steady state: the watermark is
already at the head, so `catch_up_induction` returns before `cell.publish`.
Two caveats:

- This is a volatile store with warm caches. The tree reads that induction
  and placement resolution make go through the branch's head caches, beneath
  which the meter sees nothing. The 2000 → 20000 flatness, not the zeros, is
  what shows they do not scale with the tree.
- The warm-up commit absorbed the lag. A cold stack's first commit does not
  have these zeros.

## Where a commit's instructions go

Shares are of the scenario's total, from `callgrind_annotate --inclusive=yes
--tree=calling`. Rows marked "of which" are already counted in the row above
them.

| bucket (functions) | stack-cursor | per commit | stack-event | per commit |
|---|---:|---:|---:|---:|
| **Metadata overlay** (`QueryLayer::overlay`: `derive_metadata` builds `dialog.branch/*` facts, then `Ephemeral::holding` loads them into a throwaway store) | 128.7 M, 54.5% | 643 k | 190.2 M, 20.0% | 951 k |
|   of which `derive_metadata` | 73.3 M, 31.0% | | 108.9 M, 11.5% | |
|   of which `Ephemeral::holding` (apply + mint into the throwaway store) | 55.3 M, 23.4% | | 81.2 M, 8.5% | |
| **Induction** (`induce`, minus the overlay it builds) | 17.6 M, 7.4% | 88 k | 668.3 M, 70.4% | 3.34 M |
|   rule body (`fire_seeded`, minus `emit_matches`) | 0 | | 452.3 M, 47.6% | |
|   head succession and novelty (`emit_matches`) | 0 | | 162.8 M, 17.1% | |
|   trigger probing (`Dispatch::load`/`triggers`/`expand_through_deduction`, `Reach::probes`, `OverlayTriggers::scan`) | 5.4 M, 2.3% | 27 k | 27.3 M, 2.9% | 137 k |
|   watermark lag (`lag_delta`) | 3.7 M, 1.6% | 19 k | 3.7 M, 0.4% | 19 k |
| **Watermark cell** (`Resolve<Revision>` in the commit, `catch_up_induction`) | 9.7 M, 4.1% | 48 k | 9.6 M, 1.0% | 48 k |
| **Placement resolution** (`Placements::resolve` ×3 per commit, `placement_of`, `scopes`, `route`) | 6.7 M, 2.8% | 33 k | 6.9 M, 0.7% | 35 k |
| **Commit succession** (`succession::settle`) | 5.5 M, 2.3% | 27 k | 4.6 M, 0.5% | 23 k |
| **Capture** (`Stack::capture` → `refresh_links`: link refresh and each layer's write) | 38.3 M, 16.2% | 192 k | 40.2 M, 4.2% | 201 k |
|   of which `Topology::link_facts` (15 link facts rebuilt per refresh) | 10.0 M, 4.2% | | 9.7 M, 1.0% | |
|   of which `Ephemeral::apply_held` (2 per commit: `application` and `top`) | 27.3 M, 11.6% | | 29.5 M, 3.1% | |
| **Witness landing** (`StackWitness::land`) | 0 | | 1.9 M, 0.2% | 10 k |
| **Observer bookkeeping on the stack** (`Entered::drain`/`track`/`pending`) | 3.5 M, 1.5% | 18 k | 3.4 M, 0.4% | 17 k |
| `composite_at` and topology clones | 4.5 M, 1.9% | 22 k | 4.2 M, 0.4% | 21 k |
| **Per-instant hash**: `blake3` under `State::mint_held` (all callers) | 6.2 M, 2.6% | 31 k | 9.1 M, 1.0% | 46 k |
| observer fan-out: `Instant` clones under `mint_held` | 2.4 M, 1.0% | 12 k | 2.9 M, 0.3% | 15 k |
| `Settling::release` (holds) | 0.01 M, under 0.01% | | similar | |

Totals and roll-ups that cut across the buckets:

- `ephemeral::State::apply`, all callers: 55.1 M (23.3%) for cursor, 68.9 M
  (7.3%) for event. Most of it is the throwaway overlay stores and the 15
  link facts. Each link fact goes through a `last` cell scan
  (`Facts::scan`, 7705 calls in cursor).
- `base58::to_base58` is 34.3 M (14.5%) of cursor, spread across
  `derive_metadata` (`Revision::entity`, `BranchHash`) and the link entities.
  It is the single largest leaf.

`StackPublish::perform` and `StackCommit::perform` wrap the commit and account
for 96.0% / 94.3% of cursor and 99.1% / 98.7% of event. The read in cursor is
the remaining ~6.4 M.

### Top five inclusive functions

Commit wrappers and harness frames are left out.

`stack-event` (2000):

1. `induce` — 795.8 M (83.8%)
2. `induce::fire_seeded` — 615.1 M (64.8%), 2 per commit
3. the rule body's selection (`TryCollect` → `ConceptQuery::evaluate_unsorted` → `AttributeQueryOnly::evaluate`) — 401.5 M (42.3%), the `ui/count` premise over the leaking cell
4. `QueryLayer::overlay` — 190.2 M (20.0%), 3 per commit
5. `induce::emit_matches` — 162.8 M (17.1%)

`stack-cursor` (2000):

1. `QueryLayer::overlay` — 128.7 M (54.5%), 2 per commit
2. `induce` — 81.6 M (34.6%), 64.1 M of it the overlay above
3. `QueryLayer::derive_metadata` — 73.3 M (31.0%)
4. `Ephemeral::holding` — 55.3 M (23.4%)
5. `ephemeral::State::apply` — 55.1 M (23.3%)

## Answers

### What does one ephemeral-only stack commit cost, and how does it scale?

- **Session-state write** (`stack-cursor`): **1.13 M instructions per commit**
  at 2000 entities and 1.13 M at 20000 (226.9 M and 226.4 M over 200
  commits). That is flat: 0.2% apart, with every counter equal.
- **Event loop** (`stack-event`): 4.75 M per commit as measured at both sizes
  (949.9 M and 949.2 M), but that average is inflated by the `last` leak. The
  steady state is **about 2.9 to 3.5 M per commit**, still flat in size. Rule
  evaluation is about two thirds of it.
- **Batched frame** (`stack-frame`): **1.0 M per click** at 100 clicks per
  commit, 100.6 M per frame. Batching a frame's events into one commit is
  2.9 to 3.5× cheaper per event than a commit per event, because the
  per-commit fixed cost is paid once.

Nothing measured scales with the bottom tree. The cost is fixed per commit
plus per instruction the commit carries. At 60 frames per second, one cursor
commit per frame is about 68 M instructions a second, and one event commit
per frame about 200 M. On a wasm main thread, on a phone, that is most of a
frame budget before any rendering. With the leak, it also grows every frame.

### What fraction is work a durable commit needs and an ephemeral-only one does not?

For `stack-cursor`, about **0.9 M of the 1.13 M (≈ 78%)**:

| work | share of cursor | per commit |
|---|---:|---:|
| metadata overlay rebuilt twice per commit | 54.5% | 643 k |
| watermark lag read and the two cell resolves | 5.7% | 67 k |
| placement scans (×3) | 2.8% | 33 k |
| trigger probing of the tree's rule slice | 2.3% | 27 k |
| link refresh: 15 facts rebuilt and re-applied on `top`, which captures `application` for nobody to read (a Process layer's revision outlives nothing) | ~12% | ~140 k |
| holds | under 0.01% | |

None of these depends on what the commit writes; they depend on heads that
did not move:

- **The overlay is the main cost**, and it is a stack-path regression rather
  than an inherent cost. The branch path builds it once (206 k in all of
  `branch-subscribe`, 160 k in `branch-cursor`). The stack path calls
  `QueryLayer::overlay` afresh from the commit's settlement view and again
  inside `induce`, each time deriving `dialog.branch/*` facts and minting them
  into a new `Ephemeral` with hashing. Keying it on the bottom's revision
  would remove more than half of a cursor commit.
- **The watermark, placements and triggers** only matter when the bottom has
  moved, so they too could key on the bottom's head.

For `stack-event`, the same work is about 0.95 M of the 4.75 M (20%), or about
30% of the leak-free 3.0 to 3.5 M. Rule evaluation is the rest.

Placement resolution is the one that an ephemeral commit still needs in
principle, to route; it only needs it once per bottom head. Link refresh is
needed only if something reads `top`'s captured revision of `application`.

### Is the per-instant chained hash a measurable share?

It is measurable but small: **2.6% of cursor (31 k per commit) and 1.0% of
event (46 k per commit)**, as `blake3` under `State::mint_held`, across
about four instants per commit.

Half of those instants are minted into the throwaway overlay stores, which no
observer reads and whose revision nothing keeps. So:

- Caching the overlay would halve the hashing anyway.
- A `Process`-audience layer using a plain sequence number would then save
  roughly 15 k per commit, **about 1.3% of a cursor commit and under 0.5% of an
  event commit**.

That is not where the cost is. Base58 encoding of entities (14.5%) and `last`
cell scans in `State::apply` are each larger. The better question is why a
commit mints four instants and builds two stores that nothing observes.

### What does going through the stack add per write and per poll?

| | stack | branch session store | added | ratio |
|---|---:|---:|---:|---:|
| per write (`*-cursor`) | 1.13 M (commit path) | 21.5 k (`apply_held`) | +1.11 M | ~53× |
| per write (`*-subscribe`) | 1.165 M | 33.7 k (more observers to fan out to) | +1.13 M | ~35× |
| per poll (`*-subscribe`) | 1.175 M | 167 k | +1.0 M | ~7.0× |
| the one read (`*-cursor`) | ~6.4 M | ~1.4 M | +5 M | ~4.5× |

Both subscriptions maintain every poll incrementally (200 maintenances, 0
recomputes), so the difference is not a recompute. Of the stack poll's
1.175 M, 627 k (53%) is the same metadata overlay, rebuilt twice per poll
(254.0 M of overlay in `stack-subscribe` against 128.7 M in `stack-cursor`).

The per-write comparison flatters the branch. `overlay().apply` skips the
transaction, induction and settlement entirely. A fair branch counterpart, a
branch transaction routing `ui/cursor` to a session-bound scope, is not
measured here. It would separate the cost of the transaction from the cost of
the stack, and would be the next scenario to add if the question is "stack or
branch" rather than "stack or no transaction at all".

## What to ask next

- Is the `last` leak the first thing to fix? Yes: it makes the event shape
  quadratic and unbounded in memory, and it changes what a `last` resource
  means only when a rule writes it.
- Should a commit build the metadata overlay at all? Only if the induction or
  settlement view reads `dialog.branch/*` facts. When no rule does, it is pure
  overhead, and a 2× cheaper cursor commit costs one cache keyed by revision.
- Is per-commit capture of an ephemeral top worth 12%? Only if something
  reads the top's head as "the composite's hash" within the process.
- The bevy comparison is a system over a resource in a frame loop at a few
  hundred instructions per event. A stack commit is three to four orders of
  magnitude above that even after the fixes above. The design question is
  whether per-frame events belong in a commit at all, or in the ephemeral
  layer's observer path, with commits reserved for what a rule or a peer must
  see.
