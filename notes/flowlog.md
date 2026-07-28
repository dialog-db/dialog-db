# FlowLog: what transplants to dialog

> Reading of FlowLog (VLDB 2026, [arXiv:2511.00865]) against dialog's engine: which of its techniques
> transplant, which validate choices already made, and which are explicitly rejected. Companion to
> [`query-engine-design.md`](./query-engine-design.md) (the engine as it is),
> [`incremental-subscriptions.md`](./incremental-subscriptions.md) (why pull, not push),
> [`dbsp.md`](./dbsp.md) (the incremental algebra), and
> [`goal-directed-fixpoint.md`](./goal-directed-fixpoint.md) (demand-seeded recursion).

## What FlowLog is

[FlowLog] is a Datalog-to-[Timely] compiler from Koutris's group (UW–Madison): Soufflé-syntax
programs compile through **Parser → Typechecker → Stratifier → Planner → Codegen** into standalone
[Differential Dataflow] executables, with three support modules (catalog, optimizer, profiler). Its
headline claim is that an **explicit relational IR per rule** — separating recursive control
(semi-naive execution) from each rule's logical plan — lets one engine get both Datalog-specific
optimization and off-the-shelf database primitives. On the DOOP points-to suite it beats Soufflé
20/20 (geomean 3.62× at 32 threads), paying ~2.3× Soufflé's peak memory for it.

## What is explicitly *not* transplantable

- **The architecture.** FlowLog is push/world-driven: deltas enter at the leaves and every stateful
  operator retains its fully integrated input (DD arrangements) to be ready for any delta. That
  retained state is exactly what [`incremental-subscriptions.md`](./incremental-subscriptions.md)
  rejects for a partial replica: it re-materializes locally the data the replica is designed not to
  hold. FlowLog's own numbers price the trade — the memory multiple *is* the arrangement state.
  Reading FlowLog confirms the pull/demand-driven choice rather than overturning it.
- **Compile-to-executable.** FlowLog codegens a binary per program (`build.rs` / CLI). dialog is
  embeddable, rules arrive at runtime as serializable descriptors and must round-trip; planning has
  to stay a runtime, per-scope activity. Nothing to take here beyond noting the pipeline shape
  (parse → typecheck → stratify → plan) is the same one dialog already has as
  `DeductiveRuleDescriptor → analyze → plan(scope)`.

## What transplants

### 1. Recursive control wraps ordinary per-rule plans — finish the separation

FlowLog's load-bearing lesson: semi-naive execution is a *control* concern layered over each rule's
unchanged logical plan; the delta rule is the same plan with one source substituted (Δ vs total).
dialog is most of the way there — the `Plan` operator IR exists, and
`fixpoint.rs` already re-plans the non-recursive rest of each rule per round through the ordinary
`Planner`. The gap: the recursive occurrences themselves are handled by bespoke fixpoint machinery
(`SplitRule`/`Member` binding rows into a `Match` outside the plan). The transplantable move is to
represent "this concept premise reads the delta (or the total) of the in-progress answer table" as a
*source binding on an ordinary `Concept` plan step*, so the fixpoint loop degenerates to: swap
sources, run the same compiled plan, union. Every planner improvement then applies inside recursion
for free — which is precisely the property FlowLog credits for its results, and the structure the
[goal-directed fixpoint](./goal-directed-fixpoint.md) work wants to attach demand to.

### 2. Boolean specialization: make the Z-set weight a parameter

FlowLog's dual modes are literally one generic parameter: `Present` (a unit diff type — set
semantics, existence only) for batch runs, `isize` for incremental maintenance. Its
"recursion-aware Boolean specialization" observes that *inside a fixpoint multiplicities are wasted
work*: derivation counts explode combinatorially in recursion while the fixpoint only needs
membership. Weights earn their keep only where retraction is being maintained.

`dialog-dbsp`'s `ZSet` hardcodes `BTreeMap<T, isize>`
(`rust/dialog-dbsp/src/zset.rs`). The transplant: parameterize the weight by a small
semiring trait with two instances — a unit/`Present` weight for one-shot query evaluation and
fixpoint inner loops, `isize` for the subscription/DRed layer. This also matches how retraction is
already framed: DRed's over-delete/re-derive does not depend on counts, so nothing in the planned
incremental path *requires* integer weights outside the change log itself.

### 3. Robustness-first planning, not precision-first

FlowLog's optimizer stance: recursive workloads have per-round delta cardinalities so volatile that
precise estimates are stale by design, so it combines *structural* optimization (avoid plans with a
catastrophic worst case, against runtime skew) with sideways information passing for early
filtering, and treats cardinality-based ordering as secondary (theirs is marked work-in-progress).
dialog's gate/rank split ([`query-engine-design.md`](./query-engine-design.md)) is already shaped
for this — feasibility is exact, cost is a swappable selector. Two consequences for the planned cost
redesign ([`query-cost-model.md`](./query-cost-model.md)):

- **Rank by worst case first, estimate second.** dialog's scans are network-priced: a mis-estimate
  costs roundtrips over sparse replicas, not just CPU. Preferring orderings whose worst case is
  structurally bounded (contiguous bound prefixes, no cross-product-shaped steps) over orderings
  that win only if an estimate holds is worth *more* here than it was to FlowLog.
- **Keep per-round replanning.** The fixpoint already re-plans against each round's bound scope;
  FlowLog validates that as the right response to delta volatility. A future cost model must stay
  cheap enough to keep doing it.

### 4. Arrangement reuse → fetch/scan sharing

The optimization FlowLog's planner works hardest at is *sharing*: sub-plans are deduplicated across
rules so DD arrangements (indexes over intermediate relations) are built once. dialog has no
long-lived operator state to share, but the analogue is sharper under network pricing: **the
materialized-subtree working set is the arrangement.** Concretely:

- Within one evaluation (and across fixpoint rounds, and across rules of one SCC/stratum), the same
  index range scanned by several premises should hit a shared range-keyed cache of fetched subtrees
  rather than re-scanning storage. The SCC members already share an answer table; body-predicate
  scans deserve the same treatment.
- Per-adornment plan memoization (already implemented for concept rules) is the *plan*-level half of
  this; the *data*-level half is the missing piece.

### 5. Logic fusion

FlowLog applies classic SQL-style fusion at the logical IR: filters into scans, projections into
adjacent operators. dialog already pushes bound slots into `ArtifactSelector` ranges; the remaining
fusion is `Constraint` steps that only test variables bound by a single producing `Scan` — those can
fuse into the scan as a post-filter instead of standing as separate plan steps, one fewer pass over
the binding stream each. Worth doing only where the plan shows it (see 6).

### 6. The profiler is a pipeline module, not an afterthought

FlowLog ships the profiler as one of its three planner-support modules: per-operator runtime metrics
feed the optimizer. dialog's `Plan` steps already carry a `Header` (cost/binds); the transplant is
attaching evaluation-time counters (rows in/out, subtrees fetched, roundtrips) per step and surfacing
them through `dialog-diagnose`, so the cost-model redesign is calibrated against measured operator
behavior instead of a priori constants.

### 7. Benchmark discipline

FlowLog's results are legible because they run standard recursive workloads (DOOP/DaCapo, transitive
closure, galen) against named competitors (Soufflé, RecStep, DDlog, DuckDB). dialog's
[benchmark notes](./benchmark-dataset.md) focus on realistic datasets (mbrainz); adding a small
standard recursive suite — transitive closure on graph shapes, same-generation, an Andersen-style
points-to — would give the fixpoint and planner work regression numbers comparable against the
literature, and directly exercises the goal-directed-fixpoint claim (full closure Θ(n²) vs demanded
O(n)).

## Priority

(2) and (6) are small and unblock other work: the weight parameter touches only `dialog-dbsp`, and
measured per-operator numbers should precede any cost-model redesign. (1) is the structural one —
it is the same refactor the goal-directed fixpoint plan wants first. (4) is the highest-leverage
performance item for the partial-replica model. (3) is a stance to hold during the cost redesign
rather than a task. (5) and (7) are opportunistic.

## Pointers

- [FlowLog] site: tutorial, playground, benchmarks.
- [flowlog-rs/flowlog]: the compiler (`flowlog-build`, `flowlog-compiler`, `flowlog-runtime`).
- Zhao, Yu, Rao, Frisk, Fan, Koutris, *FlowLog: Efficient and Extensible Datalog via
  Incrementality*, VLDB 2026 ([arXiv:2511.00865]); artifacts at [flowlog-rs/FlowLog-VLDB].
- This repo: `rust/dialog-dbsp/src/zset.rs` (hardcoded `isize` weights),
  `rust/dialog-query/src/concept/query/fixpoint.rs` (semi-naive loop, per-round planning),
  `rust/dialog-query/src/planner/` (gate/rank split), `notes/query-cost-model.md` (network-priced
  scans).

[FlowLog]: https://www.flowlog-rs.com
[flowlog-rs/flowlog]: https://github.com/flowlog-rs/flowlog
[flowlog-rs/FlowLog-VLDB]: https://github.com/flowlog-rs/FlowLog-VLDB
[arXiv:2511.00865]: https://arxiv.org/abs/2511.00865
[Timely]: https://github.com/TimelyDataflow/timely-dataflow
[Differential Dataflow]: https://github.com/TimelyDataflow/differential-dataflow
