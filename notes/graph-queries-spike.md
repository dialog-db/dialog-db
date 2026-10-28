# Graph queries implementation

Status: implemented and locally tested; fresh review and Linux performance/CI gates remain.

The matching query remains an ordinary `ConceptQuery`. Named graph shapes refer
to each other by name, preserving existing concept identities and allowing cycles.
Shaping reads each field through its attribute concept, retaining picks and derived
rules. Missing scalar fields are omitted; missing many-valued fields are empty.
`check_bag` is shared by root typed-edge matching and application checks of nested
bags. It checks local requiredness, cardinality and value types. Embedded legacy
`conforms` and keyed collections are rejected in graph shapes rather than silently
misinterpreted. Ordinary queries keep their existing semantics and encodings.

`QueryLayer::graph` and `TransactionQuery::graph` read through the existing query
scope, including pending writes. `Branch::subscribe_graph` composes existing
demand-gated subscriptions for root matching and per-entity fields. Cycles only
shape results, so they add no recursive rules. New targets are read in the same
poll; pruning starts from matching roots, including when a disconnected cycle
would otherwise sustain itself. Matching/shaping equalities are separate data
and enter query bindings. All-pick equalities narrow the scan demand; choosing
picks must still elect a winner before comparing its value, as ordinary queries do.

The graph cache contains normalized entity/shape bags. A node reached through
several paths emits one field delta. A changed branch currently rewalks the cached
graph and polls every retained field subscription. An unchanged branch/overlay
has a constant-time return. This is not O(changed nodes) graph assembly.

## Checkpoints

- `9588dbba`: graph schema, bags, shared conformance, query traversal and deltas.
- `29e4c6c3`: graph subscriptions and query-layer/transaction adapters.
- Eleven focused graph/cache tests and all 47 ordinary subscription tests pass.
  Coverage includes: deep rename, cycle close/cut, incomplete
  optional/many bags, shared targets, required target checks, matching/shaping,
  transaction writes, derived fields, new reachability, and actual demand bounds.
- Targeted all-target/all-feature Clippy passes with `-D warnings`.
- Tonk notation/worker/reactor integration is in a separate worktree.

## Local performance evidence

The debug `dialog-perf` sweep (native macOS, volatile provider, no callgrind), at
100 issues sharing one assignee, reports:

| Phase | Retained queries | Full evaluations added | Maintenances | Archive gets | Archive get bytes | Delta bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| unchanged poll | 202 | 0 | 0 | 0 | 0 | 0 |
| nested rename | 202 | 0 | 1 | 808 | 40,551,904 | 179 |
| edge move | 203 | 1 | 2 | 808 | 40,558,368 | 233 |
| root retitle | 202 | 0 | 2 | 808 | 40,555,136 | 182 |

Each changed phase includes the commit. The graph read returns 100 roots and
101 normalized nodes in 8,835 JSON bytes. The idea's nested display estimate is
303 subscriptions (`3 + 3N`); this graph has one public handle but 202 retained
ordinary queries, not one physical subscription. The first implementation independently
loaded each branch diff, causing excessive repeated archive reads. A per-poll
cache now shares immutable blocks between these reads without widening demand.
Failures and missing blocks are not cached. Ordinary subscriptions keep their
original uncached storage path. The cache is dropped after each graph poll.

After this optimization, the same 100-issue workload reads 3 archive blocks per
changed phase: 101,960 bytes for a rename, 101,992 for an edge move, and 101,976
for a root retitle. All outcomes, evaluation counts and delta sizes are unchanged.
An unchanged poll still reads nothing. `target/graph-perf-cached` holds the
post-optimization reports.

Scaling the nested rename from 10 to 1,000 issues retains 22 and 2,002 queries;
reads 3 blocks/31,088 bytes and 6 blocks/320,040 bytes respectively; and emits the
same single 179-byte field delta, with one entity-local maintenance and no new
full evaluations. These counters do not measure the remaining O(nodes + edges)
cache traversal CPU; callgrind remains necessary before claiming the perf goal.
`target/graph-perf-scale` holds those reports.

Reports: `target/graph-perf-local/*.json` (generated, untracked). No existing
performance baseline was changed. The new graph scenario baselines require the
pinned Linux CI toolchain and callgrind; local counters do not prove that gate.

## Fresh-review corrections

Graph publication now compares the branch cell edition and overlay sequence
before and after matching/shaping. An interrupted attempt is discarded and
retried through the retained ordinary subscriptions. After eight interrupted
attempts, `GraphError::UnstableRead` leaves the last published graph intact;
a stable later poll can retry. Counts include work performed by discarded
attempts. Deterministic provider barriers test atomic commits and overlay writes
between root selection and the first field read, plus exhaustion and retry.

One-shot query-layer and transaction graph reads use the same bounded retry
strategy across every joined branch edition and source overlay sequence. Stamps
are captured before identity resolution and metadata/overlay construction;
validation covers both successful and failed evaluations. Staged transaction
layers survive retries. The deterministic one-shot barrier is at identity
resolution (before query evaluation), and explicitly asserts a retry for changes
to the second joined branch, its overlay, and a transaction's underlying branch;
it also checks pending facts remain uncommitted. The separate subscription
barriers above exercise changes between matching and shaping.

Root deltas use ordered set differences rather than searching the other vector
for every root. A 50,000-root regression includes unsorted roots and duplicates.

Equality constraints preserve the attribute pick's meaning. An `all` probe
narrows the recorded scan demand. A choosing pick must elect its winner first:
with `max` over Alice and Zoe, equality to Alice excludes the target rather than
filtering Zoe out before election. Both cases have regression tests.

The `graph-incomplete` workload has 100 roots, ten optional targets with an email
but no required name, and one shared conforming target. It returns 111 normalized
nodes in 9,565 JSON bytes. The ten nonconforming node/bag objects account for
700 bytes (7.3183% of the result), including their entity and shape addresses.
This is a specified synthetic workload, not a claim about real Tonk data. The
ordinary all-conforming graph reports zero nonconforming bytes. Latest local
reports are in `target/graph-perf-review`, covering all six graph scenarios.

The Test workflow also offers a manual `perf_only` boolean, default false, for
collecting the new Linux instruction baselines before the full draft-PR matrix.
Its guards explicitly leave push and pull-request workflows unchanged.

## Reproduction

```sh
cargo test -p dialog-repository graph_ --lib --locked
cargo clippy -p dialog-query -p dialog-repository -p dialog-perf --all-targets --all-features --locked -- -D warnings
cargo run -p dialog-perf --locked -- sweep --no-callgrind --only graph-read,graph-incomplete,graph-rename,graph-edge,graph-root,graph-noop --out-dir target/graph-perf-local
```

The existing `target` directory holds the compiled debug dependencies. The first
repository test binary build took several minutes; do not create a fresh target
directory or a duplicate release build for routine review.

Round-two correction verification: all 12 focused graph/cache tests passed;
scoped all-target/all-feature Clippy passed with warnings denied; all six local
graph counter scenarios passed again (rename/edge/root: three blocks read each).
