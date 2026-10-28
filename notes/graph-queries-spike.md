# Graph queries implementation

Status: functional checkpoint; performance requires further work and fresh review.

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
- Five focused integration tests pass: deep rename, cycle close/cut, incomplete
  optional/many bags, shared targets, required target checks, matching/shaping,
  transaction writes, derived fields, new reachability, and actual demand bounds.
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
ordinary queries, not one physical subscription. Those queries independently
walk the branch diff, causing excessive repeated archive reads. This is a
performance problem to fix, not evidence that the graph performance goal passed.

Reports: `target/graph-perf-local/*.json` (generated, untracked). No existing
performance baseline was changed. The new graph scenario baselines require the
pinned Linux CI toolchain and callgrind; local counters do not prove that gate.

## Reproduction

```sh
cargo test -p dialog-repository graph_ --lib --locked
cargo clippy -p dialog-query -p dialog-repository -p dialog-perf --all-targets --all-features --locked -- -D warnings
cargo run -p dialog-perf --locked -- sweep --no-callgrind --only graph-read,graph-rename,graph-edge,graph-root,graph-noop --out-dir target/graph-perf-local
```

The existing `target` directory holds the compiled debug dependencies. The first
repository test binary build took several minutes; do not create a fresh target
directory or a duplicate release build for routine review.
