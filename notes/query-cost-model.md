# Query Cost Model

## Problem

A datalog query decomposes into premises, each resolved by a range scan over a prolly tree index. The tree is sparsely replicated on demand — every node traversal during a scan may require a network roundtrip. The order in which premises execute determines which variables are bound when subsequent premises run, which determines how tight their scans are. A good ordering can be the difference between a handful of point lookups and a full index walk. The query planner needs a cost model to estimate the expense of each premise given currently bound variables, so it can choose an execution order that minimizes total traversal.

## Index Structure

Each artifact is stored in a single prolly tree with three key layouts distinguished by a tag byte. Every key is 162 bytes:

```
EAV:  [tag 1B][entity 64B][attribute 64B][value_type 1B][value_ref 32B]
AEV:  [tag 1B][attribute 64B][entity 64B][value_type 1B][value_ref 32B]
VAE:  [tag 1B][value_type 1B][value_ref 32B][attribute 64B][entity 64B]
```

A range scan constructs a `(start_key, end_key)` pair. Known fields are set to their actual value in both keys. Unknown fields are `0x00..` in the start and `0xFF..` in the end. Only fields that form a contiguous prefix from the start of the key constrain the tree traversal — a known field after an unknown gap gets post-filtered, not scanned.

```
Example: {the, is} on AEV

  AEV key:  [tag][attribute][entity][value_type][value_ref]
  Known:          ^^^^^^^^^^                    ^^^^^^^^^^
  Prefix:         65 bytes   ← attribute constrains the prefix
  Gap:                       ← entity unknown, 64 bytes of 0x00..0xFF
  Tail:                                        ← value known but after gap, post-filtered

Example: {the, is} on VAE

  VAE key:  [tag][value_type][value_ref][attribute][entity]
  Known:          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  Prefix:         97 bytes   ← value + attribute form contiguous prefix

  Same two known fields, but VAE yields a 97-byte prefix vs AEV's 65.
```

The cost model selects the index that produces the longest contiguous prefix for the known fields:

```
entity known         → EAV  (entity leads, 64B+)
attribute + value    → VAE  (value + attribute contiguous, 97B)
attribute only       → AEV  (attribute leads, 65B)
value only           → VAE  (value leads, 34B)
```

When entity is known, EAV always wins — entity is 64 bytes and leads the key. If attribute is also known the prefix extends to 129 bytes. If value is known instead, it sits at the end of the EAV key and gets post-filtered, but no index places entity and value adjacent so EAV is still best.

When entity is unknown but both attribute and value are known, VAE produces a 97-byte prefix while AEV produces 65 bytes with value post-filtered.

## Decision

Use a greedy algorithm with an index-aware cost function. At each step, estimate the cost of every remaining premise given currently bound variables, pick the cheapest, execute it, extend the bindings, and repeat.

### Cost function

For a given premise, determine which of `{the, of, is}` are bound, select the best index by longest contiguous prefix, and assign a cost from the following tiers:

```
SEGMENT  = 100      1-2 tree nodes, near point-lookup
READ     = 200      small bounded range, a few nodes
SCAN     = 1000     broader range, multiple segments
FULL     = 5000     large portion of an index
VERIFY   = 100      per-match secondary lookup for cardinality-one verification
```

These are relative weights for ordering, not latency predictions. The full cost table:

```
Known       Index   Prefix    ONE              MANY
---------   -----   ------    ---------------  ---------------
{the,of,is} EAV     162B      SEGMENT (100)    SEGMENT (100)
{of,the}    EAV     129B      SEGMENT (100)    READ    (200)
{the,is}    VAE      97B      READ+V  (300)    READ    (200)
{of}        EAV      65B      READ    (200)    SCAN    (1000)
{of,is}     EAV      65B      READ    (200)    SCAN    (1000)
{the}       AEV      65B      SCAN    (1000)   FULL    (5000)
{is}        VAE      34B      SCAN+V  (1100)   FULL    (5000)
```

Notable details:

- `{of, is}` costs the same as `{of}` alone. No index places entity and value adjacent, so the value constraint is post-filtered and does not reduce tree traversal.
- `{the, is}` with `Cardinality::One` incurs a VERIFY cost. The VAE index does not group results by (entity, attribute), so each candidate needs a secondary EAV lookup to confirm it is the write-race winner.
- `{the}` is expensive despite having one field bound because attributes are shared across many entities ("person/name" applies to every person).

### Cardinality-one winner verification

When an attribute has `Cardinality::One`, only the write-race winner for each (entity, attribute) pair should be yielded. The verification strategy depends on whether the scan index groups results by (entity, attribute):

**Sliding window** — when results are grouped by (entity, attribute) in key order, buffer the group and emit the winner at the boundary. Applies to EAV scans and AEV scans with attribute known.

**Secondary lookup** — when results are not grouped, verify each candidate individually by scanning EAV with the `[entity][attribute]` prefix (129 bytes) and checking whether the winner's value matches. Applies to VAE scans.

```
sliding_window = entity_known OR (attribute_known AND NOT value_known)
```

### Greedy ordering

The greedy algorithm runs in O(N^2) for N premises. At each of N steps it scans remaining premises for the cheapest viable one. This is simple to implement, easy to reason about, and produces optimal or near-optimal orderings when the cheapest next step is clearly distinguished — which the index-aware cost tiers ensure in most cases.

### Rationale

The cost function is keyed on *which* components are bound, not merely *how many*. This matters because two premises with the same number of known components can have very different scan costs depending on index layout. `{the, of}` (129-byte EAV prefix) is fundamentally cheaper than `{the, is}` (97-byte VAE prefix with possible verification overhead). A count-based model would assign them the same cost and leave the planner unable to distinguish them.

Greedy is the right starting point because most real queries have a handful of premises where the cost differences between orderings are large enough that the locally cheapest choice at each step is globally optimal. The index-aware tiers amplify these differences — SEGMENT vs SCAN is a 10x gap — so ties that would force greedy into an arbitrary (possibly wrong) choice are rare.

## Alternatives

### Count-based cost model

Assign cost purely by number of known components (3 known = cheap, 1 known = expensive, regardless of which). Simpler but collapses genuinely different costs. `{the, of}` and `{the, is}` both have two known fields but differ by a factor of 1-3x in scan width. The planner cannot make an informed choice between them, leading to unnecessary full-range scans when a tighter index is available.

### Exhaustive search (Held-Karp DP)

Find the minimum-cost ordering over all N! permutations using dynamic programming over subsets. State is a bitmask of evaluated premises; complexity is O(2^N * N^2).

```
cost(S) = min over Pi in S: cost(S \ {Pi}) + estimate(Pi, bound(S \ {Pi}))
```

This is optimal but unnecessary as a default. For typical queries (N < 10), greedy already finds the optimal order because the cost tiers create clear winners at each step. The DP overhead is small in absolute terms but adds implementation complexity for marginal gain in the common case.

## Future Improvements

### Hybrid greedy + Held-Karp for tie-breaking

Greedy fails when multiple premises tie on cost, because it picks arbitrarily without considering how each choice's bindings cascade to downstream premises:

```
P1: (person/name, ?person, ?name)       {the} only → SCAN (1000)
P2: (dept/members, ?dept, ?person)       {the} only → SCAN (1000)
P3: (dept/budget, ?dept, ?budget)        {the} only → SCAN (1000)

Greedy picks P1 (arbitrary). Binds ?person.
  P2 → {the, is} READ (200). P3 still {the} → SCAN (1000).
  Total: 1000 + 200 + 1000 = 2200.

P2 first. Binds ?dept and ?person.
  P1 → {the, is} READ (200). P3 → {the, is} READ (200).
  Total: 1000 + 200 + 200 = 1400.
```

When greedy encounters a tie, apply Held-Karp over the tied subset and remaining premises to break it optimally. This gives O(N^2) in the common case and O(2^N * N^2) only when it matters. Planning runs once per adornment and is cached, so even the DP path costs microseconds — negligible against a single network roundtrip.
