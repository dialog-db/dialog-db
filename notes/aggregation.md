# Aggregation: the `reduce` clause

Aggregation lives on deductive rules as a third clause beside `when` and
`unless`. The head stays an ordinary concept, so aggregate results compose —
any rule or query consumes a reducing rule's concept like any other. The
design goal, kept from the start: malformed aggregation is *unwriteable*
wherever structurally possible, with exactly one narrow analyzer check as the
residue of name-based head↔body binding.

## Shape

```yaml
deduce: DeptTotal
  dept:  entity
  total: uint
when:
  - Employee(?e, dept: ?dept, salary: ?salary)
reduce:
  total: sum(?salary)
```

Semantics, as a pipeline:

1. Evaluate `when` / `unless` exactly as today → rows (`Match` stream).
2. Group rows by the bindings of the **non-reduced head fields** (`dept`).
3. Per group, compute each `reduce` entry's fold over its input variable.
4. Emit one head row per group: grouping fields from the group key, reduced
   fields from the folds.

## Why the classic hazard cannot be written

The classic Datalog formulation `total(?dept, sum(?salary)) :- ...` defines
the grouping set as "head variables minus aggregated variables". Writing a
variable in both roles makes that definition self-contradictory (the variable
must be both excluded and included), so such systems must detect and reject.

Here, roles attach to **fields, not variables**:

- `reduce` is a name-keyed map over head fields. A field is aggregated iff it
  is a key in `reduce`; a `BTreeMap` key cannot be present and absent, so "a
  field both grouped and reduced" is unrepresentable.
- The grouping set is **derived** — the head fields not in `reduce` — never
  declared, so it cannot be declared inconsistently.
- The head is a plain `ConceptDescriptor`; there is no head slot an aggregate
  expression could occupy.
- A *variable* carries no role. It may feed a grouping field, a fold, or both.
  Feeding both is well-defined, not ambiguous: grouping happens first, so
  within a group both reads of the column agree (group by `?salary` and
  `sum(?salary)` yields `salary × group-size`, exactly Datomic's legal
  `[:find ?salary (sum ?salary)]`). The classic contradiction was a statement
  about variables in a variable-defined grouping set; that sentence cannot be
  formed when the grouping set is made of fields.

The one representable error, and the single analyzer check this design needs:
a body variable named the same as a *reduced* field (body binds `?total`
while `reduce` defines `total`) — two definitions for one field. This is
inherently cross-clause and name-based (head↔body binding in this codebase is
name coincidence), so it cannot be a construction-time property. It is always
authored confusion, so it is a **hard error**, sibling of
`RequiredHeadFromOptional` in the analyzer. Optional cosmetic lint (later,
`DeadOptionality` tier): a variable feeding both a grouping field and a fold.

## Settled semantics

- **Absent rows are skipped**, SQL-NULL-style: folds consume only `Present`
  bindings; `count` counts present bindings. Coalesce first for other
  behavior.
- **Empty groups do not exist** (groups arise from rows), but a group's fold
  inputs may all be Absent. The algebra decides the output type:
  - `count`, `sum` have identities (0) → output always present; reduced field
    may be required.
  - `min`, `max`, `avg` have no identity → over an optional input the output
    type admits `Nothing`, so the head field must be declared optional —
    enforced by the **existing** `RequiredHeadFromOptional` check. No new
    rule.
- **`min`/`max` require comparable types** (the range-predicate comparable
  set); `sum`/`avg` require numeric. Violations are construction-time errors
  on the reduce entry, checked against the head field's declared type at
  descriptor construction and against the body-inferred input type in the
  analyzer's typing pass.
- **`sum` accumulates in `i128`**, returns the narrowest fitting `Value`,
  errors loudly past representability. Datomic-style arbitrary-precision
  widening awaits a big-integer `Value` variant — a format decision tracked
  in bead dialog-db-65 (lexicographic encoding side; bijou is 128-bit-capped
  and zigzag-signed, so an arbitrary-precision order-preserving encoding
  would be bijou-inspired, not bijou). `avg` returns Float.
- **Phase-1 aggregators**: `count`, `count-distinct`, `sum`, `min`, `max`,
  `avg`. `median`/`variance`/`stddev` addable behind the same enum;
  `rand`/`sample` permanently excluded (nondeterministic in a convergent
  system).
- **Grouping keys** are compared by dag-cbor bytes of the group values (the
  fixpoint `AnswerTable` precedent; `Value` has no `Ord`).

## Stratification

A reducing rule's folds read complete relations, so every positive concept
premise of a reducing rule contributes an **aggregating edge** to the
program dependency graph (`session/dependencies.rs`): a third `Polarity`
treated like negation — an aggregating edge inside its own SCC is
`AggregationThroughRecursion`, exact sibling of `NegationThroughRecursion`,
same Apt-Blair-Walker/SCC machinery. Registration stays unconditional
(replica convergence); validity is checked at acquire, as for negation.

Future refinement, deliberately out of scope: `min`/`max` are monotone
lattice joins and could in principle pass through recursion (Flix-style
least-fixpoint semantics); their aggregating edges could later be downgraded.
`sum`/`count`/`avg` stratify regardless (not idempotent).

## Composition and storage

A reducing rule deduces an ordinary concept — other rules and queries consume
it with no special machinery. Rules are stored under the reserved
`dialog.rule/*` namespace via the privileged install rail; reducing rules add
a `reduce` block to the same content-addressed descriptor
(`{ deduce, when, unless, reduce }`). Ad-hoc aggregation queries are reducing
rules over an anonymous head; no separate find-spec notation exists.

## Milestones (beads dialog-db-60..64)

- **A1** — core group-by fold: pure module over an `impl Selection` given
  (grouping field names, reduce entries); dag-cbor group keys; absent-skip;
  property tests over synthetic `Match` streams (grouping correctness,
  absent policy, overflow, determinism across row order).
- **A2** — typing: aggregator input/output types, identity-vs-no-identity
  optionality propagation, construction-time checks on reduce entries.
- **A3** — the `reduce` clause on `DeductiveRuleDescriptor` + evaluation
  wiring (group by non-reduced head fields, fold, emit) + the reduced-field
  name-collision hard error + notation round-trip.
- **A4** — stratification: aggregating `Polarity`,
  `AggregationThroughRecursion`, tests mirroring the negation violations.
- **A5 (later)** — incremental maintenance under subscriptions
  (recompute-per-poll is correct first); materialized-aggregate performance.

## Test coverage matrix (acceptance for A3/A4; do not ship without)

Engine-level (A1, landed): grouping correctness, absent-as-input and
absent-as-group-key, identities vs Absent outputs, overflow, distinct-by-
bytes, permutation determinism, compare-machinery ordering, empty stream,
global group. Typing (A2): per-aggregator input rejection, output algebra,
optionality propagation both directions.

Rule-level (A3):
- The name-collision hard error (body variable named as a reduced field).
- The grouped-and-folded variable case is WELL-DEFINED, pinned: a head field
  and a fold fed by the same variable yields key x count / identity
  semantics, not an error.
- Optional-input min/max/avg forces an optional head field via the existing
  RequiredHeadFromOptional — pinned both ways (required head + optional
  input rejected; optional head accepted and yields Absent on all-absent
  groups).
- Descriptor round-trip: a reducing rule serializes/deserializes through the
  formal notation and through dialog.rule/* storage (privileged install
  rail), and the discovery scan hydrates the reduce block intact.
- Composition: a plain rule consuming a reducing rule's concept; a reducing
  rule over another reducing rule's concept (two strata); results correct.
- Overlay rules: an uncommitted reducing rule staged on a transaction
  resolves in pending-view queries.

Subscription lifecycle (A3 acceptance, using recompute-per-poll):
- Subscribe over a reducing rule's concept; assert facts -> poll delta shows
  the old aggregate row retracted and the new one asserted.
- Retract contributing facts -> aggregate updates; retracting a group's last
  row -> the group's row disappears from the subscription.
- Optional-input min/max group transitioning between present and Absent
  output across polls.
- A subscription over a CONSUMER of the reducing concept (composition depth
  2) also updates on base-fact changes.

Stratification (A4):
- Reducing rule in its own SCC -> AggregationThroughRecursion, mirroring
  every NegationThroughRecursion test shape.
- Aggregation over a recursive-but-lower-stratum concept: fixpoint completes
  first, fold sees the full relation — pinned with a recursive ancestor-
  style fixture.
- Combined negation + aggregation strata in one program.

Cross-cutting: all tests #[dialog_common::test] (native + wasm dedicated
worker); rule-level determinism across insertion orders of base facts.
