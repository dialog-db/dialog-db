# Attribute-level deduction: rules derive relations, concepts select them

> Design note for attribute-keyed deductive rule discovery. Records what
> changes, what deliberately does not, and the invariants each change is
> held to. Companion to [`layered-rule-resolution.md`](./layered-rule-resolution.md)
> (the storage and cache disciplines this note extends) and
> [`inductive-rules.md`](./inductive-rules.md) (whose `dialog.rule/reads`
> index is the precedent for the head index introduced here).

## The gap

A concept is a set of attributes, identified by that set and nothing
else. A concept query is a conjunction of one scan per attribute joined
on `this`. An entity is a `Named` whenever it has a `person/name` fact,
whether or not anyone ever asserted it *as* a `Named`. That is the
schema-on-read promise: concepts are lenses, not containers.

Deductive rules break the promise. A rule concluding `Employee { name,
role }` is discovered by the content address of that whole head, so a
query for `Named { name }` never sees an employee's derived name. The
rows exist only inside the `Employee` lens. Two consequences follow:

- a subset concept does not see a superset rule's derivations, although
  an entity that has `name` and `role` plainly has `name`;
- anyone who models `person/name` without knowing the `Employee` concept
  exists gets stored names only. Derivations cannot propagate into
  lenses their author never saw.

Inductive rules do not have this gap, because their conclusions become
facts. Deduction is the only place where "which lens was the rule
written against" leaks into what other lenses can see.

## The semantics

A rule derives *relations*, one per head attribute, sharing `this`. A
concept selects relations. Formally, for a concept `D` with attribute
set `A(D)`:

```
rows(D) = join over stored facts of A(D)
        ∪ for every rule R with A(head R) ∩ A(D) ≠ ∅:
            π[A(D) ∩ A(head R)](R)  ⋈  ⋈[a ∈ A(D) \ A(head R)] rel(a)

rel(a)  = stored(a) ∪ ⋃[R : a ∈ A(head R)] π[this, a](R)
```

Read it as: a row of `D` either comes entirely from stored facts, or
takes some of its attributes jointly from one rule row and the rest from
wherever each remaining attribute is available, stored or derived by any
other rule.

Two properties of this definition are load-bearing.

**Tuple coherence within a rule.** The attributes a rule row contributes
stay together. A `member { group, role }` rule that derives `(alice,
g1, admin)` and `(alice, g2, viewer)` never yields `(alice, g1,
viewer)` through this definition. Attributes taken from *different*
rules combine freely, which is correct: nothing relates them.

**The exact case costs what it costs today.** When `A(head R) ⊇ A(D)`
the rule term is `π(R)` with no extra join, so a concept fully derived
by one rule evaluates that rule once, exactly as concept-keyed discovery
did. A subset query pays the same. This is what keeps the change from
being a k-fold regression on every derived concept.

## The mechanism: projected rules

Nothing changes at the scan layer. `Provider<Select>` yields stored
artifacts with keys, causes and provenance; derived rows have none of
those, and unioning them there would corrupt cardinality-one election,
demand recording and the subscription tree diff alike. The union point
is where it already is: [`ConceptRules::plan`], whose `Disjunction`
already unions one `Conjunction` per rule.

Resolution builds, per queried descriptor `D`, a **projected rule**
`P_R(D)` for every rule `R` whose head overlaps `D`:

- head: `D` itself, so every consumer of a rule (adornment, plan cache,
  fixpoint projection, dependency analysis, DRed) sees a rule whose
  parameter set is the querying concept's parameter set. This is what
  keeps the `(rule, adornment)` plan-cache key sound: an adornment is a
  bitmask over the *caller's* parameter names, and a projected rule's
  parameters are exactly those names.
- body: `R`'s premises with the head variables of the shared attributes
  renamed to `D`'s field names for those attributes (head and body bind
  by name coincidence in this codebase), any other body variable that
  would collide with one of `D`'s names alpha-renamed, and one premise
  per attribute of `D` not in `R`'s head: a scan when nothing derives
  the attribute, or a concept premise over the single-attribute concept
  `{ a }` when something does, so the remaining attributes resolve
  recursively through the same mechanism. `D`'s own conformance premises
  are appended for its concept-typed fields.
- `this` is `this` on both sides. A projection never re-derives the
  head entity, which is what keeps `is_entity_local` transferable and
  DRed's per-entity re-derivation sound.

The bundle for `D` is then the unchanged implicit rule (stored facts
only) plus every `P_R(D)`. When no rule touches any attribute of `D`,
the bundle is byte-identical to today's, and so is the plan.

A projected rule has a content address like any rule, so plans cache
per `(P_R(D), adornment)`. The projection itself is a pure function of
`(R, D)` and is cached by that pair; it is recomputed only when a rule
or a descriptor spelling is first seen at a head.

### Reducing rules

A reducing rule's fold groups by its own non-reduced head fields, so
its body cannot be re-headed. When `A(head R) ⊇ A(D)` the rule is
evaluated as today and its rows renamed to `D`'s field names. When it
covers `D` only partially it is skipped for `D`: aggregate heads do not
mix with other derivations until incremental aggregate maintenance
(milestone A5) settles how a group's row is owned.

### Optional fields

An attribute optional in `D` and required in `R` projects fine. The
reverse, required in `D` but optional in `R`, would bind a required head
from a set-widened source; such an `R` is skipped for `D` rather than
guarded, matching the analyzer's existing `RequiredHeadFromOptional`
policy.

## Storage: `dialog.rule/derives`

The `Statement` lowering for a deductive rule writes one more fact
family:

```
dialog.rule/derives  of  rule-entity  is  on:<domain>/<name>
```

one per head attribute, using the same `on:` reach entities the
`reads` index uses (a keyed collection contributes its half's cover
key). Retraction erases them. `dialog.rule/conclusion` keeps being
written, both for tooling that lists rules by head and for the legacy
path below.

Discovery for `D` is then one narrow value-constrained selector per
attribute of `D`, exactly the shape `Dispatch::expand_through_deduction`
already probes `reads` with. Nothing enumerates all rules, and there is
no whole-attribute range read on the query path.

Rules are content-addressed, so the rule entity, `retract`, and every
cache keyed by rule entity are unaffected. Replication carries the new
facts like any others.

### Legacy rules

A rule committed before this index exists has `conclusion` and `source`
facts but no `derives` facts. Resolution keeps consulting `conclusion =
D` and installs any rule found there that the attribute index did not
already return, as an exact-head rule. Such rules keep their old
behaviour, visible to the exact concept only, until re-asserted, at
which point the (idempotent) assert adds their `derives` facts and they
join the attribute index.

### Deduplication by identity

A rule can now be found through several paths at once: once per
attribute it derives, per layer, and again through the legacy index. It
must be installed once. Structural equality is not the right test:
compiling the same bytes twice does not always yield equal
`DeductiveRule`s, because analysis records its narrowings in a
hash-map-dependent order. Dedup goes by content address
(`DeductiveRule::same`), which is a pure function of the body; rules
without an encodable body (built-ins, raw attribute-query bodies) fall
back to equality.

## Caches

`RuleCache` gains two maps beside `reads`:

- `derives`: `on:` entity → rule entities, head-tagged, re-scanned when
  the head moves, one entry per attribute rather than per concept;
- `projections`: `(rule entity, descriptor bytes)` → projected rule,
  content-keyed and never stale.

Discovery by concept (`discovery`) stays for the legacy path. Hydrated
bodies, plan cache, trigger and read indexes are untouched.

The overlay is read fresh, as before: a rule staged in the transaction
is found by scanning the batch for `derives` facts naming any attribute
of `D`.

Built-in rules (the version-control concepts) are indexed by head
attribute at first use and go through the same projection, so a
single-attribute query over `dialog.revision/branch` sees the record
projection like any other derivation.

## Dependency analysis and recursion

A projected rule carries its source rule's concept premises, so the
dependency graph gains exactly the edges the definition implies: `D →
H` for every concept `R`'s body reads, plus `D → { a }` for every
uncovered derived attribute. Stratification checks run over the same
closure walk as before; the walk now resolves every referenced
descriptor through the projecting resolver, so a cycle closed by a
superset head is visible from a subset query.

Recursion does not spread to subsets. A rule `R` recursive on its head
`H` gives `P_R(H) = R`, so `H` keeps its self-edge; for a proper subset
`D`, `P_R(D)`'s body reads `H`, not `D`, so `D` depends on a recursive
concept without joining its component. Evaluating `D` runs `H`'s
fixpoint inside that premise and joins, which is what a non-recursive
concept referencing a recursive one costs today.

## Incremental maintenance

DRed's affected-entity discovery walks concept premises to arbitrary
depth through `SelectRules`, so a projected rule's premises are
followed like any rule's. Entity locality transfers because `this` is
preserved. A change to a stored fact of an attribute that some rule
also derives is matched by the single-attribute concept's implicit scan
and propagates upward through the concept premise that reads it.

Rule demand stays proportional. The subscription's rule cover is the
union of `|A(D)|` narrow `derives` slices plus the legacy `conclusion`
slice. A rule installed for an unrelated attribute lands outside the
cover and does not wake the subscription; a rule installed with a
superset head writes a `derives` fact for every attribute of `D` and
lands inside it. The footprint scan that commit-time dispatch uses is
not on the query path and records no demand.

The retained fixpoint continuation is keyed by the subscribed concept.
A subscription over a recursive concept `H` is unchanged. A
subscription over a subset `D` of a recursive `H` recomputes `H`'s
fixpoint per poll, as any non-recursive concept referencing a recursive
one does today; retaining per referenced component is a follow-up.

## Stratification

Cycles are detected at attribute granularity, which is finer than the
concept graph was: a cycle that existed only because two concepts
shared a hash is no longer reported, and a cycle through a subset
concept, invisible to the concept-keyed graph, now is. The unit an
edge belongs to is still the rule, since a multi-head rule is one
stored object with one cause. How an ill-stratified program is handled
is [`stratification-policy.md`](./stratification-policy.md).

## What this does not do

- No deduplication. An entity whose `D` row is both stored and derived
  yields two rows, as it does today for exact heads. Mixed-provenance
  rows (attribute `a` from `R1`, `b` from `R2`) can be produced by both
  `P_R1(D)` and `P_R2(D)`. A distinct step on the projected operands is
  a separate change.
- No election between a stored and a derived value on a cardinality-one
  attribute. Both surface. The merge policy an attribute declares is a
  separate design.
- No memoization of a rule's evaluation across the premises of one
  query. The exact and subset cases need none; the mixed case evaluates
  each contributing rule once fully and the others seeded per entity.
- The cost model stays blind to derivation beyond the flat
  `CONCEPT_OVERHEAD`, as it is today.
