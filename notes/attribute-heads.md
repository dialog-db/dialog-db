# Attribute heads: rules derive attributes, concepts select them

> Design note for the second attempt at attribute-level deduction. The
> first attempt kept concept heads and *projected* a rule onto the
> querying concept at resolution time; it worked, and it was the wrong
> primitive. This note replaces it. Companion to
> [`layered-rule-resolution.md`](./layered-rule-resolution.md) (storage
> and caches, updated below) and [`inductive-rules.md`](./inductive-rules.md)
> (the `reads` and `on` indexes this note's `derives` index sits beside).

## The claim

A deductive rule is a function from facts to facts. It differs from an
inductive rule in one way only: its conclusions are not persisted. So
whatever is true of asserted facts must be true of derived ones.
Asserting an instance of `Employee { name, role }` writes two
attribute facts and nothing else; a query for `Named { name }` sees
the name because it reads the `person/name` relation. A rule deriving
`Employee { name, role }` must land in the same place: it contributes
to the `person/name` relation and to the `employee/role` relation, and
`Named` sees the derived name for the same reason it sees the stored
one.

Everything below follows from taking that seriously.

## Semantics

An **attribute relation** `rel(a)` is the set of `(entity, value)`
pairs for attribute `a`:

```
rel(a) = stored(a)  ∪  ⋃ { body(R) projected to (of, is) : R derives a }
```

A **concept** is a named set of attributes. Its rows are the join of
its attribute relations on the entity, with optional attributes
left-joined. A concept has no rules of its own: it selects.

A **rule head** is a set of triples `(a, of-term, is-term)`. A rule
with several triples is exactly the set of single-triple rules sharing
its body; the engine stores and resolves it that way. A concept-headed
rule, the form every existing rule has, is sugar: `Employee { name:
?n, role: ?r }` with `this: ?e` is the triples `(person/name, ?e, ?n)`
and `(employee/role, ?e, ?r)`. The triples need not share an entity
term; nothing in the semantics requires it, and a head that names two
entities is two rules over one body.

**Both-or-none is automatic.** The two heads of `Employee` share a
body, so an entity either satisfies the body and gets both attributes
or does not and gets neither. No cross-reference between the heads is
needed, and none is written.

**Cross products are the EAV model, not a bug.** A body yielding
`(alice, g1, admin)` and `(alice, g2, viewer)` contributes `group ∈
{g1, g2}` and `role ∈ {admin, viewer}`, and a concept joining the two
sees `(alice, g1, viewer)`. Asserting those two instances as facts
gives the same answer today. A relationship with more than two parts
is an entity; a rule that wants `(group, role)` to stay paired derives
a membership entity carrying both.

**Cardinality is the attribute's.** A derived value for a
cardinality-one attribute competes with stored and other derived values
for the same entity under the attribute's election, exactly as two
stored values do. A derived row's standing in that election is the
standing of the newest claim its body consumed: the row exists because
of those claims and is as recent as the latest of them. Rows of a
cardinality-many attribute union as a set.

**Election happens at the boundary.** Inside a recursive component the
attribute is read with set semantics: every candidate value is a row,
and a rule in the component sees all of them. Election runs once, over
the finished fixpoint, where the component's rows leave it. The reason
is that election is a maximum, hence an aggregate, and an aggregate
inside its own recursion has no least fixpoint. A reader outside the
component sees one value; a rule inside sees the candidates. This is
the same asymmetry aggregation already has, and it is the only place a
derived attribute behaves differently from a stored one, because a
stored cardinality-one attribute never has two live values that a rule
could tell apart.

## Open rules are monotone

A deductive rule is *open*: it is installed as replicated facts, any
rule anyone installs later may read its head, and its body is resolved
against whatever program exists when a query runs. A rule set like that
has a unique meaning for every merge only when every rule is monotone.
So a deductive rule body admits positive attribute and concept
premises, optional (`maybe`) premises, row-local constraints and
formulas, and recursion. It does not admit `unless`, `coalesce`, or
`reduce`. Those remain available in the two *closed* places: a query or
subscription, which is compiled once and never read by a rule, and an
inductive rule, which reads a sealed state and writes facts.

Two consequences:

- the program dependency analysis shrinks to recursion detection and
  cannot fail. Stratification policy, quarantine, and the
  `*ThroughRecursion` errors go with it;
- a constraint whose operand is an absent optional is unknown and does
  not pass. Today `==` passes when both operands are absent, which makes
  a derivation disappear when a value arrives, the one non-monotone step
  that was hiding in the positive fragment.

Ordered choice, which the old `variants` desugaring expressed by
negating earlier alternatives, is expressed by election instead: give
the cardinality-one attribute an ordered value domain and elect the
highest. Every alternative is then a positive rule, and the attribute
picks. See [`ordered-domains.md`](./ordered-domains.md) when that lands;
this note only requires that election be a property of the attribute.

## Mechanism

### The attribute concept

The node the engine already knows how to evaluate, cache, maintain
incrementally and run to a fixpoint is a concept. So `rel(a)` is
realised as the **attribute concept** `{ a }`: the single-attribute
concept whose only field is `a`, with the field named by the
attribute's own name. Its rule bundle is the implicit scan of `a` plus
every rule deriving `a`, each with its head re-spelled as `{ a }`,
which is a renaming of two variables and never a projection.

A multi-attribute concept's implicit rule is a conjunction of
premises, one per field, as it is today, except that a field whose
attribute something derives is read through `{ a }` instead of a raw
scan. A field no rule derives keeps its raw scan, so a concept over
underived attributes plans byte-for-byte as it does now. The decision
is made at resolution, where the registry knows which attributes have
rules, and it is per attribute, so the byte-identical plan survives
rules landing on unrelated attributes.

A rule body naming an attribute directly, through an attribute premise
with a constant `the`, is rewritten the same way at resolution: through
`{ a }` when `a` is derived, raw otherwise. A premise whose attribute is
a variable reads stored facts only; nothing can resolve rules for an
attribute it does not know.

The implicit rule of `{ a }` itself is the raw scan. That is what
grounds the recursion.

### Recursion

The dependency graph's nodes are attribute concepts and the concepts
and rules that read them. A rule deriving `a` whose body reads `a` is a
self-edge on `{ a }`; the component machinery, semi-naive rounds,
retained continuations and DRed all operate on `{ a }` as they operate
on any concept today. A concept `D ⊇ { a }` reads `{ a }` through a
premise and is outside the component, which is where election runs.

### Election over the union

`{ a }` for a cardinality-one `a` elects after its disjunction: rows
are grouped by entity and folded through the attribute's election
with the row's standing. A stored row's standing is its artifact's; a
derived row's is the maximum standing among the claims its `Match`
carries, which is every claim a premise bound on the way to the head.
The fold is commutative, so the order rows arrive in does not matter.
Inside a component the fold is skipped and the rows stay a set.

### Storage: `dialog.rule/derives`

Installing a rule writes, beside `source`, `conclusion` and `reads`:

```
dialog.rule/derives  of  rule-entity  is  on:<domain>/<name>
```

one per head attribute, using the same `on:` reach entities `reads`
and `on` use. Discovery for `{ a }` is one value-constrained selector,
the shape `expand_through_deduction` already probes `reads` with.
`conclusion` keeps being written for tooling that lists rules by the
concept they were written against; it is no longer consulted on the
query path, except for rules that predate the `derives` index, which
resolve by `conclusion` and are re-spelled per head attribute on
hydration.

### Caches

Discovery is per attribute and head-tagged, as it was per concept.
Hydration is per rule entity. The plan cache keys by `(rule, adornment)`
and a re-spelled single-head rule has a content address of its own, so
plans for `{ a }` cache like any rule's. The descriptor memo of the
implicit plan is unchanged for underived concepts; for a concept with
a derived field the implicit rule differs by which premises are
concept premises, so it is memoised per resolution outcome.

### Incremental maintenance

A change to a stored fact of `a` is matched by `{ a }`'s implicit scan
and propagates to every concept premise reading `{ a }`, which is the
path DRed already walks. Subscription demand records one `derives`
slice per attribute of the subscribed concept, so a rule landing on an
unrelated attribute does not wake the subscription and a rule landing
on a subscribed one does. Entity locality transfers because `{ a }`
and the concept reading it share `this`.

## What changes for authors

Nothing in how a rule is written. A concept head still works and
means what it meant, with one visible difference: a subset concept
now sees the derivation. The `dialog.rule/derives` facts appear beside
the existing ones. `unless`, `coalesce` and `reduce` in a deductive
rule are compile errors that name the closed forms to use instead.

## Costs

A query over `Employee` when one rule derives both fields evaluates
that rule's body twice, once under `{ name }` and once under `{ role }`,
where the old concept-keyed path evaluated it once. A per-query memo
keyed by rule body and bound inputs would recover that; it is not part
of this change, and the benchmark in `benches/query_rules.rs` measures
the gap so the decision is made on numbers.

## Not in this change

- the ordered-domain election that replaces ordered choice;
- the per-query body memo;
- `reduce` on inductive rules, the materialised home for aggregates;
- the standing of a derived row beyond its cause: until `Match` claims
  carry the artifact's version, derived rows elect by cause and tie-break
  by value, and a versioned stored row beats a derived one. Recorded
  as a known gap, not a design.
