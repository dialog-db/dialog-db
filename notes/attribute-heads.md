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
the cardinality-one attribute a listed value domain and select the
first listed value present. Every alternative is then a positive rule,
and the attribute picks. The notation this points at puts the policy
beside the carrier type, with today's cardinalities as two of its
values and recency as the default:

```yaml
attribute!: &status
  the: io.gozala.job/status
  select: top              # last (default) | all | top | max | min
  as:
    - case:suspended
    - case:active
    - case:registered
    - case:onboarding
```

`last` is the newest write, today's cardinality one; `all` is the set,
today's cardinality many; `top` is the first listed value present;
`max` and `min` are the extremes of a naturally ordered carrier. Only
`last` depends on history rather than on the values present, which is
why it is the one policy under which a derived row needs a standing
of its own. The engine part of this is a per-attribute election
policy beside cardinality; the notation is tonk's, and this note only
requires that election be a property of the attribute.

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

`{ a }` for a cardinality-one `a` elects in one pass over every source
at once: the stored scan, each attribute-headed rule evaluated in
scope, each head split from a source rule (through the rows the body
was remembered to yield, below) and each fold. The candidates are
grouped by entity and folded through the attribute's election with
the row's standing. A stored row's standing is its artifact's,
the revision version then the cause, exactly what the stored election
compares. A derived row's is the maximum standing among the facts its
`Match` cites, which is every fact a premise bound on the way to the
head, carried across every concept boundary the row crossed. The fold
is commutative, so the order rows arrive in does not matter. A tie
falls to the value's bytes. Inside a recursive component the fold is
skipped and the rows stay a set.

### Set-widened reads

An optional field over a derived attribute reads `{ a }` with its
value term admitting `Nothing`. The concept query honours that as the
left join an optional scan is: every input row leaves at least once,
with the value `Absent` where no row, stored or derived, matched.

### One body, many heads

A rule concluding several attributes is one rule per attribute with
the same body. A concept selecting several of them would run that body
once per attribute, the first time in full and every later time as a
probe per entity. Three things make it run once.

Every head split from a source rule carries its origin: the source
rule, and the body operands its attribute's value (and key) come from.
A head with an origin plans as one `Recall` step over the source body,
not as the re-spelled rule: the body is planned once under the source
rule's identity, bound on `this` iff the caller binds it, and shared
through the plan cache by every head of that source.

Evaluating a `Recall` consults the query's memo, keyed by the source
rule and the bound entity, before it evaluates anything. The first
head to run the body remembers its rows; every later head projects
its own attribute out of the remembered rows. A bound lookup after a
free run is answered from the free rows, indexed by entity once, so a
head evaluated with `this` bound after another head scanned the whole
relation never touches storage. The memo lives on the query
environment and dies with it, so it never outlives the facts it was
computed from. It is not one of the branch's held caches: its rows
are facts as of one query's snapshot, staged writes included, and a
cache that outlived the query would need invalidating on every
commit and would hold whole relations. What the environment holds
across queries stays the plan cache and the rule cache; the memo is
reached through the query environment, which is where the in-flight
work on environment-held caches puts a query's handles too.

When exactly one source rule derives every derived attribute of a
concept, no attribute-headed rule stands beside it, none of its heads
folds and nothing is stored under those attributes, the concept's
answer is that rule re-headed onto the concept: the covering rule,
whose body is the source body with the concept's underived fields
read as stored scans. The concept evaluates the covering rule once
through the usual pipeline instead of selecting attribute by
attribute. Whether anything is stored is checked per query across
every layer the environment reads, since a range estimate sees the
committed tree alone; the moment a fact lands under one of those
attributes the exact path is off and the election decides.

An attribute concept read with its entity free leaves its rows sorted
on the entity, stored and derived alike, so it is an input to the
conjunction's N-way merge beside the stored scans of the same entity.
A set-widened read is not an input: it extends the other inputs' rows
where nothing matched, which an intersection cannot express, so it
stays a probe after the merge.

### Built-in concepts

The version-control concepts are closed views and resolve exactly as
written. Their rows are tuples over other entities: an upstream's
name, subject and peer flattened onto the branch tracking it, which
per-attribute selection would pair across upstreams. Nothing stores
or derives their attributes besides the engine, so a query over
`Revision` evaluates the projection rule once rather than once per
attribute. A query over one of their attributes goes through `{ a }`
and sees the built-in's head for it like any rule's.

### Reducing rules

A reducing rule's heads are its reduced fields, each folded over the
body grouped by the entity, and its other fields derived from the
unfolded body. A fold grouped by anything finer than the entity is
not expressible as an attribute of that entity, which is the EAV
answer again: the group is an entity of its own. An identity-less
fold whose group has no present input derives nothing for that
entity, where the concept head bound the field `Absent`.

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

### Identity

A rule's identity is the hash of its canonical spelling, not of the
bytes its author wrote. Every variable but `this` is renamed by a
labeling that depends only on the rule's structure: colour refinement
over where each variable occurs (which premise shape, under which
parameter, beside which other variables), then individualisation of
any variables refinement leaves tied, keeping the spelling with the
smallest encoding. The head's field names are variables like the
rest: a field name only ties a body variable to an attribute, and
the attribute is what pins it, which the labeling sees as one more
place the variable occurs. A keyed field's key operand follows its
field. Premises are sorted by their encoding under the labeling and
the head is re-keyed by it. So two authors writing one rule under
different names, for the head and the body alike, and in a different
order install one rule: one entity its facts are stored under, one
plan cache entry, one body memo.

The rule evaluates under a working spelling that keeps the head's
field names as given, because a caller's query binds the head by
those names, and renames only the body's locals. What is keyed by
identity is keyed by the head's spelling as well, so two spellings
of one rule never share a plan or a body's rows. A rule installed
into a concept's bundle under other field names is re-headed onto
the bundle's names first, pairing fields by attribute, which also
closes a gap the old field-named identity left open: a caller
spelling a concept under its own names used to reach stored rules
whose operands it could not bind.

The authored spelling is kept beside the canonical one and is what
the rule stores and shows, so a rule reads back as written; the
content-address check on hydration compares identities, which the
canonical spelling makes a pure function of the rule. A body the
notation cannot express (a raw attribute scan, as in a concept's
implicit rule) has no encoding, hence no identity, and keeps the
spelling it was given.

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

`benches/query_rules.rs` measures four queries over one thousand
entities with a rule concluding `member/of` and `member/title` from a
`stuff` join: the rule-free join (`stuff`), the exact head (`member`),
a subset of the head (`titled`) and a point query by entity
(`member-of`). Wall-clock, in-memory, same machine, before and after:

| query     | main    | this change |
|-----------|---------|-------------|
| stuff     | 5.1-5.9 ms | 6.2 ms   |
| member    | 5.9 ms  | 6.6 ms      |
| titled    | no rows | 7.0 ms      |
| member-of | 0.22 ms | 0.15 ms     |

### Tonk's library

Tonk's own rules are the real load: `account/status` (four rules
electing one case, three of them negated), `space/presence` (four
rules over a device's replica facts) and the notebook's
`block/position` (a keyed collection read through a recursive rule
pair). A harness in tonk (`rust/tonk-evaluator/examples/rule_load.rs`)
seeds the profile and notebook libraries the way the worker does,
asserts 2,000 accounts, 500 spaces and a notebook, and times the
queries the UI subscribes to and the subscriptions' re-polls, once
against the released dialog tonk pins and once against this branch
(dev profile, dependencies optimised, same machine):

| load                                   | released | this branch |
|----------------------------------------|----------|-------------|
| account/status, 1,500 rows             | 76 ms    | 75 ms       |
| account/status, one account            | 0.13 ms  | 0.12 ms     |
| space/presence, 500 rows               | 1.28 s   | 0.93 s      |
| block/position, 40 blocks              | 194 ms   | 7.5 ms      |
| block/position, 454 blocks             | 21.4 s   | 75 ms       |
| seeding 454 blocks (induction)         | 24.8 s   | 0.75 s      |
| account/status subscription, first poll| 499 ms   | 248 ms      |
| space/presence subscription, first poll| 17.3 s   | 5.6 s       |
| re-poll after suspending 10 accounts   | 49 ms    | 32 ms       |
| re-poll of an unrelated subscription   | 30 ms    | 11 ms       |
| re-poll after 10 replicas finish       | 30.8 s   | 7.0 s       |

Row counts and deltas are identical on both. Three things made the
difference, none of them specific to attribute heads, all of them
found by running this load:

- A concept premise reads the fields it binds and the required ones.
  Tonk fills a premise's unmentioned fields with blanks, and `space`
  carries an optional `presence` the `space/presence` rules derive, so
  every rule reading `space` read `presence` back through those rules:
  a cycle the fixpoint answered with nothing. Dropping unbound
  optional fields is also simply less work.
- The fixpoint's delta rounds paired every row of one recursive
  occurrence with every row of the next and planned the body per pair.
  They now bind the delta row and join the rest through the base
  premises, planning once per stage, which is what turns the
  notebook's quadratic second into a linear millisecond.
- The selecting and covering rules are cached per concept, and a rule
  set assembled once is reused by subscriptions, which replay the
  rule-discovery reads it was built from as their demand, where before
  every poll and every per-entity re-derivation assembled it again.

What remains slow is the engine's cost per concept premise evaluated
per row (`space/presence` pays it for two negated premises on every
space) and the subscription's per-entity re-derivation, both older
than this change and both halved or better by it.

Without the shared body, the single-pass election and the covering
rule, `member` was three times main and `titled` twice `member`. With
them `member` is within about a tenth of main, `titled` costs a scan
of `member/title` more than `member`, which it did not answer at all
before, and the point query is faster because its attribute concept
read is a bound lookup. The `stuff` range on main is run-to-run
variation of the random entity layout, which also moves the block-read
counts the bench prints by a block or two between runs.

## Not in this change

- the `select` policy that replaces ordered choice; cardinality one
  still elects by recency alone;
- `reduce` on inductive rules, the materialised home for aggregates;
- election at the exit of a recursive component: a recursive
  cardinality-one attribute concept yields its candidates as a set;
- a rule body naming a derived attribute through a raw attribute
  premise, rather than a concept, reads stored facts only; the
  notation always emits concept premises, so this reaches only the
  Rust API;
- the stratification policy note, which this design withdraws once
  open rules are monotone; until `unless` is refused in deductive
  rules the analysis and its errors stand;
- flattening a concept premise's attribute reads into the enclosing
  conjunction's merge, so two concept applications sharing an entity
  join their attributes in one pass rather than probing.
