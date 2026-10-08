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

**The policy is the attribute's.** A derived value competes with
stored and other derived values for the same entity under the
attribute's policy, exactly as two stored values do: `last` takes the
newest, `max` the greatest, `all` keeps every distinct value. A derived
row's standing is the standing of the newest claim its body consumed:
the row exists because of those claims and is as recent as the latest
of them.

**Election happens at the boundary.** Inside a recursive component the
attribute is read with set semantics: every candidate value is a row,
and a rule in the component sees all of them. Election runs once, over
the finished fixpoint, where the component's rows leave it. The reason
is that a choice is a function of the whole set, and a function of a
set still growing has no least fixpoint. A reader outside the component
sees one value; a rule inside sees the candidates. It is the only place
a derived attribute behaves differently from a stored one, because a
stored attribute never has two live values a rule could tell apart.

## Facts are operation context

A fact carries its cause and its standing: which revision wrote it,
having seen what. That is the operation context a CRDT log keeps, and
it is what makes every attribute a replicated register of a known
kind: `last` is a last-writer-wins register, `top`, `max` and `min`
are ranked registers, `all` is an add-wins set. Two writers that never
saw each other both stand; a reader elects between them by standing,
the same way on every replica; a writer that saw a claim and succeeded
it retracted it. Nothing is ever refused at a merge, because the merge
is a union of claims and the policy reads the union.

## Open rules and quarantine

A deductive rule is *open*: it is installed as replicated facts, any
rule anyone installs later may read its head, and its body is resolved
against whatever program exists when a query runs. A rule set like
that has to mean one thing under every merge, and the thing it means
has to be computable: nothing a merge can produce may make a query
unanswerable. The first attempt at this refused `unless` in deductive
rules, so that every rule would be monotone. That was too much: a
negation whose target no cycle derives is stratified and perfectly
answerable, and most negations are that. It was also not enough: an
election is a negation too, since a read under a choosing policy
returns a candidate *and nothing better*, so a ranked fallback over a
default tests absence as surely as `unless` does.

What is actually required is that the program never meets a premise
it cannot give a stratified meaning: an absence test (an `unless`, a
set-widened read, or an election) whose target is derived in the same
recursive component as the rule, so it would read a set the fixpoint
is still deriving. A merge of rule sets each fine on its own can close
such a cycle, so the program is never refused. The analysis
*quarantines* one rule of the cycle instead, and evaluation leaves it
out (`ProgramAnalysis::quarantined`). It sets aside the rule of the
cycle installed last: every read was well defined before it arrived.
A committed rule is ordered by the commit indexing it (edition, then
version hash, the order a `last` election uses), and an uncommitted
one is newer than every commit, so replicas holding the same history
set aside the same rule however the rules reached them. Among rules
installed together it takes the cycle's plainest absence test, an
`unless` or an optional read before a ranked election,
and sets aside the rule inside the cycle deriving what the test reads,
so the test keeps the meaning it had over everything outside the
cycle; when the test's own rule derives what it tests, that rule goes.
Then the greatest identity. A quarantine lifts once a rule of the
cycle is retracted.

A branch answers `dialog.rule/quarantined` from the same analysis, run
over every rule its layers hold: one row per rule set aside, valued
with the concept it concludes. It is never stored, since it is a
function of the rules and their history, and storing it would let two
replicas write conflicting answers. Outside a component
nothing changes: the negation holds when the fact is absent, the
optional read sees absence, the election picks the best candidate of a
relation derived in full.

An earlier version gave the premise a reading inside the cycle instead
(a negation held, an optional read also yielded the absent row). It
kept every query answering, but its answers ignored even the stored
facts a negation was written against, and an install elsewhere could
change what a negation meant. Quarantine keeps the answering and gives
up only the rule that closed the cycle, named in the report.

Recursion through a ranked election is the cost: a rule reading its
own relation under `top`, a chain, `max` or `min` is quarantined.
Recursion through `last` is not: inside the cycle a `last` read sees
every candidate the fixpoint derives, and readers elect the newest at
the exit, as the fixpoint always has. The component stays positive, so
it has a least fixpoint, and the answer depends on the rules alone. A
notebook needs this: it positions a run of inserted blocks from each
block's successor, reading the position relation under `last`. What
it gives up is the stratified reading of `last` inside a cycle: a
derivation can start from a candidate a reader would not elect. Inheritance
down a hierarchy through a ranked choice, a node's own value or else
its parent's, is what quarantine rules out; lattice-valued recursion,
where the chosen value may feed the recursion as long as it is only
used in ways that respect its order, could admit it later.

So `unless` and optional premises stay in deductive rules. `reduce`
does not: a fold has no reading over a set still growing, and unlike a
choice it cannot be deferred to the component's exit without giving
the rules inside a different relation than the readers outside. A
fold belongs to the closed places, a query, a subscription, an
inductive rule, and is refused in a deductive rule at compile time
(`ReduceInOpenRule`), which the author sees at once and a merge never
does.

Ordered choice, which the old `variants` desugaring expressed by
negating earlier alternatives, is better expressed by election: give
the attribute a listed value domain and select the first listed value
present. Every alternative is then a positive rule, and the attribute
picks. The notation puts the policy beside the carrier type, with
today's cardinalities as two of its values and recency as the default:

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
`max` and `min` are the extremes of a naturally ordered carrier. Every
policy chooses *members* of the candidate set, which is what lets a
rule inside a component read the set and a reader outside read the
choice without disagreeing about what the relation holds. Folds
(`sum`, `count`, `avg`) are not policies: they make a value no
candidate is, and belong to `reduce` in the closed places. Only `last`
depends on history rather than on the values present, which is why it
is the one policy under which a derived row needs a standing of its
own.

Three words, used strictly. A **relation** is what `the` names: the
`(domain, name)` pair facts are stored under, which rules derive into
and are found by. An **attribute** is a relation read under a type and
a selection policy; two reads of one relation under different policies
are two attributes, with distinct identities, and `select` on the
attribute descriptor is where the engine keeps the policy. A **field**
is a slot of a concept that holds an attribute, and the slot can be
optional. Cardinality is the policy's arity, `all` being many and
every other policy one; `cardinality: one` and `many` are read as the
older spellings of `last` and `all`, and tonk's notation no longer
writes them: `select: all` where it said `many`, nothing where it said
`one`.

A candidate is a fact: a set read sees each distinct value of the
relation once, however many rules derive it, or each distinct entry of
a keyed collection.

### Writing through an attribute

The policy decides the write too, since a write is a claim that
succeeds what the attribute stands for. A write under `all` appends.
A write under `last`, `max`, `min` or `top` succeeds the candidate a
read under the policy returns: the attribute is read for the entity
at commit, through the commit's own view and without the written
value, and the stored claim holding the elected value is retracted
beside the new one. Every other claim stays, since the policy may
elect it again once the new value is gone; under `last` that leaves
an older concurrent claim live where the cardinality-one replacement
retracted every prior, which no `last` read can tell apart. What a
write observes is the line and the writes before it in its own
transaction, in order: the statement records the assertion with its
policy (`Change::Assert(value, policy)`, the one write form beside a
retraction), the transaction keeps its writes in order, and
the tree elects among the cell's stored claims in the descent that
writes the value, retracting the claim it elects and recording the new
claim with the elected claim's versions as its cause, as a replacement
did. Where a rule derives the relation the tree cannot see the derived
candidates, so the commit first replays that cell's writes over the
claims the line holds, each succession electing among the live claims
and the derived candidates. The guarantee
is transactional: an assertion succeeds whatever a reader at that
point in the transaction would have observed, and `all` is the one
policy that asserts without succeeding anything. The transaction's
own reads settle its writes the same way on the first read, so what
a transaction reads is what its commit will leave: a transaction is a
commit not yet flushed, and every write on top squashes into it: two
`last` writes of one cell leave the later one alone, and a claim a
later retraction cancels leaves no tombstone, since it never reached
the line. A staged write stands at the edition the commit will mint,
equal to every other write of the transaction as one commit's claims
are; their order matters to settlement and to nothing else. A
candidate a rule derives is not a claim:
when the read elects a derived value nothing is retracted, and the
write stands beside it as one more candidate. Two writers succeeding
the same claim concurrently each retract it and assert their own; the
merge keeps both, and the read elects.

A derived candidate competes with a stored claim under `last` by its
standing: the standing of the fact that bound the value its head
carries, carried across every concept boundary with the value. A
change to an unrelated input of the body does not move it. A value a
formula computed cites no fact of its own and stands as the newest
fact the body consumed.

Nothing about the write is a second policy. A relation that should be
read one way and written another is two attributes: read through one,
write through the other.

The claim a write succeeds is one the line holds: a stored claim, or
none where the read elects a candidate a rule derives. The session
overlay is the newest facts: an overlay row stands past the edition
the next commit mints, above every committed claim and every staged
write, so `last` returns it and the other policies rank it with the
rest. It is not a claim a commit can take back. A succession the read
resolves to an overlay row retires nothing and stands beside it,
exactly as beside a derived candidate, and the written claim is what
the read elects once the session drops its row. A transaction reads
the overlay above its own writes, as a read after the commit will.
Retracting an overlay row is a write on the overlay, the session's to
make. The tree settles a succession alone only where it can see every
candidate; a cell the overlay holds goes through the transactor's
settlement, which reads the union.

A list is a ranked choice. `as: [case:active, case:registered]`
lists the values the attribute ranks among, best first, and `the:
[user/email, user/phone]` lists the relations it reads, best first:
either implies `top`, and no other policy fits a list. A field over
several relations gathers candidates from every relation's facts and
rules, and the first listed relation offering one wins, so a contact
handle is the email where there is one, stored or derived, and the
phone otherwise. Discovery and the dependency graph follow each
listed relation. With the entity bound, a `top` over listed relations
reads them best first and stops at the first that offers a candidate,
since nothing a later relation offers can outrank it; with the entity
free every relation is read once.

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
with a constant `the`, is rewritten the same way when the registry
assembles a bundle: through `{ a }` when `a` is derived, under the
policy the premise's cardinality implies (`last` for one, `all` for
many or none), raw otherwise; a negated premise negates `{ a }` and an
optional one reads it set-widened, and the dependency graph sees the
edge. A premise whose attribute is a variable reads stored facts only;
nothing can resolve rules for an attribute it does not know. A stored
rule always carries concept premises, the notation emitting nothing
else, so this reaches the Rust API alone.

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

`{ a }` under a choosing policy elects in one pass over every source
at once: the stored scan, each attribute-headed rule evaluated in
scope, and each head split from a source rule (through the rows the
body was remembered to yield, below). The candidates are grouped by
entity and run through the attribute's election with the row's
standing. A stored row's standing is its artifact's, the revision
version then the cause, exactly what the stored election compares. A
derived row's is the maximum standing among the facts its `Match`
cites, which is every fact a premise bound on the way to the head,
carried across every concept boundary the row crossed. The election
is commutative, so the order rows arrive in does not matter. A tie
falls to the value's bytes. Inside a recursive component the election
is skipped and the rows stay a set; under `all` they stay a set
everywhere, each distinct value once.

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

A deductive rule refuses `reduce` (see quarantine above). The
plumbing that split a reducing rule's heads and folded them stands in
the code until inductive rules take `reduce`, where a fold has a
sealed state to read and a fact to write.

### Storage: `dialog.rule/derives`

Installing a rule writes, beside `source`, `conclusion` and `reads`:

```
dialog.rule/derives  of  rule-entity  is  on:<domain>/<name>
```

one per head attribute, using the same `on:` reach entities `reads`
and `on` use. Discovery for `{ a }` is one value-constrained selector,
the shape `expand_through_deduction` already probes `reads` with.
`conclusion` keeps being written for tooling that lists rules by the
concept they were written against; it is not consulted on the query
path. A rule that predates the `derives` index is inert until
`Branch::upgrade_rules` re-installs it, which writes its index.

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
content-address check on hydration, on every read and at commit,
compares identities, which the canonical spelling makes a pure
function of the rule. A body stored before identities were canonical
sits under the hash of its bytes and is inert, like bytes under any
other entity, until `Branch::upgrade_rules` re-installs it under its
identity. A body the notation cannot
express (a raw attribute scan, as in a concept's implicit rule) has
no encoding, hence no identity, and keeps the spelling it was given.

### Caches

Discovery is per attribute and head-tagged, as it was per concept.
Hydration is per rule entity. The plan cache keys by `(rule, adornment)`
and a re-spelled single-head rule has a content address of its own, so
plans for `{ a }` cache like any rule's. The descriptor memo of the
implicit plan is unchanged for underived concepts; for a concept with
a derived field the implicit rule differs by which premises are
concept premises, so it is memoised per resolution outcome. Whether
an attribute has nothing stored under it, which the covering rule's
path asks on every evaluation of its concept, is remembered on the
query's memo beside the bodies.

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
the existing ones. `reduce` in a deductive rule is a compile error
that names the closed forms to use instead; `unless` and optional
premises compile, and an absence test inside a recursive component is
reported for the analyzer to warn about. A field that reads a relation
under `max`, `min` or `top` writes by succeeding the claim it elects.

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
| account/status, 1,500 rows             | 76 ms    | 38 ms       |
| account/status, one account            | 0.13 ms  | 0.12 ms     |
| space/presence, 500 rows               | 1.28 s   | 38 ms       |
| block/position, 40 blocks              | 194 ms   | 8 ms        |
| block/position, 454 blocks             | 21.4 s   | 80 ms       |
| seeding 454 blocks (induction)         | 24.8 s   | 0.76 s      |
| account/status subscription, first poll| 499 ms   | 56 ms       |
| space/presence subscription, first poll| 17.3 s   | 58 ms       |
| re-poll after suspending 10 accounts   | 49 ms    | 27 ms       |
| re-poll of an unrelated subscription   | 30 ms    | 11-30 ms    |
| re-poll after 10 replicas finish       | 30.8 s   | 0.29 s      |

Row counts and deltas are identical on both. With the peer's blocks
on the filesystem (`STORAGE=disk`) instead of in memory the numbers
move by a few milliseconds either way: the node cache absorbs the
reads once a tree is warm, so the engine, not block I/O, is what
these loads measure. Neither build replicates between peers here.

Five things made the difference, none of them specific to attribute
heads, all of them found by running this load:

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
- A negated premise is an anti-join. It was a probe per candidate row,
  each setting the inner pipeline up again, rules and all. Now the
  candidates are buffered; with sixteen or more, the negated relation
  is read once with nothing bound and the candidates are hashed
  against it on the variables both bind, and with fewer they all go
  through the inner plan in one pass. `space/presence` pays this for
  two negated premises on every space, which is where its second went.
- A subscription's demand cover was a vector of ranges merged by a
  linear pass per record, quadratic in what an evaluation reads; a
  poll spent four fifths of its time there. It is an ordered map now.
  Separately, a conjunction decided merge against fold per distinct
  set of bound values, estimating every scan's range per row, which
  walked the tree about as much as the scan did; it decides once.

The re-poll after a change looked like thirty milliseconds per
affected entity. Profiled, nine tenths of it was not the re-derivation
but finding the affected entities: discovery took every concept a
rule reads as touched by every changed subject, so a replica's
status change was joined sideways through `space`, `db/session`,
`space/replica` and `space/replicating` as well, and a premise
naming none of `this` joined the rest of the body with nothing
bound, which is the whole relation again. A concept selecting stored
attributes is touched only by a change under an attribute it reads,
which leaves the one concept that changed and a lookup per changed
subject. Ten replicas' status changes re-polled the presence
subscription in 287 ms before and 103 ms after, with the same delta.
What remains is the re-derivation itself: each affected entity
evaluates every presence rule's body with `this` bound, and each
body's negated premises evaluate a concept query of their own, with
its rule resolution, its merge decision and its index probes, so
forty bodies cost about a hundred negation pipelines. Writing the
presence and status rules positively, as ranked choices, removes that
cost from these rules rather than optimising it; `unless` stays
available for the rules that need it.

Without the shared body, the single-pass election and the covering
rule, `member` was three times main and `titled` twice `member`. With
them `member` is within about a tenth of main, `titled` costs a scan
of `member/title` more than `member`, which it did not answer at all
before, and the point query is faster because its attribute concept
read is a bound lookup. The `stuff` range on main is run-to-run
variation of the random entity layout, which also moves the block-read
counts the bench prints by a block or two between runs.

## Not in this change

- `reduce` on inductive rules, the materialised home for aggregates,
  and with it the removal of the reducing-rule plumbing deductive
  rules no longer reach;
- the aggregation half of the stratification analysis, unreachable
  now that deductive rules refuse `reduce`, which still stands in the
  code until removed;
- a `select` attribute on `#[derive(Attribute)]`, so a Rust-declared
  concept reads a ranked field as the notation does;
- policies as rules: a `policy!:` form that lets an author define how
  an attribute chooses among its candidates, beyond the five built
  in;
- variants, so a listed `as:` domain reads as a tagged type rather
  than as values of one carrier;
- a presence-narrowing operator, so a rule can state that an optional
  it reads is present and have the type checker take it from there,
  where today `coalesce` is the one way to use a `?T` where a `T` is
  wanted;
- the tonk analyzer's warning for an absence test inside a recursive
  component, which needs the analyzer to run the dependency analysis
  over the library it is checking against;
- flattening a concept premise's attribute reads into the enclosing
  conjunction's merge, so two concept applications sharing an entity
  join their attributes in one pass rather than probing.
