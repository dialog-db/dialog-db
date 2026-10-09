# Changelog

What changed, what it may break, how to move across, and why we did it.
Entries are grouped by the change a reader has to understand, not by
commit; each links the pull request that landed it and the note that
argues it in full. Newest first.

## Unreleased

### Writes succeed the claim the policy elects

[#583](https://github.com/dialog-db/dialog-db/pull/583)

A write through an attribute read under `last`, `max`, `min` or `top`
no longer replaces its cell. It succeeds one claim: the one a read under
the same policy returns. The tree elects that claim among the cell's
stored claims in the same descent that writes the value, retracts it,
and records the new claim with the elected claim's versions as its
cause, as a replacement did. Every other claim in the cell stays. Where
a rule derives the relation, the transactor settles the write first
against the derived candidates the tree cannot see, through the
commit's own view. A write under `all` appends, as before.

The guarantee this gives is transactional. A transaction is a commit
that has not been flushed: querying it returns what querying the
committed result would, every write on top is squashed into that one
commit, and an assertion succeeds whatever a reader at that point
would have observed. `all` is the one policy that asserts without
succeeding anything.

What this changes for you:

- A cardinality-one write used to retract every prior claim of its
  cell (the former `Instruction::Replace`). It now retracts the one a `last` read
  would have returned. An older concurrent claim that lost the
  election stays live. No `last` read can tell the difference; an
  `all` read over the same relation can, and will now see it.
- If the read elects a candidate a rule derives, nothing is retracted:
  a derived candidate is not a claim. The write lands beside it and
  competes under the policy.
- Writing a value a read under the policy already returns writes
  nothing: the revision's tree does not move. Writing a value the cell
  holds as a claim the read does not return succeeds the claim it does
  return, and folds the commit's version into the held claim, so the
  written value stands past the claim it succeeded and a read returns
  it. Under `last` the write is the newest fact of the cell, as a
  last-writer-wins write is.
- Every write carries its policy. `Change::Assert(Value, Policy)` and
  `Instruction::Assert(Artifact, Policy)` are the one write form, beside
  `Retract`; `Change::Replace`, `Instruction::Replace`, `Change::Succeed`
  and `Instruction::Succeed` are gone. `dialog_artifacts::Policy` is the
  policy an attribute is read under (`Last`, `All`, `Top(..)`, `Max`,
  `Min`); `Update::associate` takes it as its fourth argument, and
  `associate_unique` and `succeed` are gone with the variants they
  wrote. A write under `All` appends; any other policy succeeds the
  claim a read under it returns. A `Changes` batch serializes each
  assertion with its policy, a shape older readers do not know. Batches
  are not stored or sent between replicas today, so this bites only
  code that encodes a batch itself.
- The reset `Replace` performed, retracting every claim of the cell
  whatever a reader observed, has no spelling any more. A caller that
  wants it retracts what it observed under `all` and asserts.
- `Branch::commit` and `Snapshot::commit`, which took a stream of
  instructions, are no longer public: they ran no induction and saw no
  derived candidate, so a write through them could land a value
  without succeeding what a reader observed. Every write goes through
  a transaction. A caller holding instructions collects them into a
  batch (`Changes` implements `FromIterator<Instruction>`) and
  integrates it: `branch.transaction().integrate(changes).commit()
  .publish()`; an empty commit is `branch.transaction().commit()
  .allow_empty().publish()`.
- What a write observes is the line and the writes before it in its
  own transaction, in order. A cell's writes are replayed in that order
  at commit: a write under `all` after a write under `last` stands
  beside it, and a write under `max` after an `all` write succeeds the
  staged claim it elects. An inductive rule's head observes the round
  view it fired on.
- A transaction reads what its commit will leave. Reads and the commit
  share one settlement: the log is settled in order against the line
  and the writes before each one, the first read settles what is
  logged so far, a later read or the commit settles only the writes
  logged since, and the result is kept while the lines read stay the
  same. So the claim a write succeeds is already gone from
  `transaction.query()`, and what a read showed is what lands.
  Two `last` writes of one cell in one transaction leave the later one
  alone, never both and never a hash-decided one, and a staged claim a
  later write retires or retracts leaves no tombstone: it never
  reached the line.
  A staged write stands at the edition the commit will mint, equal to
  every other write of the transaction, as the claims of one commit
  are; what order the transaction wrote them in is settlement's
  business alone, never a reader's.
- One departure remains: commit-time induction runs at commit, so what
  an inductive rule would derive from the transaction's writes is not
  in `transaction.query()` yet. That was true before this change.
- The session overlay is the newest facts. An overlay row stands
  past the edition the next commit mints, above every committed claim
  and every staged write, so a `last` read returns it over a committed
  claim of the same cell (it used to lose to any committed claim, and
  could only shadow one by retracting it first), and `max`, `min` and
  `top` rank it with the rest. A commit never takes an overlay row
  back: a succession the read resolves to an overlay row retires
  nothing and stands beside it, as beside a candidate a rule derives,
  and a transaction reads the overlay above its own writes. A
  transaction that retracts an overlay row takes it off the overlay
  when the commit publishes, so the row stays gone after the commit as
  it was in the transaction's reads. A cell the overlay holds is
  settled by the transactor, not the tree.
- Concurrent `last` writes settle by commit. Candidates of equal
  edition order by version hash before value, in the tree, the
  transactor and every read alike, so when two replicas' commits meet,
  one commit's writes win every cell they share, never some cells each.

Why: `Replace` encoded one policy, last-writer-wins, in the write path,
while reads had grown four. A write is a claim that succeeds what the
attribute currently stands for, and what it stands for is the policy's
to say. Making the write follow the read is what lets a relation be
read one way and written another: declare two attributes over it.

### A derived value stands by the fact that bound it

[#583](https://github.com/dialog-db/dialog-db/pull/583)

Under `last`, a value a rule derives competes with stored claims by
standing. Its standing is now that of the fact that bound its value,
carried across every concept boundary with the value. It used to be
the newest fact the rule's body touched, so an unrelated input landing
could make a derived value look fresher than the stored claim it was
competing with. A value a formula computes cites no fact of its own
and keeps the old behaviour.

A recursive relation's values stand the same way. Each row of the
fixpoint stands as the newest of its derivations, carried from row to
row as the fixpoint runs; a row derived again from a newer fact
re-enters the next round, so what it derives stands as new. A
subscription that maintains the fixpoint across polls keeps the
standings too: a deletion re-derives a suspect row at the newest
derivation that survives it.

### `unless`, optional premises and elections are stratified; a rule closing a cycle through one is quarantined

[#583](https://github.com/dialog-db/dialog-db/pull/583), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule admits `unless` and optional premises. A read under a
ranked policy (`top`, a relation chain, `max`, `min`) is treated the
same way: it returns a candidate *and nothing better*, which negates
the better candidates, so a ranked fallback is an absence test however
it is spelled. A read under `last` is too outside a cycle; inside one
it reads every candidate and elects at the cycle's exit (see below). Outside a recursive component all of these are stratified:
the premise reads a relation derived in full before the rule runs, and
rules can derive, decide and derive again from the decision.

Inside a component, such a premise would read a relation the cycle is
still deriving, which has no stratified meaning. Merging rule sets that
are each fine can close such a cycle, so the program is never refused.
Instead the program analysis *quarantines* one rule of the cycle, and
evaluation leaves it out:

- It sets aside the rule of the cycle installed last. Every read was
  well defined before that rule arrived, so setting it aside keeps
  them as they were. A committed rule is ordered by the commit that
  indexed it (edition, then version hash, as a `last` election orders
  claims); a rule not yet committed (a session's, a transaction's, a
  query's own) is newer than every commit. A `RuleRegistry` orders
  rules by registration.
- Among rules installed together, it takes the cycle's plainest
  absence test (an `unless` or an optional read first, then a ranked
  election) and sets aside the rule inside the cycle that
  derives what that test reads; when the test's own rule derives what
  it tests, that rule.
- Then the greatest rule identity.

The order comes from the commits, which every replica sees the same,
so replicas holding the same history quarantine the same rule, however
the rules reached them. A quarantine lifts by itself once a rule of
the cycle is retracted. A plain recursion under `all`, such as an
ancestor closure, is positive and unaffected.

What this changes for you:

- A query reads the rules a branch sets aside as
  `dialog.rule/quarantined`: one row per rule, of the rule's identity,
  valued with the concept it concludes. It is never stored; the branch
  answers it from the rules its layers hold (committed, session,
  staged and the query's own), so it changes as rules are installed
  and retracted. Facts written under the attribute are not read.
- `ProgramAnalysis::quarantined()` lists the rules set aside, each with
  the concept it concludes and the cycle it closed.
  `ConceptRules::without` is how a bundle leaves them out, and
  `ConceptRules::install_at` installs a rule with when it was
  installed (`Installed`).
- A recursive rule may read its own relation under `last`: inside the
  cycle the read sees every candidate the fixpoint derives, and its
  readers elect the newest once the cycle is derived in full. A
  notebook positioning a run of inserted blocks from each block's
  successor relies on this. A read under a ranked policy (`top`, a
  relation chain, `max`, `min`) inside its own cycle is an absence
  test and is quarantined: a default chain is "this, else the
  default", which has no stratified meaning while "this" is still
  being derived. Inheritance down a hierarchy through a ranked choice
  (a node's own value, else its parent's) is the case this rules out;
  lattice-valued recursion could admit it later.
- `EvaluationError::NegationThroughRecursion` is gone, and the cycle
  policy that replaced it on this branch is gone too.
  `ProgramAnalysis::absences()` now lists only what a cycle with no
  rule to set aside still holds.
- `TypeError::NegationInOpenRule` is gone. It existed on this branch
  only.
- `reduce` in a deductive rule is still refused
  (`TypeError::ReduceInOpenRule`). A fold has no reading over a set
  that is still growing. Put the fold in a query, a subscription or an
  inductive rule.

Why: rules are installed as facts and merged from several replicas, so
whatever set arrives has to be evaluable and mean one thing everywhere.
Refusing negation outright threw away every stratified negation to
prevent the unstratified ones; reading a negation inside a cycle as
holding (the cycle policy) answered, but answered wrongly, ignoring
even stored facts the negation was written against. Quarantine keeps
every query answering and every answer a stratified one, and reports
what it set aside.

What is guaranteed: every program evaluates; evaluation is
deterministic, and replicas with the same history agree; what a query returns
is the stratified answer of the program minus the quarantined rules.
What is not: that a rule's derived set only grows as rules land. A
negation can lose derivations when a rule starts deriving what it
tests; that is what negation means.

### Selection policies choose members; aggregators are gone from `select`

[#583](https://github.com/dialog-db/dialog-db/pull/583)

`select` is one of `last`, `all`, `top`, `max`, `min`. Each returns
members of the candidate set, which is what lets a rule inside a cycle
read the set and a reader outside read the choice without disagreeing
about what the relation holds. `sum`, `count`, `count-distinct` and
`avg` are not policies and have been removed, with the carrier
distinction (`PolicyInOpenRule`) that existed to fence them. They were
introduced on this branch and never released. Use `reduce` in a query,
subscription or inductive rule for folds.

Other changes to policies on the same branch, for a reader who did not
follow it:

- An attribute is a relation (what `the` names) read under a type and
  a policy. Two policies over one relation are two attributes, with
  distinct identities. A field is a concept slot holding an attribute,
  and can be optional. These are the words the code and the notes use.
- Cardinality is derived from the policy: `all` is many, every other
  policy is one. `cardinality: one` and `cardinality: many` are read as
  the older spellings of `last` and `all`, and never written. Tonk's
  notation emits `select: all` where it said `many` and nothing where
  it said `one`.
- A list is a ranked choice. `as: [a, b]` ranks values, `the: [x, y]`
  ranks relations, best first; either implies `top`. With the entity
  bound, a `top` over listed relations stops at the first relation
  that offers a candidate. Tonk's `among:` is gone; write the list
  under `as:`.
- A rule body naming a derived relation through a raw attribute
  premise (Rust API only; the notation always writes concept premises)
  now reads the derived candidates as well as the stored facts,
  negated and optional premises included.

### Rules derive attributes; concepts select them

[#580](https://github.com/dialog-db/dialog-db/pull/580), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule's head is a set of attribute triples, and a concept
head is sugar for one triple per field. A rule contributes to the
relation each attribute names, and any concept reading that relation
sees the derived values beside the stored ones. Before this, a rule
deriving `Employee { name, role }` was visible only to a query for
`Employee`; a query for `Named { name }` did not see the name.

What this changes for you:

- A subset concept now sees derivations. If a concept's result set
  grew after upgrading, this is why.
- Installing a rule writes `dialog.rule/derives` facts, one per head
  attribute, beside `source`, `conclusion` and `reads`. Discovery
  reads `derives`; `conclusion` is kept for tooling and for rules
  installed before the index existed, which keep resolving.
- A rule's identity is the hash of its canonical spelling, not of the
  bytes you wrote: variables are renamed by structure and premises are
  ordered. The field names a premise binds a concept's fields under
  are renamed by structure too, so two premises reading the same
  attributes under different field names are one premise. Two authors
  writing one rule under different names install one rule. A body
  stored under any other entity, the older byte-hash identity
  included, is inert on every read and at commit until
  `Branch::upgrade_rules` re-installs it (see below).
- Every rule has an identity, including one a program registers with a
  `RuleRegistry`: a rule whose premises cannot be spelled canonically
  (a raw `AttributeQuery` scan) is refused
  (`EvaluationError::RuleWithoutIdentity`). Write the premise as a
  concept or attribute read, as an installed rule would.
- The dependency graph, the fixpoint and the registry key an attribute
  concept by its relation, so every read of a relation, under any type
  or policy, meets the rules deriving it.

Why: whatever is true of an asserted fact must be true of a derived
one. Asserting `Employee` writes two attribute facts and nothing else;
deriving it must land in the same place.

### Compatibility and migration

What a replica holds from the release before this branch, and what
happens to it.

- Stored claims are unchanged: a claim is its attribute, entity,
  value and cause, with the versions that carry it. A cell the older
  release wrote through a cardinality-one replacement holds one claim,
  and a `last` read returns it, as before. Nothing is rewritten.
- Stored rules decode. A rule is stored as the dag-cbor of its
  descriptor, and the older descriptor spells `cardinality: one` or
  `many` and a single relation under `the`; both read as `last` and
  `all` over that relation. Re-encoded, a rule never writes the older
  spelling back.
- Rules installed by the older release are inert until upgraded. Such
  a rule sits under the hash of its stored bytes, with its conclusion
  and its body but no `derives` fact. No read looks for it there, so
  no read pays for rules that may not exist. `Branch::upgrade_rules()
  .perform(env)` reads the branch's rule bodies, retracts the facts of
  every rule stored under an entity that is not its identity, and
  installs it again under its identity, in one commit. It is
  idempotent, and two replicas upgrading concurrently converge, since
  a rule's identity is a function of the rule. It needs the branch's
  `dialog.rule/` range, not a full replica. A replica still on the
  older release can write such rules again and sync brings them in;
  they stay inert until the upgrade runs again. When to run it, once
  or after every pull, is the embedder's decision.
- Every attribute and concept identity changes: an attribute's
  identity hashes the relations it ranks, its policy and its ranked
  values beside its domain, name and type, so two reads of one
  relation under different policies are two attributes. The upgrade
  moves the `dialog.concept/transient` markers keyed by the older
  identities, for the concepts the branch's rules conclude.
  `dialog_query::migration` reproduces the older identities, for an
  application that keyed facts of its own by them.
- Library YAML in the older spelling parses: `cardinality:` is read as
  `select: all` for `many` and the default `last` for `one`, and a
  `select:` beside it wins. tonk's standard libraries carry a seed
  version that is the hash of their text, so a worker on this release
  sees the mismatch on first start, retracts the installed library's
  facts by the provenance it recorded, and installs the new one.
- The older release cannot read what this one writes. A rule written
  here may list relations or values under `the` or `as`, which the
  older descriptor refuses, and carries `select`, which the older
  descriptor ignores, reading a `max` as `last`. A batch of changes
  serializes each assertion with its policy. Upgrade every replica of
  a repository together, or upgrade readers before writers: a replica
  on the older release pulls such a branch, but a read that meets a
  rule it cannot decode fails with the decoding error.
- A descriptor's JSON no longer spells `cardinality`. It writes
  `select` where the policy says more than the lists imply (a ranked
  `the` or `as` list reads as `top`, a plain attribute as `last`), so
  a plain `last` field carries neither key. A program that told a
  one-valued field apart by `cardinality: one` reads `select` instead:
  a field is one-valued unless it says `select: all`. tonk's template
  planner did this; a reader rebuilding a descriptor from stored facts
  has to keep the policy and the ranked list beside the cardinality,
  or a `top` field comes back as `last`.
- Programs the older release refused run. `unless` and optional
  premises inside a recursive component were an error
  (`NegationThroughRecursion`); the analysis now quarantines a rule of
  such a cycle and every query answers, without the rule.
