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
the same policy returns. At commit the transactor reads the attribute
for that entity, through the commit's own view and without the value
being written, and retracts the stored claim holding what the read
elected, beside asserting the new value. Every other claim in the cell
stays. A write under `all` appends, as before.

What this changes for you:

- A cardinality-one write used to retract every prior claim of its
  cell (`Instruction::Replace`). It now retracts the one a `last` read
  would have returned. An older concurrent claim that lost the
  election stays live. No `last` read can tell the difference; an
  `all` read over the same relation can, and will now see it.
- If the read elects a candidate a rule derives, nothing is retracted:
  a derived candidate is not a claim. The write lands beside it and
  competes under the policy.
- Writing a value the cell already holds writes nothing, as replacing
  with the value already held did: the revision's tree does not move.
- Statements emit `Change::Succeed` (a new variant of
  `dialog_artifacts::Change`, with a `Succession` saying which policy)
  instead of `Replace`. A `Changes` batch holding one serializes with a
  `succeed` key that older readers do not know. Batches are not stored
  or sent between replicas today, so this bites only code that encodes
  a batch itself.
- A `Changes` batch holding successions must commit through a
  transaction (`branch.transaction().integrate(changes).commit()`),
  which resolves them. `branch.commit(changes)` applies the batch raw
  and turns each succession into a plain assertion: the value lands,
  nothing is retired.
- Inductive rule heads follow the same rule: an `assert!:` head under
  a choosing policy succeeds the claim it elects against the round
  view the rule fired on.
- Within a transaction, a staged write now stands at the edition the
  commit will mint, so `transaction.query()` elects your own pending
  write over committed claims, which is what the commit will do. A
  later write to the same cell in one transaction succeeds the earlier
  staged one, which was never committed and simply disappears.

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

### `unless` and optional premises evaluate under the cycle policy

[#583](https://github.com/dialog-db/dialog-db/pull/583), detail in
[`notes/attribute-heads.md`](./notes/attribute-heads.md).

A deductive rule admits `unless` and optional premises. Outside a
recursive component they mean what they always meant. Inside one, where
the premise would read a relation the fixpoint is still deriving, the
engine no longer refuses the program. It evaluates the premise under
the cycle policy: a negation holds, and an optional read yields the
absent row for every entity the rule otherwise derives, beside the
present rows. The component stays positive and has a least fixpoint.

What this changes for you:

- `EvaluationError::NegationThroughRecursion` is gone. A program that
  used to fail at query time now answers. If you relied on the error
  to catch a rule negating into its own cycle, read
  `ProgramAnalysis::absences()` instead: it lists every premise the
  policy governs, with the concept it tests. Tonk will surface these
  as warnings; the plumbing for that is a follow-up.
- `TypeError::NegationInOpenRule` is gone. It existed on this branch
  only, between the refusal landing and this change.
- `reduce` in a deductive rule is still refused
  (`TypeError::ReduceInOpenRule`). A fold has no reading over a set
  that is still growing. Put the fold in a query, a subscription or an
  inductive rule.

Why: a deductive rule is installed as facts and read by whatever
program exists when a query runs, so a set of rules merged from
several replicas has to be evaluable and has to mean one thing. The
first attempt bought that by refusing negation outright, which threw
away every stratified negation to prevent the unstratified ones. The
cycle policy keeps the goal and drops the cost: no merge can produce a
program a query cannot answer, and every replica derives the same rows
from the same rules and facts, whatever order they arrived in.

What is and is not guaranteed, stated plainly. Guaranteed: every
program evaluates; evaluation is deterministic and independent of
arrival order; inside a component derivation is monotone. Not
guaranteed: that a rule's derived set only grows as rules land. A
negation or an optional read outside a cycle can lose derivations when
a rule starts deriving what it tests, and a rule that closes a cycle
through such a premise changes the premise's meaning from stratified to
the cycle policy. Both are deterministic; neither is monotone in the
rule set.

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
  ordered. Two authors writing one rule under different names install
  one rule. A body stored under the older byte-hash identity is still
  accepted on hydration.
- The dependency graph, the fixpoint and the registry key an attribute
  concept by its relation, so every read of a relation, under any type
  or policy, meets the rules deriving it.

Why: whatever is true of an asserted fact must be true of a derived
one. Asserting `Employee` writes two attribute facts and nothing else;
deriving it must land in the same place.
