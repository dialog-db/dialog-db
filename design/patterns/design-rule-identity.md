# Design decision: the identity of a rule that holds a pattern

**Problem it serves:** the direction says no stored format and no wire form changes for patterns now, and that what installing a rule that holds one does is to be said. It also asks for a rule that builds a record to run.

**Decision:** whether a rule that holds a pattern has an identity, and what gives it one.

## Facts the matrix rests on

- A rule runs only when it is registered, and `RuleRegistry::register` refuses a rule with no identity (`EvaluationError::RuleWithoutIdentity`, `rust/dialog-query/src/session/rule_registry.rs`). The program analysis sets rules aside by identity.
- A rule's identity is a merkle reference of its canonical spelling. A concept premise is referenced by its concept and its bindings, and every other premise by its wire form read as a merkle structure (`rust/dialog-query/src/rule/reference.rs`).
- The canonical spelling orders premises and labels variables by each premise's dag-cbor (`encode_premise`, `rust/dialog-query/src/rule/canonical.rs`). Those bytes are compared and never stored.
- A rule's stored body is the dag-cbor of the same serde form, in the format `body::FORMAT` names (`rust/dialog-query/src/rule/body.rs`): format 4 on main, since cardinality (#827).
- `Statement::assert` and `Update` return nothing (`rust/dialog-query/src/rule/statement.rs`).

| Decision: what gives a rule that holds a pattern its identity |
|---|---|---|---|
| criterion \ approach | A: nothing. It has no wire form, so no identity | B: a serde form for a pattern, written for identity and refused for storing | C: a reference for a pattern, by its parts, and no serde form |
| the rule runs | 🔴 it cannot be registered, so step 2's rule cannot be tested | 🟢 | 🟢 |
| no wire form is invented | 🟢 | 🔴 a serde form is a wire form to every serializer: JSON would write it wherever a rule or a query is printed, and the stored body would hold it unless each path refused | 🟢 `Serialize` for a pattern above a leaf is an error on every path |
| one rule under every spelling | n/a | holds | 🟢 holds: a pattern's reference does not depend on the order its entries were given in, and the labeling sees the variables inside it at their places (`Parameters::addressed`) |
| a layout of a pattern's bytes | n/a | 🟡 the identity would hash it | 🟢 none: a pattern is named by its parts, as the value it builds is |

## Decided

C, by the builder.

- A pattern has a reference (`pattern_reference`): a leaf as the term it is, and a map or a list as a map from `map:` or `list:` to its parts' references, with its `rest` or `run` beside them. An equality with a pattern on a side is referenced as its wire form would be read, each side by that reference, so an equality between two terms has the reference it had.
- Where the canonical spelling orders premises by their dag-cbor, a premise that holds a pattern is ordered by its reference instead, under a tag of its own.
- A composite constant in a rule is named as `Value::reference` names it, by its parts. An opaque `record:` constant is named by its bytes, as on main.
- A rule that holds a pattern has an identity (`try_this`), runs registered, and has no stored body (`try_encode` is `None`).

## What installing one does

Built first on #778, and superseded by format 5 (below) for every pattern a body can write.

On #778 it was refused, with the reason, and nothing was written. `DeductiveRule::storable` and `InductiveRule::storable` returned `TypeError::UnstorablePattern`, which named the pattern and the format of a rule body that release wrote, which had no way to write one. That refusal still stands for what format 5 cannot write (below), with the same typed error, and works the same way.

`Statement::assert` cannot return an error, so the refusal is a state of the batch it writes into:

- `Update::refuse(Refusal)` records it. A `Refusal` carries the rule's entity and the reason, typed (`Refusal::reason::<TypeError>()`). `Changes` keeps the first one it is handed.
- Asserting such a rule refuses first and then asks the target to hold the rule (`Update::hold`). A batch that is committed cannot hold it, so nothing is staged for the rule: no `source`, and no `conclusion`, `derives` or `reads` index fact either, so a durable batch never names a rule with no body.
- A transaction carries the refusal in its staged writes, and its commit returns `CommitError::Refused` before it settles, induces or writes anything: the head does not move, and nothing else the transaction held is written. A failed commit writes nothing whatever failed it, so the other statements beside the rule are not written either.
- An ephemeral line (`Ephemeral::apply`, the branch's session overlay) is exported and read back as facts, so it refuses such a batch with `DialogCorpusError::Refused` and lands nothing.
- A `Changes` that holds a refusal does not serialize.
- An inductive rule fires at commit, from its stored body, so one whose pattern no body can write is refused wherever it is asserted, a query layer included.
- Retracting a deductive rule that has no body retracts its index facts and no body, since it never had one; retracting an inductive one retracts nothing.

## Format 5, as built

**The format that stores one.** Rule body format 5 is format 4 with a pattern where a parameter stands (`rust/dialog-query/src/rule/body.rs`). The maintainer, 2026-11-20: "this becomes format 4 and stuff with patterns becomes 5". The wire form is the one decided for #799 on 2026-11-14 (`Pattern`'s `Serialize`, `rust/dialog-query/src/pattern.rs`):

- A leaf is written as a term is. A map or a list is written under the one key `match`: under it a map is a map pattern and an array a list pattern, at any depth, with no `match` again. `...` is a key in both, among a map's entries and as the one entry of a list's last item, and what is under it takes the rest.
- A constant inside a pattern is written under the name of its type, whoever reads it.
- A list with no run whose last item is a map of a rest alone, `[{...?r}]`, would read as a list with a run, so `{ "...": [] }` follows that item: the rest of the list is the list of nothing.
- A concept application writes a name that holds a pattern as that pattern in its `where`. A positive premise is lowered to an equality before it is written, so on #778 only a negated premise, which keeps its pattern, showed that the application wrote the variable standing for the whole value instead (`it_stores_a_rule_that_negates_a_pattern`).

**Which format a body is stamped with.** The lowest that can say it: format 5 for a rule that holds a pattern, and format 4 (`body::TERMS_FORMAT`) for every other rule (`body::format_for`). #827 moved every body to format 4 because the meaning of every body changed (picks became cardinalities). Format 5 changes nothing about a rule of terms, and keeping its bytes keeps three things true: a rule already stored is found and retracted by encoding the rule, `Branch::upgrade_rules` has nothing to rewrite for it, and a build that reads up to format 4 reads it. For #799, written when this was going to be format 4, the maintainer chose to write one format always; the brief for format 5 asked the builder to establish which from #827, and this is what was established.

**Identity does not move.** A rule's identity is a reference of its canonical spelling, by its parts (above), and never of the bytes of its body. A rule of terms keeps its entity and its body bytes (`it_keeps_the_name_and_the_bytes_of_a_rule_that_holds_no_pattern`, three rules captured at the head of #778), and a rule that holds a pattern keeps the name #778 gave it.

**What is still refused.** A map pattern's key is any scalar value (a symbol and a text key are two keys), and the wire form writes a map pattern as a map, whose keys are text. So `TypeError::UnstorablePattern`, now with a `reason`, still refuses, the same ways as before:

- a map pattern with a key that is not text: a symbol (as a record proper is keyed), a number, a boolean;
- a map pattern with a text key that reads as something else under `match`: `?`, `...`, or the name of a type (one ending in `:`, or `composite:map` and its kin);
- a constant map or list inside a pattern. A composite is written in dag-cbor as its canonical bytes, and a rule body's terms are read through a JSON value, which holds no bytes, so the rule would be stored and never read back. A rule of terms that holds such a constant outside a pattern is written in format 4 and does not read back either; that predates this format and is not changed here.

`Pattern::unwritable` says why a pattern has no written form. Whether a map pattern should be written as `[key, pattern]` entries instead, as #843 writes a composite map for a human reader (`{"map": [[k, v], ...]}`), so that every key is stored, is open: it is a new design, asked of the maintainer.

**Reading.** A body in a format above 5 is refused by its number and the rule is inert and listed in `dialog.rule/invalid` as newer, as for any unknown format: so a build that reads up to format 4 meets a format 5 body (`it_leaves_a_body_in_a_later_format_inert` pins the path with format 6). A body stamped below 5 that holds a pattern was written by no release and is refused when it is decoded (`it_refuses_a_pattern_in_a_body_of_an_earlier_format`). `Branch::upgrade_rules` rewrites a body below format 4 into format 4 and leaves formats 4 and 5 as they are (`it_rewrites_only_bodies_below_format_4_on_upgrade`).

**Pinned by.** In `rust/dialog-repository/src/repository/branch/pattern_rule_tests.rs`, on the volatile and the filesystem provider: `it_commits_a_pattern_rule_on_a_volatile_branch` and `_on_a_filesystem_branch` (asserted on a transaction, staged in a batch, copied from a query layer and asserted as a statement; read back from a branch loaded afresh; the deductive rule derives and the inductive one fires at commit; retracted by value; and the refusal of an unwritable rule, with the head unmoved and nothing written), `it_holds_a_pattern_rule_in_the_session_overlay`, `it_keeps_a_subscription_through_a_stored_pattern_rule_current`, `it_keeps_a_fold_through_a_stored_pattern_rule_current`, and `it_keeps_rows_a_stored_rule_builds_as_records_current` (a conclusion that builds a row as one record, committed and in the overlay, subscribed to). `it_replicates_a_rule_that_holds_a_pattern` (`integration_tests.rs`, cross-target) pulls one to another peer, where it fires.

## Laid over a branch for one query

**Decision:** how a rule that holds a pattern is laid over a branch with `Branch::with` and run, when it cannot be stored. Asked by the notation (#773), which compiles a query of several premises to a rule and runs it that way.

Facts:

- The rule has a name. Its identity comes from `pattern_reference`, by parts, and never from the stored spelling. So the name was not what stopped it.
- An overlay is a batch of facts, `Changes`, the same batch a transaction commits. `QueryLayer::with` asserts a statement into it, and a rule's assertion writes its `dialog.rule/*` index facts and its body as bytes under `dialog.rule/source`.
- A query finds an overlay's rule by those index facts and then decodes its body from the bytes (`QueryEnv::overlay_rules`, `rust/dialog-repository/src/repository/branch/session.rs`). So a rule laid over a branch went through the stored format without ever being stored, and one that format cannot write could not be laid over a branch at all.
- `Statement::assert` is generic over its target and knows nothing of where the batch is going.

| Decision: where an overlay gets the body of a rule it cannot write, for: a rule that holds a pattern run over a branch |
|---|---|---|---|
| criterion \ approach | A: a method of its own on the layer, `with_rule` | B: the target of an assertion may hold a thing as it is (`Update::hold`), and a layer's does | C: a body format that writes a pattern |
| `Branch::with(rule)` works, as it does for a rule of terms | 🔴 no: `with` is generic over a statement and still refuses it | 🟢 yes, for a rule by itself or inside any statement that passes its target on | 🟢 yes |
| what is stored | nothing changes | 🟢 nothing changes: a batch that is committed gives the rule back, and the assertion refuses it as before | 🔴 a format, which this topic was told not to add |
| what the overlay holds | the rule | 🟢 the facts that name the rule, and the rule as it was given under that name | bytes |
| a cache keyed by the overlay's rule facts | has to learn of the rule | 🟢 sees the rule: its index facts are in the batch, under an entity that is its identity | sees it |

Decided: B, by the builder.

- `Update::hold(entity, body)` asks a target to keep a thing as what `entity` is. The default gives it back. `Changes` keeps the default, so a batch that is committed stages nothing for the rule and keeps the refusal (above).
- A rule the stored format cannot write asks its target to hold it, and asserts its `conclusion`, `derives` and `reads` facts with no `source`.
- A `QueryLayer` lays statements into a target that holds a deductive rule beside the layer's changes. The layer's overlay line carries the rules it holds (`Ephemeral::keeping`), and a query that finds a rule by its index facts with no `source` beside them takes the rule from the line.

It touches nothing that is stored. A stored body is still the only way a rule reaches a tree, and a rule that holds a pattern still has none.

The layer's changes keep the refusal beside the index facts they stage, so a copy of them, into a transaction (`Transaction::integrate`, or asserted as a statement) or into an ephemeral line, is refused there as the rule itself would be. A reader that does not hold the body, another query of the same branch or another session, sees no trace of the rule.

Since format 5 this path is taken only by a rule whose pattern no body can write (above): any other rule that holds a pattern is laid over a branch with its body, as a rule of terms is, and a copy of the layer's changes commits it. A subscription reads a branch or a stack, so it reads such a rule once it is committed or in the session overlay (`it_keeps_a_subscription_through_a_stored_pattern_rule_current`).

