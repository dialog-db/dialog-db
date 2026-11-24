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

It is refused, with the reason, and nothing is written. `DeductiveRule::storable` and `InductiveRule::storable` return `TypeError::UnstorablePattern`, which names the pattern and the format of a rule body this release writes, which has no way to write one.

`Statement::assert` cannot return an error, so the refusal is a state of the batch it writes into:

- `Update::refuse(Refusal)` records it. A `Refusal` carries the rule's entity and the reason, typed (`Refusal::reason::<TypeError>()`). `Changes` keeps the first one it is handed.
- Asserting such a rule refuses first and then asks the target to hold the rule (`Update::hold`). A batch that is committed cannot hold it, so nothing is staged for the rule: no `source`, and no `conclusion`, `derives` or `reads` index fact either, so a durable batch never names a rule with no body.
- A transaction carries the refusal in its staged writes, and its commit returns `CommitError::Refused` before it settles, induces or writes anything: the head does not move, and nothing else the transaction held is written. A failed commit writes nothing whatever failed it, so the other statements beside the rule are not written either.
- An ephemeral line (`Ephemeral::apply`, the branch's session overlay) is exported and read back as facts, so it refuses such a batch with `DialogCorpusError::Refused` and lands nothing.
- A `Changes` that holds a refusal does not serialize, as `Parameters` that hold a pattern do not.
- An inductive rule fires at commit, from its stored body, so one that holds a pattern is refused wherever it is asserted, a query layer included.
- Retracting a deductive rule that holds a pattern retracts its index facts and no body, since it never had one; retracting an inductive one retracts nothing.

`it_refuses_to_commit_a_pattern_rule_on_a_volatile_branch` and `it_refuses_to_commit_a_pattern_rule_on_a_filesystem_branch` (`rust/dialog-repository/src/repository/branch/pattern_rule_tests.rs`) pin the refusal for both kinds of rule, asserted on a transaction, staged in a batch and copied from a query layer, with the head unmoved and no fact of the rule or of the statement beside it written. `it_refuses_a_pattern_rule_into_the_session_overlay` pins the overlay's.

**The format that stores one.** No rule format up to format 4, which this release writes, can hold a pattern. The format that stores a pattern in a rule body will be format 5. It is not built here; until it is, a rule that holds a pattern lives only in the query layer it was laid over.

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

The layer's changes keep the refusal beside the index facts they stage, so a copy of them, into a transaction (`Transaction::integrate`, or asserted as a statement) or into an ephemeral line, is refused there as the rule itself would be. A reader that does not hold the body, another query of the same branch or another session, sees no trace of the rule. A subscription reads a branch or a stack, neither of which can hold the rule, so it cannot be subscribed through one; a query that holds a pattern can be (`it_keeps_a_subscription_to_a_pattern_current`, `it_keeps_a_fold_over_a_pattern_current`).

