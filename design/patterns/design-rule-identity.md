# Design decision: the identity of a rule that holds a pattern

**Problem it serves:** the direction says no stored format and no wire form changes for patterns now, and that what installing a rule that holds one does is to be said. It also asks for a rule that builds a record to run.

**Decision:** whether a rule that holds a pattern has an identity, and what gives it one.

## Facts the matrix rests on

- A rule runs only when it is registered, and `RuleRegistry::register` refuses a rule with no identity (`EvaluationError::RuleWithoutIdentity`, `rust/dialog-query/src/session/rule_registry.rs`). The program analysis sets rules aside by identity.
- A rule's identity is a merkle reference of its canonical spelling. A concept premise is referenced by its concept and its bindings, and every other premise by its wire form read as a merkle structure (`rust/dialog-query/src/rule/reference.rs`).
- The canonical spelling orders premises and labels variables by each premise's dag-cbor (`encode_premise`, `rust/dialog-query/src/rule/canonical.rs`). Those bytes are compared and never stored.
- A rule's stored body is the dag-cbor of the same serde form, in format 3 (`rust/dialog-query/src/rule/body.rs`).
- `Statement::assert` and `Update` return nothing. Asserting a rule with no stored body panics today, in `DeductiveRule::this` (`rust/dialog-query/src/rule/statement.rs`).

| Decision: what gives a rule that holds a pattern its identity |
|---|---|---|---|
| criterion \ approach | A: nothing. It has no wire form, so no identity | B: a serde form for a pattern, written for identity and refused for storing | C: a reference for a pattern, by its parts, and no serde form |
| the rule runs | 🔴 it cannot be registered, so step 2's rule cannot be tested | 🟢 | 🟢 |
| no wire form is invented | 🟢 | 🔴 a serde form is a wire form to every serializer: JSON would write it wherever a rule or a query is printed, and the stored body would hold it unless each path refused | 🟢 `Serialize` for a pattern above a leaf is an error on every path |
| one rule under every spelling | n/a | holds | 🟢 holds: a pattern's reference does not depend on the order its entries were given in, and the labeling sees the variables inside it at their places (`Parameters::addressed`) |
| a layout of a pattern's bytes | n/a | 🟡 the identity would hash it | 🟢 none: a pattern is named by its parts, as the value it builds is |

## Decided

C, by the builder.

- A pattern has a reference (`pattern_reference`): a leaf as the term it is, and a map, a list or a tuple as a map from `map:`, `list:` or `tuple:` to its parts' references, with its `rest` or `run` beside them. An equality with a pattern on a side is referenced as its wire form would be read, each side by that reference, so an equality between two terms has the reference it had.
- Where the canonical spelling orders premises by their dag-cbor, a premise that holds a pattern is ordered by its reference instead, under a tag of its own.
- A record constant in a rule is named as `Value::reference` names it, so a composite constant is named by its parts there too.
- A rule that holds a pattern has an identity (`try_this`), runs registered, and has no stored body (`try_encode` is `None`).

## What installing one does

It is refused, with the reason. `DeductiveRule::storable` and `InductiveRule::storable` return `TypeError::UnstorablePattern`, which names the pattern and says that format 3 of a rule body has no way to write one. Asserting such a rule stages nothing and panics with that reason, because an assertion has no error to return. That is how a rule whose body reads an attribute through a raw scan is refused today, with a reason that now says why.

What it should do is the maintainer's, and is on the [journal](journal.md)'s agenda. A panic is the only refusal the assertion's signature allows. The other places it could be refused are a transaction's commit, which returns a `CommitError`, and a format that writes a pattern, which is the question this topic was told to bring back.
