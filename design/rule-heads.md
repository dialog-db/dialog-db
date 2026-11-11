# A rule concludes one concept

**Phase: Describing.** Nobody has picked this up. What follows is the situation, not yet a problem statement.

## Description

A rule in dialog has one head.

- A deductive rule is `{ "deduce": <concept>, "when": […], "unless": […] }` (`rust/dialog-query/src/rule/deductive/descriptor.rs`).
- An inductive rule has exactly one of `"assert!"` and `"retract!"`, and carries which as a `Polarity` (`rust/dialog-query/src/rule/inductive/descriptor.rs`, `inductive.rs`).
- A head's fields are the rule's variables: a field named `count` is the body variable `count`, and the head's entity is the variable `this`.
- What is stored follows: one `dialog.rule/conclusion` a rule, the `dialog.rule/derives` index from the attributes of that one concept, and a body in format 3, the canonical spelling of that one descriptor (`rust/dialog-repository/src/rules.rs`, `rust/dialog-query/src/rule/body.rs`, `rule/canonical.rs`).

Irakli, 2026-11-11: the single head is there because tonk's notation had that shape, and for no other reason. He has wanted rules of several heads for a while, asserts and retracts mixed in one rule, and says it belongs in the next rule body format and can be done by an agent working on its own.

Where it shows today:

- The notation being designed in [macros](macros.md) writes `(rule (assert! A) (retract! B) (when …))`. Its proof of concept (#773) turns such a rule into one dialog rule a head. They read the same round, so what they write is the same (`it_asserts_and_retracts_from_one_rule` there), but they are stored, named, quarantined and removed apart, and each repeats the body.
- Moving a to-do from one list to another is one thought and two rules: one asserts where it is, the other retracts where it was.
- A concept that reads several entities is written through as several heads, one an entity. Each is a rule of its own today.

What a second head runs into, as found while reading the code, not as a plan:

- Two heads cannot both have `this`, and two heads with a field of one name would name one variable. The proof of concept gets round it with a rule a head and equalities.
- A rule's identity and its stored body are the canonical spelling of one head and its premises. `Branch::upgrade_rules` rewrites older formats to the current one.
- `dialog.rule/conclusion` holds one concept, and whether a rule's conclusion is transient is asked of that one (`dialog.concept/transient`).
- An inductive rule's polarity is the rule's, not a head's.
- Whether a deductive rule has several heads too, or only an inductive one, has not been asked.

A stored format changes here, so whatever is built is a breaking change with a compatibility note (`.claude/rules/versioning-and-changelog.md`).
