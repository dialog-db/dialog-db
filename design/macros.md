# Macros as rules

Phase: **Diagnosing.** The title is still the feature as it was brought. It changes when the problem is stated.

Working material: [journal](macros/journal.md), [claims](macros/claims.md), [glossary](macros/glossary.md). It picks up [research/notation/macros.md](https://github.com/dialog-db/dialog-db/blob/docs/notation/research/notation/macros.md) on #728 and Gozala's tonk journal of [2026-05-16](https://github.com/tonk-labs/tonk/blob/journal/%40gozala/2026-05-16.md) and [2026-05-21](https://github.com/tonk-labs/tonk/blob/journal/%40gozala/2026-05-21.md).

## Description

What is the case today, without saying what the problem is.

- Tonk writes questions, assertions, rules and schema in a YAML notation. `tonk-notation` parses it and an analyzer in Rust gives it meaning.
- `rule!` and `concept!` are not ordinary entries. The parser reads `when:` and `unless:` as premise lists, and the analyzer lifts `assert!:`, `retract!:`, `when:`, `unless:` and `with:` into rule and concept mutations.
- Gozala's journal of 2026-05-16 says the `concept` concept cannot describe itself in the notation, because `with:` is dictionary-shaped, and that rules are the same.
- Tonk's library adds `view`, `event` and `route`. The sketches on #728 call `view`, `event`, `concept`, `command` and `rule` built in.
- Dialog stores a rule as its canonical descriptor, under the hash of that descriptor. It stores no concepts and no names: a query carries its concept descriptors inline.
- Tonk keeps names as entities that point at entities. An anchor re-points its name when the body it names changes. Tonk's views name their models by pinned URI.
- #728 designs a notation spelled as Lisp to replace the YAML one. Nothing is implemented. Its implementation note plans a parser and a compiler in a new Rust crate. Its macros note proposes that every top-level form is issued as `(expand form)` and that a macro is a rule.

What was brought on 2026-11-10, as Irakli said it:

- You operate dialog with datalog, so there are deductive and inductive rules.
- Perhaps macros are expressed as rules.
- Perhaps all primitive forms are expressed through macro rules.
- Perhaps those primitives then model the problem domain; concepts and rules are already stored in the database.
- Perhaps, like Unison, everything is hash addressed and a name is a fact that associates a name with a hash.
- Perhaps users write with names, and the names resolve to hashes or to inlined forms, so a change of name does not affect what was written. Tonk does something like this for views.
- Perhaps the compiler itself is a rule. If not, perhaps a built-in formula takes syntax and compiles it.
- It should be like McCarthy's evaluator: minimal, and defined in itself.
- Nothing in dialog may depend on conduct or on a wasm runtime.

Every line of that list is a remedy. None says what is in the way.

## Problem

Not stated yet. The [journal](macros/journal.md) has four candidates and the question that picks among them.

## Approach

Not yet. The approaches brought so far are parked in the journal for Direction.
