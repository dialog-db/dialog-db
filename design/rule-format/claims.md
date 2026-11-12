# A stored rule says less than a rule can: claims

| claim | kind | backing | status |
|---|---|---|---|
| format 4 is the natural next step, and it depends on patterns, records, rule heads and macros | opinion | Irakli, 2026-11-11 | - |
| a rule's entity does not hold the format's number | fact | `DeductiveRule::try_this`, `rule/reference.rs`; the number is on the envelope (`rule/body.rs`) | holds |
| a rule of terms has the same entity and the same body before and after a pattern can be held in memory | fact | `it_keeps_the_name_and_the_bytes_of_a_rule_format_3_wrote` passes on main and unchanged on `feat/patterns` (eb07e88), each built from its own sources alone. Two rules: a premise with a constant, a join. Not covered: a negation, a formula, a reduce, a rule that writes | holds |
| a format that only adds leaves every rule it does not touch where it is | hypothesis | the row above is its first half. Confirmed when a writer that can stamp 4 still passes that test; refuted if an addition respells a rule of terms, as several heads might | open |
| the format's number is the last byte of a body, so stamping a rule of terms with a later format moves one byte | fact | the pinned bodies end `66 666f726d6174 03`; dag-cbor orders keys by length, `rule` before `format` | holds |
| a release skips a rule whose body it cannot read, and reports nothing, on three of four load paths | fact | `session.rs` (`_ => continue`), `induce.rs` (`.ok()`, `else { return Ok(None) }`); `session.rs` `hydrate(bytes)?` returns the error. Read, not run | holds |
| a written rule holds a pattern in two places only: a side of an equality, and a parameter of a negated concept premise | fact | `pattern::lower`, [patterns journal](../patterns/journal.md) 2026-11-11 | holds |
| a rule that holds a pattern is named by its parts already, so a format that writes it moves no name | fact | `pattern_reference`, the same entry | holds |
| two releases that stamp one rule differently leave one entity with two bodies, and which a reader takes is the first it meets | hypothesis | `source_bytes` takes the first bytes value it finds. Confirmed by asserting one rule with two bodies and reading the facts under its entity; refuted if the second write replaces the first | open |
| a release reads past an envelope key it does not know, so a body that names what it uses beside `format: 3` is read as a plain format 3 body; only a later number makes a release refuse | fact | `it_reads_past_an_envelope_key_it_does_not_know` (`rule/body.rs`), on main. Was a hypothesis, confirmed 2026-11-12 | holds |
| a rule that runs and cannot be kept is the problem | hypothesis | the agent's, 2026-11-12; the maintainer has not cut it | open |
