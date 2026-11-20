# Claims

| claim | kind | backing | status |
|---|---|---|---|
| a parameter holds a variable or a constant | fact | `Term`, `rust/dialog-query/src/term.rs`; `Parameters`, `parameters.rs` | holds |
| a record is bytes dialog does not look into, named by a hash of those bytes | fact | `Value::Record(Vec<u8>)`; `RECORD.reference_of(bytes)`, `rust/dialog-corpus/src/identity.rs` | holds |
| a revision's record is a bare dag-cbor map | fact | `RevisionRecord::to_bytes`, `rust/dialog-corpus/src/history/revision_record.rs` | holds |
| a version byte in front tells a new record from one of before | hypothesis | confirmed if no dag-cbor a release has written under `Value::Record` starts with that byte; a map starts with `0xa0` to `0xbf` | open: check every writer of `Value::Record` |
| a parameter should hold a pattern, of which a term is the leaf | opinion | Irakli, 2026-11-11 | decides |
| a composite is dag-cbor under a version byte now, another layout later | opinion | Irakli, 2026-11-11 | decides |
| no stored format changes for patterns now | opinion | Irakli, 2026-11-11 | decides |
| a premise can be planned by the variables inside its patterns | hypothesis | confirmed by step 3 of [the plan](plan.md) passing in either order of premises | open |
