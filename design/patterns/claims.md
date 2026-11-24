# Claims

| claim | kind | backing | status |
|---|---|---|---|
| a parameter holds a variable or a constant | fact | `Term`, `rust/dialog-query/src/term.rs`; `Parameters`, `parameters.rs` | holds |
| a composite is a canonical map or list in `Value::Composite` | fact | `Composite`, `rust/dialog-corpus/src/model/composite.rs`, #843 | holds |
| a composite and all it nests share one sorted table of scalar atoms | fact | [composite design](../composite-value.md), #843 | holds |
| Chronicle interns composite atoms with standalone-value tokens | fact | #849 | holds |
| a composite's identity is computed from its parts, independently of its encoding | fact | `Composite::reference`, `rust/dialog-corpus/src/model/composite.rs`, #843 | holds |
| a revision's record is a bare dag-cbor map | fact | `RevisionRecord::to_bytes`, `rust/dialog-corpus/src/history/revision_record.rs` | holds |
| a parameter should hold a pattern, of which a term is the leaf | opinion | Irakli, 2026-11-11 | decides |
| no stored format changes for patterns now | opinion | Irakli, 2026-11-11 | decides |
| a premise can be planned by the variables inside its patterns | hypothesis | `it_takes_a_record_apart_in_either_order_of_premises`, `it_builds_a_record_in_either_order_of_premises` | confirmed |
| committing a rule that holds a pattern is refused, and writes nothing | fact | `CommitError::Refused`; `it_refuses_to_commit_a_pattern_rule_on_a_volatile_branch` and `_on_a_filesystem_branch` (#778) | superseded by format 5 |
| a rule that holds a pattern is stored in rule body format 5, and a rule of terms in format 4 as before | fact | `rule::body::format_for`; `it_commits_a_pattern_rule_on_a_volatile_branch` and `_on_a_filesystem_branch`, `it_keeps_the_name_and_the_bytes_of_a_rule_that_holds_no_pattern` | holds |
| committing a rule that holds a composite constant inside a pattern is refused, and writes nothing | fact | `TypeError::UnstorablePattern`; the same two tests | superseded by #855: it is stored and fires |
| a map pattern keyed by any value is stored, as pairs of a key and a pattern | fact | `it_writes_a_map_pattern_one_way_whatever_its_keys`, `it_stores_a_rule_that_builds_a_record_keyed_by_symbols` | holds |
