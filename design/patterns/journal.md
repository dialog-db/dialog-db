# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | A parameter holds a variable, constant, map pattern or list pattern. Patterns build and take apart `Value::Composite`; nested atoms use the shared canonical table and Chronicle tokens from #849. A rule that holds a pattern is stored, in rule body format 5, and runs from what is stored. | **Implementation.** Planning, typing and rule identity are built in #778, on the composite API of #843; format 5 in #853; a body reads composite constants back in #855. |
| **Agenda** | Whether a key that holds several values should be matchable by a map pattern ([meaning](design-pattern-meaning.md)). The format that stores a pattern in a rule ([rule identity](design-rule-identity.md)). | Review and merge #778 after #843. |

## 2026-11-11, Directing: a pattern in place of a term

**Asked.** In the [macros](../macros.md) topic: how dialog evaluates a struct with a variable in it, what a record is, and how to stop waiting on the layout of its bytes.

**Learned.**

- A struct with a variable in it is a destructuring pattern. The same pattern, with its variables bound, is the value. Macros, an entity derived from a row, and a command's row all need it.
- The layout of a record's bytes has been the thing everything waited on. A name that is a merkle reference of the parts does not depend on it.
- A revision's record is a bare dag-cbor map (`RevisionRecord::to_bytes`), so a version byte in front of a new record tells the two apart.

**Decided.** By the maintainer, Irakli:

- `Term` becomes the leaf of a `Pattern`, which is also a map or list of patterns.
- Patterns use `Value::Composite`, whose canonical map/list encoding and shared tokens are implemented in #843 and #849.
- The planner and inference are part of it. A stored format is not: only the new notation makes patterns.
- It is a piece of work of its own, for an agent of its own.

## 2026-11-21, Updating: patterns use the canonical composite value

**Learned.** The composite prerequisite is now implemented in #843 and #849. A `Value::Composite` map or list has canonical bytes with a shared table of scalar atoms across all nested composites. Chronicle interns those atoms through the token dictionary it also uses for standalone values, then rebuilds canonical bytes when it realizes the composite. Its identity comes from its parts and does not depend on its layout or token IDs.

**Decided.** The pattern design and implementation use this composite representation directly. They do not introduce a second record encoding, version byte, or per-pattern token table. The revision record remains a separate bare dag-cbor map.

**Implementation note, 2026-11-21.** The pattern matcher now constructs and inspects `Composite` values directly. A pattern does not encode nested composites itself: it delegates canonical ordering, shared scalar atoms and composite identity to the corpus implementation.

## 2026-11-23, Reviewing: a refusal in place of a panic, and the read's one stream

**Learned.**

- Committing a rule that holds a pattern panicked in `Statement::assert`, which on wasm takes the page down. `Statement::assert` returns nothing, so the refusal is now a state of the batch: `Update::refuse` records a `Refusal`, a batch that is committed stages nothing for the rule, and the commit returns `CommitError::Refused` before anything is written. A query layer still holds the rule and runs it.
- A map pattern decided duplicate keys by `Value`'s equality, so `3` and `3.0` were two keys of a pattern and one key of the map it built. The map decides now.
- The read of a concept had a box around it again, and a stream around that for a read that holds a pattern: f86d6983b, porting to the composite API, took 99b412efe's one stream out, and with it the branch of `canonical()` that renames a one-field read's patterns along with its terms. Both are back, and the read is the one stream [design-leaf-cost.md](design-leaf-cost.md) decided. Measured under callgrind, the box had cost little (at most 0.09% on the gate's read and rule scenarios); the numbers are in that note.
- A subscription to a query that holds a pattern with a blank inside missed a change: two rows of one entity compared equal, since the whole is bound under a name of each run's own. A row now compares the variables inside its patterns.

**Decided.** By the builder, on the review's list: the refusal's shape above, and the semantics of the edges, written in [design-pattern-meaning.md](design-pattern-meaning.md).

## 2026-11-24, Building: a composite constant reads back, and the refusal is gone

**Learned.**

- A rule holding a composite constant, as a term or inside a pattern, was written and never read back: a body writes a composite as its canonical bytes, and the reader buffered terms through `serde_json::Value`, which holds no bytes. Only a composite is written as a byte string (a bytes constant is an array of integers under `bytes:`), so reading a byte string as the composite it is reads every such body back and changes nothing for a body that read before (`it_buffers_a_body_without_composites_as_json_does`, `it_keeps_a_bytes_constant_apart_from_a_composite_of_the_same_bytes`).

**Decided.** By the coordinating session, on #855: with nothing left to refuse, the refusal of 2026-11-23 is removed whole (`storable`, `TypeError::UnstorablePattern`, `Update::refuse`, `Update::hold`, `Refusal`, `CommitError::Refused`, `DialogCorpusError::Refused`, the held rule bodies of a query layer), none of it released.
