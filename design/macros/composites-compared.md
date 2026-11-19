# Two composites, compared

Read on 2026-11-20 by a thread of its own: the session "{} record values in dialog" from 2026-11-11 16:44 on (earlier pages not read; its journal and design on `worker/laughing-maxwell-lzfhyy` stood in), draft pull requests #812 and #748, and the composite on `wip/engine-fold` (`rust/dialog-corpus/src/model/composite.rs`, from `09378ab`, `87e7005`, `033cac1`, the patterns line, #778).

| aspect | the records thread, #812 | the patterns line, under the notation | does the notation lean on the difference |
|---|---|---|---|
| what it is | `Value::Composite`, type byte 22: a sorted set of (relation, value) pairs | `Composite::{Map, List}` inside `Value::Record`, byte 7, a version byte `0x01`, then dag-cbor | |
| a key | a `Relation`: `domain/name`, 64 bytes at most | any text | 🔴 yes: a syntax node is keyed `type`, a set's members have empty labels (`syntax.rs` 620-643, `form.rs` 886) |
| a list, a tuple | none | a list; no tuple | 🔴 yes: tuples and applications of the syntax tree are lists that keep order and repeats (`compile.rs` 407, 728); `distinct` answers a list |
| a set | a repeated attribute | a sorted list without repeats | partly: it maps, and `distinct` would change type |
| identity | `COMPOSITE.map` from each name to its value's reference | `Type::MAP`, `Type::LIST`, bytes | no |
| typed | each field by its attribute | a leaf keeps its term's descriptor; a text key has no type | partly |
| as a pattern | proposed, `{a: {b: ?x}}` as a destructuring premise; not built | built: map and list patterns with a rest and a run (`pattern.rs` 457-495) | 🔴 yes |
| stored data | nothing rewritten | old records told by their first byte | no |

Said by the maintainer in the records thread: "Records should be referenced canonically by merkle-references"; "all records are just views over the same memory buffer"; "new Values:Composite variant". #778's body says a `Value::Composite` variant "was considered and not taken (2026-11-11)", which the thread's decision of 2026-11-12 contradicts.

Said by the maintainer in the notation's thread, 2026-11-15 (`journal.md` 1562): a key in a pattern is text, and the data model is IPLD's.

Both create `rust/dialog-corpus/src/model/composite.rs` and both edit `identity.rs`.

**What each order costs.**

- #812 first: the notation cannot keep its syntax trees, `distinct`'s lists or its list patterns in that shape without lists and text keys being added or encoded. That is a decision and then about a day; without lists, days.
- The patterns line first: every stored composite later moves from byte 7 to byte 22, with its references and every rule identity over one. Bearable only while nothing durable holds one; #778 still refuses to store a rule that holds a pattern.

**A hypothesis, the agent's:** they are two things and not two versions of one. #812's is a struct of attributes, what a concept's instance is as a value. The patterns line's is plain data as IPLD has it, what a syntax tree and a collected list are. They already have different type bytes. Refuted if the maintainer means every map to be keyed by relations, in which case the syntax tree needs relation keys and an ordered collection has to be found.
