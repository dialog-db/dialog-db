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

**A hypothesis, the agent's, refuted by the maintainer on 2026-11-20:** that they are two kinds of value and both should exist.

**The maintainer, 2026-11-20:** if `Value::Composite` cannot hold a syntax tree it fails its goals, which were a built-in composite value that offers random access, is stored more compactly, and can be read without decoding all of it.

So there is one kind, #812's, and it has to grow to hold what the notation holds: an ordered collection, and keys a syntax tree can use. The value on the patterns line, dag-cbor after a version byte, meets none of the three goals: it is decoded whole, in order. What the notation and the patterns built over it stay; the value under them is replaced.

```
 now      notation ──▶ patterns (#778) ──▶ Composite::{Map, List} in Value::Record, dag-cbor
 wanted   notation ──▶ patterns        ──▶ Value::Composite (#812), with an ordered collection
```

Open: what an ordered collection is in #812's encoding so that an item is reached by its index without reading the others; and whether a key must be a relation, with the syntax tree's keys named in a domain (`syntax/type`), or may be any text.
