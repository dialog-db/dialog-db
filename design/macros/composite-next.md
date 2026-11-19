# What the composite must grow to hold a syntax tree

**Decision:** what `Value::Composite` (#812) gains so that the notation and the patterns stand on it, by the maintainer's goals for it: random access, stored compactly, read without decoding all of it.

Read on 2026-11-20 by a thread of its own from #812 (`worker/laughing-maxwell-lzfhyy-composite`), its research (#748) and the notation on `wip/engine-fold`. Nothing was run.

## #812 as built

```
 composite := count · name 00 · name 00 …        names sorted, each once, nested ones included
              · (index · type · value)* · 00      fields sorted by index, then by their bytes
 nested    := type 22 · its fields only, using the outer names
```

| goal | as built | where |
|---|---|---|
| random access | 🔴 a scan: `get` parses every name, searches them in order, then walks the fields to the index. No table of offsets | `composite.rs` 115-142, 294-303 |
| read without decoding all | 🔴 `Composite(Arc<[u8]>)`, no borrowed view; a nested composite read out is encoded again into a new buffer. The research lists "a view (a buffer and a span)" as still to do | `composite.rs` 58, 174-178; `design.md` 49 |
| compact | names once for the whole buffer, nested ones included | |
| canonical | bytes that are not what encoding gives are refused | `composite.rs` 90-96 |

Not built: destructuring, the manifest's flag, bars. Decided there by the maintainer: "Fields are named by attributes … not a free name" (`journal.md` 120); "a record is a set of pairs, sorted, without duplicates" (172). Nothing about a list, order or text keys.

## What a syntax tree needs

- Keys known when the code is written: `type`, `operator`, `operand`, `items`, `terms`, `name`, `value`, `entries`, `subject`, `parameters`. Each can be `syntax/…`.
- Keys not known then: a call's labels (`state`, `of`), which fit a relation with no domain, `/state`; a row's generated `c0`, `c1`; and keys from data, which need not fit a relation at all.
- Order and repeats: a call's unlabelled parts, a tuple, what `distinct` and a fold answer.
- A map of the notation is already kept as a list of `[key, value]` pairs sorted by the key's bytes.

## An ordered collection

| criterion \ approach | L1. A second form under type byte 22 | L2. A composite whose names are indexes | L3. A list of its own, type byte 23 |
|---|---|---|---|
| an item by its index | a table of offsets of fixed width | 🔴 a lookup by name, and `/10` sorts before `/2` | a table of offsets |
| told from a map | by a form byte | 🔴 cannot be | by its type |
| identity | `list:` under the composite's type | 🔴 a map's | `list:` |
| typing | one type for both | one | two, a struct and a list |
| what changes | every reader branches on the form | nothing | a new arm wherever #812 added one, about 19 files |

## Keys

| criterion \ approach | K1. A key is a relation | K2. A relation or text | K3. Text |
|---|---|---|---|
| typed by attribute | every field | fields keyed by relations | 🔴 none; against the decision above |
| a key from data, or over 64 bytes | 🟡 refused | held as text | held |
| compact | a domain's prefix once | a tag byte for each name | least |
| patterns | keys read as relations | two kinds of key | as now |

**A column the contrast suggests, the agent's.** K1 with a list is enough for everything the notation holds, because a map from data is not a struct: it is a set of entries, which the notation already keeps as a sorted list of pairs. With offsets a sorted list is searched in the logarithm of its length.

```
 a struct        keys are attributes, each field typed       a concept's instance, a syntax node, a row
 a list          ordered, an item by its index               a tuple, a call's parts, what distinct answers
 a dictionary    a list of [key, value] sorted by key        a map built from data, {... ?entries}
```

Whichever is taken, the pairs form needs a table of offsets too, or the first goal is not met.

## Cost

About 6 to 8 days: lists and offsets in the composite 2 to 3, the notation 2, patterns and their tests 2, a day more for K2. `data.rs` does not isolate the encoding: `syntax.rs` and `compile.rs` match the old value directly. Patterns assume text keys in dag-cbor's order and a `get` that gives one value where the composite gives several. A definition's entity is made of its signature's reference, so it moves with the encoding; nothing is known to be stored yet.

## 2026-11-20, the maintainer's positions, and the order to decide in

Said by him:

- A map that holds arbitrary keys makes sense. A record is a subtype of it whose keys are all attributes, as a natural is a sub-band of the numbers.
- A composite is identified by its merkle reference, a canonical identity even where the bytes on the wire or on disk differ.
- Lists are not clear to him. Dialog orders relations by fractional indexes, which suits a distributed system, and a list is problematic there; whether that applies to a composite or is overkill he does not know. What are called lists may be tuples, the elements being of several kinds.
- A HAMT, or dialog's own tree as dialog-chronicle uses a form of it without hashes, might be what a collection should be; or it is overengineering.
- It should be measured.

The agent's reading of what depends on what:

```
 1 a value, or a place?         a value is whole and never edited; a place is edited and merged
     │ a value
 2 its identity                 the merkle reference, apart from its bytes (his)
     │                          so an encoding can be changed later without moving any identity
 3 its kinds                    a map of arbitrary keys; a record its sub-band (his); a sequence ?
     │
 4 its bytes                    the least that the sizes measured ask for
     │
 5 a large or edited collection not a composite: facts, ordered by fractional index, as now
```

| claim | kind | backing |
|---|---|---|
| fractional indexes, a HAMT and a tree each answer a collection that is edited in place, merged, or too large to hold whole; a composite that is a value has none of those | hypothesis, the agent's | refuted if a composite is ever changed other than by replacing it whole, or if composites in use are large |
| with identity apart from bytes, the choice of bytes is reversible, but a composite used as a key of the tree has to be ordered by something canonical, its reference or canonical bytes | hypothesis, the agent's | #812 keys a composite as its escaped bytes and refuses bytes that are not canonical (`varkey.rs`, `composite.rs` 90-96); to be read |
| a sequence can be a sub-band of the map as a record is: the map whose keys are the whole numbers from 0 with none missing | hypothesis, the agent's, offered as a column | against it: a list and a map are told apart by every data model read (IPLD, JSON, EDN), and the merkle reference has `list:` |
| a syntax tree's sequence is a list of one kind, syntax nodes, though the nodes are of several shapes | fact | `syntax.rs` `Parameters.items`, `Tuple` |

To measure before 4: how many fields, how many bytes and how deep the composites of the sketches' syntax trees are; whether the analyzer and the patterns reach a field by its key or walk the whole; and what a scan costs against a table of offsets at those sizes.

## 2026-11-20, decided: lists are in, and the composite is built before anything stands on dag-cbor

**The maintainer:** lists are in, and an agent pursues the composite so that it is what is used, in place of going with CBOR first and replacing it.

What the agent takes as the brief, each its own to be overruled:

| | chosen | why |
|---|---|---|
| a composite is a value | replaced whole, never edited in place | he took lists after the argument that fractional indexes, a HAMT and a tree answer a place and not a value; not said in so many words |
| kinds | a map of arbitrary keys; a record its sub-band, every key an attribute; a list, a kind of its own | the map and the record are his. The list as a sub-band of the map was offered and not taken up, and a list apart is what every data model read does and what the merkle reference already has |
| a key of a map | any value, text the usual one; canonical order by the key's bytes | "arbitrary keys" |
| identity | the merkle reference: a map of key to reference, a list of references | his |
| bytes | canonical, as #812 has them, so that a composite can be a key of the tree; offsets and views by what the sizes measured ask for | identity apart from bytes makes this reversible |
| where | a new branch on #812's head, for a pull request against main that takes #812's place | #812 belongs to another session, idle |

Then, in order: the patterns (#778) moved onto it, and the notation.

## 2026-11-20, measured: what the syntax trees are

Counted over the 133 forms of the notation's sketches, examples and prelude, by a reader written to `form.rs` and the node shapes of `syntax.rs`; exact for shapes, the bytes computed.

| | least | median | 95 in 100 | most |
|---|---|---|---|---|
| composites in a form | 11 | 28 | 53 | 213 |
| fields in a map, of 3554 | 0 | 2 | 3 | 9 |
| items in a list, of 713 | 0 | 1 | 4 | 10 |
| depth | 7 | 7 | 16 | 19 |
| bytes of a form in dag-cbor | 339 | 893 | 1833 | 7026 |

One map and two lists have more than 8 entries. So a scan is the usual way to a field and a table of offsets the rare one; and a tree is many small composites nested deep, which is what a view over one buffer is for: the earlier thread measured taking a nested record out at 6 to 7 times a copy.

Found in the earlier thread's branches: `feat/composite-types` and `feat/open-record` are of the type system only and unmerged, `decision/record-value` is superseded, and the rest are unrelated. Nothing but #812 and its research is built on. Its journal's "fields are named by attributes, not a free name" (2026-11-06) is superseded by the map of arbitrary keys with the record its sub-band (2026-11-20). Its rule that a repeated key's values are a set is kept, and the notation, whose maps refuse a key twice, has to refuse that itself.
