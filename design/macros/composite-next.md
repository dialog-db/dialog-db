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
