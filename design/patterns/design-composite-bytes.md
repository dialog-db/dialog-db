# Design decision: the dag-cbor of a composite

**Problem it serves:** dialog has no value made of values (the topic's [problem](../patterns.md)). The direction says dag-cbor under a version byte now and another layout later. This is what version 1 writes after the byte.

**Decision:** how a part, a nested composite and a map's entries are written, so that one value has one byte string.

## Facts the matrix rests on

- `Value`'s own serde form in a binary format is `{"text:": "foo"}`. Through `serde_ipld_dagcbor` it refuses a natural above `u64::MAX`, an integer outside 64 bits and a float that is not finite, and it writes bytes as an array of integers (`Bare::Bytes(&Vec<u8>)`).
- The history index already writes a claim's value another way: the byte of its type and the bytes `Value::to_bytes` gives (`value_codec` in `rust/dialog-corpus/src/history/claim.rs`, now `model::type_and_bytes`). `Value::try_from((type, bytes))` reads it back, for every value.
- A merkle map's content is its entries ordered by key reference (`Type::map`, `rust/merkle-reference/src/lib.rs`).
- Two numbers of one value have one reference and two byte strings: `Value::UnsignedInt(3)` and `Value::Float(3.0)` (`it_identifies_a_number_by_its_value_alone`).
- A tree under the number band, the default for a new one (`Manifest::default`, `rust/dialog-search-tree/src/manifest.rs`), does not record which type wrote a number. One reads back under the narrowest type that holds it.

```
 01                                  version
 a1 64 "map:"                        the shape: "map:", "list:" or "tuple:"
    82                               entries, by their keys as written
       82 82 03 41 "a"               key:   [type, bytes]
          82 04 50 …16 bytes…        value: [type, bytes]
       82 82 03 41 "b"
          82 07 58 2a 01 a1 …        a part that is a composite: a record, with its own version byte
```

| Decision: how a part is written, for: one byte string a value, every value a part |
|---|---|---|---|
| criterion \ approach | A: a part in `Value`'s own serde form, `{"text:": "foo"}` | B: a part as a claim's value is written, `[type, bytes]` | C: A, with bytes for what dag-cbor's kinds do not hold |
| every value can be a part | 🔴 a natural above 2^64-1 and an infinite float are refused, so a composite could not be built from a bound number | 🟢 `to_bytes` gives bytes for every value | holds, by a second spelling for wide numbers |
| a reader and a writer exist | the writer does; the reader takes a bare payload too, so more than one byte string reads as one value | 🟢 both, used by the history index | 🟡 a new pair |
| size | a name per part, and bytes as an array | 🟡 sixteen bytes a natural, sixty-four a symbol | a name per part |
| readable in a dag-cbor tool | 🟢 types by name, payloads in their own kinds | 🟡 types by number, payloads as bytes | types by name |

| Decision: how a nested composite is written |
|---|---|---|
| criterion \ approach | D: as a record, a part like any other, under its own version byte | E: inline, as the dag-cbor of its parts |
| a reader looks one level in | 🟢 a level is decoded when a pattern or a caller asks for it | the whole tree is decoded to read the top |
| a record of before as a part | 🟢 the same case | needs a marker to tell it from an inline composite |
| a later layout inside an earlier one | 🟢 each record names its own version | one version for the whole tree |
| size | 🟡 a version byte and a length a level | none |

| Decision: which type a number is kept under |
|---|---|---|
| criterion \ approach | I: the type it was given in, as `Value` holds it | J: the narrowest type that holds it, as the number band reads one back |
| one byte string a value | 🔴 the natural 3 and the float 3.0 are one number with one name and two byte strings, so two composites of one name differ in bytes | 🟢 one number is one part |
| a rule reads the same either way round | 🔴 a record built from a bound number is looked up by its bytes, and misses a stored one that holds the number under another type, where taking the stored one apart would have matched it | 🟢 built and stored hold the same bytes |
| what a reader gets back | the type the writer gave | 🟡 the float 3.0 comes back as the natural 3, as a scalar read from the number band does (`decode_number`, `rust/dialog-corpus/src/model/ordvalue.rs`) |

| Decision: the order of a map's entries, and when two keys are one |
|---|---|---|---|
| criterion \ approach | F: by key reference, one key a reference | G: by the key as written, type byte then bytes, with I | H: G, with J |
| no key twice, as the name sees keys | 🟢 the order and the test are the merkle map's own | 🔴 the natural 3 and the float 3.0 are two keys with one reference | 🟢 two kept keys have one reference exactly when they are written alike |
| cost to write and to read | 🟡 a keyed hash a key, on every read | a comparison of bytes | a comparison of bytes |

## Decided

B, D, J and H. A composite under version 1 is `{"map:": [[part, part], …]}`, `{"list:": [part, …]}` or `{"tuple:": [part, …]}`, a part is `[type, bytes]`, a nested composite is a record, a number is kept under the narrowest type that holds it, and a map's entries are in the order of their keys as written and hold no key twice.

- The writer is by hand (`write` in `rust/dialog-corpus/src/model/composite.rs`), so writing cannot fail. `it_writes_the_dag_cbor_serde_writes` holds it to what `serde_ipld_dagcbor` writes for the same shape.
- The reader is `serde_ipld_dagcbor`. It then writes what it read and refuses bytes that differ (`NotCanonical`), so one value has one byte string by construction, not by the reader being strict.
- J is a choice the direction did not make. It is taken because step 3 asks that a rule pass in either order of premises, and under I it would not where number types differ. The cost is the 🟡 cell: the type a number was given in is not kept. It is on the [journal](journal.md)'s agenda for the maintainer.
- A tuple is named under its own type, `tuple:` (`identity::TUPLE`), with a list's content. A tuple and a list of the same items are two values with two byte strings, so they cannot share a name. The direction named `Type::MAP` and `Type::LIST` only.
- `Composite::reference` follows nesting 128 deep, and names a part below that by its bytes, so naming a hostile record cannot exhaust the stack.

The sizes in B are what "another layout later" is for.
