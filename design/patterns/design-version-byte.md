# Design decision: the byte that says a record is versioned

**Problem it serves:** a rule cannot say a value by its shape (the topic's [problem](../patterns.md)). A composite is kept as a `Value::Record`, and so is a revision's record, which is a bare dag-cbor map. A reader has to tell the two apart from the bytes alone, and has to tell a later version from both.

**Decision:** which byte goes in front of a composite, and what a reader makes of a first byte.

## Facts the matrix rests on

- The one record dialog writes is a revision's: `serde_ipld_dagcbor::to_vec` of a struct of seven fields (`RevisionRecord::to_bytes`), so its first byte is `0xa7`. Pinned by `it_reads_a_record_of_before_as_before`.
- Every other writer of `Value::Record` carries bytes a caller gave: `Value::conform` (bytes under a `record:` slot), the tagged form `{"record:": …}`, `record:<base58>` in `FromStr`, a `record` cell of a CSV import (`rust/dialog-csv/src/row.rs`), and `RecordBytes` (`rust/dialog-query/src/types.rs`). In this repository only the revision formulas use `RecordBytes`. What an embedder wrote through them is not visible from here.
- A dag-cbor document is one item with nothing after it. A CBOR unsigned integer below 24 is one byte, `0x00` to `0x17`, and is a whole item.
- Tests in the repository write `Value::Record(vec![1, 2, 3])` and `vec![9, 8, 7]` as opaque records.

```
 a record of before      a7 65 73 6b …                one dag-cbor item
 a versioned record      01 a1 64 6d 61 70 3a …       a version, then one dag-cbor item
                         ^^
                         a whole item by itself, so the two together are never one
```

| Decision: the version byte, for: telling a composite from a record of before and from a later version |
|---|---|---|---|
| criterion \ approach | A: a small integer, `0x01`, with `0x02` to `0x17` kept for later versions | B: a byte CBOR reserves, `0x1c` | C: a tag dag-cbor refuses, `0xc1` |
| a dag-cbor record of before never starts a versioned one | 🟢 a first byte below 24 is a whole item, and a versioned record has more after it, so no single dag-cbor item is one | holds while CBOR leaves `0x1c` unassigned | holds: dag-cbor allows tag 42 only |
| a reader names the version it refuses | 🟢 the byte is the number | 🟡 the number has to be a second byte | 🟡 the tag is not the version without a table |
| room for a later layout | twenty-two more bytes, then a version of two bytes under the last | as many as a second byte holds | twenty-three tags |
| bytes a caller gave that are not dag-cbor | 🟡 a record starting `0x01` to `0x17` with more after it is read as versioned: refused when looked into, carried as bytes otherwise | 🟡 the same for `0x1c` | 🟡 the same for `0xc1` |
| a zeroed buffer | not versioned: `0x00` is given to no version | not versioned | not versioned |

## Decided

A. Version `1` is the byte `0x01`. A record is versioned when its first byte is `0x01` to `0x17` and at least one byte follows. Anything else is a record of before.

- A versioned record of a version this release does not know is refused by its number when it is looked into (`CompositeError::UnknownVersion`), and is still a `Value::Record` of its bytes everywhere else: stored, compared, keyed and named by them.
- A record under version 1 whose bytes are not the ones a composite is written in is refused when looked into (`Malformed`, `NotCanonical`) and named by its bytes. So a name changes only for bytes that are a composite's.

No column is green on bytes a caller gave. No byte can be: a record was any bytes. What it costs is in [claims](claims.md).
