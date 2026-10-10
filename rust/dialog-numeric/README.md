# dialog-numeric

Dialog's numbers: one exact decimal domain, with one canonical encoding whose
bytes sort in numeric order.

```rust
use dialog_numeric::{decode_i128, encode_f64, encode_i128};

let mut three = Vec::new();
encode_i128(3, &mut three);
let mut float = Vec::new();
encode_f64(3.0, &mut float);
assert_eq!(three, float); // 3 and 3.0 are one number

assert_eq!(decode_i128(&three), Ok(Some(3)));
```

## Why

Dialog stores every fact in one ordered key space. Numbers used to have one band
of that space per Rust type: naturals, integers and floats, each a type byte and a
fixed-width payload. The consequences:

- A natural `5`, an integer `5` and a float `5.0` were three values. They did not
  join, retracting one left the others, and a query for one type never found the
  others.
- Where a number landed depended on how it arrived. JSON reads a non-negative
  whole number as a natural and a negative one as an integer, so one attribute
  held both.
- A range over a value that may be any number could not be one scan.
- Identity followed the bands. Two definitions that meant the same number hashed
  apart, and two that read different bands could hash together.

This crate gives dialog one number domain instead. A query or an attribute that
wants integers, naturals or floats reads that domain through a *view* (below),
and a view is a refinement of a scan, not a partition of the store.

## The number domain

A number is zero, positive or negative infinity, NaN, or a finite decimal
`±0.d₁d₂…dₙ × 100^e` with base-100 digits, the first and the last not zero.
Everything that holds a number lands in this one domain:

- **Integers** of any width are exact: `u128::MAX` and `i128::MIN` included.
- **Floats** are their shortest round-trip decimal: the decimal with fewest
  significant digits that reads back as the same float. So the float `0.1` is the
  decimal `0.1`, a float written by JavaScript and the same text in JSON are one
  number, and float → number → float loses nothing.
- **Equal numbers are one value**, whatever held them: `3`, `3.0` and the float
  `3.0` encode identically, `-0.0` is `0`, and NaN is one value.

What this gives up, deliberately: a float's own binary value. The float `1e23` is
exactly 99999999999999991611392 in binary, and here it is the number 10²³. A float
is taken to be an approximation of the decimal it prints as, which is what the
person or program that wrote it almost always meant. Exact decimals that are not
floats (more than 17 significant digits, say) stay exact.

Arithmetic is not part of this crate yet. Results computed in floats are
approximations of the numbers they print as, and are treated as such.

## The encoding

The encoding is [SQLite4's numeric key encoding][sqlite4-keys], unchanged:

| First byte | Meaning | Then |
|---|---|---|
| `0x06` | NaN | |
| `0x07` | −∞ | |
| `0x08` | negative, exponent ≥ 11 | exponent as a varint, then digits, all inverted |
| `0x09`–`0x13` | negative, exponent 10 … 0 (`0x13 − e`) | digits, inverted |
| `0x14` | negative, exponent < 0 | `−e` as a varint, then digits inverted |
| `0x15` | zero | |
| `0x16` | positive, exponent < 0 | `−e` as a varint, inverted, then digits |
| `0x17`–`0x21` | positive, exponent 0 … 10 (`0x17 + e`) | digits |
| `0x22` | positive, exponent ≥ 11 | exponent as a varint, then digits |
| `0x23` | +∞ | |

Each base-100 digit `X` is the byte `2X + 1`, and the last `2X`, so the digits end
themselves and a key needs no length. The varint is SQLite4's order-preserving
one: one byte up to 240, two up to 2287, three up to 67823.

Properties, each pinned by a test:

- **Canonical:** every number has exactly one encoding, and the decoder refuses
  any other bytes (a leading or trailing zero digit, a digit above 99, an
  exponent in the wrong class). This is what lets the same bytes serve as an
  index key and as a number's identity, and what keeps content-addressed trees
  convergent.
- **Ordered:** comparing encodings byte by byte orders numbers by value, NaN
  first. Checked against exact comparison over integers and floats mixed.
- **Lossless:** integers and floats read back exactly.
- **Compact:** the exponent of most numbers lives in the first byte. A small
  integer takes 2 bytes, a price like `19.99` takes 4, an `i64` about 11.

The shortest decimal of a float is unique, so any correct shortest printer gives
the same digits. The crate uses [`zmij`][zmij] and a test pins it to [`ryu`][ryu]
over 600,000 floats, with a reference parser that shares no code with the crate's.

## The view model

A view is how an attribute or a query reads the number domain: `integer`,
`natural`, `float`, a sized integer like `uint8`, and later fixed-point decimals
like `decimal(2)`. A view applies when reading and when writing, and two
applications may read and write one attribute through different views.

**Reading.** A view sees the numbers in its range and nothing else:

- it coerces where it can only lose precision, by floor: an `integer` view reads
  `34.5` as `34`, and a `float` view reads every number, rounded to the nearest
  float;
- a number outside its range is invisible to it: a `natural` view does not see
  `−3`, and a `uint8` view does not see `300`.

Floor, not truncation, so that every integer's bucket has the same width: for an
`integer` view, `34` is the stored range `[34, 35)` and `−35` is `[−35, −34)`.

**Comparing.** A constant is compared after the view coerces the stored value.
Through a `natural` view, `34` matches a stored `34.2`. With no view known or
inferred, comparison is exact, and `34` does not match `34.2`. Because the
encoding is ordered and coercion is floor, equality through a view is one
contiguous range scan, and a view's range is one too: a `uint8` view is the
scan `[0, 256)`, whose bounds are short prefixes of class bytes.

**Writing.** A write through a view first reads the cell through the same view:

- if the read already yields the value being written, the write does nothing: a
  `natural` view writing `34` over a stored `34.5` leaves `34.5`;
- otherwise the write replaces the claim with exactly the value written: the
  same view writing `35` stores `35`, never a merge like `35.5`.

So the store only ever holds values someone asserted. Every view must satisfy two
laws, the laws of a well-behaved lens:

- writing what you read changes nothing;
- reading what you wrote gives it back, so the view's coercion is idempotent on
  its own values (the floor of `35` is `35`).

The price is that a view cannot express a change it cannot see: through a
`natural` view, "exactly 34" and "unchanged" are the same write. That is
acceptable only where what a view hides is precision, which is why a view hides,
rather than coerces, a value outside its range.

This crate provides the domain and the encoding. Views are dialog's attribute
types and are applied by its query engine and its write path; their arithmetic on
this encoding (floor, range bounds) is planned to live here.

## Size and speed

Measured on 200,000 values each, best of 20 runs, against dialog's earlier
fixed-width bands and a binary one-band encoding of the same design:

| Values | Encode (ns) | Decode (ns) | Bytes: earlier / binary / this |
|---|---|---|---|
| small integers | 16 | 17 | 17 / 4.5 / **2.9** |
| timestamps (ms) | 26 | 24 | 17 / 9.0 / **8.0** |
| random `i64` | 33 | 38 | 17 / 11.9 / **10.9** |
| random `i128` | 50 | 65 | 17 / 21.0 / 21.4 |
| prices (2 decimals) | 50 | 41 | 9 / 10.7 / **3.9** |
| any finite float | 118 | 85 | 9 / 10.9 / 10.7 |

The fixed-width bands cost 2–4 ns either way; their price was the semantics
above. Scans compare encoded bytes and never decode; decoding happens only when a
value is returned. Most of a float's encode time is reading back the decimal text
`zmij` prints, since its digit generator is not public.

In WebAssembly (opt-level `s`, LTO) the crate adds about 8 KB to a module that
already links `serde_json`, which dialog does and which brings `zmij` and std's
float parsing with it.

## Prior art

- **SQLite4's [key encoding][sqlite4-keys] and [number representation][sqlite4-decimal]**:
  this crate's encoding, and the idea of one decimal domain for integers and
  floats that distinguishes exact from approximate rather than integer from
  float. SQLite4 also rejected IEEE 754 decimal as too complex for too little
  precision. Its key-encoding note's example table gives exponents one too large
  for 123450, 1234.5 and 12.345; the crate's tests leave those rows out.
- **TerminusDB** (`tdb-succinct`, [internals][terminus]): one sorted segment per
  datatype and fixed-width encodings much like dialog's earlier bands. It never
  compares across types, which is the problem this crate removes, and its
  decimals keep the decimal string's digits, so `0.5` and `0.50` differ.
- **Shortest round-trip printing**: Steele and White's "How to print
  floating-point numbers accurately", Ryū ([`ryu`][ryu]) and [`zmij`][zmij].
- **Bijective varints**: [bijou][bijoux], used by dialog's tree manifest, and
  SQLite4's varint used here, both with exactly one encoding per value and
  bytes that sort as they count.
- **Lenses**: the read/write view laws are those of well-behaved lenses (Foster
  et al., "Combinators for bidirectional tree transformations").

## Migrating stored numbers

Moving dialog's stored numbers from the per-type bands into this encoding is a
one-time data migration, kept out of the read path so that only replicas with
old data pay for it. See `notes/identity-storage-notation.md` in the dialog
repository.

[sqlite4-keys]: https://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
[sqlite4-decimal]: https://sqlite.org/src4/doc/trunk/www/decimal.wiki
[terminus]: https://terminusdb.org/blog/2022-11-21-terminusdb-internals-3/
[ryu]: https://github.com/dtolnay/ryu
[zmij]: https://github.com/dtolnay/zmij
[bijoux]: https://docs.rs/bijoux
