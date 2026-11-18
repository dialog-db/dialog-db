//! [`FactView::checked`] against [`FactView::to_owned`], the read it
//! stands in for: a row a scan binds in place must be refused, accepted
//! and read exactly as the eager read refuses, accepts and reads it.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::panic::{AssertUnwindSafe, catch_unwind};

use super::*;
use crate::key::varkey::{KeyParts, ValuePayload, build_key, parse_key_layout};
use crate::{ENTITY_KEY_TAG, encode_bytes, encode_value_owned};

/// How a read of a row ends, as a scan tells it apart: a row that reads,
/// a corrupt row it passes over, or an error that fails the query.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Read,
    Corrupt,
    Fails(String),
}

fn outcome<T>(result: &Result<T, DialogCorpusError>) -> Outcome {
    match result {
        Ok(_) => Outcome::Read,
        Err(DialogCorpusError::CorruptEntry(_)) => Outcome::Corrupt,
        Err(other) => Outcome::Fails(format!("{other:?}")),
    }
}

/// Whether two outcomes are the same kind of outcome (two failures need
/// not carry the same message).
fn same_kind(a: &Outcome, b: &Outcome) -> bool {
    matches!(
        (a, b),
        (Outcome::Read, Outcome::Read)
            | (Outcome::Corrupt, Outcome::Corrupt)
            | (Outcome::Fails(_), Outcome::Fails(_))
    )
}

/// A scanned row over `key`, if the key parses at all.
fn scanned_key(key: Vec<u8>, spilled: Option<Vec<u8>>) -> Option<FactView> {
    let layout = parse_key_layout(&key)?;
    Some(FactView::new(
        KeyHandle::Owned(key),
        layout,
        Datum {
            cause: Some(Cause([3; 32])),
            blob: None,
            version: None,
            collapsed: vec![],
            supersedes: vec![],
            retraction: false,
        },
        spilled,
    ))
}

/// The EAV key of `entity`, `attribute` and a value of `value_type`
/// carried as `value`.
fn key(entity: &[u8], attribute: &[u8], value_type: ValueDataType, value: ValuePayload) -> Vec<u8> {
    build_key(&KeyParts {
        tag: ENTITY_KEY_TAG,
        entity: entity.to_vec(),
        attribute: attribute.to_vec(),
        value_type,
        value,
        version: None,
    })
}

/// An inline payload holding `raw` as a terminated byte string, as text,
/// bytes, records, entities and symbols are stored.
fn terminated(raw: &[u8]) -> ValuePayload {
    let mut out = Vec::new();
    encode_bytes(raw, &mut out);
    ValuePayload::Inline(out)
}

/// A spilled payload for the block `block`: the key holds its first
/// bytes and a hash; the block itself is fetched beside the key.
fn spilled_payload(block: &[u8]) -> ValuePayload {
    let mut prefix = Vec::new();
    encode_bytes(&block[..block.len().min(8)], &mut prefix);
    ValuePayload::Spilled {
        prefix,
        hash: vec![7; 32],
    }
}

/// One row of the classification table: a name, the key, and the block
/// a spilled value was fetched with.
struct Case {
    name: &'static str,
    key: Vec<u8>,
    block: Option<Vec<u8>>,
}

fn inline(
    name: &'static str,
    entity: &[u8],
    attribute: &[u8],
    value_type: ValueDataType,
    value: ValuePayload,
) -> Case {
    Case {
        name,
        key: key(entity, attribute, value_type, value),
        block: None,
    }
}

fn spilled(
    name: &'static str,
    value_type: ValueDataType,
    block: Option<&[u8]>,
    prefix_of: &[u8],
) -> Case {
    Case {
        name,
        key: key(
            b"user:alice",
            b"user/name",
            value_type,
            spilled_payload(prefix_of),
        ),
        block: block.map(<[u8]>::to_vec),
    }
}

/// Well-formed and malformed rows: every column, and each value type,
/// broken the ways stored bytes from another writer can be.
fn cases() -> Vec<Case> {
    let good = |value: &Value| ValuePayload::Inline(encode_value_owned(value));
    let long = "x".repeat(400);
    vec![
        inline(
            "well-formed text",
            b"user:alice",
            b"user/name",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "well-formed entity value",
            b"user:alice",
            b"user/friend",
            ValueDataType::Entity,
            terminated(b"user:bob"),
        ),
        inline(
            "well-formed symbol value",
            b"user:alice",
            b"user/kind",
            ValueDataType::Symbol,
            terminated(b"kind/person"),
        ),
        inline(
            "text with a zero byte",
            b"user:alice",
            b"user/name",
            ValueDataType::String,
            good(&Value::String("a\0b".into())),
        ),
        inline(
            "float NaN",
            b"user:alice",
            b"user/score",
            ValueDataType::Float,
            good(&Value::Float(f64::NAN)),
        ),
        inline(
            "float -0.0",
            b"user:alice",
            b"user/score",
            ValueDataType::Float,
            good(&Value::Float(-0.0)),
        ),
        inline(
            "entity column not UTF-8",
            b"user:\xff\xfe",
            b"user/name",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "entity column not a canonical URI",
            b"User:alice",
            b"user/name",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "entity column not a URI",
            b"not a uri",
            b"user/name",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "attribute without a slash",
            b"user:alice",
            b"username",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "attribute not UTF-8",
            b"user:alice",
            b"user/\xff",
            ValueDataType::String,
            good(&Value::String("Alice".into())),
        ),
        inline(
            "inline text not UTF-8",
            b"user:alice",
            b"user/name",
            ValueDataType::String,
            terminated(b"\xff\xfe"),
        ),
        inline(
            "inline boolean byte 2",
            b"user:alice",
            b"user/flag",
            ValueDataType::Boolean,
            ValuePayload::Inline(vec![2]),
        ),
        inline(
            "inline entity value not a canonical URI",
            b"user:alice",
            b"user/friend",
            ValueDataType::Entity,
            terminated(b"User:bob"),
        ),
        inline(
            "inline entity value not a URI",
            b"user:alice",
            b"user/friend",
            ValueDataType::Entity,
            terminated(b"not a uri"),
        ),
        inline(
            "inline entity value with a space",
            b"user:alice",
            b"user/friend",
            ValueDataType::Entity,
            terminated(b"user:a b"),
        ),
        inline(
            "inline entity value not UTF-8",
            b"user:alice",
            b"user/friend",
            ValueDataType::Entity,
            terminated(b"user:\xff"),
        ),
        inline(
            "inline symbol value without a slash",
            b"user:alice",
            b"user/kind",
            ValueDataType::Symbol,
            terminated(b"person"),
        ),
        inline(
            "inline symbol value too long",
            b"user:alice",
            b"user/kind",
            ValueDataType::Symbol,
            terminated(format!("kind/{}", "p".repeat(80)).as_bytes()),
        ),
        spilled(
            "spilled text",
            ValueDataType::String,
            Some(long.as_bytes()),
            long.as_bytes(),
        ),
        spilled(
            "spilled key with no block bytes",
            ValueDataType::String,
            None,
            long.as_bytes(),
        ),
        spilled(
            "spilled text not UTF-8",
            ValueDataType::String,
            Some(b"\xff\xfe\xfd"),
            b"\xff\xfe\xfd",
        ),
        spilled(
            "spilled entity not a canonical URI",
            ValueDataType::Entity,
            Some(b"User:bob"),
            b"User:bob",
        ),
        spilled(
            "spilled entity not a URI",
            ValueDataType::Entity,
            Some(b"not a uri"),
            b"not a uri",
        ),
        spilled(
            "spilled symbol without a slash",
            ValueDataType::Symbol,
            Some(b"person"),
            b"person",
        ),
    ]
}

/// `checked` refuses a row exactly as `to_owned` does, with the same
/// kind of error: a corrupt row (`CorruptEntry`) a scan passes over on
/// both paths, any other error failing the query on both. A row both
/// read materializes to the same fact. Pins the claim in
/// `FactView::checked`'s doc that it validates "as `to_owned` would":
/// a scan binds rows in place unless a seed or a refined kind makes it
/// bind eagerly, so a row the two classify apart is skipped by one scan
/// of a query and fails, or reads, in another.
#[dialog_common::test]
fn checked_classifies_every_row_as_to_owned_does() {
    let mut mismatches = Vec::new();
    for case in cases() {
        let Some(view) = scanned_key(case.key.clone(), case.block.clone()) else {
            mismatches.push(format!("{}: the test's key does not parse", case.name));
            continue;
        };
        let owned = view.to_owned();
        let checked = view.clone().checked();
        let (expected, actual) = (outcome(&owned), outcome(&checked));
        if !same_kind(&expected, &actual) {
            mismatches.push(format!(
                "{}: to_owned gives {expected:?} ({owned:?}), checked gives {actual:?}",
                case.name
            ));
            continue;
        }
        if let (Ok(fact), Ok(row)) = (&owned, &checked) {
            let read = Fact {
                the: row.relation(),
                of: row.entity(),
                is: row.value(),
                cause: row.cause().cloned(),
            };
            if &read != fact {
                mismatches.push(format!(
                    "{}: to_owned reads {fact:?}, checked reads {read:?}",
                    case.name
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "expected checked() to classify every row as to_owned() does; {} differ:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// A small fixed-seed generator, so the mutants are the same on every
/// run and every target.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound.max(1) as u64) as usize
    }
}

/// A well-formed key per value type, inline and spilled, to mutate.
fn seeds() -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut seeds: Vec<(Vec<u8>, Option<Vec<u8>>)> = [
        (ValueDataType::String, Value::String("Alice".into())),
        (ValueDataType::String, Value::String("a\0b".into())),
        (ValueDataType::String, Value::String(String::new())),
        (ValueDataType::Bytes, Value::Bytes(vec![0, 1, 0, 255])),
        (ValueDataType::Record, Value::Record(vec![9, 0, 9])),
        (ValueDataType::Boolean, Value::Boolean(true)),
        (ValueDataType::UnsignedInt, Value::UnsignedInt(40)),
        (ValueDataType::SignedInt, Value::SignedInt(-3)),
        (ValueDataType::Float, Value::Float(-0.0)),
        (ValueDataType::Float, Value::Float(f64::NAN)),
        (ValueDataType::Float, Value::Float(4.5)),
        (
            ValueDataType::Entity,
            Value::Entity(Entity::from_str("did:key:z6Mkabc").expect("entity")),
        ),
        (
            ValueDataType::Entity,
            Value::Entity(Entity::from_str("https://example.com/a").expect("entity")),
        ),
        (
            ValueDataType::Symbol,
            Value::Symbol(Relation::from_str("kind/person").expect("relation")),
        ),
    ]
    .into_iter()
    .map(|(value_type, value)| {
        (
            key(
                b"user:alice",
                b"user/name",
                value_type,
                ValuePayload::Inline(encode_value_owned(&value)),
            ),
            None,
        )
    })
    .collect();
    // The number band: a natural, an integer and a float, as a tree keyed
    // in it writes them.
    for value in [
        Value::UnsignedInt(3),
        Value::SignedInt(-7),
        Value::Float(2.5),
    ] {
        seeds.push((
            key(
                b"user:alice",
                b"user/count",
                ValueDataType::Number,
                ValuePayload::Inline(crate::encode_key_value_owned(&value, crate::NUMBER_BAND)),
            ),
            None,
        ));
    }
    for (value_type, block) in [
        (ValueDataType::String, "y".repeat(300).into_bytes()),
        (ValueDataType::Bytes, vec![0; 300]),
        (ValueDataType::Entity, b"user:carol".to_vec()),
        (ValueDataType::Symbol, b"kind/robot".to_vec()),
        (ValueDataType::UnsignedInt, 5u128.to_le_bytes().to_vec()),
        (ValueDataType::Boolean, vec![1]),
    ] {
        seeds.push((
            key(
                b"user:alice",
                b"user/name",
                value_type,
                spilled_payload(&block),
            ),
            Some(block),
        ));
    }
    seeds
}

/// `bytes` mutated once: a byte flipped, set or dropped, the key cut
/// short, a run of another key spliced in.
fn mutate(rng: &mut Xorshift, bytes: &[u8], donor: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    if out.is_empty() {
        return out;
    }
    match rng.below(6) {
        0 => {
            let at = rng.below(out.len());
            out[at] ^= 1 << rng.below(8);
        }
        1 => {
            let at = rng.below(out.len());
            out[at] = [0x00, 0xFF, b'/', b':', b' ', b'A', 0x80, 0x01][rng.below(8)];
        }
        2 => {
            let at = rng.below(out.len());
            out.remove(at);
        }
        3 => out.truncate(rng.below(out.len())),
        4 => {
            let at = rng.below(out.len());
            let from = rng.below(donor.len());
            let len = rng.below(donor.len() - from + 1);
            out.splice(at..at, donor[from..from + len].iter().copied());
        }
        _ => {
            let at = rng.below(out.len());
            out.insert(at, rng.next() as u8);
        }
    }
    out
}

/// What reading `row` in every way gives: the fact it materializes,
/// and the components read in place.
fn read_all(row: &CheckedRow) -> (Fact, Value, String, String) {
    let fact = Fact {
        the: row.relation(),
        of: row.entity(),
        is: row.value(),
        cause: row.cause().cloned(),
    };
    let viewed = row
        .value_view()
        .to_value()
        .expect("a checked row's value view materializes");
    (
        fact,
        viewed,
        row.of_str().to_owned(),
        row.the_str().to_owned(),
    )
}

/// Every row `checked` accepts reads without panicking, and reads as
/// `to_owned` reads it: the `expect`s in `CheckedRow::{entity, relation,
/// value}` must be unreachable for stored bytes, which can come from
/// another peer. Mutates well-formed keys of every value type, inline,
/// in the number band and spilled, and their spilled blocks, with a
/// fixed seed.
///
/// On wasm32 a panic aborts rather than unwinds, so `catch_unwind` does
/// not catch it there: the test still fails on a panic, without the
/// message naming the mutant.
#[dialog_common::test]
fn every_row_checked_accepts_reads_as_to_owned_reads_it() {
    let seeds = seeds();
    let mut rng = Xorshift(0x5eed_d1a1_09c0_ffee);
    let (mut accepted, mut failures) = (0usize, Vec::new());
    for round in 0..20_000 {
        let (key, block) = &seeds[round % seeds.len()];
        let donor = &seeds[rng.below(seeds.len())].0;
        let mut mutant = mutate(&mut rng, key, donor);
        if rng.below(3) == 0 {
            mutant = mutate(&mut rng, &mutant, donor);
        }
        let block = block.as_ref().map(|block| {
            if rng.below(2) == 0 {
                mutate(&mut rng, block, block)
            } else {
                block.clone()
            }
        });
        let Some(view) = scanned_key(mutant.clone(), block.clone()) else {
            continue;
        };
        let owned = view.to_owned();
        let Ok(row) = view.clone().checked() else {
            continue;
        };
        accepted += 1;
        let read = catch_unwind(AssertUnwindSafe(|| read_all(&row)));
        match (read, owned) {
            (Err(panic), _) => failures.push(format!(
                "reading key {mutant:?} (block {block:?}) panicked: {:?}",
                panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
            )),
            (Ok(_), Err(error)) => failures.push(format!(
                "checked accepts key {mutant:?} (block {block:?}), to_owned refuses it: {error:?}"
            )),
            (Ok((fact, viewed, of, the)), Ok(expected)) => {
                if fact != expected
                    || viewed != expected.is
                    || of != expected.of.as_str()
                    || the != expected.the.as_str()
                {
                    failures.push(format!(
                        "key {mutant:?}: to_owned reads {expected:?}, checked reads {fact:?} \
                         (view {viewed:?}, of {of:?}, the {the:?})"
                    ));
                }
            }
        }
    }
    assert!(accepted > 1_000, "only {accepted} mutants were accepted");
    assert!(
        failures.is_empty(),
        "expected every row checked() accepts to read as to_owned() reads it; {} of {accepted} \
         do not:\n{}",
        failures.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// `Uri::check_stored` accepts exactly what `Entity::from_stored`
/// accepts, so `CheckedRow::entity`, which adopts a checked string
/// unchecked, builds only entities the eager read builds. Over a table
/// of URI shapes, the provable fast path's and the parser's alike.
#[dialog_common::test]
fn check_stored_accepts_what_from_stored_accepts() {
    let shapes = [
        "user:alice",
        "did:key:z6Mkabc",
        "asset:abc",
        "https://example.com/",
        "https://example.com",
        "https://Example.com/",
        "HTTPS://example.com/",
        "User:alice",
        "user:a b",
        "user:a%20b",
        "user:a\tb",
        "user:",
        "user:/a/../b",
        "mailto:a@b",
        "urn:isbn:0451450523",
        "file:///tmp/x",
        "a:\u{e9}",
        "a:%C3%A9",
        "x-y.z+w:opaque",
        "no-colon",
        ":empty-scheme",
        "user:a#frag",
        "user:a?q=1",
        "user:a\0b",
    ];
    let mut mismatches = Vec::new();
    for shape in shapes {
        let checked = Uri::check_stored(shape).is_ok();
        let stored = Entity::from_stored(shape).is_ok();
        if checked != stored {
            mismatches.push(format!(
                "{shape:?}: check_stored {checked}, from_stored {stored}"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "expected check_stored and from_stored to agree:\n{}",
        mismatches.join("\n")
    );
}
