use super::*;
use crate::identity::RECORD;
use crate::{Entity, ValueDataType};

fn relation(name: &str) -> Relation {
    name.parse().expect("a relation")
}

fn location(lat: f64, lon: f64) -> Composite {
    Composite::new([
        (relation("geo/lat"), Value::Float(lat)),
        (relation("geo/lon"), Value::Float(lon)),
    ])
    .expect("a composite")
}

/// A small deterministic generator, so a failing composite replays.
struct Seeded(u64);

impl Seeded {
    fn step(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.step() % bound as u64) as usize
    }
}

const ATTRIBUTES: [&str; 5] = ["geo/lat", "geo/lon", "x/tag", "x.y/zero", "tonk.event/name"];

fn value(random: &mut Seeded) -> Value {
    match random.below(9) {
        0 => Value::UnsignedInt(random.below(1000) as u128),
        1 => Value::SignedInt(random.below(1000) as i128 - 500),
        2 => Value::Float((random.below(2000) as f64 - 1000.0) / 8.0),
        3 => Value::Boolean(random.below(2) == 1),
        4 => Value::String(format!("s{}", random.below(40))),
        // A string holding a zero byte, which the encoding escapes.
        5 => Value::String(format!("a\0{}", random.below(5))),
        6 => Value::Bytes(
            (0..random.below(5))
                .map(|_| random.below(3) as u8)
                .collect(),
        ),
        7 => Value::Record(vec![0xA1, random.below(9) as u8]),
        _ => Value::Entity(
            format!("id:e{}", random.below(30))
                .parse()
                .expect("an entity"),
        ),
    }
}

/// Up to six pairs, a pair in four holding a nested composite while `depth`
/// allows, attributes repeating.
fn pairs(random: &mut Seeded, depth: usize) -> Vec<(Relation, Value)> {
    (0..random.below(7))
        .map(|_| {
            let attribute = relation(ATTRIBUTES[random.below(ATTRIBUTES.len())]);
            let value = if depth > 0 && random.below(4) == 0 {
                Value::Composite(Composite::new(pairs(random, depth - 1)).expect("nested"))
            } else {
                value(random)
            };
            (attribute, value)
        })
        .collect()
}

/// Order and repeats do not change a composite, it reads back from its
/// bytes to the same composite, it says how long it is, and every field
/// it holds is found by its attribute.
#[dialog_common::test]
fn it_has_one_encoding_whatever_order_and_repeats_it_is_given() {
    for seed in 1..=5_000 {
        let mut random = Seeded(seed);
        let given = pairs(&mut random, 2);
        let composite = Composite::new(given.clone()).expect("a composite");

        let mut shuffled = given.clone();
        shuffled.reverse();
        if let Some(first) = given.first().cloned() {
            shuffled.push(first);
        }
        assert_eq!(
            Composite::new(shuffled).expect("a composite"),
            composite,
            "seed {seed}"
        );
        assert_eq!(
            Composite::from_bytes(composite.as_bytes()).expect("its bytes read back"),
            composite,
            "seed {seed}"
        );
        let mut tail = composite.as_bytes().to_vec();
        tail.extend_from_slice(b"rest");
        assert_eq!(
            Composite::encoded_len(&tail),
            Some(composite.as_bytes().len()),
            "seed {seed}"
        );
        for (attribute, _) in &given {
            let found = composite.get(attribute);
            let held: Vec<Value> = composite
                .fields()
                .into_iter()
                .filter(|(a, _)| a == attribute)
                .map(|(_, v)| v)
                .collect();
            assert_eq!(found, held, "seed {seed}: {attribute}");
            assert!(!found.is_empty(), "seed {seed}: {attribute}");
        }
    }
}

#[dialog_common::test]
fn it_holds_one_number_however_it_was_typed() {
    let natural = Composite::new([(relation("x/n"), Value::UnsignedInt(3))]).unwrap();
    let float = Composite::new([(relation("x/n"), Value::Float(3.0))]).unwrap();
    assert_eq!(natural, float);
    let both = Composite::new([
        (relation("x/n"), Value::UnsignedInt(3)),
        (relation("x/n"), Value::Float(3.0)),
    ])
    .unwrap();
    assert_eq!(both, natural);
    assert_eq!(both.len(), 1);
}

#[dialog_common::test]
fn it_holds_several_values_of_an_attribute_as_a_set() {
    let tags = Composite::new([
        (relation("x/tag"), Value::String("b".into())),
        (relation("x/tag"), Value::String("a".into())),
        (relation("x/tag"), Value::String("b".into())),
    ])
    .unwrap();
    assert_eq!(
        tags.get(&relation("x/tag")),
        vec![Value::String("a".into()), Value::String("b".into())]
    );
    assert!(tags.get(&relation("x/other")).is_empty());
}

#[dialog_common::test]
fn it_nests_and_reads_a_nested_composite_back_as_its_own() {
    let place = Composite::new([
        (relation("place/name"), Value::String("home".into())),
        (
            relation("place/at"),
            Value::Composite(location(37.77, -122.42)),
        ),
    ])
    .unwrap();
    assert_eq!(
        place.get(&relation("place/at")),
        vec![Value::Composite(location(37.77, -122.42))]
    );
    // One table of names for the composite and what it nests.
    let names = header(place.as_bytes()).unwrap().0;
    assert_eq!(names.len(), 4);
}

#[dialog_common::test]
fn it_holds_nothing_when_empty() {
    let empty = Composite::new([]).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.as_bytes(), [0, 0]);
    assert!(!location(1.0, 2.0).is_empty());
}

#[dialog_common::test]
fn it_refuses_nan() {
    assert!(matches!(
        Composite::new([(relation("x/n"), Value::Float(f64::NAN))]),
        Err(DialogCorpusError::InvalidValue(_))
    ));
}

/// Bytes that read as a composite but are not its canonical encoding are
/// refused, so two composites with one content never have two encodings.
#[dialog_common::test]
fn it_refuses_bytes_that_are_not_canonical() {
    let canonical = location(1.0, 2.0);
    let bytes = canonical.as_bytes().to_vec();
    let names = header(&bytes).unwrap();
    let fields_at = names.1;

    let mut swapped_names = vec![2];
    swapped_names.extend_from_slice(b"geo/lon\0geo/lat\0");
    swapped_names.extend_from_slice(&bytes[fields_at..]);

    let mut unused_name = vec![3];
    unused_name.extend_from_slice(b"geo/lat\0geo/lon\0geo/zzz\0");
    unused_name.extend_from_slice(&bytes[fields_at..]);

    let mut trailing = bytes.clone();
    trailing.push(0);

    let mut long_count = vec![0x82, 0x00];
    long_count.extend_from_slice(&bytes[1..]);

    // The two fields written in the other order.
    let fields = &bytes[fields_at..];
    let first_len = 1 + slot_len(&fields[1..]).unwrap();
    let mut swapped_fields = bytes[..fields_at].to_vec();
    swapped_fields.extend_from_slice(&fields[first_len..fields.len() - 1]);
    swapped_fields.extend_from_slice(&fields[..first_len]);
    swapped_fields.push(END);

    // One field written twice.
    let mut repeated = bytes[..fields_at].to_vec();
    repeated.extend_from_slice(&fields[..first_len]);
    repeated.extend_from_slice(&fields[..fields.len()]);

    for (what, bad) in [
        ("names out of order", swapped_names),
        ("a name no field uses", unused_name),
        ("bytes past the end", trailing),
        ("a count of more bytes than it needs", long_count),
        ("fields out of order", swapped_fields),
        ("a field repeated", repeated),
        ("no bytes", Vec::new()),
    ] {
        assert!(
            matches!(
                Composite::from_bytes(&bad),
                Err(DialogCorpusError::InvalidValue(_))
            ),
            "{what}: {bad:?}"
        );
    }
}

/// The reference is the map the spec gives a struct: each attribute's
/// symbol to its value's reference, or to the set of its values'.
#[dialog_common::test]
fn it_references_as_a_map_of_attributes_to_values() {
    let lat = relation("geo/lat");
    let tag = relation("x/tag");
    let composite = Composite::new([
        (lat.clone(), Value::Float(1.5)),
        (tag.clone(), Value::String("a".into())),
        (tag.clone(), Value::String("b".into())),
    ])
    .unwrap();
    let expected = COMPOSITE.map([
        (
            SYMBOL.reference_of(lat.as_str().as_bytes()),
            Value::Float(1.5).reference(),
        ),
        (
            SYMBOL.reference_of(tag.as_str().as_bytes()),
            Type::SET.set([
                Value::String("a".into()).reference(),
                Value::String("b".into()).reference(),
            ]),
        ),
    ]);
    assert_eq!(composite.reference(), expected);
    assert_eq!(Value::Composite(composite.clone()).reference(), expected);

    // A nested composite is its own reference.
    let outer =
        Composite::new([(relation("place/at"), Value::Composite(composite.clone()))]).unwrap();
    assert_eq!(
        outer.reference(),
        COMPOSITE.map([(SYMBOL.reference_of(b"place/at"), composite.reference())])
    );
}

/// Opaque records keep their own type, so no record's bytes can pass for
/// a composite's identity.
#[dialog_common::test]
fn it_is_not_referenced_as_a_record_of_its_bytes() {
    let composite = location(1.0, 2.0);
    assert_ne!(
        composite.reference(),
        RECORD.reference_of(composite.as_bytes())
    );
    assert_ne!(
        Value::Composite(composite.clone()).identity_key(),
        Value::Record(composite.as_bytes().to_vec()).identity_key()
    );
}

#[dialog_common::test]
fn it_reads_back_from_every_form_a_value_is_written_in() {
    let composite = Composite::new([
        (relation("geo/lat"), Value::Float(37.77)),
        (
            relation("x/who"),
            Value::Entity("did:key:alice".parse::<Entity>().unwrap()),
        ),
        (relation("x/tag"), Value::String("a".into())),
        (relation("x/tag"), Value::String("b".into())),
    ])
    .unwrap();
    let value = Value::Composite(composite.clone());
    assert_eq!(value.data_type(), ValueDataType::Composite);

    // JSON: tagged, its pairs in canonical order.
    let json = serde_json::to_value(&value).unwrap();
    assert_eq!(
        json,
        serde_json::json!({ "composite:": [
            ["geo/lat", 37.77],
            ["x/tag", "a"],
            ["x/tag", "b"],
            ["x/who", "did:key:alice"],
        ]})
    );
    assert_eq!(serde_json::from_value::<Value>(json).unwrap(), value);

    // dag-cbor: tagged, its canonical bytes.
    let cbor = serde_ipld_dagcbor::to_vec(&value).unwrap();
    assert_eq!(
        serde_ipld_dagcbor::from_slice::<Value>(&cbor).unwrap(),
        value
    );

    // The text form.
    assert_eq!(value.to_utf8().parse::<Value>().unwrap(), value);

    // As a stored value's bytes, the spilled path among others.
    assert_eq!(
        Value::try_from((ValueDataType::Composite, value.to_bytes())).unwrap(),
        value
    );

    // As a key's value slot.
    let mut slot = Vec::new();
    crate::encode_key_value(&value, NUMBER_BAND, &mut slot);
    let (read, rest) = crate::decode_value(ValueDataType::Composite, &slot).unwrap();
    assert_eq!(read, value);
    assert!(rest.is_empty());
}
