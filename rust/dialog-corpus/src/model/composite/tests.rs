use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;

use super::*;
use crate::identity::{RECORD, SYMBOL};
use crate::{Entity, NUMBER_BAND};

fn relation(name: &str) -> Relation {
    name.parse().expect("a relation")
}

fn text(text: &str) -> Value {
    Value::String(text.into())
}

fn natural(n: u128) -> Value {
    Value::UnsignedInt(n)
}

fn location(lat: f64, lon: f64) -> Composite {
    Composite::record([
        (relation("geo/lat"), Value::Float(lat)),
        (relation("geo/lon"), Value::Float(lon)),
    ])
    .expect("a composite")
}

fn hash(composite: &Composite) -> u64 {
    let mut state = DefaultHasher::new();
    composite.hash(&mut state);
    state.finish()
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

fn scalar_value(random: &mut Seeded) -> Value {
    match random.below(10) {
        0 => natural(random.below(1000) as u128),
        1 => Value::SignedInt(random.below(1000) as i128 - 500),
        2 => Value::Float((random.below(2000) as f64 - 1000.0) / 8.0),
        3 => Value::Boolean(random.below(2) == 1),
        4 => text(&format!("s{}", random.below(40))),
        // A string holding a zero byte, which the encoding escapes.
        5 => text(&format!("a\0{}", random.below(5))),
        6 => Value::Bytes(
            (0..random.below(5))
                .map(|_| random.below(3) as u8)
                .collect(),
        ),
        7 => Value::Record(vec![0xA1, random.below(9) as u8]),
        8 => Value::Symbol(relation(ATTRIBUTES[random.below(ATTRIBUTES.len())])),
        _ => Value::Entity(
            format!("id:e{}", random.below(30))
                .parse()
                .expect("an entity"),
        ),
    }
}

fn key(random: &mut Seeded, record: bool) -> Value {
    if record || random.below(2) == 0 {
        Value::Symbol(relation(ATTRIBUTES[random.below(ATTRIBUTES.len())]))
    } else {
        scalar_value(random)
    }
}

/// A plain model of a composite: what it holds, as a map from each key to
/// the set of its values, or a list of items.
#[derive(Clone, Debug)]
enum Model {
    Map(BTreeMap<Vec<u8>, (Value, Vec<Value>)>),
    List(Vec<Value>),
}

/// A value's identity as the model keeps it: its identity key, a nested
/// composite by its canonical bytes on its own.
fn id(value: &Value) -> Vec<u8> {
    value.identity_key()
}

impl Model {
    fn of(composite: &Composite) -> Model {
        match composite.kind() {
            CompositeKind::List => Model::List(composite.items().collect()),
            CompositeKind::Map => {
                let mut map: BTreeMap<Vec<u8>, (Value, Vec<Value>)> = BTreeMap::new();
                for (key, value) in composite.entries() {
                    map.entry(id(&key))
                        .or_insert((key, Vec::new()))
                        .1
                        .push(value);
                }
                Model::Map(map)
            }
        }
    }
}

/// Random content: the entries or items given, in the order given, and the
/// composite they make. Nested to `depth`, a record when `record` is set.
fn generate(random: &mut Seeded, depth: usize, record: bool) -> (Vec<(Value, Value)>, Composite) {
    let list = !record && random.below(3) == 0;
    let count = match random.below(6) {
        0 => random.below(40),
        _ => random.below(7),
    };
    let mut given = Vec::new();
    for _ in 0..count {
        let value = if depth > 0 && random.below(4) == 0 {
            let record = random.below(2) == 0;
            Value::Composite(generate(random, depth - 1, record).1)
        } else {
            scalar_value(random)
        };
        given.push((key(random, record), value));
    }
    let composite = if list {
        Composite::list(given.iter().map(|(_, value)| value.clone()))
    } else {
        Composite::map(given.iter().cloned())
    }
    .expect("a composite");
    (given, composite)
}

/// Against a plain model: a map holds each distinct entry once whatever
/// order or repeats it was given in, every key reads back its values, a
/// list reads back its items in order, and the length is what the model
/// holds.
#[dialog_common::test]
fn it_holds_what_a_plain_model_holds() {
    for seed in 1..=3_000 {
        let mut random = Seeded(seed);
        let (given, composite) = generate(&mut random, 2, false);
        match composite.kind() {
            CompositeKind::List => {
                let items: Vec<Value> = given.iter().map(|(_, value)| value.clone()).collect();
                assert_eq!(composite.len(), items.len(), "seed {seed}");
                let read: Vec<Vec<u8>> = composite.items().map(|item| id(&item)).collect();
                let given: Vec<Vec<u8>> = items.iter().map(id).collect();
                assert_eq!(read, given, "seed {seed}");
                for (index, item) in items.iter().enumerate() {
                    assert_eq!(
                        composite.item(index).map(|i| id(&i)),
                        Some(id(item)),
                        "seed {seed}"
                    );
                }
                assert_eq!(composite.item(items.len()), None, "seed {seed}");
                assert!(composite.get(&text("any")).is_empty());
            }
            CompositeKind::Map => {
                let mut model: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Value>> = BTreeMap::new();
                for (key, value) in &given {
                    model
                        .entry(id(key))
                        .or_default()
                        .insert(id(value), value.clone());
                }
                let pairs: usize = model.values().map(BTreeMap::len).sum();
                assert_eq!(composite.len(), pairs, "seed {seed}");
                for (key, _) in &given {
                    // Compared by identity: the natural 3 and the
                    // integer 3 are one number, read back as the narrowest.
                    let found = composite.get(key);
                    let mut ids: Vec<Vec<u8>> = found.iter().map(id).collect();
                    ids.sort();
                    let held: Vec<Vec<u8>> = model[&id(key)].keys().cloned().collect();
                    assert_eq!(ids, held, "seed {seed}");
                    assert_eq!(composite.first(key).as_ref(), found.first());
                }
                assert!(composite.get(&text("never a key")).is_empty());
                assert_eq!(composite.item(0), None, "a map has no items");
                // Given in another order, with repeats, it is the same.
                let mut shuffled = given.clone();
                shuffled.reverse();
                shuffled.extend(given.iter().take(3).cloned());
                let again = Composite::map(shuffled).expect("a composite");
                assert_eq!(again, composite, "seed {seed}");
                assert_eq!(again.to_bytes(), composite.to_bytes(), "seed {seed}");
                assert_eq!(again.reference(), composite.reference(), "seed {seed}");
            }
        }
        let read = Composite::from_bytes(&composite.to_bytes()).expect("its bytes read back");
        assert_eq!(read, composite, "seed {seed}");
        assert_eq!(hash(&read), hash(&composite), "seed {seed}");
    }
}

/// Every layout, offsets or none, is read alike: the limit changes only
/// how an item is reached, and only the canonical one is accepted as bytes.
#[dialog_common::test]
fn it_reads_every_layout_alike_and_accepts_one() {
    for seed in 1..=1_000 {
        let mut random = Seeded(seed);
        let (_, composite) = generate(&mut random, 2, false);
        let model = Model::of(&composite);
        for limit in [0, 1, 2, SCAN_LIMIT, 64, usize::MAX] {
            let bytes = composite.layout(limit);
            let read = Composite::from_layout(&bytes).expect("a layout reads");
            assert_eq!(read.len(), composite.len(), "seed {seed} limit {limit}");
            match (&model, Model::of(&read)) {
                (Model::List(a), Model::List(b)) => {
                    assert_eq!(*a, b, "seed {seed} limit {limit}");
                    for (index, item) in a.iter().enumerate() {
                        assert_eq!(read.item(index).as_ref(), Some(item));
                    }
                }
                (Model::Map(a), Model::Map(b)) => {
                    assert_eq!(*a, b, "seed {seed} limit {limit}");
                    for (key, values) in a.values() {
                        assert_eq!(read.get(key), *values, "seed {seed} limit {limit}");
                    }
                }
                _ => panic!("seed {seed}: the kind changed"),
            }
            assert_eq!(read.reference(), composite.reference());
            if bytes != composite.to_bytes().as_ref() {
                assert!(
                    matches!(
                        Composite::from_bytes(&bytes),
                        Err(CompositeError::NotCanonical)
                    ),
                    "seed {seed} limit {limit}"
                );
            }
        }
    }
}

/// Order is total and agrees with equality, and with the canonical bytes
/// the tree keys composites by; a composite nested in another equals,
/// hashes and orders as the same composite on its own.
#[dialog_common::test]
fn it_orders_totally_as_its_bytes_and_a_view_as_its_copy() {
    let mut composites = Vec::new();
    for seed in 1..=300 {
        let (_, composite) = generate(&mut Seeded(seed), 2, false);
        composites.push(composite);
    }
    for a in &composites {
        for b in &composites {
            assert_eq!(a.cmp(b), a.to_bytes().cmp(&b.to_bytes()));
            assert_eq!(a.cmp(b) == Ordering::Equal, a == b);
            assert_eq!(a.cmp(b), b.cmp(a).reverse());
            if a == b {
                assert_eq!(hash(a), hash(b));
            }
        }
    }
    for composite in composites.iter().take(100) {
        let outer = Composite::list([
            text("before"),
            Value::Composite(composite.clone()),
            Value::Composite(location(1.0, 2.0)),
        ])
        .unwrap();
        let Some(Value::Composite(view)) = outer.item(1) else {
            panic!("a nested composite reads as one");
        };
        assert!(!view.is_root());
        assert!(
            Arc::ptr_eq(&view.buffer, &outer.buffer),
            "a view, not a copy"
        );
        assert_eq!(&view, composite);
        assert_eq!(hash(&view), hash(composite));
        assert_eq!(view.cmp(composite), Ordering::Equal);
        assert_eq!(view.reference(), composite.reference());
        assert_eq!(view.to_bytes(), composite.to_bytes());
    }
}

/// A map's values under one key, nested composites among them, are in an
/// order that does not depend on the table of the composite holding them:
/// the same map nested in two composites with other keys reads alike.
#[dialog_common::test]
fn it_orders_nested_values_apart_from_the_table() {
    let a = Composite::map([(text("b"), natural(1))]).unwrap();
    let b = Composite::map([(text("a"), natural(1))]).unwrap();
    let both = Composite::map([
        (text("k"), Value::Composite(a.clone())),
        (text("k"), Value::Composite(b.clone())),
    ])
    .unwrap();
    let outer = Composite::map([
        (text("0"), natural(0)),
        (text("x"), Value::Composite(both.clone())),
    ])
    .unwrap();
    let Some(Value::Composite(view)) = outer.first(&text("x")) else {
        panic!("a nested map");
    };
    assert_eq!(view, both);
    assert_eq!(hash(&view), hash(&both));
    assert_eq!(view.get(&text("k")), both.get(&text("k")));
}

#[dialog_common::test]
fn it_holds_one_number_however_it_was_typed() {
    let natural = Composite::record([(relation("x/n"), natural(3))]).unwrap();
    let float = Composite::record([(relation("x/n"), Value::Float(3.0))]).unwrap();
    assert_eq!(natural, float);
    let both = Composite::record([
        (relation("x/n"), self::natural(3)),
        (relation("x/n"), Value::Float(3.0)),
    ])
    .unwrap();
    assert_eq!(both, natural);
    assert_eq!(both.len(), 1);
    // A number is one key however it was typed.
    let keyed = Composite::map([(Value::Float(3.0), text("three"))]).unwrap();
    assert_eq!(keyed.get(&self::natural(3)), vec![text("three")]);
    // And one item, in a list.
    assert_eq!(
        Composite::list([self::natural(3)]).unwrap(),
        Composite::list([Value::Float(3.0)]).unwrap()
    );
}

#[dialog_common::test]
fn it_holds_several_values_of_a_key_as_a_set() {
    let tags = Composite::record([
        (relation("x/tag"), text("b")),
        (relation("x/tag"), text("a")),
        (relation("x/tag"), text("b")),
    ])
    .unwrap();
    assert_eq!(tags.field(&relation("x/tag")), vec![text("a"), text("b")]);
    assert!(tags.field(&relation("x/other")).is_empty());
    assert_eq!(tags.len(), 2);
}

/// A list keeps order and repeats.
#[dialog_common::test]
fn it_keeps_a_lists_order_and_repeats() {
    let list = Composite::list([text("b"), text("a"), text("b")]).unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(
        list.items().collect::<Vec<_>>(),
        [text("b"), text("a"), text("b")]
    );
    assert_ne!(
        list,
        Composite::list([text("a"), text("b"), text("b")]).unwrap()
    );
    assert_ne!(
        list.reference(),
        Composite::list([text("a"), text("b"), text("b")])
            .unwrap()
            .reference()
    );
}

/// A record is the map whose every key is an attribute: told by its keys,
/// on construction and on read, whatever other keys its table holds for the
/// maps nested in it.
#[dialog_common::test]
fn it_tells_a_record_by_its_keys() {
    let record = location(1.0, 2.0);
    assert!(record.is_map() && record.is_record());
    let map = Composite::map([(text("lat"), Value::Float(1.0))]).unwrap();
    assert!(map.is_map() && !map.is_record());
    let mixed = Composite::map([
        (Value::Symbol(relation("geo/lat")), Value::Float(1.0)),
        (text("note"), text("x")),
    ])
    .unwrap();
    assert!(!mixed.is_record());
    let list = Composite::list([natural(1)]).unwrap();
    assert!(list.is_list() && !list.is_record() && !list.is_map());
    // A record nested in a map keyed by text, and a text-keyed map nested
    // in a record, share one table and are each told by their own keys.
    let outer = Composite::map([
        (text("place"), Value::Composite(record.clone())),
        (text("z"), natural(1)),
    ])
    .unwrap();
    let Some(Value::Composite(inner)) = outer.first(&text("place")) else {
        panic!("a nested record");
    };
    assert!(inner.is_record() && !outer.is_record());
    let outer = Composite::record([(relation("x/m"), Value::Composite(map.clone()))]).unwrap();
    let Some(Value::Composite(inner)) = outer.field(&relation("x/m")).pop() else {
        panic!("a nested map");
    };
    assert!(outer.is_record() && !inner.is_record());
    // The empty map is the empty record.
    assert!(Composite::map([]).unwrap().is_record());

    // The types say which.
    let of = |composite: &Composite| Value::Composite(composite.clone());
    assert!(ValueDataType::CompositeRecord.holds(&of(&record)));
    assert!(!ValueDataType::CompositeRecord.holds(&of(&map)));
    assert!(ValueDataType::CompositeMap.holds(&of(&record)));
    assert!(ValueDataType::CompositeMap.holds(&of(&map)));
    assert!(!ValueDataType::CompositeMap.holds(&of(&list)));
    assert!(ValueDataType::CompositeList.holds(&of(&list)));
    assert!(ValueDataType::Composite.holds(&of(&list)));
    assert!(ValueDataType::Composite.holds(&of(&map)));
    assert!(!ValueDataType::Composite.holds(&text("x")));
    assert_eq!(of(&record).data_type(), ValueDataType::CompositeMap);
    assert_eq!(of(&list).data_type(), ValueDataType::CompositeList);
    for kind in [
        ValueDataType::CompositeMap,
        ValueDataType::CompositeRecord,
        ValueDataType::CompositeList,
    ] {
        assert_eq!(kind.band(NUMBER_BAND), ValueDataType::Composite);
        assert_eq!(kind.band(0), ValueDataType::Composite);
        assert!(!kind.is_view() && !kind.is_number());
    }
    assert_eq!(
        ValueDataType::CompositeRecord.storage(),
        ValueDataType::CompositeMap
    );
}

/// Names are kept once for a composite and everything nested in it,
/// attributes and text keys alike.
#[dialog_common::test]
fn it_keeps_each_key_once_for_everything_nested() {
    let node = |kind: &str| {
        Composite::map([
            (
                text("type"),
                Value::Entity(format!("{kind}:").parse().unwrap()),
            ),
            (text("name"), text(kind)),
        ])
        .unwrap()
    };
    let tree = Composite::map([
        (text("type"), Value::Entity("tuple:".parse().unwrap())),
        (
            text("items"),
            Value::Composite(
                Composite::list((0..20).map(|_| Value::Composite(node("nested")))).unwrap(),
            ),
        ),
    ])
    .unwrap();
    let table = tree.table().unwrap();
    assert_eq!(
        table.count, 6,
        "three keys and three distinct scalar values, each once"
    );
}

#[dialog_common::test]
fn it_holds_nothing_when_empty() {
    let empty = Composite::map([]).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.to_bytes().as_ref(), [TABLE, 0, 0, MAP, 0, 0]);
    let empty = Composite::list([]).unwrap();
    assert!(empty.is_empty() && empty.is_list());
    assert_eq!(empty.to_bytes().as_ref(), [TABLE, 0, 0, LIST, 0, 0]);
    assert!(!location(1.0, 2.0).is_empty());
}

#[dialog_common::test]
fn it_refuses_nan_as_a_value_or_a_key() {
    assert!(matches!(
        Composite::record([(relation("x/n"), Value::Float(f64::NAN))]),
        Err(CompositeError::NotANumber)
    ));
    assert!(matches!(
        Composite::map([(Value::Float(f64::NAN), natural(1))]),
        Err(CompositeError::NotANumber)
    ));
    assert!(matches!(
        Composite::list([Value::Float(f64::NAN)]),
        Err(CompositeError::NotANumber)
    ));
}

#[dialog_common::test]
fn it_refuses_a_composite_as_a_key() {
    assert!(matches!(
        Composite::map([(Value::Composite(location(1.0, 2.0)), natural(1))]),
        Err(CompositeError::CompositeKey)
    ));
}

#[dialog_common::test]
fn it_refuses_to_nest_deeper_than_its_limit() {
    let mut nested = Composite::list([]).unwrap();
    for _ in 0..MAX_DEPTH - 1 {
        nested = Composite::list([Value::Composite(nested)]).unwrap();
    }
    assert!(Composite::from_bytes(&nested.to_bytes()).is_ok());
    assert!(matches!(
        Composite::list([Value::Composite(nested)]),
        Err(CompositeError::TooDeep)
    ));
}

/// The canonical bytes of `items` texts in a list, laid out by hand.
fn list_bytes(items: &[&str], width: usize) -> Vec<u8> {
    let mut body = Vec::new();
    let mut starts = Vec::new();
    for item in items {
        starts.push(body.len());
        body.push(ValueDataType::String as u8);
        body.extend_from_slice(item.as_bytes());
        body.push(0);
    }
    let mut out = vec![TABLE, 0, 0, layout::head(LIST, width), items.len() as u8];
    let offsets: Vec<u8> = if width == 0 {
        Vec::new()
    } else {
        starts[1..]
            .iter()
            .flat_map(|start| (*start as u64).to_be_bytes()[8 - width..].to_vec())
            .collect()
    };
    out.push((offsets.len() + body.len()) as u8);
    out.extend(offsets);
    out.extend(body);
    out
}

#[dialog_common::test]
fn it_writes_the_layout_the_documentation_draws() {
    let few = ["a", "b"];
    assert_eq!(
        Composite::list(few.iter().map(|t| text(t)))
            .unwrap()
            .to_bytes()
            .as_ref(),
        list_bytes(&few, 0)
    );
    let many: Vec<String> = (0..=SCAN_LIMIT).map(|n| format!("i{n}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert_eq!(
        Composite::list(many.iter().map(|t| text(t)))
            .unwrap()
            .to_bytes()
            .as_ref(),
        list_bytes(&many, 1)
    );
    // A record's keys are symbols in the table, its entries their places.
    let record = Composite::record([(relation("geo/lat"), Value::Boolean(true))]).unwrap();
    let mut expected = vec![TABLE, 1, 9, ValueDataType::Symbol as u8];
    expected.extend_from_slice(b"geo/lat\0");
    expected.extend_from_slice(&[MAP, 1, 3, 0, ValueDataType::Boolean as u8, 1]);
    assert_eq!(record.to_bytes().as_ref(), expected);

    // The drawing in design/composite-value.md.
    let drawn = Composite::map([
        (text("type"), Value::Entity("tuple:".parse().unwrap())),
        (
            text("items"),
            Value::Composite(Composite::list([text("a"), text("b")]).unwrap()),
        ),
    ])
    .unwrap();
    let mut expected = vec![TABLE, 2, 13, 3];
    expected.extend_from_slice(b"items\0\x03type\0");
    expected.extend_from_slice(&[MAP, 2, 20, 0, 22, LIST, 2, 6, 3, b'a', 0, 3, b'b', 0, 1, 1]);
    expected.extend_from_slice(b"tuple:\0");
    assert_eq!(drawn.to_bytes().as_ref(), expected);
}

fn refused(bytes: &[u8]) -> CompositeError {
    Composite::from_bytes(bytes).expect_err("refused")
}

#[dialog_common::test]
fn it_refuses_no_bytes() {
    assert!(matches!(refused(&[]), CompositeError::Malformed));
}

#[dialog_common::test]
fn it_refuses_bytes_past_the_end() {
    let mut bytes = location(1.0, 2.0).to_bytes().into_owned();
    bytes.push(0);
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
}

#[dialog_common::test]
fn it_refuses_a_varint_longer_than_it_needs() {
    // The table's count, 0, in two bytes.
    let bytes = [TABLE, 0x80, 0x00, 0, LIST, 0, 0];
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
}

/// A five-byte varint past `u32::MAX` is refused on every target. On a
/// 32-bit target a `usize` would keep only its low bits, and the table's
/// count, 2^32 here, would read as 0.
#[dialog_common::test]
fn it_refuses_a_varint_past_what_four_byte_offsets_reach() {
    let bytes = [TABLE, 0x80, 0x80, 0x80, 0x80, 0x10, 0, LIST, 0, 0];
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
    // The largest a varint may be still reads, and is refused only because
    // the bytes do not hold that many keys.
    let largest = [TABLE, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0, LIST, 0, 0];
    assert!(matches!(refused(&largest), CompositeError::Malformed));
}

/// Fixed composites, one of each shape: a small record, a map with text
/// keys (one held twice), a list past [`SCAN_LIMIT`] (so it carries
/// offsets, two bytes wide), one nested three levels deep, and an empty
/// map and list.
fn pinned() -> [(&'static str, Composite); 6] {
    let record = Composite::record([
        (relation("geo/lat"), Value::Float(37.77)),
        (relation("geo/lon"), Value::Float(-122.42)),
        (relation("x/count"), natural(3)),
        (relation("x/ok"), Value::Boolean(true)),
    ])
    .unwrap();
    let map = Composite::map([
        (text("b"), natural(2)),
        (text("a"), text("one")),
        (text("a"), Value::SignedInt(-5)),
    ])
    .unwrap();
    let list = Composite::list((0..30).map(|n| text(&format!("entry {n}")))).unwrap();
    let nested = Composite::record([
        (relation("place/name"), text("home")),
        (
            relation("place/visits"),
            Value::Composite(
                Composite::list([
                    Value::Composite(location(1.5, -2.25)),
                    Value::Composite(location(3.0, 4.0)),
                ])
                .unwrap(),
            ),
        ),
    ])
    .unwrap();
    [
        ("record", record),
        ("map", map),
        ("list", list),
        ("nested", nested),
        ("empty map", Composite::map([]).unwrap()),
        ("empty list", Composite::list([]).unwrap()),
    ]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The bytes and the identity of each [`pinned`] composite, as written on a
/// 64-bit target. A composite's bytes are canonical and key it in the tree,
/// and its reference is its identity, so a replica on a 32-bit target
/// (wasm32) and one on a 64-bit target must agree on both to the bit.
const PINNED: [(&str, &str, &str); 6] = [
    (
        "record",
        concat!(
            "00082f02010867656f2f6c6174000867656f2f6c6f6e0008782f636f756e7400",
            "08782f6f6b000911fcd2ab09180609184b9a01040c011a07021a05031a06041a",
            "00",
        ),
        "50ded4219243f1c813257bf01cf27a8a4ed98d036db724181a286202364b6aeb",
    ),
    (
        "map",
        concat!("000511036100036200036f6e65000912f5091804010309001a02001a03011a04",),
        "507f2af804513e2aeb7c9f4d7cf6aa22c3d59e21c6d855f0617a6d1eaa745bc0",
    ),
    (
        "list",
        concat!(
            "081edc0200090012001c00260030003a0044004e00580062006c0076007f0089",
            "0093009d00a700b100bb00c500cf00d900e300ec00f500fe0107011001190365",
            "6e74727920300003656e74727920310003656e7472792031300003656e747279",
            "2031310003656e7472792031320003656e7472792031330003656e7472792031",
            "340003656e7472792031350003656e7472792031360003656e74727920313700",
            "03656e7472792031380003656e7472792031390003656e74727920320003656e",
            "7472792032300003656e7472792032310003656e7472792032320003656e7472",
            "792032330003656e7472792032340003656e7472792032350003656e74727920",
            "32360003656e7472792032370003656e7472792032380003656e747279203239",
            "0003656e74727920330003656e74727920340003656e74727920350003656e74",
            "727920360003656e74727920370003656e74727920380003656e747279203900",
            "061e59020406080a0c0e10121416181a1c1e20222426282a2c2e30323436383a",
            "1a001a011a0c1a171a181a191a1a1a1b1a1c1a1d1a021a031a041a051a061a07",
            "1a081a091a0a1a0b1a0d1a0e1a0f1a101a111a121a131a141a151a16",
        ),
        "0d3d1d606bc5137764482542ee31f38917510661669151f1f0d2d64be76c6a66",
    ),
    (
        "nested",
        concat!(
            "040948060f182432363a3d03686f6d65000867656f2f6c6174000867656f2f6c",
            "6f6e0008706c6163652f6e616d650008706c6163652f766973697473000912fa",
            "cd0918036409180609180801021c031a00041602021416010206011a06021a05",
            "16010206011a07021a08",
        ),
        "34cad9fcd5b93b9bab76713956213614674465aa6da254179a924ec6e4cbda14",
    ),
    (
        "empty map",
        "000000010000",
        "7e9f034ad1c981199f1a7d0e6dd594c0957d8ecf239f3cdb60cf56b4fb7a69ee",
    ),
    (
        "empty list",
        "000000020000",
        "10fc213d5e98202cf24d703ca1eb188a1620d49206822f4560efedcd24dc645c",
    ),
];

/// Every [`pinned`] composite writes the bytes and has the reference
/// [`PINNED`] holds, on every target, and reads back from those bytes as
/// itself.
#[dialog_common::test]
fn it_writes_and_names_fixed_composites_alike_on_every_target() {
    for ((name, composite), (pinned, bytes, reference)) in pinned().into_iter().zip(PINNED) {
        assert_eq!(name, pinned);
        let written = composite.to_bytes();
        assert_eq!(hex(&written), bytes, "{name}'s bytes");
        assert_eq!(
            composite.reference().to_string(),
            reference,
            "{name}'s reference"
        );
        assert_eq!(
            Composite::from_bytes(&written).unwrap(),
            composite,
            "{name}"
        );
    }
}

/// The list [`pinned`] holds carries two-byte offsets, and the one of
/// `SCAN_LIMIT + 1` texts one-byte offsets, read where the layout puts them.
#[dialog_common::test]
fn it_writes_offsets_alike_on_every_target() {
    let [_, _, (_, list), ..] = pinned();
    let bytes = list.to_bytes();
    let body = list.seq().unwrap();
    assert_eq!(body.kind, LIST);
    assert_eq!(body.width, 1);
    assert_eq!(
        (1..body.count)
            .map(|index| body.offset(&bytes, index).unwrap() - body.items)
            .collect::<Vec<_>>(),
        (1..body.count).map(|index| 2 * index).collect::<Vec<_>>()
    );
    let many: Vec<String> = (0..=SCAN_LIMIT).map(|n| format!("i{n}")).collect();
    let many = Composite::list(many.iter().map(|t| text(t))).unwrap();
    let bytes = many.to_bytes();
    let table = many.table().unwrap();
    assert_eq!(table.count, SCAN_LIMIT + 1);
    assert_eq!(table.width, 1);
    assert_eq!(
        (1..table.count)
            .map(|index| table.offset(&bytes, index).unwrap() - table.items)
            .collect::<Vec<_>>(),
        (1..table.count).map(|index| 4 * index).collect::<Vec<_>>()
    );
    let body = many.seq().unwrap();
    assert_eq!(body.width, 1);
    assert_eq!(
        (1..body.count)
            .map(|index| body.offset(&bytes, index).unwrap() - body.items)
            .collect::<Vec<_>>(),
        (1..body.count).map(|index| 2 * index).collect::<Vec<_>>()
    );
}

#[dialog_common::test]
fn it_refuses_keys_out_of_order() {
    let canonical = Composite::map([(text("a"), natural(1)), (text("b"), natural(2))]).unwrap();
    let bytes = canonical.to_bytes().into_owned();
    let mut swapped = bytes.clone();
    // The table holds `03 a 00 03 b 00` from its fourth byte: swap them.
    swapped[4] = b'b';
    swapped[7] = b'a';
    assert!(matches!(refused(&swapped), CompositeError::NotCanonical));
}

#[dialog_common::test]
fn it_refuses_a_key_no_entry_uses() {
    let used = Composite::map([(text("a"), natural(1))]).unwrap();
    let extra = Composite::map([(text("z"), natural(2))]).unwrap();
    let mut tokens: Vec<Vec<u8>> = used
        .atom_tokens()
        .iter()
        .map(|token| token.to_vec())
        .collect();
    tokens.push(
        extra
            .atom_tokens()
            .into_iter()
            .find(|token| token.first() == Some(&(ValueDataType::Number as u8)))
            .unwrap()
            .to_vec(),
    );
    tokens.sort();
    let tokens: Vec<&[u8]> = tokens.iter().map(Vec::as_slice).collect();
    assert!(matches!(
        Composite::from_token_parts(&tokens, used.token_body()),
        Err(CompositeError::NotCanonical)
    ));
}

#[dialog_common::test]
fn it_refuses_entries_out_of_order() {
    let canonical = Composite::map([(text("a"), natural(1)), (text("b"), natural(2))]).unwrap();
    let bytes = canonical.to_bytes().into_owned();
    let body = canonical.body;
    let seq = canonical.seq().unwrap();
    let entries = bytes[seq.items..].to_vec();
    let half = entries.len() / 2;
    let mut swapped = bytes[..body].to_vec();
    swapped.extend_from_slice(&bytes[body..seq.items]);
    swapped.extend_from_slice(&entries[half..]);
    swapped.extend_from_slice(&entries[..half]);
    assert!(matches!(refused(&swapped), CompositeError::NotCanonical));
}

#[dialog_common::test]
fn it_refuses_an_entry_twice() {
    let once = Composite::map([(text("a"), natural(1))]).unwrap();
    let bytes = once.to_bytes().into_owned();
    let seq = once.seq().unwrap();
    let entry = bytes[seq.items..].to_vec();
    let mut twice = bytes[..once.body].to_vec();
    twice.extend_from_slice(&[MAP, 2, (2 * entry.len()) as u8]);
    twice.extend_from_slice(&entry);
    twice.extend_from_slice(&entry);
    assert!(matches!(refused(&twice), CompositeError::NotCanonical));
}

#[dialog_common::test]
fn it_refuses_offsets_on_a_short_sequence() {
    assert!(matches!(
        refused(&list_bytes(&["a", "b"], 1)),
        CompositeError::NotCanonical
    ));
}

#[dialog_common::test]
fn it_refuses_a_long_sequence_without_offsets() {
    let many: Vec<String> = (0..=SCAN_LIMIT).map(|n| format!("i{n}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(matches!(
        refused(&list_bytes(&many, 0)),
        CompositeError::NotCanonical
    ));
}

#[dialog_common::test]
fn it_refuses_offsets_wider_than_they_need() {
    let many: Vec<String> = (0..=SCAN_LIMIT).map(|n| format!("i{n}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(matches!(
        refused(&list_bytes(&many, 2)),
        CompositeError::NotCanonical
    ));
}

#[dialog_common::test]
fn it_refuses_an_offset_that_is_not_where_its_item_is() {
    let many: Vec<String> = (0..=SCAN_LIMIT).map(|n| format!("i{n}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    let mut bytes = list_bytes(&many, 1);
    // The first offset, of the second item, one byte late.
    bytes[6] += 1;
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
}

#[dialog_common::test]
fn it_refuses_a_number_under_a_type_other_than_the_band() {
    // The natural 3 under `natural:`'s type byte and in its own encoding,
    // which a key outside the band holds: a composite holds every number in
    // the band, so it reads no slot of `natural:`.
    let mut slot = vec![ValueDataType::UnsignedInt as u8];
    crate::encode_key_value(&natural(3), 0, &mut slot);
    let mut bytes = vec![TABLE, 0, 0, LIST, 1, slot.len() as u8];
    bytes.extend(slot);
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
    // A number in the band's encoding with more bytes than it needs reads
    // as the number, and is not its encoding.
    let mut slot = vec![ValueDataType::Number as u8];
    dialog_number::encode_f64(3.0, &mut slot);
    let canonical = Composite::list([natural(3)])
        .unwrap()
        .to_bytes()
        .into_owned();
    let mut bytes = vec![TABLE, 0, 0, LIST, 1, slot.len() as u8];
    bytes.extend(slot);
    if bytes != canonical {
        assert!(matches!(refused(&bytes), CompositeError::NotCanonical));
    }
}

#[dialog_common::test]
fn it_refuses_a_composite_in_the_table() {
    let bytes = [TABLE, 1, 4, 22, LIST, 0, 0, MAP, 0, 0];
    assert!(matches!(refused(&bytes), CompositeError::Malformed));
}

#[dialog_common::test]
fn it_refuses_nan_in_bytes() {
    let mut slot = vec![ValueDataType::Number as u8];
    dialog_number::encode_f64(f64::NAN, &mut slot);
    let mut bytes = vec![TABLE, 0, 0, LIST, 1, slot.len() as u8];
    bytes.extend(slot);
    assert!(matches!(
        refused(&bytes),
        CompositeError::NotANumber | CompositeError::Malformed
    ));
}

/// Random bytes, and valid bytes with a byte changed, cut or added, never
/// panic: they are refused, or read as a composite whose every accessor
/// answers.
#[dialog_common::test]
fn it_never_panics_on_any_bytes() {
    let mut random = Seeded(0x5eed);
    let mut valid = Vec::new();
    for seed in 1..=200 {
        valid.push(
            generate(&mut Seeded(seed), 2, false)
                .1
                .layout(random.below(4)),
        );
    }
    for round in 0..20_000 {
        let bytes: Vec<u8> = if round % 2 == 0 {
            (0..random.below(64))
                .map(|_| random.below(256) as u8)
                .collect()
        } else {
            let mut bytes = valid[random.below(valid.len())].clone();
            match random.below(3) {
                0 if !bytes.is_empty() => {
                    let at = random.below(bytes.len());
                    bytes[at] = random.below(256) as u8;
                }
                1 => bytes.truncate(random.below(bytes.len() + 1)),
                _ => bytes.insert(random.below(bytes.len() + 1), random.below(256) as u8),
            }
            bytes
        };
        let _ = Composite::from_bytes(&bytes);
        // A view over bytes nothing checked answers too, if wrongly.
        let unchecked = Composite::root(bytes.clone().into());
        let _ = (unchecked.len(), unchecked.kind(), unchecked.is_record());
        let _ = unchecked.get(&text("a"));
        let _ = unchecked.item(random.below(4));
        let _ = unchecked.entries().take(64).count();
        let _ = unchecked.items().take(64).count();
        let _ = hash(&unchecked);
        if let Ok(read) = Composite::from_layout(&bytes) {
            let _ = read.reference();
            let _ = read.to_bytes();
            let _ = format!("{read:?}");
        }
    }
}

/// Reading one entry or item decodes that one alone: the others, nested
/// composites among them, are stepped over by their sizes or reached past
/// by offsets.
#[dialog_common::test]
fn it_decodes_only_what_it_returns() {
    for count in [1, 4, SCAN_LIMIT, SCAN_LIMIT + 1, 64, 256] {
        let entries: Vec<(Value, Value)> = (0..count)
            .map(|n| {
                let value = if n % 2 == 0 {
                    Value::Composite(location(n as f64, 0.0))
                } else {
                    text(&format!("v{n}"))
                };
                (text(&format!("k{n:03}")), value)
            })
            .collect();
        let map = Composite::map(entries.clone()).unwrap();
        let list = Composite::list(entries.iter().map(|(_, value)| value.clone())).unwrap();
        for (n, (key, value)) in entries.iter().enumerate() {
            let before = decoded();
            assert_eq!(map.first(key).as_ref(), Some(value));
            assert_eq!(list.item(n).as_ref(), Some(value));
            let scalar = usize::from(matches!(value, Value::String(_)));
            assert_eq!(decoded() - before, 2 * scalar, "{count}: {n}");
        }
        let before = decoded();
        assert_eq!(map.len(), count);
        assert_eq!(list.len(), count);
        assert_eq!(decoded(), before, "a length decodes nothing");
        assert_eq!(list.items().take(2).count(), 2.min(count));
        assert!(
            decoded() - before <= 2,
            "an iteration decodes what it yields"
        );
    }
}

/// The identity is the merkle reference of the parts: a map of each key's
/// reference to its value's, or to the set of its values', and a list of
/// its items' references. A record's is the one #812 gave it.
#[dialog_common::test]
fn it_references_the_parts_and_not_the_bytes() {
    let lat = relation("geo/lat");
    let tag = relation("x/tag");
    let composite = Composite::record([
        (lat.clone(), Value::Float(1.5)),
        (tag.clone(), text("a")),
        (tag.clone(), text("b")),
    ])
    .unwrap();
    let expected = COMPOSITE.map([
        (
            SYMBOL.reference_of(lat.as_str().as_bytes()),
            Value::Float(1.5).reference(),
        ),
        (
            SYMBOL.reference_of(tag.as_str().as_bytes()),
            Type::SET.set([text("a").reference(), text("b").reference()]),
        ),
    ]);
    assert_eq!(composite.reference(), expected);
    assert_eq!(Value::Composite(composite.clone()).reference(), expected);

    let map = Composite::map([(text("k"), natural(1))]).unwrap();
    assert_eq!(
        map.reference(),
        COMPOSITE.map([(text("k").reference(), natural(1).reference())])
    );
    let list = Composite::list([text("k"), natural(1)]).unwrap();
    assert_eq!(
        list.reference(),
        Type::LIST.list([text("k").reference(), natural(1).reference()])
    );
    // A list is not a map whose pairs it would flatten to.
    assert_ne!(list.reference(), map.reference());

    // A nested composite is its own reference, so the layout of the
    // composite holding it does not enter.
    let outer =
        Composite::record([(relation("place/at"), Value::Composite(composite.clone()))]).unwrap();
    assert_eq!(
        outer.reference(),
        COMPOSITE.map([(SYMBOL.reference_of(b"place/at"), composite.reference())])
    );
    for limit in [0, usize::MAX] {
        let read = Composite::from_layout(&outer.layout(limit)).unwrap();
        assert_eq!(read.reference(), outer.reference());
    }
    assert_ne!(
        composite.reference(),
        RECORD.reference_of(&composite.to_bytes())
    );
    assert_ne!(
        Value::Composite(composite.clone()).identity_key(),
        Value::Record(composite.to_bytes().into_owned()).identity_key()
    );
}

#[dialog_common::test]
fn it_reads_back_from_every_form_a_value_is_written_in() {
    let record = Composite::record([
        (relation("geo/lat"), Value::Float(37.77)),
        (
            relation("x/who"),
            Value::Entity("did:key:alice".parse::<Entity>().unwrap()),
        ),
        (relation("x/tag"), text("a")),
        (relation("x/tag"), text("b")),
    ])
    .unwrap();
    let map = Composite::map([
        (text("type"), Value::Entity("tuple:".parse().unwrap())),
        (natural(7), Value::Composite(record.clone())),
    ])
    .unwrap();
    let list = Composite::list([text("a"), Value::Composite(map.clone()), text("a")]).unwrap();
    for composite in [record, map, list] {
        let value = Value::Composite(composite.clone());
        let json = serde_json::to_value(&value).unwrap();
        assert_eq!(serde_json::from_value::<Value>(json).unwrap(), value);
        let cbor = serde_ipld_dagcbor::to_vec(&value).unwrap();
        assert_eq!(
            serde_ipld_dagcbor::from_slice::<Value>(&cbor).unwrap(),
            value
        );
        assert_eq!(value.to_utf8().parse::<Value>().unwrap(), value);
        for kind in [value.data_type(), ValueDataType::Composite] {
            assert_eq!(Value::try_from((kind, value.to_bytes())).unwrap(), value);
        }
        let mut slot = Vec::new();
        crate::encode_key_value(&value, NUMBER_BAND, &mut slot);
        let (read, rest) = crate::decode_value(ValueDataType::Composite, &slot).unwrap();
        assert_eq!(read, value);
        assert!(rest.is_empty());
    }
    // JSON says which kind, tagged by the value's type.
    let json =
        serde_json::to_value(Value::Composite(Composite::list([text("a")]).unwrap())).unwrap();
    assert!(json.get("composite:list").is_some(), "{json}");
    // A record read where a list is declared is refused.
    assert!(
        Value::try_from((
            ValueDataType::CompositeList,
            location(1.0, 2.0).to_bytes().into_owned()
        ))
        .is_err()
    );
}

/// What the notation needs: build from entries or items in any order, read
/// them back in canonical order, and reach a nested part by a path of keys
/// and indexes, each step a view of the one buffer.
#[dialog_common::test]
fn it_builds_from_entries_and_reads_them_back_in_order() {
    let built = Composite::map([
        (text("b"), natural(2)),
        (text("a"), natural(1)),
        (
            text("c"),
            Value::Composite(Composite::list([natural(9), natural(8)]).unwrap()),
        ),
    ])
    .unwrap();
    let keys: Vec<Value> = built.entries().map(|(key, _)| key).collect();
    assert_eq!(keys, [text("a"), text("b"), text("c")]);
    let Some(Value::Composite(list)) = built.first(&text("c")) else {
        panic!("a nested list");
    };
    assert_eq!(list.item(1), Some(natural(8)));
    assert_eq!(
        list.item_view(0).and_then(|view| view.to_value().ok()),
        Some(natural(9))
    );
    assert!(matches!(
        built.first_view(&text("a")),
        Some(ValueView::Float(_) | ValueView::UnsignedInt(_))
    ));
}

/// A record is a sub-band of the map, not a type beside it: a map whose
/// keys are all attributes and the same content built as a record are one
/// value, with one byte string and one identity.
#[dialog_common::test]
fn it_is_one_value_built_as_a_map_or_as_a_record() {
    let record = Composite::record([
        (relation("geo/lat"), Value::Float(1.0)),
        (relation("geo/lon"), Value::Float(2.0)),
    ])
    .unwrap();
    let map = Composite::map([
        (Value::Symbol(relation("geo/lon")), Value::Float(2.0)),
        (Value::Symbol(relation("geo/lat")), Value::Float(1.0)),
    ])
    .unwrap();
    assert_eq!(map, record);
    assert_eq!(map.to_bytes(), record.to_bytes());
    assert_eq!(map.reference(), record.reference());
    assert_eq!(hash(&map), hash(&record));
    assert!(map.is_record());
    assert_eq!(
        Value::Composite(map).identity_key(),
        Value::Composite(record).identity_key()
    );
}

/// Keyed in the tree, maps, records and lists order as composites do, the
/// order is total, and no key's value slot is a prefix of another's: a
/// composite is escaped and terminated as a byte string is, so a shorter
/// one never reads as the front of a longer.
#[dialog_common::test]
fn it_orders_as_a_tree_key_without_prefixes_colliding() {
    let mut composites: Vec<Composite> = (1..=400)
        .map(|seed| generate(&mut Seeded(seed), 2, seed % 3 == 0).1)
        .collect();
    // A composite whose bytes are a prefix of another's.
    composites.push(Composite::list([text("a")]).unwrap());
    composites.push(Composite::list([text("a"), text("b")]).unwrap());
    composites.push(Composite::map([]).unwrap());
    composites.push(Composite::list([]).unwrap());
    let slot = |composite: &Composite| {
        let mut slot = Vec::new();
        crate::encode_key_value(&Value::Composite(composite.clone()), NUMBER_BAND, &mut slot);
        slot
    };
    let mut keyed: Vec<(Vec<u8>, Composite)> =
        composites.iter().map(|c| (slot(c), c.clone())).collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in keyed.windows(2) {
        let ((a_key, a), (b_key, b)) = (&pair[0], &pair[1]);
        assert_eq!(
            a.cmp(b),
            a_key.cmp(b_key),
            "the key orders as the composite"
        );
        if a != b {
            assert!(!b_key.starts_with(a_key), "{a:?} keys as a prefix of {b:?}");
        }
    }
    for (key, composite) in &keyed {
        let mut tail = key.clone();
        tail.extend_from_slice(b"\x05rest");
        let (read, rest) = crate::decode_value(ValueDataType::Composite, &tail).unwrap();
        assert_eq!(read, Value::Composite(composite.clone()));
        assert_eq!(rest, b"\x05rest");
    }
}
