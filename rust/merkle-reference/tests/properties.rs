//! The properties merkle references v2 promise, one test each.

use std::collections::{BTreeMap, HashMap};

use merkle_reference::{Bytes, Integer, Null, Refer, Reference, Type};

fn plain(bytes: &[u8]) -> Reference {
    Reference::from_bytes(*blake3::hash(bytes).as_bytes())
}

#[test]
fn plain_bytes_are_referenced_by_their_blake3_hash() {
    for bytes in [&b""[..], b"hello", &[7u8; 5000]] {
        assert_eq!(Bytes(bytes).refer(), plain(bytes));
        assert_eq!(Reference::of_bytes(bytes), plain(bytes));
    }
}

#[test]
fn the_same_bytes_under_two_types_have_two_references() {
    let entity = Type::named("entity:");
    let symbol = Type::named("symbol:");
    let content = b"did:key:alice";
    let references = [
        Bytes(content).refer(),
        Type::TEXT.refer(content),
        entity.refer(content),
        symbol.refer(content),
    ];
    for (i, a) in references.iter().enumerate() {
        for b in &references[i + 1..] {
            assert_ne!(a, b);
        }
    }

    // Empty content is the commonest overlap: null, the empty text, the empty
    // list and the empty map all have it.
    let empty: [Reference; 5] = [
        Bytes(b"").refer(),
        Null.refer(),
        "".refer(),
        Vec::<bool>::new().refer(),
        BTreeMap::<bool, bool>::new().refer(),
    ];
    for (i, a) in empty.iter().enumerate() {
        for b in &empty[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn no_keyed_reference_equals_the_plain_hash_of_its_content() {
    let parts = ["a".refer(), "b".refer(), 1.refer()];
    let list_content: Vec<u8> = parts.iter().flat_map(|part| *part.as_bytes()).collect();
    let cases: Vec<(Type, Vec<u8>)> = vec![
        (Type::TEXT, b"hello".to_vec()),
        (Type::BOOLEAN, vec![1]),
        (Type::INTEGER, Integer::from(1985).content()),
        (Type::FLOAT, 18.033f64.to_le_bytes().to_vec()),
        (Type::LIST, list_content.clone()),
        (Type::named("entity:"), b"did:key:alice".to_vec()),
        // Longer than one BLAKE3 chunk, where the root is a parent node.
        (Type::LIST, list_content.repeat(40)),
    ];
    for (ty, content) in cases {
        assert_ne!(ty.refer(&content), plain(&content));
    }
    assert_eq!(Type::LIST.list(parts), Type::LIST.refer(&list_content));
}

#[test]
fn a_part_linked_by_its_reference_gives_its_parent_the_same_reference() {
    let inner = vec![1, 2, 3];
    let mut entry = BTreeMap::new();
    entry.insert("done".to_string(), true);

    let embedded = Type::LIST.list(["a".refer(), inner.refer(), entry.refer()]);
    let linked = Type::LIST.list([
        "a".refer(),
        Reference::from_bytes(*inner.refer().as_bytes()).refer(),
        entry.refer().refer(),
    ]);
    assert_eq!(embedded, linked);

    // The same through the core types alone: a list of references in place of
    // the parts.
    let parts: Vec<Reference> = inner.iter().map(Refer::refer).collect();
    assert_eq!(parts.refer(), inner.refer());

    // And at any depth: link the deepest list in a map in a list.
    let deep = vec![vec![1, 2], vec![3]];
    let mut outer: BTreeMap<&str, Vec<Vec<i32>>> = BTreeMap::new();
    outer.insert("rows", deep.clone());
    let mut linked: BTreeMap<&str, Vec<Reference>> = BTreeMap::new();
    linked.insert("rows", deep.iter().map(Refer::refer).collect());
    assert_eq!(vec![outer].refer(), vec![linked.refer()].refer());

    // A user-defined struct: a map from field names under its own type.
    let rule = Type::named("rule:");
    let head = Type::named("concept:").map([("name".refer(), "person".refer())]);
    let embedded = rule.map([("head".refer(), head), ("when".refer(), inner.refer())]);
    let linked = rule.map([
        (
            "when".refer(),
            Reference::from_bytes(*inner.refer().as_bytes()),
        ),
        ("head".refer(), head),
    ]);
    assert_eq!(embedded, linked);
}

#[test]
fn a_value_has_one_reference_however_it_was_built() {
    // Maps: entry order does not matter, nor which map type holds them.
    let mut tree = BTreeMap::new();
    let mut hash = HashMap::new();
    for (key, value) in [("b", 2), ("a", 1), ("c", 3)] {
        tree.insert(key, value);
        hash.insert(key, value);
    }
    let reversed = Type::MAP
        .map([("c", 3), ("b", 2), ("a", 1)].map(|(key, value)| (key.refer(), value.refer())));
    assert_eq!(tree.refer(), hash.refer());
    assert_eq!(tree.refer(), reversed);

    // Lists: a slice, an array, a Vec and an iterator of references agree.
    let items = [true, false, true];
    assert_eq!(items.refer(), items.to_vec().refer());
    assert_eq!(items.refer(), items[..].refer());
    assert_eq!(
        items.refer(),
        Type::LIST.list(items.iter().map(Refer::refer))
    );

    // Integers: the width of the integer it came from does not matter.
    let five = 5u8;
    assert_eq!(5i64.refer(), 5u128.refer());
    assert_eq!(5i64.refer(), Integer::new(false, &[0, 0, five]).refer());
    assert_eq!(0i64.refer(), Integer::new(true, &[0]).refer());
    assert_ne!(5i64.refer(), (-5i64).refer());

    // Text and String; a value behind a pointer.
    assert_eq!("x".refer(), String::from("x").refer());
    let boxed: Box<dyn Refer> = Box::new(7);
    assert_eq!(boxed.refer(), 7.refer());
}

#[test]
fn a_float_is_not_an_integer() {
    assert_ne!(5f64.refer(), 5i64.refer());
    // The core float is bit for bit, as v1: negative zero and NaN payloads
    // are kept. Dialog's numbers do not use it.
    assert_ne!(0f64.refer(), (-0f64).refer());
}
