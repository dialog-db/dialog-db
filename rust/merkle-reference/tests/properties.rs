//! The properties merkle references v2 promise, one test each, with types a
//! user of the crate would define.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

use merkle_reference::{Reference, ToReference, Type};

static TEXT: LazyLock<Type> = LazyLock::new(|| Type::named("text:").unwrap());
static ENTITY: LazyLock<Type> = LazyLock::new(|| Type::named("entity:").unwrap());

/// A user's own scalar type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Text(&'static str);

impl ToReference for Text {
    fn to_reference(&self) -> Reference {
        TEXT.reference_of(self.0.as_bytes())
    }
}

fn plain(bytes: &[u8]) -> Reference {
    Reference::from_bytes(*blake3::hash(bytes).as_bytes())
}

#[test]
fn raw_bytes_are_referenced_by_their_blake3_hash() {
    for bytes in [&b""[..], b"hello", &[7u8; 5000]] {
        assert_eq!(Reference::hash(bytes), plain(bytes));
    }
}

#[test]
fn the_same_bytes_under_two_types_have_two_references() {
    let content = b"did:key:alice";
    let references = [
        Reference::hash(content),
        TEXT.reference_of(content),
        ENTITY.reference_of(content),
    ];
    for (i, a) in references.iter().enumerate() {
        for b in &references[i + 1..] {
            assert_ne!(a, b);
        }
    }

    // Empty content is the commonest overlap: raw bytes, an empty text and
    // the empty list, map and set all have it.
    let empty: [Reference; 5] = [
        Reference::hash(b""),
        TEXT.reference_of(b""),
        Vec::<Text>::new().to_reference(),
        BTreeMap::<Text, Text>::new().to_reference(),
        BTreeSet::<Text>::new().to_reference(),
    ];
    for (i, a) in empty.iter().enumerate() {
        for b in &empty[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn no_typed_reference_equals_the_plain_hash_of_its_content() {
    let parts = [Text("a"), Text("b"), Text("c")].map(|text| text.to_reference());
    let list_content: Vec<u8> = parts.iter().flat_map(|part| *part.as_bytes()).collect();
    let cases: Vec<(Type, Vec<u8>)> = vec![
        (*TEXT, b"hello".to_vec()),
        (*ENTITY, b"did:key:alice".to_vec()),
        (Type::LIST, list_content.clone()),
        // Longer than one BLAKE3 chunk, where the root is a parent node.
        (Type::LIST, list_content.repeat(40)),
    ];
    for (ty, content) in cases {
        assert_ne!(ty.reference_of(&content), plain(&content));
    }
    assert_eq!(
        Type::LIST.list(parts),
        Type::LIST.reference_of(&list_content)
    );
}

#[test]
fn a_part_linked_by_its_reference_gives_its_parent_the_same_reference() {
    let inner = vec![Text("1"), Text("2"), Text("3")];
    let mut entry = BTreeMap::new();
    entry.insert(Text("done"), Text("yes"));

    let embedded = (Text("a"), inner.clone(), entry.clone());
    let embedded = Type::LIST.list([
        embedded.0.to_reference(),
        embedded.1.to_reference(),
        embedded.2.to_reference(),
    ]);
    let linked = Type::LIST.list([
        Text("a").to_reference(),
        // Known only by their references.
        Reference::from_bytes(*inner.to_reference().as_bytes()),
        entry.to_reference().to_reference(),
    ]);
    assert_eq!(embedded, linked);

    // A list of references in place of the parts.
    let parts: Vec<Reference> = inner.iter().map(ToReference::to_reference).collect();
    assert_eq!(parts.to_reference(), inner.to_reference());

    // At any depth: link the deepest lists in a map in a list.
    let deep = vec![vec![Text("1"), Text("2")], vec![Text("3")]];
    let mut outer = BTreeMap::new();
    outer.insert(Text("rows"), deep.clone());
    let mut linked = BTreeMap::new();
    linked.insert(
        Text("rows"),
        deep.iter()
            .map(ToReference::to_reference)
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        vec![outer].to_reference(),
        vec![linked.to_reference()].to_reference()
    );

    // A set, and a user's struct type: a map from field names under its own
    // type.
    let members: BTreeSet<Text> = [Text("x"), Text("y")].into();
    let linked_members: BTreeSet<Reference> =
        members.iter().map(ToReference::to_reference).collect();
    assert_eq!(members.to_reference(), linked_members.to_reference());

    let rule = Type::named("rule:").unwrap();
    let head = Type::named("concept:")
        .unwrap()
        .map([(Text("name").to_reference(), Text("person").to_reference())]);
    let embedded = rule.map([
        (Text("head").to_reference(), head),
        (Text("when").to_reference(), members.to_reference()),
    ]);
    let linked = rule.map([
        (
            Text("when").to_reference(),
            Reference::from_bytes(*members.to_reference().as_bytes()),
        ),
        (Text("head").to_reference(), head),
    ]);
    assert_eq!(embedded, linked);
}

#[test]
fn a_value_has_one_reference_however_it_was_built() {
    // Maps: entry order does not matter, nor which map type holds them.
    let pairs = [("b", "2"), ("a", "1"), ("c", "3")];
    let tree: BTreeMap<Text, Text> = pairs.iter().map(|(k, v)| (Text(k), Text(v))).collect();
    let hash: HashMap<Text, Text> = pairs.iter().map(|(k, v)| (Text(k), Text(v))).collect();
    let reversed = Type::MAP.map(
        pairs
            .iter()
            .rev()
            .map(|(k, v)| (Text(k).to_reference(), Text(v).to_reference())),
    );
    assert_eq!(tree.to_reference(), hash.to_reference());
    assert_eq!(tree.to_reference(), reversed);

    // Sets: order does not matter, nor duplicates, nor the set type.
    let tree: BTreeSet<Text> = [Text("x"), Text("y"), Text("z")].into();
    let hash: HashSet<Text> = [Text("z"), Text("x"), Text("y")].into();
    let repeated = Type::SET.set(["y", "x", "z", "x"].map(|text| Text(text).to_reference()));
    assert_eq!(tree.to_reference(), hash.to_reference());
    assert_eq!(tree.to_reference(), repeated);
    // A set is not the list of its members.
    let as_list: Vec<Text> = tree.iter().cloned().collect();
    assert_ne!(tree.to_reference(), as_list.to_reference());

    // Lists: a slice, an array, a Vec and an iterator of references agree,
    // and order matters.
    let items = [Text("p"), Text("q")];
    assert_eq!(items.to_reference(), items.to_vec().to_reference());
    assert_eq!(items.to_reference(), items[..].to_reference());
    assert_eq!(
        items.to_reference(),
        Type::LIST.list(items.iter().map(ToReference::to_reference))
    );
    assert_ne!(items.to_reference(), [Text("q"), Text("p")].to_reference());

    // A value behind a pointer.
    let boxed: Box<dyn ToReference> = Box::new(Text("x"));
    assert_eq!(boxed.to_reference(), Text("x").to_reference());
}

#[test]
fn a_list_of_any_length_hashes_as_its_concatenated_parts() {
    // Around the stack buffer (32 parts) and the 64 KiB block (2048 parts).
    for count in [0, 1, 31, 32, 33, 2047, 2048, 2049, 4096, 5000] {
        let parts: Vec<Reference> = (0..count as u32)
            .map(|index| Reference::hash(&index.to_le_bytes()))
            .collect();
        let mut content = Vec::new();
        for part in &parts {
            content.extend_from_slice(part.as_bytes());
        }
        assert_eq!(
            Type::LIST.list(parts.iter().copied()),
            Type::LIST.reference_of(&content),
            "{count} parts"
        );
    }
}
