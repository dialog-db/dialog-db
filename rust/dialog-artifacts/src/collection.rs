//! Typed keyed views over a domain's claims: [`Directory`] and
//! [`Sequence`].
//!
//! A domain scan ([`crate::ArtifactSelector::with_domain`]) yields
//! every claim whose attribute lives under one domain, ordered by the
//! raw bytes of the attribute's name half. That name half admits two
//! syntactically disjoint shapes (see [`Name`]), and each shape has a
//! matching collection:
//!
//! - [`Directory`] — a [`Symbol`]-keyed dictionary: one entry per
//!   named predicate, iterated in lexicographic name order.
//! - [`Sequence`] — a [`Position`]-keyed ordered relation: one entry
//!   per member, iterated in list order (fractional positions sort by
//!   byte comparison, which is exactly the scan order — see
//!   [`crate::position`]).
//!
//! Because the shapes are disjoint by first byte, a single pass over
//! a domain scan can [`admit`](Directory::admit) each entry into the
//! right collection without a tag:
//!
//! ```no_run
//! # use dialog_artifacts::{Artifact, Directory, Sequence, Value};
//! # fn collect(artifacts: Vec<Artifact>) {
//! let mut fields: Directory<Value> = Directory::new();
//! let mut members: Sequence<Value> = Sequence::new();
//! for artifact in artifacts {
//!     if !fields.admit(&artifact.the, artifact.is.clone()) {
//!         members.admit(&artifact.the, artifact.is);
//!     }
//! }
//! # }
//! ```
//!
//! Both collections hold *values*: the caller scopes the scan (which
//! domain, which entity) and these types classify and order what it
//! returned.

use std::collections::BTreeMap;
use std::collections::btree_map;

use ::serde::{Deserialize, Serialize};

use crate::position::Position;
use crate::{Attribute, Name, Symbol};

/// A [`Symbol`]-keyed dictionary of `T` values: the named entries of
/// one domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Directory<T>(BTreeMap<Symbol, T>);

impl<T> Directory<T> {
    /// Construct an empty directory.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Construct a single-entry directory.
    pub fn entry(key: Symbol, value: T) -> Self {
        let mut map = BTreeMap::new();
        map.insert(key, value);
        Self(map)
    }

    /// Insert a `(key, value)` pair, returning the previous value for
    /// `key` if any.
    pub fn insert(&mut self, key: Symbol, value: T) -> Option<T> {
        self.0.insert(key, value)
    }

    /// The value associated with `key`, if any.
    pub fn get(&self, key: &Symbol) -> Option<&T> {
        self.0.get(key)
    }

    /// Returns `true` iff this directory has no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of entries in this directory.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate over `(key, value)` entries in [`Symbol`] order.
    pub fn iter(&self) -> btree_map::Iter<'_, Symbol, T> {
        self.0.iter()
    }

    /// Admit a scanned claim into this directory: inserts when the
    /// attribute's name half is a [`Symbol`] (an ordinary named
    /// predicate) and reports whether it was admitted. Position-named
    /// entries — an ordered relation's members — belong to a
    /// [`Sequence`] instead, and attributes outside the strict
    /// name vocabulary are not admitted either. The caller scopes the
    /// scan to one domain; this classifies by name shape alone.
    pub fn admit(&mut self, attribute: &Attribute, value: T) -> bool {
        match Name::try_from(attribute.name()) {
            Ok(Name::Symbol(key)) => {
                self.insert(key, value);
                true
            }
            _ => false,
        }
    }
}

impl<T> Default for Directory<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntoIterator for Directory<T> {
    type Item = (Symbol, T);
    type IntoIter = btree_map::IntoIter<Symbol, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Directory<T> {
    type Item = (&'a Symbol, &'a T);
    type IntoIter = btree_map::Iter<'a, Symbol, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> FromIterator<(Symbol, T)> for Directory<T> {
    fn from_iter<I: IntoIterator<Item = (Symbol, T)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A [`Position`]-keyed ordered relation of `T` values: the members
/// of one domain, in list order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence<T>(BTreeMap<Position, T>);

impl<T> Sequence<T> {
    /// Construct an empty sequence.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Insert a member at `position`, returning the member previously
    /// at that position if any.
    pub fn insert(&mut self, position: Position, value: T) -> Option<T> {
        self.0.insert(position, value)
    }

    /// The member at `position`, if any.
    pub fn get(&self, position: &Position) -> Option<&T> {
        self.0.get(position)
    }

    /// Returns `true` iff this sequence has no members.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of members in this sequence.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate over `(position, value)` entries in list order.
    pub fn iter(&self) -> btree_map::Iter<'_, Position, T> {
        self.0.iter()
    }

    /// Iterate over the members in list order.
    pub fn values(&self) -> btree_map::Values<'_, Position, T> {
        self.0.values()
    }

    /// The first member's position — the `..first` bound for a
    /// prepend (see [`crate::position::insert`]).
    pub fn first_position(&self) -> Option<&Position> {
        self.0.keys().next()
    }

    /// The last member's position — the `last..` bound for an append.
    pub fn last_position(&self) -> Option<&Position> {
        self.0.keys().next_back()
    }

    /// Admit a scanned claim into this sequence: inserts when the
    /// attribute's name half is a fractional [`Position`] (an ordered
    /// relation's member) and reports whether it was admitted.
    /// Symbol-named entries belong to a [`Directory`] instead. The
    /// caller scopes the scan to one domain; this classifies by name
    /// shape alone.
    pub fn admit(&mut self, attribute: &Attribute, value: T) -> bool {
        match Name::try_from(attribute.name()) {
            Ok(Name::Position(position)) => {
                self.insert(position, value);
                true
            }
            _ => false,
        }
    }
}

impl<T> Default for Sequence<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IntoIterator for Sequence<T> {
    type Item = (Position, T);
    type IntoIter = btree_map::IntoIter<Position, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Sequence<T> {
    type Item = (&'a Position, &'a T);
    type IntoIter = btree_map::Iter<'a, Position, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> FromIterator<(Position, T)> for Sequence<T> {
    fn from_iter<I: IntoIterator<Item = (Position, T)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::str::FromStr;

    use super::*;
    use crate::Value;
    use crate::position::{Bias, insert};

    /// One pass over a mixed domain classifies every entry: symbol
    /// names into the directory, positions into the sequence, and
    /// nothing into both.
    #[dialog_common::test]
    fn it_classifies_a_mixed_domain_scan() {
        let domain: Symbol = "todo.list".parse().unwrap();
        let title = Attribute::compose(&domain, Symbol::from_str("title").unwrap()).unwrap();
        let first = insert(&Bias::derive(b"milk"), ..).unwrap();
        let second = insert(&Bias::derive(b"bread"), &first..).unwrap();
        let head = Attribute::compose(&domain, first.clone()).unwrap();
        let tail = Attribute::compose(&domain, second.clone()).unwrap();

        let scan = [
            (title.clone(), Value::String("Groceries".into())),
            (head, Value::String("milk".into())),
            (tail, Value::String("bread".into())),
        ];

        let mut fields: Directory<Value> = Directory::new();
        let mut members: Sequence<Value> = Sequence::new();
        for (attribute, value) in scan {
            let named = fields.admit(&attribute, value.clone());
            let ordered = members.admit(&attribute, value);
            assert!(named != ordered, "each entry lands in exactly one");
        }

        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields.get(&"title".parse().unwrap()),
            Some(&Value::String("Groceries".into()))
        );
        assert_eq!(members.len(), 2);
        let ordered: Vec<&Value> = members.values().collect();
        assert_eq!(
            ordered,
            [
                &Value::String("milk".into()),
                &Value::String("bread".into())
            ],
            "members iterate in list order"
        );
        assert_eq!(members.first_position(), Some(&first));
        assert_eq!(members.last_position(), Some(&second));
    }

    /// Legacy attributes outside the strict name vocabulary are
    /// admitted by neither collection.
    #[dialog_common::test]
    fn it_declines_nonconforming_names() {
        let legacy = Attribute::from_str("person/display_name").unwrap();
        let mut fields: Directory<Value> = Directory::new();
        let mut members: Sequence<Value> = Sequence::new();
        assert!(!fields.admit(&legacy, Value::Boolean(true)));
        assert!(!members.admit(&legacy, Value::Boolean(true)));
    }

    /// Directories iterate in symbol order and round-trip serde.
    #[dialog_common::test]
    fn it_orders_and_serializes_directory_entries() {
        let mut directory: Directory<u32> = Directory::new();
        directory.insert("bravo".parse().unwrap(), 2);
        directory.insert("alpha".parse().unwrap(), 1);
        let keys: Vec<&Symbol> = directory.iter().map(|(key, _)| key).collect();
        assert_eq!(
            keys.iter().map(|key| key.as_str()).collect::<Vec<_>>(),
            ["alpha", "bravo"]
        );

        let json = serde_json::to_string(&directory).unwrap();
        assert_eq!(json, r#"{"alpha":1,"bravo":2}"#);
        let restored: Directory<u32> = serde_json::from_str(&json).unwrap();
        assert_eq!(directory, restored);
    }
}
