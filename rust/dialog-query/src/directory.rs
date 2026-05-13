//! [`Directory`] is a [`Symbol`]-keyed collection of values.
//!
//! A directory describes a field that aggregates many entries under a
//! common attribute prefix. Each entry pairs a [`Symbol`] key (the
//! `name` half of a matched attribute) with a value of the inner
//! type `T`.
//!
//! At the query layer each matched row binds *one* entry; aggregation
//! across rows happens at a higher layer (concept realize). At the
//! assertion layer a directory's entries are emitted as one statement
//! per entry.

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::artifact::{Symbol, Type as ValueType};
use crate::type_system;
use crate::types::{Scalar, TypeDescriptor, Typed};

/// A [`Symbol`]-keyed collection of `T` values.
///
/// `T` is constrained to [`Scalar`] for now — the inner type is a
/// primitive value at the storage layer. Richer inner shapes (e.g.
/// nested concepts) will be lifted as separate work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Directory<T>(BTreeMap<Symbol, T>);

impl<T> Directory<T> {
    /// Construct an empty directory.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Construct a single-entry directory. Used by realize-time
    /// construction where a matched row produces one `(key, value)`
    /// pair.
    pub fn entry(key: Symbol, value: T) -> Self {
        let mut map = BTreeMap::new();
        map.insert(key, value);
        Self(map)
    }

    /// Insert a `(key, value)` pair, returning the previous value
    /// for `key` if any.
    pub fn insert(&mut self, key: Symbol, value: T) -> Option<T> {
        self.0.insert(key, value)
    }

    /// Get the value associated with `key`, if any.
    pub fn get(&self, key: &Symbol) -> Option<&T> {
        self.0.get(key)
    }

    /// Returns `true` iff this directory has no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of entries in this directory.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate over `(key, value)` entries in [`Symbol`] order.
    pub fn iter(&self) -> btree_map::Iter<'_, Symbol, T> {
        self.0.iter()
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

/// Descriptor for [`Directory<T>`]. Reports a [`type_system::Type`]
/// kind whose [`Composite::Directory`](type_system::Composite::Directory)
/// inner type matches `T`'s kind.
pub struct DirectoryOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> Clone for DirectoryOf<D> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<D: TypeDescriptor> fmt::Debug for DirectoryOf<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectoryOf").finish()
    }
}

impl<D: TypeDescriptor> Default for DirectoryOf<D> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<D: TypeDescriptor> PartialEq for DirectoryOf<D> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<D: TypeDescriptor> Eq for DirectoryOf<D> {}

impl<D: TypeDescriptor> Hash for DirectoryOf<D> {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl<D: TypeDescriptor> TypeDescriptor for DirectoryOf<D> {
    /// No collapsible legacy storage tag — a directory is a
    /// composite shape, not a single primitive.
    const TYPE: Option<ValueType> = None;

    fn kind(&self) -> Option<type_system::Type> {
        D::default().kind().map(type_system::Type::directory)
    }
}

impl<T: Scalar> Typed for Directory<T> {
    type Descriptor = DirectoryOf<<T as Typed>::Descriptor>;
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::Entity;
    use crate::type_system::{Composite, Type as Kind};

    #[dialog_common::test]
    fn it_constructs_empty_directory() {
        let d: Directory<Entity> = Directory::new();
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);
    }

    #[dialog_common::test]
    fn it_constructs_single_entry_directory() {
        let key: Symbol = "alice".parse().unwrap();
        let value = Entity::new().unwrap();
        let d = Directory::entry(key.clone(), value.clone());
        assert_eq!(d.len(), 1);
        assert_eq!(d.get(&key), Some(&value));
    }

    #[dialog_common::test]
    fn it_inserts_and_iterates_in_symbol_order() {
        let bob: Symbol = "bob".parse().unwrap();
        let alice: Symbol = "alice".parse().unwrap();
        let mut d: Directory<String> = Directory::new();
        d.insert(bob.clone(), "Bob".into());
        d.insert(alice.clone(), "Alice".into());
        let keys: Vec<_> = d.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![alice, bob], "BTreeMap orders by Symbol");
    }

    #[dialog_common::test]
    fn directory_descriptor_reports_directory_kind() {
        let kind = <Directory<Entity> as Typed>::Descriptor::default().kind();
        let expected = Kind::directory(Kind::primitive(ValueType::Entity));
        assert_eq!(kind, Some(expected));
    }

    #[dialog_common::test]
    fn directory_of_string_descriptor_reports_directory_of_text() {
        let kind = <Directory<String> as Typed>::Descriptor::default().kind();
        let composites = kind.as_ref().unwrap().composite_part().unwrap();
        match composites.iter().next().unwrap() {
            Composite::Directory(inner) => {
                assert_eq!(*inner, Kind::primitive(ValueType::String));
            }
            other => panic!("expected Directory, got {other:?}"),
        }
    }

    #[dialog_common::test]
    fn directory_descriptor_round_trips_through_attribute_descriptor() {
        use crate::Cardinality;
        use crate::attribute::AttributeDescriptor;
        use crate::the;

        let descriptor = AttributeDescriptor::with_kind(
            the!("person/favorites"),
            "Favorites by name",
            Cardinality::One,
            <Directory<Entity> as Typed>::Descriptor::default().kind(),
        );
        let expected = Kind::directory(Kind::primitive(ValueType::Entity));
        assert_eq!(descriptor.content_type(), Some(&expected));
        // value_type() returns None because the content type is composite.
        assert_eq!(descriptor.value_type(), None);

        // Serialization round-trips through the shorthand.
        let json: serde_json::Value = serde_json::to_value(&descriptor).unwrap();
        assert_eq!(json["as"], serde_json::json!({ "directory": "Entity" }));
    }
}
