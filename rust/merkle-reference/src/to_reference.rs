use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::BuildHasher,
    rc::Rc,
    sync::Arc,
};

use crate::{Reference, Type};

/// A value with a reference.
///
/// The crate implements it for the shapes, generic over their parts: slices,
/// arrays and `Vec`s are lists, `BTreeMap`s and `HashMap`s are maps, and
/// `BTreeSet`s and `HashSet`s are sets. [`Reference`] implements it as
/// itself, which is how a part stored apart goes into its parent: by its
/// reference, with the same result as the part.
///
/// The crate defines no scalar types, so it implements this trait for no
/// scalar Rust type. A user names its own types and implements this trait
/// for the types it owns, through [`Type::reference_of`] for a scalar and
/// [`Type::list`], [`Type::map`] or [`Type::set`] for a composite. Collections
/// of those types then reference as lists, maps and sets. A scalar Rust type
/// the user does not own, such as `str`, takes a newtype, or its type's
/// [`Type::reference_of`] directly.
///
/// ```no_run
/// use std::sync::LazyLock;
/// use merkle_reference::{Reference, ToReference, Type};
///
/// static TEXT: LazyLock<Type> = LazyLock::new(|| Type::named("text:").unwrap());
///
/// struct Text(String);
///
/// impl ToReference for Text {
///     fn to_reference(&self) -> Reference {
///         TEXT.reference_of(self.0.as_bytes())
///     }
/// }
///
/// let names = vec![Text("Alice".into()), Text("Bob".into())];
/// let reference = names.to_reference(); // a list of two texts
/// # let _ = reference;
/// ```
pub trait ToReference {
    /// The value's reference.
    fn to_reference(&self) -> Reference;
}

impl ToReference for Reference {
    fn to_reference(&self) -> Reference {
        *self
    }
}

impl<T: ToReference> ToReference for [T] {
    fn to_reference(&self) -> Reference {
        Type::LIST.list(self.iter().map(ToReference::to_reference))
    }
}

impl<T: ToReference, const N: usize> ToReference for [T; N] {
    fn to_reference(&self) -> Reference {
        self.as_slice().to_reference()
    }
}

impl<T: ToReference> ToReference for Vec<T> {
    fn to_reference(&self) -> Reference {
        self.as_slice().to_reference()
    }
}

impl<K: ToReference, V: ToReference> ToReference for BTreeMap<K, V> {
    fn to_reference(&self) -> Reference {
        Type::MAP.map(
            self.iter()
                .map(|(key, value)| (key.to_reference(), value.to_reference())),
        )
    }
}

impl<K: ToReference, V: ToReference, S: BuildHasher> ToReference for HashMap<K, V, S> {
    fn to_reference(&self) -> Reference {
        Type::MAP.map(
            self.iter()
                .map(|(key, value)| (key.to_reference(), value.to_reference())),
        )
    }
}

impl<T: ToReference> ToReference for BTreeSet<T> {
    fn to_reference(&self) -> Reference {
        Type::SET.set(self.iter().map(ToReference::to_reference))
    }
}

impl<T: ToReference, S: BuildHasher> ToReference for HashSet<T, S> {
    fn to_reference(&self) -> Reference {
        Type::SET.set(self.iter().map(ToReference::to_reference))
    }
}

impl<T: ToReference + ?Sized> ToReference for &T {
    fn to_reference(&self) -> Reference {
        (**self).to_reference()
    }
}

impl<T: ToReference + ?Sized> ToReference for Box<T> {
    fn to_reference(&self) -> Reference {
        (**self).to_reference()
    }
}

impl<T: ToReference + ?Sized> ToReference for Rc<T> {
    fn to_reference(&self) -> Reference {
        (**self).to_reference()
    }
}

impl<T: ToReference + ?Sized> ToReference for Arc<T> {
    fn to_reference(&self) -> Reference {
        (**self).to_reference()
    }
}
