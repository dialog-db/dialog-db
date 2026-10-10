#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), deny(clippy::panic))]

//! Merkle references, version 2: an identifier that is a function of a value,
//! the same whichever of the value's parts are stored inline and whichever are
//! stored apart and linked by their reference.
//!
//! A value is bytes read under a [`Type`]. Plain bytes are referenced by their
//! BLAKE3 hash. A value of any other type is referenced by the BLAKE3 keyed
//! hash of its content, with the type's own reference as the key, so the same
//! bytes under two types give two unrelated references. A composite's content
//! is its parts' references, never their bytes, which is what makes a part
//! stored inline indistinguishable from a part linked by its reference.
//!
//! The crate defines the core types ([`Type::NULL`], [`Type::BOOLEAN`],
//! [`Type::INTEGER`], [`Type::FLOAT`], [`Type::TEXT`], [`Type::LIST`] and
//! [`Type::MAP`]) and nothing else. A user of the crate defines its own types
//! by name with [`Type::named`], and gives their values content with
//! [`Type::refer`], [`Type::list`] or [`Type::map`].
//!
//! ```no_run
//! use merkle_reference::{Refer, Reference, Type};
//!
//! // A user-defined scalar type: content is whatever bytes the type defines.
//! let entity = Type::named("entity:");
//! let alice = entity.refer(b"did:key:alice");
//!
//! // The same bytes as text have another reference.
//! assert_ne!(alice, "did:key:alice".refer());
//!
//! // A user-defined struct: a map from field names to values.
//! let person = Type::named("person:");
//! let name = "Alice".refer();
//! let reference = person.map([("name".refer(), name), ("id".refer(), alice)]);
//! # let _ = reference;
//! ```
//!
//! The format is specified in `spec.md` beside this crate.

mod integer;
mod refer;

pub use integer::{Integer, IntegerError};
pub use refer::{Bytes, Null, Refer};

use std::fmt;

/// The reference of a value: 32 bytes of BLAKE3 output.
///
/// A value's reference does not depend on how the value is stored, only on
/// what it is. A [`Reference`] is also a value in its own right in the sense
/// that it [refers](Refer) to itself: a part known only by its reference
/// contributes the same reference to its parent as the part itself would.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reference([u8; 32]);

impl Reference {
    /// How many bytes a reference has.
    pub const LENGTH: usize = 32;

    /// A reference with the given bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The reference's bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The reference of plain bytes: `BLAKE3(bytes)`.
    ///
    /// Bytes are the one type hashed in BLAKE3's plain mode, so a blob's
    /// reference is its BLAKE3 hash.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

impl From<[u8; 32]> for Reference {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<Reference> for [u8; 32] {
    fn from(reference: Reference) -> Self {
        reference.0
    }
}

impl AsRef<[u8]> for Reference {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Reference({self})")
    }
}

/// A type: an encoding of bytes, identified by its own reference.
///
/// The type is not part of a value's content. It is the key the content is
/// hashed under, so `reference(v : T) = BLAKE3-keyed(reference(T), content)`.
///
/// A type is identified by its name: [`Type::named`] is the reference of the
/// name as a value of [`Type::TYPE`]. The recursion ends at [`Type::TYPE`],
/// whose reference is `BLAKE3-derive_key("merkle-reference v2 type", "")`.
///
/// Deriving a type's reference takes a hash, so a type used often is best kept
/// as a constant: [`Type::from_reference`] with the reference
/// [`Type::named`] gives, pinned by a test.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Type(Reference);

impl Type {
    /// The type of types, whose values are type names.
    pub const TYPE: Self = Self::from_reference(Reference(hex(
        "6c1c13119c7904d1293af5aebc87fdaa284de8c36bc9b0d0e7802cd13dbd6c4e",
    )));
    /// The type of null, whose one value has empty content.
    pub const NULL: Self = Self::from_reference(Reference(hex(
        "63dba2b2bec2ed479d938dd6133eb89f5423b43844b83f39ed208dd3e93fcb02",
    )));
    /// The type of booleans: one byte, `0` or `1`.
    pub const BOOLEAN: Self = Self::from_reference(Reference(hex(
        "7f7f177fd3fcde3f4baabe1e2cc39374fbc6a2efad84a0dfdf53f067945e349b",
    )));
    /// The type of integers of any size (see [`Integer`]).
    pub const INTEGER: Self = Self::from_reference(Reference(hex(
        "fdc9909fb1f4f66fb4a878b4a4279bfc1b63e7504860d39ef0cd05fd692d2626",
    )));
    /// The type of floats: an IEEE 754 binary64, little-endian, bit for bit.
    pub const FLOAT: Self = Self::from_reference(Reference(hex(
        "d620bf8e93b9cc079024a551f4d1bb38e4fe2ea0c3cfe03ed2a8ce456161a60b",
    )));
    /// The type of text: UTF-8, as written.
    pub const TEXT: Self = Self::from_reference(Reference(hex(
        "51531dc865323c8fc012200946b9e63147ba0bdb4175b0d62568c1dedc479b78",
    )));
    /// The type of lists: the items' references, in order.
    pub const LIST: Self = Self::from_reference(Reference(hex(
        "684b01901abbcf0ccade0c97ae922b053f02ae46669632680f8aa51ae85b44e8",
    )));
    /// The type of maps: each entry's key reference then value reference,
    /// entries ordered by key reference.
    pub const MAP: Self = Self::from_reference(Reference(hex(
        "642836d5bd219ed6ad72a31913814819ea3da8f480de93483a7ea5a2821d4d24",
    )));

    /// The type of types' key context, from which [`Type::TYPE`] is derived.
    pub const CONTEXT: &'static str = "merkle-reference v2 type";

    /// The type with the given name.
    pub fn named(name: &str) -> Self {
        Self(Self::TYPE.refer(name.as_bytes()))
    }

    /// The type whose reference is `reference`.
    pub const fn from_reference(reference: Reference) -> Self {
        Self(reference)
    }

    /// The type's own reference, which is the key its values are hashed under.
    pub const fn reference(&self) -> Reference {
        self.0
    }

    /// The reference of a value of this type whose content is `content`.
    pub fn refer(&self, content: &[u8]) -> Reference {
        Reference(*blake3::keyed_hash(self.key(), content).as_bytes())
    }

    /// The reference of a value of this type whose content is `parts`'
    /// references, in order.
    ///
    /// This is the content of a list, and of any type a user defines as a
    /// sequence of parts.
    pub fn list<I>(&self, parts: I) -> Reference
    where
        I: IntoIterator<Item = Reference>,
    {
        let mut hasher = blake3::Hasher::new_keyed(self.key());
        for part in parts {
            hasher.update(part.as_bytes());
        }
        Reference(*hasher.finalize().as_bytes())
    }

    /// The reference of a value of this type whose content is `entries`'
    /// references: each key's reference then its value's, with entries
    /// ordered by key reference.
    ///
    /// This is the content of a map, and of any type a user defines as one,
    /// such as a struct, which is a map from field names to values. The order
    /// `entries` come in does not matter. A map does not hold two entries
    /// with one key; should `entries` do so, they are ordered by value
    /// reference, so the result is still a function of the input.
    pub fn map<I>(&self, entries: I) -> Reference
    where
        I: IntoIterator<Item = (Reference, Reference)>,
    {
        let mut entries: Vec<(Reference, Reference)> = entries.into_iter().collect();
        entries.sort_unstable();
        self.list(entries.into_iter().flat_map(|(key, value)| [key, value]))
    }

    fn key(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Type({})", self.0)
    }
}

/// The reference of `value`.
pub fn refer<T: Refer + ?Sized>(value: &T) -> Reference {
    value.refer()
}

/// Decodes 64 hex digits at compile time.
const fn hex(digits: &str) -> [u8; 32] {
    const fn nibble(digit: u8) -> u8 {
        match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            _ => unreachable!(),
        }
    }
    let digits = digits.as_bytes();
    assert!(digits.len() == 64);
    let mut bytes = [0u8; 32];
    let mut index = 0;
    while index < 32 {
        bytes[index] = nibble(digits[2 * index]) << 4 | nibble(digits[2 * index + 1]);
        index += 1;
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_derives_the_type_of_types_from_its_context() {
        let root = blake3::derive_key(Type::CONTEXT, b"");
        assert_eq!(Type::TYPE.reference(), Reference::from_bytes(root));
    }

    #[test]
    fn it_names_the_core_types() {
        let core = [
            ("null", Type::NULL),
            ("boolean", Type::BOOLEAN),
            ("integer", Type::INTEGER),
            ("float", Type::FLOAT),
            ("text", Type::TEXT),
            ("list", Type::LIST),
            ("map", Type::MAP),
        ];
        for (name, ty) in core {
            assert_eq!(Type::named(name), ty, "{name}");
        }
    }
}
