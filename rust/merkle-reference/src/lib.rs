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
//! A value is bytes read under a [`Type`]. Raw bytes are referenced by their
//! BLAKE3 hash ([`Reference::hash`]). A value of any other type is referenced
//! by the BLAKE3 keyed hash of its content, keyed by the type's own reference,
//! so the same bytes under two types give two unrelated references. A
//! composite's content is its parts' references, never their bytes, which is
//! what makes a part stored inline indistinguishable from a part linked by its
//! reference.
//!
//! Types are named by URI ([`Type::named`]). The crate defines no scalar types
//! of its own, only the three shapes a composite can take: [`Type::LIST`],
//! [`Type::MAP`] and [`Type::SET`]. A user of the crate names its own types,
//! gives their values content with [`Type::reference_of`], [`Type::list`],
//! [`Type::map`] or [`Type::set`], and implements [`ToReference`] for the
//! types it owns, so that collections of them reference as lists, maps and
//! sets.
//!
//! ```no_run
//! use merkle_reference::{Reference, ToReference, Type};
//!
//! // A scalar type: its content is whatever bytes the type defines.
//! let text = Type::named("text:").unwrap();
//! let alice = text.reference_of(b"Alice");
//!
//! // The same bytes as another type have another reference.
//! let entity = Type::named("entity:").unwrap();
//! assert_ne!(entity.reference_of(b"Alice"), alice);
//!
//! // A struct type: a map from field names to values.
//! let person = Type::named("person:").unwrap();
//! let reference = person.map([(text.reference_of(b"name"), alice)]);
//!
//! // A linked part gives its parent the same reference as the part itself.
//! let people = vec![reference].to_reference();
//! assert_eq!(people, Type::LIST.list([reference]));
//! ```
//!
//! The format is specified in `spec.md` beside this crate.

mod to_reference;

pub use to_reference::ToReference;

use std::{error, fmt};

/// The reference of a value: 32 bytes of BLAKE3 output.
///
/// A value's reference does not depend on how the value is stored, only on
/// what it is. A [`Reference`] is also a value in its own right: as a
/// [`ToReference`] it is itself, which is how a part stored apart goes into
/// its parent.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reference([u8; 32]);

impl Reference {
    /// How many bytes a reference has.
    pub const LENGTH: usize = 32;

    /// The reference whose bytes are `bytes`.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The reference written as 64 lowercase hex digits, as [`Display`]
    /// prints it. Meant for constants: it panics, at compile time in a
    /// `const`, on anything else.
    ///
    /// [`Display`]: fmt::Display
    pub const fn from_hex(digits: &str) -> Self {
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
        Self(bytes)
    }

    /// The reference's bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The reference of raw bytes: `BLAKE3(bytes)`.
    ///
    /// Raw bytes are the one value hashed in BLAKE3's plain mode, so a blob's
    /// reference is the hash every BLAKE3 tool computes for it, and no typed
    /// reference can equal it.
    pub fn hash(bytes: &[u8]) -> Self {
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

/// Why a name is not a type's name: a type is named by a URI, which starts
/// with a scheme (a letter, then letters, digits, `+`, `-` or `.`) and a
/// colon, and holds no whitespace or control characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidTypeName {
    /// The name does not start with a URI scheme and a colon.
    Scheme,
    /// The name holds whitespace or a control character.
    Character,
}

impl fmt::Display for InvalidTypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Scheme => "a type name must be a URI, starting with a scheme and a colon",
            Self::Character => "a type name must hold no whitespace or control characters",
        })
    }
}

impl error::Error for InvalidTypeName {}

/// A type: an encoding of bytes, identified by its own reference.
///
/// The type is not part of a value's content. It is the key the content is
/// hashed under, so `reference(v : T) = BLAKE3-keyed(reference(T), content)`.
///
/// A type is named by a URI, and its reference is that name's reference as a
/// value of [`Type::TYPE`]. The recursion ends at [`Type::TYPE`], whose
/// reference is `BLAKE3-derive_key("merkle-reference v2 type", "")`.
///
/// Deriving a type's reference takes a hash, so a type used often is best kept
/// as a constant: [`Type::from_reference`] with the reference [`Type::named`]
/// gives, pinned by a test, or a lazily initialized static.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Type(Reference);

impl Type {
    /// The type of types, whose values are type names.
    pub const TYPE: Self = Self::from_reference(Reference::from_hex(
        "6c1c13119c7904d1293af5aebc87fdaa284de8c36bc9b0d0e7802cd13dbd6c4e",
    ));
    /// Lists, named `list:`: the items' references, in order.
    pub const LIST: Self = Self::from_reference(Reference::from_hex(
        "23429cd400159e798e011b09c3542e4991daa3b3c3ffed2483122365bdcb65e5",
    ));
    /// Maps, named `map:`: each entry's key reference then value reference,
    /// entries ordered by key reference.
    pub const MAP: Self = Self::from_reference(Reference::from_hex(
        "37fcd51f5d47c860cb7799de8e2f394578f43d2d2ae7cc208f356c61059db149",
    ));
    /// Sets, named `set:`: the members' references, ordered, each once.
    pub const SET: Self = Self::from_reference(Reference::from_hex(
        "07ccc5423568cb7e635819ed3562c9ede113ff165faa9e8c144d3259732456e8",
    ));

    /// The key-derivation context [`Type::TYPE`] is derived from.
    pub const CONTEXT: &'static str = "merkle-reference v2 type";

    /// The type named by the URI `name`.
    pub fn named(name: &str) -> Result<Self, InvalidTypeName> {
        let (scheme, _) = name.split_once(':').ok_or(InvalidTypeName::Scheme)?;
        let mut letters = scheme.bytes();
        let starts_with_letter = letters
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic());
        if !starts_with_letter
            || !letters.all(|byte| byte.is_ascii_alphanumeric() || b"+-.".contains(&byte))
        {
            return Err(InvalidTypeName::Scheme);
        }
        if name
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(InvalidTypeName::Character);
        }
        Ok(Self(Self::TYPE.reference_of(name.as_bytes())))
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
    pub fn reference_of(&self, content: &[u8]) -> Reference {
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
        // BLAKE3 is fastest given whole chunks at once: it hashes several
        // 1 KiB chunks per SIMD pass. Feeding it one 32-byte part at a time
        // was 1.7 times slower at 100 parts and over 4 times slower at a
        // thousand, so parts are gathered first: on the stack up to one
        // chunk, then in a buffer handed over in large chunk-aligned blocks.
        const STACK: usize = 1024;
        const BLOCK: usize = 64 * 1024;
        let mut stack = [0u8; STACK];
        let mut length = 0;
        let mut spill: Vec<u8> = Vec::new();
        let mut spilled = false;
        let mut hasher: Option<blake3::Hasher> = None;
        for part in parts {
            if !spilled {
                if length < STACK {
                    stack[length..length + Reference::LENGTH].copy_from_slice(part.as_bytes());
                    length += Reference::LENGTH;
                    continue;
                }
                spilled = true;
                spill.reserve(BLOCK);
                spill.extend_from_slice(&stack);
            }
            spill.extend_from_slice(part.as_bytes());
            if spill.len() == BLOCK {
                hasher
                    .get_or_insert_with(|| blake3::Hasher::new_keyed(self.key()))
                    .update(&spill);
                spill.clear();
            }
        }
        let rest: &[u8] = if spilled { &spill } else { &stack[..length] };
        let hash = match hasher {
            Some(mut hasher) => {
                hasher.update(rest);
                hasher.finalize()
            }
            None => blake3::keyed_hash(self.key(), rest),
        };
        Reference(*hash.as_bytes())
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

    /// The reference of a value of this type whose content is `members`'
    /// references, ordered, each once.
    ///
    /// This is the content of a set, and of any type a user defines as an
    /// unordered collection of distinct parts. The order `members` come in
    /// does not matter, and a member given twice counts once.
    pub fn set<I>(&self, members: I) -> Reference
    where
        I: IntoIterator<Item = Reference>,
    {
        let mut members: Vec<Reference> = members.into_iter().collect();
        members.sort_unstable();
        members.dedup();
        self.list(members)
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

#[cfg(test)]
mod tests {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_derives_the_type_of_types_from_its_context() {
        let root = blake3::derive_key(Type::CONTEXT, b"");
        assert_eq!(Type::TYPE.reference(), Reference::from_bytes(root));
    }

    #[dialog_common::test]
    fn it_names_the_shapes() {
        for (name, ty) in [
            ("list:", Type::LIST),
            ("map:", Type::MAP),
            ("set:", Type::SET),
        ] {
            assert_eq!(Type::named(name), Ok(ty), "{name}");
        }
    }

    #[dialog_common::test]
    fn it_names_types_by_uri() {
        for name in [
            "text:",
            "https://example.com/person",
            "urn:isbn:0451450523",
            "a+b.c-d:x",
        ] {
            assert!(Type::named(name).is_ok(), "{name}");
        }
        for name in ["", "text", ":text", "1text:", "te xt:", "-x:"] {
            assert_eq!(Type::named(name), Err(InvalidTypeName::Scheme), "{name}");
        }
        assert_eq!(Type::named("text: x"), Err(InvalidTypeName::Character));
        assert_eq!(Type::named("text:\n"), Err(InvalidTypeName::Character));
    }
}
