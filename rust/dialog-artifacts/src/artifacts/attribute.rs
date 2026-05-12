//! [`Attribute`] is the predicate of a claim: a pair of [`Symbol`]s.
//!
//! An attribute is structurally a pair of symbols: a `domain` and a
//! `name`. The two are joined for storage in the 64-byte attribute slot of
//! an index key, separated by a delimiter byte.
//!
//! The string form `"domain/name"` is a presentation choice. The
//! delimiter byte is currently `/` (`0x2F`) — pinned for backward
//! compatibility with existing stored data.
//!
//! TODO: switch the delimiter byte from `/` (`0x2F`) to `\0` on the next
//! binary format break.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError, Symbol};

/// Byte used to delimit the domain and name halves of an attribute in
/// the index key slot. Reserved: cannot appear inside a [`Symbol`].
const DELIMITER: u8 = b'/';

/// An [`Attribute`] is the predicate of a claim.
///
/// Structurally a pair of [`Symbol`]s: a domain and a name. The two
/// halves together fit in the 64-byte index slot, separated by the
/// delimiter byte (one byte of overhead).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Attribute {
    domain: Symbol,
    name: Symbol,
    /// Cached 64-byte representation for index keying:
    /// `domain.as_bytes() ++ [DELIMITER] ++ name.as_bytes() ++ zero padding`.
    key_bytes: [u8; ATTRIBUTE_LENGTH],
}

impl Attribute {
    /// Compose two symbols into an attribute.
    ///
    /// Returns `Err` if the joint length exceeds the attribute slot budget
    /// (`domain.len() + 1 + name.len() > ATTRIBUTE_LENGTH`).
    pub fn new(domain: Symbol, name: Symbol) -> Result<Self, DialogArtifactsError> {
        let total = domain.as_bytes().len() + 1 + name.as_bytes().len();
        if total > ATTRIBUTE_LENGTH {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Joint length of \"{domain}/{name}\" is {total} bytes, exceeds {ATTRIBUTE_LENGTH}"
            )));
        }

        let mut key_bytes = [0u8; ATTRIBUTE_LENGTH];
        let ns = domain.as_bytes();
        let nm = name.as_bytes();
        key_bytes[..ns.len()].copy_from_slice(ns);
        key_bytes[ns.len()] = DELIMITER;
        key_bytes[ns.len() + 1..ns.len() + 1 + nm.len()].copy_from_slice(nm);

        Ok(Self {
            domain,
            name,
            key_bytes,
        })
    }

    /// Returns the domain half.
    pub fn domain(&self) -> &Symbol {
        &self.domain
    }

    /// Returns the name half.
    pub fn name(&self) -> &Symbol {
        &self.name
    }

    /// Returns a byte representation of this attribute suitable for use
    /// within an index key. Layout is `domain ++ DELIMITER ++ name`,
    /// zero-padded to [`ATTRIBUTE_LENGTH`].
    pub fn key_bytes(&self) -> &[u8; ATTRIBUTE_LENGTH] {
        &self.key_bytes
    }

    /// Splits this attribute into its domain and name halves.
    pub fn split(&self) -> (Symbol, Symbol) {
        (self.domain.clone(), self.name.clone())
    }

    /// Composes a domain and name (by reference) into an attribute.
    pub fn from_parts(domain: &Symbol, name: &Symbol) -> Result<Self, DialogArtifactsError> {
        Self::new(domain.clone(), name.clone())
    }
}

impl TryFrom<String> for Attribute {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        // String form: "domain/name". Split, validate each half as a
        // Symbol, then compose. Joint-length budget enforced by Attribute::new.
        let Some((ns_str, name_str)) = value.split_once('/') else {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"domain/name\", but got \"{value}\""
            )));
        };

        let domain = Symbol::try_from(ns_str.to_owned())?;
        let name = Symbol::try_from(name_str.to_owned())?;

        Self::new(domain, name)
    }
}

impl FromStr for Attribute {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Attribute::try_from(s.to_owned())
    }
}

impl From<Attribute> for String {
    fn from(value: Attribute) -> Self {
        format!("{}/{}", value.domain, value.name)
    }
}

impl From<&Attribute> for String {
    fn from(value: &Attribute) -> Self {
        format!("{}/{}", value.domain, value.name)
    }
}

impl Display for Attribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}/{}", self.domain, self.name)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_parses_attribute_string() {
        let attr: Attribute = "person/name".parse().unwrap();
        assert_eq!(attr.domain().as_str(), "person");
        assert_eq!(attr.name().as_str(), "name");
    }

    #[dialog_common::test]
    fn it_round_trips_to_string() {
        let attr: Attribute = "dialog.concept.with/name".parse().unwrap();
        assert_eq!(attr.to_string(), "dialog.concept.with/name");
    }

    #[dialog_common::test]
    fn it_splits_into_symbols() {
        let attr: Attribute = "dialog.concept.with/name".parse().unwrap();
        let (ns, nm) = attr.split();
        assert_eq!(ns.as_str(), "dialog.concept.with");
        assert_eq!(nm.as_str(), "name");
    }

    #[dialog_common::test]
    fn it_composes_from_parts() {
        let ns: Symbol = "person".parse().unwrap();
        let nm: Symbol = "age".parse().unwrap();
        let attr = Attribute::from_parts(&ns, &nm).unwrap();
        assert_eq!(attr.to_string(), "person/age");
    }

    #[dialog_common::test]
    fn it_round_trips_via_from_parts() {
        let attr: Attribute = "person/age".parse().unwrap();
        let (ns, nm) = attr.split();
        let rebuilt = Attribute::from_parts(&ns, &nm).unwrap();
        assert_eq!(attr, rebuilt);
    }

    #[dialog_common::test]
    fn it_rejects_oversized_join() {
        // Symbol max length is ATTRIBUTE_LENGTH - 1 = 63. Two symbols of
        // length 32 plus a delimiter is 65, exceeding the 64-byte budget.
        let ns: Symbol = "a".repeat(32).parse().unwrap();
        let nm: Symbol = "b".repeat(32).parse().unwrap();
        assert!(Attribute::from_parts(&ns, &nm).is_err());
    }

    #[dialog_common::test]
    fn it_rejects_string_without_slash() {
        assert!("foobar".parse::<Attribute>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_string_with_invalid_domain() {
        assert!("Foo/bar".parse::<Attribute>().is_err());
        assert!("3foo/bar".parse::<Attribute>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_string_with_invalid_name() {
        assert!("foo/Bar".parse::<Attribute>().is_err());
        assert!("foo/bar-".parse::<Attribute>().is_err());
    }

    #[dialog_common::test]
    fn it_encodes_key_bytes_with_delimiter() {
        let attr: Attribute = "person/name".parse().unwrap();
        let bytes = attr.key_bytes();
        assert_eq!(&bytes[..6], b"person");
        assert_eq!(bytes[6], DELIMITER);
        assert_eq!(&bytes[7..11], b"name");
        // Padding is zero.
        assert!(bytes[11..].iter().all(|&b| b == 0));
    }

    #[dialog_common::test]
    fn it_round_trips_through_serde() {
        let attr: Attribute = "person/age".parse().unwrap();
        let json = serde_json::to_string(&attr).unwrap();
        assert_eq!(json, "\"person/age\"");
        let restored: Attribute = serde_json::from_str(&json).unwrap();
        assert_eq!(attr, restored);
    }
}
