//! [`Attribute`] is the predicate of a claim.
//!
//! An attribute is a `domain/name` identifier subject to strict
//! validation: both halves must use the [`Symbol`] character set,
//! the joint length must fit in the attribute slot of an index key.
//! Internally the type is opaque — it stores the string form and a
//! cached 64-byte representation for index keying — and exposes its
//! halves through lazy accessors when needed.
//!
//! TODO: switch the delimiter byte from `/` (`0x2F`) to `\0` on the
//! next binary format break.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError, Symbol};

/// Byte used to delimit the domain and name halves of an attribute in
/// the index key slot. Reserved: cannot appear inside a [`Symbol`].
const DELIMITER: u8 = b'/';

/// An [`Attribute`] is the predicate of a claim, in `domain/name` form.
///
/// Validated against the [`Symbol`] rules per half plus the joint
/// length budget at construction time. The internal representation is
/// the canonical string plus a cached 64-byte index-key representation;
/// callers that need the halves use [`Attribute::domain`] and
/// [`Attribute::name`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Attribute(String, [u8; ATTRIBUTE_LENGTH]);

impl Attribute {
    /// Returns a byte representation of this attribute suitable for
    /// use within an index key. Layout is
    /// `domain ++ DELIMITER ++ name`, zero-padded to
    /// [`ATTRIBUTE_LENGTH`].
    pub fn key_bytes(&self) -> &[u8; ATTRIBUTE_LENGTH] {
        &self.1
    }

    /// Returns the domain half of this attribute.
    pub fn domain(&self) -> &str {
        let slash = self
            .0
            .as_bytes()
            .iter()
            .position(|&b| b == DELIMITER)
            .expect("Attribute always contains the delimiter");
        &self.0[..slash]
    }

    /// Returns the name half of this attribute.
    pub fn name(&self) -> &str {
        let slash = self
            .0
            .as_bytes()
            .iter()
            .position(|&b| b == DELIMITER)
            .expect("Attribute always contains the delimiter");
        &self.0[slash + 1..]
    }

    /// Splits this attribute into its domain and name [`Symbol`]
    /// halves. Each half was validated as a [`Symbol`] at construction
    /// time, so re-parsing here is infallible by construction.
    pub fn split(&self) -> (Symbol, Symbol) {
        let domain = Symbol::try_from(self.domain().to_owned())
            .expect("Attribute halves are validated Symbols");
        let name = Symbol::try_from(self.name().to_owned())
            .expect("Attribute halves are validated Symbols");
        (domain, name)
    }
}

impl TryFrom<String> for Attribute {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > ATTRIBUTE_LENGTH {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute \"{value}\" is too long (must be no longer than {ATTRIBUTE_LENGTH} bytes)"
            )));
        }

        let Some((domain_str, name_str)) = value.split_once('/') else {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"domain/name\", but got \"{value}\""
            )));
        };

        Symbol::try_from(domain_str.to_owned())?;
        Symbol::try_from(name_str.to_owned())?;

        let mut bytes = [0u8; ATTRIBUTE_LENGTH];
        bytes[..value.len()].copy_from_slice(value.as_bytes());

        Ok(Self(value, bytes))
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
        value.0
    }
}

impl From<&Attribute> for String {
    fn from(value: &Attribute) -> Self {
        value.0.clone()
    }
}

impl Display for Attribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
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
        assert_eq!(attr.domain(), "person");
        assert_eq!(attr.name(), "name");
    }

    #[dialog_common::test]
    fn it_round_trips_to_string() {
        let attr: Attribute = "dialog.concept.with/name".parse().unwrap();
        assert_eq!(attr.to_string(), "dialog.concept.with/name");
    }

    #[dialog_common::test]
    fn it_rejects_oversized_join() {
        // Symbol max length is ATTRIBUTE_LENGTH - 1 = 63. Two symbols of
        // length 32 plus a delimiter is 65, exceeding the 64-byte budget.
        let oversized = format!("{}/{}", "a".repeat(32), "b".repeat(32));
        assert!(oversized.parse::<Attribute>().is_err());
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
