//! [`Domain`] and [`AttributeName`] — the two halves of an [`Attribute`].
//!
//! An [`Attribute`] is formatted as `domain/name`. This module defines the
//! halves as separate validated newtypes so callers (especially
//! [`ArtifactSelector`](crate::ArtifactSelector)) can constrain queries by
//! domain alone — enabling a contiguous prefix scan over the attribute slot
//! of an index key — without committing to a specific name.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError};

/// The domain half of an [`Attribute`].
///
/// A non-empty string containing no `/`, no longer than the attribute slot
/// (with room left for `/` and at least one byte of name).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Domain(String);

impl Domain {
    /// The string value of this domain.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The bytes that form the prefix of an attribute key for this domain,
    /// including the trailing `/` separator.
    ///
    /// Concretely, `Domain("foo").key_prefix_bytes()` returns `b"foo/"`. The
    /// trailing `/` ensures the prefix is unambiguous: it will not match
    /// attributes whose domain merely starts with `"foo"` (e.g. `"foo-bar"`).
    pub fn key_prefix_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.0.len() + 1);
        v.extend_from_slice(self.0.as_bytes());
        v.push(b'/');
        v
    }
}

impl TryFrom<String> for Domain {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(DialogArtifactsError::InvalidAttribute(
                "Domain must not be empty".into(),
            ));
        }
        if value.contains('/') {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Domain must not contain '/', got \"{value}\""
            )));
        }
        // Domain bytes plus a '/' plus at least one name byte must fit in the slot.
        if value.len() + 2 > ATTRIBUTE_LENGTH {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Domain \"{value}\" leaves no room for '/' and a name within {ATTRIBUTE_LENGTH} bytes"
            )));
        }
        Ok(Self(value))
    }
}

impl FromStr for Domain {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Domain::try_from(s.to_owned())
    }
}

impl From<Domain> for String {
    fn from(value: Domain) -> Self {
        value.0
    }
}

impl From<&Domain> for String {
    fn from(value: &Domain) -> Self {
        value.0.clone()
    }
}

impl Display for Domain {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

/// The name half of an [`Attribute`].
///
/// A non-empty string containing no `/`. Combined with a [`Domain`] via
/// [`Attribute::from_parts`](crate::Attribute::from_parts) it forms a full
/// attribute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct AttributeName(String);

impl AttributeName {
    /// The string value of this attribute name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AttributeName {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(DialogArtifactsError::InvalidAttribute(
                "Attribute name must not be empty".into(),
            ));
        }
        if value.contains('/') {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute name must not contain '/', got \"{value}\""
            )));
        }
        if value.len() >= ATTRIBUTE_LENGTH {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute name \"{value}\" is too long (must be shorter than {ATTRIBUTE_LENGTH} bytes)"
            )));
        }
        Ok(Self(value))
    }
}

impl FromStr for AttributeName {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AttributeName::try_from(s.to_owned())
    }
}

impl From<AttributeName> for String {
    fn from(value: AttributeName) -> Self {
        value.0
    }
}

impl From<&AttributeName> for String {
    fn from(value: &AttributeName) -> Self {
        value.0.clone()
    }
}

impl Display for AttributeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_a_valid_domain() {
        let d: Domain = "foo".parse().unwrap();
        assert_eq!(d.as_str(), "foo");
        assert_eq!(d.key_prefix_bytes(), b"foo/");
    }

    #[test]
    fn it_parses_a_dotted_domain() {
        let d: Domain = "dialog.concept.with".parse().unwrap();
        assert_eq!(d.as_str(), "dialog.concept.with");
        assert_eq!(d.key_prefix_bytes(), b"dialog.concept.with/");
    }

    #[test]
    fn it_rejects_empty_domain() {
        assert!("".parse::<Domain>().is_err());
    }

    #[test]
    fn it_rejects_domain_with_slash() {
        assert!("foo/bar".parse::<Domain>().is_err());
    }

    #[test]
    fn it_rejects_domain_too_long_for_attribute() {
        let long = "a".repeat(ATTRIBUTE_LENGTH);
        assert!(long.parse::<Domain>().is_err());
    }

    #[test]
    fn it_parses_a_valid_attribute_name() {
        let n: AttributeName = "name".parse().unwrap();
        assert_eq!(n.as_str(), "name");
    }

    #[test]
    fn it_rejects_empty_attribute_name() {
        assert!("".parse::<AttributeName>().is_err());
    }

    #[test]
    fn it_rejects_attribute_name_with_slash() {
        assert!("foo/bar".parse::<AttributeName>().is_err());
    }
}
