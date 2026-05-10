//! Attribute types for semantic triple predicates.
//!
//! This module defines the [`Attribute`] type which represents the predicate part
//! of semantic triples. Attributes must follow a namespace/predicate format and
//! are limited to 64 bytes in length.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, AttributeName, DialogArtifactsError, Domain};

/// An [`Attribute`] is the predicate part of a semantic triple. [`Attribute`]s
/// in this crate may be a maximum of 64 bytes, and must be formated as
/// "namespace/predicate". The namespace part of an attribute is required.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Attribute(String, [u8; ATTRIBUTE_LENGTH]);

impl Attribute {
    /// Returns a byte representation of this attribute suitable for use within a key.
    ///
    /// The returned byte array is used for indexing and comparison operations
    /// within the prolly tree structure.
    pub fn key_bytes(&self) -> &[u8; ATTRIBUTE_LENGTH] {
        &self.1
    }

    /// The domain part of this attribute (the substring before the `/`).
    pub fn domain(&self) -> &str {
        // Construction validates the `/`, so unwrap is safe.
        self.0
            .split_once('/')
            .map(|(d, _)| d)
            .expect("Attribute always contains '/'")
    }

    /// The name part of this attribute (the substring after the `/`).
    pub fn name(&self) -> &str {
        self.0
            .split_once('/')
            .map(|(_, n)| n)
            .expect("Attribute always contains '/'")
    }

    /// Splits this attribute into its [`Domain`] and [`AttributeName`] halves.
    ///
    /// Always succeeds — the same validation that produced this `Attribute`
    /// guarantees both halves are well-formed.
    pub fn split(&self) -> (Domain, AttributeName) {
        let domain = Domain::try_from(self.domain().to_owned())
            .expect("Attribute domain is valid by construction");
        let name = AttributeName::try_from(self.name().to_owned())
            .expect("Attribute name is valid by construction");
        (domain, name)
    }

    /// Joins a [`Domain`] and an [`AttributeName`] into an [`Attribute`].
    ///
    /// Returns `Err` if the joined `domain/name` exceeds the attribute slot
    /// length budget.
    pub fn from_parts(domain: &Domain, name: &AttributeName) -> Result<Self, DialogArtifactsError> {
        let mut joined = String::with_capacity(domain.as_str().len() + 1 + name.as_str().len());
        joined.push_str(domain.as_str());
        joined.push('/');
        joined.push_str(name.as_str());
        Self::try_from(joined)
    }
}

impl TryFrom<String> for Attribute {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > ATTRIBUTE_LENGTH {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute \"{value}\" is too long (must be no longer than {} bytes)",
                ATTRIBUTE_LENGTH
            )));
        }

        // TODO: Decide if we want to enforce this
        let Some((_namespace, _predicate)) = value.split_once('/') else {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"namespace/predicate\", but got \"{value}\""
            )));
        };

        let mut bytes = [0; ATTRIBUTE_LENGTH];
        bytes[0..value.len()].copy_from_slice(value.as_bytes());

        Ok(Self(value, bytes))
    }
}

impl FromStr for Attribute {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO: Switch this and TryFrom<String>
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
        write!(f, "{}", String::from(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_exposes_domain_and_name() {
        let attr: Attribute = "person/name".parse().unwrap();
        assert_eq!(attr.domain(), "person");
        assert_eq!(attr.name(), "name");
    }

    #[test]
    fn it_splits_into_typed_halves() {
        let attr: Attribute = "dialog.concept.with/name".parse().unwrap();
        let (d, n) = attr.split();
        assert_eq!(d.as_str(), "dialog.concept.with");
        assert_eq!(n.as_str(), "name");
    }

    #[test]
    fn it_round_trips_through_from_parts() {
        let attr: Attribute = "person/age".parse().unwrap();
        let (d, n) = attr.split();
        let joined = Attribute::from_parts(&d, &n).unwrap();
        assert_eq!(attr, joined);
    }

    #[test]
    fn it_rejects_oversized_join() {
        let d: Domain = "x".repeat(60).parse().unwrap();
        let n: AttributeName = "y".repeat(10).parse().unwrap();
        // 60 + 1 + 10 = 71 > 64 attribute slot
        assert!(Attribute::from_parts(&d, &n).is_err());
    }
}
