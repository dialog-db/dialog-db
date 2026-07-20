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

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError};

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

    /// The attribute's raw `namespace/predicate` string.
    pub fn as_str(&self) -> &str {
        &self.0
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

        // The variable-length key encoding relies on attributes being NUL-free
        // (`0x00` is the field terminator; see `key::varkey::field`, which
        // returns the raw segment on that premise). An interior NUL would
        // double-escape when a key is re-projected across orderings, writing
        // AEV/VAE keys whose attribute no longer parses.
        if value.as_bytes().contains(&0x00) {
            return Err(DialogArtifactsError::InvalidAttribute(format!(
                "Attribute must not contain a NUL byte: {value:?}"
            )));
        }

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
    #![allow(unexpected_cfgs)]

    use std::str::FromStr;

    use super::Attribute;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// An interior NUL must be rejected at construction: `0x00` terminates
    /// variable-length key fields, and an attribute carrying one corrupts the
    /// AEV/VAE keys projected from the EAV key (the escaped segment would be
    /// re-escaped and no longer parse as UTF-8).
    #[dialog_common::test]
    fn it_rejects_attributes_containing_nul() {
        assert!(Attribute::from_str("a/b\u{0}c").is_err());
        assert!(Attribute::from_str("a\u{0}/bc").is_err());
        assert!(Attribute::from_str("a/bc").is_ok());
    }
}
