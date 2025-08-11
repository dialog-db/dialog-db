//! Attribute types for semantic triple predicates.
//!
//! This module defines the [`Attribute`] type which represents the predicate part
//! of semantic triples. Attributes must follow a namespace/predicate format and
//! are limited to 64 bytes in length.

use std::{fmt::Display, str::FromStr};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError};

/// An [`Attribute`] is the predicate part of a semantic triple. [`Attribute`]s
/// in this crate may be a maximum of 64 bytes, and must be formated as
/// "namespace/predicate". The namespace part of an attribute is required.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}
