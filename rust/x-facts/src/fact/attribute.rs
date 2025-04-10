use std::str::FromStr;

use crate::{ATTRIBUTE_LENGTH, RawAttribute, XFactsError};

/// An [`Attribute`] is the predicate part of a semantic triple. [`Attribute`]s
/// in this crate may be a maximum of 64 bytes, and must be formated as
/// "namespace/predicate". The namespace part of an attribute is required.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Attribute(RawAttribute, [u8; ATTRIBUTE_LENGTH]);

impl Attribute {
    /// A byte representation of this attribute in a format that is suitable
    /// for use within a [`KeyType`].
    pub fn key_bytes(&self) -> &[u8; ATTRIBUTE_LENGTH] {
        &self.1
    }
}

impl TryFrom<String> for Attribute {
    type Error = XFactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.as_bytes().len() > ATTRIBUTE_LENGTH {
            return Err(XFactsError::InvalidAttribute(format!(
                "Attribute \"{value}\" is too long (must be no longer than {} bytes)",
                ATTRIBUTE_LENGTH
            )));
        }

        // TODO: Decide if we want to enforce this
        let Some((_namespace, _predicate)) = value.split_once('/') else {
            return Err(XFactsError::InvalidAttribute(format!(
                "Attribute format is \"namespace/predicate\", but got \"{value}\""
            )));
        };

        let mut bytes = [0; ATTRIBUTE_LENGTH];
        bytes[0..value.len()].copy_from_slice(value.as_bytes());

        Ok(Self(value, bytes))
    }
}

impl FromStr for Attribute {
    type Err = XFactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Attribute::try_from(s.to_owned())
    }
}

impl From<Attribute> for String {
    fn from(value: Attribute) -> Self {
        value.0
    }
}
