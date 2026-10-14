//! Relations: the predicate part of semantic triples.
//!
//! This module defines the [`Relation`] type which represents the predicate part
//! of semantic triples. Relations must follow a domain/name format and
//! are limited to 64 bytes in length.

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    str::FromStr,
    sync::Arc,
};

use ::serde::{Deserialize, Serialize};

use crate::{DialogArtifactsError, Name, RELATION_LENGTH, Symbol};

/// A [`Relation`] is the predicate part of a semantic triple. [`Relation`]s
/// in this crate may be a maximum of 64 bytes, and must be formated as
/// "domain/name". The domain part of a relation is required.
///
/// A relation is immutable, and is cloned into every claim, key and
/// row that names it, so its name and key bytes are shared rather than
/// copied by each clone.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Relation(Arc<RelationParts>);

/// What a [`Relation`] shares between its clones: its name and the
/// bytes it takes in an index key. Ordered and hashed name first, then
/// key bytes.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RelationParts {
    name: String,
    key: [u8; RELATION_LENGTH],
}

impl Debug for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("Relation")
            .field(&self.0.name)
            .field(&self.0.key)
            .finish()
    }
}

impl Relation {
    /// Returns a byte representation of this relation suitable for use within a key.
    ///
    /// The returned byte array is used for indexing and comparison operations
    /// within the prolly tree structure.
    pub fn key_bytes(&self) -> &[u8; RELATION_LENGTH] {
        &self.0.key
    }

    /// The relation's raw `namespace/predicate` string.
    pub fn as_str(&self) -> &str {
        &self.0.name
    }

    /// The domain half of this relation: everything before the first
    /// `/`. Always present — construction requires the delimiter.
    pub fn domain(&self) -> &str {
        self.0
            .name
            .split_once('/')
            .map(|(domain, _)| domain)
            .unwrap_or(&self.0.name)
    }

    /// The name half of this relation: everything after the first
    /// `/`. Always present — construction requires the delimiter.
    pub fn name(&self) -> &str {
        self.0
            .name
            .split_once('/')
            .map(|(_, name)| name)
            .unwrap_or("")
    }

    /// Split this relation into its typed halves: the domain as a
    /// [`Symbol`] and the name as a [`Name`] (a [`Symbol`] when it
    /// starts lowercase, a fractional position when it starts with an
    /// uppercase major).
    ///
    /// Fallible: relation construction validates only the coarse
    /// shape (`domain/name`, length, no NUL), so relations exist
    /// whose halves do not conform to the stricter [`Symbol`] /
    /// [`Name`] vocabulary — those return an error here rather than
    /// misclassify.
    pub fn split(&self) -> Result<(Symbol, Name), DialogArtifactsError> {
        let domain = Symbol::try_from(self.domain().to_owned())?;
        let name = Name::try_from(self.name())?;
        Ok((domain, name))
    }

    /// Compose a relation from its typed halves. The halves are
    /// individually valid by construction; this checks only the joint
    /// budget (`domain + '/' + name` must fit [`RELATION_LENGTH`]).
    pub fn compose(domain: &Symbol, name: impl Into<Name>) -> Result<Self, DialogArtifactsError> {
        Relation::try_from(format!("{domain}/{}", name.into()))
    }
}

impl TryFrom<String> for Relation {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > RELATION_LENGTH {
            return Err(DialogArtifactsError::InvalidRelation(format!(
                "Relation \"{value}\" is too long (must be no longer than {} bytes)",
                RELATION_LENGTH
            )));
        }

        // TODO: Decide if we want to enforce this
        let Some((_namespace, _predicate)) = value.split_once('/') else {
            return Err(DialogArtifactsError::InvalidRelation(format!(
                "Relation format is \"namespace/predicate\", but got \"{value}\""
            )));
        };

        // The variable-length key encoding relies on relations being NUL-free
        // (`0x00` is the field terminator; see `key::varkey::field`, which
        // returns the raw segment on that premise). An interior NUL would
        // double-escape when a key is re-projected across orderings, writing
        // AEV/VAE keys whose relation no longer parses.
        if value.as_bytes().contains(&0x00) {
            return Err(DialogArtifactsError::InvalidRelation(format!(
                "Relation must not contain a NUL byte: {value:?}"
            )));
        }

        let mut bytes = [0; RELATION_LENGTH];
        bytes[0..value.len()].copy_from_slice(value.as_bytes());

        Ok(Self(Arc::new(RelationParts {
            name: value,
            key: bytes,
        })))
    }
}

impl FromStr for Relation {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO: Switch this and TryFrom<String>
        Relation::try_from(s.to_owned())
    }
}

impl From<Relation> for String {
    fn from(value: Relation) -> Self {
        match Arc::try_unwrap(value.0) {
            Ok(parts) => parts.name,
            Err(shared) => shared.name.clone(),
        }
    }
}

impl From<&Relation> for String {
    fn from(value: &Relation) -> Self {
        value.0.name.clone()
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::str::FromStr;

    use super::Relation;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// An interior NUL must be rejected at construction: `0x00` terminates
    /// variable-length key fields, and a relation carrying one corrupts the
    /// AEV/VAE keys projected from the EAV key (the escaped segment would be
    /// re-escaped and no longer parse as UTF-8).
    #[dialog_common::test]
    fn it_rejects_attributes_containing_nul() {
        assert!(Relation::from_str("a/b\u{0}c").is_err());
        assert!(Relation::from_str("a\u{0}/bc").is_err());
        assert!(Relation::from_str("a/bc").is_ok());
    }

    /// The lazy accessors split at the first delimiter.
    #[dialog_common::test]
    fn it_exposes_domain_and_name_halves() {
        let relation = Relation::from_str("todo.item/title").unwrap();
        assert_eq!(relation.domain(), "todo.item");
        assert_eq!(relation.name(), "title");
    }

    /// `split` classifies the name half by its first byte, and
    /// `compose` rebuilds the same relation from the typed halves.
    #[dialog_common::test]
    fn it_splits_and_composes_typed_halves() {
        use crate::position::{Bias, insert};

        let relation = Relation::from_str("todo.item/title").unwrap();
        let (domain, name) = relation.split().unwrap();
        assert_eq!(domain.as_str(), "todo.item");
        assert_eq!(name.symbol().map(|s| s.as_str()), Some("title"));
        assert_eq!(Relation::compose(&domain, name).unwrap(), relation);

        let position = insert(&Bias::derive(b"member"), ..).unwrap();
        let ordered = Relation::compose(&domain, position.clone()).unwrap();
        assert_eq!(ordered.domain(), "todo.item");
        let (_, name) = ordered.split().unwrap();
        assert_eq!(name.position(), Some(&position));
    }

    /// Relations with halves outside the strict vocabulary (legacy
    /// shapes) still construct, but decline to split.
    #[dialog_common::test]
    fn it_declines_to_split_nonconforming_attributes() {
        let legacy = Relation::from_str("person/display_name").unwrap();
        assert!(legacy.split().is_err());
        let numeric = Relation::from_str("person/1st").unwrap();
        assert!(numeric.split().is_err());
    }
}
