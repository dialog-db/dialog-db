use crate::attribute::AttributeDescriptor;
use crate::error::TypeError;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A non-empty collection of named attribute descriptors.
///
/// `NamedAttributes` is valid by construction: it cannot hold zero
/// entries. Every public construction path is fallible
/// ([`TryFrom`] for arrays / `Vec` / `HashMap`, and [`Deserialize`])
/// and returns an error on an empty input. A concept's required
/// (`with`) attribute set is a `NamedAttributes`, so this single
/// chokepoint guarantees a concept always declares at least one
/// required attribute — a concept with none would constrain nothing
/// and match every entity.
///
/// Serializes as a JSON map: `{ "field-name": { "the": "domain/name", ... } }`
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct NamedAttributes(Vec<(String, AttributeDescriptor)>);

impl NamedAttributes {
    /// Returns an iterator over all attributes as (name, descriptor) pairs.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &AttributeDescriptor)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns an iterator over attribute names.
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.iter().map(|(k, _)| k.as_str())
    }

    /// Build from pairs whose non-emptiness the caller already
    /// guarantees by other means — the `#[derive(Concept)]` macro
    /// (which emits a compile-time assertion that at least one field
    /// is required) and conversions from an already-validated
    /// descriptor. Internal-only; public construction goes through
    /// the fallible [`TryFrom`] impls.
    pub(crate) fn from_pairs(pairs: Vec<(String, AttributeDescriptor)>) -> Self {
        NamedAttributes(pairs)
    }

    /// Fallible builder shared by the [`TryFrom`] impls: rejects an
    /// empty set with [`TypeError::EmptyConcept`].
    fn try_new(pairs: Vec<(String, AttributeDescriptor)>) -> Result<Self, TypeError> {
        if pairs.is_empty() {
            return Err(TypeError::EmptyConcept);
        }
        Ok(NamedAttributes(pairs))
    }
}

impl Serialize for NamedAttributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let map: HashMap<&str, &AttributeDescriptor> = self.iter().collect();
        map.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NamedAttributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = HashMap::<String, AttributeDescriptor>::deserialize(deserializer)?;
        if map.is_empty() {
            // Non-empty by construction: a concept's required (`with`)
            // attribute set must have at least one entry, otherwise
            // the concept constrains nothing and every entity matches.
            return Err(D::Error::invalid_length(0, &"at least one attribute"));
        }
        Ok(NamedAttributes(map.into_iter().collect()))
    }
}

impl<const N: usize> TryFrom<[(&str, AttributeDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(&str, AttributeDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(
            arr.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl<const N: usize> TryFrom<[(String, AttributeDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(String, AttributeDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(arr.into_iter().collect())
    }
}

impl TryFrom<Vec<(&str, AttributeDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(&str, AttributeDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(
            vec.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl TryFrom<Vec<(String, AttributeDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(String, AttributeDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(vec)
    }
}

impl TryFrom<HashMap<String, AttributeDescriptor>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(map: HashMap<String, AttributeDescriptor>) -> Result<Self, Self::Error> {
        Self::try_new(map.into_iter().collect())
    }
}
