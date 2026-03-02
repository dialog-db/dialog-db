use crate::attribute::AttributeDescriptor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A non-empty collection of named attribute descriptors.
///
/// This is a transparent wrapper around `Vec<(String, AttributeDescriptor)>` that
/// enforces non-emptiness — you cannot create a `NamedAttributes` with zero entries.
///
/// Serializes as a JSON map: `{ "fieldName": { "the": "domain/name", ... } }`
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
        Ok(NamedAttributes::from(map))
    }
}

impl<const N: usize> From<[(&str, AttributeDescriptor); N]> for NamedAttributes {
    fn from(arr: [(&str, AttributeDescriptor); N]) -> Self {
        NamedAttributes(
            arr.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl<const N: usize> From<[(String, AttributeDescriptor); N]> for NamedAttributes {
    fn from(arr: [(String, AttributeDescriptor); N]) -> Self {
        NamedAttributes(arr.into_iter().collect())
    }
}

impl From<Vec<(&str, AttributeDescriptor)>> for NamedAttributes {
    fn from(vec: Vec<(&str, AttributeDescriptor)>) -> Self {
        NamedAttributes(
            vec.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl From<Vec<(String, AttributeDescriptor)>> for NamedAttributes {
    fn from(vec: Vec<(String, AttributeDescriptor)>) -> Self {
        NamedAttributes(vec)
    }
}

impl From<HashMap<String, AttributeDescriptor>> for NamedAttributes {
    fn from(map: HashMap<String, AttributeDescriptor>) -> Self {
        NamedAttributes(map.into_iter().collect())
    }
}
