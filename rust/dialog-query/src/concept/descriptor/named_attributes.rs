use crate::Cardinality;
use crate::artifact::Type;
use crate::attribute::{AttributeDescriptor, The};
use crate::error::TypeError;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `skip_serializing_if` helper: a required field omits the
/// `optional` key on the wire, so a required entry serializes
/// exactly as a bare [`AttributeDescriptor`] did before optionality
/// existed.
fn is_not_optional(optional: &bool) -> bool {
    !*optional
}

/// A single attribute as it participates in a concept: the
/// [`AttributeDescriptor`] plus the concept-membership facts about
/// it (currently just whether it is `optional`).
///
/// Attributes themselves are always required — an attribute is an
/// attribute. Optionality is a property of *how a concept uses* the
/// attribute (`Option<T>` field in a `#[derive(Concept)]` struct),
/// so it lives here, at the concept-field layer, not on
/// [`AttributeDescriptor`].
///
/// On the wire the descriptor is flattened, so a field serializes as
/// a single object carrying the attribute fields plus an optional
/// `"optional": true`:
///
/// ```json
/// { "the": "person/nickname", "as": "Text", "optional": true }
/// ```
///
/// A required field omits `optional`, so it is byte-identical to the
/// pre-optionality encoding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConceptFieldDescriptor {
    #[serde(flatten)]
    descriptor: AttributeDescriptor,
    /// Whether this field is optional (set-widened) within the
    /// concept: a missing fact yields an `Absent` row instead of
    /// dropping the row. Required fields (the default) omit this on
    /// the wire.
    #[serde(default, skip_serializing_if = "is_not_optional")]
    optional: bool,
}

impl ConceptFieldDescriptor {
    /// A required concept field wrapping the given attribute descriptor.
    pub fn required(descriptor: AttributeDescriptor) -> Self {
        Self {
            descriptor,
            optional: false,
        }
    }

    /// An optional (set-widened) concept field wrapping the given
    /// attribute descriptor.
    pub fn optional(descriptor: AttributeDescriptor) -> Self {
        Self {
            descriptor,
            optional: true,
        }
    }

    /// The underlying attribute descriptor.
    pub fn descriptor(&self) -> &AttributeDescriptor {
        &self.descriptor
    }

    /// Returns `true` iff this field is optional (set-widened).
    pub fn is_optional(&self) -> bool {
        self.optional
    }

    /// Convenience: the attribute's relation identifier.
    pub fn the(&self) -> &The {
        self.descriptor.the()
    }

    /// Convenience: the attribute's domain.
    pub fn domain(&self) -> &str {
        self.descriptor.domain()
    }

    /// Convenience: the attribute's name.
    pub fn name(&self) -> &str {
        self.descriptor.name()
    }

    /// Convenience: the attribute's content type, if known.
    pub fn content_type(&self) -> Option<Type> {
        self.descriptor.content_type()
    }

    /// Convenience: the attribute's cardinality.
    pub fn cardinality(&self) -> Cardinality {
        self.descriptor.cardinality()
    }

    /// Convenience: the attribute's description.
    pub fn description(&self) -> &str {
        self.descriptor.description()
    }

    /// Convenience: the attribute's content-addressed URI.
    pub fn to_uri(&self) -> String {
        self.descriptor.to_uri()
    }

    /// Convenience: cost estimate for scanning this attribute. See
    /// [`AttributeDescriptor::estimate`].
    pub fn estimate(&self, of: bool, is: bool) -> usize {
        self.descriptor.estimate(of, is)
    }
}

impl From<AttributeDescriptor> for ConceptFieldDescriptor {
    /// A bare attribute descriptor becomes a *required* concept field.
    fn from(descriptor: AttributeDescriptor) -> Self {
        Self::required(descriptor)
    }
}

/// A non-empty collection of named concept fields.
///
/// `NamedAttributes` maps a field name to a [`ConceptFieldDescriptor`]
/// (an attribute plus its concept-membership facts), stored in a
/// [`BTreeMap`] so iteration and serialization are deterministic
/// (sorted by field name) and field names are de-duplicated.
///
/// It is valid by construction: it cannot hold zero entries, and at
/// least one entry must be required. Every public construction path
/// is fallible ([`TryFrom`] for arrays / `Vec` / `HashMap`, and
/// [`Deserialize`]) and errors otherwise — a concept with no required
/// attribute would constrain nothing and match every entity.
///
/// Serializes as a JSON map: `{ "field-name": { "the": "domain/name", ... } }`.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct NamedAttributes(BTreeMap<String, ConceptFieldDescriptor>);

impl NamedAttributes {
    /// Returns an iterator over all fields as (name, field) pairs,
    /// sorted by field name.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &ConceptFieldDescriptor)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns an iterator over field names, sorted.
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.keys().map(String::as_str)
    }

    /// Fallible builder shared by the [`TryFrom`] impls: rejects an
    /// empty set, or a set with no required field, with
    /// [`TypeError::EmptyConcept`].
    fn try_new(map: BTreeMap<String, ConceptFieldDescriptor>) -> Result<Self, TypeError> {
        if map.is_empty() || map.values().all(|field| field.is_optional()) {
            return Err(TypeError::EmptyConcept);
        }
        Ok(NamedAttributes(map))
    }
}

impl Serialize for NamedAttributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NamedAttributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = BTreeMap::<String, ConceptFieldDescriptor>::deserialize(deserializer)?;
        // Reuse the same validity check as every other construction
        // path rather than re-implementing it: at least one entry,
        // at least one of them required.
        NamedAttributes::try_from(map).map_err(D::Error::custom)
    }
}

impl<const N: usize> TryFrom<[(&str, ConceptFieldDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(&str, ConceptFieldDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(
            arr.into_iter()
                .map(|(name, field)| (name.to_string(), field))
                .collect(),
        )
    }
}

impl<const N: usize> TryFrom<[(String, ConceptFieldDescriptor); N]> for NamedAttributes {
    type Error = TypeError;

    fn try_from(arr: [(String, ConceptFieldDescriptor); N]) -> Result<Self, Self::Error> {
        Self::try_new(arr.into_iter().collect())
    }
}

impl TryFrom<Vec<(&str, ConceptFieldDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(&str, ConceptFieldDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(
            vec.into_iter()
                .map(|(name, field)| (name.to_string(), field))
                .collect(),
        )
    }
}

impl TryFrom<Vec<(String, ConceptFieldDescriptor)>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(vec: Vec<(String, ConceptFieldDescriptor)>) -> Result<Self, Self::Error> {
        Self::try_new(vec.into_iter().collect())
    }
}

impl TryFrom<BTreeMap<String, ConceptFieldDescriptor>> for NamedAttributes {
    type Error = TypeError;

    fn try_from(map: BTreeMap<String, ConceptFieldDescriptor>) -> Result<Self, Self::Error> {
        Self::try_new(map)
    }
}
