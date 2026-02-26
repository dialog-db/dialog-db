pub use crate::artifact::{Attribute, Cause};
pub use crate::error::SchemaError;
pub use crate::relation::application::RelationApplication;
use crate::{Cardinality, Type};
pub use crate::{Parameters, Term};

/// Describes the parameter signature of a relation predicate.
///
/// A relation maps `(Attribute, Entity) -> Value` where the value type may be
/// constrained. This descriptor captures the type-level information about a
/// relation — what kind of value it produces and its cardinality — without
/// binding any specific attribute, entity, or value.
///
/// This is the relation equivalent of [`super::concept::Concept`] (the concept
/// descriptor) which describes a concept's fields and their types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RelationDescriptor {
    /// The expected value type, or `None` if any type is accepted.
    pub content_type: Option<Type>,
    /// Whether this relation allows one or many values per entity.
    pub cardinality: Cardinality,
}

impl RelationDescriptor {
    /// A descriptor accepting any value type with cardinality one.
    pub const ANY: Self = Self {
        content_type: None,
        cardinality: Cardinality::One,
    };

    /// Creates a descriptor with the given value type and cardinality one.
    pub fn typed(content_type: Type) -> Self {
        Self {
            content_type: Some(content_type),
            cardinality: Cardinality::One,
        }
    }

    /// Creates a descriptor with the given value type and cardinality.
    pub fn new(content_type: Option<Type>, cardinality: Cardinality) -> Self {
        Self {
            content_type,
            cardinality,
        }
    }
}
