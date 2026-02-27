//! Read-side relation type representing a query result with full metadata.

/// Relation descriptor for parameter signatures.
pub mod descriptor;
/// Relation application for queries.
pub mod query;
pub use descriptor::RelationDescriptor;
pub use query::RelationQuery;

pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Value};
pub use crate::attribute::Cardinality;
use crate::attribute::The;
use serde::{Deserialize, Serialize};

/// A relation represents a read-side query result with full metadata.
///
/// This is the result type for relation queries. It carries the attribute
/// metadata (the, cardinality) alongside the entity-value data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Relation {
    /// The relation identifier (e.g., "user/name")
    pub the: The,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
    /// The cause (provenance hash) of this relation
    pub cause: Cause,
    /// The cardinality of this attribute
    pub cardinality: Cardinality,
}

impl Relation {
    /// Get the attribute for this relation
    pub fn the(&self) -> Attribute {
        Attribute::from(&self.the)
    }

    /// Get the domain of this relation's attribute
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Get the name of this relation's attribute
    pub fn name(&self) -> &str {
        self.the.name()
    }

    /// Get the entity of this relation
    pub fn of(&self) -> &Entity {
        &self.of
    }

    /// Get the value of this relation
    pub fn is(&self) -> &Value {
        &self.is
    }

    /// Get the cause (provenance hash) of this relation
    pub fn cause(&self) -> &Cause {
        &self.cause
    }
}

impl From<&Artifact> for Relation {
    fn from(artifact: &Artifact) -> Self {
        Relation {
            the: The::from(artifact.the.clone()),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
            cardinality: Cardinality::Many,
        }
    }
}
