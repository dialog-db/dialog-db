//! Read-side relation type representing a query result with full metadata.

/// Relation descriptor for parameter signatures.
pub mod descriptor;
/// Relation application for queries.
pub mod query;
pub use descriptor::RelationDescriptor;
pub use query::RelationQuery;

pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Value};
pub use crate::attribute::Cardinality;
use serde::{Deserialize, Serialize};

/// A relation represents a read-side query result with full metadata.
///
/// This is the result type for relation queries. It carries the attribute
/// metadata (domain, name, cardinality) alongside the entity-value data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Relation {
    /// The domain of the attribute (e.g., "user")
    pub domain: String,
    /// The name of the attribute within the domain (e.g., "name")
    pub name: String,
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
    /// Get the combined attribute string (e.g., "user/name")
    pub fn the(&self) -> Attribute {
        format!("{}/{}", self.domain, self.name)
            .parse()
            .expect("Failed to parse combined attribute")
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
        let attr_str = artifact.the.to_string();
        let (domain, name) = attr_str
            .split_once('/')
            .map(|(ns, n)| (ns.to_string(), n.to_string()))
            .unwrap_or_else(|| (String::new(), attr_str));

        Relation {
            domain,
            name,
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
            cardinality: Cardinality::Many,
        }
    }
}
