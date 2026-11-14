//! Read-side claim type representing a stored EAV datum.

use crate::attribute::Relation;
use crate::concept::Conclusion;
pub use crate::corpus::{Cause, CorpusRelation, Entity, Fact, Value};
use serde::{Deserialize, Serialize};

/// A claim represents a stored EAV datum with full metadata.
///
/// This is the result type for relation queries. It carries the attribute
/// identifier alongside the entity-value-cause data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Claim {
    /// The claim identifier (e.g., "user/name")
    pub the: Relation,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
    /// The cause (provenance hash) of this claim
    pub cause: Cause,
    /// The layer the claim was read from (`in:`), when the read was over
    /// several layers and the row is not the primary's; `None` is the
    /// primary, `memory:`.
    #[serde(rename = "in", default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<Entity>,
}

impl Claim {
    /// The layer this claim was read from; `None` is the primary,
    /// `memory:`.
    pub fn layer(&self) -> Option<&Entity> {
        self.layer.as_ref()
    }

    /// Get the attribute for this claim
    pub fn the(&self) -> &Relation {
        &self.the
    }

    /// Get the domain of this claim's attribute
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Get the name of this claim's attribute
    pub fn name(&self) -> &str {
        self.the.name()
    }

    /// Get the entity of this claim
    pub fn of(&self) -> &Entity {
        &self.of
    }

    /// Get the value of this claim
    pub fn is(&self) -> &Value {
        &self.is
    }

    /// Get the cause (provenance hash) of this claim
    pub fn cause(&self) -> &Cause {
        &self.cause
    }
}

impl Conclusion for Claim {
    /// The claim's subject entity. Lets claim-valued query results
    /// participate in subject-keyed machinery (e.g. per-entity
    /// incremental maintenance) uniformly with concept conclusions.
    fn this(&self) -> &Entity {
        &self.of
    }
}

impl From<Fact> for Claim {
    fn from(fact: Fact) -> Self {
        Claim {
            the: Relation::from(fact.the),
            of: fact.of,
            is: fact.is,
            cause: fact.cause.unwrap_or(Cause([0; 32])),
            layer: None,
        }
    }
}

impl From<&Fact> for Claim {
    fn from(fact: &Fact) -> Self {
        Claim {
            the: Relation::from(fact.the.clone()),
            of: fact.of.clone(),
            is: fact.is.clone(),
            cause: fact.cause.clone().unwrap_or(Cause([0; 32])),
            layer: None,
        }
    }
}
