//! Read-side claim type representing a stored EAV datum.

pub use crate::artifact::{Artifact, ArtifactsAttribute, Cause, Entity, Value};
use crate::attribute::The;
use serde::{Deserialize, Serialize};

/// A claim represents a stored EAV datum with full metadata.
///
/// This is the result type for relation queries. It carries the attribute
/// identifier alongside the entity-value-cause data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Claim {
    /// The claim identifier (e.g., "user/name")
    pub the: The,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
    /// The cause (provenance hash) of this claim
    pub cause: Cause,
}

impl Claim {
    /// Get the attribute for this claim
    pub fn the(&self) -> &The {
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

impl From<&Artifact> for Claim {
    fn from(artifact: &Artifact) -> Self {
        Claim {
            the: The::from(artifact.the.clone()),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
        }
    }
}
