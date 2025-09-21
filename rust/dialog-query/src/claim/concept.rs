pub use super::Claim;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::predicate::concept::{Concept, Instance};
pub use crate::types::Scalar;
use crate::Cardinality;
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relation {
    pub the: Attribute,
    pub is: Value,
    pub cardinality: Cardinality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConceptClaim {
    Assert(Instance),
    Retract(Instance),
}

impl ConceptClaim {
    pub fn this(&self) -> &'_ Entity {
        match self {
            Self::Assert(instance) => instance.this(),
            Self::Retract(instance) => instance.this(),
        }
    }
}

impl From<ConceptClaim> for Vec<Instruction> {
    fn from(claim: ConceptClaim) -> Self {
        match claim {
            ConceptClaim::Assert(instance) => instance
                .into_artifacts()
                .into_iter()
                .map(Instruction::Assert)
                .collect(),
            ConceptClaim::Retract(instance) => instance
                .into_artifacts()
                .into_iter()
                .map(Instruction::Retract)
                .collect(),
        }
    }
}

impl From<ConceptClaim> for Claim {
    fn from(claim: ConceptClaim) -> Self {
        Claim::Concept(claim)
    }
}
