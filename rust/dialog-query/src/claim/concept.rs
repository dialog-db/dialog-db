pub use super::Claim;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::predicate::concept::{Concept, Instance};
pub use crate::session::transaction::{Edit, Transaction, TransactionError};
pub use crate::types::Scalar;
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};

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

/// Implement Edit for concept claims
///
/// This allows concept claims to merge their operations into a transaction
/// instead of immediately converting to instructions. Concept claims can
/// generate multiple operations (one per attribute).
impl Edit for ConceptClaim {
    fn merge(self, transaction: &mut Transaction) {
        let instance = match &self {
            ConceptClaim::Assert(instance) => instance,
            ConceptClaim::Retract(instance) => instance,
        };

        for relation in &instance.with {
            let relation = crate::claim::fact::Relation::new(
                relation.the.clone(),
                instance.this().clone(),
                relation.is.clone(),
            );
            let claim = match self {
                ConceptClaim::Assert(_) => crate::claim::fact::Claim::Assert(relation),
                ConceptClaim::Retract(_) => crate::claim::fact::Claim::Retract(relation),
            };
            claim.merge(transaction);
        }
    }
}


impl From<ConceptClaim> for Claim {
    fn from(claim: ConceptClaim) -> Self {
        Claim::Concept(claim)
    }
}
