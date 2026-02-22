pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::claim::Claim;
use crate::claim::Revert;
pub use crate::session::transaction::{Edit, Transaction};
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};
use std::ops::Not;

/// A relation represents an entity-attribute-value triple
///
/// This is the fundamental unit of data in the dialog-query system.
/// Relations follow the EAV pattern:
/// - `the` - attribute (predicate/property)
/// - `of` - entity (subject)
/// - `is` - value (object)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relation {
    /// The attribute (predicate) - what property is being asserted
    pub the: Attribute,
    /// The entity (subject) - what entity the property applies to
    pub of: Entity,
    /// The value (object) - what value the property has
    pub is: Value,
}

impl Relation {
    /// Create a new relation from its components
    pub fn new(the: Attribute, of: Entity, is: Value) -> Self {
        Self { the, of, is }
    }
}

impl Claim for Relation {
    fn assert(self, transaction: &mut Transaction) {
        transaction.associate(self);
    }

    fn retract(self, transaction: &mut Transaction) {
        transaction.dissociate(self);
    }
}

impl Not for Relation {
    type Output = Revert<Relation>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}
