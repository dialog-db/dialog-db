pub use crate::artifact::{Artifact, Attribute, Instruction};
use crate::attribute::The;
pub use crate::session::transaction::{Change, Edit, Transaction};
use crate::statement::{Retraction, Statement};
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};
use std::ops::Not;

/// An association represents an entity-attribute-value triple for writes.
///
/// This is the fundamental unit of data in the dialog-query system.
/// Associations follow the EAV pattern:
/// - `the` - attribute (predicate/property)
/// - `of` - entity (subject)
/// - `is` - value (object)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Association {
    /// The attribute (predicate) - what property is being asserted
    pub the: The,
    /// The entity (subject) - what entity the property applies to
    pub of: Entity,
    /// The value (object) - what value the property has
    pub is: Value,
}

impl Association {
    /// Create a new association from its components
    pub fn new(the: The, of: Entity, is: Value) -> Self {
        Self { the, of, is }
    }
}

/// A retraction reverses an association, removing it from the store.
pub type Dissociation = Retraction<Association>;

impl From<Association> for Change {
    fn from(association: Association) -> Self {
        Change::Assert(association.is)
    }
}

impl From<Dissociation> for Change {
    fn from(retraction: Dissociation) -> Self {
        Change::Retract(retraction.not().is)
    }
}

impl Statement for Association {
    fn assert(self, transaction: &mut Transaction) {
        transaction.associate(self);
    }

    fn retract(self, transaction: &mut Transaction) {
        transaction.dissociate(self);
    }
}

impl Not for Association {
    type Output = Retraction<Association>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}
