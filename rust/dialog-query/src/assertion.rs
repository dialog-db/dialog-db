pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::claim::Claim;
use crate::claim::Revert;
pub use crate::session::transaction::{Change, Edit, Transaction};
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};
use std::ops::Not;

/// An assertion represents an entity-attribute-value triple for writes.
///
/// This is the fundamental unit of data in the dialog-query system.
/// Assertions follow the EAV pattern:
/// - `the` - attribute (predicate/property)
/// - `of` - entity (subject)
/// - `is` - value (object)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assertion {
    /// The attribute (predicate) - what property is being asserted
    pub the: Attribute,
    /// The entity (subject) - what entity the property applies to
    pub of: Entity,
    /// The value (object) - what value the property has
    pub is: Value,
}

impl Assertion {
    /// Create a new assertion from its components
    pub fn new(the: Attribute, of: Entity, is: Value) -> Self {
        Self { the, of, is }
    }
}

/// A retraction reverses an assertion, removing it from the store.
pub type Retraction = Revert<Assertion>;

impl From<Assertion> for Change {
    fn from(assertion: Assertion) -> Self {
        Change::Assert(assertion.is)
    }
}

impl From<Retraction> for Change {
    fn from(retraction: Retraction) -> Self {
        Change::Retract(retraction.not().is)
    }
}

impl Claim for Assertion {
    fn assert(self, transaction: &mut Transaction) {
        transaction.associate(self);
    }

    fn retract(self, transaction: &mut Transaction) {
        transaction.dissociate(self);
    }
}

impl Not for Assertion {
    type Output = Revert<Assertion>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}
