//! Fact-based claims for assertions and retractions
//!
//! Provides claims for basic fact operations - adding and removing entity-attribute-value triples.

pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::session::transaction::{Edit, Transaction, TransactionError};
pub use crate::types::Scalar;
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};

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
    pub fn new(the: Attribute, of: Entity, is: Value) -> Self {
        Self { the, of, is }
    }
}

/// Trait for types that can be viewed as relations
pub trait AsRelation {
    fn the(&self) -> &Attribute;
    fn of(&self) -> &Entity;
    fn is(&self) -> &Value;
}

impl AsRelation for Relation {
    fn the(&self) -> &Attribute {
        &self.the
    }

    fn of(&self) -> &Entity {
        &self.of
    }

    fn is(&self) -> &Value {
        &self.is
    }
}

impl AsRelation for (Attribute, Entity, Value) {
    fn the(&self) -> &Attribute {
        &self.0
    }

    fn of(&self) -> &Entity {
        &self.1
    }

    fn is(&self) -> &Value {
        &self.2
    }
}

/// Implement Edit for Relation to allow direct merging into transactions
impl Edit for Relation {
    fn merge(self, transaction: &mut Transaction) {
        // By default, a standalone relation is treated as an assertion
        transaction.assert(self);
    }
}

/// A fact-based claim for assertions and retractions
///
/// Represents proposed changes to entity-attribute-value relations.
/// Claims are built around the Relation type for consistency.
///
/// # Examples
///
/// ```ignore
/// use dialog_query::claim::fact::{Claim, Relation};
///
/// // Create an assertion claim
/// let relation = Relation::new("user/name".parse()?, entity, Value::String("Alice".to_string()));
/// let assertion = Claim::Assert(relation);
///
/// // Create a retraction claim
/// let relation = Relation::new("user/email".parse()?, entity, Value::String("old@example.com".to_string()));
/// let retraction = Claim::Retract(relation);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Claim {
    /// An assertion claim - proposes adding a fact to the database
    ///
    /// When committed, this creates a new fact with the specified relation.
    Assert(Relation),
    /// A retraction claim - proposes removing a fact from the database
    ///
    /// When committed, this removes an existing fact with the specified relation.
    Retract(Relation),
}

impl Claim {
    pub fn the(&self) -> &'_ Attribute {
        match self {
            Self::Assert(relation) => &relation.the,
            Self::Retract(relation) => &relation.the,
        }
    }
    pub fn of(&self) -> &'_ Entity {
        match self {
            Self::Assert(relation) => &relation.of,
            Self::Retract(relation) => &relation.of,
        }
    }
    pub fn is(&self) -> &'_ Value {
        match self {
            Self::Assert(relation) => &relation.is,
            Self::Retract(relation) => &relation.is,
        }
    }

    /// Get the underlying relation
    pub fn relation(&self) -> &Relation {
        match self {
            Self::Assert(relation) => relation,
            Self::Retract(relation) => relation,
        }
    }

    fn merge(self, transaction: &mut Transaction) {
        match self {
            Claim::Assert(relation) => {
                transaction.assert(relation);
            }
            Claim::Retract(relation) => {
                transaction.retract(relation);
            }
        }
    }
}

/// Convert fact claims to database instructions
///
/// Each fact claim generates exactly one instruction:
/// - `Assertion` → `Instruction::Assert`
/// - `Retraction` → `Instruction::Retract`
///
/// # Examples
///
/// ```ignore
/// use dialog_query::claim::fact::Claim;
///
/// let claim = Claim::Assertion {
///     the: "user/name".parse()?,
///     of: entity,
///     is: Value::String("Alice".to_string()),
/// };
///
/// let instructions: Vec<Instruction> = claim.into();
/// assert_eq!(instructions.len(), 1); // Fact claims generate one instruction each
/// ```
/// Implement Edit for fact claims
///
/// This allows fact claims to merge their operations into a transaction
/// instead of immediately converting to instructions
impl Edit for Claim {
    fn merge(self, transaction: &mut Transaction) {
        self.merge(transaction);
    }
}

/// Convert fact claims to database instructions (legacy API)
///
/// **Deprecated**: Use the `Edit` trait with `claim.merge(&mut transaction)` instead.
/// This provides better performance and composability.
///
/// Each fact claim generates exactly one instruction:
/// - `Assertion` → `Instruction::Assert`
/// - `Retraction` → `Instruction::Retract`
impl From<Claim> for Vec<Instruction> {
    fn from(claim: Claim) -> Self {
        let instruction = match claim {
            Claim::Assert(relation) => Instruction::Assert(Artifact {
                the: relation.the,
                of: relation.of,
                is: relation.is,
                cause: None,
            }),
            Claim::Retract(relation) => Instruction::Retract(Artifact {
                the: relation.the,
                of: relation.of,
                is: relation.is,
                cause: None,
            }),
        };
        vec![instruction]
    }
}
