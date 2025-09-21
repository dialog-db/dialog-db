//! Fact-based claims for assertions and retractions
//!
//! Provides claims for basic fact operations - adding and removing entity-attribute-value triples.

pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::types::Scalar;
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};

/// A fact-based claim for assertions and retractions
///
/// Represents proposed changes to entity-attribute-value triples.
/// Claims follow the EAV pattern:
/// - `the` - attribute (predicate/property)
/// - `of` - entity (subject)
/// - `is` - value (object)
///
/// # Examples
///
/// ```ignore
/// use dialog_query::claim::fact::Claim;
///
/// // Create an assertion claim
/// let assertion = Claim::Assertion {
///     the: "user/name".parse()?,
///     of: entity,
///     is: Value::String("Alice".to_string()),
/// };
///
/// // Create a retraction claim
/// let retraction = Claim::Retraction {
///     the: "user/email".parse()?,
///     of: entity,
///     is: Value::String("old@example.com".to_string()),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Claim {
    /// An assertion claim - proposes adding a fact to the database
    ///
    /// When committed, this creates a new fact with the specified
    /// entity-attribute-value triple.
    Assert {
        /// The attribute (predicate) - what property is being asserted
        the: Attribute,
        /// The entity (subject) - what entity the property applies to
        of: Entity,
        /// The value (object) - what value the property has
        is: Value,
    },
    /// A retraction claim - proposes removing a fact from the database
    ///
    /// When committed, this removes an existing fact with the specified
    /// entity-attribute-value triple.
    Retract {
        /// The attribute (predicate) - what property is being retracted
        the: Attribute,
        /// The entity (subject) - what entity the property applies to
        of: Entity,
        /// The value (object) - what value the property had
        is: Value,
    },
}

impl Claim {
    pub fn the(&self) -> &'_ Attribute {
        match self {
            Self::Assert { the, .. } => the,
            Self::Retract { the, .. } => the,
        }
    }
    pub fn of(&self) -> &'_ Entity {
        match self {
            Self::Assert { of, .. } => of,
            Self::Retract { of, .. } => of,
        }
    }
    pub fn is(&self) -> &'_ Value {
        match self {
            Self::Assert { is, .. } => is,
            Self::Retract { is, .. } => is,
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
impl From<Claim> for Vec<Instruction> {
    fn from(claim: Claim) -> Self {
        let instruction = match claim {
            Claim::Assert { the, of, is } => Instruction::Assert(Artifact {
                the,
                of,
                is,
                cause: None,
            }),
            Claim::Retract { the, of, is } => Instruction::Retract(Artifact {
                the,
                of,
                is,
                cause: None,
            }),
        };
        vec![instruction]
    }
}
