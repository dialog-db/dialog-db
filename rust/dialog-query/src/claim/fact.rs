pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::types::Scalar;
pub use crate::{Entity, Value};
use serde::{Deserialize, Serialize};
/// A claim represents an assertion or retraction before it becomes a fact
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Claim {
    /// An assertion claim
    Assertion {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: Value,
    },
    /// A retraction claim
    Retraction {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: Value,
    },
}

impl From<Claim> for Vec<Instruction> {
    fn from(claim: Claim) -> Self {
        let instruction = match claim {
            Claim::Assertion { the, of, is } => Instruction::Assert(Artifact {
                the,
                of,
                is,
                cause: None,
            }),
            Claim::Retraction { the, of, is } => Instruction::Retract(Artifact {
                the,
                of,
                is,
                cause: None,
            }),
        };
        vec![instruction]
    }
}
