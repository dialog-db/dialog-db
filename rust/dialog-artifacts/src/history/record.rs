use serde::{Deserialize, Serialize};

use crate::{Datum, Instruction};

use super::{Cause, Claim};

/// A [`Claim`] paired with its polarity, as stored in the history index.
///
/// Retractions are claims like any other and participate in the same cause
/// lineage — a retraction's cause identifies the claim(s) whose assertion it
/// withdraws — but the history index must remember which of the two a claim
/// was in order to reconstruct state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Record {
    /// The claim asserts its value
    Assert(Claim),
    /// The claim withdraws a previous assertion of its value
    Retract(Claim),
}

impl Record {
    /// The [`Claim`] carried by this record, regardless of polarity
    pub fn claim(&self) -> &Claim {
        match self {
            Record::Assert(claim) => claim,
            Record::Retract(claim) => claim,
        }
    }

    /// Whether this record asserts (rather than retracts) its claim
    pub fn is_assertion(&self) -> bool {
        matches!(self, Record::Assert(_))
    }

    /// Derive the history record for an instruction, given the [`Datum`]s
    /// currently asserted at the instruction's `(entity, attribute)`: the
    /// record's cause lists the versions of the claims the instruction
    /// supersedes. Data that carries no version (committed outside of
    /// version control) contributes nothing to the cause.
    ///
    /// - An assertion is purely additive: it supersedes nothing.
    /// - A replacement supersedes every currently asserted claim with a
    ///   different value — exactly the data the cardinality-one supersession
    ///   removes from the indexes.
    /// - A retraction withdraws the assertions of its exact value.
    pub fn derive(instruction: &Instruction, current: &[Datum]) -> Record {
        match instruction {
            Instruction::Assert(artifact) => Record::Assert(Claim {
                the: artifact.the.clone(),
                of: artifact.of.clone(),
                is: artifact.is.clone(),
                cause: Cause::genesis(),
            }),
            Instruction::Replace(artifact) => {
                let value = artifact.is.to_bytes();
                let versions = current
                    .iter()
                    .filter(|datum| datum.value != value)
                    .filter_map(|datum| datum.version)
                    .collect();

                Record::Assert(Claim {
                    the: artifact.the.clone(),
                    of: artifact.of.clone(),
                    is: artifact.is.clone(),
                    cause: Cause::new(versions),
                })
            }
            Instruction::Retract(artifact) => {
                let value = artifact.is.to_bytes();
                let versions = current
                    .iter()
                    .filter(|datum| datum.value == value)
                    .filter_map(|datum| datum.version)
                    .collect();

                Record::Retract(Claim {
                    the: artifact.the.clone(),
                    of: artifact.of.clone(),
                    is: artifact.is.clone(),
                    cause: Cause::new(versions),
                })
            }
        }
    }
}
