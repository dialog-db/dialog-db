use serde::{Deserialize, Serialize};

use super::Claim;

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
}
