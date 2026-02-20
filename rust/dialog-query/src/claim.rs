pub use super::relation::Relation;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::session::transaction::{Edit, Transaction, TransactionError};
use std::ops::Not;

/// Represents a claim that can be asserted or retracted.
pub trait Claim: Sized {
    /// Asserts the claim into a given transaction.
    fn assert(self, transaction: &mut Transaction);
    /// Retracts the claim from a given transaction.
    fn retract(self, transaction: &mut Transaction);

    /// Creates a claim that is inverse of this one.
    fn revert(self) -> Revert<Self> {
        Revert(self)
    }
}

impl<C: Claim> Edit for C {
    fn merge(self, transaction: &mut Transaction) {
        self.assert(transaction);
    }
}

/// A reversed claim that retracts the original when asserted
pub struct Revert<C: Claim>(C);
impl<C: Claim> Edit for Revert<C> {
    fn merge(self, transaction: &mut Transaction) {
        self.0.retract(transaction);
    }
}

/// Negation produces edit that retracts original claim when merged.
impl<C: Claim> Not for Revert<C> {
    type Output = C;

    fn not(self) -> Self::Output {
        let Revert(claim) = self;
        claim
    }
}
