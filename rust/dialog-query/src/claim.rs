pub use super::assertion::Assertion;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::error::TransactionError;
pub use crate::session::transaction::{Edit, Transaction};
use std::ops::Not;

/// A domain-level write operation that can be asserted or retracted.
///
/// `Claim` is the high-level write API. Types like [`Assertion`],
/// [`With<A>`](crate::attribute::With), and user-defined concept structs
/// implement this trait. Asserting a claim adds facts to the knowledge
/// base; retracting it removes them. The [`Revert`] wrapper inverts the
/// direction — asserting a `Revert<C>` retracts the inner claim.
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

/// A claim wrapper that inverts the assert/retract direction.
///
/// When merged into a [`Transaction`](crate::session::transaction::Transaction),
/// a `Revert<C>` calls `C::retract` instead of `C::assert`, effectively
/// undoing the original claim. Applying `Not` to a `Revert` recovers the
/// original claim.
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
