pub use super::association::Association;
pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::error::TransactionError;
pub use crate::session::transaction::{Edit, Transaction};
use std::ops::Not;

/// A domain-level write operation that can be asserted or retracted.
///
/// `Assertion` is the high-level write API. Types like [`Association`],
/// [`With<A>`](crate:::With), and user-defined concept structs
/// implement this trait. Asserting a claim adds facts to the knowledge
/// base; retracting it removes them. The [`Retraction`] wrapper inverts the
/// direction — asserting a `Retraction<C>` retracts the inner claim.
pub trait Assertion: Sized {
    /// Asserts the claim into a given transaction.
    fn assert(self, transaction: &mut Transaction);
    /// Retracts the claim from a given transaction.
    fn retract(self, transaction: &mut Transaction);

    /// Creates a claim that is inverse of this one.
    fn revert(self) -> Retraction<Self> {
        Retraction(self)
    }
}

impl<C: Assertion> Edit for C {
    fn merge(self, transaction: &mut Transaction) {
        self.assert(transaction);
    }
}

/// A claim wrapper that inverts the assert/retract direction.
///
/// When merged into a [`Transaction`](crate::session::transaction::Transaction),
/// a `Retraction<C>` calls `C::retract` instead of `C::assert`, effectively
/// undoing the original claim. Applying `Not` to a `Retraction` recovers the
/// original claim.
pub struct Retraction<C: Assertion>(C);
impl<C: Assertion> Edit for Retraction<C> {
    fn merge(self, transaction: &mut Transaction) {
        self.0.retract(transaction);
    }
}

/// Negation produces edit that retracts original claim when merged.
impl<C: Assertion> Not for Retraction<C> {
    type Output = C;

    fn not(self) -> Self::Output {
        let Retraction(claim) = self;
        claim
    }
}
