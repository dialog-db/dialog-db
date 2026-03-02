pub use crate::artifact::{Artifact, Attribute, Instruction};
pub use crate::error::TransactionError;
pub use crate::session::transaction::{Edit, Transaction};
use std::ops::Not;

/// A domain-level write operation that can be asserted or retracted.
///
/// `Statement` is the high-level write API. Types like [`Association`],
/// [`AttributeStatement<A>`](crate::attribute::expression::AttributeStatement), and user-defined concept structs
/// implement this trait. Asserting a statement adds facts to the knowledge
/// base; retracting it removes them. The [`Retraction`] wrapper inverts the
/// direction — asserting a `Retraction<S>` retracts the inner statement.
pub trait Statement: Sized {
    /// Asserts the statement into a given transaction.
    fn assert(self, transaction: &mut Transaction);
    /// Retracts the statement from a given transaction.
    fn retract(self, transaction: &mut Transaction);

    /// Creates a statement that is inverse of this one.
    fn revert(self) -> Retraction<Self> {
        Retraction(self)
    }
}

impl<S: Statement> Edit for S {
    fn merge(self, transaction: &mut Transaction) {
        self.assert(transaction);
    }
}

/// A statement wrapper that inverts the assert/retract direction.
///
/// When merged into a [`Transaction`](crate::session::transaction::Transaction),
/// a `Retraction<S>` calls `S::retract` instead of `S::assert`, effectively
/// undoing the original statement. Applying `Not` to a `Retraction` recovers the
/// original statement.
pub struct Retraction<S: Statement>(S);
impl<S: Statement> Edit for Retraction<S> {
    fn merge(self, transaction: &mut Transaction) {
        self.0.retract(transaction);
    }
}

/// Negation produces edit that retracts original statement when merged.
impl<S: Statement> Not for Retraction<S> {
    type Output = S;

    fn not(self) -> Self::Output {
        let Retraction(statement) = self;
        statement
    }
}
