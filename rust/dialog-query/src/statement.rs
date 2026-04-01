pub use crate::artifact::{Artifact, Instruction};
pub use crate::error::TransactionError;
pub use crate::session::transaction::{Edit, Transaction};
use std::ops::Not;

// Re-export Statement and Update from dialog-artifacts
pub use dialog_artifacts::{Statement, Update};

/// Extension trait adding `revert()` to all [`Statement`] implementors.
///
/// The core [`Statement`] trait lives in `dialog-artifacts` and provides
/// `assert`/`retract`. This extension adds a convenience method that wraps
/// the statement in a [`Retraction`], inverting its merge direction.
pub trait StatementExt: Statement {
    /// Creates a statement that is inverse of this one.
    fn revert(self) -> Retraction<Self> {
        Retraction(self)
    }
}

impl<S: Statement> StatementExt for S {}

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
pub struct Retraction<S: Statement>(pub S);
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
