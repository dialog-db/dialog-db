use std::ops::Not;

pub use dialog_artifacts::{Changes, Statement, Update};

/// Extension trait adding `revert()` to all [`Statement`] implementors.
pub trait StatementExt: Statement {
    /// Creates a statement that is inverse of this one.
    fn revert(self) -> Retraction<Self> {
        Retraction(self)
    }
}

impl<S: Statement> StatementExt for S {}

/// A statement wrapper that inverts the assert/retract direction.
pub struct Retraction<S: Statement>(pub S);

impl<S: Statement> Not for Retraction<S> {
    type Output = S;

    fn not(self) -> Self::Output {
        self.0
    }
}
