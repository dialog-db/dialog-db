use dialog_artifacts::{ChangeStream, Changes, Statement};

use super::Branch;
use super::commit::Commit;

/// A transaction on a branch.
///
/// Created by [`Branch::edit`]. Accumulates changes via `.assert()` and
/// `.retract()`, then commits atomically via `.commit().perform(&env)`.
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,
}

impl<'a> Transaction<'a> {
    /// Assert a claim into this transaction.
    pub fn assert<C: Statement>(mut self, claim: C) -> Self {
        self.changes.assert(claim);
        self
    }

    /// Retract a claim from this transaction.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        self.changes.retract(claim);
        self
    }

    /// Finalize the transaction into a commit command.
    pub fn commit(self) -> Commit<'a, ChangeStream> {
        self.branch.commit(self.changes.into_stream())
    }
}

impl Branch {
    /// Start a transaction on this branch.
    ///
    /// Use `.assert()` and `.retract()` to accumulate changes,
    /// then `.commit().perform(&env)` to apply them.
    pub fn edit(&self) -> Transaction<'_> {
        Transaction {
            branch: self,
            changes: Changes::new(),
        }
    }
}
