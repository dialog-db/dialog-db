use crate::transaction_query::TransactionQuery;
use crate::{Branch, Commit};
use dialog_artifacts::{ChangeStream, Changes, Statement};

/// A transaction on a branch.
///
/// Created by [`Branch::transaction`]. Accumulates changes via `.assert()`
/// and `.retract()`, then commits atomically via `.commit().perform(&env)`.
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,
}

impl<'a> Transaction<'a> {
    /// Assert a claim into this transaction.
    pub fn assert<C: Statement>(mut self, claim: C) -> Self {
        // Disambiguate from `Statement::assert` (which Changes now
        // implements) by calling the claim's own assert into our
        // changes buffer directly.
        claim.assert(&mut self.changes);
        self
    }

    /// Retract a claim from this transaction.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        claim.retract(&mut self.changes);
        self
    }

    /// Run queries against this transaction's "as-if committed" view of
    /// the branch.
    ///
    /// Pending asserts and retracts are surfaced through a
    /// [`TransactionQuery`] handle — assertions show up alongside the
    /// branch's stored facts; retractions tombstone matching facts in
    /// the branch's stream before the merge. The transaction itself
    /// stays open and committable.
    pub fn query(&self) -> TransactionQuery<'_> {
        TransactionQuery::new(self.branch, &self.changes)
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
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction {
            branch: self,
            changes: Changes::new(),
        }
    }
}
