mod query;
pub use query::{TransactionQuery, TransactionSelectQuery};

use std::sync::Arc;

use crate::rules::RuleSource;
use crate::{Branch, Commit};
use dialog_artifacts::{ChangeStream, Changes, Instruction, Statement, Update};

/// A transaction on a branch.
///
/// Created by [`Branch::transaction`]. Accumulates changes via `.assert()`
/// and `.retract()`, then commits atomically via `.commit().perform(&env)`.
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,
    rule_source: Option<Arc<dyn RuleSource>>,
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

    /// Install a [`RuleSource`](crate::RuleSource) so queries run against
    /// this transaction's view (via [`query`](Self::query)) resolve
    /// deductive rules stored as facts — the same way a committed
    /// [`Branch::query`](crate::Branch::query) does. Propagated to every
    /// `query()` handle, so a mid-transaction or dry-run read returns the
    /// same deductions a post-commit read would.
    pub fn with_rules(mut self, source: Arc<dyn RuleSource>) -> Self {
        self.rule_source = Some(source);
        self
    }

    /// Integrate an external [`Changes`] batch into this transaction.
    ///
    /// Each instruction is replayed as if it had been asserted or
    /// retracted on the transaction directly — `Assert`/`Replace`
    /// become additive entries, `Retract` becomes a retraction entry.
    /// Useful for callers that build a [`Changes`] independently
    /// (e.g. a reactor accumulating effect outputs across rounds) and
    /// need to merge it into a running transaction.
    pub fn integrate(mut self, changes: Changes) -> Self {
        for instruction in changes.into_instructions() {
            match instruction {
                Instruction::Assert(a) => {
                    Update::associate(&mut self.changes, a.the, a.of, a.is);
                }
                Instruction::Replace(a) => {
                    Update::associate_unique(&mut self.changes, a.the, a.of, a.is);
                }
                Instruction::Retract(a) => {
                    Update::dissociate(&mut self.changes, a.the, a.of, a.is);
                }
            }
        }
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
        let query = TransactionQuery::new(self.branch, &self.changes);
        match &self.rule_source {
            Some(source) => query.with_rules(Arc::clone(source)),
            None => query,
        }
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
            rule_source: None,
        }
    }
}
