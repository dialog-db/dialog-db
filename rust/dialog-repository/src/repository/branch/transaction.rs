mod query;
pub use query::{TransactionQuery, TransactionSelectQuery};

use crate::{Branch, Commit, RuleWrite};
use dialog_artifacts::{ChangeStream, Changes, Instruction, Statement, Update};
use dialog_query::DeductiveRule;

/// A transaction on a branch.
///
/// Created by [`Branch::transaction`]. Accumulates changes via `.assert()`
/// and `.retract()`, then commits atomically via `.commit().perform(&env)`.
///
/// Deductive rules are a distinct, privileged kind of write: they live under
/// the reserved `dialog.rule/*` namespace, which the public assert path
/// refuses, so they are staged separately via [`install_rule`](Self::install_rule)
/// / [`remove_rule`](Self::remove_rule) and travel to the commit on their own
/// rail — the same one revision records use — rather than through the gated
/// instruction stream.
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,
    rules: Vec<RuleWrite>,
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

    /// Install a deductive `rule` durably on the branch.
    ///
    /// This is the sanctioned way an app persists a rule: the rule's
    /// `dialog.rule/*` facts are staged for the privileged write rail rather
    /// than the public instruction stream, so the reserved-namespace gate that
    /// refuses a hand-asserted `dialog.rule/*` fact does not reject a
    /// legitimate install. When the transaction commits, the facts land in the
    /// tree through [`BufferedBatch::install`](dialog_artifacts::BufferedBatch::install),
    /// the same rail carrying revision records.
    pub fn install_rule(mut self, rule: &DeductiveRule) -> Self {
        self.rules.push(RuleWrite::Install(rule.clone()));
        self
    }

    /// Remove a previously installed deductive `rule` from the branch.
    ///
    /// The inverse of [`install_rule`](Self::install_rule): the rule's
    /// `dialog.rule/*` facts are erased through the privileged rail at commit
    /// time. Removing a rule that was never installed is a no-op.
    pub fn remove_rule(mut self, rule: &DeductiveRule) -> Self {
        self.rules.push(RuleWrite::Remove(rule.clone()));
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
        TransactionQuery::new(self.branch, &self.changes)
    }

    /// Finalize the transaction into a commit command.
    pub fn commit(self) -> Commit<'a, ChangeStream> {
        self.branch
            .commit(self.changes.into_stream())
            .with_rules(self.rules)
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
            rules: Vec::new(),
        }
    }
}
