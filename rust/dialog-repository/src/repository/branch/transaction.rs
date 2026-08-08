mod induce;
mod query;
pub use query::{TransactionQuery, TransactionSelectQuery};

use crate::{Branch, CommitError, RemoteSite, Revision};
use dialog_artifacts::{Changes, Instruction, Statement, Update};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{Publish, Resolve};

/// A transaction on a branch.
///
/// Created by [`Branch::transaction`]. Accumulates durable changes via
/// `.assert()` / `.retract()` and *transient* facts (commands) via
/// `.dispatch()`, then commits atomically via `.commit().perform(&env)`.
///
/// Transients are visible to every read through [`query`](Self::query)
/// and to inductive-rule bodies during commit-time induction, but they
/// never enter the durable batch: they live for exactly one induction
/// round and leave no trace in the committed tree.
pub struct Transaction<'a> {
    branch: &'a Branch,
    changes: Changes,
    transients: Changes,
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

    /// Dispatch a claim as a *transient* fact (a command): visible to
    /// reads and to inductive-rule bodies during this commit, seeding
    /// commit-time induction, but never committed to the branch.
    pub fn dispatch<C: Statement>(mut self, claim: C) -> Self {
        claim.assert(&mut self.transients);
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
    /// the branch's stream before the merge. Dispatched transients are
    /// part of the view too. The transaction itself stays open and
    /// committable.
    pub fn query(&self) -> TransactionQuery<'_> {
        let mut view = self.changes.clone();
        self.transients.clone().assert(&mut view);
        TransactionQuery::new(self.branch, &view)
    }

    /// Finalize the transaction into a commit command.
    ///
    /// [`TransactionCommit::perform`] first runs commit-time induction:
    /// the commit's delta (durable changes and dispatched transients
    /// alike) probes the `dialog.rule/on` trigger index, matching inductive
    /// rules fire against the transaction view, and their durable
    /// novelty folds into the commit while transient heads seed further
    /// rounds. Only then is the durable batch committed; transients are
    /// dropped, never written.
    pub fn commit(self) -> TransactionCommit<'a> {
        TransactionCommit {
            branch: self.branch,
            changes: self.changes,
            transients: self.transients,
            allow_empty: false,
            canonicalize: false,
        }
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
            transients: Changes::new(),
        }
    }
}

/// Command committing a [`Transaction`]: runs commit-time induction
/// over the transaction's delta, then delegates the settled durable
/// batch to [`Branch::commit`].
///
/// Mirrors [`Commit`](crate::Commit)'s builder surface
/// ([`allow_empty`](Self::allow_empty) /
/// [`canonicalize`](Self::canonicalize)); the difference is the
/// induction step in front and that transients never reach the
/// durable batch.
pub struct TransactionCommit<'a> {
    branch: &'a Branch,
    changes: Changes,
    transients: Changes,
    allow_empty: bool,
    canonicalize: bool,
}

impl<'a> TransactionCommit<'a> {
    /// Mint a revision even when the settled change batch leaves the
    /// indexes untouched. See [`Commit::allow_empty`](crate::Commit::allow_empty).
    pub fn allow_empty(mut self) -> Self {
        self.allow_empty = true;
        self
    }

    /// Flush write buffers to the leaves before publishing. See
    /// [`Commit::canonicalize`](crate::Commit::canonicalize).
    pub fn canonicalize(mut self) -> Self {
        self.canonicalize = true;
        self
    }

    /// Run induction, then execute the commit, returning the
    /// newly-published [`Revision`] (or the unchanged head when the
    /// settled batch is a no-op).
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let mut changes = self.changes;
        induce::induce(self.branch, &mut changes, self.transients, env).await?;

        let mut commit = self.branch.commit(changes.into_stream());
        if self.allow_empty {
            commit = commit.allow_empty();
        }
        if self.canonicalize {
            commit = commit.canonicalize();
        }
        let revision = Box::pin(commit.perform(env)).await?;

        // Advance the induction watermark: rules have now evaluated
        // through this revision (induction ran over the commit's delta
        // plus any lag, and the settled batch is what `revision`
        // holds). A raced publish here at worst regresses the
        // watermark, which re-induces a covered span — idempotent
        // under the novelty check.
        let cell = self.branch.induction_cell();
        if cell.content().as_ref() != Some(&revision) {
            cell.publish(revision.clone()).perform(env).await?;
        }
        Ok(revision)
    }
}

impl Branch {
    /// Run commit-time induction with no changes of this transaction's
    /// own: catches inductive rules up over `(watermark, head]` — the
    /// facts that entered the branch through pulls, raw commits, or a
    /// crash-interrupted instant — and commits whatever durable
    /// novelty they derive. A no-op (returning the unchanged head)
    /// when the watermark is already at the head or the lag fires
    /// nothing.
    ///
    /// This is the explicit post-pull instant: call it after
    /// [`pull`](Self::pull) to let level-triggered rules enforce
    /// themselves over the merged-in facts.
    pub async fn induce<Env>(&self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        self.transaction().commit().perform(env).await
    }
}
