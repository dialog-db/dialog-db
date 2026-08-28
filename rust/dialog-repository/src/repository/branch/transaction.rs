mod induce;
mod query;
pub use query::{TransactionQuery, TransactionSelectQuery};

use crate::Commit;
use crate::repository::source::SourceRef;
use crate::rules::{SharedRuleCache, TriggerFootprint, on_attr, reads_attr};
use crate::{Branch, CommitError, RemoteSite, Revision, Snapshot};
use dialog_artifacts::{Changes, Instruction, Statement, Update};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{Publish, Resolve};

/// A transaction on a branch or a snapshot.
///
/// Created by [`Branch::transaction`] or [`Snapshot::transaction`].
/// Accumulates durable changes via `.assert()` / `.retract()` and
/// *transient* facts (commands) via `.dispatch()`, then commits
/// atomically via `.commit().perform(&env)`.
///
/// Transients are visible to every read through [`query`](Self::query)
/// and to inductive-rule bodies during commit-time induction, but they
/// never enter the durable batch: they live for exactly one induction
/// round and leave no trace in the committed tree.
pub struct Transaction<'a> {
    source: SourceRef<'a>,
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
    /// the line it runs on.
    ///
    /// Pending asserts and retracts are surfaced through a
    /// [`TransactionQuery`] handle — assertions show up alongside the
    /// stored facts; retractions tombstone matching facts in the stored
    /// stream before the merge. Dispatched transients are part of the
    /// view too. The transaction itself stays open and committable.
    pub fn query(&self) -> TransactionQuery<'_> {
        let mut view = self.changes.clone();
        self.transients.clone().assert(&mut view);
        TransactionQuery::new(self.source, &view)
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
            source: self.source,
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
            source: SourceRef::from(self),
            changes: Changes::new(),
            transients: Changes::new(),
        }
    }
}

impl Snapshot {
    /// Start a transaction on this snapshot: the same [`Transaction`]
    /// a branch runs, committing through [`Snapshot::commit`].
    ///
    /// Use `.assert()` and `.retract()` to accumulate changes, then
    /// `.commit().perform(&env)` to apply them; the snapshot advances to
    /// the revision `perform` returns. Clone first to keep the view you
    /// have.
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction {
            source: SourceRef::from(self),
            changes: Changes::new(),
            transients: Changes::new(),
        }
    }
}

/// Command committing a [`Transaction`]: runs commit-time induction
/// over the transaction's delta, then delegates the settled durable
/// batch to [`Branch::commit`] / [`Snapshot::commit`].
///
/// Mirrors [`Commit`](crate::Commit)'s builder surface
/// ([`allow_empty`](Self::allow_empty) /
/// [`canonicalize`](Self::canonicalize)); the difference is the
/// induction step in front and that transients never reach the
/// durable batch.
pub struct TransactionCommit<'a> {
    source: SourceRef<'a>,
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
        induce::induce(self.source, &mut changes, self.transients, env).await?;

        let previous = self.source.revision();
        let touches_rules = touches_rules(&changes);

        let mut commit = Commit::new(self.source, changes.into_stream());
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
        // under the novelty check. A branch keeps the watermark in its
        // induction cell; a snapshot keeps its own in memory, beside
        // the head only it can move — a raw `Snapshot::commit` lags it
        // exactly as a raw `Branch::commit` or a pull lags a branch's.
        match self.source {
            SourceRef::Branch(branch) => {
                let cell = branch.induction_cell();
                if cell.content().as_ref() != Some(&revision) {
                    cell.publish(revision.clone()).perform(env).await?;
                }
            }
            SourceRef::Snapshot(snapshot) => snapshot.record_induction(&revision),
        }

        if !touches_rules {
            carry_footprint(&self.source.rule_cache(), previous.as_ref(), &revision);
        }
        Ok(revision)
    }
}

/// Whether a settled change batch touches the trigger structures, i.e.
/// asserts or retracts `dialog.rule/on` or `dialog.rule/reads` facts.
pub(crate) fn touches_rules(changes: &Changes) -> bool {
    let on = on_attr();
    let reads = reads_attr();
    changes
        .iter()
        .any(|(_, attribute, _)| *attribute == on || *attribute == reads)
}

/// Carry the trigger footprint cached at `previous` forward to
/// `revision`.
///
/// The footprint is a pure function of the committed `dialog.rule/on`
/// and `dialog.rule/reads` facts, so a commit touching neither (checked
/// after induction, which may fold rule installs into the batch) keys
/// the same footprint under the head it minted. Without this every
/// commit advances the head past the cache's key and the steady-state
/// no-rules commit re-pays both footprint range scans.
pub(crate) fn carry_footprint(
    cache: &SharedRuleCache,
    previous: Option<&Revision>,
    revision: &Revision,
) {
    let footprint = match previous {
        // A genesis commit sees an empty line: no committed rules
        // exist, so the empty footprint is exact.
        None => Some(TriggerFootprint::default()),
        Some(previous) => cache.footprint(previous),
    };
    if let Some(footprint) = footprint {
        cache.record_footprint(revision.clone(), footprint);
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
