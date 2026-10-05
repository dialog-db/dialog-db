mod batch;
mod induce;
mod query;
mod succession;
pub use batch::*;
pub use query::{TransactionQuery, TransactionSelectQuery};
pub(crate) use succession::{Settlement, settle};

use crate::Commit;
use crate::repository::branch::QueryLayer;
use crate::repository::branch::asset::store_assets;
use crate::repository::source::SourceRef;
use crate::rules::{SharedRuleCache, TriggerFootprint, on_attr, reads_attr};
use crate::{Branch, CommitError, RemoteSite, Revision, Snapshot, Staged};
use dialog_artifacts::{Changes, Statement};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::{Publish, Resolve};

/// A transaction on a line of the repository.
///
/// `Line` is what the transaction runs on, and it decides what
/// committing does:
///
/// - `&`[`Branch`]: `.commit().perform(&env)` STAGES the batch. It mints
///   a revision on the branch's own origin — the successor edition of
///   the head, exactly what a published commit would mint — but the
///   branch head does not move and nothing becomes visible. The returned
///   [`TransactionBatch`] chains further commits and finally
///   [`publish`](TransactionBatch::publish)es the whole chain with one
///   head CAS. A one-shot writer commits and publishes in one step with
///   [`.commit().publish()`](TransactionCommit::publish).
/// - [`TransactionBatch`]: the same, extending the staged chain by one
///   more commit.
/// - `&`[`Snapshot`]: commits advance the snapshot in place, minted on
///   the snapshot's own lineage. Nothing publishes; a snapshot is a fork,
///   not a stage.
///
/// Created by [`Branch::transaction`], [`Snapshot::transaction`], or
/// [`TransactionBatch::transaction`]. Accumulates durable changes via
/// `.assert()` / `.retract()` and *transient* facts (commands) via
/// `.dispatch()`.
///
/// Transients are visible to every read through [`query`](Self::query)
/// and to inductive-rule bodies during commit-time induction, but they
/// never enter the durable batch: they live for exactly one induction
/// round and leave no trace in the committed tree.
pub struct Transaction<Line> {
    line: Line,
    /// The durable writes, held so reading them is a range read (see
    /// [`Staged`]); exported to a [`Changes`] batch once, at commit.
    changes: Staged,
    transients: Staged,
}

impl<Line> Transaction<Line> {
    pub(crate) fn on(line: Line) -> Self {
        Transaction {
            line,
            changes: Staged::default(),
            transients: Staged::default(),
        }
    }

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

    /// Dispatch a claim as a *transient* fact (a command): visible to
    /// reads and to inductive-rule bodies during this commit, seeding
    /// commit-time induction, but never committed to the branch.
    pub fn dispatch<C: Statement>(mut self, claim: C) -> Self {
        self.transients.assert(claim);
        self
    }

    /// Integrate an external [`Changes`] batch into this transaction.
    ///
    /// Each instruction is replayed as if it had been asserted or
    /// retracted on the transaction directly — `Assert` becomes an
    /// entry under its policy, `Retract` a retraction entry —
    /// and the batch's asset changes are staged on the transaction.
    /// Useful for callers that build a [`Changes`] independently
    /// (e.g. a reactor accumulating effect outputs across rounds) and
    /// need to merge it into a running transaction.
    pub fn integrate(mut self, changes: Changes) -> Self {
        self.changes.apply(changes);
        self
    }

    /// Finalize the transaction into a commit command.
    ///
    /// `perform` first runs commit-time induction: the commit's delta
    /// (durable changes and dispatched transients alike) probes the
    /// `dialog.rule/on` trigger index, matching inductive rules fire
    /// against the transaction view, and their durable novelty folds
    /// into the commit while transient heads seed further rounds. Only
    /// then is the durable batch committed; transients are dropped,
    /// never written.
    pub fn commit(self) -> TransactionCommit<Line> {
        TransactionCommit {
            line: self.line,
            changes: self.changes.clone(),
            transients: self.transients.export(),
            allow_empty: false,
            canonicalize: false,
            amend: false,
        }
    }
}

impl<Line> Transaction<Line> {
    /// The staged layers [`Transaction::query`] reads over the line:
    /// the durable writes, then the transients. Shared, not copied.
    fn layers(&self) -> Vec<Staged> {
        [&self.changes, &self.transients]
            .into_iter()
            .filter(|layer| !layer.is_empty())
            .cloned()
            .collect()
    }
}

impl<'a> Transaction<&'a Branch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the branch.
    ///
    /// Pending asserts and retracts are surfaced through a
    /// [`TransactionQuery`] handle — assertions show up alongside the
    /// stored facts; retractions tombstone matching facts in the stored
    /// stream before the merge. Dispatched transients are part of the
    /// view too. The transaction itself stays open and committable.
    pub fn query(&self) -> TransactionQuery<'a> {
        TransactionQuery::new(SourceRef::Branch(self.line), self.layers())
    }
}

impl<'a> Transaction<&'a Snapshot> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the snapshot. See [`Transaction::<&Branch>::query`].
    pub fn query(&self) -> TransactionQuery<'a> {
        TransactionQuery::new(SourceRef::Snapshot(self.line), self.layers())
    }
}

impl Branch {
    /// Start a transaction on this branch.
    ///
    /// Use `.assert()` and `.retract()` to accumulate changes, then
    /// either `.commit().publish().perform(&env)` to commit and publish
    /// in one step, or `.commit().perform(&env)` to stage a
    /// [`TransactionBatch`] that chains further commits before one
    /// atomic publish.
    pub fn transaction(&self) -> Transaction<&Branch> {
        Transaction::on(self)
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
    pub fn transaction(&self) -> Transaction<&Snapshot> {
        Transaction::on(self)
    }
}

/// Command committing a [`Transaction`]: runs commit-time induction
/// over the transaction's delta, then mints the settled durable batch
/// on the line.
///
/// What `perform` returns follows the line — see [`Transaction`]. The
/// builder mirrors [`Commit`](crate::Commit)'s surface
/// ([`allow_empty`](Self::allow_empty) /
/// [`canonicalize`](Self::canonicalize)); the difference is the
/// induction step in front and that transients never reach the
/// durable batch.
pub struct TransactionCommit<Line> {
    pub(super) line: Line,
    pub(super) changes: Staged,
    pub(super) transients: Changes,
    pub(super) allow_empty: bool,
    pub(super) canonicalize: bool,
    pub(super) amend: bool,
}

impl<Line> TransactionCommit<Line> {
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
}

impl TransactionCommit<&Snapshot> {
    /// Run induction, then execute the commit, advancing the snapshot to
    /// the returned [`Revision`] (or returning the unchanged head when
    /// the settled batch is a no-op).
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let snapshot = self.line;
        let source = SourceRef::Snapshot(snapshot);
        let operator = Identify.perform(env).await?;
        let mut changes = Box::pin(succession::settle(
            vec![source.to_source()],
            QueryLayer::from(source).overlay(&operator),
            &self.changes,
            Settlement::Commit,
            env,
        ))
        .await?;
        // A snapshot commit reports only its revision: the transients
        // induction emitted surface on a staged
        // [`TransactionBatch::induced`] alone.
        induce::induce(
            SourceRef::Snapshot(snapshot),
            &mut changes,
            self.transients,
            env,
        )
        .await?;

        let previous = snapshot.revision();
        let touches_rules = touches_rules(&changes);
        let machinery =
            store_assets(SourceRef::Snapshot(snapshot), changes.take_assets(), env).await?;

        let mut commit = Commit::new(snapshot, changes.into_stream()).with_machinery(machinery);
        if self.allow_empty {
            commit = commit.allow_empty();
        }
        if self.canonicalize {
            commit = commit.canonicalize();
        }
        let revision = Box::pin(commit.perform(env)).await?;

        if !touches_rules {
            carry_footprint(
                &SourceRef::Snapshot(snapshot).rule_cache(),
                Some(&previous),
                &revision,
            );
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
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        Box::pin(self.transaction().commit().publish().perform(env)).await
    }
}
