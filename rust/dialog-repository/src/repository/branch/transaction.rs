mod batch;
pub(crate) mod induce;
#[cfg(test)]
mod invariants;
#[cfg(test)]
mod migration;
#[cfg(test)]
mod oracle;
#[cfg(test)]
mod quarantine;
pub(crate) use induce::induction_ignores;
mod query;
mod succession;
pub(crate) use batch::refuse_nan;
pub use batch::*;
pub use query::{TransactionQuery, TransactionSelectQuery};
pub(crate) use succession::ReadSettlement;
pub(crate) use succession::settle as settle_successions;
#[cfg(test)]
pub(crate) use succession::stored_claims;

use crate::Commit;
use crate::placement::{Partitioned, Placements};
use crate::repository::branch::asset::store_assets;
use crate::repository::branch::session::Composite;
use crate::repository::branch::session::shared_metadata;
use crate::repository::source::{Live, View};
use crate::rules::{SharedRuleCache, TriggerFootprint, is_rule_attribute, on_attr, reads_attr};
use crate::{Branch, CommitError, PublishError, RemoteSite, Revision, Snapshot, Staged};
use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{AssetChange, Changes, Statement};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::{self, Edition, Publish, Resolve};
use dialog_search_tree::nodes;
use std::slice::from_ref;

/// A transaction on a line of the repository.
///
/// `L` is the [`Line`] the transaction runs on, and it decides what
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
/// Where an asserted or retracted fact lands is the attribute's
/// decision, not the caller's: each instruction routes to the store
/// the scope its attribute is [placed](crate::placement) on is
/// [bound](crate::Bindings) to — the tree by default. A concept whose
/// attributes span scopes fans out accordingly.
///
/// Transients are visible to every read through [`query`](Self::query)
/// and to inductive-rule bodies during commit-time induction, but they
/// never enter the durable batch: they live for exactly one induction
/// round and leave no trace in the committed tree.
///
/// # A transaction reads and commits against one revision
///
/// Opening a transaction pins its line at the head the line stands at
/// then ([`Line::pin`]). Every query of the transaction reads that
/// revision, however a commit through another handle of the branch
/// moves the head meanwhile, and the commit builds on it: a branch
/// transaction's publish CAS's the head cell against the version it
/// read when it opened, so a head that moved since is refused with
/// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch)
/// rather than built over, and a snapshot transaction's advance is
/// refused the same way when a commit through the same handle moved
/// the snapshot. The caller opens a new transaction on the fresh head.
pub struct Transaction<L: Line> {
    line: L,
    /// The head the line stood at when the transaction opened: what
    /// its queries read and its commit builds on.
    pinned: L::Pin,
    /// The durable writes, held so reading them is a range read (see
    /// [`Staged`]); exported to a [`Changes`] batch once, at commit.
    changes: Staged,
    transients: Staged,
}

/// What a [`Transaction`] runs on: a `&`[`Branch`], a `&`[`Snapshot`]
/// or a [`TransactionBatch`]. Sealed; the three lines are the only ones.
///
/// Opening a transaction pins the line (see [`Transaction`]): the pin is
/// what the line has of a head that another handle can move. A branch's
/// is the revision in its head cell together with the cell version the
/// commit's CAS expects; a snapshot's is the revision it holds; a staged
/// chain is owned by its transaction, so nothing else can move it and it
/// pins nothing.
pub trait Line: sealed::Sealed {
    /// What the transaction holds of the line's head from open to
    /// commit.
    type Pin;

    /// Read the line's head now: the one read of it a transaction makes.
    fn pin(&self) -> Self::Pin;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for &crate::Branch {}
    impl Sealed for &crate::Snapshot {}
    impl Sealed for super::TransactionBatch {}
}

impl Line for &Branch {
    type Pin = Option<Edition<Revision>>;

    fn pin(&self) -> Self::Pin {
        // One read of the cell, so the revision built on and the version
        // the publish CAS's against name the same head.
        self.revision.edition()
    }
}

impl Line for &Snapshot {
    type Pin = Revision;

    fn pin(&self) -> Self::Pin {
        self.revision()
    }
}

impl Line for TransactionBatch {
    type Pin = ();

    fn pin(&self) -> Self::Pin {}
}

impl<L: Line> Transaction<L> {
    pub(crate) fn on(line: L) -> Self {
        let pinned = line.pin();
        Transaction {
            line,
            pinned,
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
    /// entry under its pick, `Retract` a retraction entry —
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
    pub fn commit(self) -> TransactionCommit<L> {
        TransactionCommit {
            line: self.line,
            pinned: self.pinned,
            changes: self.changes.clone(),
            transients: self.transients.export(),
            allow_empty: false,
            canonicalize: false,
            amend: false,
        }
    }
}

impl<L: Line> Transaction<L> {
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

impl Transaction<&Branch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the branch, at the revision the transaction opened at.
    ///
    /// Pending asserts and retracts are surfaced through a
    /// [`TransactionQuery`] handle — assertions show up alongside the
    /// stored facts; retractions tombstone matching facts in the stored
    /// stream before the merge. Dispatched transients are part of the
    /// view too. The transaction itself stays open and committable.
    pub fn query(&self) -> TransactionQuery<'_> {
        TransactionQuery::new(self.view(), self.layers())
    }

    /// The branch at the revision this transaction pinned.
    fn view(&self) -> View<'_> {
        View::at(
            Live::Branch(self.line),
            self.pinned.as_ref().map(|edition| &edition.content),
        )
    }
}

impl Transaction<&Snapshot> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the snapshot, at the revision the transaction opened at. See
    /// [`Transaction::<&Branch>::query`].
    pub fn query(&self) -> TransactionQuery<'_> {
        TransactionQuery::new(
            View::at(Live::Snapshot(self.line), Some(&self.pinned)),
            self.layers(),
        )
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
    /// the revision `perform` returns. The transaction reads and builds
    /// on the revision the snapshot holds now (see [`Transaction`]).
    /// Clone first to keep the view you have.
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
pub struct TransactionCommit<L: Line> {
    pub(super) line: L,
    pub(super) pinned: L::Pin,
    pub(super) changes: Staged,
    pub(super) transients: Changes,
    pub(super) allow_empty: bool,
    pub(super) canonicalize: bool,
    pub(super) amend: bool,
}

impl<L: Line> TransactionCommit<L> {
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
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let snapshot = self.line;
        // The revision the transaction opened at: what it read, and
        // what the commit builds on. A commit through the same handle
        // since has moved the snapshot past it, and the advance below
        // would refuse to build over that commit: refuse now, with the
        // same answer, before settling and inducing.
        let previous = self.pinned;
        if snapshot.revision() != previous {
            return Err(PublishError::VersionMismatch {
                expected: Some(memory::Version::from(previous.tree.hash())),
                current: memory::Current::Unknown,
            }
            .into());
        }
        let view = View::at(Live::Snapshot(snapshot), Some(&previous));
        let operator = Identify.perform(env).await?;
        let stores = Placements::stores(&view, &self.changes, env).await?;
        let changes = Box::pin(succession::settle(
            &Composite::of(view.to_source()),
            shared_metadata(from_ref(&view), &operator),
            &self.changes,
            &stores,
            env,
        ))
        .await?;
        // A snapshot commit reports only its revision: the transients
        // induction emitted surface on a staged
        // [`TransactionBatch::induced`] alone.
        let settled = settle(
            &view,
            &Composite::of(view.to_source()),
            changes,
            self.transients,
            env,
        )
        .await?;

        let touches_rules = touches_rules(&settled.tree);
        let touches_rule_facts = touches_rule_facts(&settled.tree);
        let changes = settled.tree;
        let machinery = store_assets(&view, settled.assets, env).await?;
        let rule_cache = view.rule_cache();
        drop(view);

        let mut commit = Commit::new(snapshot, changes.into_stream()).with_machinery(machinery);
        if self.allow_empty {
            commit = commit.allow_empty();
        }
        if self.canonicalize {
            commit = commit.canonicalize();
        }
        // The lineage is read now: it is allocated by the snapshot's first
        // commit, and the advance refuses a head that moved since the
        // transaction opened whichever lineage it was minted on.
        let (_, lineage) = snapshot.head();
        let revision =
            Box::pin(commit.perform_on_snapshot_at(snapshot, previous.clone(), lineage, env))
                .await?;
        // Session-bound instructions land once the tree commit has
        // succeeded, so a failed commit leaves the session untouched too.
        snapshot.overlay().apply(settled.session)?;

        if !touches_rules {
            carry_footprint(&rule_cache, Some(&previous), &revision);
        }
        if !touches_rule_facts {
            rule_cache.carry_rules_root(Some(*previous.tree.hash()), Some(*revision.tree.hash()));
        }
        Ok(revision)
    }
}

/// A transaction's changes after commit-time induction, routed by
/// attribute placement: what the tree commit mints, what the line's
/// session store lands once the commit has succeeded, and the
/// transients rule heads emitted along the way.
pub(crate) struct Settled {
    /// The tree-bound instructions, induction's novelty folded in.
    pub(crate) tree: Changes,
    /// The session-bound instructions.
    pub(crate) session: Changes,
    /// The asset changes, which only the tree commit stores.
    pub(crate) assets: Vec<AssetChange>,
    /// Every transient a rule head emitted, across all rounds.
    pub(crate) induced: Changes,
}

/// Settle a transaction's changes against `view`: run commit-time
/// induction (each round witnessed on `source`'s session store, so a
/// command that fired a rule is seen by the store's observers even
/// though the commit folds it away), then route by attribute
/// placement. Nothing lands here: the caller mints the tree-bound
/// remainder and applies the session-bound part once that succeeded.
pub(crate) async fn settle<Env>(
    source: &View<'_>,
    view: &Composite,
    mut changes: Changes,
    transients: Changes,
    env: &Env,
) -> Result<Settled, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    // The assets ride apart from the facts, for the tree commit alone:
    // induction and routing rebuild the batch fact by fact.
    let assets = changes.take_assets();
    let mut witness = source.overlay();
    let staged = Placements::resolve(source, &changes, env).await?;
    let induced = induce::induce(
        source,
        induce::Watermark::Cell,
        view,
        &staged,
        &induce::Landing::Line(source.bindings()),
        &mut changes,
        transients,
        Changes::new(),
        Vec::new(),
        &mut witness,
        env,
    )
    .await?;
    let placements = Placements::resolve(source, &changes, env).await?;
    let Partitioned { tree, session } = placements.partition(changes, source.bindings())?;
    Ok(Settled {
        tree,
        session,
        assets,
        induced,
    })
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

/// Whether a settled change batch writes a rule fact
/// ([`is_rule_attribute`]): when it does not, the tree it commits holds
/// the rule facts of the tree it was committed onto, which carries its
/// rules root to the new head (see [`RuleCache::rules_root`]).
///
/// [`RuleCache::rules_root`]: crate::RuleCache
pub(crate) fn touches_rule_facts(changes: &Changes) -> bool {
    changes
        .iter()
        .any(|(_, attribute, _)| is_rule_attribute(attribute))
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
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        Box::pin(self.transaction().commit().publish().perform(env)).await
    }
}
