//! What a read or a commit needs from the line it works on, whether
//! that line is a [`Branch`] or a [`Snapshot`], split by what each may
//! know of the line's head.
//!
//! The two kinds of line differ in one thing only: where the head
//! lives. A branch keeps it in a memory cell that advances under CAS
//! and that every handle to the branch shares; a snapshot keeps its
//! own, which moves only through commits on that very handle.
//! Everything a query or a commit does downstream of "which root, which
//! store, which caches" is identical, so it is written once and both
//! kinds plug in.
//!
//! What is not identical between a read and a commit is whether the
//! head may be read live. A read is a unit of isolation (a query, a
//! select, one poll of a subscription, a transaction from open to
//! commit) and performs many tree scans; every one must read the
//! revision the unit began with, however a commit through another
//! handle of the branch moves the head meanwhile
//! (dialog-db/dialog-db#757). So the types here are two:
//!
//! - A [`Live`] is the live handle: `&Branch` or `&Snapshot`, where the
//!   head is now. It reaches the line's identity, its caches and its
//!   session overlay (all shared between every handle, and read live by
//!   design), but no tree root. What needs the current head (a commit's
//!   CAS, a subscription deciding whether to re-evaluate) asks the
//!   branch itself.
//! - A [`View`] is a line pinned at a revision: a `Live` and the head it
//!   stood at when the view was taken. Every read API takes a view, so a
//!   read that follows the head cannot be written. A view is taken from
//!   a live line by [`Live::pin`], the one place a revision is read from
//!   a live head, or built on a revision the caller already holds (a
//!   transaction's, a staged chain's).
//!
//! [`Source`] is a view that owns its line, for the query environments,
//! whose only lifetime is the capability environment's.

use dialog_capability::{Capability, Provider, Subject};
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync, Holds};
use dialog_corpus::history::{
    CausalityCache, ContextCache, RevisionRecord, TreeHistory, Version, log,
};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::tree::{CorpusCache, SpillCache};
use dialog_corpus::{Changes, DialogCorpusError, Entity, SpineSlot, Statement as _};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
use dialog_effects::authority::{Operator, OperatorExt as _};
use dialog_effects::blob::Load as BlobLoad;
use dialog_query::concept::query::PlanCache;
use dialog_search_tree::Cache;
use dialog_search_tree::nodes;
use dialog_storage::Blake3Hash;
use std::borrow::Cow;
use std::sync::Arc;

use crate::repository::branch::BranchMetadata;
use crate::repository::branch::LayerMetadataMemo;
use crate::repository::caches::CacheBudget;
use crate::rules::{RuleCache, RuleContent, SharedRuleCache};
use crate::schema::Replica;
use crate::{Bindings, Branch, Ephemeral, Index, Revision, Snapshot};

/// The live handle of a line: a branch or a snapshot, where its head is
/// now. `Copy`, so builders that hold one stay as cheap to pass around
/// as the `&Branch` they used to hold.
///
/// A live line reaches what every handle of the line shares and reads
/// live by design: its identity, its caches, its session overlay. It
/// reaches no tree root: a read takes a [`View`] with [`pin`](Self::pin)
/// first, and reads that.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Live<'a> {
    /// A named line whose head lives in a memory cell.
    Branch(&'a Branch),
    /// A detached line whose head is held by value.
    Snapshot(&'a Snapshot),
}

impl<'a> From<&'a Branch> for Live<'a> {
    fn from(branch: &'a Branch) -> Self {
        Live::Branch(branch)
    }
}

impl<'a> From<&'a Snapshot> for Live<'a> {
    fn from(snapshot: &'a Snapshot) -> Self {
        Live::Snapshot(snapshot)
    }
}

impl<'a> Live<'a> {
    /// Pin this line: a view of it at the head it stands at now.
    ///
    /// This is the one place a read source takes a revision from a live
    /// head. Everything the view then reads names that revision, however
    /// the head moves afterwards. A branch with no commits yet pins to
    /// no revision, and reads as the empty tree.
    pub(crate) fn pin(self) -> View<'a> {
        let revision = match self {
            Live::Branch(branch) => branch.revision(),
            Live::Snapshot(snapshot) => Some(snapshot.revision()),
        };
        View {
            live: self,
            revision: revision.map(Cow::Owned),
        }
    }

    /// The branch behind this line, for what only a branch has: its
    /// tracking, its induction watermark, the head cell a commit CAS's.
    pub(crate) fn branch(self) -> Option<&'a Branch> {
        match self {
            Live::Branch(branch) => Some(branch),
            Live::Snapshot(_) => None,
        }
    }

    /// The snapshot behind this line, when it is one.
    pub(crate) fn snapshot(self) -> Option<&'a Snapshot> {
        match self {
            Live::Branch(_) => None,
            Live::Snapshot(snapshot) => Some(snapshot),
        }
    }

    /// The repository this line lives in.
    pub(crate) fn subject(self) -> Subject {
        match self {
            Live::Branch(branch) => branch.subject(),
            Live::Snapshot(snapshot) => snapshot.subject(),
        }
    }

    /// The archive capability for this line's repository.
    pub(crate) fn archive(self) -> ArchiveScope {
        ArchiveScope::new(self.subject())
    }

    /// The catalog this line's tree keeps its blocks in: its repository's
    /// archive index.
    pub(crate) fn catalog(self) -> CatalogScope {
        self.archive().index()
    }

    /// The transient session overlay every read of this line folds in.
    /// Read live, as between any two handles of the line: it is a line
    /// of its own, named by its revision.
    pub(crate) fn overlay(self) -> &'a Ephemeral {
        match self {
            Live::Branch(branch) => branch.overlay(),
            Live::Snapshot(snapshot) => snapshot.overlay(),
        }
    }

    /// The scope bindings a commit on this line routes by.
    pub(crate) fn bindings(self) -> &'a Bindings {
        match self {
            Live::Branch(branch) => branch.bindings(),
            Live::Snapshot(snapshot) => snapshot.bindings(),
        }
    }

    /// The shared spilled-value block cache.
    pub(crate) fn spill_cache(self) -> SpillCache {
        match self {
            Live::Branch(branch) => branch.spill_cache(),
            Live::Snapshot(snapshot) => snapshot.caches().spills.clone(),
        }
    }

    /// The shared deductive-rule cache.
    pub(crate) fn rule_cache(self) -> SharedRuleCache {
        match self {
            Live::Branch(branch) => branch.rule_cache(),
            Live::Snapshot(snapshot) => snapshot.caches().rules.clone(),
        }
    }

    /// The shared query-plan cache.
    pub(crate) fn plan_cache(self) -> PlanCache {
        match self {
            Live::Branch(branch) => branch.plan_cache(),
            Live::Snapshot(snapshot) => snapshot.caches().plans.clone(),
        }
    }

    /// The shared verified-record memo.
    pub(crate) fn records(self) -> Cache<Version, RevisionRecord> {
        match self {
            Live::Branch(branch) => branch.records(),
            Live::Snapshot(snapshot) => snapshot.caches().records.clone(),
        }
    }

    /// The shared causal-context memo.
    pub(crate) fn contexts(self) -> ContextCache {
        match self {
            Live::Branch(branch) => branch.contexts(),
            Live::Snapshot(snapshot) => snapshot.caches().contexts.clone(),
        }
    }

    /// The live-spine slot commits on this line reuse.
    pub(crate) fn spine(self) -> &'a SpineSlot {
        match self {
            Live::Branch(branch) => branch.spine(),
            Live::Snapshot(snapshot) => &snapshot.caches().spine,
        }
    }

    /// This line held by value, for a [`Source`].
    fn to_owned(self) -> Held {
        match self {
            Live::Branch(branch) => Held::Branch(Arc::new(branch.clone())),
            Live::Snapshot(snapshot) => Held::Snapshot(Arc::new(snapshot.clone())),
        }
    }
}

/// A line pinned at a revision: what every read reads.
///
/// Holds the [`Live`] line, for its caches, its session overlay and its
/// tracking (shared and live, as between any two handles), and the
/// revision the line stood at when the view was taken (`None` for a
/// branch with no commits yet, whose tree is the empty one). Every scan
/// through the view reads that revision's tree, however a commit
/// through another handle of the branch moves the head meanwhile.
///
/// The revision is borrowed when whoever took the view holds it (a
/// transaction, a [`Source`]) and owned when the view was taken from the
/// live head, so a view borrowed from a source costs nothing per scan.
#[derive(Debug, Clone)]
pub(crate) struct View<'a> {
    live: Live<'a>,
    revision: Option<Cow<'a, Revision>>,
}

impl<'a> View<'a> {
    /// A view of `live` at `revision`, which the caller holds: what a
    /// transaction reads throughout, at the head it pinned when it
    /// opened. `None` is a branch with no commits yet; a snapshot always
    /// has a revision.
    pub(crate) fn at(live: Live<'a>, revision: Option<&'a Revision>) -> Self {
        Self {
            live,
            revision: revision.map(Cow::Borrowed),
        }
    }

    /// The branch behind this view, when its line is one.
    pub(crate) fn branch(&self) -> Option<&'a Branch> {
        self.live.branch()
    }

    /// The revision this view names: `None` for a branch with no
    /// commits yet.
    pub(crate) fn revision(&self) -> Option<&Revision> {
        self.revision.as_deref()
    }

    /// This view owning its line: the branch or snapshot is cloned once,
    /// and the revision is moved when the view owns it.
    pub(crate) fn into_source(self) -> Source {
        Source {
            held: self.live.to_owned(),
            revision: self.revision.map(Cow::into_owned),
            follows: false,
        }
    }

    /// This view owning its line, the revision copied.
    pub(crate) fn to_source(&self) -> Source {
        self.clone().into_source()
    }

    /// The repository this line lives in.
    pub(crate) fn subject(&self) -> Subject {
        self.live.subject()
    }

    /// The archive capability for this line's repository.
    pub(crate) fn archive(&self) -> ArchiveScope {
        self.live.archive()
    }

    /// The catalog this line's tree keeps its blocks in.
    pub(crate) fn catalog(&self) -> CatalogScope {
        self.live.catalog()
    }

    /// The tree root to read: the revision's, or `None` for a branch with
    /// no commits yet, which has no tree at all.
    pub(crate) fn root(&self) -> Option<Blake3Hash> {
        self.revision().map(|revision| *revision.tree.hash())
    }

    /// This line's tree, in its catalog: the revision's, or the empty tree
    /// for a branch with no commits yet.
    pub(crate) fn tree(&self) -> Index {
        match self.root() {
            Some(root) => Index::at(self.catalog(), NodeHash::from(root)),
            None => Index::empty_in(self.catalog()),
        }
    }

    /// The branch's metadata as of the revision this view names, for a
    /// line that is a branch; a snapshot has none.
    pub(crate) fn branch_metadata(
        &self,
        operator: &Capability<Operator>,
    ) -> Option<BranchMetadata> {
        self.branch()
            .map(|branch| branch.metadata_at(operator, self.revision()))
    }

    /// The line's session overlay, read live (see [`Live::overlay`]).
    pub(crate) fn overlay(&self) -> &'a Ephemeral {
        self.live.overlay()
    }

    /// The scope bindings a commit on this line routes by.
    pub(crate) fn bindings(&self) -> &'a Bindings {
        self.live.bindings()
    }

    /// The shared spilled-value block cache.
    pub(crate) fn spill_cache(&self) -> SpillCache {
        self.live.spill_cache()
    }

    /// The shared deductive-rule cache.
    pub(crate) fn rule_cache(&self) -> SharedRuleCache {
        self.live.rule_cache()
    }

    /// The shared query-plan cache.
    pub(crate) fn plan_cache(&self) -> PlanCache {
        self.live.plan_cache()
    }

    /// The shared verified-record memo.
    pub(crate) fn records(&self) -> Cache<Version, RevisionRecord> {
        self.live.records()
    }

    /// The shared causal-context memo.
    pub(crate) fn contexts(&self) -> ContextCache {
        self.live.contexts()
    }

    /// The live-spine slot commits on this line reuse.
    pub(crate) fn spine(&self) -> &'a SpineSlot {
        self.live.spine()
    }

    /// Fold this line's schema metadata into `changes`, returning the
    /// branch entity when the line is a branch (a
    /// [`SessionBranch`](crate::schema::SessionBranch) row is minted
    /// per branch in scope; a snapshot is not a branch and gets none).
    ///
    /// A branch contributes its full
    /// [`BranchMetadata`](crate::BranchMetadata) as of the revision this
    /// view names, with the [`BranchRevision`](crate::BranchRevision)
    /// (the only part that moves with the head) folded into `heads`; a
    /// snapshot contributes the [`Replica`] it is a view of. Its
    /// revision is already queryable through the derived
    /// [`Revision`](crate::schema::Revision) concepts, concluded from
    /// the signed record in the tree.
    pub(crate) fn metadata(
        &self,
        operator: &Capability<Operator>,
        changes: &mut Changes,
        heads: &mut Changes,
    ) -> Option<Entity> {
        match self.branch_metadata(operator) {
            Some(metadata) => {
                let entity = metadata.branch.this.clone();
                metadata.replica.assert(changes);
                metadata.branch.assert(changes);
                if let Some(revision) = metadata.revision {
                    revision.assert(heads);
                }
                Some(entity)
            }
            None => {
                let subject = self.subject();
                Replica::new(operator.profile().clone(), subject.did().clone()).assert(changes);
                None
            }
        }
    }

    /// The recorded claim lineage at this view's revision. History
    /// records live in the same tree as the data, so this reads the
    /// history region of the revision's tree.
    ///
    /// A read that misses locally hydrates through the env from the
    /// remotes the repository's open branches track, exactly as a fact
    /// read does (see [`Routes`](crate::Routes)), so a replica that materialized only the operational regions
    /// hydrates the history it turns out to need instead of failing with
    /// `IncompleteHistory`. A line tracking no remote reads purely
    /// locally, so an offline replica behaves as it always did.
    pub(crate) fn history<'e, Env>(&self, env: &'e Env) -> TreeHistory<&'e Env>
    where
        Env: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSync
            + ?Sized,
    {
        let history = match self.root() {
            Some(root) => TreeHistory::from_root(self.catalog(), &root, env),
            // No revision, no tree, no records.
            None => TreeHistory::empty(self.catalog(), env),
        };
        history.with_record_cache(self.records())
    }

    /// This view's committed history, newest first — at most `limit`
    /// entries of `(version, record)`. See [`Branch::log`].
    pub(crate) async fn log<Env>(
        &self,
        env: &Env,
        limit: usize,
    ) -> Result<Vec<(Version, RevisionRecord)>, DialogCorpusError>
    where
        Env: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSync
            + ?Sized,
    {
        let Some(head) = self.revision() else {
            return Ok(Vec::new());
        };
        log(&head.version(), &self.history(env), limit).await
    }
}

/// A line held by value, behind a [`Source`].
#[derive(Debug, Clone)]
enum Held {
    Branch(Arc<Branch>),
    Snapshot(Arc<Snapshot>),
}

/// A pinned line that owns its branch or snapshot: a [`View`] whose only
/// lifetime is the capability environment's. Query environments hold
/// these; a query hands one to every scan it runs through
/// [`as_view`](Self::as_view), which borrows the revision rather than
/// copying it.
///
/// Pinned like every view: the revision is taken when the source is
/// made (from the live head by [`From<Branch>`] and [`From<Snapshot>`],
/// or carried over from a view), and every read through the source
/// names it. The line's caches, session overlay and tracking are read
/// live, as between any two clones.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    held: Held,
    revision: Option<Revision>,
    /// Whether a standing read pins this source afresh at every poll
    /// ([`refreshed`](Self::refreshed)): a line it follows, rather than
    /// one pinned at a revision the caller holds.
    follows: bool,
}

impl Source {
    /// `branch` pinned at `revision`, a head the caller captured rather
    /// than the one the branch stands at now: what a
    /// [`Stack`](crate::Stack) reads a layer beneath its top as. `None`
    /// is a branch captured before its first commit.
    pub(crate) fn pinned(branch: Branch, revision: Option<Revision>) -> Self {
        Source {
            held: Held::Branch(Arc::new(branch)),
            revision,
            follows: false,
        }
    }

    /// `live` pinned at the head it stands at now, for a standing read
    /// that follows it: each poll pins it afresh with
    /// [`refreshed`](Self::refreshed) and reads that one revision
    /// throughout.
    pub(crate) fn following(live: Live<'_>) -> Self {
        Source {
            follows: true,
            ..live.pin().into_source()
        }
    }

    /// This source as a standing read's next poll reads it: a line it
    /// follows pinned at the head it stands at now, any other source as
    /// it is.
    pub(crate) fn refreshed(&self) -> Self {
        if self.follows {
            Self::following(self.live())
        } else {
            self.clone()
        }
    }

    /// The line held, borrowed.
    fn live(&self) -> Live<'_> {
        match &self.held {
            Held::Branch(branch) => Live::Branch(branch),
            Held::Snapshot(snapshot) => Live::Snapshot(snapshot),
        }
    }

    /// Borrow this source as a view, its revision borrowed.
    pub(crate) fn as_view(&self) -> View<'_> {
        View::at(self.live(), self.revision.as_ref())
    }

    /// The revision this source names: `None` for a branch with no
    /// commits yet.
    pub(crate) fn revision(&self) -> Option<&Revision> {
        self.revision.as_ref()
    }

    /// The line's session overlay, read live (see [`Live::overlay`]).
    pub(crate) fn overlay(&self) -> &Ephemeral {
        self.live().overlay()
    }

    /// This line with `store` as its session store, read at the same head:
    /// what a read sees of the line once a commit lands its session writes.
    /// The line itself keeps its store.
    pub(crate) fn under(&self, store: Ephemeral) -> Source {
        let held = match &self.held {
            Held::Branch(branch) => Held::Branch(Arc::new(branch.under(store))),
            Held::Snapshot(snapshot) => Held::Snapshot(Arc::new(snapshot.under(store))),
        };
        Source {
            held,
            revision: self.revision.clone(),
            follows: self.follows,
        }
    }
}

impl From<Branch> for Source {
    /// Pin `branch` at the head it stands at now (see [`Live::pin`]).
    fn from(branch: Branch) -> Self {
        let revision = branch.revision();
        Source {
            held: Held::Branch(Arc::new(branch)),
            revision,
            follows: false,
        }
    }
}

impl From<Snapshot> for Source {
    /// Pin `snapshot` at the revision it holds now.
    fn from(snapshot: Snapshot) -> Self {
        let revision = Some(snapshot.revision());
        Source {
            held: Held::Snapshot(Arc::new(snapshot)),
            revision,
            follows: false,
        }
    }
}

/// The caches a line carries between its reads and commits. Every one
/// is content- or version-addressed, so a set may be shared between a
/// branch and the snapshots minted from it, and between a snapshot and
/// the snapshots its transactions produce, without ever serving a
/// stale entry.
#[derive(Debug, Clone)]
pub(crate) struct Caches {
    /// Tree nodes by hash, so blocks one read fetched stay warm for the next.
    pub(crate) nodes: CorpusCache,
    /// Spilled value blocks by content reference.
    pub(crate) spills: SpillCache,
    /// Deductive-rule discovery (by head) and hydrated bodies (by entity).
    pub(crate) rules: SharedRuleCache,
    /// Query plans by content-addressed `(rule, adornment)`.
    pub(crate) plans: PlanCache,
    /// Causal verdicts between fixed claims or revisions.
    pub(crate) causality: CausalityCache,
    /// Causal contexts by head version.
    pub(crate) contexts: ContextCache,
    /// Verified revision records by version.
    pub(crate) records: Cache<Version, RevisionRecord>,
    /// Trees rewritten under the current number band, by the root of the
    /// tree they rewrite.
    pub(crate) rewrites: Cache<dialog_common::Blake3Hash, dialog_common::Blake3Hash>,
    /// The live buffered spine between commits, keyed by the root it was
    /// persisted as.
    pub(crate) spine: SpineSlot,
    /// The metadata a query over the branch alone folds in (see
    /// [`Branch::layer_metadata`](crate::Branch)), so a branch opened again
    /// does not derive and key it again.
    pub(crate) metadata: LayerMetadataMemo,
    /// How large the caches of a set derived from this one (a transaction
    /// batch's) may grow.
    pub(crate) limits: CacheBudget,
}

impl Caches {
    /// A cold set of its own, every cache at the size `limits` gives it.
    pub(crate) fn sized(limits: CacheBudget) -> Self {
        let entries = limits.entry_capacity();
        Self {
            nodes: dialog_search_tree::NodeCache::with_budget(limits.node_bytes()),
            spills: SpillCache::with_budget(limits.spill_bytes()),
            rules: Arc::new(RuleCache::sharing(Arc::new(RuleContent::with_capacity(
                limits.rule_capacity(),
            )))),
            plans: PlanCache::with_capacity(limits.plan_capacity()),
            causality: CausalityCache::with_capacity(entries),
            contexts: ContextCache::with_capacity(entries),
            records: Cache::with_capacity(entries),
            rewrites: Cache::with_capacity(entries),
            spine: SpineSlot::new(),
            metadata: LayerMetadataMemo::default(),
            limits,
        }
    }
}
