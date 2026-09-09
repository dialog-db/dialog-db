//! What a read or a commit needs from the layer it works on, whether
//! that layer is a [`Branch`] or a [`Snapshot`].
//!
//! The two differ in one thing only: where the head lives. A branch keeps
//! it in a memory cell that advances under CAS and that every handle to
//! the branch shares; a snapshot keeps its own, which moves only through
//! commits on that very handle. Everything a query or a commit does
//! downstream of "which root, which store, which caches" is identical,
//! so it is written once against [`SourceRef`] and both kinds plug in.

use dialog_artifacts::history::{
    CausalityCache, ContextCache, RevisionRecord, TreeHistory, Version, log,
};
use dialog_artifacts::tree::{SpillCache, spill_cache};
use dialog_artifacts::{Changes, DialogArtifactsError, Entity, SpineSlot, Statement as _};
use dialog_capability::{Capability, Fork, Provider, Subject};
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Archive, Get as ArchiveGet, Put as ArchivePut};
use dialog_effects::authority::{Operator, OperatorExt as _};
use dialog_effects::memory::Resolve;
use dialog_query::concept::query::PlanCache;
use dialog_search_tree::{Buffer, Cache};
use dialog_storage::Blake3Hash;
use std::sync::Arc;

use crate::rules::{RuleCache, SharedRuleCache};
use crate::schema::Replica;
use crate::{
    Bindings, Branch, EMPTY_TREE_HASH, Ephemeral, NetworkedIndex, RemoteFallback, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt as _, Revision, Snapshot, Upstream,
};

/// An owned layer to read from: a branch or a snapshot, cheaply cloned
/// (both share their caches by handle). Query environments hold these
/// so the only lifetime they carry is the capability environment's.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// A named layer whose head lives in a memory cell.
    Branch(Branch),
    /// A detached layer whose head is held by value.
    Snapshot(Snapshot),
    /// A branch read at a captured revision rather than its live head:
    /// the branch's caches, remote fallback, session store and
    /// bindings, with the tree root fixed. `None` is a branch captured
    /// before its first commit. What a [`Stack`](crate::Stack) reads
    /// every layer beneath its top as.
    Pinned(Branch, Option<Revision>),
}

impl Source {
    /// Borrow this layer.
    pub(crate) fn as_ref(&self) -> SourceRef<'_> {
        match self {
            Source::Branch(branch) => SourceRef::Branch(branch),
            Source::Snapshot(snapshot) => SourceRef::Snapshot(snapshot),
            Source::Pinned(branch, revision) => SourceRef::Pinned(branch, revision.as_ref()),
        }
    }
}

impl From<Branch> for Source {
    fn from(branch: Branch) -> Self {
        Source::Branch(branch)
    }
}

impl From<Snapshot> for Source {
    fn from(snapshot: Snapshot) -> Self {
        Source::Snapshot(snapshot)
    }
}

/// A borrowed layer to read from. `Copy`, so builders that hold one stay
/// as cheap to pass around as the `&Branch` they used to hold.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SourceRef<'a> {
    /// A named layer whose head lives in a memory cell.
    Branch(&'a Branch),
    /// A detached layer whose head is held by value.
    Snapshot(&'a Snapshot),
    /// A branch read at a captured revision; see [`Source::Pinned`].
    Pinned(&'a Branch, Option<&'a Revision>),
}

impl<'a> From<&'a Branch> for SourceRef<'a> {
    fn from(branch: &'a Branch) -> Self {
        SourceRef::Branch(branch)
    }
}

impl<'a> From<&'a Snapshot> for SourceRef<'a> {
    fn from(snapshot: &'a Snapshot) -> Self {
        SourceRef::Snapshot(snapshot)
    }
}

impl<'a> From<&'a Source> for SourceRef<'a> {
    fn from(source: &'a Source) -> Self {
        source.as_ref()
    }
}

impl<'a> SourceRef<'a> {
    /// An owned handle to the same layer.
    pub(crate) fn to_source(self) -> Source {
        match self {
            SourceRef::Branch(branch) => Source::Branch(branch.clone()),
            SourceRef::Snapshot(snapshot) => Source::Snapshot(snapshot.clone()),
            SourceRef::Pinned(branch, revision) => {
                Source::Pinned(branch.clone(), revision.cloned())
            }
        }
    }

    /// The branch behind this layer, when it is one: a branch read live
    /// or at a captured revision. A snapshot has none.
    pub(crate) fn branch(self) -> Option<&'a Branch> {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => Some(branch),
            SourceRef::Snapshot(_) => None,
        }
    }

    /// The repository this layer lives in.
    pub(crate) fn subject(self) -> Subject {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.subject(),
            SourceRef::Snapshot(snapshot) => snapshot.subject(),
        }
    }

    /// The archive capability for this layer's repository.
    pub(crate) fn archive(self) -> Capability<Archive> {
        self.subject().archive()
    }

    /// The revision this layer currently names, or `None` for a branch
    /// with no commits yet. A snapshot always has one.
    pub(crate) fn revision(self) -> Option<Revision> {
        match self {
            SourceRef::Branch(branch) => branch.revision(),
            SourceRef::Snapshot(snapshot) => Some(snapshot.revision()),
            SourceRef::Pinned(_, revision) => revision.cloned(),
        }
    }

    /// The tree root to read: the revision's, or the empty tree's.
    pub(crate) fn root(self) -> Blake3Hash {
        self.revision()
            .map(|revision| *revision.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH)
    }

    /// The default upstream: a branch's tracked one. A snapshot tracks
    /// nothing, so blob reads through it are local (see
    /// [`SnapshotExport::download`](crate::SnapshotExport::download)
    /// for hydrating one ahead of time).
    pub(crate) fn upstream(self) -> Option<Upstream> {
        self.branch().and_then(Branch::upstream)
    }

    /// The remote block reads fall back to on a local miss: the first
    /// remote among a branch's tracked upstreams (a branch whose default
    /// upstream is local but which tracks a remote must still hydrate
    /// blocks it holds by reference); none for a snapshot.
    ///
    /// A remote that fails to load is carried as
    /// [`RemoteFallback::Unavailable`] rather than dropped: reads the
    /// local archive serves still succeed, and a local miss surfaces the
    /// load failure as its cause instead of a bare not-found.
    pub(crate) async fn fallback<Env>(self, env: &Env) -> RemoteFallback
    where
        Env: Provider<Resolve> + ConditionalSync + 'static,
    {
        let Some(branch) = self.branch() else {
            return RemoteFallback::None;
        };
        let upstreams = branch.upstreams();
        match upstreams.remote_name() {
            Some(name) => {
                let loaded = branch
                    .subject()
                    .remote(name.to_string())
                    .load()
                    .perform(env)
                    .await;
                RemoteFallback::from_load(name, loaded)
            }
            None => RemoteFallback::None,
        }
    }

    /// The shared node cache tree reads go through.
    pub(crate) fn node_cache(self) -> Cache<NodeHash, Buffer> {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.node_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().nodes.clone(),
        }
    }

    /// The shared spilled-value block cache.
    pub(crate) fn spill_cache(self) -> SpillCache {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.spill_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().spills.clone(),
        }
    }

    /// The shared deductive-rule cache.
    pub(crate) fn rule_cache(self) -> SharedRuleCache {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.rule_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().rules.clone(),
        }
    }

    /// The shared query-plan cache.
    pub(crate) fn plan_cache(self) -> PlanCache {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.plan_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().plans.clone(),
        }
    }

    /// The shared verified-record memo.
    pub(crate) fn records(self) -> Cache<Version, RevisionRecord> {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.records(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().records.clone(),
        }
    }

    /// The shared causal-context memo.
    pub(crate) fn contexts(self) -> ContextCache {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.contexts(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().contexts.clone(),
        }
    }

    /// The live-spine slot commits on this layer reuse.
    pub(crate) fn spine(self) -> &'a SpineSlot {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.spine(),
            SourceRef::Snapshot(snapshot) => &snapshot.caches().spine,
        }
    }

    /// The ephemeral layer every read of this layer folds in.
    pub(crate) fn overlay(self) -> &'a Ephemeral {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.overlay(),
            SourceRef::Snapshot(snapshot) => snapshot.overlay(),
        }
    }

    /// The layer bindings a commit on this layer routes by.
    pub(crate) fn bindings(self) -> &'a Bindings {
        match self {
            SourceRef::Branch(branch) | SourceRef::Pinned(branch, _) => branch.bindings(),
            SourceRef::Snapshot(snapshot) => snapshot.bindings(),
        }
    }

    /// Fold this layer's schema metadata into `changes`, returning the
    /// branch entity when the layer is a branch (a
    /// [`SessionBranch`](crate::schema::SessionBranch) row is minted
    /// per branch in scope; a snapshot is not a branch and gets none).
    ///
    /// A branch contributes its full
    /// [`BranchMetadata`](crate::BranchMetadata); a snapshot contributes
    /// the [`Replica`] it is a view of. Its revision is already
    /// queryable through the derived
    /// [`Revision`](crate::schema::Revision) concepts, concluded from
    /// the signed record in the tree.
    pub(crate) fn metadata(
        self,
        operator: &Capability<Operator>,
        changes: &mut Changes,
    ) -> Option<Entity> {
        match self {
            SourceRef::Branch(branch) => {
                let metadata = branch.metadata(operator);
                let entity = metadata.branch.this.clone();
                metadata.assert(changes);
                Some(entity)
            }
            SourceRef::Pinned(branch, revision) => {
                let metadata = branch.metadata_at(operator, revision.cloned());
                let entity = metadata.branch.this.clone();
                metadata.assert(changes);
                Some(entity)
            }
            SourceRef::Snapshot(snapshot) => {
                Replica::new(operator.profile().clone(), snapshot.of().clone()).assert(changes);
                None
            }
        }
    }

    /// The recorded claim lineage at this layer's revision. History
    /// records live in the same tree as the data, so this reads the
    /// history region of the revision's tree. Reads that miss locally
    /// are not fetched from a remote — traversal over unreplicated
    /// history surfaces as `IncompleteHistory`.
    pub(crate) fn history<'e, Env>(self, env: &'e Env) -> TreeHistory<NetworkedIndex<'e, Env>>
    where
        Env: Provider<ArchiveGet>
            + Provider<ArchivePut>
            + Provider<Fork<RemoteSite, ArchiveGet>>
            + ConditionalSync
            + 'static,
    {
        let store = NetworkedIndex::new(env, self.archive().index(), None);
        TreeHistory::from_root_with_cache(&self.root(), store, self.node_cache())
            .with_record_cache(self.records())
    }

    /// This layer's committed history, newest first — at most `limit`
    /// entries of `(version, record)`. See [`Branch::log`].
    pub(crate) async fn log<Env>(
        self,
        env: &Env,
        limit: usize,
    ) -> Result<Vec<(Version, RevisionRecord)>, DialogArtifactsError>
    where
        Env: Provider<ArchiveGet>
            + Provider<ArchivePut>
            + Provider<Fork<RemoteSite, ArchiveGet>>
            + ConditionalSync
            + 'static,
    {
        let Some(head) = self.revision() else {
            return Ok(Vec::new());
        };
        log(&head.version(), &self.history(env), limit).await
    }
}

/// The caches a layer carries between its reads and commits. Every one
/// is content- or version-addressed, so a set may be shared between a
/// branch and the snapshots minted from it, and between a snapshot and
/// the snapshots its transactions produce, without ever serving a
/// stale entry.
#[derive(Debug, Clone)]
pub(crate) struct Caches {
    /// Tree nodes by hash, so blocks one read fetched stay warm for the next.
    pub(crate) nodes: Cache<NodeHash, Buffer>,
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
    /// The live buffered spine between commits, keyed by the root it was
    /// persisted as.
    pub(crate) spine: SpineSlot,
}

impl Caches {
    /// A cold set.
    pub(crate) fn new() -> Self {
        Self {
            nodes: Cache::new(),
            spills: spill_cache(),
            rules: Arc::new(RuleCache::new()),
            plans: PlanCache::default(),
            causality: CausalityCache::new(),
            contexts: ContextCache::new(),
            records: Cache::new(),
            spine: SpineSlot::new(),
        }
    }
}
