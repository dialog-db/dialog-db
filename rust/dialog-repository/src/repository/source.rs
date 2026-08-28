//! What a read or a commit needs from the line it works on, whether
//! that line is a [`Branch`] or a [`Snapshot`].
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
    Branch, EMPTY_TREE_HASH, NetworkedIndex, Overlay, RemoteFallback, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt as _, Revision, Snapshot, Upstream,
};

/// An owned line to read from: a branch or a snapshot, cheaply cloned
/// (both share their caches by handle). Query environments hold these
/// so the only lifetime they carry is the capability environment's.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// A named line whose head lives in a memory cell.
    Branch(Branch),
    /// A detached line whose head is held by value.
    Snapshot(Snapshot),
}

impl Source {
    /// Borrow this line.
    pub(crate) fn as_ref(&self) -> SourceRef<'_> {
        match self {
            Source::Branch(branch) => SourceRef::Branch(branch),
            Source::Snapshot(snapshot) => SourceRef::Snapshot(snapshot),
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

/// A borrowed line to read from. `Copy`, so builders that hold one stay
/// as cheap to pass around as the `&Branch` they used to hold.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SourceRef<'a> {
    /// A named line whose head lives in a memory cell.
    Branch(&'a Branch),
    /// A detached line whose head is held by value.
    Snapshot(&'a Snapshot),
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
    /// An owned handle to the same line.
    pub(crate) fn to_source(self) -> Source {
        match self {
            SourceRef::Branch(branch) => Source::Branch(branch.clone()),
            SourceRef::Snapshot(snapshot) => Source::Snapshot(snapshot.clone()),
        }
    }

    /// The repository this line lives in.
    pub(crate) fn subject(self) -> Subject {
        match self {
            SourceRef::Branch(branch) => branch.subject(),
            SourceRef::Snapshot(snapshot) => snapshot.subject(),
        }
    }

    /// The archive capability for this line's repository.
    pub(crate) fn archive(self) -> Capability<Archive> {
        self.subject().archive()
    }

    /// The revision this line currently names, or `None` for a branch
    /// with no commits yet. A snapshot always has one.
    pub(crate) fn revision(self) -> Option<Revision> {
        match self {
            SourceRef::Branch(branch) => branch.revision(),
            SourceRef::Snapshot(snapshot) => Some(snapshot.revision()),
        }
    }

    /// The tree root to read: the revision's, or the empty tree's.
    pub(crate) fn root(self) -> Blake3Hash {
        match self {
            SourceRef::Branch(branch) => branch
                .revision()
                .map(|revision| *revision.tree.hash())
                .unwrap_or(EMPTY_TREE_HASH),
            SourceRef::Snapshot(snapshot) => *snapshot.revision().tree.hash(),
        }
    }

    /// The default upstream: a branch's tracked one. A snapshot tracks
    /// nothing, so blob reads through it are local (see
    /// [`SnapshotExport::download`](crate::SnapshotExport::download)
    /// for hydrating one ahead of time).
    pub(crate) fn upstream(self) -> Option<Upstream> {
        match self {
            SourceRef::Branch(branch) => branch.upstream(),
            SourceRef::Snapshot(_) => None,
        }
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
        let SourceRef::Branch(branch) = self else {
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
            SourceRef::Branch(branch) => branch.node_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().nodes.clone(),
        }
    }

    /// The shared spilled-value block cache.
    pub(crate) fn spill_cache(self) -> SpillCache {
        match self {
            SourceRef::Branch(branch) => branch.spill_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().spills.clone(),
        }
    }

    /// The shared deductive-rule cache.
    pub(crate) fn rule_cache(self) -> SharedRuleCache {
        match self {
            SourceRef::Branch(branch) => branch.rule_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().rules.clone(),
        }
    }

    /// The shared query-plan cache.
    pub(crate) fn plan_cache(self) -> PlanCache {
        match self {
            SourceRef::Branch(branch) => branch.plan_cache(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().plans.clone(),
        }
    }

    /// The shared verified-record memo.
    pub(crate) fn records(self) -> Cache<Version, RevisionRecord> {
        match self {
            SourceRef::Branch(branch) => branch.records(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().records.clone(),
        }
    }

    /// The shared causal-context memo.
    pub(crate) fn contexts(self) -> ContextCache {
        match self {
            SourceRef::Branch(branch) => branch.contexts(),
            SourceRef::Snapshot(snapshot) => snapshot.caches().contexts.clone(),
        }
    }

    /// The live-spine slot commits on this line reuse.
    pub(crate) fn spine(self) -> &'a SpineSlot {
        match self {
            SourceRef::Branch(branch) => branch.spine(),
            SourceRef::Snapshot(snapshot) => &snapshot.caches().spine,
        }
    }

    /// The transient session overlay every read of this line folds in.
    pub(crate) fn overlay(self) -> &'a Overlay {
        match self {
            SourceRef::Branch(branch) => branch.overlay(),
            SourceRef::Snapshot(snapshot) => snapshot.overlay(),
        }
    }

    /// Fold this line's schema metadata into `changes`, returning the
    /// branch entity when the line is a branch (a
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
            SourceRef::Snapshot(snapshot) => {
                Replica::new(operator.profile().clone(), snapshot.of().clone()).assert(changes);
                None
            }
        }
    }

    /// The recorded claim lineage at this line's revision. History
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

    /// This line's committed history, newest first — at most `limit`
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

/// The caches a line carries between its reads and commits.
///
/// The content- and version-addressed ones — nodes, spills, plans,
/// causality, contexts, records — are shared by handle between a branch
/// and the snapshots minted from it: their keys are hashes or
/// [`Version`]s, so a shared entry is never stale.
///
/// [`rules`](Self::rules) and [`spine`](Self::spine) are not shared, and
/// [`Branch::caches`] hands a snapshot fresh ones. Both are single-slot:
/// the rule cache keeps one head-tagged entry per key (one trigger
/// footprint in total) and the spine slot one live buffered tree. A
/// mismatched tag only misses, so sharing them would stay *correct* —
/// but the two lines' heads diverge on the snapshot's first commit, and
/// from there each write by one line evicts the other's.
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
