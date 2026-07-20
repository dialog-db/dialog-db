use super::memory::Cell;
use crate::rules::SharedRuleCache;
use crate::{ResolveError, Revision};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_query::concept::query::PlanCache;

use crate::RevisionExt as _;
use crate::{NetworkedIndex, RemoteSite, RepositoryArchiveExt as _};
use dialog_artifacts::DialogArtifactsError;
use dialog_artifacts::history::{
    CausalityCache, ContextCache, RevisionRecord, TreeHistory, Version, log,
};
use dialog_artifacts::tree::SpillCache;
use dialog_artifacts::{Exporter, Importer};
use dialog_capability::Fork;
use dialog_capability::{Capability, Did, Subject};
use dialog_common::Blake3Hash;
use dialog_effects::archive::Archive;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get as ArchiveGet, Put as ArchivePut};
use dialog_query::query::Application;
use dialog_search_tree::{Buffer, Cache};

mod blob;
pub use blob::*;

mod claims;
pub use claims::*;

mod commit;
pub use commit::*;

mod export;
pub use export::*;

mod fetch;
pub use fetch::*;

mod import;
pub use import::*;

mod load;
pub use load::*;

mod metadata;

mod open;
pub use open::*;

mod overlay;
pub use overlay::*;

mod pull;
pub use pull::*;

mod push;
pub use push::*;

mod reference;
pub use reference::*;

mod reset;
pub use reset::*;

mod select;
pub use select::*;

mod session;
pub use session::*;

mod subscription;
pub use subscription::*;

mod set_upstream;
pub use set_upstream::*;

mod transaction;
pub use transaction::*;

mod upstream;
pub use upstream::*;

#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;

#[cfg(all(test, feature = "integration-tests"))]
mod read_amplification;

/// Type alias for the search tree index.
pub type Index = dialog_artifacts::Index;

/// A branch represents a named line of development within a repository.
///
/// Holds a [`BranchReference`] (scoped to `branch/{name}`) plus cells
/// for the branch's latest revision and optional upstream tracking.
#[derive(Debug, Clone)]
pub struct Branch {
    reference: BranchReference,
    revision: Cell<Revision>,
    upstream: Cell<Upstreams>,
    /// Shared node cache for tree reads. Created once per opened branch and
    /// carried (as a shared handle) into every `Select`'s tree, so blocks read
    /// by one query stay warm for the next instead of being re-fetched from
    /// storage. Content-addressed keys make sharing across revisions safe.
    node_cache: Cache<Blake3Hash, Buffer>,
    /// Shared cache of spilled value blocks, keyed by their 32-byte content
    /// reference. Like `node_cache`, created once per opened branch and carried
    /// into every select so a repeated read of the same large (spilled) value
    /// skips the store fetch. Content-addressed, so it never serves stale bytes.
    spill_cache: SpillCache,
    /// Shared deductive-rule cache (discovery by head + hydrated bodies).
    /// Like `node_cache`, created once per opened branch and carried into
    /// every query's durable rule resolution, so the `db.rule/*` scan is
    /// paid once per (concept, head) rather than per query.
    rule_cache: SharedRuleCache,
    /// Transient session overlay: ephemeral facts folded into every
    /// read of this branch, never committed. Shared across clones
    /// like the caches; mutations bump an epoch subscriptions gate
    /// on. See [`Overlay`].
    overlay: Overlay,
    /// Shared plan cache for the deductive rules resolved on this branch,
    /// keyed by content-addressed `(rule, adornment)`. Handed to each
    /// per-query `ConceptRules` assembly so a re-assembled rule set reuses
    /// plans an earlier query computed. Content-addressed keys make it
    /// safe across revisions, like `node_cache`.
    plan_cache: PlanCache,
    /// Shared memo of causal verdicts — claim causality and common
    /// ancestors — resolved against this branch's history. A verdict
    /// between fixed claims or revisions is immutable (append-only
    /// history can only extend the DAG above them), so entries never
    /// invalidate and one DAG walk serves every later query,
    /// transaction, or pull that asks the same question. See
    /// [`CausalityCache`].
    causality_cache: CausalityCache,
    /// Shared memo of causal contexts, keyed by head version. The
    /// context of a fixed head is immutable, like causal verdicts, so
    /// entries never invalidate. Pull reads the local head's context
    /// from here (falling back to the O(ancestry) walk once), and
    /// commit/pull insert the successor head's context derived
    /// incrementally — so steady-state sync never re-walks the DAG.
    /// See [`ContextCache`].
    context_cache: ContextCache,
    /// Shared memo of verified revision records, keyed by version. A
    /// version's record is immutable, so entries never invalidate; a
    /// hit spares the tree read, the decode, and the Ed25519
    /// verification that otherwise run on every ancestry step (skip
    /// extension, context walks, causality).
    record_cache: dialog_search_tree::Cache<Version, RevisionRecord>,
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &str {
        self.reference.name()
    }

    /// Returns the current revision of this branch, or `None` if the branch
    /// has no commits yet (equivalent to an orphan branch in git).
    pub fn revision(&self) -> Option<Revision> {
        self.revision.content()
    }

    /// Returns the default upstream — the target of a bare pull/push/fetch —
    /// or `None` if no upstream is configured.
    pub fn upstream(&self) -> Option<Upstream> {
        self.upstreams().default_upstream().cloned()
    }

    /// Returns every configured upstream tracking entry, default first. A
    /// branch can track several upstreams and pull from / push to any of
    /// them — see [`Pull::from`](crate::Pull::from) and
    /// [`Push::to`](crate::Push::to).
    pub fn upstreams(&self) -> Upstreams {
        self.upstream.content().unwrap_or_default()
    }

    /// Re-resolve this handle's head and upstream from storage, updating its
    /// caches to the current versions.
    ///
    /// The recovery path for a stale handle. [`pull`](Self::pull) publishes the
    /// new head CAS'd against the version it merged from; if a concurrent write
    /// advanced the head in between, that publish fails with a version mismatch
    /// rather than clobbering the concurrent change. A caller that hits such a
    /// mismatch calls `refresh` to pick up the current head, then re-pulls —
    /// the re-pull merges from the now-current snapshot and its blocks are
    /// already in the local archive, so it does not re-hit the network for what
    /// the first attempt already fetched.
    pub async fn refresh<Env>(&self, env: &Env) -> Result<(), ResolveError>
    where
        Env: Provider<memory::Resolve> + ConditionalSync,
    {
        self.revision.resolve().perform(env).await?;
        self.upstream.resolve().perform(env).await?;
        Ok(())
    }

    /// Returns the DID of the host repository.
    pub fn of(&self) -> &Did {
        self.reference.of()
    }

    /// The subject (repository) this branch lives in.
    pub fn subject(&self) -> Subject {
        self.reference.subject()
    }

    /// Archive capability for this branch's subject.
    pub fn archive(&self) -> Capability<Archive> {
        self.subject().archive()
    }

    /// The recorded claim lineage at this branch's current revision, which
    /// powers claim-level conflict detection (see
    /// [`dialog_artifacts::history::causality`]).
    ///
    /// History records live in the same tree as the data, so this reads the
    /// history region of the current revision's tree. Reads that miss
    /// locally are not fetched from a remote — traversal over unreplicated
    /// history surfaces as `IncompleteHistory`.
    pub fn history<'a, Env>(&self, env: &'a Env) -> TreeHistory<NetworkedIndex<'a, Env>>
    where
        Env: Provider<ArchiveGet>
            + Provider<ArchivePut>
            + Provider<Fork<RemoteSite, ArchiveGet>>
            + ConditionalSync
            + 'static,
    {
        let store = NetworkedIndex::new(env, self.archive().index(), None);
        let root = self
            .revision()
            .map(|revision| *revision.tree.hash())
            .unwrap_or(crate::EMPTY_TREE_HASH);
        TreeHistory::from_root_with_cache(&root, store, self.node_cache())
            .with_record_cache(self.records())
    }

    /// The branch's committed history, newest first — at most `limit`
    /// entries of `(version, record)`, every revision before any of its
    /// ancestors (see [`dialog_artifacts::history::log`]). A branch with
    /// no commits logs nothing; unreplicated ancestry truncates the walk
    /// rather than failing it. Each record was verified on read, so the
    /// attribution it reports is the issuer's own signed claim.
    pub async fn log<Env>(
        &self,
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

    /// Export all artifacts from this branch to the given exporter.
    pub fn export<E: Exporter>(&self, exporter: E) -> Export<'_, E> {
        Export::new(self, exporter)
    }

    /// Import artifacts into this branch from the given importer.
    ///
    /// Each artifact read from the importer is committed as an assertion.
    pub fn import<I: Importer>(&self, importer: I) -> Import<'_, I> {
        Import::new(self, importer)
    }

    /// Query with an application. Shortcut for `branch.query().select(query)`.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery::new(self, query)
    }

    /// A shared handle to this branch's node cache, for seeding a read tree.
    pub(crate) fn node_cache(&self) -> Cache<Blake3Hash, Buffer> {
        self.node_cache.clone()
    }

    /// A shared handle to this branch's spilled-value block cache, handed to
    /// each select so spilled reads stay warm across queries.
    pub(crate) fn spill_cache(&self) -> SpillCache {
        self.spill_cache.clone()
    }

    /// A shared handle to this branch's deductive-rule cache.
    pub(crate) fn rule_cache(&self) -> SharedRuleCache {
        self.rule_cache.clone()
    }

    /// A shared handle to this branch's deductive-rule plan cache.
    pub(crate) fn plan_cache(&self) -> PlanCache {
        self.plan_cache.clone()
    }

    /// A shared handle to this branch's causal-verdict memo. Resolve
    /// conflicts through it — `branch.causality().causality(a, b,
    /// &branch.history(env))` — and the DAG walk behind a verdict is
    /// paid once per distinct question rather than once per caller.
    pub fn causality(&self) -> CausalityCache {
        self.causality_cache.clone()
    }

    /// A shared handle to this branch's causal-context memo. Pull reads
    /// the local head's context through it (one O(ancestry) walk on the
    /// first miss) and writes the successor head's context back, derived
    /// incrementally — so steady-state sync never re-walks the DAG.
    pub fn contexts(&self) -> ContextCache {
        self.context_cache.clone()
    }

    /// A shared handle to this branch's verified-record memo.
    pub(crate) fn records(&self) -> dialog_search_tree::Cache<Version, RevisionRecord> {
        self.record_cache.clone()
    }
}
