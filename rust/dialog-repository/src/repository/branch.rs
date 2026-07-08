use super::memory::Cell;
use crate::rules::SharedRuleCache;
use crate::{ResolveError, Revision};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_query::concept::query::PlanCache;

use dialog_artifacts::{Exporter, Importer};
use dialog_capability::{Capability, Did, Subject};
use dialog_common::Blake3Hash;
use dialog_effects::archive::Archive;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_query::query::Application;
use dialog_search_tree::{Buffer, Cache};

mod blob;
pub use blob::*;

mod blobs;
pub use blobs::*;

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

mod set_upstream;
pub use set_upstream::*;

mod transaction;
pub use transaction::*;

mod upstream;
pub use upstream::*;

#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;

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
    upstream: Cell<Upstream>,
    /// Shared node cache for tree reads. Created once per opened branch and
    /// carried (as a shared handle) into every `Select`'s tree, so blocks read
    /// by one query stay warm for the next instead of being re-fetched from
    /// storage. Content-addressed keys make sharing across revisions safe.
    node_cache: Cache<Blake3Hash, Buffer>,
    /// Shared deductive-rule cache (discovery by head + hydrated bodies).
    /// Like `node_cache`, created once per opened branch and carried into
    /// every query's durable rule resolution, so the `db.rule/*` scan is
    /// paid once per (concept, head) rather than per query.
    rule_cache: SharedRuleCache,
    /// Shared plan cache for the deductive rules resolved on this branch,
    /// keyed by content-addressed `(rule, adornment)`. Handed to each
    /// per-query `ConceptRules` assembly so a re-assembled rule set reuses
    /// plans an earlier query computed. Content-addressed keys make it
    /// safe across revisions, like `node_cache`.
    plan_cache: PlanCache,
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

    /// Returns the upstream state, or `None` if no upstream is configured.
    pub fn upstream(&self) -> Option<Upstream> {
        self.upstream.content()
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

    /// A shared handle to this branch's deductive-rule cache.
    pub(crate) fn rule_cache(&self) -> SharedRuleCache {
        self.rule_cache.clone()
    }

    /// A shared handle to this branch's deductive-rule plan cache.
    pub(crate) fn plan_cache(&self) -> PlanCache {
        self.plan_cache.clone()
    }
}
