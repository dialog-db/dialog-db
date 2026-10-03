//! The caches an environment holds for the repositories opened through it.
//!
//! A branch reads and commits through caches: tree nodes, spilled values,
//! verified revision records, causal verdicts, rule sets, the live spine of
//! its tree. They used to be made with the branch handle and to die with it,
//! so opening a branch again started cold, and a caller had no say in how
//! long any of it lived.
//!
//! The environment holds them instead, as one [`HeldCaches`], so whoever
//! owns the environment decides: build it with the caches it should use,
//! keep it and every later open of a branch is warm, drop it and everything
//! it held goes, or let one repository's caches go early. Each cache is
//! held at the width it is sound to share:
//!
//! - **Tree nodes, for the whole environment.** One cache under one byte
//!   budget, however many repositories are open. Each repository reads it
//!   through its own [`Scope`], so it is answered only with nodes it holds
//!   itself: a node another repository holds is kept once, but is not a hit
//!   until this repository's own archive has produced it.
//! - **Everything addressed by content or version, per repository:**
//!   spilled values, plans, causal verdicts, contexts, revision records.
//!   Every branch of a repository shares them.
//! - **What is tagged by a head, per branch:** the rule cache and the live
//!   spine.
//! - **The outputs of cached formulas, for the whole environment:** a
//!   formula is a pure function of its inputs, so what it computed for one
//!   repository holds for any (see [`FormulaCache`]).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use dialog_artifacts::SpineSlot;
use dialog_artifacts::history::{CausalityCache, ContextCache, RevisionRecord, Version};
use dialog_artifacts::tree::{ArtifactNodeCache, SpillCache, spill_cache};
use dialog_common::{Holds, held_key};
use dialog_query::concept::query::PlanCache;
use dialog_query::formula::FormulaCache;
use dialog_search_tree::{Cache, NODE_CACHE_BUDGET, NodeCache, Scope};
use dialog_varsig::Did;

use super::source::Caches;
use crate::BranchReference;
use crate::rules::{RuleCache, SharedRuleCache};

/// The key an environment holds its caches under.
const HELD: &str = "dialog.caches";

/// The scope `repository` reads the environment's node cache through.
fn scope(repository: &Did) -> Scope {
    Scope::named(repository.to_string().as_bytes())
}

/// What the branches of one repository share, and what each keeps to
/// itself.
struct Repository {
    spills: SpillCache,
    plans: PlanCache,
    causality: CausalityCache,
    contexts: ContextCache,
    records: Cache<Version, RevisionRecord>,
    /// By branch name: its rule cache and its live spine.
    branches: Mutex<HashMap<String, (SharedRuleCache, SpineSlot)>>,
}

impl Repository {
    fn new() -> Self {
        Self {
            spills: spill_cache(),
            plans: PlanCache::default(),
            causality: CausalityCache::new(),
            contexts: ContextCache::new(),
            records: Cache::new(),
            branches: Mutex::default(),
        }
    }
}

/// The caches an environment holds for the repositories opened through it.
///
/// An environment that is given none makes its own on first use, with the
/// default node budget. To decide for it, build the environment with one,
/// or [`hold`](Self::hold) one before anything is opened:
///
/// ```no_run
/// # use dialog_repository::HeldCaches;
/// # fn configure(env: &impl dialog_common::Holds) {
/// // At most 32 MiB of tree nodes, for every repository together.
/// HeldCaches::with_budget(32 * 1024 * 1024).hold(env);
/// # }
/// ```
///
/// Clones share the caches. A set may be held by more than one environment,
/// which then share everything in it.
#[derive(Clone)]
pub struct HeldCaches {
    /// Tree nodes of every repository, each reading through its own scope.
    nodes: ArtifactNodeCache,
    /// The outputs of formulas marked `#[formula(cached)]`.
    formulas: FormulaCache,
    /// By repository DID.
    repositories: Arc<Mutex<HashMap<String, Arc<Repository>>>>,
}

impl fmt::Debug for HeldCaches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldCaches")
            .field("budget", &self.budget())
            .field("bytes", &self.bytes())
            .field("repositories", &self.repositories().len())
            .field("formulas", &self.formulas.len())
            .finish()
    }
}

impl Default for HeldCaches {
    fn default() -> Self {
        Self::new()
    }
}

impl HeldCaches {
    /// Caches holding up to [`NODE_CACHE_BUDGET`] bytes of tree nodes.
    pub fn new() -> Self {
        Self::with_budget(NODE_CACHE_BUDGET)
    }

    /// Caches holding up to `bytes` of stored tree nodes, for every
    /// repository together. With no room for a node, nothing is kept and
    /// every read goes to the archive.
    pub fn with_budget(bytes: usize) -> Self {
        Self::with_formulas(bytes, FormulaCache::new())
    }

    fn with_formulas(bytes: usize, formulas: FormulaCache) -> Self {
        Self {
            nodes: NodeCache::with_budget(bytes),
            formulas,
            repositories: Arc::default(),
        }
    }

    /// The caches `env` holds, made with the default budget if it holds
    /// none yet.
    pub fn of<Env: Holds>(env: &Env) -> Self {
        // Queries may have run through `env` before anything was opened,
        // with a formula cache of their own: these caches take that one.
        let formulas = FormulaCache::of(env);
        env.held_or(&held_key::<Self>(HELD), &|| {
            Arc::new(Self::with_formulas(NODE_CACHE_BUDGET, formulas.clone()))
        })
        .downcast_ref::<Self>()
        .cloned()
        // Only if something else is held under the key: caches of the
        // caller's own, which reuse nothing and break nothing.
        .unwrap_or_default()
    }

    /// Have `env` hold these caches, in place of any it held.
    ///
    /// A branch already open keeps the caches it was opened with; what is
    /// opened from here on uses these.
    pub fn hold<Env: Holds>(&self, env: &Env) {
        env.hold(held_key::<Self>(HELD), Arc::new(self.clone()));
        self.formulas.hold(env);
    }

    /// The outputs of cached formulas these caches remember.
    pub fn formulas(&self) -> &FormulaCache {
        &self.formulas
    }

    /// The bytes of tree nodes these caches may hold.
    pub fn budget(&self) -> usize {
        self.nodes.budget()
    }

    /// The bytes of tree nodes held now, for every repository.
    pub fn bytes(&self) -> usize {
        self.nodes.bytes()
    }

    /// The repositories something is held for.
    pub fn repositories(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Let go of everything held for `repository`: its nodes that no other
    /// repository holds, and every cache of its branches.
    ///
    /// A branch of it that is still open keeps working: it reads its nodes
    /// again, and keeps the other caches it was opened with. The next open
    /// starts cold.
    pub fn release(&self, repository: &Did) {
        self.lock().remove(&repository.to_string());
        self.nodes.scoped(scope(repository)).release();
    }

    /// Let go of everything, for every repository.
    pub fn clear(&self) {
        self.lock().clear();
        self.nodes.clear();
        self.formulas.clear();
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Arc<Repository>>> {
        self.repositories
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// The caches every open of `branch` reads and commits through.
    pub(crate) fn branch(&self, branch: &BranchReference) -> Caches {
        let repository = self
            .lock()
            .entry(branch.of().to_string())
            .or_insert_with(|| Arc::new(Repository::new()))
            .clone();
        let (rules, spine) = repository
            .branches
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entry(branch.name().to_string())
            .or_insert_with(|| (Arc::new(RuleCache::new()), SpineSlot::new()))
            .clone();
        Caches {
            nodes: self.nodes.scoped(scope(branch.of())),
            spills: repository.spills.clone(),
            rules,
            plans: repository.plans.clone(),
            causality: repository.causality.clone(),
            contexts: repository.contexts.clone(),
            records: repository.records.clone(),
            spine,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{Artifact, Instruction, Value};
    use dialog_capability::{Command, Fork, Provider};
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_common::{ConditionalSync, Held, Holds};
    use dialog_credentials::Credential;
    use dialog_effects::archive::{ArchiveError, Get, Import, Put};
    use dialog_effects::authority::{Attest, Identify};
    use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
    use dialog_effects::memory::{Publish, Resolve};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_peer::{Peer, PeerSpace, Session};
    use futures_util::stream;

    use super::HeldCaches;
    use crate::helpers::test_repo;
    use crate::{Branch, CommitError, RemoteSite, Repository};

    /// A provider that passes every effect a commit needs through to the
    /// environment it wraps, except the archive import, which it refuses:
    /// a stand-in for a flush that dies at the store (a quota, a lost
    /// connection) after the commit has sealed its nodes.
    struct RefusingImport<P> {
        inner: P,
    }

    impl<P: Holds> Holds for RefusingImport<P> {
        fn held(&self, key: &str) -> Option<Held> {
            self.inner.held(key)
        }

        fn hold(&self, key: String, handle: Held) {
            self.inner.hold(key, handle)
        }

        fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
            self.inner.held_or(key, make)
        }
    }

    macro_rules! delegate_provider {
        ($($command:ty),+ $(,)?) => {
            $(
                #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
                #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
                impl<P> Provider<$command> for RefusingImport<P>
                where
                    P: Provider<$command> + ConditionalSync,
                {
                    async fn execute(
                        &self,
                        input: <$command as Command>::Input,
                    ) -> <$command as Command>::Output {
                        self.inner.execute(input).await
                    }
                }
            )+
        };
    }

    delegate_provider!(
        Get,
        Put,
        Identify,
        Attest,
        Resolve,
        Publish,
        BlobRead,
        BlobImport,
        crate::Hydrate,
        Fork<RemoteSite, Resolve>,
    );

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<P: ConditionalSync> Provider<Import> for RefusingImport<P> {
        async fn execute(&self, _input: <Import as Command>::Input) -> <Import as Command>::Output {
            Err(ArchiveError::Storage(
                "rigged: the store refuses every import".into(),
            ))
        }
    }

    /// Opens `main` of `repository`, commits one fact to it, and answers
    /// with the root the commit landed on.
    async fn commit<S: PeerSpace>(
        repository: &Repository<Credential>,
        operator: &Peer<S, Session>,
    ) -> Result<(Branch, NodeHash)> {
        let main = repository.branch("main").open().perform(operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:1".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(operator)
        .await?;
        let revision = main.revision().expect("the commit set a head");
        Ok((main, NodeHash::from(*revision.tree.hash())))
    }

    /// The caches are the environment's, not the handle's: a branch opened
    /// again through the same environment finds what the last handle left.
    #[dialog_common::test]
    async fn it_opens_a_branch_again_on_warm_caches() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let (main, root) = commit(&repository, &operator).await?;
        drop(main);

        let again = repository.branch("main").open().perform(&operator).await?;

        assert!(
            again.node_cache().get_cached(&root).is_some(),
            "the root the last handle wrote must still be held"
        );
        Ok(())
    }

    /// Two repositories opened through one environment share its node
    /// cache, and neither is answered with a node only the other holds.
    #[dialog_common::test]
    async fn it_answers_a_repository_only_with_nodes_it_holds() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let ours = test_repo(&operator, &profile).await;
        let theirs = test_repo(&operator, &profile).await;
        let (main, root) = commit(&ours, &operator).await?;

        let other = theirs.branch("main").open().perform(&operator).await?;

        assert!(main.node_cache().get_cached(&root).is_some());
        assert!(
            other.node_cache().get_cached(&root).is_none(),
            "a node another repository holds must not answer this one"
        );
        assert_eq!(
            other.node_cache().len(),
            main.node_cache().len(),
            "both read the one cache the environment holds"
        );
        assert!(!other.node_cache().is_empty());
        Ok(())
    }

    /// A released repository starts cold on its next open, and one that was
    /// not released is untouched.
    #[dialog_common::test]
    async fn it_starts_cold_after_a_repository_is_released() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let released = test_repo(&operator, &profile).await;
        let kept = test_repo(&operator, &profile).await;
        let (_, released_root) = commit(&released, &operator).await?;
        let (_, kept_root) = commit(&kept, &operator).await?;

        HeldCaches::of(&operator).release(&released.did());

        let cold = released.branch("main").open().perform(&operator).await?;
        let warm = kept.branch("main").open().perform(&operator).await?;
        assert!(cold.node_cache().get_cached(&released_root).is_none());
        assert!(warm.node_cache().get_cached(&kept_root).is_some());
        Ok(())
    }

    /// Clearing lets everything go, for every repository, and a branch
    /// opened afterwards starts cold.
    #[dialog_common::test]
    async fn it_starts_every_repository_cold_once_cleared() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let (_, root) = commit(&repository, &operator).await?;
        let caches = HeldCaches::of(&operator);
        assert!(caches.bytes() > 0);
        assert!(
            caches
                .repositories()
                .contains(&repository.did().to_string())
        );

        caches.clear();

        assert_eq!(caches.bytes(), 0);
        assert!(caches.repositories().is_empty());
        let cold = repository.branch("main").open().perform(&operator).await?;
        assert!(cold.node_cache().get_cached(&root).is_none());
        Ok(())
    }

    /// A commit whose flush to the store fails leaves nothing of itself
    /// in the caches the environment holds. The nodes it sealed went into
    /// the cache ahead of the flush, and the cache outlives the handle, so
    /// they must be forgotten: a scope holds only what its archive has,
    /// or a later read would be answered with a block the store never
    /// received and never fetch it.
    #[dialog_common::test]
    async fn it_forgets_the_nodes_a_failed_flush_sealed() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let (main, landed) = commit(&repository, &operator).await?;
        let caches = HeldCaches::of(&operator);
        let (held, bytes) = (main.node_cache().len(), caches.bytes());

        let refusing = RefusingImport {
            inner: operator.clone(),
        };
        let failed = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:2".parse()?,
                is: Value::String("Bob".into()),
                cause: None,
            })]))
            .perform(&refusing)
            .await;

        assert!(
            matches!(failed, Err(CommitError::Artifact(_))),
            "the commit fails at the flush: {failed:?}"
        );
        assert_eq!(
            main.node_cache().len(),
            held,
            "a node the failed flush sealed must not stay in the cache"
        );
        assert_eq!(caches.bytes(), bytes);
        assert!(
            main.node_cache().get_cached(&landed).is_some(),
            "what landed before stays held"
        );
        assert_eq!(
            main.revision()
                .map(|revision| NodeHash::from(*revision.tree.hash())),
            Some(landed),
            "the head did not move"
        );
        Ok(())
    }

    /// The budget bounds the nodes the environment holds for every
    /// repository together: with no room, nothing is kept and commits still
    /// land.
    #[dialog_common::test]
    async fn it_holds_no_more_nodes_than_the_budget_allows() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        HeldCaches::with_budget(1).hold(&operator);
        let repository = test_repo(&operator, &profile).await;

        let (main, root) = commit(&repository, &operator).await?;

        assert!(main.node_cache().is_empty());
        assert!(main.node_cache().get_cached(&root).is_none());
        Ok(())
    }
}
