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
//! - **Rule content and plans, for the whole environment:** decoded rule
//!   bodies (keyed by entity and the bytes a reader read), heads re-spelled
//!   onto attributes, selecting rules and query plans are pure functions
//!   of content, and none answers which rules a repository holds, so two
//!   repositories with the same rules decode and plan them once.
//! - **Spilled values, for the whole environment.** One cache under one
//!   byte budget, however many repositories are open. A spilled value is
//!   addressed by its content, so a block cached for one repository is
//!   the same bytes for any; a repository reads it only by a reference
//!   its own tree carries.
//! - **Everything else addressed by content or version, per repository:**
//!   causal verdicts, contexts, revision records, rewritten trees. Every
//!   branch of a repository shares them.
//! - **What is tagged by a line's rules or head, per branch:** assembled
//!   rule sets, the rules set aside, rule discovery and the trigger
//!   footprint. An assembled set is keyed by its lines' rules roots, one
//!   set per spelling of a concept, so held for the environment the sets
//!   of repositories querying the same concept would evict each other on
//!   every query.
//! - **The live spine, per branch, while a handle of it is open.** The
//!   spine is the accelerator for the next commit on the branch, and only
//!   a handle (or a snapshot or transaction taken from one) can make that
//!   commit, so every handle of one branch shares one spine and the last
//!   handle dropped takes it with it. A branch opened again starts with
//!   its nodes warm and decodes its root once, on its first commit.
//!   Without this, a repository visited by many writers once each held a
//!   spine per writer for the life of the environment.
//! - **The outputs of cached formulas, for the whole environment:** a
//!   formula is a pure function of its inputs, so what it computed for one
//!   repository holds for any (see [`FormulaCache`]).
//! - **The writer of each open branch,** the lock its head moves under.
//!   It is held here, with the caches, so that every environment sharing
//!   one set shares the writers too: a session of a peer writes to its
//!   peer's archive, and its handles must take turns with the peer's.
//!
//! How large each cache may grow is set when the set is built (see
//! [`CacheBudget`]); a snapshot or transaction batch made through an
//! environment holding the set sizes its own caches by the same budget.
//!
//! One set is for one archive. Environments over different storages must
//! not share a set: a node one archive holds would answer a read of
//! another that does not, and a release or clear would reach them both.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use dialog_artifacts::history::{CausalityCache, ContextCache, RevisionRecord, Version};
use dialog_artifacts::tree::{ArtifactNodeCache, SPILL_CACHE_BUDGET, SpillCache};
use dialog_artifacts::{SpineSlot, WeakSpineSlot};
use dialog_common::{Blake3Hash, Holds, held_key};
use dialog_query::concept::query::{PLAN_CACHE_CAPACITY, PlanCache};
use dialog_query::formula::{FORMULA_CACHE_CAPACITY, FormulaCache};
use dialog_search_tree::{CACHE_CAPACITY, Cache, NODE_CACHE_BUDGET, NodeCache, Scope};
use dialog_varsig::Did;

use super::branch::{LayerMetadataMemo, Writer, Writers};
use super::source::Caches;
use crate::BranchReference;
use crate::rules::{RuleCache, RuleContent, SharedRuleCache};

/// The key an environment holds its caches under.
const HELD: &str = "dialog.caches";

/// The scope `repository` reads the environment's node cache through.
fn scope(repository: &Did) -> Scope {
    Scope::named(repository.to_string().as_bytes())
}

/// How large each cache an environment holds may grow, and the builder
/// of a [`HeldCaches`] that keeps to it.
///
/// Byte budgets bound the caches whose entries vary without limit in
/// size (tree nodes, spilled values); entry counts bound the rest. Each
/// starts at the size every environment used before it could be chosen,
/// so a budget that sets nothing builds exactly the caches
/// [`HeldCaches::new`] does:
///
/// ```no_run
/// # use dialog_repository::HeldCaches;
/// # fn configure(env: &impl dialog_common::Holds) {
/// const MIB: usize = 1024 * 1024;
/// HeldCaches::builder()
///     .nodes(24 * MIB) // every repository's tree nodes together
///     .spills(4 * MIB) // every repository's spilled values together
///     .entries(512) // per repository: verdicts, contexts, records, rewrites
///     .rules(512) // rule content and query plans, for the environment
///     .formulas(1024) // formula outputs, for the environment
///     .build()
///     .hold(env);
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheBudget {
    nodes: usize,
    spills: usize,
    entries: usize,
    rules: usize,
    plans: usize,
    formulas: usize,
}

impl Default for CacheBudget {
    fn default() -> Self {
        Self {
            nodes: NODE_CACHE_BUDGET,
            spills: SPILL_CACHE_BUDGET,
            entries: CACHE_CAPACITY,
            rules: CACHE_CAPACITY,
            plans: PLAN_CACHE_CAPACITY,
            formulas: FORMULA_CACHE_CAPACITY,
        }
    }
}

impl CacheBudget {
    /// Hold at most `bytes` of stored tree nodes, for every repository
    /// together. With no room for a node, nothing is kept and every read
    /// goes to the archive.
    pub fn nodes(mut self, bytes: usize) -> Self {
        self.nodes = bytes;
        self
    }

    /// Hold at most `bytes` of spilled values, for every repository
    /// together. A value larger than the whole budget is read every time
    /// and never kept.
    pub fn spills(mut self, bytes: usize) -> Self {
        self.spills = bytes;
        self
    }

    /// Remember at most `n` entries in each of a repository's causal
    /// verdicts (of each kind), contexts, revision records and rewritten
    /// trees, and in the same caches of a snapshot or transaction batch.
    /// Zero is raised to one.
    pub fn entries(mut self, n: usize) -> Self {
        self.entries = n;
        self
    }

    /// Remember at most `n` entries in each cache of rule content (heads,
    /// deductive and inductive bodies, selecting rules) and at most `n`
    /// query plans, for the environment. Zero is raised to one.
    pub fn rules(mut self, n: usize) -> Self {
        self.rules = n;
        self.plans = n;
        self
    }

    /// Remember at most `n` outputs of cached formulas. Zero is raised to
    /// one.
    pub fn formulas(mut self, n: usize) -> Self {
        self.formulas = n;
        self
    }

    /// The bytes of tree nodes held at most.
    pub fn node_bytes(&self) -> usize {
        self.nodes
    }

    /// The bytes of spilled values held at most.
    pub fn spill_bytes(&self) -> usize {
        self.spills
    }

    /// The entries each version- or content-keyed cache of a repository
    /// remembers at most.
    pub fn entry_capacity(&self) -> usize {
        self.entries
    }

    /// The entries each cache of rule content remembers at most.
    pub fn rule_capacity(&self) -> usize {
        self.rules
    }

    /// The query plans remembered at most.
    pub fn plan_capacity(&self) -> usize {
        self.plans
    }

    /// The formula outputs remembered at most.
    pub fn formula_capacity(&self) -> usize {
        self.formulas
    }

    /// Caches that keep to this budget.
    pub fn build(self) -> HeldCaches {
        HeldCaches::within(self, FormulaCache::with_capacity(self.formulas))
    }
}

/// What the branches of one repository share, and what each keeps to
/// itself.
struct Repository {
    causality: CausalityCache,
    contexts: ContextCache,
    records: Cache<Version, RevisionRecord>,
    /// Trees rewritten under the current number band, by the root of the
    /// tree they rewrite. The rewrite's blocks land in this repository's
    /// archive, so an entry answers only for it.
    rewrites: Cache<Blake3Hash, Blake3Hash>,
    /// By branch name: its rule cache, its live spine (held by its open
    /// handles, not here), and the metadata its queries fold in.
    branches: Mutex<HashMap<String, (SharedRuleCache, WeakSpineSlot, LayerMetadataMemo)>>,
}

impl Repository {
    fn new(budget: &CacheBudget) -> Self {
        Self {
            causality: CausalityCache::with_capacity(budget.entries),
            contexts: ContextCache::with_capacity(budget.entries),
            records: Cache::with_capacity(budget.entries),
            rewrites: Cache::with_capacity(budget.entries),
            branches: Mutex::default(),
        }
    }

    /// How many entries its entry-bounded caches remember now.
    fn entries(&self) -> usize {
        self.causality.len() + self.contexts.len() + self.records.len() + self.rewrites.len()
    }
}

/// What every repository opened through an environment shares: rule
/// content and query plans, pure functions of content that answer
/// nothing about what a repository holds.
#[derive(Clone)]
struct Shared {
    content: Arc<RuleContent>,
    plans: PlanCache,
}

impl Shared {
    fn new(budget: &CacheBudget) -> Self {
        Self {
            content: Arc::new(RuleContent::with_capacity(budget.rules)),
            plans: PlanCache::with_capacity(budget.plans),
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
/// To bound every cache it holds, not only the nodes, build it from a
/// [`CacheBudget`] (see [`builder`](Self::builder)).
///
/// Clones share the caches. A set may be held by more than one environment
/// over the same storage, which then share everything in it, the writers
/// of their branches included.
#[derive(Clone)]
pub struct HeldCaches {
    /// How large each cache may grow.
    limits: CacheBudget,
    /// Tree nodes of every repository, each reading through its own scope.
    nodes: ArtifactNodeCache,
    /// Spilled values of every repository, by content.
    spills: SpillCache,
    /// The outputs of formulas marked `#[formula(cached)]`.
    formulas: FormulaCache,
    /// The rule content and plans every repository shares.
    shared: Arc<Mutex<Shared>>,
    /// By repository DID.
    repositories: Arc<Mutex<HashMap<String, Arc<Repository>>>>,
    /// The writers of the branches open through any environment holding
    /// this set, by branch. Not a cache: untouched by `release` and
    /// `clear`, and gone with its last handle.
    writers: Arc<Writers>,
    /// Where the repositories' blocks hydrate from, as their open branches
    /// registered. Not a cache: untouched by `release` and `clear`.
    routes: crate::Routes,
}

impl fmt::Debug for HeldCaches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldCaches")
            .field("limits", &self.limits)
            .field("bytes", &self.bytes())
            .field("entries", &self.entries())
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
    /// Caches holding up to [`NODE_CACHE_BUDGET`] bytes of tree nodes,
    /// with every other cache at its default size ([`CacheBudget::default`]).
    pub fn new() -> Self {
        CacheBudget::default().build()
    }

    /// Caches holding up to `bytes` of stored tree nodes, for every
    /// repository together, with every other cache at its default size.
    /// With no room for a node, nothing is kept and every read goes to
    /// the archive.
    pub fn with_budget(bytes: usize) -> Self {
        CacheBudget::default().nodes(bytes).build()
    }

    /// A budget at the default sizes, to set the size of each cache and
    /// [`build`](CacheBudget::build) the caches that keep to it.
    pub fn builder() -> CacheBudget {
        CacheBudget::default()
    }

    fn within(limits: CacheBudget, formulas: FormulaCache) -> Self {
        Self {
            limits: CacheBudget {
                formulas: formulas.capacity(),
                ..limits
            },
            nodes: NodeCache::with_budget(limits.nodes),
            spills: SpillCache::with_budget(limits.spills),
            formulas,
            shared: Arc::new(Mutex::new(Shared::new(&limits))),
            repositories: Arc::default(),
            writers: Arc::default(),
            routes: crate::Routes::new(),
        }
    }

    /// The caches `env` holds, made with the default budget if it holds
    /// none yet.
    pub fn of<Env: Holds + ?Sized>(env: &Env) -> Self {
        // Queries may have run through `env` before anything was opened,
        // with a formula cache of their own: these caches take that one.
        let key = held_key::<Self>(HELD);
        if let Some(caches) = env
            .held(&key)
            .and_then(|held| held.downcast_ref::<Self>().cloned())
        {
            return caches;
        }
        let formulas = FormulaCache::of(env);
        // So are routes branches registered before any caches were held,
        // and the nodes trees read through `env` kept before then.
        let routes = crate::Routes::of(env);
        let nodes = ArtifactNodeCache::held_by(env);
        let caches = env
            .held_or(&key, &|| {
                let mut caches = Self::within(CacheBudget::default(), formulas.clone());
                caches.routes = routes.clone();
                if let Some(nodes) = &nodes {
                    caches.nodes = nodes.clone();
                    caches.limits.nodes = nodes.budget();
                }
                Arc::new(caches)
            })
            .downcast_ref::<Self>()
            .cloned()
            // Only if something else is held under the key: caches of the
            // caller's own, which reuse nothing and break nothing.
            .unwrap_or_default();
        // A tree reads the node cache `env` holds (`NodeCache::of`): it is
        // this one, so a node a branch's read kept answers every tree read
        // through `env`, and the budget, release and clear here govern it.
        caches.nodes.hold(env);
        caches
    }

    /// Have `env` hold these caches, in place of any it held.
    ///
    /// A branch already open keeps the caches it was opened with; what is
    /// opened from here on uses these.
    pub fn hold<Env: Holds>(&self, env: &Env) {
        env.hold(held_key::<Self>(HELD), Arc::new(self.clone()));
        self.nodes.hold(env);
        self.formulas.hold(env);
        self.routes.hold(env);
    }

    /// Where the repositories opened through an environment holding these
    /// caches hydrate their blocks from (see [`Routes`](crate::Routes)).
    pub fn routes(&self) -> crate::Routes {
        self.routes.clone()
    }

    /// The outputs of cached formulas these caches remember.
    pub fn formulas(&self) -> &FormulaCache {
        &self.formulas
    }

    /// How large each of these caches may grow.
    pub fn limits(&self) -> CacheBudget {
        self.limits
    }

    /// The bytes of tree nodes these caches may hold.
    pub fn budget(&self) -> usize {
        self.nodes.budget()
    }

    /// The bytes held now by every byte-bounded cache, for every
    /// repository: tree nodes and spilled values. The entry-bounded
    /// caches are counted by [`entries`](Self::entries) instead.
    pub fn bytes(&self) -> usize {
        self.nodes.bytes() + self.spills.bytes()
    }

    /// The bytes of spilled values held now, for every repository.
    pub fn spill_bytes(&self) -> usize {
        self.spills.bytes()
    }

    /// How many entries the entry-bounded caches remember now: every
    /// repository's causal verdicts, contexts, revision records and
    /// rewritten trees, the rule content and plans every repository
    /// shares, and the outputs of cached formulas. A count, not a size:
    /// the entries vary in size.
    ///
    /// What each branch keeps by its rules root (assembled rule sets and
    /// rule discovery) is not counted; it is bounded by the branch's own
    /// rules, not by the budget.
    pub fn entries(&self) -> usize {
        let shared = self
            .shared
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let repositories: usize = self
            .lock()
            .values()
            .map(|repository| repository.entries())
            .sum();
        repositories + shared.content.len() + shared.plans.len() + self.formulas.len()
    }

    /// How many branches of `repository` a spine is held for: those with
    /// a handle open whose last commit left one.
    #[cfg(test)]
    pub(crate) fn held_spines(&self, repository: &Did) -> usize {
        self.lock()
            .get(&repository.to_string())
            .map(|repository| {
                repository
                    .branches
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .values()
                    .filter(|(_, spine, _)| spine.upgrade().is_some_and(|slot| slot.is_held()))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The repositories something is held for.
    pub fn repositories(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Let go of everything held for `repository`: its nodes that no other
    /// repository holds, and every cache of its own and of its branches.
    /// The rule content and plans every repository shares stay, since
    /// nothing in them is the repository's, and so do the spilled values:
    /// one cache holds them for every repository, by content, and does not
    /// record which repository read a value in. They age out under the
    /// spill budget; [`clear`](Self::clear) lets all of them go.
    ///
    /// A branch of it that is still open keeps working: it reads its nodes
    /// again, and keeps the other caches it was opened with. The next open
    /// starts cold.
    pub fn release(&self, repository: &Did) {
        self.lock().remove(&repository.to_string());
        self.nodes.scoped(scope(repository)).release();
    }

    /// The writer of the branch `key` names, shared by every handle of it
    /// open through an environment holding this set.
    pub(crate) fn writer(&self, key: String) -> Arc<Writer> {
        Writer::among(&self.writers, key)
    }

    /// Let go of everything, for every repository. The writers of open
    /// branches are not caches and stay.
    pub fn clear(&self) {
        *self
            .shared
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Shared::new(&self.limits);
        self.lock().clear();
        self.nodes.clear();
        self.spills.clear();
        self.formulas.clear();
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Arc<Repository>>> {
        self.repositories
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// The caches every open of `branch` reads and commits through.
    pub(crate) fn branch(&self, branch: &BranchReference) -> Caches {
        let shared = self
            .shared
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let repository = self
            .lock()
            .entry(branch.of().to_string())
            .or_insert_with(|| Arc::new(Repository::new(&self.limits)))
            .clone();
        let (rules, spine, metadata) = {
            let mut branches = repository
                .branches
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let (rules, weak, metadata) =
                branches
                    .entry(branch.name().to_string())
                    .or_insert_with(|| {
                        (
                            Arc::new(RuleCache::sharing(shared.content.clone())),
                            WeakSpineSlot::new(),
                            LayerMetadataMemo::default(),
                        )
                    });
            // The spine of the handles already open, or a fresh slot for
            // this one when the last of them went.
            let spine = weak.upgrade().unwrap_or_else(|| {
                let slot = SpineSlot::new();
                *weak = slot.downgrade();
                slot
            });
            (rules.clone(), spine, metadata.clone())
        };
        Caches {
            nodes: self.nodes.scoped(scope(branch.of())),
            spills: self.spills.clone(),
            rules,
            plans: shared.plans,
            causality: repository.causality.clone(),
            contexts: repository.contexts.clone(),
            records: repository.records.clone(),
            rewrites: repository.rewrites.clone(),
            spine,
            metadata,
            limits: self.limits,
        }
    }

    /// Cold caches for a snapshot of `repository` taken through an
    /// environment holding these, sized by their budget.
    ///
    /// What is addressed by content alone (tree nodes, spilled values,
    /// rule content and plans) is these caches' own, so the snapshot adds
    /// nothing to what they may hold; what is keyed by version (causal
    /// verdicts, contexts, revision records, rewrites) and the rule sets of
    /// the snapshot's line start empty, each at most the budget's entries.
    pub(crate) fn cold(&self, repository: &Did) -> Caches {
        let shared = self
            .shared
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let entries = self.limits.entries;
        Caches {
            nodes: self.nodes.scoped(scope(repository)),
            spills: self.spills.clone(),
            rules: Arc::new(RuleCache::sharing(shared.content)),
            plans: shared.plans,
            causality: CausalityCache::with_capacity(entries),
            contexts: ContextCache::with_capacity(entries),
            records: Cache::with_capacity(entries),
            rewrites: Cache::with_capacity(entries),
            spine: SpineSlot::new(),
            metadata: LayerMetadataMemo::default(),
            limits: self.limits,
        }
    }
}

#[cfg(test)]
mod tests {
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_effects::blob::Load as BlobLoad;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{Artifact, Instruction, Pick, Value};
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

    use dialog_artifacts::tree::SPILL_CACHE_BUDGET;
    use dialog_query::concept::query::PLAN_CACHE_CAPACITY;
    use dialog_query::formula::FORMULA_CACHE_CAPACITY;
    use dialog_query::the;
    use dialog_search_tree::{CACHE_CAPACITY, NODE_CACHE_BUDGET};

    use super::{CacheBudget, HeldCaches};
    use crate::helpers::test_repo;
    use crate::registry::RegistryEnv;
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
        ArchiveLoad,
        BlobLoad,
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
        main.commit(stream::iter(vec![Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            },
            Pick::All,
        )]))
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
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "user/name".parse()?,
                    of: "user:2".parse()?,
                    is: Value::String("Bob".into()),
                    cause: None,
                },
                Pick::All,
            )]))
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

    /// Commits `fact` (one entity) to `branch`.
    async fn commit_fact<Env>(branch: &Branch, fact: usize, env: &Env) -> Result<()>
    where
        Env: RegistryEnv,
    {
        branch
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "user/name".parse()?,
                    of: format!("user:{fact}").parse()?,
                    is: Value::String("Alice".into()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(env)
            .await?;
        Ok(())
    }

    /// A branch's spine lives with its handles: a writer that opens a
    /// branch, commits once and drops the handle leaves no spine behind,
    /// however many writers do so.
    #[dialog_common::test]
    async fn it_drops_a_branch_spine_with_its_last_handle() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let caches = HeldCaches::of(&operator);
        let did = repository.did();
        for i in 0..5 {
            let writer = repository
                .branch(format!("writer-{i}"))
                .open()
                .perform(&operator)
                .await?;
            commit_fact(&writer, i, &operator).await?;
            assert_eq!(
                caches.held_spines(&did),
                1,
                "the open handle holds the spine its commit left"
            );
            drop(writer);
            assert_eq!(caches.held_spines(&did), 0, "the dropped handle took it");
        }
        Ok(())
    }

    /// Every handle of one branch shares one spine, and a snapshot taken
    /// from a handle keeps it: a commit through one handle is the next
    /// commit's spine through the other, and the spine goes only with the
    /// last of them.
    #[dialog_common::test]
    async fn it_shares_one_spine_among_the_handles_of_a_branch() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let caches = HeldCaches::of(&operator);
        let did = repository.did();
        let first = repository.branch("main").open().perform(&operator).await?;
        let second = repository.branch("main").open().perform(&operator).await?;
        assert!(first.spine().shares(second.spine()));

        commit_fact(&first, 1, &operator).await?;
        assert!(second.spine().is_held(), "the other handle sees the spine");
        assert_eq!(caches.held_spines(&did), 1);
        let snapshot = first.snapshot().expect("a head");
        drop(first);
        drop(second);
        assert_eq!(
            caches.held_spines(&did),
            1,
            "the snapshot taken from a handle keeps the spine"
        );
        drop(snapshot);
        assert_eq!(caches.held_spines(&did), 0);
        Ok(())
    }

    /// A branch opened again after its last handle went commits on a
    /// fresh spine opened from its root, which the node cache holds: the
    /// commit reads no block and lands as the handle's successor.
    #[dialog_common::test]
    async fn it_reopens_a_branch_on_a_fresh_spine_without_a_block_read() -> Result<()> {
        use crate::helpers::Counting;

        let (operator, profile) = test_session_with_peer().await;
        let repository = test_repo(&operator, &profile).await;
        let env = Counting::new(operator);
        let caches = HeldCaches::of(&env);
        let did = repository.did();
        let main = repository.branch("main").open().perform(&env).await?;
        commit_fact(&main, 1, &env).await?;
        let landed = main.revision().expect("the commit set a head");
        drop(main);
        assert_eq!(caches.held_spines(&did), 0);

        let again = repository.branch("main").open().perform(&env).await?;
        assert!(
            !again.spine().is_held(),
            "the new handle starts without a spine"
        );
        env.reset();
        commit_fact(&again, 2, &env).await?;

        assert_eq!(env.block_reads(), 0, "the root came from the node cache");
        let advanced = again.revision().expect("the commit set a head");
        assert!(
            advanced.version().edition > landed.version().edition,
            "the commit landed as the successor of {landed:?}: {advanced:?}"
        );
        assert!(
            again.spine().is_held(),
            "the commit left a spine for the next"
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

    /// A budget that sets nothing is today's sizes, and so are the caches
    /// [`HeldCaches::new`] and [`HeldCaches::with_budget`] build: every
    /// default is the constant each cache was fixed at before it could be
    /// chosen.
    #[dialog_common::test]
    fn it_builds_todays_caches_from_a_budget_that_sets_nothing() -> Result<()> {
        let defaults = HeldCaches::builder();
        assert_eq!(defaults, CacheBudget::default());
        assert_eq!(defaults.node_bytes(), NODE_CACHE_BUDGET);
        assert_eq!(defaults.spill_bytes(), SPILL_CACHE_BUDGET);
        assert_eq!(defaults.entry_capacity(), CACHE_CAPACITY);
        assert_eq!(defaults.rule_capacity(), CACHE_CAPACITY);
        assert_eq!(defaults.plan_capacity(), PLAN_CACHE_CAPACITY);
        assert_eq!(defaults.formula_capacity(), FORMULA_CACHE_CAPACITY);

        let caches = HeldCaches::new();
        assert_eq!(caches.limits(), defaults);
        assert_eq!(caches.budget(), NODE_CACHE_BUDGET);
        assert_eq!(caches.spills.budget(), SPILL_CACHE_BUDGET);
        assert_eq!(caches.formulas().capacity(), FORMULA_CACHE_CAPACITY);
        let shared = caches.shared.lock().unwrap().clone();
        assert_eq!(shared.content.capacity(), CACHE_CAPACITY);
        assert_eq!(shared.plans.capacity(), PLAN_CACHE_CAPACITY);
        let repository = super::Repository::new(&caches.limits());
        assert_eq!(repository.records.capacity(), CACHE_CAPACITY);
        assert_eq!(repository.rewrites.capacity(), CACHE_CAPACITY);

        assert_eq!(
            HeldCaches::with_budget(1024).limits(),
            defaults.nodes(1024),
            "with_budget sets the nodes alone"
        );
        Ok(())
    }

    /// Every cap the builder sets reaches the cache it names: a repository's
    /// entry-bounded caches hold `entries` and evict past them, and the
    /// rule content, plans and formula outputs hold what they were given.
    #[dialog_common::test]
    async fn it_keeps_each_cache_to_the_cap_the_builder_set() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let caches = HeldCaches::builder()
            .nodes(4096)
            .spills(2048)
            .entries(2)
            .rules(3)
            .formulas(5)
            .build();
        caches.hold(&operator);
        let repository = test_repo(&operator, &profile).await;
        commit(&repository, &operator).await?;

        let opened = caches.branch(&repository.branch("main"));
        assert_eq!(opened.records.capacity(), 2);
        for key in 0..3u8 {
            opened
                .rewrites
                .insert(NodeHash::from([key; 32]), NodeHash::from([key; 32]));
        }
        assert_eq!(opened.rewrites.len(), 2, "the third entry evicts one");
        assert_eq!(opened.limits.entry_capacity(), 2);

        assert_eq!(caches.budget(), 4096);
        assert_eq!(opened.spills.budget(), 2048);
        assert_eq!(opened.plans.capacity(), 3);
        let shared = caches.shared.lock().unwrap().clone();
        assert_eq!(shared.content.capacity(), 3);
        assert_eq!(caches.formulas().capacity(), 5);
        assert!(
            caches.entries() >= 2,
            "the entries held are counted: {}",
            caches.entries()
        );
        Ok(())
    }

    /// One spill budget bounds every repository together: two repositories
    /// read through one cache, a block one put in is evicted by a block the
    /// other put in once they do not both fit, and `bytes` counts it.
    /// Releasing a repository leaves the spilled values, which are kept
    /// for every repository by content; clearing lets them go.
    #[dialog_common::test]
    async fn it_shares_one_spill_budget_among_repositories() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let caches = HeldCaches::builder().spills(1000).build();
        caches.hold(&operator);
        let ours = test_repo(&operator, &profile).await;
        let theirs = test_repo(&operator, &profile).await;
        let our_spills = caches.branch(&ours.branch("main")).spills;
        let their_spills = caches.branch(&theirs.branch("main")).spills;

        our_spills.insert([1; 32], vec![1; 600]);
        assert_eq!(
            their_spills.get(&[1; 32]),
            Some(vec![1; 600]),
            "both read the one cache"
        );
        their_spills.insert([2; 32], vec![2; 600]);
        assert_eq!(caches.spill_bytes(), 600, "the budget holds one block");
        assert!(our_spills.get(&[1; 32]).is_none(), "the older block went");
        assert_eq!(caches.bytes(), caches.nodes.bytes() + 600);

        caches.release(&theirs.did());
        assert_eq!(
            our_spills.get(&[2; 32]),
            Some(vec![2; 600]),
            "release keeps spilled values"
        );
        caches.clear();
        assert_eq!(caches.spill_bytes(), 0);
        Ok(())
    }

    /// A snapshot taken through an environment holding budgeted caches
    /// keeps to the budget: its caches keyed by version hold the budget's
    /// entries, and it reads spilled values, rule content and plans
    /// through the environment's own. A snapshot taken without the
    /// environment keeps today's sizes.
    #[dialog_common::test]
    async fn it_sizes_a_snapshot_by_the_budget_of_its_environment() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let caches = HeldCaches::builder()
            .spills(1000)
            .entries(2)
            .rules(3)
            .build();
        caches.hold(&operator);
        let repository = test_repo(&operator, &profile).await;
        let (main, _) = commit(&repository, &operator).await?;
        let revision = main.revision().expect("the commit set a head");

        let snapshot = repository.snapshot_in(&operator, revision.clone());
        let held = snapshot.caches();
        assert_eq!(held.records.capacity(), 2);
        assert_eq!(held.rewrites.capacity(), 2);
        assert_eq!(held.plans.capacity(), 3);
        assert_eq!(held.spills.budget(), 1000);
        held.spills.insert([3; 32], vec![3; 10]);
        assert_eq!(caches.spill_bytes(), 10, "the snapshot's spills are held");
        assert!(
            held.records.is_empty(),
            "what is keyed by version starts cold"
        );

        let unbudgeted = repository.snapshot(revision);
        assert_eq!(unbudgeted.caches().records.capacity(), CACHE_CAPACITY);
        assert_eq!(unbudgeted.caches().spills.budget(), SPILL_CACHE_BUDGET);
        Ok(())
    }

    /// The caches a transaction batch keeps to itself are sized by the
    /// budget of the branch it was staged from.
    #[dialog_common::test]
    async fn it_sizes_a_batch_by_the_budget_of_its_branch() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        HeldCaches::builder().entries(2).build().hold(&operator);
        let repository = test_repo(&operator, &profile).await;
        let main = repository.branch("main").open().perform(&operator).await?;

        let batch = main
            .transaction()
            .assert(
                the!("user/name")
                    .of("user:1".parse::<dialog_artifacts::Entity>()?)
                    .is("Alice".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;

        assert_eq!(batch.caches().records.capacity(), 2);
        assert_eq!(batch.caches().limits.entry_capacity(), 2);
        Ok(())
    }
}
