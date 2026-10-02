//! The caches an environment holds for the repositories opened through it.
//!
//! A branch reads and commits through caches: tree nodes, spilled values,
//! verified revision records, causal verdicts, rule sets, the live spine of
//! its tree. They used to be made with the branch handle and to die with it,
//! so opening a branch again started cold, and a caller had no say in how
//! long any of it lived.
//!
//! The environment holds them instead ([`Holds`]), so whoever owns the
//! environment decides: keep it and every later open of a branch is warm,
//! drop it and everything it held goes, or let one repository's caches go
//! early with [`HeldCaches::release`]. They are held at the width each is
//! sound to share:
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dialog_artifacts::SpineSlot;
use dialog_artifacts::history::{CausalityCache, ContextCache, RevisionRecord, Version};
use dialog_artifacts::tree::{ArtifactNodeCache, SpillCache, spill_cache};
use dialog_common::Holds;
use dialog_query::concept::query::PlanCache;
use dialog_search_tree::{Cache, NodeCache, Scope};
use dialog_varsig::Did;

use super::source::Caches;
use crate::BranchReference;
use crate::rules::{RuleCache, SharedRuleCache};

/// The key the environment's node cache is held under.
const NODES: &str = "dialog.caches/nodes";

/// The key `repository`'s caches are held under.
fn held(repository: &Did) -> String {
    format!("dialog.caches/repository:{repository}")
}

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

/// The node cache `env` holds, made on first use.
fn nodes<Env: Holds>(env: &Env) -> ArtifactNodeCache {
    if let Some(nodes) = env
        .held(NODES)
        .and_then(|held| held.downcast_ref::<ArtifactNodeCache>().cloned())
    {
        return nodes;
    }
    let nodes = NodeCache::new();
    env.hold(NODES.to_string(), Arc::new(nodes.clone()));
    nodes
}

/// The caches `env` holds for `branch`: the ones every open of this branch
/// through `env` reads and commits through.
pub(crate) fn of<Env: Holds>(env: &Env, branch: &BranchReference) -> Caches {
    let key = held(branch.of());
    let repository = match env
        .held(&key)
        .and_then(|held| held.downcast_ref::<Arc<Repository>>().cloned())
    {
        Some(repository) => repository,
        None => {
            let repository = Arc::new(Repository::new());
            env.hold(key, Arc::new(repository.clone()));
            repository
        }
    };
    let (rules, spine) = repository
        .branches
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .entry(branch.name().to_string())
        .or_insert_with(|| (Arc::new(RuleCache::new()), SpineSlot::new()))
        .clone();
    Caches {
        nodes: nodes(env).scoped(scope(branch.of())),
        spills: repository.spills.clone(),
        rules,
        plans: repository.plans.clone(),
        causality: repository.causality.clone(),
        contexts: repository.contexts.clone(),
        records: repository.records.clone(),
        spine,
    }
}

/// The caches an environment holds for the repositories opened through it.
///
/// Nothing here needs calling: a branch opened through an environment finds
/// its caches there, and they go when the environment does. These are for
/// an embedder that wants a say before then.
pub struct HeldCaches;

impl HeldCaches {
    /// Bound the tree nodes `env` holds, for every repository together, at
    /// `bytes` of stored nodes.
    ///
    /// Set it before opening anything through `env`: a branch already open
    /// keeps the cache it was opened with.
    pub fn budget<Env: Holds>(env: &Env, bytes: usize) {
        let nodes: ArtifactNodeCache = NodeCache::with_budget(bytes);
        env.hold(NODES.to_string(), Arc::new(nodes));
    }

    /// Let go of everything `env` holds for `repository`: its nodes that no
    /// other repository holds, and every cache of its branches.
    ///
    /// A branch of it that is still open keeps working: it reads its nodes
    /// again, and keeps the other caches it was opened with. The next open
    /// starts cold.
    pub fn release<Env: Holds>(env: &Env, repository: &Did) {
        env.release(&held(repository));
        nodes(env).scoped(scope(repository)).release();
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{Artifact, Instruction, Pick, Value};
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_credentials::Credential;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_peer::{Peer, PeerSpace, Session};
    use futures_util::stream;

    use super::HeldCaches;
    use crate::helpers::test_repo;
    use crate::{Branch, Repository};

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

        HeldCaches::release(&operator, &released.did());

        let cold = released.branch("main").open().perform(&operator).await?;
        let warm = kept.branch("main").open().perform(&operator).await?;
        assert!(cold.node_cache().get_cached(&released_root).is_none());
        assert!(warm.node_cache().get_cached(&kept_root).is_some());
        Ok(())
    }

    /// The budget bounds the nodes the environment holds for every
    /// repository together: with no room, nothing is kept and commits still
    /// land.
    #[dialog_common::test]
    async fn it_holds_no_more_nodes_than_the_budget_allows() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        HeldCaches::budget(&operator, 1);
        let repository = test_repo(&operator, &profile).await;

        let (main, root) = commit(&repository, &operator).await?;

        assert!(main.node_cache().is_empty());
        assert!(main.node_cache().get_cached(&root).is_none());
        Ok(())
    }
}
