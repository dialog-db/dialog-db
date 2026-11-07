//! Speculative replication for evaluations: the env's ambient
//! [`PreloadQueue`] of ranges worth fetching ahead of demand, driven by
//! whatever evaluation is currently polling.
//!
//! Design: `notes/fetch-scheduler.md` (beads dialog-db-75/82). The
//! load-bearing constraints, restated:
//!
//! - **The env is never owned.** The queue holds descriptions only —
//!   selectors and ranks, no futures, no env. Work materializes into
//!   fetch futures exclusively inside a [`Driven`] stream, borrowing the
//!   same env the wrapped evaluation already borrows, and lives exactly
//!   as long as that stream.
//! - **Demand is never behind speculation.** Demand reads keep their
//!   existing path untouched; when a preload's fetch for the same object
//!   is in flight, the env's `Hydrate` flight joins them. A queued-but-
//!   unstarted item is simply ignored by demand, and hydration makes it a
//!   local no-op when the driver later reaches it.
//! - **The driver is the evaluation.** Progress happens whenever a
//!   consumer polls a driven stream — on any executor, wasm included,
//!   with nothing detached. A queue nobody drives holds no resources.
//!   The queue being ambient (an operator field, reached by the
//!   [`Speculation`](dialog_artifacts::Speculation) command) is what
//!   lets one evaluation's polling execute another's hints: queries,
//!   subscriptions, and transaction queries all enqueue and all drive.

use std::pin::Pin;
use std::sync::Arc;

use std::task::{Context, Poll};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, FetchBudget, Likelihood, PreloadQueue};
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt as _};

use dialog_artifacts::tree::selector_range;
use dialog_common::Blake3Hash as NodeHash;
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_search_tree::{DialogSearchTreeError, Traversable as _};

use crate::repository::archive::TreeEnv;
use crate::repository::branch::Erased;
use crate::repository::source::Source;
use crate::{Index, Routes};

#[cfg(not(target_arch = "wasm32"))]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + 'a>>;

/// A stream that also drives the env's [`PreloadQueue`]: polling it
/// executes queued preload hints, borrowing `env` for exactly the
/// stream's lifetime.
///
/// Each job replicates its selector against every source in `sources`:
/// every block the walk touches lands in the node cache the env holds and,
/// through the env's hydrating load, the local archive — so the later demand
/// read is local. Job errors surface as nothing (a preload that fails
/// must stay invisible; the demand read owns the error).
///
/// A source is warmed only while its misses hydrate from somewhere, as
/// the env's routes say (see [`Routes`]): read once per driver, and asked
/// per job.
pub(crate) struct Driven<'a, S> {
    inner: S,
    queue: Arc<PreloadQueue>,
    sources: Vec<Source>,
    /// The env's routes, read when the first job starts.
    routes: Option<Routes>,
    env: &'a Erased,
    budget: FetchBudget,
    likely_inflight: usize,
    maybe_inflight: usize,
    inflight: FuturesUnordered<FetchFuture<'a>>,
}

impl<'a, S> Driven<'a, S>
where
    S: Stream + Unpin + 'a,
{
    /// Wrap `stream` so that polling it also drives the env's queue.
    /// The budget is read once: a per-driver cap, so concurrent driven
    /// streams each bound their own in-flight work.
    pub(crate) fn new(
        stream: S,
        sources: Vec<Source>,
        env: &'a Erased,
        queue: Arc<PreloadQueue>,
    ) -> Self {
        Self {
            routes: None,
            inner: stream,
            budget: queue.budget(),
            queue,
            sources,
            env,
            likely_inflight: 0,
            maybe_inflight: 0,
            inflight: FuturesUnordered::new(),
        }
    }

    /// Start pending jobs up to the budget. Returns whether any started.
    fn start_jobs(&mut self) -> bool {
        let mut started = false;
        loop {
            let likely_spare = self.likely_inflight < self.budget.likely;
            let maybe_spare = self.maybe_inflight < self.budget.maybe;
            if !likely_spare && !maybe_spare {
                return started;
            }
            let Some((selector, likelihood)) = self.queue.next(likely_spare, maybe_spare) else {
                return started;
            };
            match likelihood {
                Likelihood::Likely => self.likely_inflight += 1,
                Likelihood::Maybe => self.maybe_inflight += 1,
            }
            let sources = self.sources.clone();
            let routes = self
                .routes
                .get_or_insert_with(|| Routes::of(self.env))
                .clone();
            let env = self.env;
            let future = async move {
                for source in sources {
                    // Warming is advisory: an error ends this source's
                    // walk silently, and the demand read that actually
                    // needs the data owns the failure.
                    let _ = warm_source(source, &routes, env, &selector, likelihood).await;
                }
                likelihood
            };
            #[cfg(not(target_arch = "wasm32"))]
            self.inflight.push(Box::pin(future) as FetchFuture<'a>);
            #[cfg(target_arch = "wasm32")]
            self.inflight.push(Box::pin(future) as FetchFuture<'a>);
            started = true;
        }
    }

    /// Reap completed jobs without blocking, freeing budget capacity.
    fn reap(&mut self, context: &mut Context<'_>) {
        while let Poll::Ready(Some(likelihood)) = self.inflight.poll_next_unpin(context) {
            match likelihood {
                Likelihood::Likely => self.likely_inflight -= 1,
                Likelihood::Maybe => self.maybe_inflight -= 1,
            }
        }
    }
}

impl<'a, S> Stream for Driven<'a, S>
where
    S: Stream + Unpin + 'a,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.start_jobs();
        this.reap(context);
        let polled = this.inner.poll_next_unpin(context);
        // The inner poll may have enqueued new work (an evaluator hook
        // firing mid-evaluation): start it now so it overlaps with the
        // very fetch the inner stream is parked on, instead of waiting
        // for the next wake.
        if this.start_jobs() {
            this.reap(context);
        }
        polled
    }
}

/// Replicate every block `selector`'s range can touch on `source`, level
/// by level: each depth's whole frontier fetches concurrently, so a cold
/// range costs tree-depth round trips instead of block-count round trips
/// (the level-parallel walk `traverse_available_within` already gives
/// downloads). This is the range-granular job executor of bead
/// dialog-db-76, replacing the select-and-drain executor, which paid row
/// parsing and spilled-value fetches for rows nobody read and fetched
/// leaf by leaf.
///
/// Reads load through the env, which keeps what they read in the node
/// cache it holds (so a later demand read is free) and hydrates the local
/// archive with every fetched block; in-flight hydrations are shared by the env's own
/// [`Hydrate`] scheduler with every concurrent reader anywhere in the
/// process, and ranked at the hint's likelihood there, so warming never
/// takes a site's slot from a demand read. The scope is the selector's exact key range; a subtree the
/// range cannot touch is never fetched, and warming a conservative
/// superset at the edges is harmless.
async fn warm_source(
    source: Source,
    routes: &Routes,
    env: &Erased,
    selector: &ArtifactSelector<Constrained>,
    likelihood: Likelihood,
) -> Result<(), DialogSearchTreeError> {
    let Some(root) = source.as_ref().root() else {
        return Ok(());
    };
    // Warming exists to fetch ahead of demand. With nowhere to fetch
    // from, every block is local already and a demand read finds it; a
    // walk here would only re-read, and re-hash, what is already there.
    if !routes.fetches(source.as_ref().subject().did()) {
        return Ok(());
    }
    let catalog = ArchiveScope::new(source.as_ref().subject()).index();
    let storage: &dyn TreeEnv = env;
    // Every load the walk performs is ranked at the hint's likelihood.
    let tree = Index::at(catalog, NodeHash::from(root)).ranked(likelihood.into());

    let manifest = tree.manifest(&storage).await?;
    let range = selector_range(selector, &manifest);
    let scope = [range.start().as_ref().to_vec()..=range.end().as_ref().to_vec()];

    let visits = tree.traverse_available_within(&storage, &scope);
    futures_util::pin_mut!(visits);
    while let Some(visit) = visits.next().await {
        // A present node has landed in the caches, which is the whole
        // point; an absent block is a partial region (nothing to warm);
        // an error ends the walk, owned by whichever demand read hits it.
        if visit.is_err() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_effects::archive::ArchiveError;
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_peer::helpers::{test_session_with_peer, unique_name};
    use dialog_query::{AttributeQuery, Term, the};

    use super::*;
    use crate::RepositoryExt as _;
    use crate::helpers::{Counting, connect};
    use dialog_artifacts::tree::ArtifactTreeExt as _;
    use dialog_artifacts::{
        ArchiveDelta, Artifact, Datum, Entity, Instruction, Key, Preload, PreloadRequest,
        Speculation, State, Value,
    };
    use dialog_capability::{Capability, Subject};
    use dialog_search_tree::helpers::Blocks;
    use dialog_storage::DialogStorageError;
    use dialog_varsig::did;
    use futures_util::stream;

    use async_trait::async_trait;
    use dialog_artifacts::tree::ArtifactNodeCache;
    use dialog_capability::Provider;
    use dialog_common::{Held, Holdings, Holds};
    use dialog_effects::archive::Get;
    use dialog_query::query::Output as _;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// An archive that answers every read with the same block, whatever
    /// was asked for, and holds nothing to hydrate; it holds the line's
    /// node cache.
    struct Impostor(Vec<u8>, Holdings);

    impl Holds for Impostor {
        fn held(&self, key: &str) -> Option<Held> {
            self.1.held(key)
        }

        fn hold(&self, key: String, handle: Held) {
            self.1.hold(key, handle)
        }

        fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
            self.1.held_or(key, make)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Provider<Get> for Impostor {
        async fn execute(&self, _: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
            Ok(Some(self.0.clone()))
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl Provider<ArchiveLoad> for Impostor {
        async fn execute(
            &self,
            _: ArchiveLoad,
        ) -> Result<Option<dialog_common::Buffer>, ArchiveError> {
            Ok(Some(dialog_common::Buffer::from(self.0.clone())))
        }
    }

    dialog_search_tree::reads_nodes_through_blocks!(Impostor);

    /// Persists a one-fact tree into `blocks` and returns its root.
    async fn persist(blocks: &Blocks, name: &str) -> Result<NodeHash> {
        let mut tree = Index::empty_in(blocks.catalog().clone());
        let mut delta = ArchiveDelta::zero();
        tree.apply(
            blocks,
            &mut delta,
            stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "profile/name".parse()?,
                    of: Entity::new()?,
                    is: Value::String(name.into()),
                    cause: None,
                },
                dialog_artifacts::Pick::All,
            )]),
        )
        .await?;
        delta.flush_into(blocks);
        Ok(tree.root().clone())
    }

    /// A well-formed node the archive returns for some other hash is
    /// refused and never enters the line's node cache, so a later read of
    /// that hash cannot be served the wrong node from memory.
    #[dialog_common::test]
    async fn it_keeps_a_node_that_is_not_the_one_asked_for_out_of_the_cache() -> Result<()> {
        let catalog =
            ArchiveScope::new(Subject::from(did!("key:zImpostorArchiveTest"))).catalog("index");
        let blocks = Blocks::in_catalog(catalog.clone());
        let asked = persist(&blocks, "Alice").await?;
        let other = persist(&blocks, "Bob").await?;
        let archive = Impostor(
            blocks
                .get(&other)
                .expect("the other tree's root is stored")
                .into_vec(),
            Holdings::default(),
        );
        let cache = ArtifactNodeCache::new();
        cache.hold(&archive);

        let loaded: Result<Option<dialog_search_tree::PersistentNode<Key, State<Datum>>>, _> =
            dialog_search_tree::load_node(&archive, &catalog, &asked).await;

        assert!(
            matches!(
                loaded,
                Err(DialogSearchTreeError::Storage(
                    DialogStorageError::Verification(_)
                ))
            ),
            "a node that does not hash to the one asked for must be refused, got {loaded:?}"
        );
        let held = ArtifactNodeCache::of(&archive, catalog.subject())
            .expect("the archive holds the line's node cache");
        assert!(
            held.get_cached(&asked).is_none(),
            "the impostor must not be cached under the hash asked for"
        );
        Ok(())
    }

    fn selector(attribute: &str) -> ArtifactSelector<Constrained> {
        ArtifactSelector::new().the(attribute.parse().expect("a valid attribute"))
    }

    /// A zero budget turns speculation off without touching demand:
    /// hints are refused (so evaluators stop composing them, keeping
    /// the deterministic soak profile's demand shape exact) and the
    /// query's rows flow as if the machinery did not exist.
    #[dialog_common::test]
    async fn it_refuses_hints_and_flows_demand_with_a_zero_budget() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let env = Counting::new(operator);
        let repo = profile
            .space(unique_name("preload-off"))
            .create()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;
        branch
            .transaction()
            .assert(
                the!("left/name")
                    .of("id:only".parse()?)
                    .is("left".to_string()),
            )
            .commit()
            .publish()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        let queue = Provider::<Speculation>::execute(&env, ()).await;
        queue.set_budget(dialog_artifacts::FetchBudget::ZERO);

        let listening = Provider::<Preload>::execute(
            &env,
            PreloadRequest {
                selector: selector("right/name"),
                likelihood: Likelihood::Likely,
            },
        )
        .await;
        assert!(!listening, "a zero budget refuses hints");
        assert_eq!(queue.pending(), 0, "a refused hint enqueues nothing");

        let rows = branch
            .query()
            .select(AttributeQuery::new(
                Term::from(the!("left/name")),
                Term::blank(),
                Term::blank(),
                Term::blank(),
                None,
            ))
            .perform(&env)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "demand rows flow with speculation off");
        Ok(())
    }

    /// A hint enqueued through the env's ambient queue is executed by
    /// whatever evaluation polls next — here a query that never asked
    /// for it — and the hinted range is local afterwards: its own
    /// select then reads nothing from the backend. This is the
    /// cross-evaluation sharing the ambient design exists for.
    #[dialog_common::test]
    async fn it_replicates_hinted_ranges_while_any_query_runs() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let env = Counting::new(operator);
        let repo = profile
            .space(unique_name("preload"))
            .create()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        let mut transaction = branch.transaction();
        for index in 0..40 {
            let entity: dialog_artifacts::Entity = format!("id:{index}").parse()?;
            transaction = transaction
                .assert(
                    the!("left/name")
                        .of(entity.clone())
                        .is(format!("left {index}")),
                )
                .assert(the!("right/name").of(entity).is(format!("right {index}")));
        }
        transaction.commit().publish().perform(&env).await?;
        // Reopen so the durable layer reads the published head.
        let branch = repo.branch("main").open().perform(&env).await?;

        let listening = Provider::<Preload>::execute(
            &env,
            PreloadRequest {
                selector: selector("right/name"),
                likelihood: Likelihood::Likely,
            },
        )
        .await;
        assert!(listening, "the default budget accepts hints");

        let left = AttributeQuery::new(
            Term::from(the!("left/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );
        let rows = branch.query().select(left).perform(&env).try_vec().await?;
        assert_eq!(rows.len(), 40, "the demand query yields its rows");

        let queue = Provider::<Speculation>::execute(&env, ()).await;
        assert_eq!(queue.pending(), 0, "the driven stream executed the hint");

        // The hinted range is now local: reading it touches the
        // backend not at all (every node is in the line's shared cache).
        let before = env.count("archive::Get");
        let right = AttributeQuery::new(
            Term::from(the!("right/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );
        let rows = branch.query().select(right).perform(&env).try_vec().await?;
        assert_eq!(rows.len(), 40);
        assert_eq!(
            env.count("archive::Get") - before,
            0,
            "a hinted range reads nothing from the backend"
        );
        Ok(())
    }

    /// Every warm-up job asks whether the source's misses hydrate from
    /// somewhere. It is answered from the routes the branch registered on
    /// the env, with no store read, and a driver reads them once for
    /// every job: four hints in flight add no memory reads to the query.
    #[dialog_common::test]
    async fn it_loads_a_sources_remote_fallback_once_per_driver() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let env = Counting::new(operator);
        let repo = profile
            .space(unique_name("fallback-once"))
            .create()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        let mut transaction = branch.transaction();
        for index in 0..10 {
            let entity: dialog_artifacts::Entity = format!("id:{index}").parse()?;
            transaction = transaction
                .assert(the!("a/name").of(entity.clone()).is(format!("a {index}")))
                .assert(the!("b/name").of(entity.clone()).is(format!("b {index}")))
                .assert(the!("c/name").of(entity.clone()).is(format!("c {index}")))
                .assert(the!("d/name").of(entity.clone()).is(format!("d {index}")))
                .assert(the!("e/name").of(entity).is(format!("e {index}")));
        }
        transaction.commit().publish().perform(&env).await?;

        // Track a remote, so a warm-up has a fallback to load. Nothing
        // here reaches the network: every block the query and the
        // warm-ups read is local.
        let site = dialog_remote_s3::Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()?;
        let origin = connect("origin", site, repo.did(), &env).await?;
        let remote_branch = origin.branch("main").open().perform(&env).await?;
        branch.set_upstream(remote_branch).perform(&env).await?;
        let branch = repo.branch("main").open().perform(&env).await?;

        let query = || {
            AttributeQuery::new(
                Term::from(the!("a/name")),
                Term::blank(),
                Term::blank(),
                Term::blank(),
                None,
            )
        };

        // The routes are the branch's own tracking: asking them reads no
        // memory. What the query costs with nothing to warm:
        let before = env.count("memory::Resolve");
        assert!(
            branch.routes().fetches(branch.of()),
            "a branch tracking a remote hydrates from it"
        );
        assert_eq!(env.count("memory::Resolve"), before);
        let before = env.count("memory::Resolve");
        branch
            .query()
            .select(query())
            .perform(&env)
            .try_vec()
            .await?;
        let bare = env.count("memory::Resolve") - before;

        for attribute in ["b/name", "c/name", "d/name", "e/name"] {
            let listening = Provider::<Preload>::execute(
                &env,
                PreloadRequest {
                    selector: selector(attribute),
                    likelihood: Likelihood::Likely,
                },
            )
            .await;
            assert!(listening, "the default budget accepts hints");
        }
        let before = env.count("memory::Resolve");
        branch
            .query()
            .select(query())
            .perform(&env)
            .try_vec()
            .await?;
        let warmed = env.count("memory::Resolve") - before;
        let queue = Provider::<Speculation>::execute(&env, ()).await;
        assert_eq!(queue.pending(), 0, "the driven query executed every hint");

        assert_eq!(
            warmed,
            bare,
            "four warm-ups add no fallback loads: {:?}",
            env.snapshot()
        );
        Ok(())
    }
}
