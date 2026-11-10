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
//! - **A hint outlives the driver that took it.** A driver that ends
//!   with walks unfinished (its evaluation's stream ended or was dropped)
//!   hands their descriptions back to the queue, so the next driver
//!   resumes them. What moves back is the selector and its rank, never
//!   the walk: the queue still holds only descriptions.

use std::mem;
use std::pin::Pin;
use std::sync::Arc;

use std::task::{Context, Poll};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, FetchBudget, Likelihood, PreloadQueue, PreloadRequest};
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

/// One taken hint: the walk, and the description it was started from,
/// kept beside it so a driver that ends before the walk does can hand
/// the description back to the queue.
struct Job<'a> {
    selector: Arc<ArtifactSelector<Constrained>>,
    likelihood: Likelihood,
    walk: FetchFuture<'a>,
}

impl Future for Job<'_> {
    type Output = Likelihood;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Likelihood> {
        self.walk.as_mut().poll(context)
    }
}

/// A stream that also drives the env's [`PreloadQueue`]: polling it
/// executes queued preload hints, borrowing `env` for exactly the
/// stream's lifetime. Hints it took and did not finish go back to the
/// queue when it drops (see the [`Drop`] impl).
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
    inflight: FuturesUnordered<Job<'a>>,
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
            let selector = Arc::new(selector);
            let walked = selector.clone();
            let future = async move {
                for source in sources {
                    // Warming is advisory: an error ends this source's
                    // walk silently, and the demand read that actually
                    // needs the data owns the failure.
                    let _ = warm_source(source, &routes, env, &walked, likelihood).await;
                }
                likelihood
            };
            self.inflight.push(Job {
                selector,
                likelihood,
                walk: Box::pin(future),
            });
            started = true;
        }
    }

    /// Whether nothing is queued for this driver and none of its jobs is
    /// in flight: every hint it took has been walked to the end.
    #[cfg(test)]
    fn is_idle(&self) -> bool {
        self.inflight.is_empty() && self.queue.pending() == 0
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

/// A driver that ends with walks unfinished hands their hints back to
/// the queue, so whichever evaluation drives next resumes them:
/// speculation belongs to whoever is running, not to the evaluation that
/// happened to pop it. Each goes through [`PreloadQueue::preload`], the
/// path a [`Preload`](dialog_artifacts::Preload) hint takes, so it obeys
/// what that path does: with a zero budget it is refused (speculation was
/// turned off, so it is dropped), a selector already pending in its rank
/// is not enqueued twice, and it queues behind fresher hints, under the
/// same drop-oldest bound.
///
/// A walk that ended, completed or failed, has left `inflight` and is
/// not handed back: an error ends a walk normally (the `break` in
/// [`warm_source`]), and the demand read that hits it owns the failure.
///
/// Why resuming terminates: a resumed walk restarts from the root, but
/// every block an earlier attempt fetched was hydrated into the local
/// archive inside its fetch, so the next attempt reads it locally and
/// gets further whenever an attempt lasted long enough for one fetch to
/// land. An attempt shorter than that (its reads were still waiting in
/// the site's lane behind demand, or were cancelled with it) makes no
/// progress, but costs no more than the requests the lane had admitted
/// for it (a read still waiting for a slot leaves for free), and leaves
/// one queue entry, not a growing pile: the hint is one pending selector
/// however many drivers hand it back. Its next attempt reaches the same
/// point no later, so the first driver that lasts one round trip moves it.
/// So no resumption cap: a cap would drop exactly the hints of a slow
/// remote, where queries end before walks do.
///
/// Concurrent drivers need nothing more than the queue's own lock: a hint
/// handed back here is popped by another driver's next `start_jobs`.
impl<S> Drop for Driven<'_, S> {
    fn drop(&mut self) {
        for job in mem::take(&mut self.inflight) {
            let Job {
                selector,
                likelihood,
                walk,
            } = job;
            // The walk holds the other reference to the selector.
            drop(walk);
            self.queue.preload(PreloadRequest {
                selector: Arc::unwrap_or_clone(selector),
                likelihood,
            });
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
    // Counted by whoever subscribes (the perf harness counts spans by
    // name): a walk started, and below, each node it visited.
    drop(tracing::trace_span!("warm_walk"));
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
        drop(tracing::trace_span!("warm_node"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_effects::archive::ArchiveError;
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_peer::helpers::{test_session_with_peer, unique_name};
    use dialog_peer::{Peer, Session};
    use dialog_query::{AttributeQuery, Term, the};
    use dialog_storage::provider::storage::VolatileSpace;

    use super::*;
    use crate::RepositoryExt as _;
    use crate::helpers::{Counting, connect};
    use dialog_artifacts::tree::ArtifactNodeCache;
    use dialog_artifacts::tree::ArtifactTreeExt as _;
    use dialog_artifacts::{
        ArchiveDelta, Artifact, Datum, Entity, Instruction, Key, Preload, PreloadRequest,
        Speculation, State, Value,
    };
    use dialog_capability::Provider;
    use dialog_capability::{Capability, Subject};
    use dialog_common::{Held, Holdings, Holds};
    use dialog_effects::archive::Get;
    use dialog_query::query::Output as _;
    use dialog_search_tree::helpers::{Blocks, buffer_decodes};
    use dialog_storage::DialogStorageError;
    use dialog_varsig::did;
    use futures_util::future::poll_fn;
    use futures_util::stream;

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

    #[dialog_capability::provider]
    impl Provider<Get> for Impostor {
        async fn execute(&self, _: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
            Ok(Some(self.0.clone()))
        }
    }

    #[dialog_capability::provider]
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

    /// `rounds` batches of `per` entities' `a/name` and `b/name`. Each
    /// committed on its own leaves the index nodes above the leaves
    /// holding ops in their links' buffers.
    fn rounds(rounds: usize, per: usize) -> Result<Vec<dialog_artifacts::Changes>> {
        use dialog_artifacts::Statement as _;
        let mut batches = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let mut changes = dialog_artifacts::Changes::new();
            for index in 0..per {
                let entity: dialog_artifacts::Entity = format!("id:{round}-{index}").parse()?;
                the!("a/name")
                    .of(entity.clone())
                    .is(format!("a {round} {index}"))
                    .assert(&mut changes);
                the!("b/name")
                    .of(entity)
                    .is(format!("b {round} {index}"))
                    .assert(&mut changes);
            }
            batches.push(changes);
        }
        Ok(batches)
    }

    fn attribute_query(attribute: &str) -> AttributeQuery {
        AttributeQuery::new(
            Term::from(
                attribute
                    .parse::<dialog_query::Relation>()
                    .expect("an attribute"),
            ),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        )
    }

    /// A member that tracks a remote and holds every block warms a hinted
    /// range without reading a block, fetching one, or decoding a buffer:
    /// the walk picks the children a read of the range descends into from
    /// the separators alone, and finds each of them in the line's cache.
    ///
    /// "Reads no block" holds because the read before it left every node
    /// of both ranges in the node cache, and the cache keeps them: its
    /// budget is `NODE_CACHE_BUDGET` (128 MiB) on every target and profile,
    /// against a tree of a few hundred kilobytes here. A cache small enough
    /// to evict them would make the walk read them back from the local
    /// archive, which is still no fetch and no decode.
    #[dialog_common::test]
    async fn it_warms_a_local_member_without_reading_fetching_or_decoding() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let env = Counting::new(operator);
        let repo = profile
            .space(unique_name("local-member"))
            .create()
            .perform(&env)
            .await?;
        let branch = repo.branch("main").open().perform(&env).await?;
        for batch in rounds(24, 12)? {
            branch
                .transaction()
                .integrate(batch)
                .commit()
                .publish()
                .perform(&env)
                .await?;
        }

        // Track a remote nothing here reaches: every block is local.
        let site = dialog_remote_s3::Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()?;
        let origin = connect("origin", site, repo.did(), &env).await?;
        let remote_branch = origin.branch("main").open().perform(&env).await?;
        branch.set_upstream(remote_branch).perform(&env).await?;
        let branch = repo.branch("main").open().perform(&env).await?;
        assert!(
            branch.routes().fetches(branch.of()),
            "a member tracking a remote has reads warm ahead"
        );

        // Both ranges read once: what a view's reads touch is cached.
        for attribute in ["a/name", "b/name"] {
            branch
                .query()
                .select(attribute_query(attribute))
                .perform(&env)
                .try_vec()
                .await?;
        }

        // The nodes a walk of `b/name` passes hold buffered links it could
        // decode: without them this pins nothing.
        let source = Source::from(branch.clone());
        let root = source.as_ref().root().expect("the branch has a head");
        let tree = Index::at(
            ArchiveScope::new(source.as_ref().subject()).index(),
            NodeHash::from(root),
        );
        let manifest = tree.manifest(&env).await?;
        let range = selector_range(&selector("b/name"), &manifest);
        let scope = [range.start().as_ref().to_vec()..=range.end().as_ref().to_vec()];
        let mut buffered = 0usize;
        {
            let visits = tree.traverse_available_within(&env, &scope);
            futures_util::pin_mut!(visits);
            while let Some(visit) = visits.next().await {
                if let Some(node) = visit?.node()
                    && let dialog_search_tree::NodeBody::Index(index) = node.body()
                {
                    buffered += index.novelty_len();
                }
            }
        }
        assert!(
            buffered > 0,
            "fixture: the walk's nodes must hold buffered ops it could decode"
        );

        // What the read of `a/name` costs with nothing to warm.
        let decodes = buffer_decodes();
        let rows = branch
            .query()
            .select(attribute_query("a/name"))
            .perform(&env)
            .try_vec()
            .await?;
        let bare = buffer_decodes() - decodes;
        assert_eq!(rows.len(), 24 * 12);

        let listening = Provider::<Preload>::execute(
            &env,
            PreloadRequest {
                selector: selector("b/name"),
                likelihood: Likelihood::Likely,
            },
        )
        .await;
        assert!(listening, "the default budget accepts hints");

        let gets = env.count("archive::Get");
        let hydrations = env.count("hydrate::Hydrate");
        let decodes = buffer_decodes();
        let rows = branch
            .query()
            .select(attribute_query("a/name"))
            .perform(&env)
            .try_vec()
            .await?;
        let warmed = buffer_decodes() - decodes;
        assert_eq!(rows.len(), 24 * 12);
        let queue = Provider::<Speculation>::execute(&env, ()).await;
        assert_eq!(queue.pending(), 0, "the driven query executed the hint");

        assert_eq!(
            env.count("archive::Get") - gets,
            0,
            "the walk reads no block"
        );
        assert_eq!(
            env.count("hydrate::Hydrate") - hydrations,
            0,
            "the walk fetches nothing"
        );
        assert_eq!(
            warmed, bare,
            "the walk decodes no buffer: the query decoded what it decodes alone"
        );
        Ok(())
    }

    type Replica = Counting<Peer<VolatileSpace, Session>>;

    /// A replica that adopted, by reference, the head an origin pushed to
    /// a filesystem remote, so it holds none of the tree: 4 rounds of 40
    /// entities' `a/name` and `b/name`. Returns the replica's counting env
    /// and its branch.
    async fn partial_replica() -> Result<(Replica, crate::Branch)> {
        use crate::SiteAddress;
        use dialog_effects::credential::prelude::*;
        use dialog_effects::storage::{Directory, Location};
        use dialog_remote_fs::FsAddress;
        use dialog_storage::provider::FileSystem;
        use dialog_storage::resource::Resource as _;

        // The origin: a repository that commits and pushes to a directory
        // a remote serves.
        let (operator, profile) = test_session_with_peer().await;
        let source_repo = profile
            .space(unique_name("partial-source"))
            .create()
            .perform(&operator)
            .await?;
        let location = Location::new(Directory::Temp, unique_name("partial-remote"));
        let site = FileSystem::open(&location).await?;
        source_repo
            .did()
            .credential()
            .key("self")
            .save(dialog_credentials::Credential::Signer(
                source_repo.credential().clone(),
            ))
            .perform(&site)
            .await?;
        let address = SiteAddress::Fs(FsAddress::new(location));
        let origin = connect("origin", address.clone(), source_repo.did(), &operator).await?;
        let source = source_repo.branch("main").open().perform(&operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        source.set_upstream(remote).perform(&operator).await?;
        for batch in rounds(4, 40)? {
            source
                .transaction()
                .integrate(batch)
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        assert!(source.push().perform(&operator).await?.is_some());

        // A replica on its own profile and storage, adopting the head by
        // reference: it holds none of the tree.
        let (replica_operator, replica_profile) = test_session_with_peer().await;
        replica_profile
            .access()
            .save(
                source_repo
                    .access()
                    .claim(&source_repo)
                    .delegate(replica_profile.did())
                    .perform(&operator)
                    .await?,
            )
            .perform(&replica_operator)
            .await?;
        let replica_repo = replica_profile
            .space(unique_name("partial-replica"))
            .open()
            .perform(&replica_operator)
            .await?;
        let origin = connect("origin", address, source_repo.did(), &replica_operator).await?;
        let replica = replica_repo
            .branch("main")
            .open()
            .perform(&replica_operator)
            .await?;
        let remote = origin
            .branch("main")
            .open()
            .perform(&replica_operator)
            .await?;
        replica
            .set_upstream(remote)
            .perform(&replica_operator)
            .await?;
        replica
            .pull()
            .perform(&replica_operator)
            .await?
            .expect("the replica adopts the upstream head");
        let replica = replica_repo
            .branch("main")
            .open()
            .perform(&replica_operator)
            .await?;

        Ok((Counting::new(replica_operator), replica))
    }

    /// Enqueue a `Likely` hint for `attribute` through the env.
    async fn hint(env: &Replica, attribute: &str) {
        let listening = Provider::<Preload>::execute(
            env,
            PreloadRequest {
                selector: selector(attribute),
                likelihood: Likelihood::Likely,
            },
        )
        .await;
        assert!(listening, "the default budget accepts hints");
    }

    /// Poll `driver` until it is idle: every hint it took walked to the end.
    async fn drive_to_idle<'a, S: Stream + Unpin + 'a>(driver: &mut Driven<'a, S>) {
        poll_fn(|context| {
            let _ = driver.poll_next_unpin(context);
            if driver.is_idle() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    /// Read `attribute` on `replica` and assert it fetched nothing and read
    /// no block: the walk left the whole range local and in the line's cache.
    async fn assert_read_is_local(
        env: &Replica,
        replica: &crate::Branch,
        attribute: &str,
    ) -> Result<()> {
        let hydrations = env.count("hydrate::Hydrate");
        let gets = env.count("archive::Get");
        let rows = replica
            .query()
            .select(attribute_query(attribute))
            .perform(env)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 4 * 40);
        assert_eq!(
            env.count("hydrate::Hydrate") - hydrations,
            0,
            "the walk fetched the hinted range, so its own read fetches nothing"
        );
        assert_eq!(
            env.count("archive::Get") - gets,
            0,
            "and the walk left every node it fetched in the line's cache"
        );
        Ok(())
    }

    /// A replica that adopted a head by reference still warms ahead of
    /// demand: a walk of a hinted range the replica does not hold fetches
    /// the whole range, so the range's own read then fetches nothing. A
    /// driver that walked its hints to the end hands nothing back when it
    /// drops.
    ///
    /// This test drives the walk itself, with a driver over a stream that
    /// never yields, until the driver is idle, so it pins what a walk
    /// fetches rather than how long a query happens to last.
    #[dialog_common::test]
    async fn it_fetches_a_hinted_range_a_partial_replica_lacks() -> Result<()> {
        let (env, replica) = partial_replica().await?;
        hint(&env, "b/name").await;

        let queue = Provider::<Speculation>::execute(&env, ()).await;
        let erased: &Erased = &env;
        let mut driver = Driven::new(
            stream::pending::<()>(),
            vec![Source::from(replica.clone())],
            erased,
            queue.clone(),
        );
        drive_to_idle(&mut driver).await;
        assert_eq!(queue.pending(), 0, "the driver took the hint");
        assert!(
            env.count("hydrate::Hydrate") > 0,
            "the replica held none of the range, so the walk fetched it"
        );
        drop(driver);
        assert_eq!(
            queue.pending(),
            0,
            "a walk that ended is not handed back to the queue"
        );

        assert_read_is_local(&env, &replica, "b/name").await
    }

    /// A driver dropped while its walk waits on the remote hands the hint
    /// back to the queue, and the next driver, which never saw it hinted,
    /// finishes it: the range's own read then fetches nothing. This is the
    /// query that ends before the walk it took, as on a slow remote.
    #[dialog_common::test]
    async fn it_resumes_a_hint_a_dropped_driver_did_not_finish() -> Result<()> {
        let (env, replica) = partial_replica().await?;
        hint(&env, "b/name").await;

        let queue = Provider::<Speculation>::execute(&env, ()).await;
        let erased: &Erased = &env;
        let sources = vec![Source::from(replica.clone())];

        // One poll: the driver takes the hint and starts the walk, whose
        // first read is a fetch of the root from the remote.
        let mut first = Driven::new(
            stream::pending::<()>(),
            sources.clone(),
            erased,
            queue.clone(),
        );
        poll_fn(|context| {
            let _ = first.poll_next_unpin(context);
            Poll::Ready(())
        })
        .await;
        assert_eq!(queue.pending(), 0, "the first driver took the hint");
        assert!(
            !first.is_idle(),
            "fixture: the walk must be waiting on the remote when its driver drops"
        );
        drop(first);
        assert_eq!(
            queue.pending(),
            1,
            "the dropped driver handed its unfinished hint back"
        );

        let mut second = Driven::new(stream::pending::<()>(), sources, erased, queue.clone());
        drive_to_idle(&mut second).await;
        drop(second);
        assert_eq!(queue.pending(), 0, "the second driver finished the walk");

        assert_read_is_local(&env, &replica, "b/name").await
    }
}
