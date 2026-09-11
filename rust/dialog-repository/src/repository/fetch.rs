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
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt as _};

use async_trait::async_trait;
use dialog_artifacts::tree::{TreeStorageBridge, selector_range};
use dialog_common::Blake3Hash as NodeHash;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_search_tree::{
    Buffer, Cache, ContentAddressedStorage, DialogSearchTreeError, Traversable as _,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};

use crate::repository::source::Source;
use crate::{
    EMPTY_TREE_HASH, Hydrate, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
};

#[cfg(not(target_arch = "wasm32"))]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + 'a>>;

/// A stream that also drives the env's [`PreloadQueue`]: polling it
/// executes queued preload hints, borrowing `env` for exactly the
/// stream's lifetime.
///
/// Each job replicates its selector against every source in `sources`:
/// every block the walk touches lands in the line's node cache and,
/// through the networked index, the local archive — so the later demand
/// read is local. Job errors surface as nothing (a preload that fails
/// must stay invisible; the demand read owns the error).
pub(crate) struct Driven<'a, S, Env> {
    inner: S,
    queue: Arc<PreloadQueue>,
    sources: Vec<Source>,
    env: &'a Env,
    budget: FetchBudget,
    likely_inflight: usize,
    maybe_inflight: usize,
    inflight: FuturesUnordered<FetchFuture<'a>>,
}

impl<'a, S, Env> Driven<'a, S, Env>
where
    S: Stream + Unpin + 'a,
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// Wrap `stream` so that polling it also drives the env's queue.
    /// The budget is read once: a per-driver cap, so concurrent driven
    /// streams each bound their own in-flight work.
    pub(crate) fn new(
        stream: S,
        sources: Vec<Source>,
        env: &'a Env,
        queue: Arc<PreloadQueue>,
    ) -> Self {
        Self {
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
            let env = self.env;
            let future = async move {
                for source in sources {
                    // Warming is advisory: an error ends this source's
                    // walk silently, and the demand read that actually
                    // needs the data owns the failure.
                    let _ = warm_source(source, env, &selector).await;
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

impl<'a, S, Env> Stream for Driven<'a, S, Env>
where
    S: Stream + Unpin + 'a,
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
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
/// Reads go through the line's shared node cache (so a later demand read
/// is free) and the networked index (so every fetched block hydrates the
/// local archive); in-flight hydrations are shared by the env's own
/// [`Hydrate`] flight, with every concurrent reader anywhere in the
/// process. The scope is the selector's exact key range; a subtree the
/// range cannot touch is never fetched, and warming a conservative
/// superset at the edges is harmless.
async fn warm_source<Env>(
    source: Source,
    env: &Env,
    selector: &ArtifactSelector<Constrained>,
) -> Result<(), DialogSearchTreeError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let root = source.as_ref().root();
    if root == EMPTY_TREE_HASH {
        return Ok(());
    }
    let remote = source.as_ref().fallback(env).await;
    let catalog = source.as_ref().subject().archive().index();
    let store = NetworkedIndex::new(env, catalog, remote);
    let store = CacheThrough {
        cache: source.as_ref().node_cache(),
        store,
    };
    let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
    let tree = Index::from_hash(NodeHash::from(root));

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

/// The node cache in front of a hydrating store, as a raw block backend:
/// the traversal's reads hit the line's shared cache first (a job whose
/// spine a peer already warmed re-reads nothing), and every miss lands
/// in it, so the demand read that follows a warm is served from memory.
struct CacheThrough<'a, Env> {
    cache: Cache<NodeHash, Buffer>,
    store: NetworkedIndex<'a, Env>,
}

impl<Env> Clone for CacheThrough<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            store: self.store.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> StorageBackend for CacheThrough<'_, Env>
where
    Env: Provider<Get> + Provider<Put> + Provider<Hydrate> + ConditionalSync + 'static,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        StorageBackend::set(&mut self.store, key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let buffer = self
            .cache
            .get_or_fetch(&NodeHash::from(*key), async |hash| {
                self.store
                    .get(hash.as_bytes())
                    .await
                    .map(|bytes| bytes.map(Buffer::from))
            })
            .await?;
        Ok(buffer.map(|buffer| buffer.as_ref().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_operator::helpers::{test_operator_with_profile, unique_name};
    use dialog_query::{AttributeQuery, Term, the};

    use super::*;
    use crate::RepositoryExt as _;
    use crate::helpers::Counting;
    use dialog_artifacts::{Preload, PreloadRequest, Speculation};
    use dialog_query::query::Output as _;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn selector(attribute: &str) -> ArtifactSelector<Constrained> {
        ArtifactSelector::new().the(attribute.parse().expect("a valid attribute"))
    }

    /// A zero budget turns speculation off without touching demand:
    /// hints are refused (so evaluators stop composing them, keeping
    /// the deterministic soak profile's demand shape exact) and the
    /// query's rows flow as if the machinery did not exist.
    #[dialog_common::test]
    async fn it_refuses_hints_and_flows_demand_with_a_zero_budget() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("preload-off"))
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
        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("preload"))
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
}
