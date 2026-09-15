//! Speculative replication for queries: a plan of ranges worth fetching
//! ahead of demand, driven by the query's own evaluation.
//!
//! Design: `notes/fetch-scheduler.md` (bead dialog-db-75). The load-bearing
//! constraints, restated:
//!
//! - **The env is never owned.** A [`FetchPlan`] holds descriptions only —
//!   selectors and ranks, no futures, no env. Work materializes into fetch
//!   futures exclusively inside [`FetchPlan::drive`], borrowing the same
//!   env the wrapped query stream already borrows, and lives exactly as
//!   long as that stream.
//! - **Demand is never behind speculation.** Demand reads keep their
//!   existing path untouched; when a preload's fetch for the same object
//!   is in flight, the transport's `Flight` joins them. A queued-but-
//!   unstarted item is simply ignored by demand, and hydration makes it a
//!   local no-op when the driver later reaches it.
//! - **The driver is the query.** Progress happens whenever the consumer
//!   polls the driven stream — on any executor, wasm included, with
//!   nothing detached. A plan nobody drives holds no resources.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use std::task::{Context, Poll};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, Likelihood};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use futures_util::stream::FuturesUnordered;
use futures_util::{Stream, StreamExt as _};

use crate::RemoteSite;
use crate::repository::archive::networked::HydrationFlight;
use crate::repository::branch::select_from_source;
use crate::repository::source::Source;

#[cfg(not(target_arch = "wasm32"))]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type FetchFuture<'a> = Pin<Box<dyn Future<Output = Likelihood> + 'a>>;

/// How many speculative fetch jobs may run concurrently, per rank.
///
/// A budget is per driven stream, not global: two queries each drive
/// their own plan under their own budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchBudget {
    /// Concurrent jobs for [`Likelihood::Likely`] work.
    pub likely: usize,
    /// Concurrent jobs for [`Likelihood::Maybe`] work.
    pub maybe: usize,
}

impl Default for FetchBudget {
    /// Sized from the soak's cold-join budget sweep (see
    /// `notes/fetch-scheduler.md`): rounds shrink with budget up to a
    /// knee near 256 once hydration is single-flighted; `Maybe` work
    /// stays narrow until something promotes it.
    fn default() -> Self {
        Self {
            likely: 256,
            maybe: 16,
        }
    }
}

/// One enqueued preload: a selector to replicate, tagged by the handle
/// that owns it so the handle can abort or promote it while it is still
/// pending.
#[derive(Debug, Clone)]
struct Job {
    handle: u64,
    selector: ArtifactSelector<Constrained>,
}

#[derive(Debug, Default)]
struct State {
    likely: VecDeque<Job>,
    maybe: VecDeque<Job>,
    next_handle: u64,
}

/// A plan of speculative fetches: pure data, shared by clone, carrying
/// the budget it is driven under.
///
/// [`preload`](Self::preload) enqueues; [`drive`](Self::drive) executes
/// against a borrowed env. See the module docs for the ownership rules.
#[derive(Debug, Clone, Default)]
pub struct FetchPlan {
    budget: FetchBudget,
    state: Arc<Mutex<State>>,
}

/// A bare budget is a fresh plan driven under it, so a query can stage
/// speculation without naming the plan: `.preload(FetchBudget::default())`.
impl From<FetchBudget> for FetchPlan {
    fn from(budget: FetchBudget) -> Self {
        Self::new().with_budget(budget)
    }
}

impl FetchPlan {
    /// A fresh, empty plan under the default budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// This plan under `budget`: how many of its jobs may run
    /// concurrently, per rank, when a query drives it. Clones made
    /// before or after carry their own copy; the one handed to
    /// `.preload(..)` is the one whose budget the driven stream honors.
    pub fn with_budget(mut self, budget: FetchBudget) -> Self {
        self.budget = budget;
        self
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Never held across an await.
        self.state.lock()
    }

    /// Enqueue a selector's backing blocks for speculative replication.
    ///
    /// Returns a handle that can [`abort`](PreloadHandle::abort) or
    /// [`promote`](PreloadHandle::promote) the work while it is still
    /// pending; dropping the handle changes nothing.
    pub fn preload(
        &self,
        selector: ArtifactSelector<Constrained>,
        likelihood: Likelihood,
    ) -> PreloadHandle {
        let mut state = self.lock();
        state.next_handle += 1;
        let handle = state.next_handle;
        let job = Job { handle, selector };
        match likelihood {
            Likelihood::Likely => state.likely.push_back(job),
            Likelihood::Maybe => state.maybe.push_back(job),
        }
        PreloadHandle {
            id: handle,
            state: self.state.clone(),
        }
    }

    /// Pending jobs, for tests and introspection.
    pub fn pending(&self) -> usize {
        let state = self.lock();
        state.likely.len() + state.maybe.len()
    }

    /// Dequeue the next job whose rank has spare capacity: `Likely`
    /// drains before `Maybe`.
    fn next(&self, likely_spare: bool, maybe_spare: bool) -> Option<(Job, Likelihood)> {
        let mut state = self.lock();
        if likely_spare && let Some(job) = state.likely.pop_front() {
            return Some((job, Likelihood::Likely));
        }
        if maybe_spare && let Some(job) = state.maybe.pop_front() {
            return Some((job, Likelihood::Maybe));
        }
        None
    }

    /// Wrap `stream` so that polling it also executes this plan's jobs,
    /// borrowing `env` for exactly the stream's lifetime.
    ///
    /// Each job replicates its selector by running the ordinary line
    /// select against every source and draining it: every block the scan
    /// touches lands in the line's node cache and, through the networked
    /// index, the local archive — so the later demand read is local. Job
    /// errors surface as nothing (a preload that fails must stay
    /// invisible; the demand read owns the error).
    pub(crate) fn drive<'a, S, Env>(
        &self,
        stream: S,
        sources: Vec<Source>,
        env: &'a Env,
        hydration: Arc<HydrationFlight<'a>>,
    ) -> Driven<'a, S, Env>
    where
        S: Stream + Unpin + 'a,
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        Driven {
            inner: stream,
            plan: self.clone(),
            sources,
            env,
            hydration,
            budget: self.budget,
            likely_inflight: 0,
            maybe_inflight: 0,
            inflight: FuturesUnordered::new(),
        }
    }
}

/// A pending preload's handle: cheap queue surgery on work that has not
/// started yet. In-flight and completed work is unaffected.
#[derive(Debug, Clone)]
pub struct PreloadHandle {
    id: u64,
    state: Arc<Mutex<State>>,
}

impl PreloadHandle {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock()
    }

    /// Drop this handle's still-pending jobs. Work already in flight
    /// completes (and hydrates) regardless: started fetches are paid
    /// for, and throwing paid-for bytes away is the pre-#495 bug this
    /// module exists to prevent.
    pub fn abort(&self) {
        let mut state = self.lock();
        let id = self.id;
        state.likely.retain(|job| job.handle != id);
        state.maybe.retain(|job| job.handle != id);
    }

    /// Move this handle's still-pending `Maybe` jobs to the `Likely`
    /// rank: the decision point ahead of them has committed.
    pub fn promote(&self) {
        let mut state = self.lock();
        let id = self.id;
        let mut promoted = VecDeque::new();
        state.maybe.retain(|job| {
            if job.handle == id {
                promoted.push_back(job.clone());
                false
            } else {
                true
            }
        });
        state.likely.extend(promoted);
    }
}

/// A stream that also drives a [`FetchPlan`]: see [`FetchPlan::drive`].
pub(crate) struct Driven<'a, S, Env> {
    inner: S,
    plan: FetchPlan,
    sources: Vec<Source>,
    env: &'a Env,
    hydration: Arc<HydrationFlight<'a>>,
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
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// Start pending jobs up to the budget. Returns whether any started.
    fn start_jobs(&mut self) -> bool {
        let mut started = false;
        loop {
            let likely_spare = self.likely_inflight < self.budget.likely;
            let maybe_spare = self.maybe_inflight < self.budget.maybe;
            if !likely_spare && !maybe_spare {
                return started;
            }
            let Some((job, likelihood)) = self.plan.next(likely_spare, maybe_spare) else {
                return started;
            };
            match likelihood {
                Likelihood::Likely => self.likely_inflight += 1,
                Likelihood::Maybe => self.maybe_inflight += 1,
            }
            let sources = self.sources.clone();
            let env = self.env;
            let hydration = self.hydration.clone();
            let future = async move {
                for source in sources {
                    let mut scan = select_from_source(
                        source,
                        env,
                        job.selector.clone(),
                        Some(hydration.clone()),
                    );
                    // Drain: rows are discarded, blocks land in the
                    // caches. An error ends this source's scan silently.
                    while let Some(row) = scan.next().await {
                        if row.is_err() {
                            break;
                        }
                    }
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
        + Provider<Fork<RemoteSite, Get>>
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_operator::helpers::{test_operator_with_profile, unique_name};
    use dialog_query::{AttributeQuery, Term, the};

    use super::*;
    use crate::RepositoryExt as _;
    use crate::helpers::Counting;
    use dialog_query::query::Output as _;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn selector(attribute: &str) -> ArtifactSelector<Constrained> {
        ArtifactSelector::new().the(attribute.parse().expect("a valid attribute"))
    }

    /// `Likely` work drains before `Maybe` work regardless of enqueue
    /// order, and a rank without spare capacity is skipped.
    #[dialog_common::test]
    fn it_drains_likely_before_maybe_within_capacity() {
        let plan = FetchPlan::new();
        plan.preload(selector("a/b"), Likelihood::Maybe);
        plan.preload(selector("c/d"), Likelihood::Likely);

        let (_, rank) = plan.next(true, true).expect("two jobs pending");
        assert_eq!(rank, Likelihood::Likely, "likely rank drains first");
        assert!(plan.next(false, false).is_none(), "no capacity, no dequeue");
        let (_, rank) = plan.next(true, true).expect("one job pending");
        assert_eq!(rank, Likelihood::Maybe);
        assert!(plan.next(true, true).is_none());
    }

    /// A handle's abort drops only its own pending jobs; promote moves
    /// them to the likely rank without touching other handles' work.
    #[dialog_common::test]
    fn it_aborts_and_promotes_by_handle() {
        let plan = FetchPlan::new();
        let doomed = plan.preload(selector("a/b"), Likelihood::Maybe);
        let kept = plan.preload(selector("c/d"), Likelihood::Maybe);

        doomed.abort();
        assert_eq!(plan.pending(), 1, "only the aborted handle's job left");

        kept.promote();
        let (_, rank) = plan.next(true, false).expect("promoted job pending");
        assert_eq!(
            rank,
            Likelihood::Likely,
            "a promoted job dequeues at the likely rank"
        );
        assert_eq!(plan.pending(), 0);
    }

    /// A zero budget never blocks the demand query: the plan's jobs
    /// simply stay pending. The driver is an accelerator, not a
    /// dependency.
    #[dialog_common::test]
    async fn it_never_blocks_demand_on_an_undriven_plan() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("preload-starved"))
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

        let plan = FetchPlan::new().with_budget(FetchBudget {
            likely: 0,
            maybe: 0,
        });
        plan.preload(selector("right/name"), Likelihood::Likely);

        let rows = branch
            .query()
            .select(AttributeQuery::new(
                Term::from(the!("left/name")),
                Term::blank(),
                Term::blank(),
                Term::blank(),
                None,
            ))
            .preload(plan.clone())
            .perform(&env)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "demand rows flow with a starved plan");
        assert_eq!(plan.pending(), 1, "the starved plan holds its job");
        Ok(())
    }

    /// A query staged with a plan replicates the plan's ranges while it
    /// runs: after the driven query drains, the preloaded attribute's
    /// blocks are already local (its own select then reads nothing from
    /// the backend), and the plan is empty.
    #[dialog_common::test]
    async fn it_replicates_preloaded_ranges_while_the_query_runs() -> Result<()> {
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

        let plan = FetchPlan::new();
        plan.preload(selector("right/name"), Likelihood::Likely);

        let left = AttributeQuery::new(
            Term::from(the!("left/name")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );
        let rows = branch
            .query()
            .select(left)
            .preload(plan.clone())
            .perform(&env)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 40, "the demand query yields its rows");
        assert_eq!(plan.pending(), 0, "the driven stream executed the plan");

        // The preloaded range is now local: reading it touches the
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
            "a preloaded range reads nothing from the backend"
        );
        Ok(())
    }
}
