use dialog_capability::Command;
use futures_util::Stream;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::selector::Constrained;
use crate::{ArtifactSelector, ArtifactView, DialogArtifactsError};

/// A boxed stream of artifact query results, as borrowed-access
/// [`ArtifactView`]s: read fields off each row, or call
/// [`ArtifactView::to_owned`] where ownership is genuinely needed.
#[cfg(not(target_arch = "wasm32"))]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ArtifactView, DialogArtifactsError>> + Send + 'a>>;

/// A boxed stream of artifact query results, as borrowed-access
/// [`ArtifactView`]s: read fields off each row, or call
/// [`ArtifactView::to_owned`] where ownership is genuinely needed.
#[cfg(target_arch = "wasm32")]
pub type ArtifactStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ArtifactView, DialogArtifactsError>> + 'a>>;

/// Command for selecting artifacts from a source.
///
/// The lifetime parameter `'a` ties the output stream to the provider,
/// allowing the stream to borrow from the environment.
pub struct Select<'a> {
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Command for Select<'a> {
    type Input = ArtifactSelector<Constrained>;
    type Output = Result<ArtifactStream<'a>, DialogArtifactsError>;
}

/// How confident the requester is that a preloaded range will be read.
///
/// The distinction is scheduling, not semantics: `Likely` work (a range
/// the evaluation has committed to, a spine) is served before `Maybe`
/// work (leaf frontiers, ranges a decision point may abandon).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Likelihood {
    /// The evaluation will read this range unless it fails first.
    Likely,
    /// The evaluation may read this range; a decision point ahead may
    /// abandon it.
    Maybe,
}

/// A hint that a selector's blocks should replicate ahead of demand.
#[derive(Debug, Clone)]
pub struct PreloadRequest {
    /// The selector whose backing blocks are wanted.
    pub selector: ArtifactSelector<Constrained>,
    /// How the request ranks against other speculative work.
    pub likelihood: Likelihood,
}

/// Command hinting that a selector's backing blocks will probably be
/// needed, so replication may fetch them ahead of demand.
///
/// Purely advisory: a provider may do nothing, and no fetch outcome is
/// ever reported, because a preload that fails must surface as nothing
/// — the demand read that actually needs the data owns the error. The
/// output says only whether anyone is listening (the env's
/// [`PreloadQueue`] has a non-zero budget), so an evaluator can stop
/// composing hints nobody will act on.
pub struct Preload;

impl Command for Preload {
    type Input = PreloadRequest;
    type Output = bool;
}

/// Command acquiring the env's ambient speculative-fetch state: the
/// [`PreloadQueue`] that [`Preload`] hints land in.
///
/// A driven evaluation stream performs this once at construction and
/// then pops jobs synchronously while it polls, borrowing the env for
/// its own poll — the queue holds descriptions only (selectors and
/// ranks, no futures, no env), so handing out the shared handle moves
/// no work and owns nothing.
pub struct Speculation;

impl Command for Speculation {
    type Input = ();
    type Output = Arc<PreloadQueue>;
}

/// How many speculative fetch jobs may run concurrently, per rank.
///
/// A budget is per driver, not global: each driven evaluation stream
/// counts its own in-flight jobs against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchBudget {
    /// Concurrent jobs for [`Likelihood::Likely`] work.
    pub likely: usize,
    /// Concurrent jobs for [`Likelihood::Maybe`] work.
    pub maybe: usize,
}

impl FetchBudget {
    /// Speculation off: hints are refused and nothing is driven. The
    /// deterministic soak profile pins the engine's exact demand shape
    /// with this.
    pub const ZERO: Self = Self {
        likely: 0,
        maybe: 0,
    };
}

impl Default for FetchBudget {
    /// Sized from the soak's cold-join budget sweep (see
    /// `notes/fetch-scheduler.md`): rounds shrink with budget up to a
    /// knee near 256 once hydration is single-flighted; `Maybe` work
    /// stays narrow.
    fn default() -> Self {
        Self {
            likely: 256,
            maybe: 16,
        }
    }
}

/// Pending speculative preloads a rank may hold; the oldest hint is
/// dropped past this. Hints age fast — they describe what a running
/// evaluation is about to read — so keeping the freshest is the point.
const PENDING_LIMIT: usize = 1024;

#[derive(Debug)]
struct QueueState {
    budget: FetchBudget,
    likely: VecDeque<ArtifactSelector<Constrained>>,
    maybe: VecDeque<ArtifactSelector<Constrained>>,
}

/// The env's ambient queue of speculative preloads: pure data (ranked
/// selectors and the budget they are driven under), shared by every
/// evaluation performing through one env.
///
/// [`Preload`] hints enqueue here from any path — queries,
/// subscriptions, transaction queries — and any driven evaluation
/// stream pops and executes them while it polls, so cross-query warming
/// needs no per-query wiring. The queue owns no futures and no env:
/// work materializes only inside a driver borrowing the env, and a
/// queue nobody drives holds descriptions, not resources.
#[derive(Debug)]
pub struct PreloadQueue {
    state: parking_lot::Mutex<QueueState>,
}

impl Default for PreloadQueue {
    fn default() -> Self {
        Self::new(FetchBudget::default())
    }
}

impl PreloadQueue {
    /// An empty queue driven under `budget`.
    pub fn new(budget: FetchBudget) -> Self {
        Self {
            state: parking_lot::Mutex::new(QueueState {
                budget,
                likely: VecDeque::new(),
                maybe: VecDeque::new(),
            }),
        }
    }

    /// Whether hints are acted on: the budget admits at least one job.
    pub fn listening(&self) -> bool {
        let state = self.state.lock();
        state.budget.likely > 0 || state.budget.maybe > 0
    }

    /// The budget drivers count their in-flight jobs against.
    pub fn budget(&self) -> FetchBudget {
        self.state.lock().budget
    }

    /// Change the budget. [`FetchBudget::ZERO`] turns speculation off:
    /// later hints are refused, and drivers start nothing new.
    /// Already-running jobs complete (started fetches are paid for).
    pub fn set_budget(&self, budget: FetchBudget) {
        self.state.lock().budget = budget;
    }

    /// Enqueue a hint, returning whether anyone is listening. With a
    /// zero budget nothing enqueues; past [`PENDING_LIMIT`] the rank's
    /// oldest pending hint is dropped.
    pub fn preload(&self, request: PreloadRequest) -> bool {
        let mut state = self.state.lock();
        if state.budget.likely == 0 && state.budget.maybe == 0 {
            return false;
        }
        let rank = match request.likelihood {
            Likelihood::Likely => &mut state.likely,
            Likelihood::Maybe => &mut state.maybe,
        };
        if rank.len() >= PENDING_LIMIT {
            rank.pop_front();
        }
        rank.push_back(request.selector);
        true
    }

    /// Pending hints, for tests and introspection.
    pub fn pending(&self) -> usize {
        let state = self.state.lock();
        state.likely.len() + state.maybe.len()
    }

    /// Dequeue the next hint whose rank has spare capacity: `Likely`
    /// drains before `Maybe`.
    pub fn next(
        &self,
        likely_spare: bool,
        maybe_spare: bool,
    ) -> Option<(ArtifactSelector<Constrained>, Likelihood)> {
        let mut state = self.state.lock();
        if likely_spare && let Some(selector) = state.likely.pop_front() {
            return Some((selector, Likelihood::Likely));
        }
        if maybe_spare && let Some(selector) = state.maybe.pop_front() {
            return Some((selector, Likelihood::Maybe));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn selector(attribute: &str) -> ArtifactSelector<Constrained> {
        ArtifactSelector::new().the(attribute.parse().expect("a valid attribute"))
    }

    fn hint(attribute: &str, likelihood: Likelihood) -> PreloadRequest {
        PreloadRequest {
            selector: selector(attribute),
            likelihood,
        }
    }

    /// `Likely` work drains before `Maybe` work regardless of enqueue
    /// order, and a rank without spare capacity is skipped.
    #[dialog_common::test]
    fn it_drains_likely_before_maybe_within_capacity() {
        let queue = PreloadQueue::default();
        assert!(queue.preload(hint("a/b", Likelihood::Maybe)));
        assert!(queue.preload(hint("c/d", Likelihood::Likely)));

        let (_, rank) = queue.next(true, true).expect("two hints pending");
        assert_eq!(rank, Likelihood::Likely, "likely rank drains first");
        assert!(
            queue.next(false, false).is_none(),
            "no capacity, no dequeue"
        );
        let (_, rank) = queue.next(true, true).expect("one hint pending");
        assert_eq!(rank, Likelihood::Maybe);
        assert!(queue.next(true, true).is_none());
    }

    /// A zero budget refuses hints outright — nothing enqueues, so an
    /// evaluator that sees `false` can stop composing them and a
    /// deterministic run stays deterministic.
    #[dialog_common::test]
    fn it_refuses_hints_with_a_zero_budget() {
        let queue = PreloadQueue::new(FetchBudget::ZERO);
        assert!(!queue.listening());
        assert!(!queue.preload(hint("a/b", Likelihood::Likely)));
        assert_eq!(queue.pending(), 0, "a refused hint enqueues nothing");

        queue.set_budget(FetchBudget::default());
        assert!(queue.listening());
        assert!(queue.preload(hint("a/b", Likelihood::Likely)));
        assert_eq!(queue.pending(), 1);
    }

    /// Past the pending limit the rank's oldest hint drops: hints
    /// describe what a running evaluation is about to read, so the
    /// freshest are the ones worth keeping.
    #[dialog_common::test]
    fn it_drops_the_oldest_hint_past_the_pending_limit() {
        let queue = PreloadQueue::default();
        assert!(queue.preload(hint("first/attr", Likelihood::Likely)));
        for _ in 0..PENDING_LIMIT {
            assert!(queue.preload(hint("later/attr", Likelihood::Likely)));
        }
        assert_eq!(queue.pending(), PENDING_LIMIT, "the rank stays bounded");
        let (front, _) = queue.next(true, true).expect("hints pending");
        assert_eq!(
            format!("{front:?}"),
            format!("{:?}", selector("later/attr")),
            "the oldest hint was the one dropped"
        );
    }
}
