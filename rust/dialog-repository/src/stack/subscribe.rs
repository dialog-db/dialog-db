//! Subscriptions over a stack: one evaluation of a query, shared by
//! every reader of it, polled when a write scheduled it and delivered to
//! each reader.
//!
//! [`Stack::subscribe`] registers the stack's subscription for a query,
//! or attaches to the one it holds: a subscription is keyed by the
//! query's identity ([`QueryHash`], the hash of its canonical encoding)
//! and its type, so a second reader of the same query reads the same
//! evaluation rather than running its own. The evaluation is dialog's
//! demand-gated [`Subscription`], the one [`Stack::subscription`] hands
//! out unshared: this module is a registry and a delivery layer over
//! it, not a second evaluator.
//!
//! A write that can move what a query reads **schedules a poll** of
//! the stack: a dispatch through it that published, or that wrote into
//! a memory layer ([`SESSION`](crate::SESSION), a session layer), a
//! sync that moved the primary, a channel the stack created or a
//! provider it registered, which write into its own layer. A write into
//! a channel does not: its rows live one tick inside a dispatch, and
//! what lands on its outside end is read with
//! [`Arrivals`](crate::Arrivals). A write outside the stack, into the
//! primary's session overlay ([`Branch::overlay`](crate::Branch::overlay))
//! or straight onto a layer, is the embedder's to schedule, with
//! [`Stack::schedule_poll`]. Scheduling costs nothing but a flag, so the
//! writes of one request coalesce: [`Stack::settle`] polls every
//! subscription of the stack once if a poll is scheduled, delivers what
//! moved to each reader, and clears the mark, and a second `settle`
//! with nothing scheduled does nothing. [`Effect::perform`](crate::Effect::perform)
//! settles after its answer dispatch, so an embedder that drives effects
//! is delivered to without calling `settle` itself.
//!
//! A reader ([`Subscriber`]) is handed **deliveries**, each a [`Delta`]
//! of the conclusions the poll asserted and retracted. Its first is the
//! whole result set, as `asserted`: the subscription's first
//! evaluation, for a reader that attached before it, or the set as the
//! last poll left it, for one that attached after. Each later one is
//! what a poll changed, and a poll that changed nothing delivers
//! nothing. [`Subscriber::results`] reads the whole current set, for an
//! embedder that sends that rather than the change.
//!
//! The stack holds its subscriptions weakly: a subscription lives as
//! long as a reader of it does, and goes, with its evaluation, when its
//! last reader is dropped or closed ([`Subscriber::close`]). A reader
//! that stops reading without being dropped keeps its subscription
//! evaluating; one that is dropped costs nothing more.
//!
//! ```no_run
//! # use dialog_corpus::Entity;
//! # use dialog_query::attribute::Relation;
//! # use dialog_query::{AttributeQuery, Term};
//! # use dialog_repository::{Stack, StackEnv};
//! # async fn example(stack: Stack, env: impl StackEnv + Clone + dialog_common::ConditionalSend) -> anyhow::Result<()> {
//! let selected = || {
//!     AttributeQuery::from(
//!         Term::<Relation>::from(Relation::from(
//!             "ui/selected".parse::<dialog_corpus::Relation>().expect("an attribute"),
//!         ))
//!         .of(Term::<Entity>::var("doc"))
//!         .is(Term::<bool>::var("on")),
//!     )
//! };
//! let mut first = stack.subscribe(selected());
//! let mut second = stack.subscribe(selected());
//! assert_eq!(first.hash(), second.hash());
//!
//! stack
//!     .assert(dialog_query::the!("ui/selected").of("doc:1".parse::<Entity>()?).is(true))
//!     .perform(&env)
//!     .await?;
//! // One evaluation, delivered to both readers.
//! stack.settle(&env).await?;
//! for delta in first.poll() {
//!     println!("asserted {:?}, retracted {:?}", delta.asserted, delta.retracted);
//! }
//! let whole = second.results();
//! # let _ = whole;
//! # Ok(())
//! # }
//! ```

use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_query::query::Application;
use dialog_query::{Conclusion, EvaluationError};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::Notify;

use crate::{Delta, Frame, OpenStackEnv, PollError, Stack, Subscription};

pub use dialog_query::QueryHash;

/// A subscription of the stack's as the registry holds it, its query's
/// type erased.
#[cfg(not(target_arch = "wasm32"))]
type Erased = dyn Any + Send + Sync;
/// A subscription of the stack's as the registry holds it, its query's
/// type erased.
#[cfg(target_arch = "wasm32")]
type Erased = dyn Any;

/// The environment a [`Stack::settle`] polls with, its type erased so
/// the registry can poll a subscription of any query type with it: a
/// shared handle, `Sized` and `'static` as the evaluation asks, that
/// provides what the environment provides, through the `Arc` blanket
/// impls of `Provider` and `Holds`. On native it is also `Send`:
/// `OpenStackEnv` asks only `Sync`, and an `Arc` is `Send` and `Sync`
/// only over what is both, which is why [`Stack::settle`] asks its
/// environment to be `ConditionalSend`.
#[cfg(not(target_arch = "wasm32"))]
type Environment = Arc<dyn OpenStackEnv + Send>;
/// The environment a [`Stack::settle`] polls with, its type erased.
#[cfg(target_arch = "wasm32")]
type Environment = Arc<dyn OpenStackEnv>;

/// The future one subscription's poll returns.
#[cfg(not(target_arch = "wasm32"))]
type Polling<'a> = Pin<Box<dyn Future<Output = Result<Polled, EvaluationError>> + Send + 'a>>;
/// The future one subscription's poll returns.
#[cfg(target_arch = "wasm32")]
type Polling<'a> = Pin<Box<dyn Future<Output = Result<Polled, EvaluationError>> + 'a>>;

/// The subscriptions a stack holds, and whether a poll of them is
/// scheduled.
#[derive(Default)]
pub(crate) struct Subscriptions {
    /// Set by a write that can move what a subscription reads, cleared
    /// by [`Stack::settle`]. Shared with every transaction through the
    /// stack, whose publish sets it.
    scheduled: Arc<AtomicBool>,
    /// Every subscription the stack holds, in the order they were
    /// registered, each held weakly: its readers keep it.
    entries: Mutex<Vec<Entry>>,
    /// Counts the subscriptions registered unshared, for a query with no
    /// encoding to hash, so each has an identity of its own.
    unshared: Mutex<u64>,
}

struct Entry {
    hash: QueryHash,
    kind: TypeId,
    /// The subscription, as `settle` polls it.
    subscription: Weak<dyn Poll>,
    /// The same subscription as its own type, a `Weak<Shared<Q>>`, to
    /// attach a reader of `Q` to it.
    typed: Box<Erased>,
}

impl Subscriptions {
    /// The flag a transaction through the stack sets when its publish
    /// schedules a poll.
    pub(crate) fn scheduled(&self) -> Arc<AtomicBool> {
        self.scheduled.clone()
    }

    /// The subscriptions some reader still holds, in the order they were
    /// registered. The ones none does are dropped from the registry.
    fn live(&self) -> Vec<Arc<dyn Poll>> {
        let mut entries = self.entries.lock();
        let mut live = Vec::with_capacity(entries.len());
        entries.retain(|entry| match entry.subscription.upgrade() {
            Some(subscription) => {
                live.push(subscription);
                true
            }
            None => false,
        });
        live
    }
}

/// What polling one subscription did.
enum Polled {
    /// It delivered a change, or its first evaluation, to its readers.
    Delivered,
    /// Nothing it reads moved, or it moved to the set it was.
    Unchanged,
    /// Another `settle` was polling it.
    Busy,
}

/// One subscription, polled by [`Stack::settle`] with its query's type
/// erased.
trait Poll: ConditionalSend + ConditionalSync + 'static {
    /// Poll the evaluation at `frame` and deliver what moved.
    fn poll<'a>(&'a self, frame: &'a Frame, env: &'a Environment) -> Polling<'a>;
}

/// One subscription: the evaluation, and what its readers have yet to
/// read.
struct Shared<Q: Application> {
    hash: QueryHash,
    /// The evaluation, taken out while a poll runs it, so no lock is held
    /// across the poll's awaits.
    engine: Mutex<Option<Subscription<Q>>>,
    state: Mutex<State<Q::Conclusion>>,
    /// Woken each time a poll delivers: what [`Subscriber::next`] waits
    /// on.
    delivered: Notify,
}

struct State<C> {
    /// Whether the subscription has been evaluated: until it has, a
    /// reader that attaches is handed nothing, and the first evaluation
    /// is its first delivery.
    evaluated: bool,
    /// The result set as the last poll left it.
    results: Vec<C>,
    /// The evaluation's counts as the last poll left them.
    recomputes: usize,
    maintenances: usize,
    /// What each reader has not read yet, by the reader's number,
    /// oldest first.
    readers: Vec<(u64, VecDeque<Delta<C>>)>,
    /// The number the next reader takes.
    next: u64,
}

impl<Q> Shared<Q>
where
    Q: Application + ConditionalSync,
    Q::Conclusion: Clone + ConditionalSync,
{
    /// Attach a reader, handing it the result set when the subscription
    /// has been evaluated: its first delivery.
    fn attach(&self) -> u64 {
        let mut state = self.state.lock();
        let reader = state.next;
        state.next += 1;
        let mut queue = VecDeque::new();
        if state.evaluated {
            queue.push_back(Delta {
                asserted: state.results.clone(),
                retracted: Vec::new(),
            });
        }
        state.readers.push((reader, queue));
        reader
    }
}

impl<Q> Poll for Shared<Q>
where
    Q: Application + Clone + ConditionalSync,
    Q::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
{
    fn poll<'a>(&'a self, frame: &'a Frame, env: &'a Environment) -> Polling<'a> {
        Box::pin(async move {
            let Some(mut engine) = self.engine.lock().take() else {
                return Ok(Polled::Busy);
            };
            // A frame older than the one the evaluation last polled at is
            // one another `settle` took after this one's and polled it at
            // first: what it reads is already of a later state, which that
            // `settle` delivered.
            let polled = match engine.poll_at(frame, env).await {
                Err(PollError::Stale) => Ok(None),
                Err(PollError::Evaluation(error)) => Err(error),
                Err(PollError::Foreign) => Err(EvaluationError::Store(
                    "a stack's subscription was polled at a frame of another stack".into(),
                )),
                Ok(delta) => Ok(delta),
            };
            let results = match &polled {
                Ok(Some(_)) => Some(engine.results().to_vec()),
                _ => None,
            };
            let (recomputes, maintenances) = (engine.recomputes(), engine.maintenances());
            *self.engine.lock() = Some(engine);
            let delta = polled?;
            let mut state = self.state.lock();
            state.recomputes = recomputes;
            state.maintenances = maintenances;
            let (Some(delta), Some(results)) = (delta, results) else {
                return Ok(Polled::Unchanged);
            };
            let first = !state.evaluated;
            state.evaluated = true;
            state.results = results;
            // The first evaluation is delivered even when it found
            // nothing: it is each reader's whole result set.
            if delta.is_empty() && !first {
                return Ok(Polled::Unchanged);
            }
            for (_, queue) in &mut state.readers {
                queue.push_back(delta.clone());
            }
            drop(state);
            self.delivered.notify_waiters();
            Ok(Polled::Delivered)
        })
    }
}

/// A reader of one of a stack's subscriptions: what each poll of the
/// query delivered, read from its own queue. Built by
/// [`Stack::subscribe`]; read with [`poll`](Self::poll), or
/// [`next`](Self::next) to wait for a delivery. See the
/// [module docs](self).
///
/// Every reader of one query reads the same evaluation, and each reads
/// every delivery once: a reader that was not reading loses nothing, its
/// next read yields every delivery it missed, oldest first. Dropping the
/// reader, or [`close`](Self::close), releases it; the subscription goes
/// with its last reader.
pub struct Subscriber<Q: Application> {
    subscription: Arc<Shared<Q>>,
    reader: u64,
}

impl Stack {
    /// The stack's subscription to `query`, registered now, or the one it
    /// holds when some reader already reads the same query: a reader of
    /// it, whose deliveries [`settle`](Self::settle) polls (see
    /// [`Subscriber`]). Registering a subscription schedules a poll, its
    /// first evaluation; a reader attaching to one already evaluated is
    /// handed the result set at once, as its first delivery.
    ///
    /// The subscription is keyed by the query's identity
    /// ([`QueryHash`]) and its type. A query with no encoding to hash is
    /// registered apart, unshared.
    pub fn subscribe<Q>(&self, query: Q) -> Subscriber<Q>
    where
        Q: Application + Serialize + Clone + ConditionalSync,
        Q::Conclusion: Conclusion + PartialEq + Clone + ConditionalSync,
    {
        let subscriptions = self.subscriptions();
        let kind = TypeId::of::<Q>();
        let hash = QueryHash::of(&query);
        let mut entries = subscriptions.entries.lock();
        if let Some(hash) = &hash {
            let held = entries
                .iter()
                .filter(|entry| entry.kind == kind && entry.hash == *hash)
                .filter_map(|entry| entry.typed.downcast_ref::<Weak<Shared<Q>>>())
                .find_map(Weak::upgrade);
            if let Some(subscription) = held {
                let reader = subscription.attach();
                return Subscriber {
                    subscription,
                    reader,
                };
            }
        }
        let hash = hash.unwrap_or_else(|| {
            let mut unshared = subscriptions.unshared.lock();
            *unshared += 1;
            QueryHash::from(Blake3Hash::hash_iter(
                [b"unshared\0".as_slice(), &unshared.to_be_bytes()].into_iter(),
            ))
        });
        let subscription = Arc::new(Shared {
            hash: hash.clone(),
            engine: Mutex::new(Some(self.subscription(query))),
            state: Mutex::new(State {
                evaluated: false,
                results: Vec::new(),
                recomputes: 0,
                maintenances: 0,
                readers: Vec::new(),
                next: 0,
            }),
            delivered: Notify::new(),
        });
        let reader = subscription.attach();
        let polled: Arc<dyn Poll> = subscription.clone();
        entries.retain(|entry| entry.subscription.strong_count() > 0);
        entries.push(Entry {
            hash,
            kind,
            subscription: Arc::downgrade(&polled),
            typed: Box::new(Arc::downgrade(&subscription)),
        });
        drop(entries);
        self.schedule_poll();
        Subscriber {
            subscription,
            reader,
        }
    }

    /// Schedule a poll of the stack's subscriptions, which the next
    /// [`settle`](Self::settle) runs. A dispatch through the stack that
    /// published or wrote into a memory layer schedules one itself; a
    /// write the stack does not see, into the primary's session overlay
    /// ([`Branch::overlay`](crate::Branch::overlay)) among them, is
    /// scheduled with this. Costs a flag: the polls a request schedules
    /// coalesce into one.
    pub fn schedule_poll(&self) {
        self.subscriptions()
            .scheduled
            .store(true, Ordering::Release);
    }

    /// Whether a poll of the stack's subscriptions is scheduled (see
    /// [`schedule_poll`](Self::schedule_poll)).
    pub fn poll_scheduled(&self) -> bool {
        self.subscriptions().scheduled.load(Ordering::Acquire)
    }

    /// Poll every subscription of the stack once if a poll is
    /// scheduled, deliver what moved to each reader, and clear the mark;
    /// with none scheduled, do nothing. Returns how many subscriptions
    /// delivered.
    ///
    /// Every subscription is polled at one [`Frame`] of the stack, taken
    /// as the round begins, so what they deliver is of one state and
    /// they agree: a write that lands while they are polled is in none
    /// of them, and schedules the next round.
    ///
    /// A subscription whose poll fails is left scheduled, so the next
    /// `settle` polls it again, and the first failure is returned once
    /// every other subscription has been polled. One another `settle` is
    /// polling at the same time is left to it, and scheduled again, so a
    /// write that landed after that poll began is not missed.
    ///
    /// The environment is `Clone`: `settle` clones it once into a shared
    /// handle whose type it erases, so one poll serves subscriptions of
    /// every query type. Dialog's environments are cheap handles by
    /// design, so the clone costs a reference count.
    pub async fn settle<Env>(&self, env: &Env) -> Result<usize, EvaluationError>
    where
        Env: OpenStackEnv + Clone + ConditionalSend + 'static,
    {
        let subscriptions = self.subscriptions();
        if !subscriptions.scheduled.swap(false, Ordering::AcqRel) {
            return Ok(0);
        }
        let live = subscriptions.live();
        if live.is_empty() {
            return Ok(0);
        }
        let env: Environment = Arc::new(env.clone());
        let frame = self.frame();
        let mut delivered = 0;
        let mut failed = None;
        for subscription in live {
            match subscription.poll(&frame, &env).await {
                Ok(Polled::Delivered) => delivered += 1,
                Ok(Polled::Unchanged) => {}
                Ok(Polled::Busy) => self.schedule_poll(),
                Err(error) => {
                    self.schedule_poll();
                    failed.get_or_insert(error);
                }
            }
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(delivered),
        }
    }

    /// How many subscriptions the stack holds that some reader still
    /// reads.
    pub fn subscriptions_held(&self) -> usize {
        self.subscriptions().live().len()
    }
}

impl<Q> Subscriber<Q>
where
    Q: Application + ConditionalSync,
    Q::Conclusion: Clone + ConditionalSync,
{
    /// The subscription's identity: the hash of its query. Every reader
    /// of one subscription has the same.
    pub fn hash(&self) -> &QueryHash {
        &self.subscription.hash
    }

    /// What was delivered to this reader since its last read, oldest
    /// first, without waiting; empty when nothing was.
    pub fn poll(&mut self) -> Vec<Delta<Q::Conclusion>> {
        let mut state = self.subscription.state.lock();
        state
            .readers
            .iter_mut()
            .find(|(reader, _)| *reader == self.reader)
            .map(|(_, queue)| queue.drain(..).collect())
            .unwrap_or_default()
    }

    /// The next deliveries: what [`poll`](Self::poll) yields, waiting
    /// for the next [`Stack::settle`] that delivers while it yields
    /// nothing.
    pub async fn next(&mut self) -> Vec<Delta<Q::Conclusion>> {
        let subscription = self.subscription.clone();
        loop {
            // Listening before reading, so a delivery between the read
            // and the wait is not slept through.
            let delivered = subscription.delivered.notified();
            let mut delivered = pin!(delivered);
            delivered.as_mut().enable();
            let deltas = self.poll();
            if !deltas.is_empty() {
                return deltas;
            }
            delivered.await;
        }
    }

    /// The whole result set, as the last poll of the subscription left
    /// it; empty before its first evaluation.
    pub fn results(&self) -> Vec<Q::Conclusion> {
        self.subscription.state.lock().results.clone()
    }

    /// Whether the subscription has been evaluated.
    pub fn evaluated(&self) -> bool {
        self.subscription.state.lock().evaluated
    }

    /// The full evaluations the subscription has run, as of its last
    /// poll (see [`Subscription::recomputes`]): shared by every reader.
    pub fn recomputes(&self) -> usize {
        self.subscription.state.lock().recomputes
    }

    /// The polls of the subscription maintained incrementally, as of its
    /// last poll (see [`Subscription::maintenances`]).
    pub fn maintenances(&self) -> usize {
        self.subscription.state.lock().maintenances
    }

    /// Stop reading: the reader's undelivered deltas are dropped, and
    /// the subscription goes once its last reader has. The same as
    /// dropping the reader.
    pub fn close(self) {}
}

impl<Q: Application> Drop for Subscriber<Q> {
    fn drop(&mut self) {
        let reader = self.reader;
        self.subscription
            .state
            .lock()
            .readers
            .retain(|(held, _)| *held != reader);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::super::tests::{
        Operator, declare_fetch, declare_state, fetch, fetch_concept, flag, node_of, primary_flag,
        selected, selected_query,
    };
    use crate::helpers::test_repo;
    use crate::{Branch, Delta, Stack};
    use anyhow::Result;
    use dialog_corpus::{Changes, Entity, Statement as _};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::{AttributeQuery, Claim, Term};
    use futures_util::future::join;

    /// A stack on a fresh branch whose schema declares the memory layer
    /// `session:state`, with that layer's name.
    async fn stack_with_state() -> Result<(Operator, Branch, Stack, Entity)> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        Ok((operator, branch, stack, state))
    }

    /// Each delta's asserted and retracted rows, as `selected` spells
    /// them.
    fn spelled(deltas: &[Delta<Claim>]) -> Vec<(Vec<String>, Vec<String>)> {
        deltas
            .iter()
            .map(|delta| (selected(&delta.asserted), selected(&delta.retracted)))
            .collect()
    }

    /// One delta asserting `rows` and retracting nothing.
    fn asserting(rows: &[String]) -> Vec<(Vec<String>, Vec<String>)> {
        vec![(rows.to_vec(), Vec::new())]
    }

    /// Two readers of one query read one subscription: the stack holds
    /// one, it is evaluated once for both, and one dispatch delivers the
    /// same delta to each, maintained once.
    #[dialog_common::test]
    async fn it_shares_one_evaluation_among_the_readers_of_a_query() -> Result<()> {
        let (operator, _branch, stack, state) = stack_with_state().await?;
        let doc: Entity = "doc:1".parse()?;
        let mut first = stack.subscribe(selected_query());
        let mut second = stack.subscribe(selected_query());
        assert_eq!(stack.subscriptions_held(), 1, "one subscription");
        assert_eq!(first.hash(), second.hash());
        assert!(
            stack.poll_scheduled(),
            "registering scheduled its first poll"
        );

        assert_eq!(stack.settle(&operator).await?, 1);
        assert_eq!(spelled(&first.poll()), asserting(&[]), "the initial set");
        assert_eq!(spelled(&second.poll()), asserting(&[]), "the initial set");
        assert_eq!(first.recomputes(), 1, "evaluated once for both");

        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert!(stack.poll_scheduled(), "a write into a memory layer");
        assert_eq!(stack.settle(&operator).await?, 1);
        let expected = asserting(&[format!("{doc} Boolean(true)")]);
        assert_eq!(spelled(&first.poll()), expected);
        assert_eq!(spelled(&second.poll()), expected);
        assert_eq!(first.recomputes(), 1, "no second evaluation");
        assert_eq!(second.maintenances(), 1, "maintained once, for both");
        assert_eq!(
            selected(&second.results()),
            vec![format!("{doc} Boolean(true)")]
        );
        assert!(first.poll().is_empty(), "each delivery is read once");
        Ok(())
    }

    /// A dispatch that moves nothing a subscription reads delivers
    /// nothing: the poll it scheduled finds the result unchanged.
    #[dialog_common::test]
    async fn it_delivers_nothing_for_a_dispatch_outside_what_it_reads() -> Result<()> {
        let (operator, _branch, stack, _state) = stack_with_state().await?;
        let mut reader = stack.subscribe(selected_query());
        stack.settle(&operator).await?;
        assert_eq!(spelled(&reader.poll()), asserting(&[]));

        stack
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await?;
        assert!(stack.poll_scheduled(), "the dispatch published");
        assert_eq!(stack.settle(&operator).await?, 0, "nothing delivered");
        assert!(reader.poll().is_empty());
        Ok(())
    }

    /// A commit and a write into a memory layer in one request schedule
    /// one poll: one `settle` delivers both as one delta, and a second
    /// `settle`, with nothing scheduled, polls nothing.
    #[dialog_common::test]
    async fn it_coalesces_the_writes_of_a_request_into_one_delivery() -> Result<()> {
        let (operator, _branch, stack, state) = stack_with_state().await?;
        let mut reader = stack.subscribe(selected_query());
        stack.settle(&operator).await?;
        reader.poll();
        let (committed, held): (Entity, Entity) = ("doc:1".parse()?, "doc:2".parse()?);

        stack
            .assert(primary_flag(&committed, true))
            .perform(&operator)
            .await?;
        stack
            .assert(flag(&held, false, &state))
            .perform(&operator)
            .await?;
        assert_eq!(stack.settle(&operator).await?, 1);
        let mut both = vec![
            format!("{committed} Boolean(true)"),
            format!("{held} Boolean(false)"),
        ];
        both.sort();
        assert_eq!(spelled(&reader.poll()), asserting(&both), "one delivery");

        assert!(!stack.poll_scheduled());
        assert_eq!(stack.settle(&operator).await?, 0);
        assert!(
            reader.poll().is_empty(),
            "the second settle delivers nothing"
        );
        Ok(())
    }

    /// A reader that closes is delivered nothing more, and the
    /// subscription goes, with its evaluation, when its last reader
    /// does: the stack holds subscriptions weakly. A reader of the same
    /// query after that registers a new one.
    #[dialog_common::test]
    async fn it_drops_a_subscription_with_its_last_reader() -> Result<()> {
        let (operator, _branch, stack, state) = stack_with_state().await?;
        let doc: Entity = "doc:1".parse()?;
        let first = stack.subscribe(selected_query());
        let mut second = stack.subscribe(selected_query());
        stack.settle(&operator).await?;
        first.close();
        assert_eq!(stack.subscriptions_held(), 1, "the second reader holds it");

        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert_eq!(stack.settle(&operator).await?, 1);
        assert_eq!(
            spelled(&second.poll()),
            vec![
                (Vec::new(), Vec::new()),
                (vec![format!("{doc} Boolean(true)")], Vec::new()),
            ],
            "the initial set, then the change"
        );

        second.close();
        assert_eq!(stack.subscriptions_held(), 0, "gone with its last reader");
        stack
            .assert(flag(&doc, false, &state))
            .perform(&operator)
            .await?;
        assert_eq!(stack.settle(&operator).await?, 0, "nothing left to poll");

        let third = stack.subscribe(selected_query());
        assert!(!third.evaluated(), "a new subscription");
        assert_eq!(stack.subscriptions_held(), 1);
        Ok(())
    }

    /// An effect settles the stack after its answer dispatch: a
    /// provider's answer that changes a row a subscription reads reaches
    /// its reader without the embedder calling `settle`.
    #[dialog_common::test]
    async fn it_delivers_a_providers_answer_when_the_effect_settles() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let node: Entity = "node:1".parse()?;
        let at = state.clone();
        stack.provide(crate::Provider::new(&fetch_concept(), move |command| {
            let at = at.clone();
            async move {
                let node = node_of(&command)?;
                let mut answer = Changes::new();
                dialog_query::the!("node/fetched")
                    .of(node)
                    .is(command.entity().clone())
                    .at(at)
                    .assert(&mut answer);
                Ok(answer)
            }
        }))?;
        let mut reader = stack.subscribe(AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "node/fetched".parse::<dialog_corpus::Relation>()?,
            ))
            .of(Term::<Entity>::var("node"))
            .is(Term::<Entity>::var("command")),
        ));
        stack.settle(&operator).await?;
        assert_eq!(reader.poll().len(), 1, "the initial set");

        let report = stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        let effect = report.effect().perform(&operator).await?;
        assert!(effect.unsettled().is_none());
        assert!(!stack.poll_scheduled(), "the effect settled");
        let deltas = reader.poll();
        assert_eq!(deltas.len(), 1, "the answer, delivered");
        assert_eq!(deltas[0].asserted.len(), 1);
        assert_eq!(deltas[0].asserted[0].of, node);
        Ok(())
    }

    /// A write into the primary's session overlay is outside the stack:
    /// nothing schedules a poll for it until the embedder does, and the
    /// next `settle` after `schedule_poll` delivers the row.
    #[dialog_common::test]
    async fn it_delivers_an_overlay_write_once_a_poll_is_scheduled() -> Result<()> {
        let (operator, branch, stack, _state) = stack_with_state().await?;
        let doc: Entity = "doc:1".parse()?;
        let mut reader = stack.subscribe(selected_query());
        stack.settle(&operator).await?;
        reader.poll();

        let mut changes = Changes::new();
        primary_flag(&doc, true).assert(&mut changes);
        branch.overlay().apply(changes)?;
        assert!(!stack.poll_scheduled(), "the stack did not see the write");
        assert_eq!(stack.settle(&operator).await?, 0);
        assert!(reader.poll().is_empty());

        stack.schedule_poll();
        assert_eq!(stack.settle(&operator).await?, 1);
        assert_eq!(
            spelled(&reader.poll()),
            asserting(&[format!("{doc} Boolean(true)")])
        );
        Ok(())
    }

    /// A reader waiting for its next delivery is woken by the `settle`
    /// that delivers one, after a dispatch.
    #[dialog_common::test]
    async fn it_wakes_a_reader_waiting_for_the_next_delivery() -> Result<()> {
        let (operator, _branch, stack, state) = stack_with_state().await?;
        let doc: Entity = "doc:1".parse()?;
        let mut reader = stack.subscribe(selected_query());
        stack.settle(&operator).await?;
        reader.poll();

        let (deltas, settled) = join(reader.next(), async {
            stack
                .assert(flag(&doc, true, &state))
                .perform(&operator)
                .await?;
            stack.settle(&operator).await.map_err(anyhow::Error::from)
        })
        .await;
        assert_eq!(settled?, 1);
        assert_eq!(
            spelled(&deltas),
            asserting(&[format!("{doc} Boolean(true)")])
        );
        Ok(())
    }

    /// A reader attaching to a subscription already evaluated is handed
    /// the result set at once, as its first delivery, without a poll.
    #[dialog_common::test]
    async fn it_hands_a_late_reader_the_result_set() -> Result<()> {
        let (operator, _branch, stack, state) = stack_with_state().await?;
        let doc: Entity = "doc:1".parse()?;
        let _first = stack.subscribe(selected_query());
        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        stack.settle(&operator).await?;

        let mut late = stack.subscribe(selected_query());
        assert!(!stack.poll_scheduled(), "attaching schedules nothing");
        assert_eq!(
            spelled(&late.poll()),
            asserting(&[format!("{doc} Boolean(true)")])
        );
        Ok(())
    }
}
