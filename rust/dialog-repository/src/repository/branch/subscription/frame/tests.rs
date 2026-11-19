//! A poll reads one frame: tests that interleave a write between two
//! reads of one poll, or between the polls of two subscriptions.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use dialog_capability::{Command, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync, Held, Holds};
use dialog_corpus::Entity;
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::{Output as _, Query};
use dialog_search_tree::nodes;

use super::super::Delta;
use super::super::tests::concepts::{Badge, Manager, Name, Report};
use crate::helpers::test_repo;
use crate::{Branch, RemoteSite};

/// What an [`Interleaved`] env runs between two reads of a poll.
#[cfg(not(target_arch = "wasm32"))]
type Hook = Pin<Box<dyn Future<Output = ()> + Send>>;
/// What an [`Interleaved`] env runs between two reads of a poll.
#[cfg(target_arch = "wasm32")]
type Hook = Pin<Box<dyn Future<Output = ()>>>;

/// Which commands an [`Interleaved`] env counts: an identity, which
/// every evaluation of a poll asks for once as it begins.
trait Counted: Command {
    fn counted() -> bool {
        false
    }
}

impl Counted for Identify {
    fn counted() -> bool {
        true
    }
}
impl Counted for BlobRead {}
impl Counted for Get {}
impl Counted for Put {}
impl Counted for Resolve {}
impl Counted for ArchiveLoad {}
impl Counted for BlobLoad {}
impl Counted for nodes::Read<CorpusNode> {}
impl Counted for nodes::Preload<CorpusNode> {}
impl Counted for nodes::Write<CorpusNode> {}
impl Counted for dialog_corpus::Preload {}
impl Counted for dialog_corpus::Speculation {}
impl Counted for Fork<RemoteSite, Resolve> {}

/// An env that forwards to `inner` and, as the identity it counts as
/// `at` (from zero) is asked for, first runs `hook` to its end: a write
/// that lands between two reads of one poll, at a read the test
/// chooses.
struct Interleaved<P> {
    inner: P,
    calls: AtomicUsize,
    at: usize,
    hook: Mutex<Option<Hook>>,
}

impl<P> Interleaved<P> {
    fn new(inner: P, at: usize, hook: Hook) -> Self {
        Self {
            inner,
            calls: AtomicUsize::new(0),
            at,
            hook: Mutex::new(Some(hook)),
        }
    }

    /// Whether the hook ran.
    fn fired(&self) -> bool {
        self.hook.lock().expect("hook lock").is_none()
    }
}

impl<P: Holds> Holds for Interleaved<P> {
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

#[dialog_capability::provider]
impl<C, P> Provider<C> for Interleaved<P>
where
    C: Counted + 'static,
    C::Input: ConditionalSend,
    C::Output: ConditionalSend,
    P: Provider<C> + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        if C::counted() && self.calls.fetch_add(1, Ordering::SeqCst) == self.at {
            let hook = self.hook.lock().expect("hook lock").take();
            if let Some(hook) = hook {
                hook.await;
            }
        }
        self.inner.execute(input).await
    }
}

/// The reports the branch answers now.
async fn reports<Env>(branch: &Branch, env: &Env) -> anyhow::Result<Vec<Report>>
where
    Env: crate::OpenStackEnv,
{
    Ok(branch
        .query()
        .select(Query::<Report>::default())
        .perform(env)
        .try_vec()
        .await?)
}

/// What changed from `before` to `after`, as a poll reports it.
fn difference<T: Clone + PartialEq>(before: &[T], after: &[T]) -> Delta<T> {
    Delta {
        asserted: after
            .iter()
            .filter(|row| !before.contains(row))
            .cloned()
            .collect(),
        retracted: before
            .iter()
            .filter(|row| !after.contains(row))
            .cloned()
            .collect(),
    }
}

/// Whether two deltas hold the same rows, in any order.
fn same<T: PartialEq>(left: &Delta<T>, right: &Delta<T>) -> bool {
    let equal = |left: &[T], right: &[T]| {
        left.len() == right.len() && left.iter().all(|row| right.contains(row))
    };
    equal(&left.asserted, &right.asserted) && equal(&left.retracted, &right.retracted)
}

/// A commit that lands between two reads of one poll is not in what the
/// poll reports: the delta is the difference of the query's answers at
/// the frame the poll began at and the one before, and the next poll
/// reports the commit.
///
/// The query joins two entities (a report and the manager who must hold
/// a badge). A badge given to a manager moves a report whose own facts
/// did not change, so the poll discovers it at one read and derives it
/// again at another; the commit renames that report in between. A poll
/// that reads the branch where it stands at each read derives the
/// renamed row, of a revision the discovery never saw.
#[dialog_common::test]
async fn it_reports_the_change_of_one_frame() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let alice = Entity::new()?;
    let carol = Entity::new()?;
    let bob = Entity::new()?;
    let mallory = Entity::new()?;
    branch
        .transaction()
        .assert(Badge::of(alice.clone()).is("A-1"))
        .assert(Name::of(bob.clone()).is("Bob"))
        .assert(Manager::of(bob.clone()).is(alice.clone()))
        .assert(Name::of(mallory.clone()).is("Mallory"))
        .assert(Manager::of(mallory.clone()).is(carol.clone()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut subscription = branch.subscribe(Query::<Report>::default());
    subscription.poll(&operator).await?.expect("initial");
    let first = reports(&branch, &operator).await?;

    branch
        .transaction()
        .assert(Badge::of(carol.clone()).is("C-3"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let second = reports(&branch, &operator).await?;

    // The poll asks for an identity once to discover the reports the
    // badge moved, then once for each report it derives again: the
    // rename lands before mallory is derived again.
    let rename = {
        let branch = branch.clone();
        let operator = operator.clone();
        let mallory = mallory.clone();
        Box::pin(async move {
            branch
                .transaction()
                .retract(Name::of(mallory.clone()).is("Mallory"))
                .assert(Name::of(mallory).is("Mal"))
                .commit()
                .publish()
                .perform(&operator)
                .await
                .expect("the interleaved commit");
        })
    };
    let interleaved = Interleaved::new(operator.clone(), 1, rename);
    let polled = subscription
        .poll(&interleaved)
        .await?
        .expect("the badge moved a report");
    assert!(interleaved.fired(), "the commit landed inside the poll");
    let third = reports(&branch, &operator).await?;

    assert!(
        same(&polled, &difference(&first, &second)),
        "the poll reports the change to the frame it began at, got {polled:?}"
    );
    assert_eq!(
        subscription.maintenances(),
        1,
        "the poll was maintained, not recomputed"
    );

    let next = subscription
        .poll(&operator)
        .await?
        .expect("the interleaved commit moved a report");
    assert!(
        same(&next, &difference(&second, &third)),
        "the next poll reports the interleaved commit, got {next:?}"
    );
    assert!(!next.is_empty());
    assert_eq!(subscription.results().len(), third.len());
    Ok(())
}
