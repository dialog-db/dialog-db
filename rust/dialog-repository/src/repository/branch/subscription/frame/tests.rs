//! A poll reads one frame: tests that interleave a write between two
//! reads of one poll, or between the polls of two subscriptions.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dialog_capability::{Command, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync, Held, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Entity, Relation as CorpusRelation};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::attribute::Relation;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::types::Any;
use dialog_query::{
    AttributeQuery, Claim, Conclusion as _, Fold, FoldConclusion, Output as _, Query, Term, the,
};
use dialog_search_tree::nodes;
use dialog_storage::provider::storage::VolatileSpace;

use super::super::Delta;
use super::super::tests::concepts::{Badge, Manager, Name, Report};
use super::PollError;
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
/// chooses. Clones share the count and the hook, so a `settle` that
/// clones its env counts as one.
#[derive(Clone)]
struct Interleaved<P> {
    inner: P,
    calls: Arc<AtomicUsize>,
    at: usize,
    hook: Arc<Mutex<Option<Hook>>>,
}

impl<P> Interleaved<P> {
    fn new(inner: P, at: usize, hook: Hook) -> Self {
        Self {
            inner,
            calls: Arc::new(AtomicUsize::new(0)),
            at,
            hook: Arc::new(Mutex::new(Some(hook))),
        }
    }

    /// An env that runs nothing and counts the identities asked for.
    fn counting(inner: P) -> Self {
        Self::new(inner, usize::MAX, Box::pin(async {}))
    }

    /// Whether the hook ran.
    fn fired(&self) -> bool {
        self.hook.lock().expect("hook lock").is_none()
    }

    /// How many identities were asked for.
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
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

/// A branch with a badge holder and one report to them, and a report
/// whose manager holds no badge yet: alice, bob, carol, mallory.
async fn staffed() -> anyhow::Result<(Operator, Branch, [Entity; 4])> {
    let (operator, branch, _, people) = staffed_beside().await?;
    Ok((operator, branch, people))
}

/// [`staffed`], with another branch of the same repository.
async fn staffed_beside() -> anyhow::Result<(Operator, Branch, Branch, [Entity; 4])> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let people = [
        Entity::new()?,
        Entity::new()?,
        Entity::new()?,
        Entity::new()?,
    ];
    let [alice, bob, carol, mallory] = people.clone();
    branch
        .transaction()
        .assert(Badge::of(alice.clone()).is("A-1"))
        .assert(Name::of(bob.clone()).is("Bob"))
        .assert(Manager::of(bob).is(alice))
        .assert(Name::of(mallory.clone()).is("Mallory"))
        .assert(Manager::of(mallory).is(carol))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let other = repo.branch("other").open().perform(&operator).await?;
    Ok((operator, branch, other, people))
}

type Operator = Peer<VolatileSpace, Session>;

/// Give `who` a badge.
async fn badge(branch: &Branch, operator: &Operator, who: &Entity) -> anyhow::Result<()> {
    branch
        .transaction()
        .assert(Badge::of(who.clone()).is("C-3"))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// A poll at the frame the subscription last polled at does nothing, a
/// commit since notwithstanding; one at a frame taken before that one
/// is refused as stale, and one at a frame of another branch as foreign.
/// Neither refusal changes the subscription: its results and its counts
/// stand, and its next poll reports the commit as it would have.
#[dialog_common::test]
async fn it_refuses_a_frame_older_than_its_last() -> anyhow::Result<()> {
    let (operator, branch, other, [_, _, carol, _]) = staffed_beside().await?;
    let mut subscription = branch.subscribe(Query::<Report>::default());

    let older = subscription.frame();
    let newer = branch.frame();
    assert!(newer.is_after(&older));
    subscription
        .poll_at(&newer, &operator)
        .await?
        .expect("initial");
    let results = subscription.results().to_vec();
    let counts = (subscription.recomputes(), subscription.maintenances());

    badge(&branch, &operator, &carol).await?;
    assert!(
        subscription.poll_at(&newer, &operator).await?.is_none(),
        "the frame it last polled at holds nothing new"
    );

    let stale = subscription.poll_at(&older, &operator).await;
    assert!(matches!(stale, Err(PollError::Stale)), "got {stale:?}");
    let foreign = subscription.poll_at(&other.frame(), &operator).await;
    assert!(
        matches!(foreign, Err(PollError::Foreign)),
        "got {foreign:?}"
    );
    let through = branch.stack().perform(&operator).await?;
    let stacked = subscription.poll_at(&through.frame(), &operator).await;
    assert!(
        matches!(stacked, Err(PollError::Foreign)),
        "a frame of a stack on the branch is not of the branch alone, got {stacked:?}"
    );
    assert_eq!(subscription.results(), results.as_slice());
    assert_eq!(
        (subscription.recomputes(), subscription.maintenances()),
        counts
    );

    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the commit moved a report");
    assert_eq!(delta.asserted.len(), 1);
    assert_eq!(subscription.maintenances(), counts.1 + 1);
    Ok(())
}

/// Two subscriptions polled at one frame agree: a commit that changes
/// both, landing between their polls, shows in neither, and both report
/// it at the next frame. Polled where the branch stands instead, the
/// second would report it and the first not.
#[dialog_common::test]
async fn it_has_two_subscriptions_at_one_frame_agree() -> anyhow::Result<()> {
    use super::super::tests::concepts::BadgeHolder;

    let (operator, branch, [_, _, carol, _]) = staffed().await?;
    let mut reports = branch.subscribe(Query::<Report>::default());
    let mut holders = branch.subscribe(Query::<BadgeHolder>::default());
    let mut apart = branch.subscribe(Query::<BadgeHolder>::default());
    reports.poll(&operator).await?.expect("initial");
    holders.poll(&operator).await?.expect("initial");
    apart.poll(&operator).await?.expect("initial");

    let frame = branch.frame();
    assert!(reports.poll_at(&frame, &operator).await?.is_none());
    badge(&branch, &operator, &carol).await?;
    assert!(
        holders.poll_at(&frame, &operator).await?.is_none(),
        "the badge landed after the frame"
    );
    let moved = apart.poll(&operator).await?.expect("the badge");
    assert_eq!(
        moved.asserted.len(),
        1,
        "where the branch stands, the badge is there"
    );

    let next = branch.frame();
    let reported = reports
        .poll_at(&next, &operator)
        .await?
        .expect("the badge moved a report");
    let held = holders
        .poll_at(&next, &operator)
        .await?
        .expect("the badge moved a holder");
    assert_eq!(reported.asserted.len(), 1);
    assert_eq!(held.asserted, moved.asserted);
    Ok(())
}

/// A fact asserted into the session overlay after a frame is taken is
/// not read by a poll at that frame, and the next poll reports it; a
/// fact asserted and retracted between two frames moves nothing.
#[dialog_common::test]
async fn it_reads_the_session_overlay_as_the_frame_froze_it() -> anyhow::Result<()> {
    let (operator, branch, [_, _, carol, mallory]) = staffed().await?;
    let mut subscription = branch.subscribe(Query::<Report>::default());

    let frame = branch.frame();
    branch
        .overlay()
        .assert(Badge::of(carol.clone()).is("C-3"))?;
    let initial = subscription
        .poll_at(&frame, &operator)
        .await?
        .expect("initial");
    assert!(
        !initial.asserted.iter().any(|row| row.this == mallory),
        "the badge was asserted after the frame, got {initial:?}"
    );
    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the badge moved a report");
    assert_eq!(
        delta
            .asserted
            .iter()
            .map(|row| row.this.clone())
            .collect::<Vec<_>>(),
        vec![mallory.clone()]
    );
    assert_eq!(subscription.maintenances(), 1);

    let held = subscription.results().to_vec();
    branch
        .overlay()
        .assert(Badge::of(mallory.clone()).is("M-9"))?;
    branch
        .overlay()
        .retract(Badge::of(mallory.clone()).is("M-9"))?;
    let churned = subscription.poll(&operator).await?;
    assert!(
        churned.as_ref().is_none_or(Delta::is_empty),
        "a fact that came and went between two frames moves nothing, got {churned:?}"
    );
    assert_eq!(subscription.results(), held.as_slice());
    Ok(())
}

/// `the` of every to-do, its value bound to `is` and its cause to
/// `cause`.
fn todo_read(the: &str, is: &str, cause: &str) -> AttributeQuery {
    AttributeQuery::new(
        Term::from(Relation::from(
            the.parse::<CorpusRelation>().expect("an attribute"),
        )),
        Term::<Entity>::var("t"),
        Term::<Any>::var(is),
        Term::var(cause),
        None,
    )
}

/// Each to-do with a title, and how many tags it has.
fn tag_counts() -> Fold<AttributeQuery, AttributeQuery> {
    Fold::new(
        todo_read("todo/title", "title", "c1"),
        todo_read("todo/tag", "g", "c2"),
        Reduce::new(
            vec!["t".into()],
            vec![ReduceEntry::new("tags", Aggregator::Count, Term::var("g"))],
        ),
    )
}

/// Each folded row as its to-do and its count, ordered.
fn tallied<'a>(rows: impl IntoIterator<Item = &'a FoldConclusion>) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = rows
        .into_iter()
        .map(|row| (row.this().to_string(), format!("{:?}", row.get("tags"))))
        .collect();
    rows.sort();
    rows
}

/// The to-do `which`.
fn todo(which: usize) -> Entity {
    format!("id:todo-{which}").parse().expect("an entity")
}

/// A commit of `facts`, each `(attribute, to-do, value)`.
async fn todos(
    branch: &Branch,
    operator: &Operator,
    facts: &[(&str, usize, &str)],
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for (the, which, is) in facts {
        let (of, is) = (todo(*which), is.to_string());
        transaction = match *the {
            "todo/title" => transaction.assert(the!("todo/title").of(of).is(is)),
            "todo/tag" => transaction.assert(the!("todo/tag").of(of).is(is)),
            other => anyhow::bail!("no to-do attribute {other}"),
        };
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// Two to-dos, one tagged.
async fn listed() -> anyhow::Result<(Operator, Branch)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    todos(
        &branch,
        &operator,
        &[
            ("todo/title", 1, "Buy milk"),
            ("todo/tag", 1, "home"),
            ("todo/title", 2, "Call mum"),
        ],
    )
    .await?;
    Ok((operator, branch))
}

/// The change one poll reads: a new to-do and a tag of another, moving
/// both of a fold's queries.
const FIRST: [(&str, usize, &str); 2] =
    [("todo/title", 3, "Water plants"), ("todo/tag", 2, "call")];

/// The change that lands inside the poll, moving both again.
const SECOND: [(&str, usize, &str); 2] = [("todo/tag", 3, "garden"), ("todo/title", 4, "Pay rent")];

/// A commit landing between the two inner polls of a standing fold is in
/// neither: the fold after the poll is a select of the fold at the frame
/// the poll began at, and the next poll brings it to a select after the
/// commit. The commit lands at every read where the poll asks for an
/// identity, the first inside the groups query's poll and the next
/// between it and the inputs query's.
#[dialog_common::test]
async fn it_folds_one_frame_when_a_commit_lands_between_its_inner_polls() -> anyhow::Result<()> {
    let calls = {
        let (operator, branch) = listed().await?;
        let mut fold = branch.subscribe_fold(tag_counts());
        fold.poll(&operator).await?.expect("initial");
        todos(&branch, &operator, &FIRST).await?;
        let counting = Interleaved::counting(operator.clone());
        fold.poll(&counting).await?.expect("the first change");
        counting.calls()
    };
    assert!(
        calls >= 2,
        "each inner query derives an entity again: {calls}"
    );

    for at in 0..calls {
        let (operator, branch) = listed().await?;
        let mut fold = branch.subscribe_fold(tag_counts());
        fold.poll(&operator).await?.expect("initial");
        todos(&branch, &operator, &FIRST).await?;
        let selected = |branch: Branch, operator: Operator| async move {
            let rows: Vec<FoldConclusion> = branch
                .query()
                .select(tag_counts())
                .perform(&operator)
                .try_vec()
                .await?;
            anyhow::Ok(tallied(&rows))
        };
        let first = selected(branch.clone(), operator.clone()).await?;

        let commit = {
            let (branch, operator) = (branch.clone(), operator.clone());
            Box::pin(async move {
                todos(&branch, &operator, &SECOND)
                    .await
                    .expect("the interleaved commit");
            })
        };
        let interleaved = Interleaved::new(operator.clone(), at, commit);
        fold.poll(&interleaved).await?.expect("the first change");
        assert!(
            interleaved.fired(),
            "at {at}: the commit landed inside the poll"
        );
        assert_eq!(
            tallied(fold.results()),
            first,
            "at {at}: the fold is of the frame the poll began at"
        );

        let second = selected(branch.clone(), operator.clone()).await?;
        assert_ne!(first, second);
        fold.poll(&operator).await?.expect("the interleaved commit");
        assert_eq!(tallied(fold.results()), second, "at {at}: the next poll");
    }
    Ok(())
}

/// Each row of a stack's subscriber, as its entity and value, ordered.
fn claimed(claims: &[Claim]) -> Vec<String> {
    let mut rows: Vec<String> = claims
        .iter()
        .map(|claim| format!("{} {:?}", claim.of, claim.is))
        .collect();
    rows.sort();
    rows
}

/// What `query` reads through `stack` now.
async fn through(
    stack: &crate::Stack,
    operator: &Operator,
    query: AttributeQuery,
) -> anyhow::Result<Vec<String>> {
    let claims: Vec<Claim> = stack
        .query()
        .select(query)
        .perform(operator)
        .try_vec()
        .await?;
    Ok(claimed(&claims))
}

/// One round of a stack's `settle` polls every subscription at one
/// frame: a commit that moves two subscriptions, landing while the
/// round polls them, is delivered by neither, and the next round
/// delivers it to both. The commit lands at every read where the round
/// asks for an identity, between the two polls among them.
#[dialog_common::test]
async fn it_settles_a_round_at_one_frame() -> anyhow::Result<()> {
    let titles = || todo_read("todo/title", "title", "c1");
    let tags = || todo_read("todo/tag", "g", "c2");
    let opened = || async {
        let (operator, branch) = listed().await?;
        let stack = branch.stack().perform(&operator).await?;
        let mut first = stack.subscribe(titles());
        let mut second = stack.subscribe(tags());
        stack.settle(&operator).await?;
        first.poll();
        second.poll();
        todos(&branch, &operator, &FIRST).await?;
        stack.schedule_poll();
        anyhow::Ok((operator, branch, stack, first, second))
    };
    let calls = {
        let (operator, _branch, stack, _first, _second) = opened().await?;
        let counting = Interleaved::counting(operator);
        assert_eq!(stack.settle(&counting).await?, 2);
        counting.calls()
    };
    assert!(
        calls >= 2,
        "each subscription derives an entity again: {calls}"
    );

    for at in 0..calls {
        let (operator, branch, stack, mut first, mut second) = opened().await?;
        let before = (
            through(&stack, &operator, titles()).await?,
            through(&stack, &operator, tags()).await?,
        );
        let commit = {
            let (branch, operator) = (branch.clone(), operator.clone());
            Box::pin(async move {
                todos(&branch, &operator, &SECOND)
                    .await
                    .expect("the interleaved commit");
            })
        };
        let interleaved = Interleaved::new(operator.clone(), at, commit);
        stack.settle(&interleaved).await?;
        assert!(
            interleaved.fired(),
            "at {at}: the commit landed inside the round"
        );
        assert_eq!(
            (claimed(&first.results()), claimed(&second.results())),
            before,
            "at {at}: the round is of the frame it began at"
        );
        assert_eq!((first.poll().len(), second.poll().len()), (1, 1));

        stack.schedule_poll();
        assert_eq!(
            stack.settle(&operator).await?,
            2,
            "at {at}: both move again"
        );
        let after = (
            through(&stack, &operator, titles()).await?,
            through(&stack, &operator, tags()).await?,
        );
        assert_ne!(after, before);
        assert_eq!(
            (claimed(&first.results()), claimed(&second.results())),
            after
        );
    }
    Ok(())
}
