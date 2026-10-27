//! A UI's per-frame traffic through a stack: events a rule turns into
//! session state, session state rewritten in place, a frame's batch of
//! events, and a subscription polled after every write. Every write is
//! placed on an ephemeral layer, so nothing durable is written; the
//! scenarios measure what an ephemeral-only stack commit costs over a
//! space of `size` entities. The `branch-*` scenarios are the same
//! session writes made to the branch's own session store, without a
//! stack, for comparison.
//!
//! The stack is `[space, application, top]`: the space is the scenario
//! branch, `application` an ephemeral layer linked under
//! `memory:application`, and `top` the ephemeral layer the stack is
//! opened from, which holds that link. A name is bound only by a link,
//! so the layer a scope names cannot be the top; tonk's reactor has the
//! same shape.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{ArtifactSelector, Entity, Pick, Update as _, Value};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptDescriptor, ConceptQuery, InductiveRule};
use dialog_repository::{
    Ephemeral, Placement, Stack, StackSubscription, Subscription, TransientAttribute,
};

use super::{Outcome, Prepared, seed_stuff};
use crate::env::{Env, assert_all, attribute, concept, entity, fact, query, var};

/// The commits of `stack-event`, `stack-cursor` and `stack-subscribe`.
const ROUNDS: usize = 200;

/// The commits of `stack-frame`.
const FRAMES: usize = 20;

/// The clicks each frame of `stack-frame` carries, on as many counters.
const CLICKS: usize = 100;

/// The scope the session attributes and the events are placed on.
fn application_scope() -> Entity {
    "memory:application"
        .parse()
        .expect("a scope name is an entity")
}

/// The scope the space is linked under.
fn space_scope() -> Entity {
    "memory:space".parse().expect("a scope name is an entity")
}

/// `ui/cursor` of a cursor, read under `last`.
fn cursor() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "position": { "the": "ui/cursor", "as": "natural:", "pick": "last:" }
    }}))
}

/// `assert! ui/count{this: ?this, count: ?prev + 1} when
/// ui/click{this: ?click, counter: ?this}, ui/count{this: ?this, count:
/// ?prev}`: the system of the event-system-resource loop.
fn count_rule() -> Result<InductiveRule> {
    Ok(serde_json::from_value(serde_json::json!({
        "assert!": { "with": {
            "count": { "the": "ui/count", "as": "natural:", "pick": "last:" }
        }},
        "when": [
            {
                "assert": { "with": {
                    "counter": { "the": "ui/click", "as": "entity:" }
                }},
                "where": { "this": var("click"), "counter": var("this") }
            },
            {
                "assert": { "with": {
                    "count": { "the": "ui/count", "as": "natural:", "pick": "last:" }
                }},
                "where": { "this": var("this"), "count": var("prev") }
            },
            {
                "assert": "math/sum",
                "where": { "of": var("prev"), "with": 1, "is": var("count") }
            }
        ]
    }))?)
}

/// The click `click` on `counter`.
fn click(click: Entity, counter: &Entity) -> dialog_artifacts::Changes {
    assert_all(
        [fact(&click, "ui/click", Value::Entity(counter.clone()))],
        Pick::All,
    )
}

/// The cursor moved to `position`.
fn moved(position: usize) -> dialog_artifacts::Changes {
    assert_all(
        [fact(
            &entity("cursor", 0),
            "ui/cursor",
            Value::UnsignedInt(position as u128),
        )],
        Pick::Last,
    )
}

/// The facts of `the` the application layer holds.
fn held(application: &Ephemeral, the: &str) -> Vec<dialog_artifacts::Artifact> {
    application.scan(&ArtifactSelector::new().the(attribute(the)))
}

/// The sum of every `ui/count` the application layer holds.
fn counted(application: &Ephemeral) -> u64 {
    held(application, "ui/count")
        .iter()
        .map(|fact| match fact.is {
            Value::UnsignedInt(count) => count as u64,
            _ => 0,
        })
        .sum()
}

/// A space of `size` stuff entities with the session attributes placed
/// on `memory:application` and `ui/click` declared transient, the count
/// rule committed when `rule` is set, and the stack over it. The first
/// stack commit induces over the space's whole lag (the seed), so the
/// setup makes one, stating `seed` through the stack.
async fn stacked(
    size: usize,
    rule: bool,
    seed: dialog_artifacts::Changes,
) -> Result<(Env, Stack, Ephemeral)> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    let mut declarations = env
        .branch
        .transaction()
        .assert(Placement::new(attribute("ui/click"), application_scope()))
        .assert(TransientAttribute::new(attribute("ui/click")))
        .assert(Placement::new(attribute("ui/count"), application_scope()))
        .assert(Placement::new(attribute("ui/cursor"), application_scope()))
        .assert(Placement::new(attribute("ui/item"), application_scope()));
    if rule {
        declarations = declarations.assert(count_rule()?);
    }
    declarations
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    env.reopen().await?;
    let application = Ephemeral::create().perform(&env.operator).await;
    let top = Ephemeral::create().perform(&env.operator).await;
    let stack = Stack::open(top.clone())
        .link(&top, &application, application_scope())
        .link(&application, &env.branch, space_scope())
        .perform(&env.operator)
        .await?;
    stack
        .transaction()
        .assert(seed)
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    Ok((env, stack, application))
}

/// One commit of `changes` through the stack, published.
async fn commit(env: &Env, stack: &Stack, changes: dialog_artifacts::Changes) -> Result<()> {
    stack
        .transaction()
        .assert(changes)
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    Ok(())
}

/// The counters at zero, for `stack-event` and `stack-frame`.
fn counters(count: usize) -> dialog_artifacts::Changes {
    assert_all(
        (0..count).map(|index| fact(&entity("counter", index), "ui/count", Value::UnsignedInt(0))),
        Pick::Last,
    )
}

/// What the application layer holds after the phase: the counts summed,
/// and the clicks held, which a transient never is.
fn held_outcome(application: &Ephemeral) -> Outcome {
    Outcome::from([
        ("counted".into(), counted(application)),
        (
            "clicks.held".into(),
            held(application, "ui/click").len() as u64,
        ),
    ])
}

struct Event {
    env: Env,
    stack: Stack,
    application: Ephemeral,
}

impl Prepared for Event {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let counter = entity("counter", 0);
            for round in 0..ROUNDS {
                commit(
                    &self.env,
                    &self.stack,
                    click(entity("click", round), &counter),
                )
                .await?;
            }
            let mut outcome = held_outcome(&self.application);
            outcome.insert("commits".into(), ROUNDS as u64);
            Ok(outcome)
        })
    }
}

/// `stack-event`: the count rule over a stack on `size` entities, then
/// two hundred commits of one transient click each, which the rule turns
/// into the counter's next count on the application layer.
pub async fn event(size: usize) -> Result<Box<dyn Prepared>> {
    let (env, stack, application) = stacked(size, true, counters(1)).await?;
    Ok(Box::new(Event {
        env,
        stack,
        application,
    }))
}

/// The space under `stack-event-held`: small, since the scenario is about
/// what the application layer holds.
const HELD_SPACE: usize = 100;

/// `stack-event-held`: `stack-event` with `size` session facts held in the
/// application layer beside the counter, as a UI's state accumulates. An
/// event's cost must not grow with them: no read of the commit may walk
/// what the layers hold to find what it needs.
pub async fn event_held(size: usize) -> Result<Box<dyn Prepared>> {
    let mut seed = counters(1);
    for index in 0..size {
        seed.associate(
            attribute("ui/item"),
            entity("item", index),
            Value::UnsignedInt(index as u128),
            Pick::Last,
        );
    }
    let (env, stack, application) = stacked(HELD_SPACE, true, seed).await?;
    anyhow::ensure!(
        held(&application, "ui/item").len() == size,
        "the application layer holds every item"
    );
    Ok(Box::new(Event {
        env,
        stack,
        application,
    }))
}

struct Frame {
    env: Env,
    stack: Stack,
    application: Ephemeral,
}

impl Prepared for Frame {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            for frame in 0..FRAMES {
                let clicks = (0..CLICKS).map(|index| {
                    fact(
                        &entity(&format!("click-{frame}"), index),
                        "ui/click",
                        Value::Entity(entity("counter", index)),
                    )
                });
                commit(&self.env, &self.stack, assert_all(clicks, Pick::All)).await?;
            }
            let mut outcome = held_outcome(&self.application);
            outcome.insert("commits".into(), FRAMES as u64);
            Ok(outcome)
        })
    }
}

/// `stack-frame`: the count rule over a stack on `size` entities, then
/// twenty commits of a hundred transient clicks each, one per counter.
pub async fn frame(size: usize) -> Result<Box<dyn Prepared>> {
    let (env, stack, application) = stacked(size, true, counters(CLICKS)).await?;
    Ok(Box::new(Frame {
        env,
        stack,
        application,
    }))
}

/// The cursor's position read through `rows`.
fn position(rows: &[ConceptConclusion]) -> u64 {
    rows.iter()
        .filter_map(|row| row.get::<u64>("position").ok())
        .max()
        .unwrap_or(0)
}

struct Cursor {
    env: Env,
    stack: Stack,
    application: Ephemeral,
}

impl Prepared for Cursor {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            for round in 1..=ROUNDS {
                commit(&self.env, &self.stack, moved(round)).await?;
            }
            let rows: Vec<ConceptConclusion> = self
                .stack
                .query()
                .select(query(&cursor(), &["position"], Some(&entity("cursor", 0))))
                .perform(&self.env.operator)
                .try_vec()
                .await?;
            Ok(Outcome::from([
                ("commits".into(), ROUNDS as u64),
                ("rows".into(), rows.len() as u64),
                ("position".into(), position(&rows)),
                (
                    "cursors.held".into(),
                    held(&self.application, "ui/cursor").len() as u64,
                ),
            ]))
        })
    }
}

/// `stack-cursor`: a stack on `size` entities, then two hundred commits
/// each moving one cursor under `last` on the application layer, and one
/// read of the cursor through the stack.
pub async fn cursor_moves(size: usize) -> Result<Box<dyn Prepared>> {
    let (env, stack, application) = stacked(size, false, moved(0)).await?;
    Ok(Box::new(Cursor {
        env,
        stack,
        application,
    }))
}

/// The subscription every `*-subscribe` scenario polls: every cursor.
fn cursors() -> ConceptQuery {
    query(&cursor(), &["position"], None)
}

/// The outcome of a subscription's phase.
fn subscribed(polls: u64, deltas: u64, recomputes: usize, maintenances: usize) -> Outcome {
    Outcome::from([
        ("commits".into(), ROUNDS as u64),
        ("polls".into(), polls),
        ("deltas".into(), deltas),
        ("recomputes".into(), recomputes as u64),
        ("maintenances".into(), maintenances as u64),
    ])
}

struct Subscribe {
    env: Env,
    stack: Stack,
    subscription: StackSubscription<ConceptQuery>,
}

impl Prepared for Subscribe {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let (recomputes, maintenances) = (
                self.subscription.recomputes(),
                self.subscription.maintenances(),
            );
            let mut deltas = 0u64;
            for round in 1..=ROUNDS {
                commit(&self.env, &self.stack, moved(round)).await?;
                if self.subscription.poll(&self.env.operator).await?.is_some() {
                    deltas += 1;
                }
            }
            anyhow::ensure!(
                position(self.subscription.results()) == ROUNDS as u64,
                "the subscription follows the cursor"
            );
            Ok(subscribed(
                ROUNDS as u64,
                deltas,
                self.subscription.recomputes() - recomputes,
                self.subscription.maintenances() - maintenances,
            ))
        })
    }
}

/// `stack-subscribe`: `stack-cursor` with a subscription to every cursor
/// open over the stack, polled once in setup and after every commit.
pub async fn subscribe(size: usize) -> Result<Box<dyn Prepared>> {
    let (env, stack, _) = stacked(size, false, moved(0)).await?;
    let mut subscription = stack.query().subscribe(cursors());
    subscription.poll(&env.operator).await?;
    Ok(Box::new(Subscribe {
        env,
        stack,
        subscription,
    }))
}

/// A branch on `size` entities with the cursor at zero in its session
/// store: the `branch-*` counterparts, with no stack and no placements.
async fn branch(size: usize) -> Result<Env> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    env.branch.overlay().apply(moved(0))?;
    Ok(env)
}

struct BranchCursor {
    env: Env,
}

impl Prepared for BranchCursor {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            for round in 1..=ROUNDS {
                self.env.branch.overlay().apply(moved(round))?;
            }
            let rows: Vec<ConceptConclusion> = self
                .env
                .branch
                .select(query(&cursor(), &["position"], Some(&entity("cursor", 0))))
                .perform(&self.env.operator)
                .try_vec()
                .await?;
            Ok(Outcome::from([
                ("commits".into(), ROUNDS as u64),
                ("rows".into(), rows.len() as u64),
                ("position".into(), position(&rows)),
                (
                    "cursors.held".into(),
                    held(self.env.branch.overlay(), "ui/cursor").len() as u64,
                ),
            ]))
        })
    }
}

/// `branch-cursor`: `stack-cursor` without the stack. A branch on `size`
/// entities, then two hundred cursor moves under `last` written straight
/// into its session store, and one read of the cursor through the branch.
pub async fn branch_cursor(size: usize) -> Result<Box<dyn Prepared>> {
    let env = branch(size).await?;
    Ok(Box::new(BranchCursor { env }))
}

struct BranchSubscribe {
    env: Env,
    subscription: Subscription<ConceptQuery>,
}

impl Prepared for BranchSubscribe {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let (recomputes, maintenances) = (
                self.subscription.recomputes(),
                self.subscription.maintenances(),
            );
            let mut deltas = 0u64;
            for round in 1..=ROUNDS {
                self.env.branch.overlay().apply(moved(round))?;
                if self.subscription.poll(&self.env.operator).await?.is_some() {
                    deltas += 1;
                }
            }
            anyhow::ensure!(
                position(self.subscription.results()) == ROUNDS as u64,
                "the subscription follows the cursor"
            );
            Ok(subscribed(
                ROUNDS as u64,
                deltas,
                self.subscription.recomputes() - recomputes,
                self.subscription.maintenances() - maintenances,
            ))
        })
    }
}

/// `branch-subscribe`: `stack-subscribe` without the stack. A branch on
/// `size` entities with a subscription to every cursor, then two hundred
/// cursor moves written into its session store, each followed by a poll.
pub async fn branch_subscribe(size: usize) -> Result<Box<dyn Prepared>> {
    let env = branch(size).await?;
    let mut subscription = env.branch.subscribe(cursors());
    subscription.poll(&env.operator).await?;
    Ok(Box::new(BranchSubscribe { env, subscription }))
}
