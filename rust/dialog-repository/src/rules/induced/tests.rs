//! Plans kept with a rule: that a kept plan is the plan made afresh,
//! that a commit plans once for the seeds that bind the same names, and
//! that a rule keeps no more plans than its bound.
//!
//! Beside these, every kept plan handed out in any test of this crate
//! is compared with the plan made afresh for its scope
//! ([`Induced::plan`]).

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::{Induced, PLANS, probe};
use crate::helpers::test_repo;
use anyhow::Result;
use dialog_corpus::{Entity, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::{Environment, InductiveRule};
use futures_util::StreamExt as _;
use serde_json::json;

/// `assert! counter{count: ?prev + 1} when increment{counter: ?this},
/// counter{this: ?this, count: ?prev}`.
fn increment() -> InductiveRule {
    serde_json::from_value(json!({
        "assert!": {
            "with": { "count": { "the": "counter/count", "as": "natural:" } }
        },
        "when": [
            {
                "assert": {
                    "with": { "counter": { "the": "cmd.increment/counter", "as": "entity:" } }
                },
                "where": { "counter": { "?": { "name": "this" } } }
            },
            {
                "assert": {
                    "with": { "count": { "the": "counter/count", "as": "natural:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "count": { "?": { "name": "prev" } }
                }
            },
            {
                "assert": "math/sum",
                "where": {
                    "of": { "?": { "name": "prev" } },
                    "with": 1,
                    "is": { "?": { "name": "count" } }
                }
            }
        ]
    }))
    .expect("the increment rule compiles")
}

fn scope(names: &[&str]) -> Environment {
    let mut scope = Environment::new();
    for name in names {
        scope.add(*name);
    }
    scope
}

/// Keeping plans off for as long as this lives, and back on when it
/// drops.
struct Unkept;

impl Unkept {
    fn new() -> Self {
        probe::off(true);
        Unkept
    }
}

impl Drop for Unkept {
    fn drop(&mut self) {
        probe::off(false);
    }
}

/// For every set of names a seed of the rule may bind, the plan handed
/// out the first time and every time after is the plan made afresh, and
/// it is made once. Two sets of names are two plans: a plan is ordered
/// for what is bound.
#[dialog_common::test]
fn it_hands_out_the_plan_made_afresh_for_each_set_of_names() {
    let rule = increment();
    let induced = Induced::new(rule.clone());
    // The rule's own name for what the body reads the count into.
    let prev = rule
        .analysis()
        .types
        .iter()
        .map(|(name, _)| name.clone())
        .find(|name| name != "this" && name != "count")
        .expect("the body has a local");
    let scopes = [
        scope(&[]),
        scope(&["this"]),
        scope(&[prev.as_str()]),
        scope(&["this", prev.as_str()]),
        scope(&["this", "count"]),
    ];
    probe::take();
    for _ in 0..3 {
        for scope in &scopes {
            assert_eq!(induced.plan(scope), rule.plan(scope));
        }
    }
    assert_eq!(
        probe::take(),
        (scopes.len(), 2 * scopes.len()),
        "each scope is planned once and handed out from then on"
    );
    assert_eq!(induced.kept(), scopes.len());
    assert_ne!(
        induced.plan(&scopes[0]),
        induced.plan(&scopes[1]),
        "a plan for nothing bound is not the plan for `this` bound"
    );

    // The same scope built in another order is the same set of names.
    probe::take();
    assert_eq!(
        induced.plan(&scope(&[prev.as_str(), "this"])),
        rule.plan(&scopes[3])
    );
    assert_eq!(probe::take(), (0, 1));
}

/// A rule keeps at most [`PLANS`] plans. A scope past the bound is
/// planned each time it is asked for, and is still the right plan.
#[dialog_common::test]
fn it_keeps_no_more_plans_than_its_bound() {
    let rule = increment();
    let induced = Induced::new(rule.clone());
    let names: Vec<String> = (0..PLANS + 8).map(|at| format!("extra{at}")).collect();
    probe::take();
    for name in &names {
        let scope = scope(&["this", name.as_str()]);
        assert_eq!(induced.plan(&scope), rule.plan(&scope));
    }
    assert_eq!(induced.kept(), PLANS);
    assert_eq!(probe::take(), (PLANS + 8, 0));

    let past = scope(&["this", names[PLANS + 3].as_str()]);
    assert_eq!(induced.plan(&past), rule.plan(&past));
    assert_eq!(probe::take(), (1, 0), "a scope past the bound is not kept");
    let within = scope(&["this", names[3].as_str()]);
    assert_eq!(induced.plan(&within), rule.plan(&within));
    assert_eq!(probe::take(), (0, 1), "a scope within it is");
}

/// What a commit of `commands` increments leaves in each counter, and
/// how many plans the commit made and reused; then the same for a
/// second commit.
async fn incremented(
    commands: usize,
    keep: bool,
) -> Result<(Vec<Vec<Value>>, [(usize, usize); 2])> {
    let _unkept = (!keep).then(Unkept::new);
    let (operator, profile) = test_session_with_peer().await;
    let repository = test_repo(&operator, &profile).await;
    let branch = repository.branch("main").open().perform(&operator).await?;

    let counter = |at: usize| -> Entity { format!("ctr:{at}").parse().expect("an entity") };
    let mut transaction = branch.transaction().assert(increment());
    for at in 0..commands {
        transaction = transaction.assert(
            dialog_query::the!("counter/count")
                .of(counter(at))
                .is(10 * at as u64),
        );
    }
    transaction.commit().publish().perform(&operator).await?;
    branch.refresh(&operator).await?;

    let mut counts = [(0, 0); 2];
    for (round, count) in counts.iter_mut().enumerate() {
        probe::take();
        let mut transaction = branch.transaction();
        for at in 0..commands {
            let command: Entity = format!("cmd:{round}-{at}").parse()?;
            transaction = transaction.dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter(at)),
            );
        }
        transaction.commit().publish().perform(&operator).await?;
        branch.refresh(&operator).await?;
        *count = probe::take();
    }

    let mut held = Vec::new();
    for at in 0..commands {
        let selector = Selector::new()
            .the("counter/count".parse()?)
            .of(counter(at));
        let stream = branch.claims().select(selector).perform(&operator).await?;
        let facts: Vec<_> = stream.collect().await;
        held.push(
            facts
                .into_iter()
                .map(|item| item.and_then(|view| view.to_owned()).map(|fact| fact.is))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok((held, counts))
}

/// A commit of six commands seeds the rule six times with one set of
/// names, and plans it once. The next commit plans nothing: the plans
/// are kept with the rule, not for the commit. With keeping off every
/// seed is planned, and the counters end the same.
#[dialog_common::test]
async fn it_plans_once_for_the_seeds_that_bind_the_same_names() -> Result<()> {
    const COMMANDS: usize = 6;
    let (kept_counts, [first, second]) = incremented(COMMANDS, true).await?;
    let (unkept_counts, [first_unkept, second_unkept]) = incremented(COMMANDS, false).await?;

    assert_eq!(kept_counts, unkept_counts, "the counters end the same");
    for (at, held) in kept_counts.iter().enumerate() {
        assert_eq!(
            held,
            &vec![Value::UnsignedInt(10 * at as u128 + 2)],
            "counter {at} was incremented twice"
        );
    }

    assert_eq!(first_unkept.1, 0, "with keeping off no plan is reused");
    assert!(
        first_unkept.0 >= COMMANDS,
        "and every seed is planned: {first_unkept:?}"
    );
    assert_eq!(
        first.0 + first.1,
        first_unkept.0,
        "the same seeds ask for a plan either way: {first:?} against {first_unkept:?}"
    );
    assert!(
        first.0 <= 3,
        "the first commit plans once for each set of names, not once a seed: {first:?}"
    );
    assert_eq!(second.0, 0, "the second commit plans nothing: {second:?}");
    assert_eq!(second.0 + second.1, second_unkept.0);
    Ok(())
}
