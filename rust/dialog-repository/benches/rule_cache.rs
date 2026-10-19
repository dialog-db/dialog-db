//! What a query costs after a write, depending on whether the write
//! changes a rule.
//!
//! A branch holds `PEOPLE` people and a committed chain of `DEPTH`
//! projection rules, `org/a0` (stored) through `org/a{DEPTH}`, and each
//! scenario queries the concept over the last link. Only the query is
//! timed; the write before it is not.
//!
//! - `warm`: the same query again, nothing written in between.
//! - `overlay_write`: a session-overlay fact that is no rule, toggled.
//! - `commit`: a commit of a fact that is no rule, toggled.
//! - `transaction`: a query through a transaction staging a fact that is
//!   no rule.
//! - `with_rule`: the query carries the last link of the chain in its
//!   own overlay (`.with(rule)`) instead of finding it committed.
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use dialog_capability::Subject;
use dialog_effects::storage::Location;
use dialog_peer::helpers::{open_peer, test_storage};
use dialog_peer::{Peer, Session};
use dialog_query::concept::descriptor::{ConceptConclusion, ConceptDescriptor};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::query::Output as _;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{DeductiveRule, Entity, Parameters, Term, the};
use dialog_repository::{Branch, RepositoryExt as _};
use dialog_storage::provider::storage::VolatileSpace;

const PEOPLE: usize = 50;
const DEPTH: usize = 10;

/// A rule deriving `org/a{to}` from `org/a{to - 1}`.
fn link(to: usize) -> DeductiveRule {
    let json = serde_json::json!({
        "deduce": { "with": { "v": { "the": format!("org/a{to}"), "as": "text:" } } },
        "when": [{
            "assert": { "with": { "v": { "the": format!("org/a{}", to - 1), "as": "text:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "v": { "?": { "name": "v" } }
            }
        }]
    });
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
    descriptor.compile().unwrap()
}

fn query() -> ConceptQuery {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": { "v": { "the": format!("org/a{DEPTH}"), "as": "text:" } }
    }))
    .unwrap();
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("v".into(), Term::var("v"));
    ConceptQuery { predicate, terms }
}

/// A branch holding the people and the chain, every link committed
/// except the last when `committed_last` is false.
async fn open(name: &str, committed_last: bool) -> (Branch, Peer<VolatileSpace, Session>) {
    let storage = test_storage().await;
    let profile = open_peer(storage.clone(), Location::profile(name.to_string()))
        .await
        .unwrap();
    let operator = profile
        .session(b"bench")
        .space(profile.state())
        .allow(Subject::any())
        .await
        .unwrap();
    let repo = profile
        .space(format!("{name}-repo"))
        .open()
        .perform(&operator)
        .await
        .unwrap();
    let branch = repo.branch("main").open().perform(&operator).await.unwrap();
    let mut tx = branch.transaction();
    for person in 0..PEOPLE {
        let entity: Entity = format!("id:person-{person}").parse().unwrap();
        tx = tx.assert(the!("org/a0").of(entity).is(format!("person {person}")));
    }
    let last = if committed_last { DEPTH } else { DEPTH - 1 };
    for to in 1..=last {
        tx = tx.assert(link(to));
    }
    tx.commit().publish().perform(&operator).await.unwrap();
    let branch = repo.branch("main").open().perform(&operator).await.unwrap();
    (branch, operator)
}

/// A fact no rule reads or is: `on` toggles it in or out.
fn unrelated(on: u64) -> impl dialog_query::Statement {
    the!("org/unrelated")
        .of("id:unrelated".parse::<Entity>().unwrap())
        .is(format!("{}", on % 2))
}

async fn rows(branch: &Branch, operator: &Peer<VolatileSpace, Session>) -> usize {
    let rows: Vec<ConceptConclusion> = branch
        .query()
        .select(query())
        .perform(operator)
        .try_vec()
        .await
        .unwrap();
    rows.len()
}

fn rule_cache(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("rule_cache");

    let (branch, operator) = runtime.block_on(open("rule-cache-committed", true));
    assert_eq!(runtime.block_on(rows(&branch, &operator)), PEOPLE);

    group.bench_function("warm", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let (branch, operator) = (&branch, &operator);
            async move {
                let mut spent = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    assert_eq!(rows(branch, operator).await, PEOPLE);
                    spent += start.elapsed();
                }
                spent
            }
        })
    });

    group.bench_function("overlay_write", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let (branch, operator) = (&branch, &operator);
            async move {
                let mut spent = Duration::ZERO;
                for iteration in 0..iters {
                    if iteration % 2 == 0 {
                        branch.overlay().assert(unrelated(iteration)).unwrap();
                    } else {
                        branch.overlay().retract(unrelated(iteration - 1)).unwrap();
                    }
                    let start = Instant::now();
                    assert_eq!(rows(branch, operator).await, PEOPLE);
                    spent += start.elapsed();
                }
                spent
            }
        })
    });

    group.bench_function("commit", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let (branch, operator) = (&branch, &operator);
            async move {
                let mut spent = Duration::ZERO;
                for iteration in 0..iters {
                    branch
                        .transaction()
                        .assert(unrelated(iteration))
                        .commit()
                        .publish()
                        .perform(operator)
                        .await
                        .unwrap();
                    let start = Instant::now();
                    assert_eq!(rows(branch, operator).await, PEOPLE);
                    spent += start.elapsed();
                }
                spent
            }
        })
    });

    group.bench_function("transaction", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let (branch, operator) = (&branch, &operator);
            async move {
                let mut spent = Duration::ZERO;
                for iteration in 0..iters {
                    let tx = branch.transaction().assert(unrelated(iteration));
                    let start = Instant::now();
                    let found: Vec<ConceptConclusion> = tx
                        .query()
                        .select(query())
                        .perform(operator)
                        .try_vec()
                        .await
                        .unwrap();
                    assert_eq!(found.len(), PEOPLE);
                    spent += start.elapsed();
                }
                spent
            }
        })
    });

    let (partial, partial_operator) = runtime.block_on(open("rule-cache-partial", false));
    group.bench_function("with_rule", |b| {
        b.to_async(&runtime).iter_custom(|iters| {
            let (branch, operator) = (&partial, &partial_operator);
            async move {
                let mut spent = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let found: Vec<ConceptConclusion> = branch
                        .query()
                        .with(link(DEPTH))
                        .select(query())
                        .perform(operator)
                        .try_vec()
                        .await
                        .unwrap();
                    assert_eq!(found.len(), PEOPLE);
                    spent += start.elapsed();
                }
                spent
            }
        })
    });

    group.finish();
}

criterion_group!(benches, rule_cache);
criterion_main!(benches);
