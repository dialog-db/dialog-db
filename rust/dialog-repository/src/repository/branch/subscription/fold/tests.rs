//! A query that folds, read through a branch: selected once, and under a
//! standing fold, which must give what a select gives after every poll.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::{BTreeMap, BTreeSet};

use crate::Branch;
use crate::helpers::test_repo;
use dialog_corpus::{Entity, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::Relation;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::types::Any;
use dialog_query::{AttributeQuery, Conclusion, Fold, FoldConclusion, Output as _, Term, the};
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn read(the: Term<Relation>, is: &str, cause: &str) -> AttributeQuery {
    AttributeQuery::new(
        the,
        Term::<Entity>::var("t"),
        Term::<Any>::var(is),
        Term::var(cause),
        None,
    )
}

/// Each to-do with a title, its inputs of `the` folded by `aggregator`.
fn folded(the: Term<Relation>, aggregator: Aggregator) -> Fold<AttributeQuery, AttributeQuery> {
    folded_by(the, ReduceEntry::new("tags", aggregator, Term::var("g")))
}

/// Each to-do with a title, its inputs of `the` folded by `entry`, whose
/// field is `tags` and whose input is `?g`.
fn folded_by(the: Term<Relation>, entry: ReduceEntry) -> Fold<AttributeQuery, AttributeQuery> {
    Fold::new(
        read(Term::from(the!("todo/title")), "title", "c1"),
        read(the, "g", "c2"),
        Reduce::new(vec!["t".into()], vec![entry]),
    )
}

/// Each to-do with a title, and how many tags it has.
fn counted() -> Fold<AttributeQuery, AttributeQuery> {
    folded(Term::from(the!("todo/tag")), Aggregator::Count)
}

fn counts<'a>(rows: impl IntoIterator<Item = &'a FoldConclusion>) -> BTreeMap<Entity, Value> {
    rows.into_iter()
        .filter_map(|row| Some((row.this().clone(), row.get("tags")?.clone())))
        .collect()
}

async fn opened() -> anyhow::Result<(Operator, Branch)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    Ok((operator, branch))
}

/// Selected through a branch, a fold gives each group, and 0 for the
/// to-do with no tag; under a standing fold a tag added and a tag taken
/// away each report the count that started to hold and the one that
/// stopped, and neither asks either of the fold's queries again whole:
/// each inner subscription maintains its rows and the one group moved is
/// folded again in memory.
#[dialog_common::test]
async fn it_counts_through_a_branch_and_under_a_subscription() -> anyhow::Result<()> {
    let (operator, branch) = opened().await?;
    let one: Entity = "id:todo-1".parse()?;
    let two: Entity = "id:todo-2".parse()?;
    branch
        .transaction()
        .assert(
            the!("todo/title")
                .of(one.clone())
                .is("Buy milk".to_string()),
        )
        .assert(the!("todo/tag").of(one.clone()).is("home".to_string()))
        .assert(
            the!("todo/title")
                .of(two.clone())
                .is("Call mum".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let selected = branch
        .query()
        .select(counted())
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(
        counts(&selected),
        BTreeMap::from([
            (one.clone(), Value::UnsignedInt(1)),
            (two.clone(), Value::UnsignedInt(0)),
        ])
    );

    let mut subscription = branch.subscribe_fold(counted());
    // The poll future is `Send` on native, as a subscription's is: the
    // compile is the assertion.
    fn assert_send<T: dialog_common::ConditionalSend>(_: T) {}
    assert_send(subscription.poll(&operator));
    let first = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(counts(&first.asserted), counts(&selected));
    assert!(first.retracted.is_empty());
    let before = subscription.recomputes();
    assert_eq!(before, 2, "the first poll evaluates each inner query once");

    branch
        .transaction()
        .assert(the!("todo/tag").of(two.clone()).is("call".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a tag added");
    assert_eq!(
        counts(&delta.asserted),
        BTreeMap::from([(two.clone(), Value::UnsignedInt(1))])
    );
    assert_eq!(
        counts(&delta.retracted),
        BTreeMap::from([(two.clone(), Value::UnsignedInt(0))])
    );

    branch
        .transaction()
        .retract(the!("todo/tag").of(one.clone()).is("home".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription
        .poll(&operator)
        .await?
        .expect("a tag taken away");
    assert_eq!(
        counts(&delta.asserted),
        BTreeMap::from([(one.clone(), Value::UnsignedInt(0))])
    );
    assert_eq!(
        counts(&delta.retracted),
        BTreeMap::from([(one.clone(), Value::UnsignedInt(1))])
    );
    assert_eq!(
        subscription.recomputes(),
        before,
        "neither later poll evaluated an inner query whole"
    );
    assert_eq!(subscription.maintenances(), 2);
    assert!(subscription.poll(&operator).await?.is_none());
    Ok(())
}

/// A value a randomized step asserts or retracts.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum Datum {
    Text(String),
    Integer(i64),
    Float(f64),
}

/// A small deterministic generator (xorshift64*).
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())].clone()
    }
}

/// One change of a step: a title (which makes a group hold) or an input,
/// asserted or retracted.
#[derive(Clone, Debug)]
struct Move {
    assert: bool,
    title: bool,
    of: usize,
    is: Datum,
}

/// The facts asserted now, as `(title, of, is)`.
type Present = Vec<(bool, usize, Datum)>;

fn entity(of: usize) -> Entity {
    format!("id:todo-{of}").parse().expect("an entity")
}

async fn commit(
    branch: &Branch,
    operator: &Operator,
    the: &Relation,
    moves: &[Move],
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for change in moves {
        let attribute = if change.title {
            the!("todo/title")
        } else {
            the.clone()
        };
        let of = entity(change.of);
        transaction = match (&change.is, change.assert) {
            (Datum::Text(is), true) => transaction.assert(attribute.of(of).is(is.clone())),
            (Datum::Text(is), false) => transaction.retract(attribute.of(of).is(is.clone())),
            (Datum::Integer(is), true) => transaction.assert(attribute.of(of).is(*is)),
            (Datum::Integer(is), false) => transaction.retract(attribute.of(of).is(*is)),
            (Datum::Float(is), true) => transaction.assert(attribute.of(of).is(*is)),
            (Datum::Float(is), false) => transaction.retract(attribute.of(of).is(*is)),
        };
    }
    if moves.is_empty() {
        // A transaction that moves nothing the fold reads.
        transaction = transaction.assert(the!("todo/note").of(entity(0)).is("unread".to_string()));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// A folded row as exact text: its entity and the `Debug` of every
/// value, which writes a float as the shortest text that reads back as
/// the same bits, so two rows are the same text only when they are the
/// same row to the bit.
fn exact(row: &FoldConclusion) -> String {
    format!("{} {:?}", row.this(), row.values())
}

/// Apply the moves to the model of what is asserted.
fn record(present: &mut Present, moves: &[Move]) {
    for change in moves {
        let fact = (change.title, change.of, change.is.clone());
        if change.assert {
            if !present.contains(&fact) {
                present.push(fact);
            }
        } else {
            present.retain(|held| *held != fact);
        }
    }
}

/// A retraction of something asserted now, `title` or input, if any is.
fn retraction(random: &mut Random, present: &Present, title: bool) -> Option<Move> {
    let candidates: Vec<_> = present.iter().filter(|fact| fact.0 == title).collect();
    if candidates.is_empty() {
        return None;
    }
    let (title, of, is) = candidates[random.below(candidates.len())].clone();
    Some(Move {
        assert: false,
        title,
        of,
        is,
    })
}

/// Random asserts and retracts of titles and of the inputs of `the`,
/// over 20 to-dos, with `values` the inputs drawn from; after every
/// poll the answer the standing fold has reported, delta by delta,
/// equals a select of the same fold, row for row and bit for bit. It
/// opens with what the random part might not reach soon: inputs before
/// their group, a group with no inputs, a group that stops holding and
/// comes back, a group with two titles, two changes in one transaction
/// and a transaction that moves nothing the fold reads.
async fn equivalence(
    the: Relation,
    aggregator: Aggregator,
    values: &[Datum],
    seed: u64,
) -> anyhow::Result<()> {
    let entry = ReduceEntry::new("tags", aggregator, Term::var("g"));
    equivalence_of(the, entry, values, seed).await
}

/// [`equivalence`], folding with `entry` (its field `tags`, its input
/// `?g`).
async fn equivalence_of(
    the: Relation,
    entry: ReduceEntry,
    values: &[Datum],
    seed: u64,
) -> anyhow::Result<()> {
    const STEPS: usize = 200;
    const TODOS: usize = 20;
    let aggregator = entry.aggregator;
    let (operator, branch) = opened().await?;
    let fold = folded_by(Term::from(the.clone()), entry);
    let mut subscription = branch.subscribe_fold(fold.clone());
    let mut random = Random(seed);
    let titles = [
        Datum::Text("Buy milk".into()),
        Datum::Text("Call mum".into()),
    ];
    let input = |of: usize, is: Datum, assert: bool| Move {
        assert,
        title: false,
        of,
        is,
    };
    let title = |of: usize, is: Datum, assert: bool| Move {
        assert,
        title: true,
        of,
        is,
    };
    let mut script: Vec<Vec<Move>> = vec![
        // Inputs before their group: to-do 0 has two, and no title.
        vec![
            input(0, values[0].clone(), true),
            input(0, values[1].clone(), true),
        ],
        // To-do 0 starts to hold, and to-do 1 holds with no input.
        vec![
            title(0, titles[0].clone(), true),
            title(1, titles[0].clone(), true),
        ],
        // Nothing the fold reads.
        vec![],
        // To-do 0 stops holding, its inputs stay, and it comes back.
        vec![title(0, titles[0].clone(), false)],
        vec![title(0, titles[1].clone(), true)],
        // A second title: the group holds twice, each row joined.
        vec![title(0, titles[0].clone(), true)],
    ];
    script.reverse();
    let mut present: Present = Vec::new();
    let mut answer: Vec<FoldConclusion> = Vec::new();
    let mut refused = 0;
    for step in 0..STEPS {
        let moves = match script.pop() {
            Some(moves) => moves,
            None => {
                let of = random.below(TODOS);
                match random.below(10) {
                    0 | 1 => vec![title(of, random.pick(&titles), true)],
                    2 => retraction(&mut random, &present, true)
                        .into_iter()
                        .collect(),
                    3..=5 => vec![input(of, random.pick(values), true)],
                    6 | 7 => retraction(&mut random, &present, false)
                        .into_iter()
                        .collect(),
                    // Two changes in one transaction: a to-do with its
                    // first input, or one input for another.
                    8 => vec![
                        title(of, random.pick(&titles), true),
                        input(of, random.pick(values), true),
                    ],
                    _ => match retraction(&mut random, &present, false) {
                        Some(gone) => {
                            let mut added = random.pick(values);
                            while added == gone.is {
                                added = random.pick(values);
                            }
                            vec![input(gone.of, added, true), gone]
                        }
                        None => Vec::new(),
                    },
                }
            }
        };
        commit(&branch, &operator, &the, &moves).await?;
        record(&mut present, &moves);
        let polled = subscription.poll(&operator).await;
        let selected = branch
            .query()
            .select(fold.clone())
            .perform(&operator)
            .try_vec()
            .await;
        // A group a fold refuses (a `max` of incomparable values, say)
        // fails both the same way, and a later poll folds it again.
        let (delta, selected) = match (polled, selected) {
            (Ok(delta), Ok(selected)) => (delta, selected),
            (Err(polled), Err(selected)) => {
                assert_eq!(polled, selected, "step {step}: they fail alike");
                refused += 1;
                continue;
            }
            (polled, selected) => {
                panic!("step {step}: the poll gave {polled:?} and the select {selected:?}")
            }
        };
        if let Some(delta) = delta {
            for row in &delta.retracted {
                let at = answer
                    .iter()
                    .position(|held| held == row)
                    .unwrap_or_else(|| panic!("step {step}: retracted {row:?}, never asserted"));
                answer.swap_remove(at);
            }
            for row in delta.asserted {
                assert!(
                    !answer.contains(&row),
                    "step {step}: asserted {row:?} twice"
                );
                answer.push(row);
            }
        }
        let kept: BTreeSet<String> = answer.iter().map(exact).collect();
        let expected: BTreeSet<String> = selected.iter().map(exact).collect();
        assert_eq!(kept.len(), answer.len());
        assert_eq!(expected.len(), selected.len());
        assert_eq!(
            kept, expected,
            "step {step} ({aggregator}, moves {moves:?}): the standing fold differs from a select"
        );
        let held: BTreeSet<String> = subscription.results().map(exact).collect();
        assert_eq!(held, expected, "step {step}: the rows it holds");
    }
    assert!(
        refused < STEPS / 2,
        "{refused} of {STEPS} steps refused: the comparison compared little"
    );
    if aggregator.orders_by_standing() {
        // A fold that orders by standing is evaluated afresh on every
        // poll a row moved in: the inner queries' first polls, and more.
        assert!(
            subscription.recomputes() > 2,
            "a fold under `{aggregator}` was evaluated afresh"
        );
        assert_eq!(subscription.refolds(), 0, "no group folded from kept rows");
    } else {
        assert_eq!(
            subscription.recomputes(),
            2,
            "every later poll maintained both inner queries"
        );
    }
    Ok(())
}

fn texts() -> Vec<Datum> {
    ["home", "errand", "work", "call"]
        .into_iter()
        .map(|text| Datum::Text(text.to_string()))
        .collect()
}

fn integers() -> Vec<Datum> {
    [3, -7, 0, 12, -1, 40]
        .into_iter()
        .map(Datum::Integer)
        .collect()
}

/// Integers no fold refuses to order: a stored natural reads back
/// unsigned and a negative signed, and `max` does not order the two.
fn naturals() -> Vec<Datum> {
    [3, 0, 12, 40, 7, 25]
        .into_iter()
        .map(Datum::Integer)
        .collect()
}

/// Floats whose sum depends on the order they are added in: the ulp of
/// 1e15 is 0.125, so 1e15 + 0.5 + 0.1 rounds the 0.1, and only a fold
/// that adds a group's values in one order whatever order its rows come
/// in gives one sum. None is integral: a stored float with an integral
/// value, 1e16 say, reads back as an integer, and a sum refuses to mix
/// the two.
fn floats() -> Vec<Datum> {
    [0.1, 0.2, 1e15 + 0.5, -(1e15 + 0.5), 0.3, 2.5]
        .into_iter()
        .map(Datum::Float)
        .collect()
}

/// `last` over tags: the tag bound from the newest fact. A retraction
/// of the tag it chose folds the group again over the tags left, and
/// the newest of them stands.
#[dialog_common::test]
async fn it_keeps_the_newest_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/tag"), Aggregator::Last, &texts(), 0x5eed_0009).await
}

/// `top` over tags, ranked by a listing best first. A retraction of the
/// tag it chose folds the group again over the tags left, and the best
/// listed of them stands.
#[dialog_common::test]
async fn it_keeps_the_top_listed_equal_to_a_select() -> anyhow::Result<()> {
    let mut entry = ReduceEntry::new("tags", Aggregator::Top, Term::var("g"));
    entry.listed = ["work", "call", "errand", "home"]
        .into_iter()
        .map(|text| Value::String(text.to_string()))
        .collect();
    equivalence_of(the!("todo/tag"), entry, &texts(), 0x5eed_000a).await
}

/// `top` over tags whose listing leaves two of them out: among the
/// unlisted ones the newest stands, which only rows carrying their
/// standing can tell, so the standing fold still equals a select.
#[dialog_common::test]
async fn it_keeps_the_top_of_a_partial_listing_equal_to_a_select() -> anyhow::Result<()> {
    let mut entry = ReduceEntry::new("tags", Aggregator::Top, Term::var("g"));
    entry.listed = ["work", "call"]
        .into_iter()
        .map(|text| Value::String(text.to_string()))
        .collect();
    equivalence_of(the!("todo/tag"), entry, &texts(), 0x5eed_000b).await
}

/// A `sum` is maintained per group: after its first poll no poll
/// evaluates anything whole, and only the moved group is folded again.
/// A `last` over the same changes is evaluated afresh on each poll that
/// moved a row, and folds no group from kept rows.
#[dialog_common::test]
async fn it_maintains_a_sum_per_group_and_evaluates_a_last_afresh() -> anyhow::Result<()> {
    let (operator, branch) = opened().await?;
    let mut sum = branch.subscribe_fold(folded(Term::from(the!("todo/size")), Aggregator::Sum));
    let mut last = branch.subscribe_fold(folded(Term::from(the!("todo/size")), Aggregator::Last));
    let one = || Move {
        assert: true,
        title: true,
        of: 1,
        is: Datum::Text("Buy milk".into()),
    };
    let size = |is: i64, assert: bool| Move {
        assert,
        title: false,
        of: 1,
        is: Datum::Integer(is),
    };
    let steps = [
        vec![one()],
        vec![size(3, true)],
        vec![size(5, true)],
        vec![size(5, false)],
    ];
    for (step, moves) in steps.iter().enumerate() {
        commit(&branch, &operator, &the!("todo/size"), moves).await?;
        sum.poll(&operator).await?;
        last.poll(&operator).await?;
        if step == 0 {
            assert_eq!(sum.recomputes(), 2, "the first poll evaluates each query");
        }
    }
    assert_eq!(sum.recomputes(), 2, "every later poll was maintained");
    assert_eq!(sum.refolds(), 4, "the one group, folded once per poll");
    assert_eq!(
        last.recomputes(),
        2 + steps.len(),
        "each poll evaluated the fold afresh"
    );
    assert_eq!(last.refolds(), 0);
    let newest: Vec<_> = last.results().filter_map(|row| row.get("tags")).collect();
    assert_eq!(
        newest,
        vec![&Value::UnsignedInt(3)],
        "5 taken back, 3 stands"
    );
    Ok(())
}

/// `count` over tags.
#[dialog_common::test]
async fn it_keeps_a_count_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/tag"), Aggregator::Count, &texts(), 0x5eed_0001).await
}

/// `count-distinct` over tags.
#[dialog_common::test]
async fn it_keeps_a_distinct_count_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(
        the!("todo/tag"),
        Aggregator::CountDistinct,
        &texts(),
        0x5eed_0002,
    )
    .await
}

/// `sum` over integers, negative ones among them.
#[dialog_common::test]
async fn it_keeps_an_integer_sum_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/size"), Aggregator::Sum, &integers(), 0x5eed_0004).await
}

/// `sum` over floats, to the bit.
#[dialog_common::test]
async fn it_keeps_a_float_sum_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/weight"), Aggregator::Sum, &floats(), 0x5eed_0005).await
}

/// `avg` over floats, to the bit.
#[dialog_common::test]
async fn it_keeps_a_float_average_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/weight"), Aggregator::Avg, &floats(), 0x5eed_0006).await
}

/// `min` over tags.
#[dialog_common::test]
async fn it_keeps_a_minimum_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/tag"), Aggregator::Min, &texts(), 0x5eed_0007).await
}

/// `max` over integers.
#[dialog_common::test]
async fn it_keeps_a_maximum_equal_to_a_select() -> anyhow::Result<()> {
    equivalence(the!("todo/size"), Aggregator::Max, &naturals(), 0x5eed_0008).await
}

/// A tag added to one of 200 to-dos folds that one group again, and no
/// other: the first poll folds all 200, the next one.
#[dialog_common::test]
async fn it_folds_only_the_group_a_change_moves() -> anyhow::Result<()> {
    const TODOS: usize = 200;
    let (operator, branch) = opened().await?;
    let mut transaction = branch.transaction();
    for of in 0..TODOS {
        transaction = transaction
            .assert(the!("todo/title").of(entity(of)).is(format!("Task {of}")))
            .assert(the!("todo/tag").of(entity(of)).is("home".to_string()));
    }
    transaction.commit().publish().perform(&operator).await?;

    let mut subscription = branch.subscribe_fold(counted());
    let first = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(first.asserted.len(), TODOS);
    assert_eq!(subscription.refolds(), TODOS);

    branch
        .transaction()
        .assert(the!("todo/tag").of(entity(77)).is("errand".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a tag added");
    assert_eq!(
        subscription.refolds(),
        TODOS + 1,
        "one group folded again, no other"
    );
    assert_eq!(
        counts(&delta.asserted),
        BTreeMap::from([(entity(77), Value::UnsignedInt(2))])
    );
    assert_eq!(
        counts(&delta.retracted),
        BTreeMap::from([(entity(77), Value::UnsignedInt(1))])
    );
    assert_eq!(subscription.recomputes(), 2);
    Ok(())
}
