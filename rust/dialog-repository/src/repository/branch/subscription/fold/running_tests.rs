//! A standing fold whose groups keep a running value: the to-dos of a
//! list, each with a value, folded per list. A to-do moves between lists,
//! several to-dos of a list hold one value, and a list appears and
//! vanishes, so every step a running value takes is met; after every
//! poll the answer equals a select of the same fold, and each delta is
//! the difference between the answer before it and after.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::{BTreeMap, BTreeSet};

use crate::Branch;
use crate::helpers::test_repo;
use dialog_corpus::{Entity, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::types::Any;
use dialog_query::{
    AttributeQuery, ConceptDescriptor, ConceptQuery, Conclusion as _, Fold, FoldConclusion,
    Output as _, Parameters, Term, the,
};
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

async fn opened() -> anyhow::Result<(Operator, Branch)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    Ok((operator, branch))
}

/// What a to-do's value is.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    /// `todo/tag`, text.
    Text,
    /// `todo/size`, a natural.
    Natural,
    /// `todo/score`, an integer, negative ones among them.
    Integer,
}

/// A value a to-do holds.
#[derive(Clone, Debug, PartialEq)]
enum Held {
    Text(&'static str),
    Integer(i64),
}

impl Kind {
    fn relation(self) -> &'static str {
        match self {
            Kind::Text => "todo/tag",
            Kind::Natural => "todo/size",
            Kind::Integer => "todo/score",
        }
    }

    fn content(self) -> &'static str {
        match self {
            Kind::Text => "text:",
            Kind::Natural => "natural:",
            Kind::Integer => "integer:",
        }
    }

    /// Few values, so that several to-dos of a list hold one.
    fn values(self) -> Vec<Held> {
        match self {
            Kind::Text => ["home", "errand", "work"]
                .into_iter()
                .map(Held::Text)
                .collect(),
            Kind::Natural => [3, 0, 12, 7].into_iter().map(Held::Integer).collect(),
            Kind::Integer => [3, -7, 0, 12, -1].into_iter().map(Held::Integer).collect(),
        }
    }
}

/// A to-do of a list, with a value: `?t` of list `?l`, holding `?v`.
fn todos(kind: Kind) -> anyhow::Result<ConceptQuery> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": {
            "list": { "the": "todo/list", "as": "entity:", "cardinality": "one" },
            "value": { "the": kind.relation(), "as": kind.content(), "cardinality": "one" },
        }
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("t"));
    terms.insert("list".into(), Term::var("l"));
    terms.insert("value".into(), Term::var("v"));
    Ok(ConceptQuery { predicate, terms })
}

/// Each named list, with its to-dos' values folded by `aggregator` over
/// `input` (`?v` the value, `?t` the to-do).
fn lists(
    kind: Kind,
    aggregator: Aggregator,
    input: &str,
) -> anyhow::Result<Fold<AttributeQuery, ConceptQuery>> {
    Ok(Fold::new(
        AttributeQuery::new(
            Term::from(the!("list/name")),
            Term::<Entity>::var("l"),
            Term::<Any>::var("name"),
            Term::var("named"),
            None,
        ),
        todos(kind)?,
        Reduce::new(
            vec!["l".into()],
            vec![ReduceEntry::new("out", aggregator, Term::var(input))],
        ),
    ))
}

fn todo(of: usize) -> Entity {
    format!("id:todo-{of}").parse().expect("an entity")
}

fn list(of: usize) -> Entity {
    format!("id:list-{of}").parse().expect("an entity")
}

/// One fact asserted or retracted.
#[derive(Clone, Debug)]
enum Fact {
    /// `list/name` of a list.
    Name(usize, bool),
    /// `todo/list` of a to-do.
    In(usize, usize, bool),
    /// The value of a to-do.
    Holds(usize, Held, bool),
}

async fn commit(
    branch: &Branch,
    operator: &Operator,
    kind: Kind,
    facts: &[Fact],
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for fact in facts {
        transaction = match fact.clone() {
            Fact::Name(of, true) => {
                transaction.assert(the!("list/name").of(list(of)).is(format!("List {of}")))
            }
            Fact::Name(of, false) => {
                transaction.retract(the!("list/name").of(list(of)).is(format!("List {of}")))
            }
            Fact::In(of, to, true) => {
                transaction.assert(the!("todo/list").of(todo(of)).is(list(to)))
            }
            Fact::In(of, to, false) => {
                transaction.retract(the!("todo/list").of(todo(of)).is(list(to)))
            }
            Fact::Holds(of, held, assert) => {
                let attribute = match kind {
                    Kind::Text => the!("todo/tag"),
                    Kind::Natural => the!("todo/size"),
                    Kind::Integer => the!("todo/score"),
                };
                match (held, assert) {
                    (Held::Text(is), true) => {
                        transaction.assert(attribute.of(todo(of)).is(is.to_string()))
                    }
                    (Held::Text(is), false) => {
                        transaction.retract(attribute.of(todo(of)).is(is.to_string()))
                    }
                    (Held::Integer(is), true) => transaction.assert(attribute.of(todo(of)).is(is)),
                    (Held::Integer(is), false) => {
                        transaction.retract(attribute.of(todo(of)).is(is))
                    }
                }
            }
        };
    }
    if facts.is_empty() {
        // A transaction that moves nothing the fold reads.
        transaction = transaction.assert(the!("todo/note").of(todo(0)).is("unread".to_string()));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// A folded row as exact text, its entity and every value.
fn exact(row: &FoldConclusion) -> String {
    format!("{} {:?}", row.this(), row.values())
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
}

/// What is asserted now: the named lists, and each to-do's list and
/// value.
#[derive(Default)]
struct Model {
    named: BTreeSet<usize>,
    lists: BTreeMap<usize, usize>,
    values: BTreeMap<usize, Held>,
}

impl Model {
    /// A random commit: name a list or take its name away (a group
    /// appears or vanishes), put a to-do in a list with a value, move a
    /// to-do to another list, replace its value, take its value away, or
    /// take it out of its list; now and then two of these at once.
    fn change(&mut self, random: &mut Random, values: &[Held]) -> Vec<Fact> {
        const LISTS: usize = 3;
        const TODOS: usize = 12;
        let mut facts = Vec::new();
        for _ in 0..1 + usize::from(random.below(4) == 0) {
            let of = random.below(TODOS);
            match random.below(12) {
                0 => {
                    let at = random.below(LISTS);
                    let named = !self.named.contains(&at);
                    if named {
                        self.named.insert(at);
                    } else {
                        self.named.remove(&at);
                    }
                    facts.push(Fact::Name(at, named));
                }
                1..=4 => {
                    let to = random.below(LISTS);
                    if let Some(from) = self.lists.insert(of, to) {
                        // Moved: one commit takes it out of one list and
                        // puts it in another.
                        facts.push(Fact::In(of, from, false));
                    }
                    facts.push(Fact::In(of, to, true));
                    if !self.values.contains_key(&of) {
                        let held = values[random.below(values.len())].clone();
                        self.values.insert(of, held.clone());
                        facts.push(Fact::Holds(of, held, true));
                    }
                }
                5..=7 => {
                    let held = values[random.below(values.len())].clone();
                    if let Some(before) = self.values.insert(of, held.clone()) {
                        if before == held {
                            continue;
                        }
                        facts.push(Fact::Holds(of, before, false));
                    }
                    facts.push(Fact::Holds(of, held, true));
                }
                8 | 9 => {
                    if let Some(before) = self.values.remove(&of) {
                        facts.push(Fact::Holds(of, before, false));
                    }
                }
                _ => {
                    if let Some(from) = self.lists.remove(&of) {
                        facts.push(Fact::In(of, from, false));
                    }
                }
            }
        }
        facts
    }
}

/// Random commits over three lists and twelve to-dos; after every poll
/// the answer the standing fold has reported equals a select of the same
/// fold row for row, the rows it holds equal it too, and the delta it
/// reported is exactly the rows of the select that were not there before
/// and the rows that were and are no more: a group whose row did not
/// change reports nothing. Most groups are given by a running value.
async fn differential(
    kind: Kind,
    aggregator: Aggregator,
    input: &str,
    seed: u64,
) -> anyhow::Result<()> {
    const STEPS: usize = 160;
    let (operator, branch) = opened().await?;
    let fold = lists(kind, aggregator, input)?;
    let mut subscription = branch.subscribe_fold(fold.clone());
    let mut random = Random(seed);
    let mut model = Model::default();
    let values = kind.values();
    // Two named lists, one with no to-do, before the random part.
    let mut script = vec![vec![Fact::Name(0, true), Fact::Name(1, true)]];
    model.named.extend([0, 1]);
    let mut answer: BTreeSet<String> = BTreeSet::new();
    let mut before: BTreeSet<String> = BTreeSet::new();
    for step in 0..STEPS {
        let facts = match script.pop() {
            Some(facts) => facts,
            None => model.change(&mut random, &values),
        };
        commit(&branch, &operator, kind, &facts).await?;
        let delta = subscription.poll(&operator).await?;
        let selected: BTreeSet<String> = branch
            .query()
            .select(fold.clone())
            .perform(&operator)
            .try_vec()
            .await?
            .iter()
            .map(exact)
            .collect();
        let (asserted, retracted) = match delta {
            Some(delta) => (
                delta.asserted.iter().map(exact).collect::<Vec<_>>(),
                delta.retracted.iter().map(exact).collect::<Vec<_>>(),
            ),
            None => (Vec::new(), Vec::new()),
        };
        let started: BTreeSet<String> = selected.difference(&before).cloned().collect();
        let stopped: BTreeSet<String> = before.difference(&selected).cloned().collect();
        assert_eq!(
            asserted.iter().cloned().collect::<BTreeSet<_>>(),
            started,
            "step {step} ({aggregator} of ?{input}, {facts:?}): the rows asserted"
        );
        assert_eq!(
            retracted.iter().cloned().collect::<BTreeSet<_>>(),
            stopped,
            "step {step} ({aggregator} of ?{input}, {facts:?}): the rows retracted"
        );
        assert_eq!(asserted.len(), started.len(), "step {step}: once each");
        assert_eq!(retracted.len(), stopped.len(), "step {step}: once each");
        for row in retracted {
            assert!(answer.remove(&row), "step {step}: {row} never asserted");
        }
        answer.extend(asserted);
        assert_eq!(answer, selected, "step {step}: the answer reported");
        let held: BTreeSet<String> = subscription.results().map(exact).collect();
        assert_eq!(held, selected, "step {step}: the rows it holds");
        before = selected;
    }
    assert_eq!(
        subscription.recomputes(),
        2,
        "every later poll was maintained"
    );
    // A `min` or `max` whose extreme leaves is folded again, which over
    // three lists and a few values is often: it steps, but not mostly.
    let (steps, refolds) = (subscription.steps(), subscription.refolds());
    if matches!(aggregator, Aggregator::Min | Aggregator::Max) {
        assert!(
            steps > STEPS / 10,
            "{aggregator}: {steps} steps, {refolds} folds"
        );
    } else {
        assert!(
            steps > refolds,
            "{aggregator}: {steps} steps, {refolds} folds"
        );
    }
    Ok(())
}

/// `count` of the values of each list's to-dos.
#[dialog_common::test]
async fn it_runs_a_count_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Text, Aggregator::Count, "v", 0x5eed_1001).await
}

/// `count-distinct` of the values of each list's to-dos, several to-dos
/// holding one.
#[dialog_common::test]
async fn it_runs_a_distinct_count_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Text, Aggregator::CountDistinct, "v", 0x5eed_1002).await
}

/// `count-distinct` of each list's to-dos, a view's count.
#[dialog_common::test]
async fn it_runs_a_count_of_todos_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Text, Aggregator::CountDistinct, "t", 0x5eed_1003).await
}

/// `sum` of integers, negative ones among them.
#[dialog_common::test]
async fn it_runs_an_integer_sum_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Integer, Aggregator::Sum, "v", 0x5eed_1004).await
}

/// `max` of naturals: a retracted maximum folds its list again.
#[dialog_common::test]
async fn it_runs_a_maximum_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Natural, Aggregator::Max, "v", 0x5eed_1005).await
}

/// `min` of text.
#[dialog_common::test]
async fn it_runs_a_minimum_equal_to_a_select() -> anyhow::Result<()> {
    differential(Kind::Text, Aggregator::Min, "v", 0x5eed_1006).await
}

/// A standing fold of `aggregator` over `?v`, lists 0 and 1 named and
/// the to-dos `holding` put in list 0, each with its value: all but the
/// last before the first poll, and the last after it, so that the poll
/// that takes it in folds list 0 again and gives it a running value.
async fn standing(
    kind: Kind,
    aggregator: Aggregator,
    holding: &[(usize, Held)],
) -> anyhow::Result<(
    Operator,
    Branch,
    super::FoldSubscription<AttributeQuery, ConceptQuery>,
)> {
    let (operator, branch) = opened().await?;
    let put =
        |(of, held): &(usize, Held)| [Fact::In(*of, 0, true), Fact::Holds(*of, held.clone(), true)];
    let (last, first) = holding.split_last().expect("a to-do");
    let mut facts = vec![Fact::Name(0, true), Fact::Name(1, true)];
    facts.extend(first.iter().flat_map(put));
    commit(&branch, &operator, kind, &facts).await?;
    let mut subscription = branch.subscribe_fold(lists(kind, aggregator, "v")?);
    subscription.poll(&operator).await?.expect("the first poll");
    commit(&branch, &operator, kind, &put(last)).await?;
    subscription.poll(&operator).await?.expect("the last to-do");
    Ok((operator, branch, subscription))
}

/// The value list `of` reports in `rows`, if it reports one.
fn value_of(rows: &[FoldConclusion], of: usize) -> Option<Value> {
    rows.iter()
        .find(|row| row.this() == &list(of))
        .and_then(|row| row.get("out").cloned())
}

/// The last to-do holding a value leaves its list: the distinct count
/// drops by one, given by the running value with no fold.
#[dialog_common::test]
async fn it_drops_a_distinct_count_when_the_last_row_of_a_value_leaves() -> anyhow::Result<()> {
    let holding = [
        (1, Held::Text("home")),
        (2, Held::Text("home")),
        (3, Held::Text("work")),
    ];
    let (operator, branch, mut subscription) =
        standing(Kind::Text, Aggregator::CountDistinct, &holding).await?;
    let refolds = subscription.refolds();
    commit(
        &branch,
        &operator,
        Kind::Text,
        &[Fact::Holds(3, Held::Text("work"), false)],
    )
    .await?;
    let delta = subscription.poll(&operator).await?.expect("a value left");
    assert_eq!(value_of(&delta.retracted, 0), Some(Value::UnsignedInt(2)));
    assert_eq!(value_of(&delta.asserted, 0), Some(Value::UnsignedInt(1)));
    assert_eq!(subscription.refolds(), refolds, "no group folded again");
    Ok(())
}

/// One of two to-dos holding a value leaves its list: the distinct
/// count does not move, and the poll reports nothing for the list.
#[dialog_common::test]
async fn it_keeps_a_distinct_count_when_one_of_two_rows_of_a_value_leaves() -> anyhow::Result<()> {
    let holding = [
        (1, Held::Text("home")),
        (2, Held::Text("home")),
        (3, Held::Text("work")),
    ];
    let (operator, branch, mut subscription) =
        standing(Kind::Text, Aggregator::CountDistinct, &holding).await?;
    let (refolds, steps) = (subscription.refolds(), subscription.steps());
    commit(&branch, &operator, Kind::Text, &[Fact::In(2, 0, false)]).await?;
    let delta = subscription.poll(&operator).await?.expect("a row moved");
    assert!(delta.asserted.is_empty(), "{:?}", delta.asserted);
    assert!(delta.retracted.is_empty(), "{:?}", delta.retracted);
    assert_eq!(subscription.refolds(), refolds, "no group folded again");
    assert_eq!(subscription.steps(), steps + 1, "the list stepped");
    Ok(())
}

/// The current maximum is retracted: its list is folded again, and the
/// next greatest value stands.
#[dialog_common::test]
async fn it_folds_again_when_the_maximum_is_retracted() -> anyhow::Result<()> {
    let holding = [(1, Held::Integer(3)), (2, Held::Integer(12))];
    let (operator, branch, mut subscription) =
        standing(Kind::Natural, Aggregator::Max, &holding).await?;
    let refolds = subscription.refolds();
    commit(
        &branch,
        &operator,
        Kind::Natural,
        &[Fact::Holds(2, Held::Integer(12), false)],
    )
    .await?;
    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the maximum left");
    assert_eq!(value_of(&delta.retracted, 0), Some(Value::UnsignedInt(12)));
    assert_eq!(value_of(&delta.asserted, 0), Some(Value::UnsignedInt(3)));
    assert_eq!(subscription.refolds(), refolds + 1, "the list folded again");
    Ok(())
}

/// A to-do moves from one list to another in one commit: both lists
/// report, each from its running value or folded again, and the counts
/// are the ones a select gives.
#[dialog_common::test]
async fn it_moves_a_row_between_groups_in_one_commit() -> anyhow::Result<()> {
    let holding = [(1, Held::Text("home")), (2, Held::Text("work"))];
    let (operator, branch, mut subscription) =
        standing(Kind::Text, Aggregator::Count, &holding).await?;
    let refolds = subscription.refolds();
    commit(
        &branch,
        &operator,
        Kind::Text,
        &[Fact::In(2, 0, false), Fact::In(2, 1, true)],
    )
    .await?;
    let delta = subscription.poll(&operator).await?.expect("a to-do moved");
    assert_eq!(value_of(&delta.retracted, 0), Some(Value::UnsignedInt(2)));
    assert_eq!(value_of(&delta.asserted, 0), Some(Value::UnsignedInt(1)));
    assert_eq!(value_of(&delta.retracted, 1), Some(Value::UnsignedInt(0)));
    assert_eq!(value_of(&delta.asserted, 1), Some(Value::UnsignedInt(1)));
    // The list it left steps; the one it joined had no input, and a group
    // with none is folded again.
    assert_eq!(subscription.refolds(), refolds + 1);
    let selected = branch
        .query()
        .select(lists(Kind::Text, Aggregator::Count, "v")?)
        .perform(&operator)
        .try_vec()
        .await?;
    let held: BTreeSet<String> = subscription.results().map(exact).collect();
    assert_eq!(held, selected.iter().map(exact).collect());
    Ok(())
}
