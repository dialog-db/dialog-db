//! A round of induction that retracts a claim and asserts one, for one
//! cell.
//!
//! Retracting the old value and asserting the new one is how a rule
//! renames or replaces a record. When the two values coincide in one
//! round, the claim must hold afterwards: retracting it and asserting it
//! again leaves it where it was. The mirror cases pin that a retract
//! alone still removes a claim, an assert alone still adds one, and a
//! retract of one value beside an assert of another leaves the other.
//!
//! Two rules meeting in one round fire in the order of their identities.
//! The outcome must not depend on it, so each case that pairs a retract
//! with an assert runs with the retracting rule firing first and with
//! the asserting rule firing first (see [`Pair`]).
//!
//! Run on the volatile and the filesystem providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[cfg(not(target_arch = "wasm32"))]
use crate::helpers::filesystem_session_with_peer;
use crate::helpers::test_repo;
use crate::{Branch, LayerDeclaration, LayerType};
use anyhow::Result;
use dialog_corpus::{Cardinality, Changes, Entity, Relation, Selector, Update, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::InductiveRule;
use futures_util::StreamExt as _;
use serde_json::{Value as Json, json};

fn var(name: &str) -> Json {
    json!({ "?": { "name": name } })
}

fn entity(name: &str) -> Entity {
    name.parse().expect("a test entity parses")
}

fn relation(the: &str) -> Relation {
    the.parse().expect("a test attribute parses")
}

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn task() -> Entity {
    entity("task:1")
}

fn list() -> Entity {
    entity("list:1")
}

/// A text field under `the`, of `cardinality`.
fn field(the: &str, cardinality: Cardinality) -> Json {
    json!({ "the": the, "as": "text:", "cardinality": cardinality.name() })
}

/// The values `the` of `of` holds on the branch, sorted.
async fn values<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    the: &str,
    of: &Entity,
) -> Result<Vec<Value>> {
    branch.refresh(session).await?;
    let selector = Selector::new().the(relation(the)).of(of.clone());
    let stream = branch.claims().select(selector).perform(session).await?;
    let facts: Vec<_> = stream.collect().await;
    let mut values = facts
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()).map(|fact| fact.is))
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by_key(|value| format!("{value:?}"));
    Ok(values)
}

/// Commits `changes`, the rules in `rules` beside them.
async fn commit<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    rules: &[InductiveRule],
    changes: Changes,
) -> Result<()> {
    let mut transaction = branch.transaction();
    for rule in rules {
        transaction = transaction.assert(rule.clone());
    }
    transaction
        .assert(changes)
        .commit()
        .publish()
        .perform(session)
        .await?;
    branch.refresh(session).await?;
    Ok(())
}

/// Dispatches `command`: the round it starts is the one tick the rules
/// it triggers write in.
async fn dispatch<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    command: Changes,
) -> Result<()> {
    branch
        .transaction()
        .dispatch(command)
        .commit()
        .publish()
        .perform(session)
        .await?;
    branch.refresh(session).await?;
    Ok(())
}

/// Writes `is` as `the` of `of`, under `cardinality`.
fn write(changes: &mut Changes, the: &str, of: &Entity, is: Value, cardinality: Cardinality) {
    changes.associate(relation(the), of.clone(), is, cardinality);
}

/// A command `cmd:1` under `domain`, its fields as given.
fn command(domain: &str, fields: &[(&str, Value)]) -> Changes {
    let mut changes = Changes::new();
    for (name, value) in fields {
        write(
            &mut changes,
            &format!("{domain}/{name}"),
            &entity("cmd:1"),
            value.clone(),
            Cardinality::One,
        );
    }
    changes
}

/// A premise reading a command under `domain`: its fields, each from
/// `relations`, bound as `binds` says.
fn command_premise(domain: &str, relations: &[(&str, &str)], binds: Json) -> Json {
    let mut with = serde_json::Map::new();
    for (name, value_type) in relations {
        with.insert(
            (*name).into(),
            json!({ "the": format!("{domain}/{name}"), "as": value_type }),
        );
    }
    json!({ "assert": { "with": with }, "where": binds })
}

/// A rule retracting and a rule asserting, built for one firing order.
///
/// Rules meeting in a round fire in the order of their identities, and
/// a rule's identity is its content. The command domain both rules read
/// is varied (`cmd.<name><n>`) until the retracting rule's identity sorts
/// before the asserting rule's, or after it, as asked.
struct Pair {
    domain: String,
    retract: InductiveRule,
    assert: InductiveRule,
}

impl Pair {
    fn build(
        name: &str,
        retract_first: bool,
        retract: impl Fn(&str) -> Result<InductiveRule>,
        assert: impl Fn(&str) -> Result<InductiveRule>,
    ) -> Result<Self> {
        for n in 0..256 {
            let domain = format!("cmd.{name}{n}");
            let (retracting, asserting) = (retract(&domain)?, assert(&domain)?);
            if (retracting.this() < asserting.this()) == retract_first {
                return Ok(Pair {
                    domain,
                    retract: retracting,
                    assert: asserting,
                });
            }
        }
        anyhow::bail!("no command domain gives the firing order asked for")
    }

    fn rules(&self) -> Vec<InductiveRule> {
        vec![self.retract.clone(), self.assert.clone()]
    }
}

/// `retract! task{title: ?title} when <domain>{task: ?this}, task{this:
/// ?this, title: ?title}`: retracts the title the task holds.
fn retract_title(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "retract!": { "with": { "title": field("task/title", Cardinality::One) } },
        "when": [
            command_premise(domain, &[("task", "entity:")], json!({ "task": var("this") })),
            {
                "assert": { "with": { "title": field("task/title", Cardinality::One) } },
                "where": { "this": var("this"), "title": var("title") }
            }
        ]
    }))?)
}

/// `assert! task{title: ?title} when <domain>{task: ?this, title:
/// ?title}`: asserts the title the command names.
fn assert_title(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "title": field("task/title", Cardinality::One) } },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), ("title", "text:")],
                json!({ "task": var("this"), "title": var("title") }),
            )
        ]
    }))?)
}

/// Renames `task:1`, titled `from`, to `to` in one tick: a rule
/// retracting the old title and a rule asserting the new one, firing in
/// the order asked. What the task is titled afterwards.
async fn rename<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
    from: &str,
    to: &str,
    retract_first: bool,
) -> Result<Vec<Value>> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let pair = Pair::build("rename", retract_first, retract_title, assert_title)?;

    let mut ground = Changes::new();
    write(
        &mut ground,
        "task/title",
        &task(),
        text(from),
        Cardinality::One,
    );
    commit(&branch, session, &pair.rules(), ground).await?;
    dispatch(
        &branch,
        session,
        command(
            &pair.domain,
            &[("task", Value::Entity(task())), ("title", text(to))],
        ),
    )
    .await?;
    values(&branch, session, "task/title", &task()).await
}

/// Case 1: renamed to the title it already has, the task keeps it.
async fn rename_to_the_same_title<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for retract_first in [true, false] {
        assert_eq!(
            rename(session, peer, "Buy milk", "Buy milk", retract_first).await?,
            vec![text("Buy milk")],
            "renamed to its own title, the task keeps it (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// Renamed to another title, the task holds the new one alone.
async fn rename_to_another_title<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for retract_first in [true, false] {
        assert_eq!(
            rename(session, peer, "Buy milk", "Buy oat milk", retract_first).await?,
            vec![text("Buy oat milk")],
            "renamed, the task holds the new title alone (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// A retract with no assert beside it removes the claim.
async fn retract_alone<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let domain = "cmd.clear";
    let mut ground = Changes::new();
    write(
        &mut ground,
        "task/title",
        &task(),
        text("Buy milk"),
        Cardinality::One,
    );
    commit(&branch, session, &[retract_title(domain)?], ground).await?;
    dispatch(
        &branch,
        session,
        command(domain, &[("task", Value::Entity(task()))]),
    )
    .await?;
    assert_eq!(
        values(&branch, session, "task/title", &task()).await?,
        Vec::<Value>::new(),
        "a retract alone removes the title"
    );
    Ok(())
}

/// An assert with no retract beside it adds the claim; under
/// cardinality one it replaces the value held.
async fn assert_alone<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let domain = "cmd.title";
    commit(&branch, session, &[assert_title(domain)?], Changes::new()).await?;

    dispatch(
        &branch,
        session,
        command(
            domain,
            &[("task", Value::Entity(task())), ("title", text("Buy milk"))],
        ),
    )
    .await?;
    assert_eq!(
        values(&branch, session, "task/title", &task()).await?,
        vec![text("Buy milk")],
        "an assert alone adds the title"
    );

    dispatch(
        &branch,
        session,
        command(
            domain,
            &[
                ("task", Value::Entity(task())),
                ("title", text("Buy oat milk")),
            ],
        ),
    )
    .await?;
    assert_eq!(
        values(&branch, session, "task/title", &task()).await?,
        vec![text("Buy oat milk")],
        "an assert alone of cardinality one replaces the title held"
    );
    Ok(())
}

/// `retract! task{tag: ?tag} when <domain>{task: ?this, from: ?tag},
/// task{this: ?this, tag: ?tag}`: retracts the tag the command names
/// from among the task's tags.
fn retract_tag(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "retract!": { "with": { "tag": field("task/tag", Cardinality::Many) } },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), ("from", "text:")],
                json!({ "task": var("this"), "from": var("tag") }),
            ),
            {
                "assert": { "with": { "tag": field("task/tag", Cardinality::Many) } },
                "where": { "this": var("this"), "tag": var("tag") }
            }
        ]
    }))?)
}

/// `assert! task{tag: ?tag} when <domain>{task: ?this, to: ?tag}`: adds
/// the tag the command names.
fn assert_tag(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "tag": field("task/tag", Cardinality::Many) } },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), ("to", "text:")],
                json!({ "task": var("this"), "to": var("tag") }),
            )
        ]
    }))?)
}

/// Retags `task:1`, tagged `errand` and `home`, from `from` to `to` in
/// one tick, the rules firing in the order asked. The task's tags
/// afterwards.
async fn retag<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
    from: &str,
    to: &str,
    retract_first: bool,
) -> Result<Vec<Value>> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let pair = Pair::build("retag", retract_first, retract_tag, assert_tag)?;

    let mut ground = Changes::new();
    for tag in ["errand", "home"] {
        write(
            &mut ground,
            "task/tag",
            &task(),
            text(tag),
            Cardinality::Many,
        );
    }
    commit(&branch, session, &pair.rules(), ground).await?;
    dispatch(
        &branch,
        session,
        command(
            &pair.domain,
            &[
                ("task", Value::Entity(task())),
                ("from", text(from)),
                ("to", text(to)),
            ],
        ),
    )
    .await?;
    values(&branch, session, "task/tag", &task()).await
}

/// Under cardinality many, a retract of one value beside an assert of
/// another leaves the other and not the one.
async fn retag_to_another_tag<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for retract_first in [true, false] {
        assert_eq!(
            retag(session, peer, "errand", "work", retract_first).await?,
            vec![text("home"), text("work")],
            "retagged, the task holds the new tag and not the old (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// Under cardinality many, a retract of a value beside an assert of the
/// same value leaves the value.
async fn retag_to_the_same_tag<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for retract_first in [true, false] {
        assert_eq!(
            retag(session, peer, "errand", "errand", retract_first).await?,
            vec![text("errand"), text("home")],
            "retagged to the tag it has, the task keeps it (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// The fields of a task record, each of cardinality one.
fn record() -> Json {
    json!({
        "title": field("task/title", Cardinality::One),
        "state": field("task/state", Cardinality::One),
        "list": { "the": "task/list", "as": "entity:" }
    })
}

/// `retract! task{title, state, list} when <domain>{task: ?this},
/// task{this: ?this, title: ?title, state: ?state, list: ?list}`:
/// retracts the record, every field named.
fn retract_record(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "retract!": { "with": record() },
        "when": [
            command_premise(domain, &[("task", "entity:")], json!({ "task": var("this") })),
            {
                "assert": { "with": record() },
                "where": {
                    "this": var("this"),
                    "title": var("title"),
                    "state": var("state"),
                    "list": var("list")
                }
            }
        ]
    }))?)
}

/// `assert! task{title, state, list} when <domain>{task: ?this, state:
/// ?state}, task{this: ?this, title: ?title, list: ?list}`: asserts the
/// record with the state the command names.
fn assert_record(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": record() },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), ("state", "text:")],
                json!({ "task": var("this"), "state": var("state") }),
            ),
            {
                "assert": {
                    "with": {
                        "title": field("task/title", Cardinality::One),
                        "list": { "the": "task/list", "as": "entity:" }
                    }
                },
                "where": { "this": var("this"), "title": var("title"), "list": var("list") }
            }
        ]
    }))?)
}

/// Case 2: a record retracted whole and asserted with its state changed
/// keeps every field, the state changed.
async fn replace_a_record<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    for retract_first in [true, false] {
        let repo = test_repo(session, peer).await;
        let branch = repo.branch("main").open().perform(session).await?;
        let pair = Pair::build("complete", retract_first, retract_record, assert_record)?;

        let mut ground = Changes::new();
        write(
            &mut ground,
            "task/title",
            &task(),
            text("Buy milk"),
            Cardinality::One,
        );
        write(
            &mut ground,
            "task/state",
            &task(),
            text("open"),
            Cardinality::One,
        );
        write(
            &mut ground,
            "task/list",
            &task(),
            Value::Entity(list()),
            Cardinality::One,
        );
        commit(&branch, session, &pair.rules(), ground).await?;
        dispatch(
            &branch,
            session,
            command(
                &pair.domain,
                &[("task", Value::Entity(task())), ("state", text("done"))],
            ),
        )
        .await?;

        let record = (
            values(&branch, session, "task/title", &task()).await?,
            values(&branch, session, "task/state", &task()).await?,
            values(&branch, session, "task/list", &task()).await?,
        );
        assert_eq!(
            record,
            (
                vec![text("Buy milk")],
                vec![text("done")],
                vec![Value::Entity(list())]
            ),
            "the replaced record keeps its title and list, its state done (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// No rule: one transaction retracting a claim the branch holds and
/// asserting it again, in that order, leaves it; asserting it and then
/// retracting it removes it. A cell's writes in one commit take effect
/// in the order written.
async fn direct_writes_in_order<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let the = relation("task/title");
    let mut ground = Changes::new();
    write(
        &mut ground,
        "task/title",
        &task(),
        text("Buy milk"),
        Cardinality::One,
    );
    commit(&branch, session, &[], ground).await?;

    let mut again = Changes::new();
    again.dissociate(the.clone(), task(), text("Buy milk"));
    again.associate(the.clone(), task(), text("Buy milk"), Cardinality::One);
    commit(&branch, session, &[], again).await?;
    assert_eq!(
        values(&branch, session, "task/title", &task()).await?,
        vec![text("Buy milk")],
        "retracted and asserted again in one commit, the title holds"
    );

    let mut undone = Changes::new();
    undone.associate(the.clone(), task(), text("Buy milk"), Cardinality::One);
    undone.dissociate(the, task(), text("Buy milk"));
    commit(&branch, session, &[], undone).await?;
    assert_eq!(
        values(&branch, session, "task/title", &task()).await?,
        Vec::<Value>::new(),
        "asserted and then retracted in one commit, the title is gone"
    );
    Ok(())
}

/// One write of [`three_direct_writes`]: an assertion or a retraction of
/// a title.
#[derive(Clone, Copy, Debug)]
enum Write {
    Assert(&'static str),
    Retract(&'static str),
}

/// `task/title` of every task, as an attribute query reads it.
fn titles() -> dialog_query::AttributeQuery {
    use dialog_query::Term;
    use dialog_query::attribute::Relation as QueryRelation;
    dialog_query::AttributeQuery::from(
        Term::<QueryRelation>::from(QueryRelation::from(relation("task/title")))
            .of(Term::<Entity>::var("this"))
            .is(Term::<String>::var("title")),
    )
}

/// The titles `claims` hold, sorted.
fn read_titles(claims: &[dialog_query::Claim]) -> Vec<Value> {
    let mut titles: Vec<Value> = claims.iter().map(|claim| claim.is.clone()).collect();
    titles.sort_by_key(|value| format!("{value:?}"));
    titles
}

/// No rule: one transaction writing `writes` of the task's title, each
/// its own call, in order, under `cardinality`, over the titles `ground`
/// committed before. What the title reads as three ways, which must
/// agree: the transaction's own query before it commits, the branch's
/// query after, and the claims the branch stores.
async fn three_direct_writes<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
    ground: &[&str],
    writes: [Write; 3],
    cardinality: Cardinality,
) -> Result<Vec<Value>> {
    use dialog_query::query::Output as _;

    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    if !ground.is_empty() {
        let mut held = Changes::new();
        for title in ground {
            write(&mut held, "task/title", &task(), text(title), cardinality);
        }
        commit(&branch, session, &[], held).await?;
    }

    let mut transaction = branch.transaction();
    for step in writes {
        let mut change = Changes::new();
        match step {
            Write::Assert(title) => {
                write(&mut change, "task/title", &task(), text(title), cardinality)
            }
            Write::Retract(title) => change.dissociate(relation("task/title"), task(), text(title)),
        }
        transaction = transaction.assert(change);
    }
    let staged = read_titles(
        &transaction
            .query()
            .select(titles())
            .perform(session)
            .try_vec()
            .await?,
    );
    transaction.commit().publish().perform(session).await?;
    branch.refresh(session).await?;

    let committed = read_titles(
        &branch
            .query()
            .select(titles())
            .perform(session)
            .try_vec()
            .await?,
    );
    let stored = values(&branch, session, "task/title", &task()).await?;
    assert_eq!(
        staged, committed,
        "{writes:?} under {cardinality:?}: the transaction reads what it commits"
    );
    assert_eq!(
        committed, stored,
        "{writes:?} under {cardinality:?}: a query reads what is stored"
    );
    Ok(stored)
}

/// No rule: three writes of one claim in one transaction take effect in
/// the order written, as two do ([`direct_writes_in_order`]). Asserting,
/// retracting and asserting again leaves the claim; with it stored,
/// retracting, asserting and retracting again leaves nothing; and under
/// cardinality one, asserting `A`, `B` and `A` again leaves `A`.
async fn three_direct_writes_in_order<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    use Write::{Assert, Retract};
    for cardinality in [Cardinality::One, Cardinality::Many] {
        assert_eq!(
            three_direct_writes(
                session,
                peer,
                &[],
                [Assert("A"), Retract("A"), Assert("A")],
                cardinality
            )
            .await?,
            vec![text("A")],
            "asserted, retracted and asserted again under {cardinality:?}, the claim holds"
        );
        assert_eq!(
            three_direct_writes(
                session,
                peer,
                &["A"],
                [Retract("A"), Assert("A"), Retract("A")],
                cardinality
            )
            .await?,
            Vec::<Value>::new(),
            "retracted, asserted and retracted again under {cardinality:?}, the claim is gone"
        );
    }
    assert_eq!(
        three_direct_writes(
            session,
            peer,
            &[],
            [Assert("A"), Assert("B"), Assert("A")],
            Cardinality::One
        )
        .await?,
        vec![text("A")],
        "under cardinality one, the last of three assertions holds alone"
    );
    Ok(())
}

/// `assert! task{title: ?title} when <domain>{task: ?this, <name>:
/// ?title}`: asserts the title the command names in its field `name`.
fn assert_title_from(domain: &str, name: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "title": field("task/title", Cardinality::One) } },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), (name, "text:")],
                json!({ "task": var("this"), name: var("title") }),
            )
        ]
    }))?)
}

/// The permutation of `rules` their identities sort them in: the order
/// they fire in when they meet in a round.
fn firing_order(rules: &[InductiveRule]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..rules.len()).collect();
    order.sort_by_key(|at| rules[*at].this());
    order
}

/// On a cardinality-one cell holding `A`, one round in which a rule
/// asserts `A`, another asserts `B` and a third retracts `A`: two
/// claims asserted for one cell, a conflict before any retraction
/// meets it. Every one of the six firing orders the rules can take
/// leaves the cell as the others do, holding `B` alone: the assertion
/// of `B` succeeds `A` whether or not the retraction of `A` is kept.
async fn retract_beside_two_asserts<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let mut seen: Vec<Vec<usize>> = Vec::new();
    for n in 0..512 {
        let domain = format!("cmd.conflict{n}");
        let rules = vec![
            assert_title_from(&domain, "same")?,
            assert_title_from(&domain, "other")?,
            retract_title(&domain)?,
        ];
        let order = firing_order(&rules);
        if seen.contains(&order) {
            continue;
        }
        seen.push(order.clone());

        let repo = test_repo(session, peer).await;
        let branch = repo.branch("main").open().perform(session).await?;
        let mut ground = Changes::new();
        write(
            &mut ground,
            "task/title",
            &task(),
            text("A"),
            Cardinality::One,
        );
        commit(&branch, session, &rules, ground).await?;
        dispatch(
            &branch,
            session,
            command(
                &domain,
                &[
                    ("task", Value::Entity(task())),
                    ("same", text("A")),
                    ("other", text("B")),
                ],
            ),
        )
        .await?;
        assert_eq!(
            values(&branch, session, "task/title", &task()).await?,
            vec![text("B")],
            "the cell holds the other assertion alone (firing order {order:?})"
        );
        if seen.len() == 6 {
            return Ok(());
        }
    }
    anyhow::bail!("only the firing orders {seen:?} were found")
}

/// `assert! task{pinned: "yes"} when task{wanted: "yes"} unless
/// task{pinned: "no"}`: watches `task/pinned` through its `unless`, so
/// a round touching the claim evaluates it.
fn pin() -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "pinned": field("task/pinned", Cardinality::One) } },
        "when": [
            {
                "assert": { "with": { "wanted": field("task/wanted", Cardinality::One) } },
                "where": { "this": var("this"), "wanted": "yes" }
            },
            { "assert": "==", "where": { "this": var("pinned"), "is": "yes" } }
        ],
        "unless": [
            {
                "assert": { "with": { "pinned": field("task/pinned", Cardinality::One) } },
                "where": { "this": var("this"), "pinned": "no" }
            }
        ]
    }))?)
}

/// `retract! task{pinned: ?pinned} when task{pinned: ?pinned}, task{wanted:
/// "yes"}`: takes back the pin wherever the task is wanted.
fn unpin() -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "retract!": { "with": { "pinned": field("task/pinned", Cardinality::One) } },
        "when": [
            {
                "assert": { "with": { "pinned": field("task/pinned", Cardinality::One) } },
                "where": { "this": var("this"), "pinned": var("pinned") }
            },
            {
                "assert": { "with": { "wanted": field("task/wanted", Cardinality::One) } },
                "where": { "this": var("this"), "wanted": "yes" }
            }
        ]
    }))?)
}

/// An `assert!` rule and a `retract!` rule of one claim, both watching
/// its attribute, meeting with the claim held. They retract the old
/// value and assert the new, and the two coincide, so the claim holds
/// and the round writes nothing: the commit settles. (Before, the
/// retraction was kept, the next round asserted the claim again, the
/// one after retracted it, and the commit failed with
/// `InductionDivergence`.)
async fn opposed_rules_settle<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let mut ground = Changes::new();
    write(
        &mut ground,
        "task/pinned",
        &task(),
        text("yes"),
        Cardinality::One,
    );
    commit(&branch, session, &[pin()?, unpin()?], ground).await?;

    let mut wanted = Changes::new();
    write(
        &mut wanted,
        "task/wanted",
        &task(),
        text("yes"),
        Cardinality::One,
    );
    commit(&branch, session, &[], wanted).await?;
    assert_eq!(
        values(&branch, session, "task/pinned", &task()).await?,
        vec![text("yes")],
        "the pin both rules name holds"
    );
    Ok(())
}

/// The layer the layered cases write in.
fn state() -> Entity {
    entity("session:state")
}

/// `task/title` as a head or premise field landing in the state layer.
fn layered_title() -> Json {
    json!({ "the": "task/title", "as": "text:", "in": "session:state" })
}

/// [`retract_title`], its head and premise in the state layer.
fn retract_layered_title(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "retract!": { "with": { "title": layered_title() } },
        "when": [
            command_premise(domain, &[("task", "entity:")], json!({ "task": var("this") })),
            {
                "assert": { "with": { "title": layered_title() } },
                "where": { "this": var("this"), "title": var("title") }
            }
        ]
    }))?)
}

/// [`assert_title`], its head in the state layer.
fn assert_layered_title(domain: &str) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "title": layered_title() } },
        "when": [
            command_premise(
                domain,
                &[("task", "entity:"), ("title", "text:")],
                json!({ "task": var("this"), "title": var("title") }),
            )
        ]
    }))?)
}

/// Renames `task:1`, titled `from` in the state layer, to `to` there in
/// one tick, through a stack, the rules firing in the order asked. What
/// the layer holds of the task's title afterwards.
async fn rename_in_a_layer<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
    from: &str,
    to: &str,
    retract_first: bool,
) -> Result<Vec<Value>> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let pair = Pair::build(
        "relabel",
        retract_first,
        retract_layered_title,
        assert_layered_title,
    )?;
    let mut transaction = branch
        .transaction()
        .assert(LayerDeclaration::new(state(), LayerType::Session));
    for rule in pair.rules() {
        transaction = transaction.assert(rule);
    }
    transaction.commit().publish().perform(session).await?;
    branch.refresh(session).await?;

    let stack = branch.stack().perform(session).await?;
    let mut ground = Changes::new();
    ground.associate_in(
        state(),
        relation("task/title"),
        task(),
        text(from),
        Cardinality::One,
    );
    stack
        .transaction()
        .assert(ground)
        .commit()
        .publish()
        .perform(session)
        .await?;
    stack
        .invoke(command(
            &pair.domain,
            &[("task", Value::Entity(task())), ("title", text(to))],
        ))
        .perform(session)
        .await?;

    let memory = stack
        .layer(&state())
        .and_then(|layer| layer.ephemeral().cloned())
        .ok_or_else(|| anyhow::anyhow!("the state layer"))?;
    let mut held: Vec<Value> = memory
        .scan(&Selector::new().the(relation("task/title")).of(task()))
        .into_iter()
        .map(|fact| fact.is)
        .collect();
    held.sort_by_key(|value| format!("{value:?}"));
    Ok(held)
}

/// In a layer, a rename to the title the task has keeps it, and a
/// rename to another title leaves the new one alone.
async fn rename_in_a_layer_cases<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for retract_first in [true, false] {
        assert_eq!(
            rename_in_a_layer(session, peer, "Buy milk", "Buy milk", retract_first).await?,
            vec![text("Buy milk")],
            "renamed to its own title in the layer, the task keeps it (retract fires first: {retract_first})"
        );
        assert_eq!(
            rename_in_a_layer(session, peer, "Buy milk", "Buy oat milk", retract_first).await?,
            vec![text("Buy oat milk")],
            "renamed in the layer, the task holds the new title alone (retract fires first: {retract_first})"
        );
    }
    Ok(())
}

/// Every case, on one provider.
macro_rules! cases {
    ($provider:ident, $root:ident = $make:expr, $session:expr) => {
        mod $provider {
            use super::*;

            #[dialog_common::test]
            async fn it_keeps_a_title_renamed_to_itself() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                rename_to_the_same_title(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_keeps_a_record_retracted_and_asserted_with_one_field_changed() -> Result<()>
            {
                let $root = $make;
                let (session, peer) = $session;
                replace_a_record(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_renames_to_another_title() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                rename_to_another_title(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_removes_a_title_retracted_alone() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                retract_alone(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_adds_and_replaces_a_title_asserted_alone() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                assert_alone(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_keeps_a_tag_retagged_to_itself() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                retag_to_the_same_tag(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_retags_to_another_tag() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                retag_to_another_tag(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_takes_one_commits_writes_to_a_cell_in_order() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                direct_writes_in_order(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_takes_three_writes_of_one_claim_in_order() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                three_direct_writes_in_order(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_settles_opposed_rules_of_a_held_claim() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                opposed_rules_settle(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_leaves_one_value_whatever_order_two_asserts_and_a_retract_fire_in()
            -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                retract_beside_two_asserts(&session, &peer).await
            }

            #[dialog_common::test]
            async fn it_keeps_a_layered_title_renamed_to_itself() -> Result<()> {
                let $root = $make;
                let (session, peer) = $session;
                rename_in_a_layer_cases(&session, &peer).await
            }
        }
    };
}

// The volatile provider needs no root.
cases!(volatile, _root = (), test_session_with_peer().await);

// Native only: the filesystem provider is the native one. In the browser
// a branch is kept in IndexedDB and OPFS, and the web run of the
// `volatile` cases pins the same scenarios there.
#[cfg(not(target_arch = "wasm32"))]
cases!(
    filesystem,
    root = tempfile::tempdir()?,
    filesystem_session_with_peer(root.path()).await?
);
