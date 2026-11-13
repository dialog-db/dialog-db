//! Writing several facts on one condition, three ways, for
//! `research/rule-heads.md`: one rule whose head is wide, one rule a
//! head with the body written again, and a rule that issues a command
//! the rules of one head read.
//!
//! A `move` command names a to-do, the list it goes to, who moved it
//! and when. The body reads the list the to-do is on, and in the heavy
//! shape that list's owner, the owner's team and the to-do's state. The
//! heads write what the body bound under `out/…`, two of them or four.
//!
//! `heads-…` dispatches a command a to-do in one commit. `heads-each-…`
//! makes a commit a command, `size` of them, over [`TODOS`] to-dos, after
//! one commit off the record that leaves the rules loaded. Two variables
//! vary what a sweep of those runs against, for checking that the store
//! and the warm-up do not decide the result: `DIALOG_PERF_HEADS_TODOS`
//! is how many to-dos are seeded, and `DIALOG_PERF_HEADS_COLD`, set to
//! anything, leaves the commit off the record out.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use anyhow::Result;
use dialog_corpus::{Entity, Pick, Value};
use dialog_query::attribute::expression::dynamic::DynamicAttributeExpression;
use dialog_query::{InductiveRule, Relation};
use dialog_repository::Transient;
use serde_json::{Value as Json, json};

use super::{Outcome, Prepared};
use crate::env::{Env, assert_all, entity, fact, text, var};

/// How many lists the to-dos are spread over.
const LISTS: usize = 50;

/// How many owners the lists are spread over.
const OWNERS: usize = 10;

/// How many teams the owners are spread over.
const TEAMS: usize = 3;

/// How many to-dos a commit of one command is made against, unless
/// `DIALOG_PERF_HEADS_TODOS` says otherwise.
const TODOS: usize = 500;

/// What a head writes, in the order heads are added: the variable the
/// body binds and the attribute it is written under.
const WRITES: [&str; 4] = ["to", "from", "by", "at"];

/// How the writes are spread over rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// One rule, one head of every attribute, the whole body.
    Wide,
    /// One rule a head, each with the whole body.
    Repeat,
    /// One rule issues a command carrying what the body bound, and one
    /// rule a head reads it.
    Named,
    /// No rule at all: what the commit costs before any rule runs.
    None,
    /// No rule and no command: the transaction asserts what the heads
    /// would write. What committing those facts costs before any rule.
    Plain,
}

/// A premise reading `attribute` of the entity `of` into `into`.
fn read(of: &str, attribute: &str, kind: &str, into: &str) -> Json {
    json!({
        "assert": { "with": { "value": { "the": attribute, "as": kind } } },
        "where": { "this": var(of), "value": var(into) }
    })
}

/// What a command carries beside the to-do it is about: all of it, or
/// the list alone when it is narrow.
fn carried(narrow: bool) -> &'static [&'static str] {
    if narrow { &["to"] } else { &["to", "by", "at"] }
}

/// The premises every shape's condition is: the command, the list the
/// to-do is on, and in the heavy body three reads more.
fn body(heavy: bool, narrow: bool) -> Vec<Json> {
    let mut with = serde_json::Map::new();
    let mut bound = serde_json::Map::new();
    with.insert(
        "target".into(),
        json!({ "the": "cmd.move/target", "as": "entity:" }),
    );
    bound.insert("target".into(), var("this"));
    for field in carried(narrow) {
        with.insert(
            (*field).into(),
            json!({ "the": format!("cmd.move/{field}"), "as": "entity:" }),
        );
        bound.insert((*field).into(), var(field));
    }
    let mut premises = vec![
        json!({ "assert": { "with": with }, "where": bound }),
        read("this", "todo/list", "entity:", "from"),
    ];
    if heavy {
        premises.push(read("from", "list/owner", "entity:", "owner"));
        premises.push(read("owner", "owner/team", "entity:", "team"));
        premises.push(read("this", "todo/state", "text:", "state"));
    }
    premises
}

/// A head of the attributes `prefix/<write>` for each of `writes`,
/// each taking the variable of its own name.
fn head(prefix: &str, writes: &[&str]) -> Json {
    let fields: serde_json::Map<String, Json> = writes
        .iter()
        .map(|write| {
            (
                (*write).to_string(),
                json!({ "the": format!("{prefix}/{write}"), "as": "entity:" }),
            )
        })
        .collect();
    json!({ "with": fields })
}

/// The rule that concludes `conclusion` with `polarity` when `premises`
/// hold.
fn induce(polarity: &str, conclusion: Json, premises: Vec<Json>) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        polarity: conclusion,
        "when": premises
    }))?)
}

/// The rules of `shape` writing `heads` facts on a body that is `heavy`
/// or not, and the concept to mark transient when the shape issues a
/// command.
fn rules(
    shape: Shape,
    heavy: bool,
    narrow: bool,
    heads: usize,
) -> Result<(Vec<InductiveRule>, Option<Transient>)> {
    let writes = &WRITES[..heads];
    Ok(match shape {
        Shape::None | Shape::Plain => (Vec::new(), None),
        Shape::Wide => (
            vec![induce("assert!", head("out", writes), body(heavy, narrow))?],
            None,
        ),
        Shape::Repeat => (
            writes
                .iter()
                .map(|write| induce("assert!", head("out", &[write]), body(heavy, narrow)))
                .collect::<Result<_>>()?,
            None,
        ),
        Shape::Named => {
            let issue = induce("assert!", head("moved", writes), body(heavy, narrow))?;
            let transient = Transient(issue.conclusion().this());
            let mut rules = vec![issue];
            for write in writes {
                rules.push(induce(
                    "assert!",
                    head("out", &[write]),
                    vec![read("this", &format!("moved/{write}"), "entity:", write)],
                )?);
            }
            (rules, Some(transient))
        }
    })
}

/// The rules that move a to-do for real: the list it goes to asserted
/// and the list it was on retracted, with the body written again or
/// with the condition named.
fn moving(shape: Shape) -> Result<(Vec<InductiveRule>, Option<Transient>)> {
    let put = |premises| {
        induce(
            "assert!",
            json!({ "with": { "to": { "the": "todo/list", "as": "entity:" } } }),
            premises,
        )
    };
    let take = |premises| {
        induce(
            "retract!",
            json!({ "with": { "from": { "the": "todo/list", "as": "entity:" } } }),
            premises,
        )
    };
    Ok(match shape {
        Shape::Named => {
            let issue = induce(
                "assert!",
                head("moved", &["to", "from"]),
                body(false, false),
            )?;
            let transient = Transient(issue.conclusion().this());
            (
                vec![
                    issue,
                    put(vec![read("this", "moved/to", "entity:", "to")])?,
                    take(vec![read("this", "moved/from", "entity:", "from")])?,
                ],
                Some(transient),
            )
        }
        _ => (
            vec![put(body(false, false))?, take(body(false, false))?],
            None,
        ),
    })
}

/// Seed `size` to-dos, the lists they are on, the lists' owners and the
/// owners' teams.
async fn seed(env: &Env, size: usize) -> Result<()> {
    let todos = (0..size).flat_map(|index| {
        let this = entity("todo", index);
        [
            fact(
                &this,
                "todo/list",
                Value::Entity(entity("list", index % LISTS)),
            ),
            fact(&this, "todo/state", text("open")),
        ]
    });
    let lists = (0..LISTS).map(|index| {
        fact(
            &entity("list", index),
            "list/owner",
            Value::Entity(entity("owner", index % OWNERS)),
        )
    });
    let owners = (0..OWNERS).map(|index| {
        fact(
            &entity("owner", index),
            "owner/team",
            Value::Entity(entity("team", index % TEAMS)),
        )
    });
    env.commit(assert_all(todos.chain(lists).chain(owners), Pick::All))
        .await
}

/// Install `rules`, and mark `transient` when there is one.
async fn install(env: &Env, rules: Vec<InductiveRule>, transient: Option<Transient>) -> Result<()> {
    let mut transaction = env.branch.transaction();
    for rule in rules {
        transaction = transaction.assert(rule);
    }
    if let Some(transient) = transient {
        transaction = transaction.assert(transient);
    }
    transaction
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    Ok(())
}

/// What the command for to-do `index` carries, by field: the to-do, the
/// next list, one of seven movers and a time of its own.
fn command(index: usize) -> [(&'static str, Entity); 4] {
    [
        ("target", entity("todo", index)),
        ("to", entity("list", (index + 1) % LISTS)),
        ("by", entity("user", index % 7)),
        ("at", entity("tick", index)),
    ]
}

/// Dispatch a `move` command for each of the to-dos `todos` in one
/// transaction.
async fn dispatch(env: &Env, todos: Range<usize>, narrow: bool) -> Result<()> {
    let mut transaction = env.branch.transaction();
    for index in todos {
        let of = entity("move", index);
        for (field, value) in command(index) {
            let name = format!("cmd.move/{field}");
            if field != "target" && !carried(narrow).contains(&field) {
                continue;
            }
            transaction = transaction.dispatch(DynamicAttributeExpression {
                the: name.parse::<Relation>()?,
                of: of.clone(),
                is: Value::Entity(value),
                cause: None,
                cardinality: None,
                pick: None,
            });
        }
    }
    transaction
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    Ok(())
}

/// Assert what `heads` heads would write for to-do `index`, in a
/// transaction of its own with no command: each fact under `last`, as a
/// head writes it.
async fn write(env: &Env, index: usize, heads: usize) -> Result<()> {
    let this = entity("todo", index);
    let facts = WRITES[..heads].iter().map(|write| {
        let value = match *write {
            "from" => entity("list", index % LISTS),
            carried => command(index)
                .into_iter()
                .find(|(field, _)| *field == carried)
                .map(|(_, value)| value)
                .expect("a head writes what the command carries or the list it was on"),
        };
        fact(&this, &format!("out/{write}"), Value::Entity(value))
    });
    env.commit(assert_all(facts, Pick::Last)).await
}

struct Heads {
    env: Env,
    size: usize,
    facts: usize,
    narrow: bool,
}

impl Prepared for Heads {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            dispatch(&self.env, 0..self.size, self.narrow).await?;
            Ok(Outcome::from([(
                "facts".into(),
                (self.facts * self.size) as u64,
            )]))
        })
    }
}

/// `heads-<shape>-<body or narrow>-<heads>`: `size` to-dos seeded, the rules of
/// `shape` installed, then one transaction dispatching a `move` command
/// a to-do. The phase is that commit and the induction it runs.
pub async fn writing(
    size: usize,
    shape: Shape,
    heavy: bool,
    narrow: bool,
    heads: usize,
) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    seed(&env, size).await?;
    let (rules, transient) = rules(shape, heavy, narrow, heads)?;
    install(&env, rules, transient).await?;
    Ok(Box::new(Heads {
        env,
        size,
        facts: if shape == Shape::None { 0 } else { heads },
        narrow,
    }))
}

struct Each {
    env: Env,
    commits: usize,
    shape: Shape,
    heads: usize,
    todos: usize,
    cold: bool,
}

impl Each {
    /// The commit for to-do `index`: its command dispatched, or for
    /// [`Shape::Plain`] its facts asserted.
    async fn commit(&self, index: usize) -> Result<()> {
        match self.shape {
            Shape::Plain => write(&self.env, index, self.heads).await,
            _ => dispatch(&self.env, index..index + 1, false).await,
        }
    }
}

impl Prepared for Each {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            for index in 0..self.commits {
                self.commit(index).await?;
            }
            // What the run was made against, beside its counters, so a
            // report says which store and which start it measured.
            self.env.count("heads.todos", self.todos as u64);
            self.env.count("heads.cold", u64::from(self.cold));
            let facts = if self.shape == Shape::None {
                0
            } else {
                self.heads
            };
            Ok(Outcome::from([
                ("commits".into(), self.commits as u64),
                ("facts".into(), (facts * self.commits) as u64),
            ]))
        })
    }
}

/// `commits` commits of one command each over `todos` to-dos, the rules
/// of `shape` installed. Unless `cold`, one commit for the last to-do is
/// made first, off the record, so the measured commits find the rules
/// loaded, as every commit of a running application but its first does.
async fn each_over(
    todos: usize,
    cold: bool,
    commits: usize,
    shape: Shape,
    heavy: bool,
    heads: usize,
) -> Result<Box<dyn Prepared>> {
    anyhow::ensure!(
        commits < todos,
        "{commits} commits need a to-do each and one for the commit off the record, of {todos}"
    );
    let env = Env::open().await?;
    seed(&env, todos).await?;
    let (rules, transient) = rules(shape, heavy, false, heads)?;
    install(&env, rules, transient).await?;
    let each = Each {
        env,
        commits,
        shape,
        heads,
        todos,
        cold,
    };
    if !cold {
        each.commit(todos - 1).await?;
    }
    Ok(Box::new(each))
}

/// `heads-each-<shape>-<body>-<heads>`: `size` commits of one command
/// each. The phase is those commits, so its instructions over `size` are
/// one commit's.
pub async fn each(
    size: usize,
    shape: Shape,
    heavy: bool,
    heads: usize,
) -> Result<Box<dyn Prepared>> {
    let todos = match std::env::var("DIALOG_PERF_HEADS_TODOS") {
        Ok(todos) => todos.parse()?,
        Err(_) => TODOS,
    };
    let cold = std::env::var_os("DIALOG_PERF_HEADS_COLD").is_some();
    each_over(todos, cold, size, shape, heavy, heads).await
}

/// `heads-move-<shape>`: the same commit against the rules that assert
/// the list a to-do goes to and retract the list it was on.
pub async fn moved(size: usize, shape: Shape) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    seed(&env, size).await?;
    let (rules, transient) = moving(shape)?;
    install(&env, rules, transient).await?;
    Ok(Box::new(Heads {
        env,
        size,
        facts: 2,
        narrow: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_query::query::Output as _;
    use dialog_query::{ConceptQuery, Parameters, Term};

    use crate::env::concept;

    /// Every `(to-do, value)` under `attribute`, sorted.
    async fn written(env: &Env, attribute: &str) -> Result<Vec<String>> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("value".into(), Term::var("value"));
        let query = ConceptQuery {
            predicate: concept(json!({
                "with": { "value": { "the": attribute, "as": "entity:" } }
            })),
            terms,
        };
        let mut rows: Vec<String> = env
            .branch
            .select(query)
            .perform(&env.operator)
            .try_vec()
            .await?
            .iter()
            .map(|row| Ok(format!("{} {}", row.entity(), row.get::<Entity>("value")?)))
            .collect::<Result<_>>()?;
        rows.sort();
        Ok(rows)
    }

    /// What `shape` leaves under every attribute it may write.
    async fn outcome(
        shape: Shape,
        heavy: bool,
        narrow: bool,
        heads: usize,
    ) -> Result<Vec<Vec<String>>> {
        let mut prepared = writing(20, shape, heavy, narrow, heads).await?;
        prepared.measure().await?;
        let mut all = Vec::new();
        for write in WRITES {
            all.push(written(prepared.env(), &format!("out/{write}")).await?);
        }
        for write in WRITES {
            all.push(written(prepared.env(), &format!("moved/{write}")).await?);
        }
        Ok(all)
    }

    /// The three shapes write the same facts, and the command one of
    /// them issues is not kept. Without this the costs compared are of
    /// different work.
    #[tokio::test]
    async fn it_writes_the_same_facts_in_every_shape() -> Result<()> {
        for (heavy, narrow, heads) in [
            (false, false, 2),
            (false, false, 4),
            (true, false, 2),
            (true, false, 4),
            (false, true, 2),
        ] {
            let wide = outcome(Shape::Wide, heavy, narrow, heads).await?;
            for (at, rows) in wide.iter().enumerate() {
                let expected = if at < heads { 20 } else { 0 };
                assert_eq!(rows.len(), expected, "wide, attribute {at}");
            }
            assert_eq!(wide, outcome(Shape::Repeat, heavy, narrow, heads).await?);
            assert_eq!(wide, outcome(Shape::Named, heavy, narrow, heads).await?);
            let none = outcome(Shape::None, heavy, narrow, heads).await?;
            assert!(none.iter().all(Vec::is_empty), "no rule writes nothing");
        }
        Ok(())
    }

    /// What five commits of one command each leave under every
    /// attribute `shape` may write, over twenty to-dos.
    async fn each_outcome(shape: Shape, heavy: bool, heads: usize) -> Result<Vec<Vec<String>>> {
        let mut prepared = each_over(20, false, 5, shape, heavy, heads).await?;
        prepared.measure().await?;
        let mut all = Vec::new();
        for write in WRITES {
            all.push(written(prepared.env(), &format!("out/{write}")).await?);
        }
        Ok(all)
    }

    /// A commit a command writes the same facts in every shape, and the
    /// same as a transaction that asserts them itself: the five measured
    /// and the one off the record. Without this `plain` is not the floor
    /// of the others.
    #[tokio::test]
    async fn it_writes_the_same_facts_a_commit_a_command() -> Result<()> {
        for (heavy, heads) in [(false, 2), (false, 4), (true, 2), (true, 4)] {
            let plain = each_outcome(Shape::Plain, heavy, heads).await?;
            for (at, rows) in plain.iter().enumerate() {
                let expected = if at < heads { 6 } else { 0 };
                assert_eq!(rows.len(), expected, "plain, attribute {at}");
            }
            for shape in [Shape::Wide, Shape::Repeat, Shape::Named] {
                assert_eq!(
                    plain,
                    each_outcome(shape, heavy, heads).await?,
                    "{shape:?}, heavy {heavy}, {heads} heads"
                );
            }
            let none = each_outcome(Shape::None, heavy, heads).await?;
            assert!(none.iter().all(Vec::is_empty), "no rule writes nothing");
        }
        Ok(())
    }

    /// The rules that move a to-do leave it on the list it went to and
    /// off the one it was on, written again or named.
    #[tokio::test]
    async fn it_moves_a_todo_in_either_shape() -> Result<()> {
        let mut lists = Vec::new();
        for shape in [Shape::Repeat, Shape::Named] {
            let mut prepared = moved(20, shape).await?;
            prepared.measure().await?;
            let rows = written(prepared.env(), "todo/list").await?;
            assert_eq!(rows.len(), 20, "{shape:?}: one list a to-do");
            lists.push(rows);
        }
        assert_eq!(lists[0], lists[1]);
        Ok(())
    }
}
