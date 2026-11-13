//! Writing several facts on one condition, three ways, for
//! `research/rule-heads.md`: one rule whose head is wide, one rule a
//! head with the body written again, and a rule that issues a command
//! the rules of one head read.
//!
//! A `move` command names a to-do, the list it goes to, who moved it
//! and when. The body reads the list the to-do is on, and in the heavy
//! shape that list's owner, the owner's team and the to-do's state. The
//! heads write what the body bound under `out/…`, two of them or four.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_corpus::{Pick, Value};
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
        Shape::None => (Vec::new(), None),
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

/// Dispatch a `move` command for each of `size` to-dos in one
/// transaction: to the next list, by one of seven movers, at a time of
/// its own.
async fn dispatch(env: &Env, size: usize, narrow: bool) -> Result<()> {
    let mut transaction = env.branch.transaction();
    for index in 0..size {
        let command = entity("move", index);
        for (name, value) in [
            ("cmd.move/target", entity("todo", index)),
            ("cmd.move/to", entity("list", (index + 1) % LISTS)),
            ("cmd.move/by", entity("user", index % 7)),
            ("cmd.move/at", entity("tick", index)),
        ] {
            if name != "cmd.move/target" && !carried(narrow).contains(&&name["cmd.move/".len()..]) {
                continue;
            }
            transaction = transaction.dispatch(DynamicAttributeExpression {
                the: name.parse::<Relation>()?,
                of: command.clone(),
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
            dispatch(&self.env, self.size, self.narrow).await?;
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
    use dialog_corpus::Entity;
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
