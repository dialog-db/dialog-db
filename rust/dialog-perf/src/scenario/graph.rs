//! Graph reads and maintenance, with normalized shared targets.
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Pick, Value};
use dialog_query::graph::{GraphEdge, GraphQuery, GraphSchema, GraphShape};
use dialog_repository::GraphSubscription;
use serde_json::json;

use super::{Outcome, Prepared};
use crate::env::{Env, assert_all, entity, fact, query, text};

fn graph() -> Result<GraphQuery> {
    let issue = serde_json::from_value(json!({"with": {
        "title": {"the": "issue/title", "as": "text:"},
        "assignee": {"the": "issue/assignee", "as": "entity:"}
    }}))?;
    let person = serde_json::from_value(json!({"with": {
        "name": {"the": "person/name", "as": "text:"}
    }}))?;
    Ok(GraphQuery {
        root: query(&issue, &["title", "assignee"], None),
        shape: "issue".into(),
        schema: GraphSchema {
            shapes: BTreeMap::from([
                (
                    "issue".into(),
                    GraphShape {
                        concept: issue,
                        edges: BTreeMap::from([(
                            "assignee".into(),
                            GraphEdge {
                                target: "person".into(),
                                matching: BTreeMap::new(),
                                shaping: BTreeMap::new(),
                            },
                        )]),
                    },
                ),
                (
                    "person".into(),
                    GraphShape {
                        concept: person,
                        edges: BTreeMap::new(),
                    },
                ),
            ]),
        },
    })
}

async fn seed(size: usize) -> Result<Env> {
    let mut env = Env::open().await?;
    let mut facts = vec![
        fact(&entity("person", 0), "person/name", text("Alice")),
        fact(&entity("person", 1), "person/name", text("Bob")),
    ];
    for index in 0..size {
        let issue = entity("issue", index);
        facts.push(fact(&issue, "issue/title", text(format!("Issue {index}"))));
        facts.push(fact(
            &issue,
            "issue/assignee",
            Value::Entity(entity("person", 0)),
        ));
    }
    env.commit(assert_all(facts, Pick::Last)).await?;
    env.reopen().await?;
    Ok(env)
}

struct Read {
    env: Env,
    size: usize,
}
impl Prepared for Read {
    fn env(&self) -> &Env {
        &self.env
    }
    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let result = self
                .env
                .branch
                .query()
                .graph(graph()?)
                .perform(&self.env.operator)
                .await?;
            anyhow::ensure!(result.roots.len() == self.size && result.nodes.len() == self.size + 1);
            Ok(Outcome::from([
                ("roots".into(), result.roots.len() as u64),
                ("nodes".into(), result.nodes.len() as u64),
                (
                    "result.bytes".into(),
                    serde_json::to_vec(&result)?.len() as u64,
                ),
            ]))
        })
    }
}

/// Read `size` issues with one shared typed assignee.
pub async fn read(size: usize) -> Result<Box<dyn Prepared>> {
    Ok(Box::new(Read {
        env: seed(size).await?,
        size,
    }))
}

/// The changed layer of a standing graph query.
#[derive(Clone, Copy)]
pub enum Change {
    /// Poll without changing the branch.
    Noop,
    /// Rename a shared target.
    Rename,
    /// Move a root edge to another target.
    Edge,
    /// Retitle one root.
    Root,
}
struct Repoll {
    env: Env,
    subscription: GraphSubscription,
    change: Change,
    size: usize,
    recomputes: usize,
}
impl Prepared for Repoll {
    fn env(&self) -> &Env {
        &self.env
    }
    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            if matches!(self.change, Change::Noop) {
                anyhow::ensure!(self.subscription.poll(&self.env.operator).await?.is_none());
                return Ok(Outcome::from([(
                    "retained_subscriptions".into(),
                    self.subscription.retained_subscriptions() as u64,
                )]));
            }
            let changed = match self.change {
                Change::Noop => unreachable!("returned above"),
                Change::Rename => fact(&entity("person", 0), "person/name", text("Alicia")),
                Change::Edge => fact(
                    &entity("issue", 0),
                    "issue/assignee",
                    Value::Entity(entity("person", 1)),
                ),
                Change::Root => fact(&entity("issue", 0), "issue/title", text("Retitled")),
            };
            self.env.commit(assert_all([changed], Pick::Last)).await?;
            let delta = self
                .subscription
                .poll(&self.env.operator)
                .await?
                .expect("one graph change");
            anyhow::ensure!(delta.fields.len() == 1, "shared target emits one delta");
            Ok(Outcome::from([
                ("field_changes".into(), delta.fields.len() as u64),
                ("new_entities".into(), delta.nodes_added.len() as u64),
                (
                    "new_evaluations".into(),
                    (self.subscription.recomputes() - self.recomputes) as u64,
                ),
                (
                    "maintenances".into(),
                    self.subscription.maintenances() as u64,
                ),
                (
                    "retained_subscriptions".into(),
                    self.subscription.retained_subscriptions() as u64,
                ),
                (
                    "nested_display_subscriptions".into(),
                    (3 + 3 * self.size) as u64,
                ),
                (
                    "delta.bytes".into(),
                    serde_json::to_vec(&delta)?.len() as u64,
                ),
            ]))
        })
    }
}

/// Warm a graph, then measure a nested rename, edge move, or root change.
pub async fn repoll(size: usize, change: Change) -> Result<Box<dyn Prepared>> {
    let env = seed(size).await?;
    let mut subscription = env.branch.subscribe_graph(graph()?)?;
    subscription.poll(&env.operator).await?;
    let recomputes = subscription.recomputes();
    Ok(Box::new(Repoll {
        env,
        subscription,
        change,
        size,
        recomputes,
    }))
}
