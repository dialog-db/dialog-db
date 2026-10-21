//! The write path: bulk and per-row commits, writes that succeed
//! standing claims, writes to a relation a rule derives, and writes that
//! dispatch a chain of inductive rules.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Pick, Value};
use dialog_query::InductiveRule;

use super::{Outcome, Prepared, seed_stuff};
use crate::env::{Env, assert_all, entity, fact, rule, text, var};

struct Batch {
    env: Env,
    size: usize,
}

impl Prepared for Batch {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            seed_stuff(&self.env, self.size, Pick::All).await?;
            Ok(Outcome::from([("facts".into(), 2 * self.size as u64)]))
        })
    }
}

/// `commit-batch`: an empty branch, then one transaction of `size`
/// entities.
pub async fn batch(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    Ok(Box::new(Batch { env, size }))
}

struct Rows {
    env: Env,
    size: usize,
}

impl Prepared for Rows {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            for index in 0..self.size {
                let this = entity("stuff", index);
                let facts = [
                    fact(&this, "stuff/name", text(format!("name-{index}"))),
                    fact(&this, "stuff/role", text(format!("role-{}", index % 8))),
                ];
                self.env.commit(assert_all(facts, Pick::All)).await?;
            }
            Ok(Outcome::from([("commits".into(), self.size as u64)]))
        })
    }
}

/// `commit-rows`: an empty branch, then `size` commits of one entity.
pub async fn rows(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    Ok(Box::new(Rows { env, size }))
}

struct Succeed {
    env: Env,
    size: usize,
}

impl Prepared for Succeed {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let facts = (0..self.size).map(|index| {
                fact(
                    &entity("stuff", index),
                    "stuff/name",
                    text(format!("renamed-{index}")),
                )
            });
            self.env.commit(assert_all(facts, Pick::Last)).await?;
            Ok(Outcome::from([("facts".into(), self.size as u64)]))
        })
    }
}

/// `commit-succeed`: `size` entities named under `last`, then one
/// transaction renaming every one.
pub async fn succeed(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Succeed { env, size }))
}

struct Derived {
    env: Env,
    size: usize,
}

impl Prepared for Derived {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let facts = (0..self.size).map(|index| {
                fact(
                    &entity("stuff", index),
                    "derived/name",
                    text(format!("stored-{index}")),
                )
            });
            self.env.commit(assert_all(facts, Pick::Last)).await?;
            Ok(Outcome::from([("facts".into(), self.size as u64)]))
        })
    }
}

/// `commit-derived`: a rule deriving `derived/name` from `stuff/name`
/// and `size` named entities, then one transaction writing
/// `derived/name` under `last` for every one.
pub async fn derived(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    let copy = rule(serde_json::json!({
        "deduce": { "with": { "name": { "the": "derived/name", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "name": { "the": "stuff/name", "as": "text:" } } },
            "where": { "this": var("this"), "name": var("name") }
        }]
    }));
    env.install(&[copy]).await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Derived { env, size }))
}

/// How many inductive rules the `rule-dispatch` chain relays through.
const RELAYS: usize = 3;

/// An inductive rule asserting `dispatch/a{to}` wherever
/// `dispatch/a{to - 1}` holds, with the same target.
fn relay(to: usize) -> Result<InductiveRule> {
    Ok(serde_json::from_value(serde_json::json!({
        "assert!": {
            "with": { "target": { "the": format!("dispatch/a{to}"), "as": "entity:" } }
        },
        "when": [{
            "assert": {
                "with": { "target": { "the": format!("dispatch/a{}", to - 1), "as": "entity:" } }
            },
            "where": { "this": var("this"), "target": var("target") }
        }]
    }))?)
}

struct Dispatch {
    env: Env,
    size: usize,
}

impl Prepared for Dispatch {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let facts = (0..self.size).map(|index| {
                fact(
                    &entity("stuff", index),
                    "dispatch/a0",
                    Value::Entity(entity("target", index)),
                )
            });
            self.env.commit(assert_all(facts, Pick::All)).await?;
            Ok(Outcome::from([(
                "facts".into(),
                ((RELAYS + 1) * self.size) as u64,
            )]))
        })
    }
}

/// `rule-dispatch`: a chain of inductive rules relaying `dispatch/a0`
/// through `dispatch/a3`, then one transaction asserting `dispatch/a0` for
/// `size` entities: the commit dispatches a round of induction per link.
pub async fn dispatch(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    let mut transaction = env.branch.transaction();
    for to in 1..=RELAYS {
        transaction = transaction.assert(relay(to)?);
    }
    transaction
        .commit()
        .publish()
        .perform(&env.operator)
        .await?;
    Ok(Box::new(Dispatch { env, size }))
}
