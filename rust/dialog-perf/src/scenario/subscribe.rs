//! A standing query's re-poll after a commit that moves rows between
//! the cases of a ranked field.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::Pick;
use dialog_query::{ConceptConclusion, ConceptQuery};
use dialog_repository::{Delta, Subscription};

use super::rule::{accounts, status};
use super::{Outcome, Prepared};
use crate::env::{Env, assert_all, entity, fact, query, text};

/// How many accounts the measured commit promotes.
const MOVED: usize = 10;

struct Repoll {
    env: Env,
    subscription: Subscription<ConceptQuery>,
}

impl Prepared for Repoll {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            // Registered accounts (index 1 mod 4) become active.
            let facts = (0..MOVED).map(|moved| {
                fact(
                    &entity("account", 4 * moved + 1),
                    "account/active",
                    text("yes"),
                )
            });
            self.env.commit(assert_all(facts, Pick::Last)).await?;
            let delta = self.subscription.poll(&self.env.operator).await?;
            let (asserted, retracted) = delta
                .as_ref()
                .map(|delta| (delta.asserted.len(), delta.retracted.len()))
                .unwrap_or((0, 0));
            Ok(Outcome::from([
                ("asserted".into(), asserted as u64),
                ("retracted".into(), retracted as u64),
                ("recomputes".into(), self.subscription.recomputes() as u64),
                (
                    "maintenances".into(),
                    self.subscription.maintenances() as u64,
                ),
            ]))
        })
    }
}

/// `subscribe-repoll`: the status rules over `size` accounts and a
/// subscription to every status, polled once; then a commit promoting
/// `MOVED` accounts and the poll that follows it.
pub async fn repoll(size: usize) -> Result<Box<dyn Prepared>> {
    let env = accounts(size).await?;
    let mut subscription = env.branch.subscribe(query(&status(), &["status"], None));
    let initial: Option<Delta<ConceptConclusion>> = subscription.poll(&env.operator).await?;
    anyhow::ensure!(
        initial.is_some_and(|delta| delta.asserted.len() == size),
        "the first poll answers every account"
    );
    Ok(Box::new(Repoll { env, subscription }))
}

/// The note concept a per-entity display reads: a title and a body.
fn note() -> dialog_query::ConceptDescriptor {
    crate::env::concept(serde_json::json!({ "with": {
        "title": { "the": "note/title", "as": "text:" },
        "body": { "the": "note/body", "as": "text:" }
    }}))
}

struct Fanout {
    env: Env,
    subscriptions: Vec<Subscription<ConceptQuery>>,
    size: usize,
}

impl Prepared for Fanout {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let added = entity("note", self.size);
            self.env
                .commit(assert_all(
                    [
                        fact(&added, "note/title", text("new")),
                        fact(&added, "note/body", text("written elsewhere")),
                    ],
                    Pick::Last,
                ))
                .await?;
            let mut changed = 0u64;
            for subscription in &mut self.subscriptions {
                if subscription.poll(&self.env.operator).await?.is_some() {
                    changed += 1;
                }
            }
            Ok(Outcome::from([
                ("subscriptions".into(), self.subscriptions.len() as u64),
                ("changed".into(), changed),
            ]))
        })
    }
}

/// `subscribe-fanout`: `size` notes, one subscription to each note's
/// fields (as a list view renders one display per item) and one to the
/// list; then a commit adding a note and a poll of every subscription.
pub async fn fanout(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    let facts = (0..size).flat_map(|index| {
        let this = entity("note", index);
        [
            fact(&this, "note/title", text(format!("Note {index}"))),
            fact(
                &this,
                "note/body",
                text(format!(
                    "Body of note {index}, long enough to be a real payload."
                )),
            ),
        ]
    });
    env.commit(assert_all(facts, Pick::Last)).await?;
    env.reopen().await?;
    let mut subscriptions = vec![
        env.branch
            .subscribe(query(&note(), &["title", "body"], None)),
    ];
    for index in 0..size {
        subscriptions.push(env.branch.subscribe(query(
            &note(),
            &["title", "body"],
            Some(&entity("note", index)),
        )));
    }
    for subscription in &mut subscriptions {
        subscription.poll(&env.operator).await?;
    }
    Ok(Box::new(Fanout {
        env,
        subscriptions,
        size,
    }))
}
