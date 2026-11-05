//! A standing query's re-poll after a commit that moves rows between
//! the cases of a ranked field.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Artifact, Pick};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery};
use dialog_repository::{Branch, Delta, Subscription};

use super::rule::{accounts, status};
use super::{Outcome, Prepared};
use crate::env::{Adopter, Env, Operator, assert_all, entity, fact, query, text};

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
            self.env
                .commit(assert_all(added_note(self.size), Pick::Last))
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

/// The two facts of note `index`.
fn note_facts(index: usize) -> [Artifact; 2] {
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
}

/// The two facts of a note written elsewhere, as the measured commits add
/// it: short, so the commit's cost is the note and not its text.
fn added_note(index: usize) -> [Artifact; 2] {
    let this = entity("note", index);
    [
        fact(&this, "note/title", text("new")),
        fact(&this, "note/body", text("written elsewhere")),
    ]
}

/// The subscriptions a list view of `size` notes holds on `branch`: one
/// to the list and one to each note's fields, each polled once.
async fn note_subscriptions(
    branch: &Branch,
    operator: &Operator,
    size: usize,
) -> Result<Vec<Subscription<ConceptQuery>>> {
    let mut subscriptions = vec![branch.subscribe(query(&note(), &["title", "body"], None))];
    for index in 0..size {
        subscriptions.push(branch.subscribe(query(
            &note(),
            &["title", "body"],
            Some(&entity("note", index)),
        )));
    }
    for subscription in &mut subscriptions {
        subscription.poll(operator).await?;
    }
    Ok(subscriptions)
}

/// `subscribe-fanout`: `size` notes, one subscription to each note's
/// fields (as a list view renders one display per item) and one to the
/// list; then a commit adding a note and a poll of every subscription.
pub async fn fanout(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    env.commit(assert_all((0..size).flat_map(note_facts), Pick::Last))
        .await?;
    env.reopen().await?;
    let subscriptions = note_subscriptions(&env.branch, &env.operator, size).await?;
    Ok(Box::new(Fanout {
        env,
        subscriptions,
        size,
    }))
}

struct Pulled {
    env: Env,
    reader: Operator,
    adopter: Branch,
    upstream: Branch,
    subscriptions: Vec<Subscription<ConceptQuery>>,
}

impl Prepared for Pulled {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            // The device pulls the note the other wrote, then every
            // subscription re-checks the commit. The pull's own
            // differences and the subscriptions diff the same two trees:
            // the changed path is read from the archive once, by whichever
            // reads it first, and answered from memory to the rest.
            self.adopter
                .pull()
                .from(&self.upstream)
                .perform(&self.reader)
                .await?;
            let mut changed = 0u64;
            for subscription in &mut self.subscriptions {
                if subscription.poll(&self.reader).await?.is_some() {
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

/// `subscribe-pulled`: `size` notes, adopted by a device holding a
/// subscription per note and one to the list, warm for its own tree; the
/// origin commits a note the device has not pulled, whose blocks are cold
/// for it; then the device's pull and the poll of every subscription.
pub async fn pulled(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    env.commit(assert_all((0..size).flat_map(note_facts), Pick::Last))
        .await?;
    let Adopter {
        reader,
        branch: adopter,
        upstream,
    } = env.adopter("device", crate::env::SCENARIO_BRANCH).await?;
    let subscriptions = note_subscriptions(&adopter, &reader, size).await?;

    // The origin's commit seals its nodes into the caches the peer holds,
    // which the device's session shares. Forget them all, then read the
    // device's own tree once more, so that tree is warm and the commit's
    // blocks are the cold ones, as a write from another device is.
    env.commit(assert_all(added_note(size), Pick::Last)).await?;
    env.forget();
    adopter
        .select(query(&note(), &["title", "body"], None))
        .perform(&reader)
        .try_vec()
        .await?;
    Ok(Box::new(Pulled {
        env,
        reader,
        adopter,
        upstream,
        subscriptions,
    }))
}
