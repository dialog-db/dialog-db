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
