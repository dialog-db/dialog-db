//! A transaction read between its writes: what a library install does
//! when it stages a schema and then resolves names against it.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::Policy;
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;

use super::{Outcome, Prepared, seed_stuff, stuff_name};
use crate::env::{Env, assert_all, entity, fact, query, text};

struct Reads {
    env: Env,
    size: usize,
}

impl Prepared for Reads {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let name = stuff_name();
            let mut transaction = self.env.branch.transaction();
            let mut rows = 0u64;
            for index in 0..self.size {
                let this = entity("stuff", index);
                let write = fact(&this, "stuff/name", text(format!("renamed-{index}")));
                transaction = transaction.integrate(assert_all([write], Policy::Last));
                let read: Vec<ConceptConclusion> = transaction
                    .query()
                    .select(query(&name, &["name"], Some(&this)))
                    .perform(&self.env.operator)
                    .try_vec()
                    .await?;
                rows += read.len() as u64;
            }
            transaction
                .commit()
                .publish()
                .perform(&self.env.operator)
                .await?;
            Ok(Outcome::from([
                ("rows".into(), rows),
                ("facts".into(), self.size as u64),
            ]))
        })
    }
}

/// `transaction-reads`: `size` named entities, then one transaction
/// that renames each and reads the name back before the next write.
pub async fn reads(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Policy::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Reads { env, size }))
}
