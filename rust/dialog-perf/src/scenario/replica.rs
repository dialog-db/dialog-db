//! A replica that faults blocks in on demand: what its first query costs
//! once it has adopted a head by root and remembers nothing.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::Pick;
use dialog_query::query::Output as _;
use dialog_repository::Branch;

use super::{Outcome, Prepared, seed_stuff, stuff_name};
use crate::env::{Env, Operator, assert_all, entity, fact, query, text};

/// How many entities the history seeds before the origins commit.
const SEEDED: usize = 100;

struct FirstQuery {
    env: Env,
    reader: Operator,
    adopter: Branch,
}

impl Prepared for FirstQuery {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            // One point read of a seeded entity. The adopter holds no node
            // of the tree, so the read starts at the root: the first block
            // it faults in is the root frame, whose bytes are the write
            // buffer the history's commits left in it.
            let rows = self
                .adopter
                .select(query(&stuff_name(), &["name"], Some(&entity("stuff", 7))))
                .perform(&self.reader)
                .try_vec()
                .await?
                .len() as u64;
            self.env.count(
                "archive.get.root.bytes",
                self.env.first_read().unwrap_or(0) as u64,
            );
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `first-query`: `SEEDED` entities, then `size` one-commit origins, each
/// a writer branch adopting the previous writer's head; the scenario
/// branch adopts the last head, and a replica in a session of its own
/// adopts it by root and runs one point query.
pub async fn first_query(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    seed_stuff(&env, SEEDED, Pick::All).await?;
    let mut previous = env.branch.clone();
    for index in 0..size {
        let writer = env.branch(&format!("writer-{index}")).await?;
        writer.pull().from(&previous).perform(&env.operator).await?;
        let facts = [fact(
            &entity("origin", index),
            "origin/mark",
            text(format!("origin-{index}")),
        )];
        writer
            .transaction()
            .integrate(assert_all(facts, Pick::All))
            .commit()
            .publish()
            .perform(&env.operator)
            .await?;
        previous = writer;
    }
    env.branch
        .pull()
        .from(&previous)
        .perform(&env.operator)
        .await?;
    let (reader, adopter) = env.adopter("adopter", crate::env::SCENARIO_BRANCH).await?;
    Ok(Box::new(FirstQuery {
        env,
        reader,
        adopter,
    }))
}
