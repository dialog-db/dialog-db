//! A walk of the whole tree outside any query: what a snapshot export
//! reads, over a tree the branch's own commits left warm.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Result, anyhow};
use dialog_corpus::Pick;
use futures_util::TryStreamExt as _;

use super::{Outcome, Prepared, seed_stuff};
use crate::env::Env;

struct Walk {
    env: Env,
}

impl Prepared for Walk {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            // The export visits every node of the tree. The nodes are the
            // ones the seeding commit sealed, kept by the repository's
            // cache, so the walk reads them from memory and the archive
            // sees no block read.
            let snapshot = self
                .env
                .branch
                .snapshot()
                .ok_or_else(|| anyhow!("the seeded branch has a revision"))?;
            let items = snapshot.export().perform(&self.env.operator);
            futures_util::pin_mut!(items);
            let mut count = 0u64;
            while items.try_next().await?.is_some() {
                count += 1;
            }
            Ok(Outcome::from([("items".into(), count)]))
        })
    }
}

/// `export-walk`: `size` entities, the branch reopened, then one snapshot
/// export streaming every block of the tree.
pub async fn walk(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Walk { env }))
}
