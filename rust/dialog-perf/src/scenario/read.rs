//! The read path over stored facts: point reads, a scan, and a join.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Entity, Pick};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptDescriptor};

use super::{Outcome, Prepared, seed_stuff, stuff, stuff_name};
use crate::env::{Env, entity, query};

/// How many point reads a point scenario makes.
pub const PROBES: usize = 200;

/// Read `descriptor` for every `free` field, `of` one entity or all.
pub async fn read(
    env: &Env,
    descriptor: &ConceptDescriptor,
    free: &[&str],
    of: Option<&Entity>,
) -> Result<Vec<ConceptConclusion>> {
    Ok(env
        .branch
        .select(query(descriptor, free, of))
        .perform(&env.operator)
        .try_vec()
        .await?)
}

/// Point reads of `descriptor` over `PROBES` entities of the `stuff`
/// population spread across `size`, counting the rows.
pub async fn probe(
    env: &Env,
    descriptor: &ConceptDescriptor,
    free: &[&str],
    size: usize,
) -> Result<u64> {
    let step = (size / PROBES).max(1);
    let mut rows = 0u64;
    for probe in 0..PROBES {
        let this = entity("stuff", (probe * step) % size);
        rows += read(env, descriptor, free, Some(&this)).await?.len() as u64;
    }
    Ok(rows)
}

struct Point {
    env: Env,
    size: usize,
}

impl Prepared for Point {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = probe(&self.env, &stuff_name(), &["name"], self.size).await?;
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `read-point`: `size` entities, then `PROBES` single-entity reads.
pub async fn point(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Point { env, size }))
}

struct Scan {
    env: Env,
}

impl Prepared for Scan {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &stuff_name(), &["name"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `read-scan`: `size` entities, then one read of every name.
pub async fn scan(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Scan { env }))
}

struct Join {
    env: Env,
}

impl Prepared for Join {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &stuff(), &["name", "role"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `query-join`: `size` entities, then one read of name and role.
pub async fn join(size: usize) -> Result<Box<dyn Prepared>> {
    let mut env = Env::open().await?;
    seed_stuff(&env, size, Pick::Last).await?;
    env.reopen().await?;
    Ok(Box::new(Join { env }))
}
