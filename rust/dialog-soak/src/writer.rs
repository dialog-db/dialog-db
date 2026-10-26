//! The writer scenario: what a joined client pays to publish, under a
//! shaped link. The scenario that turns "more blocks per commit" into a
//! number a writer feels (issue #642).
//!
//! A client joins (adopt by root, outside the measured window) and then,
//! each as its own pair of phases:
//!
//! - **commit-1** / **push-1** — one fact in one commit, then a push.
//! - **commit-10** / **push-10** — ten facts in ten commits, then one push.
//! - **commit-2000** / **push-2000** — one bulk batch of 2 000 facts, then
//!   a push.
//!
//! The commit phases read: integrating into a tree held by root hydrates
//! what the write touches. A small commit appends to the root's write
//! buffer (`notes/novelty-buffer.md`) and rewrites the root alone, so
//! `commit-1` reads the paths its entries would land on (the first commit
//! on a fresh replica) and `commit-10` reads nothing; the bulk batch
//! overflows the buffer and cascades into the leaves, reading each leaf
//! it rewrites.
//!
//! The push phases write, and what they write is `push.rs`'s
//! `push_upstream` by construction: one `memory.resolve` to confirm where
//! the upstream stands; the blobs and spilled values the novelty names
//! (none here: the facts fit inline); no frontier probes (the target is
//! the sole tracked remote, so content held by reference is attributed
//! to it at zero requests); the held novelty as one `archive.put` per
//! node in waves children-before-parents, concurrent within a wave up to
//! the upload concurrency of 16 and with a barrier between waves; and one
//! `memory.publish`. So `requests` is `2 + novel nodes`, and the chain is
//! `2 + Σ ceil(wave / 16)` over the waves. A buffered commit's novelty is
//! the root alone: `push-1` and `push-10` are one put each, and the put is
//! the size of the root with its buffer, which is the cost #640's flush
//! policy trades against; the bulk push ships every rewritten leaf, the
//! index nodes above them and the root.
//!
//! The facts are new entities beyond the seeded range, in the seed's
//! shape; the bulk batch is the seed's own entity facts, 2 000 of them.

use anyhow::{Context as _, Result};
use dialog_artifacts::{Artifact, Instruction, Pick, Value};

use crate::join::entity_facts;
use crate::report::PhaseReport;
use crate::scenario::{Scenario, Site, measured};

/// Facts in the bulk batch.
pub const BULK_FACTS: usize = 2_000;

/// One new entity's title: the single fact a small commit carries.
fn title(index: usize) -> Result<Instruction> {
    Ok(Instruction::Assert(
        Artifact {
            the: "bug/title".parse()?,
            of: format!("bug:{index}").parse()?,
            is: Value::String(format!("Bug #{index}: filed from the writer")),
            cause: None,
        },
        Pick::All,
    ))
}

/// The writer's phases over a seeded site.
pub(crate) async fn phases(
    site: &Site,
    scenario: &Scenario,
    phases: &mut Vec<PhaseReport>,
) -> Result<()> {
    let (client, operator) = site.mount("soak-writer").await?;
    let operator = &operator;
    client.pull().perform(operator).await?;
    // New entities start past the seeded range.
    let mut next = scenario.entities;

    measured("commit-1", phases, async {
        client
            .transaction()
            .integrate(std::iter::once(title(next)?).collect())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok(())
    })
    .await?;
    next += 1;
    measured("push-1", phases, async {
        client
            .push()
            .perform(operator)
            .await?
            .context("the push should publish")?;
        Ok(())
    })
    .await?;

    measured("commit-10", phases, async {
        for index in next..next + 10 {
            client
                .transaction()
                .integrate(std::iter::once(title(index)?).collect())
                .commit()
                .publish()
                .perform(operator)
                .await?;
        }
        Ok(())
    })
    .await?;
    next += 10;
    measured("push-10", phases, async {
        client
            .push()
            .perform(operator)
            .await?
            .context("the push should publish")?;
        Ok(())
    })
    .await?;

    let mut bulk = Vec::with_capacity(BULK_FACTS);
    while bulk.len() < BULK_FACTS {
        bulk.extend(entity_facts(next)?);
        next += 1;
    }
    bulk.truncate(BULK_FACTS);
    measured("commit-2000", phases, async {
        client
            .transaction()
            .integrate(bulk.into_iter().collect())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok(())
    })
    .await?;
    measured("push-2000", phases, async {
        client
            .push()
            .perform(operator)
            .await?
            .context("the push should publish")?;
        Ok(())
    })
    .await?;
    Ok(())
}
