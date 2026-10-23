//! The catch-up scenario: a replica with a live subscription, when the
//! branch moves. The scenario issue #642 names as the one that would have
//! decided #640.
//!
//! A client joins (adopt by root), subscribes to the landing concept
//! ([`Card`]) and pays the first poll outside the measured window. Then
//! an upstream writer commits `k` facts, one per commit, and the client
//! pulls and polls; each `k` is its own pair of phases:
//!
//! - **pull-k** — what one head move costs the replica's pull. With no
//!   novelty of its own (its tree is its sync base), the pull takes
//!   `pull.rs`'s fast-forward arm: one `memory.resolve` for the upstream
//!   head, then `head_observes` to decide whether the upstream has seen
//!   the local head. The two heads share an origin here (the client
//!   adopted the server's head, and the server wrote the commits), so an
//!   origin's editions form one chain and the answer is an edition
//!   compare: zero block reads, and the head is adopted by root.
//! - **poll-k** — what the subscription pays for the move:
//!   `subscription.rs`'s `touched` diffs the pinned root against the new
//!   one *within the demand cover* (`differentiate_within`), so the walk
//!   reads the new root and the changed paths inside the ranges the
//!   first evaluation demanded and nothing outside them; then
//!   `maintain` re-derives the touched entities' rows by evaluating the
//!   query restricted to each, one entity after another. The meter sees
//!   one phase, and the root re-read, the diff and the maintenance
//!   re-queries are all `archive.get`s; what it can attribute it does.
//!   The root is the one block whose digest the new revision names, and
//!   the phase prints its request and bytes to stderr. The rest splits
//!   by what the tree does with small commits: a commit appends to the
//!   root's write buffer (`notes/novelty-buffer.md`), so until a flush
//!   cascades the entries into leaves the diff finds every change in
//!   the root it already read and descends nowhere, and every get past
//!   the root is a maintenance re-query reading one touched entity's
//!   leaf. After a flush the diff descends to the changed leaves inside
//!   the cover and the two are mixed. The chain reads one per touched
//!   entity because the re-queries run in sequence.
//!
//! Then the same with the `k` commits spread across `m` writers, each a
//! replica of its own (its own origin) that commits, pulls what the
//! others pushed (a merge, minting a merge revision of its origin) and
//! pushes; the client then catches up on a head minted by another origin
//! than the one it adopted:
//!
//! - **pull-kxm** — the fast-forward arm again, but across origins.
//!   `head_observes` cannot compare editions across origins, so each of
//!   the arm's two gates is a point read of a context region
//!   (`context.rs`'s `region_entry`, the root and the path to one leaf):
//!   the "nothing new" gate reads our own tree's region for their head,
//!   and the fast-forward gate reads their tree's region for ours, before
//!   adopting by root. Our root and region leaf are usually cached from
//!   the previous poll and pull, so the phase reads their root and
//!   region leaf plus whichever of ours is cold. This is the #624 regime
//!   at small scale: the causal context lives in the tree, and a replica
//!   reads it to know a foreign head has seen its own. The root re-read
//!   the poll would pay has moved here: the pull read the new root for
//!   its region, and the poll finds it cached (the stderr line reads
//!   zero for it).
//! - **poll-kxm** — the diff now crosses the merge paths the writers
//!   minted.
//!
//! The commits toggle `bug/status` on `k` entities spread evenly over the
//! key space, so each commit touches a different leaf: the widest shape
//! `k` edits can take, and the one a space with many writers has. Every
//! poll is checked to have taken the maintenance path, not a recompute.

use anyhow::{Context as _, Result};
use dialog_artifacts::{Artifact, Instruction, Pick, Value};
use dialog_query::{Query, Term};
use dialog_remote_fs::simulation;
use dialog_repository::Branch;

use crate::join::Card;
use crate::report::PhaseReport;
use crate::scenario::{Operator, Scenario, Site, measured};

/// The commit sizes a single writer catches the client up on.
pub const SINGLE_WRITER: [usize; 3] = [1, 10, 100];

/// The `(commits, writers)` pairs the multi-writer rounds spread the
/// commits over.
pub const MANY_WRITERS: [(usize, usize); 2] = [(10, 3), (100, 10)];

/// The landing concept with every field free.
fn landing() -> Query<Card> {
    Query::<Card> {
        this: Term::var("this"),
        title: Term::var("title"),
        status: Term::var("status"),
        rank: Term::var("rank"),
        reporter: Term::var("reporter"),
        created: Term::var("created"),
    }
}

/// One status toggle: the `j`th of `k` edits, on an entity `k` edits
/// spread evenly over the seeded range reach, to a status no seed
/// carries so every toggle changes the card.
fn toggle(entities: usize, k: usize, j: usize, round: usize) -> Result<Instruction> {
    let index = j * entities / k;
    Ok(Instruction::Assert(
        Artifact {
            the: "bug/status".parse()?,
            of: format!("bug:{index}").parse()?,
            is: Value::String(format!("touched-{round}")),
            cause: None,
        },
        Pick::Last,
    ))
}

/// Commit `facts` on `writer`, one commit each, and publish each locally.
async fn commit_each(writer: &Branch, operator: &Operator, facts: Vec<Instruction>) -> Result<()> {
    for fact in facts {
        writer
            .transaction()
            .integrate(std::iter::once(fact).collect())
            .commit()
            .publish()
            .perform(operator)
            .await?;
    }
    Ok(())
}

/// The catch-up's phases over a seeded site.
pub(crate) async fn phases(
    site: &Site,
    scenario: &Scenario,
    phases: &mut Vec<PhaseReport>,
) -> Result<()> {
    let operator = &site.operator;
    let entities = scenario.entities;

    // The subscribed replica: joined, and its first poll paid.
    let client = site.mount("soak-catchup").await?;
    client.pull().perform(operator).await?;
    let mut subscription = client.subscribe(landing());
    let first = subscription
        .poll(operator)
        .await
        .map_err(|error| anyhow::anyhow!("first poll failed: {error}"))?
        .context("the first poll evaluates")?;
    anyhow::ensure!(
        first.asserted.len() == entities,
        "the first poll should see every card, saw {}",
        first.asserted.len()
    );

    let mut round = 0;
    for k in SINGLE_WRITER {
        round += 1;
        let facts = (0..k)
            .map(|j| toggle(entities, k, j, round))
            .collect::<Result<Vec<_>>>()?;
        commit_each(&site.branch, operator, facts).await?;
        site.branch
            .push()
            .perform(operator)
            .await?
            .context("the writer's push should publish")?;
        catch_up(
            &client,
            &mut subscription,
            operator,
            k,
            &k.to_string(),
            phases,
        )
        .await?;
    }

    for (k, m) in MANY_WRITERS {
        round += 1;
        let mut writers = Vec::with_capacity(m);
        for index in 0..m {
            let writer = site.mount(&format!("soak-writer-{index}")).await?;
            writer.pull().perform(operator).await?;
            writers.push(writer);
        }
        for j in 0..k {
            let writer = &writers[j % m];
            commit_each(writer, operator, vec![toggle(entities, k, j, round)?]).await?;
            // Merge what the other writers pushed since, then push: the
            // head the client catches up on is a chain of merges minted
            // by m origins.
            writer.pull().perform(operator).await?;
            writer
                .push()
                .perform(operator)
                .await?
                .context("a writer's push should publish")?;
        }
        catch_up(
            &client,
            &mut subscription,
            operator,
            k,
            &format!("{k}x{m}"),
            phases,
        )
        .await?;
    }
    Ok(())
}

/// The measured pair: the client pulls the moved head, then polls its
/// subscription, which must see exactly the `k` toggled cards through
/// the maintenance path.
async fn catch_up(
    client: &Branch,
    subscription: &mut dialog_repository::Subscription<Query<Card>>,
    operator: &Operator,
    k: usize,
    suffix: &str,
    phases: &mut Vec<PhaseReport>,
) -> Result<()> {
    measured(&format!("pull-{suffix}"), phases, async {
        client.pull().perform(operator).await?;
        Ok(())
    })
    .await?;

    let maintained = subscription.maintenances();
    let before = simulation::get_ledger();
    measured(&format!("poll-{suffix}"), phases, async {
        let delta = subscription
            .poll(operator)
            .await
            .map_err(|error| anyhow::anyhow!("poll failed: {error}"))?
            .context("the poll should see the toggled cards")?;
        anyhow::ensure!(
            delta.asserted.len() == k && delta.retracted.len() == k,
            "the poll should re-derive the {k} toggled cards, saw +{} -{}",
            delta.asserted.len(),
            delta.retracted.len()
        );
        Ok(())
    })
    .await?;
    anyhow::ensure!(
        subscription.maintenances() == maintained + 1,
        "the poll should maintain incrementally, not recompute"
    );

    // The root re-read, attributed: the one block the new head names.
    if let Some(revision) = client.revision() {
        let root = dialog_common::Blake3Hash::from(*revision.tree.hash()).to_string();
        let record = simulation::get_ledger().since(&before).record(&root);
        eprintln!(
            "poll-{suffix}: root re-read requests={} bytes={}",
            record.requests, record.bytes
        );
    }
    Ok(())
}
