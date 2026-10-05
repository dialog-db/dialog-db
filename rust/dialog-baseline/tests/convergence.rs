//! History independence, in-tree: the same fact log must settle into the
//! same facts, and the same tree, no matter how its writes were grouped.
//!
//! This is the campaign's convergence oracle promoted from a manually-run
//! example (`converge_check`) to a test that runs with the suite. Every
//! write-path change this repository takes (buffered enqueue structure,
//! flush policy, edit fast paths, bulk plants) is obligated to preserve
//! this property. The default scale keeps the test in CI budget; set
//! `DIALOG_CONVERGE_TXNS` to sweep larger logs.
//!
//! Two properties are checked, each where it is well defined:
//! - one branch replaying the log as a staged chain, one transaction per
//!   link with every link after the first amending the tip, must reach the
//!   same canonical tree whether it canonicalizes after every link or only
//!   after the last (a history-independence break otherwise);
//! - replaying the log as published commits grouped differently must
//!   yield the same facts (a data bug otherwise, e.g. batched supersession
//!   dropping a write). Published groupings mint different versions, so
//!   their trees are compared as facts, not roots.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_artifacts::tree::ArtifactTree;
use dialog_artifacts::{ArtifactSelector, Attribute, Instruction};
use dialog_baseline::repo::{DialogRepo, VolatileRepo};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_common::Blake3Hash as NodeHash;
use dialog_repository::TransactionBatch;

fn txn_count() -> usize {
    std::env::var("DIALOG_CONVERGE_TXNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(120)
}

/// The tree a staged batch's tip names.
fn tree_of(batch: &TransactionBatch) -> ArtifactTree {
    ArtifactTree::from_hash(NodeHash::from(*batch.revision().tree.hash()))
}

/// Asserts the stored tree is shaped exactly as the canonical constructor
/// shapes its entry set, localizing any break to a node before a root
/// comparison reports it as an opaque hash difference.
async fn assert_canonical(repo: &VolatileRepo, tree: &ArtifactTree, what: &str) -> Result<()> {
    let divergences = tree.canonical_divergences(&repo.index()).await?;
    assert_eq!(
        divergences,
        Vec::<String>::new(),
        "{what}: canonical tree failed canonical-form validation"
    );
    Ok(())
}

/// Canonicalizing after every amended link and canonicalizing only after
/// the last must reach the same tree over the same log.
#[tokio::test]
async fn it_converges_across_canonicalization_points() -> Result<()> {
    let log = SeLog::synthetic(txn_count());
    let repo = DialogRepo::volatile().await?;

    // Both chains stage on the same unpublished head, so they mint the
    // same version and differ only in when their trees were canonicalized.
    let every = repo.stage_se(&log, 1).await?;
    let last = repo.stage_se(&log, usize::MAX).await?;
    assert_eq!(every.version(), last.version());

    assert_canonical(&repo, &tree_of(&every), "canonicalized every link").await?;
    assert_canonical(&repo, &tree_of(&last), "canonicalized the last link").await?;
    assert_eq!(
        every.revision().tree,
        last.revision().tree,
        "same log, different canonical trees depending on when the chain \
         canonicalized: history independence is broken"
    );
    Ok(())
}

/// Every fact in `repo`'s branch whose attribute `log` writes, as sorted
/// `(the, of, is)` rows.
async fn facts(repo: &VolatileRepo, log: &SeLog) -> Result<Vec<String>> {
    let mut attributes = Vec::new();
    for commit in &log.transactions {
        for instruction in se_instructions(commit)? {
            let (Instruction::Assert(fact)
            | Instruction::Replace(fact)
            | Instruction::Retract(fact)
            | Instruction::Succeed(fact, _)) = instruction;
            attributes.push(fact.the.to_string());
        }
    }
    attributes.sort();
    attributes.dedup();

    let mut rows = Vec::new();
    for attribute in attributes {
        let selector = ArtifactSelector::new().the(Attribute::from_str(&attribute)?);
        for fact in repo.collect(selector).await? {
            rows.push(format!("{} {} {:?}", fact.the, fact.of, fact.is));
        }
    }
    rows.sort();
    Ok(rows)
}

/// Replays `log` on a fresh branch, publishing one commit per `group`
/// transactions, and returns the facts it holds.
async fn publish_grouped(log: &SeLog, group: usize) -> Result<Vec<String>> {
    let repo = DialogRepo::volatile().await?;
    repo.publish_se_grouped(log, group).await?;
    facts(&repo, log).await
}

/// Per-transaction commits, five-transaction commits, and one commit must
/// all settle into the same facts over the same log.
#[tokio::test]
async fn it_converges_across_commit_groupings() -> Result<()> {
    let log = SeLog::synthetic(txn_count());

    let per_txn = publish_grouped(&log, 1).await?;
    let by_five = publish_grouped(&log, 5).await?;
    let single = publish_grouped(&log, usize::MAX).await?;

    assert!(!per_txn.is_empty(), "the log must leave facts behind");
    assert_eq!(
        per_txn, by_five,
        "per-txn and by-five groupings disagree on the FACT SET (data bug)"
    );
    assert_eq!(
        per_txn, single,
        "per-txn and single-commit groupings disagree on the FACT SET (data bug)"
    );
    Ok(())
}

/// Canonicalizing a canonical tree must be a fixpoint: amending the tip
/// with nothing and canonicalizing again keeps the same tree.
#[tokio::test]
async fn it_reaches_a_canonical_fixpoint() -> Result<()> {
    let log = SeLog::synthetic(40);
    let repo = DialogRepo::volatile().await?;
    let batch = repo.stage_se(&log, usize::MAX).await?;
    assert_canonical(&repo, &tree_of(&batch), "first canonicalize").await?;
    let first = batch.revision().tree;

    let again = batch
        .transaction()
        .commit()
        .allow_empty()
        .amend()
        .canonicalize()
        .perform(repo.operator())
        .await?;
    assert_eq!(
        first,
        again.revision().tree,
        "canonicalize must be idempotent"
    );
    Ok(())
}
