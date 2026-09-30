//! History-independence check on the real workload: the same SE log
//! staged as one amended chain must reach the same canonical tree
//! whatever links it canonicalized at.
//!
//! The pacing-ramp prototype (`DIALOG_TREE_PACING_RAMP`) makes cut
//! decisions read frame-prefix weight (outcome-dependent context), so
//! its convergence rests on the edit path's rightward fusion re-deciding
//! across every boundary it moves. Adversarial unit fixtures already
//! show residue (four order-convergence tests fail with the ramp on);
//! this measures whether the REAL workload hits it. The log is staged on
//! one branch as a chain, one transaction per link, every link after the
//! first amending the tip, so every arm mints the same version: arms
//! canonicalize after every link, every fifth link, or only the last, and
//! their trees are compared.
//!
//! `DIALOG_CONVERGE_ONLY=n` runs the single arm canonicalizing every `n`
//! links. `DIALOG_CONVERGE_SCAN` checks the stored-leaf invariants after
//! every link; `DIALOG_CONVERGE_DIFF` dumps each arm's leaf partition.
//!
//! ```sh
//! DIALOG_TREE_PACING_RAMP=200 cargo run --release -p dialog-baseline \
//!   --example converge_check -- 10000
//! ```

use dialog_artifacts::{Datum, Key, State};
use dialog_baseline::changes_of;
use dialog_baseline::nodes::walk;
use dialog_baseline::repo::{DialogRepo, VolatileRepo};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_repository::TransactionBatch;
use dialog_search_tree::{
    Buffer as TreeBuffer, Distribution as _, LoadBlock, NodeBody, Value as _,
};

/// Walks the tree under `root` and returns every stored-leaf invariant
/// violation: a non-final leaf whose terminal coin is unfunded, or an
/// interior entry whose coin cuts. The canonical-edit machinery must keep
/// stored leaves free of both at every step.
async fn leaf_violations<Env>(env: &Env, root: Blake3Hash) -> anyhow::Result<Vec<String>>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    let mut ordered: Vec<(Vec<u8>, bool, bool)> = Vec::new();
    let mut forced_links = 0usize;
    walk(env, root, |visit| {
        let manifest = visit.node.manifest()?;
        match visit.node.body() {
            NodeBody::Index(index) => {
                for at in 0..index.len() {
                    if index.separator(at)?.len() > manifest.max_separator as usize {
                        forced_links += 1;
                    }
                }
            }
            NodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                let mut leaf: Vec<(Vec<u8>, bool)> = Vec::new();
                while let Some((at, key)) = keys.next_key()? {
                    let value: State<Datum> =
                        dialog_search_tree::into_owned(segment.value_at(at)?)?;
                    let charge = key.len() + value.payload_weight() + manifest.entry_overhead();
                    let cut = dialog_search_tree::Geometric::leaf_cut(key, charge, &manifest);
                    leaf.push((key.to_vec(), cut));
                }
                let len = leaf.len();
                for (at, (key, cut)) in leaf.into_iter().enumerate() {
                    ordered.push((key, cut, at + 1 == len));
                }
            }
        }
        Ok(())
    })
    .await?;
    let mut violations = Vec::new();
    if forced_links > 0 {
        violations.push(format!("forced-links {forced_links}"));
    }
    for (at, (key, cut, terminal)) in ordered.iter().enumerate() {
        let global_last = at + 1 == ordered.len();
        if *terminal && !cut && !global_last {
            violations.push(format!("open-terminal {}", hex_prefix(key, 24)));
        }
        if !terminal && *cut {
            violations.push(format!("missing-cut {}", hex_prefix(key, 24)));
        }
    }
    Ok(violations)
}

fn hex_prefix(bytes: &[u8], take: usize) -> String {
    bytes
        .iter()
        .take(take)
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn hex(hash: &Blake3Hash) -> String {
    hex_prefix(hash.as_bytes(), usize::MAX)
}

/// Stages `log` on `repo`'s branch as one chain, canonicalizing every
/// `every` links and after the last, checking the stored-leaf invariants
/// after every link when `DIALOG_CONVERGE_SCAN` is set.
async fn stage(repo: &VolatileRepo, log: &SeLog, every: usize) -> anyhow::Result<TransactionBatch> {
    let scan = std::env::var("DIALOG_CONVERGE_SCAN").is_ok();
    let last = log.transactions.len().saturating_sub(1);
    let mut tip: Option<TransactionBatch> = None;
    let mut last_violations: Vec<String> = Vec::new();
    for (at, commit) in log.transactions.iter().enumerate() {
        let canonicalize = (at + 1).is_multiple_of(every) || at == last;
        let changes = changes_of(se_instructions(commit)?);
        let link = repo.stage_link(tip, changes, canonicalize).await?;
        if scan {
            let root = Blake3Hash::from(*link.revision().tree.hash());
            let violations = leaf_violations(&repo.index(), root).await?;
            if violations != last_violations {
                println!("  SCAN every={every} txn={at}: {violations:?}");
                last_violations = violations;
            }
        }
        tip = Some(link);
    }
    tip.ok_or_else(|| anyhow::anyhow!("the log has no transactions"))
}

/// The arm's canonical tree root, its entry count, and a digest over its
/// keys and values in order. The digest separates "different entries" (a
/// data bug) from "same entries, different shape" (a history-independence
/// break).
async fn arm(
    repo: &VolatileRepo,
    log: &SeLog,
    every: usize,
) -> anyhow::Result<(Blake3Hash, usize, Blake3Hash)> {
    let batch = stage(repo, log, every).await?;
    let root = Blake3Hash::from(*batch.revision().tree.hash());
    let diff = std::env::var("DIALOG_CONVERGE_DIFF").is_ok();
    let mut keyroll: Vec<u8> = Vec::new();
    let mut entries = 0usize;
    walk(&repo.index(), root.clone(), |visit| {
        let NodeBody::Segment(segment) = visit.node.body() else {
            return Ok(());
        };
        let manifest = visit.node.manifest()?;
        let mut leaf_entries = 0usize;
        let mut first: Option<Vec<u8>> = None;
        let mut coins: Vec<(Vec<u8>, bool)> = Vec::new();
        let mut keys = segment.keys::<Key>()?;
        while let Some((at, key)) = keys.next_key()? {
            if first.is_none() {
                first = Some(key.to_vec());
            }
            keyroll.extend_from_slice(key);
            // Values ride the digest too: a same-key different-value
            // divergence changes the coin's weight charge and is a DATA
            // bug, which a key-only digest would misclassify as shape-only.
            let value: State<Datum> = dialog_search_tree::into_owned(segment.value_at(at)?)?;
            keyroll.extend_from_slice(format!("{value:?}").as_bytes());
            if diff {
                // The production coin charge: key bytes + payload weight +
                // per-entry encoding overhead.
                let charge = key.len() + value.payload_weight() + manifest.entry_overhead();
                let cut = dialog_search_tree::Geometric::leaf_cut(key, charge, &manifest);
                coins.push((key.to_vec(), cut));
            }
            entries += 1;
            leaf_entries += 1;
        }
        if diff {
            // Canonicality census: every stored leaf must end at a cutting
            // entry (unless it is the global last leaf) and contain no
            // interior cutting entry. Violations name the arm holding a
            // stale shape.
            for (at, (key, cut)) in coins.iter().enumerate() {
                let terminal = at + 1 == coins.len();
                if *cut && !terminal {
                    println!(
                        "  MISSING-CUT every={every} at={at}/{leaf_entries} key={}",
                        hex_prefix(key, 16)
                    );
                }
                if terminal && !*cut {
                    println!(
                        "  OPEN-TERMINAL every={every} entries={leaf_entries} key={}",
                        hex_prefix(key, 16)
                    );
                }
            }
            // The leaf partition, one line per leaf: the stored separator
            // length marks forced pieces (longer than the max_separator
            // bound is the self-identifying forced seam), the first-key
            // prefix aligns the arms.
            println!(
                "  LEAF every={every} sep_len={} entries={leaf_entries} bytes={} first={}",
                visit.separator.len(),
                visit.size,
                first
                    .as_deref()
                    .map(|key| hex_prefix(key, 12))
                    .unwrap_or_default(),
            );
        }
        Ok(())
    })
    .await?;
    let digest = TreeBuffer::from(keyroll).blake3_hash().clone();
    Ok((root, entries, digest))
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10000);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Every arm stages on the same unpublished head, so every arm mints
        // the same version and only the canonicalization points differ.
        let repo = DialogRepo::volatile().await?;
        if let Ok(only) = std::env::var("DIALOG_CONVERGE_ONLY") {
            let every: usize = only.parse().unwrap_or(5);
            let (root, _, _) = arm(&repo, &log, every).await?;
            println!("every={every}: root {}", hex(&root));
            return Ok(());
        }
        let per_link = arm(&repo, &log, 1).await?;
        let by_five = arm(&repo, &log, 5).await?;
        let last = arm(&repo, &log, usize::MAX).await?;
        println!(
            "txns={} facts={}\n  every link  : root {} / {} entries, digest {}\n  every fifth : root {} / {} entries, digest {}\n  last only   : root {} / {} entries, digest {}",
            log.transactions.len(),
            log.fact_count(),
            hex(&per_link.0),
            per_link.1,
            hex(&per_link.2),
            hex(&by_five.0),
            by_five.1,
            hex(&by_five.2),
            hex(&last.0),
            last.1,
            hex(&last.2),
        );
        if per_link.0 == by_five.0 && by_five.0 == last.0 {
            println!("CONVERGED: every arm canonicalizes to the same root");
        } else if per_link.2 == by_five.2 && by_five.2 == last.2 {
            println!("DIVERGED (shape only): same entries, different canonical roots");
        } else {
            println!("DIVERGED (data): the stored entries themselves differ");
        }
        Ok(())
    })
}
