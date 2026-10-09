//! Adversarial branch-level convergence: randomized
//! assert/replace/retract programs with spill-boundary values, run under
//! several commit groupings and lifecycles, all required to converge.
//!
//! This covers the conjunction the SE-log convergence test cannot:
//! cardinality-one supersession chains (`Replace` with cause chaining),
//! explicit retracts (including retracts of absent facts), and values AT
//! and around the spill threshold, where a value's encoding flips between
//! inline and a prefix + content-hash reference to a staged blob. The
//! read-back oracle materializes every fact through the public select
//! path, so a supersession bug that drops or orphans a spilled value
//! surfaces as a hard error, not just a digest mismatch.
//!
//! Knobs: `DIALOG_ARTIFACT_FUZZ_SEEDS`, `DIALOG_ARTIFACT_FUZZ_OPS`.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::ArtifactTree;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Entity, Instruction, Relation, Value, sort_key,
};
use dialog_baseline::changes_of;
use dialog_baseline::repo::{DialogRepo, VolatileRepo};
use dialog_common::Blake3Hash as NodeHash;
use dialog_repository::TransactionBatch;
use dialog_search_tree::Manifest;
use futures_util::TryStreamExt as _;

fn xorshift(state: &mut u64) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    (*state >> 32) as u32
}

fn entity(n: u32) -> Entity {
    Entity::from_str(&format!("entity:fuzz-{n:03}")).expect("valid entity")
}

fn attribute(n: u32) -> Relation {
    Relation::from_str(&format!("fuzz/attr{n}")).expect("valid attribute")
}

/// Values straddling every encoding decision: tiny inline strings, strings
/// AT and around the 4096-byte spill threshold, big definitely-spilled
/// strings, numerics, and entity references. The spill-boundary trio is
/// the point: one byte decides whether the value lives in the key or in an
/// archive block.
fn value(rng: &mut u64) -> Value {
    match xorshift(rng) % 8 {
        0 => Value::String(format!("v{}", xorshift(rng) % 50)),
        1 => Value::String("x".repeat(4095)),
        2 => Value::String("x".repeat(4096)),
        3 => Value::String("x".repeat(4097)),
        4 => Value::String(format!("{}{}", "y".repeat(8000), xorshift(rng) % 8)),
        5 => Value::UnsignedInt(u128::from(xorshift(rng) % 1000)),
        6 => Value::Entity(entity(xorshift(rng) % 30)),
        _ => Value::Boolean(xorshift(rng).is_multiple_of(2)),
    }
}

/// A seeded instruction program over small entity/attribute pools, with an
/// approximate live-set so retracts usually target real facts — and
/// sometimes deliberately absent ones.
fn generate(seed: u64, op_count: usize) -> Vec<Instruction> {
    let mut rng = 0xC2B2AE3D27D4EB4Fu64 ^ seed;
    let mut live: Vec<Artifact> = Vec::new();
    let mut ops = Vec::with_capacity(op_count);
    for _ in 0..op_count {
        let of = entity(xorshift(&mut rng) % 30);
        let the = attribute(xorshift(&mut rng) % 5);
        let instruction = match xorshift(&mut rng) % 10 {
            // Assert: cardinality-many additive facts.
            0..=3 => {
                let artifact = Artifact {
                    the,
                    of,
                    is: value(&mut rng),
                    cause: None,
                };
                live.push(artifact.clone());
                Instruction::Assert(artifact, dialog_artifacts::Pick::All)
            }
            // Replace: cardinality-one supersession (cause chains from the
            // superseded fact — the ordering-sensitive path).
            4..=6 => {
                let artifact = Artifact {
                    the: the.clone(),
                    of: of.clone(),
                    is: value(&mut rng),
                    cause: None,
                };
                // A `last` write retires the one claim the read elects,
                // which the model can name only while the cell holds at
                // most one; a fuller cell takes an append instead.
                let held = live
                    .iter()
                    .filter(|held| held.the == the && held.of == of)
                    .count();
                if held <= 1 {
                    live.retain(|held| !(held.the == the && held.of == of));
                    live.push(artifact.clone());
                    Instruction::Assert(artifact, dialog_artifacts::Pick::Last)
                } else {
                    live.push(artifact.clone());
                    Instruction::Assert(artifact, dialog_artifacts::Pick::All)
                }
            }
            // Retract a real fact when one exists...
            7 | 8 if !live.is_empty() => {
                let at = (xorshift(&mut rng) as usize) % live.len();
                let artifact = live.swap_remove(at);
                Instruction::Retract(artifact)
            }
            // ...and sometimes an absent one: a retract with nothing to hit
            // must vanish identically under every grouping.
            _ => Instruction::Retract(Artifact {
                the,
                of,
                is: Value::String("never-asserted".into()),
                cause: None,
            }),
        };
        ops.push(instruction);
    }
    ops
}

/// Every fuzzed fact.
fn everything() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().of_starting_with("entity:fuzz-")
}

/// The facts' fingerprint: every row, in one fixed order. Any fixed order
/// works; one fixed format keeps it a function of the facts alone,
/// whatever format an arm's tree uses.
fn fingerprint(mut rows: Vec<Artifact>) -> Vec<String> {
    let order = Manifest::default();
    rows.sort_by_cached_key(|row| sort_key(row, &order));
    rows.iter()
        .map(|row| format!("{} {} {:?}", row.the, row.of, row.is))
        .collect()
}

/// Replays the program (regenerated from its seed, since `Instruction` is
/// not `Clone`) on a fresh branch, publishing one commit per `group`
/// instructions, and reads every fact back through the public select path:
/// spilled values must resolve, so a dropped or orphaned blob errors here.
async fn publish_grouped(seed: u64, op_count: usize, group: usize) -> Result<Vec<String>> {
    let repo = DialogRepo::volatile().await?;
    let ops = generate(seed, op_count).into_iter().map(|op| vec![op]);
    repo.publish_grouped(ops, group).await?;
    Ok(fingerprint(repo.collect(everything()).await?))
}

/// Stages the program on `repo`'s branch as one chain, one instruction per
/// link: the first commits, every later one amends the tip. Canonicalizes
/// every `canonicalize_every` links and after the last.
async fn stage(
    repo: &VolatileRepo,
    seed: u64,
    op_count: usize,
    canonicalize_every: usize,
) -> Result<TransactionBatch> {
    let links = generate(seed, op_count)
        .into_iter()
        .map(|op| changes_of([op]));
    repo.stage(links, canonicalize_every).await
}

/// Every fact a staged batch holds, read back through the public select
/// path.
async fn staged_facts(repo: &VolatileRepo, batch: &TransactionBatch) -> Result<Vec<String>> {
    let rows = batch
        .claims()
        .select(everything())
        .to_owned()
        .perform(repo.operator())
        .await?;
    Ok(fingerprint(rows.try_collect().await?))
}

fn knob(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn seeds() -> u64 {
    knob("DIALOG_ARTIFACT_FUZZ_SEEDS", 3) as u64
}

fn op_count() -> usize {
    knob("DIALOG_ARTIFACT_FUZZ_OPS", 120)
}

/// Every grouping of the same instruction program, published or staged as
/// an amended chain, must agree on the facts read back through the public
/// API, including spilled-value resolution.
#[tokio::test]
async fn it_converges_across_groupings_with_supersession_and_spills() -> Result<()> {
    let op_count = op_count();
    for seed in 0..seeds() {
        let reference = publish_grouped(seed, op_count, 1).await?;
        assert!(
            !reference.is_empty(),
            "seed {seed}: the program leaves facts"
        );
        for (label, group) in [
            ("group=2", 2usize),
            ("group=3", 3),
            ("group=7", 7),
            ("single", usize::MAX),
        ] {
            let arm = publish_grouped(seed, op_count, group).await?;
            assert_eq!(
                arm, reference,
                "seed {seed}, {label}: FACT SETS diverge from per-op commits (data bug)"
            );
        }

        let repo = DialogRepo::volatile().await?;
        let amended = stage(&repo, seed, op_count, usize::MAX).await?;
        assert_eq!(
            staged_facts(&repo, &amended).await?,
            reference,
            "seed {seed}: the amended chain's FACT SET diverges from per-op commits"
        );
    }
    Ok(())
}

/// One amended chain must reach the same canonical tree whether it
/// canonicalizes after every link, every fourth, or only the last; and the
/// tree must BE canonical.
#[tokio::test]
async fn it_converges_across_canonicalization_points_with_supersession_and_spills() -> Result<()> {
    let op_count = op_count();
    for seed in 0..seeds() {
        // The chains stage on the same unpublished head, so they mint the
        // same version and differ only in when they canonicalized.
        let repo = DialogRepo::volatile().await?;
        let reference = stage(&repo, seed, op_count, usize::MAX).await?;
        for (label, every) in [("every link", 1usize), ("every fourth link", 4)] {
            let arm = stage(&repo, seed, op_count, every).await?;
            assert_eq!(arm.version(), reference.version());
            assert_eq!(
                arm.revision().tree,
                reference.revision().tree,
                "seed {seed}, canonicalized {label}: same facts, different \
                 canonical tree (history-independence break)"
            );
        }

        let tree = ArtifactTree::from_hash(NodeHash::from(*reference.revision().tree.hash()));
        let divergences = tree.canonical_divergences(&repo.index()).await?;
        assert_eq!(
            divergences,
            Vec::<String>::new(),
            "seed {seed}: canonical-form validation failed"
        );
    }
    Ok(())
}
