//! Adversarial artifacts-level convergence: randomized
//! assert/replace/retract programs with spill-boundary values, run under
//! several commit groupings and lifecycles, all required to converge.
//!
//! This covers the conjunction the SE-log convergence test cannot:
//! cardinality-one supersession chains (`Replace` with cause chaining),
//! explicit retracts (including retracts of absent facts), and values AT
//! and around the spill threshold — where a value's encoding flips between
//! inline and a prefix + content-hash reference to an archive block. The
//! read-back oracle materializes every fact through the public select
//! path, so a supersession bug that drops or orphans a spilled block
//! surfaces as a hard error, not just a digest mismatch.
//!
//! Knobs: `DIALOG_ARTIFACT_FUZZ_SEEDS`, `DIALOG_ARTIFACT_FUZZ_OPS`.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStoreMut as _, ArtifactViewStream as _, Artifacts,
    Attribute, Datum, Entity, IndexRoot, Instruction, Key, State, Value, default_sort_key,
};
use dialog_common::Blake3Hash as NodeHash;
use dialog_search_tree::{ContentAddressedStorage, PersistentTree};
use dialog_storage::{
    Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend as _,
};
use futures_util::{TryStreamExt as _, stream};

fn xorshift(state: &mut u64) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    (*state >> 32) as u32
}

fn entity(n: u32) -> Entity {
    Entity::from_str(&format!("entity:fuzz-{n:03}")).expect("valid entity")
}

fn attribute(n: u32) -> Attribute {
    Attribute::from_str(&format!("fuzz/attr{n}")).expect("valid attribute")
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
                Instruction::Assert(artifact)
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
                live.retain(|held| !(held.the == the && held.of == of));
                live.push(artifact.clone());
                Instruction::Replace(artifact)
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

/// Replays the program (regenerated from its seed, since `Instruction`
/// is not `Clone`) committing every `group` instructions (with an
/// optional canonicalize every `canonicalize_every` commits), then
/// canonicalizes and reports (root, sorted-fact digest via the public read
/// path, count) plus a clean canonical-form validation.
async fn replay(
    seed: u64,
    op_count: usize,
    group: usize,
    canonicalize_every: Option<usize>,
) -> Result<(Blake3Hash, Vec<u8>, usize)> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend.clone()).await?;

    let mut ops = generate(seed, op_count);
    let mut commits = 0usize;
    while !ops.is_empty() {
        let take = group.min(ops.len());
        let chunk: Vec<Instruction> = ops.drain(..take).collect();
        store.commit(stream::iter(chunk)).await?;
        commits += 1;
        if let Some(every) = canonicalize_every
            && commits.is_multiple_of(every)
        {
            store.canonicalize().await?;
        }
    }
    let revision = store.canonicalize().await?;

    // Read every fact back through the public select path: spilled values
    // must resolve (a dropped or orphaned archive block errors here), and
    // the sorted fact list is the arm's semantic fingerprint.
    let mut rows: Vec<Artifact> = store
        .select(ArtifactSelector::new().of_starting_with("entity:fuzz-"))
        .owned()
        .try_collect()
        .await?;
    rows.sort_by_key(default_sort_key);
    let mut digest = Vec::new();
    for row in &rows {
        digest.extend_from_slice(format!("{row:?}").as_bytes());
    }

    // The canonical tree must also BE the canonical tree.
    let bytes = backend
        .get(&revision)
        .await?
        .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
    let root: IndexRoot = CborEncoder.decode(&bytes).await?;
    let tree: PersistentTree<Key, State<Datum>> =
        PersistentTree::from_hash_with_cache(NodeHash::from(*root.index()), Default::default());
    let divergences = tree
        .canonical_divergences(&ContentAddressedStorage::new(TreeStorageBridge(
            backend.clone(),
        )))
        .await?;
    assert_eq!(
        divergences,
        Vec::<String>::new(),
        "group={group}: canonical-form validation failed"
    );

    Ok((
        revision,
        dialog_artifacts::make_reference(&digest).to_vec(),
        rows.len(),
    ))
}

fn knob(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// Every grouping and lifecycle of the same instruction program must agree
/// on the canonical root AND on the facts read back through the public
/// API — including spilled-value resolution.
#[tokio::test]
async fn it_converges_across_groupings_with_supersession_and_spills() -> Result<()> {
    let seeds = knob("DIALOG_ARTIFACT_FUZZ_SEEDS", 3) as u64;
    let op_count = knob("DIALOG_ARTIFACT_FUZZ_OPS", 120);
    for seed in 0..seeds {
        let reference = replay(seed, op_count, 1, None).await?;
        for (label, group, canonicalize_every) in [
            ("group=2", 2usize, None),
            ("group=3", 3, None),
            ("group=7", 7, None),
            ("single", usize::MAX, None),
            ("group=2+canonicalize-every-4", 2, Some(4)),
        ] {
            let arm = replay(seed, op_count, group, canonicalize_every).await?;
            assert_eq!(
                arm.1, reference.1,
                "seed {seed}, {label}: FACT SETS diverge from per-op commits (data bug)"
            );
            assert_eq!(arm.2, reference.2, "seed {seed}, {label}: fact count");
            assert_eq!(
                arm.0, reference.0,
                "seed {seed}, {label}: same facts, different canonical root \
                 (history-independence break)"
            );
        }
    }
    Ok(())
}
