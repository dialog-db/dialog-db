//! Program-convergence harness (v1): one serializable op program run
//! through several executor surfaces, with the oracles checked at every
//! checkpoint.
//!
//! This is the "one convergence harness" the campaign's hardening feedback
//! called for: instead of hand-compiling each regression as its own test,
//! a `Program` — seeded ops drawn from the known stress-pattern vocabulary
//! plus lifecycle markers — is executed by every write surface, and at
//! each checkpoint every executor's canonicalized tree must agree on the
//! root AND pass the canonical-form validator (which localizes any break
//! to a level and node). Adding a future bug's pattern to the vocabulary
//! institutionalizes its regression test.
//!
//! Executor surfaces covered here (all public API):
//! - canonical sequential edits, persisted and settled per op;
//! - buffered (hitchhiker) writes with small, medium, and default op
//!   buffers — different cascade timings, same canonical outcome;
//! - buffered writes with periodic persist + reopen (the commit-shaped
//!   lifecycle), including canonicalize-and-continue at checkpoints.
//!
//! Not yet covered (future harness growth): stitch/differential merges and
//! replica reconciliation orders.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_search_tree::{Buffer, ContentAddressedStorage, Delta, HitchhikerTree, PersistentTree};
use dialog_storage::MemoryStorageBackend;

type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
type Tree = PersistentTree<[u8; 4], Vec<u8>>;

/// One write op. Keys are `u32` big-endian so byte order is key order;
/// values vary in length so weight-sensitive seams are exercised.
#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(u32, u8),
    Delete(u32),
}

/// A deterministic program: ops plus checkpoint positions (after which
/// every executor's canonical form must agree).
struct Program {
    ops: Vec<Op>,
    checkpoints: Vec<usize>,
}

fn xorshift(state: &mut u64) -> u32 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    (*state >> 32) as u32
}

/// Draws a program by SAMPLING CONJUNCTIONS of the stress-pattern
/// vocabulary — every op picks a pattern, so patterns interleave rather
/// than run as separate phases (bugs live in the conjunctions):
///
/// - `scatter`: uniform keys across the space;
/// - `cluster`: keys concentrated in a narrow prefix band (deep shared
///   prefixes stress separator derivation);
/// - `churn`: delete-then-reinsert of recently written keys (boundary
///   re-decisions);
/// - `min-churn`: deletes of the smallest live key (min-move edits, the
///   boundary-reroute path).
fn generate(seed: u64, op_count: usize) -> Program {
    let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
    let mut ops = Vec::with_capacity(op_count);
    let mut live: Vec<u32> = Vec::new();
    for _ in 0..op_count {
        let pattern = xorshift(&mut rng) % 10;
        let op = match pattern {
            // scatter (4/10)
            0..=3 => Op::Insert(
                xorshift(&mut rng) % 100_000,
                (xorshift(&mut rng) % 60) as u8,
            ),
            // cluster (3/10): a narrow band around a per-seed anchor
            4..=6 => {
                let anchor = ((seed as u32) * 7919) % 90_000;
                Op::Insert(
                    anchor + xorshift(&mut rng) % 64,
                    (xorshift(&mut rng) % 60) as u8,
                )
            }
            // churn (2/10): rewrite or delete a recent key
            7 | 8 if !live.is_empty() => {
                let victim = live[(xorshift(&mut rng) as usize) % live.len()];
                if xorshift(&mut rng).is_multiple_of(2) {
                    Op::Delete(victim)
                } else {
                    Op::Insert(victim, (xorshift(&mut rng) % 60) as u8)
                }
            }
            // min-churn (1/10): delete the smallest live key
            9 if !live.is_empty() => Op::Delete(*live.iter().min().expect("non-empty")),
            _ => Op::Insert(xorshift(&mut rng) % 100_000, 1),
        };
        match op {
            Op::Insert(key, _) => {
                if !live.contains(&key) {
                    live.push(key);
                }
            }
            Op::Delete(key) => live.retain(|held| *held != key),
        }
        ops.push(op);
    }
    let checkpoints = vec![op_count / 3, (op_count * 2) / 3, op_count];
    Program { ops, checkpoints }
}

async fn settle(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Store) -> Result<()> {
    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await?;
    }
    Ok(())
}

/// What one executor reports at a checkpoint: the canonical root, checked
/// clean by the canonical-form validator before being compared.
async fn checkpoint_root(tree: &Tree, storage: &Store, label: &str) -> Result<Blake3Hash> {
    let divergences = tree.canonical_divergences(storage).await?;
    assert_eq!(
        divergences,
        Vec::<String>::new(),
        "{label}: canonical-form validation failed"
    );
    Ok(tree.root().clone())
}

/// Canonical sequential edits: persist + settle every op.
async fn run_canonical(program: &Program) -> Result<Vec<Blake3Hash>> {
    let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut tree = Tree::empty();
    let mut roots = Vec::new();
    for (at, op) in program.ops.iter().enumerate() {
        let mut delta = Delta::zero();
        let edit = tree.edit();
        tree = match *op {
            Op::Insert(key, len) => {
                edit.insert(
                    key.to_be_bytes(),
                    vec![key as u8; len as usize + 1],
                    &storage,
                )
                .await?
            }
            Op::Delete(key) => edit.delete(&key.to_be_bytes(), &storage).await?,
        }
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        if program.checkpoints.contains(&(at + 1)) {
            roots.push(checkpoint_root(&tree, &storage, "canonical").await?);
        }
    }
    Ok(roots)
}

/// Buffered writes with the given op buffer, persisting + reopening every
/// `persist_every` ops (the commit-shaped lifecycle), canonicalizing at
/// checkpoints and continuing from the canonical tree.
async fn run_buffered(
    program: &Program,
    op_buf: Option<usize>,
    persist_every: usize,
) -> Result<Vec<Blake3Hash>> {
    let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut tree = Tree::empty();
    let mut roots = Vec::new();
    let open = |tree: &Tree, op_buf: Option<usize>| {
        let buffered = HitchhikerTree::open(tree);
        match op_buf {
            Some(size) => buffered.with_op_buf_size(size),
            None => buffered,
        }
    };
    let mut buffered = open(&tree, op_buf);
    for (at, op) in program.ops.iter().enumerate() {
        buffered = match *op {
            Op::Insert(key, len) => {
                buffered
                    .insert(
                        key.to_be_bytes(),
                        vec![key as u8; len as usize + 1],
                        &storage,
                    )
                    .await?
            }
            Op::Delete(key) => buffered.delete(key.to_be_bytes(), &storage).await?,
        };
        let done = at + 1;
        if program.checkpoints.contains(&done) {
            // Canonicalize, check, and continue from the canonical tree —
            // a legitimate lifecycle, and the only point the property
            // makes a claim about.
            let mut delta = Delta::zero();
            tree = buffered.canonicalize(&storage, &mut delta).await?;
            settle(&mut delta, &mut storage).await?;
            roots.push(
                checkpoint_root(&tree, &storage, &format!("buffered(buf {op_buf:?})")).await?,
            );
            buffered = open(&tree, op_buf);
        } else if done.is_multiple_of(persist_every) {
            // Publish the buffered form and reopen over it: the persisted
            // spine round-trip every real commit performs.
            let mut delta = Delta::zero();
            let root = buffered.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            tree = Tree::from_hash_with_cache(root, Default::default());
            buffered = open(&tree, op_buf);
        }
    }
    Ok(roots)
}

/// Every executor surface must produce the same canonical root at every
/// checkpoint, and every checkpoint tree must be THE canonical tree.
#[tokio::test]
async fn it_converges_across_executor_surfaces() -> Result<()> {
    let op_count: usize = std::env::var("DIALOG_PROGRAM_OPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(240);
    for seed in 0..4u64 {
        let program = generate(seed, op_count);

        let canonical = run_canonical(&program).await?;
        let tiny = run_buffered(&program, Some(4), 13).await?;
        let medium = run_buffered(&program, Some(32), 7).await?;
        let default_buf = run_buffered(&program, None, 50).await?;

        for (arm, roots) in [
            ("buffered(4)/persist-13", &tiny),
            ("buffered(32)/persist-7", &medium),
            ("buffered(default)/persist-50", &default_buf),
        ] {
            assert_eq!(
                roots, &canonical,
                "seed {seed}: {arm} diverged from canonical sequential edits"
            );
        }
    }
    Ok(())
}
