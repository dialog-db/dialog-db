//! Adversarial convergence soak: the program harness pointed at the parts
//! of the shape machinery that ordinary workloads barely touch.
//!
//! The default-manifest workloads (SE replay, the program harness) run
//! with `max_separator = 512`, so forced-long separators — and with them
//! the forced-run merge, the widen/quiet-check regimes, and the anchor
//! election edge cases — are rare events. This soak drives them
//! CONSTANTLY: keys are long (40 bytes) with deep shared prefixes, and the
//! manifest matrix includes `max_separator` values far below the key
//! length, so most seams derive forced-long separators; `max_segment` is
//! shrunk so trees get real depth at test-sized op counts.
//!
//! Executors and oracles are the program harness's: canonical sequential
//! edits vs buffered writes across op-buffer sizes and persist cadences,
//! canonical roots compared at checkpoints, the canonical-form validator
//! run on every checkpoint tree first.
//!
//! Scale knobs: `DIALOG_ADVERSARIAL_SEEDS`, `DIALOG_ADVERSARIAL_OPS`.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_common::Blake3Hash;
use dialog_search_tree::{
    Buffer, ContentAddressedStorage, Delta, HitchhikerTree, Manifest, PersistentTree, TransientTree,
};
use dialog_storage::MemoryStorageBackend;

type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
type TreeKey = [u8; 40];
type Tree = PersistentTree<TreeKey, Vec<u8>>;

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(TreeKey, u8),
    Delete(TreeKey),
}

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

/// A 40-byte key with a deep shared prefix: `band` picks the first byte
/// (a short-separator seam between bands), `cluster` a 3-byte middle, and
/// only the final 4 bytes vary within a cluster — so separators between
/// same-cluster neighbors need 37+ bytes and exceed every small
/// `max_separator` in the matrix.
fn make_key(band: u8, cluster: u32, suffix: u32) -> TreeKey {
    let mut key = [0x61u8; 40];
    key[0] = band;
    key[1..4].copy_from_slice(&cluster.to_be_bytes()[1..]);
    key[36..40].copy_from_slice(&suffix.to_be_bytes());
    key
}

/// Conjunction-sampled ops over the long-key space: scatter across bands,
/// deep clusters, churn of recent keys, min-churn of the smallest.
fn generate(seed: u64, op_count: usize) -> Program {
    let mut rng = 0xA24BAED4963EE407u64 ^ seed;
    let mut ops = Vec::with_capacity(op_count);
    let mut live: Vec<TreeKey> = Vec::new();
    for _ in 0..op_count {
        let pattern = xorshift(&mut rng) % 10;
        let op = match pattern {
            // scatter: any band, any cluster, any suffix
            0..=2 => Op::Insert(
                make_key(
                    (xorshift(&mut rng) % 4) as u8,
                    xorshift(&mut rng) % 8,
                    xorshift(&mut rng) % 10_000,
                ),
                (xorshift(&mut rng) % 60) as u8,
            ),
            // deep cluster: one band, one cluster, dense suffixes — the
            // forced-run factory
            3..=6 => Op::Insert(
                make_key(1, seed as u32 % 4, xorshift(&mut rng) % 400),
                (xorshift(&mut rng) % 60) as u8,
            ),
            // churn: rewrite or delete a recent key
            7 | 8 if !live.is_empty() => {
                let victim = live[(xorshift(&mut rng) as usize) % live.len()];
                if xorshift(&mut rng).is_multiple_of(2) {
                    Op::Delete(victim)
                } else {
                    Op::Insert(victim, (xorshift(&mut rng) % 60) as u8)
                }
            }
            // min-churn: delete the smallest live key (boundary moves)
            9 if !live.is_empty() && xorshift(&mut rng).is_multiple_of(2) => {
                Op::Delete(*live.iter().min().expect("non-empty"))
            }
            // absent-churn: delete a key that was never inserted — a
            // buffered retract with nothing to hit must vanish without
            // trace on every surface
            9 => Op::Delete(make_key(3, 7, 999_000 + xorshift(&mut rng) % 50)),
            // idempotent rewrite: same key, same value — the no-change
            // edit that quiet checks and fast paths key on
            _ if !live.is_empty() => {
                let victim = live[(xorshift(&mut rng) as usize) % live.len()];
                Op::Insert(victim, 7)
            }
            _ => Op::Insert(make_key(0, 0, xorshift(&mut rng) % 100), 1),
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

/// The manifest matrix: `max_separator` far below the key length so most
/// seams force, and small segments so depth appears fast. The default
/// manifest rides along as the control arm.
fn manifests() -> Vec<(&'static str, Manifest)> {
    let tight = Manifest {
        max_separator: 8,
        max_segment: 2048,
        ..Manifest::default()
    };
    let mid = Manifest {
        max_separator: 32,
        max_segment: 4096,
        ..Manifest::default()
    };
    // Small separator bound with DEFAULT segments: long forced runs inside
    // few, large leaves — the widen windows at their longest.
    let long_runs = Manifest {
        max_separator: 8,
        ..Manifest::default()
    };
    vec![
        ("max_sep=8/seg=2k", tight),
        ("max_sep=32/seg=4k", mid),
        ("max_sep=8/seg=default", long_runs),
        ("default", Manifest::default()),
    ]
}

async fn settle(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Store) -> Result<()> {
    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await?;
    }
    Ok(())
}

async fn checkpoint_root(tree: &Tree, storage: &Store, label: &str) -> Result<Blake3Hash> {
    let divergences = tree.canonical_divergences(storage).await?;
    assert_eq!(
        divergences,
        Vec::<String>::new(),
        "{label}: canonical-form validation failed"
    );
    Ok(tree.root().clone())
}

/// Canonical sequential edits under `manifest`: the first write stamps it,
/// every later edit recovers it from the root.
async fn run_canonical(program: &Program, manifest: Manifest) -> Result<Vec<Blake3Hash>> {
    let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut tree = Tree::empty();
    let mut roots = Vec::new();
    for (at, op) in program.ops.iter().enumerate() {
        let mut delta = Delta::zero();
        let edit = if tree.stored_root().is_none() {
            TransientTree::empty_with_manifest(Default::default(), manifest)
        } else {
            tree.edit_with_manifest(&storage).await?
        };
        tree = match *op {
            Op::Insert(key, len) => {
                edit.insert(key, vec![key[39]; len as usize + 1], &storage)
                    .await?
            }
            Op::Delete(key) => edit.delete(&key, &storage).await?,
        }
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        if program.checkpoints.contains(&(at + 1)) {
            roots.push(checkpoint_root(&tree, &storage, "canonical").await?);
        }
    }
    Ok(roots)
}

/// Buffered writes under `manifest` with periodic persist + reopen,
/// canonicalizing (and continuing) at checkpoints.
async fn run_buffered(
    program: &Program,
    manifest: Manifest,
    // One entry per session: each persist-and-reopen advances to the next
    // buffer size (wrapping), so a single lifecycle can change its op
    // buffer between sessions — the upgrade scenario.
    op_bufs: &[Option<usize>],
    persist_every: usize,
) -> Result<Vec<Blake3Hash>> {
    let mut session = 0usize;
    let mut next_buf = move || {
        let chosen = op_bufs[session % op_bufs.len()];
        session += 1;
        chosen
    };
    let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

    // Stamp the manifest with the first op through a canonical edit, so the
    // buffered session opens over a root that carries it.
    let mut ops = program.ops.iter();
    let first = *ops.next().expect("programs are non-empty");
    let mut delta = Delta::zero();
    let seed_edit =
        TransientTree::empty_with_manifest(Default::default(), manifest);
    let mut tree = match first {
        Op::Insert(key, len) => {
            seed_edit
                .insert(key, vec![key[39]; len as usize + 1], &storage)
                .await?
        }
        Op::Delete(key) => seed_edit.delete(&key, &storage).await?,
    }
    .persist(&mut delta)?;
    settle(&mut delta, &mut storage).await?;

    // Pin the manifest on every session: the manifest travels in the tree,
    // and a session opened over an EMPTIED tree would otherwise silently
    // write under the default — the divergence this soak caught.
    let open = |tree: &Tree, op_buf: Option<usize>| {
        let buffered = HitchhikerTree::open(tree).with_manifest(manifest);
        match op_buf {
            Some(size) => buffered.with_op_buf_size(size),
            None => buffered,
        }
    };
    let mut roots = Vec::new();
    if program.checkpoints.contains(&1) {
        roots.push(checkpoint_root(&tree, &storage, "buffered/first").await?);
    }
    let mut buffered = open(&tree, next_buf());
    for (at, op) in ops.enumerate() {
        buffered = match *op {
            Op::Insert(key, len) => {
                buffered
                    .insert(key, vec![key[39]; len as usize + 1], &storage)
                    .await?
            }
            Op::Delete(key) => buffered.delete(key, &storage).await?,
        };
        let done = at + 2;
        if program.checkpoints.contains(&done) {
            let mut delta = Delta::zero();
            tree = buffered.canonicalize(&storage, &mut delta).await?;
            settle(&mut delta, &mut storage).await?;
            roots.push(checkpoint_root(&tree, &storage, "buffered checkpoint").await?);
            buffered = open(&tree, next_buf());
        } else if done.is_multiple_of(persist_every) {
            let mut delta = Delta::zero();
            let root = buffered.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            tree = Tree::from_hash_with_cache(root, Default::default());
            buffered = open(&tree, next_buf());
        }
    }
    Ok(roots)
}

fn knob(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// Under every manifest in the matrix, every executor surface must agree
/// on every checkpoint's canonical root, and every checkpoint tree must be
/// THE canonical tree — with forced-long separators the common case, not
/// the exception.
#[tokio::test]
async fn it_converges_under_adversarial_manifests() -> Result<()> {
    let seeds = knob("DIALOG_ADVERSARIAL_SEEDS", 3) as u64;
    let op_count = knob("DIALOG_ADVERSARIAL_OPS", 180);
    for (label, manifest) in manifests() {
        for seed in 0..seeds {
            let program = generate(seed, op_count);
            let canonical = run_canonical(&program, manifest).await?;
            let tiny = run_buffered(&program, manifest, &[Some(4)], 13).await?;
            let medium = run_buffered(&program, manifest, &[Some(32)], 7).await?;
            let default_buf = run_buffered(&program, manifest, &[None], 50).await?;
            let mixed = run_buffered(&program, manifest, &[Some(4), Some(32), None], 11).await?;
            for (arm, roots) in [
                ("buffered(4)/persist-13", &tiny),
                ("buffered(32)/persist-7", &medium),
                ("buffered(default)/persist-50", &default_buf),
                ("buffered(mixed 4/32/default)/persist-11", &mixed),
            ] {
                assert_eq!(
                    roots, &canonical,
                    "manifest {label}, seed {seed}: {arm} diverged from canonical"
                );
            }
        }
    }
    Ok(())
}

/// Minimizer for a caught divergence: finds the shortest op prefix where
/// an executor arm's canonicalized entry set differs from canonical, then
/// prints the entry-level diff. Ignored by default; run explicitly while
/// hunting.
#[tokio::test]
#[ignore]
async fn minimize_caught_divergence() -> Result<()> {
    use dialog_search_tree::ContentAddressedStorage as Cas;
    use futures_util::StreamExt as _;

    let manifest = manifests()
        .into_iter()
        .find(|(label, _)| *label == "max_sep=8/seg=2k")
        .expect("manifest present")
        .1;
    let seed = 14u64;
    let full = generate(seed, 1200);

    async fn entries_of(tree: &Tree, storage: &Store) -> Result<Vec<(TreeKey, Vec<u8>)>> {
        let mut out = Vec::new();
        let stream = tree.stream_range(.., storage);
        futures_util::pin_mut!(stream);
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            out.push((entry.key, entry.value));
        }
        Ok(out)
    }

    let _ = |storage: &Cas<MemoryStorageBackend<Blake3Hash, Vec<u8>>>| storage.clone();

    // Find the shortest prefix (scanning coarsely then finely) where the
    // mixed arm diverges.
    let mut bad = None;
    let mut len = 8;
    while len <= full.ops.len() {
        let program = Program {
            ops: full.ops[..len].to_vec(),
            checkpoints: vec![len],
        };
        let canonical = run_canonical(&program, manifest).await?;
        let mixed = run_buffered(&program, manifest, &[Some(4), Some(32), None], 11).await?;
        if canonical != mixed {
            bad = Some(len);
            break;
        }
        len += 8;
    }
    let Some(bad_len) = bad else {
        println!("no divergence up to {} ops", full.ops.len());
        return Ok(());
    };
    // Tighten to the exact op.
    let mut lo = bad_len - 8;
    while lo < bad_len {
        let probe = lo + 1;
        let program = Program {
            ops: full.ops[..probe].to_vec(),
            checkpoints: vec![probe],
        };
        let canonical = run_canonical(&program, manifest).await?;
        let mixed = run_buffered(&program, manifest, &[Some(4), Some(32), None], 11).await?;
        if canonical != mixed {
            break;
        }
        lo = probe;
    }
    let minimal = lo + 1;
    println!("first divergent prefix: {minimal} ops");
    for (at, op) in full.ops[..minimal].iter().enumerate() {
        match op {
            Op::Insert(key, len) => println!(
                "  {at}: INSERT band={} cluster={:?} suffix={:?} len={len}",
                key[0],
                &key[1..4],
                &key[36..40]
            ),
            Op::Delete(key) => println!(
                "  {at}: DELETE band={} cluster={:?} suffix={:?}",
                key[0],
                &key[1..4],
                &key[36..40]
            ),
        }
    }

    // Entry-level diff at the divergent prefix.
    let program = Program {
        ops: full.ops[..minimal].to_vec(),
        checkpoints: vec![minimal],
    };
    // Re-run both arms, retaining the trees this time.
    let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut tree = Tree::empty();
    for op in &program.ops {
        let mut delta = Delta::zero();
        let edit = if tree.stored_root().is_none() {
            TransientTree::empty_with_manifest(Default::default(), manifest)
        } else {
            tree.edit_with_manifest(&storage).await?
        };
        tree = match *op {
            Op::Insert(key, len) => {
                edit.insert(key, vec![key[39]; len as usize + 1], &storage)
                    .await?
            }
            Op::Delete(key) => edit.delete(&key, &storage).await?,
        }
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
    }
    let canonical_entries = entries_of(&tree, &storage).await?;

    // Mixed arm, retaining the final canonicalized tree: reuse run_buffered
    // by reading back its root through a fresh replay with the same store
    // is intrusive; instead re-run its logic inline at this small size.
    let mut storage2: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut ops2 = program.ops.iter();
    let first = *ops2.next().expect("non-empty");
    let mut delta = Delta::zero();
    let seed_edit =
        TransientTree::empty_with_manifest(Default::default(), manifest);
    let mut tree2 = match first {
        Op::Insert(key, len) => {
            seed_edit
                .insert(key, vec![key[39]; len as usize + 1], &storage2)
                .await?
        }
        Op::Delete(key) => seed_edit.delete(&key, &storage2).await?,
    }
    .persist(&mut delta)?;
    settle(&mut delta, &mut storage2).await?;
    let bufs = [Some(4usize), Some(32), None];
    let mut session = 0usize;
    let open = |tree: &Tree, buf: Option<usize>| {
        let buffered = HitchhikerTree::open(tree).with_manifest(manifest);
        match buf {
            Some(size) => buffered.with_op_buf_size(size),
            None => buffered,
        }
    };
    let mut buffered = open(&tree2, bufs[session % bufs.len()]);
    session += 1;
    for (at, op) in ops2.enumerate() {
        buffered = match *op {
            Op::Insert(key, len) => {
                buffered
                    .insert(key, vec![key[39]; len as usize + 1], &storage2)
                    .await?
            }
            Op::Delete(key) => buffered.delete(key, &storage2).await?,
        };
        let done = at + 2;
        if done == program.ops.len() {
            let mut delta = Delta::zero();
            tree2 = buffered.canonicalize(&storage2, &mut delta).await?;
            settle(&mut delta, &mut storage2).await?;
            break;
        } else if done.is_multiple_of(11) {
            let mut delta = Delta::zero();
            let root = buffered.persist(&mut delta)?;
            settle(&mut delta, &mut storage2).await?;
            tree2 = Tree::from_hash_with_cache(root, Default::default());
            buffered = open(&tree2, bufs[session % bufs.len()]);
            session += 1;
        }
    }
    let mixed_entries = entries_of(&tree2, &storage2).await?;

    println!(
        "canonical entries: {}, mixed entries: {}",
        canonical_entries.len(),
        mixed_entries.len()
    );
    for entry in &canonical_entries {
        if !mixed_entries.contains(entry) {
            println!(
                "  MISSING from mixed: band={} cluster={:?} suffix={:?} value_len={}",
                entry.0[0],
                &entry.0[1..4],
                &entry.0[36..40],
                entry.1.len()
            );
        }
    }
    for entry in &mixed_entries {
        if !canonical_entries.contains(entry) {
            println!(
                "  PHANTOM in mixed: band={} cluster={:?} suffix={:?} value_len={}",
                entry.0[0],
                &entry.0[1..4],
                &entry.0[36..40],
                entry.1.len()
            );
        }
    }
    Ok(())
}
