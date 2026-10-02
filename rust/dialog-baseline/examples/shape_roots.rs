//! Deterministic tree shapes: builds trees from fixed key sequences through
//! the canonical edit path and the buffered path, and prints the root each
//! phase lands on. Two builds that print the same roots shape every one of
//! these trees byte for byte the same.
//!
//! The key sets are chosen to reach every boundary rule: ordinary keys (the
//! weight coin), clusters agreeing past the separator bound (vetoed seams and
//! the stretch backstop), and long keys (frames over the ceiling).
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example shape_roots
//! ```

use dialog_search_tree::{
    Delta, DialogSearchTreeError, HitchhikerTree, Key as TreeKey, MemoryBlocks, PersistentTree,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct VarKey(Vec<u8>);

impl AsRef<[u8]> for VarKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TreeKey for VarKey {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        Ok(VarKey(bytes.to_vec()))
    }

    fn min() -> Self {
        VarKey(Vec::new())
    }

    fn max() -> Self {
        VarKey(vec![u8::MAX; 64])
    }
}

type Tree = PersistentTree<VarKey, Vec<u8>>;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for chunk in out.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        out
    }
}

/// An ordinary fact-shaped key: a tag, one of a pool of entities, one of a
/// few attributes, and a value of varying length.
fn fact_key(rng: &mut Rng) -> VarKey {
    let mut key = vec![(rng.below(3) + 1) as u8];
    let entity = rng.below(500) as u64;
    key.extend_from_slice(b"entity:");
    key.extend_from_slice(&entity.to_be_bytes());
    key.extend_from_slice(b"/attribute-");
    key.push(b'a' + rng.below(20) as u8);
    let value = rng.below(160);
    key.extend(rng.bytes(value));
    VarKey(key)
}

/// A key in a cluster whose members agree on 700 bytes, past the separator
/// bound, so every seam inside the cluster is vetoed.
fn cluster_key(cluster: usize, member: u64) -> VarKey {
    let mut key = vec![9u8, b'A' + cluster as u8];
    key.extend(std::iter::repeat_n(b'p', 700));
    key.extend_from_slice(&member.to_be_bytes());
    VarKey(key)
}

/// A long key that diverges from every other early.
fn long_key(rng: &mut Rng) -> VarKey {
    let mut key = vec![12u8];
    let len = 1000 + rng.below(3000);
    key.extend(rng.bytes(len));
    VarKey(key)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let storage = MemoryBlocks::new();
    let mut delta = Delta::zero();

    // Ordinary keys through the canonical edit path, in small sessions.
    let mut rng = Rng::new(1);
    let facts: Vec<VarKey> = (0..6000).map(|_| fact_key(&mut rng)).collect();
    let mut tree = Tree::empty();
    for batch in facts.chunks(37) {
        let mut edit = tree.edit();
        for key in batch {
            edit = edit.insert(key.clone(), rng.bytes(24), &storage).await?;
        }
        tree = edit.persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("facts inserted      {}", hex(tree.root().as_bytes()));

    for batch in facts
        .chunks(7)
        .map(|chunk| &chunk[0])
        .collect::<Vec<_>>()
        .chunks(29)
    {
        let mut edit = tree.edit();
        for key in batch {
            edit = edit.delete(key, &storage).await?;
        }
        tree = edit.persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("facts deleted       {}", hex(tree.root().as_bytes()));

    // Near-duplicate clusters, one key a session, so every insert meets the
    // stored run the way a commit does.
    let mut members: Vec<(usize, u64)> = (0..6)
        .flat_map(|cluster| (0..300u64).map(move |member| (cluster, member * 7919 % 100_003)))
        .collect();
    for at in (1..members.len()).rev() {
        members.swap(at, rng.below(at + 1));
    }
    for (cluster, member) in &members {
        tree = tree
            .edit()
            .insert(cluster_key(*cluster, *member), rng.bytes(16), &storage)
            .await?
            .persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("clusters inserted   {}", hex(tree.root().as_bytes()));

    for (cluster, member) in members.iter().step_by(5) {
        tree = tree
            .edit()
            .delete(&cluster_key(*cluster, *member), &storage)
            .await?
            .persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("clusters thinned    {}", hex(tree.root().as_bytes()));

    // Long keys, where a handful of entries fill a segment.
    let longs: Vec<VarKey> = (0..400).map(|_| long_key(&mut rng)).collect();
    for batch in longs.chunks(11) {
        let mut edit = tree.edit();
        for key in batch {
            edit = edit.insert(key.clone(), rng.bytes(8), &storage).await?;
        }
        tree = edit.persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("long keys inserted  {}", hex(tree.root().as_bytes()));

    // The buffered path: every kind of key through novelty buffers, sealed
    // as written and then canonicalized.
    let mut buffered_root = tree.root().clone();
    let mut writes: Vec<VarKey> = (0..3000).map(|_| fact_key(&mut rng)).collect();
    writes.extend((0..600u64).map(|member| cluster_key(7, member * 104_729 % 1_000_003)));
    writes.extend((0..150).map(|_| long_key(&mut rng)));
    for at in (1..writes.len()).rev() {
        writes.swap(at, rng.below(at + 1));
    }
    for batch in writes.chunks(3) {
        let stored = Tree::from_hash_with_cache(buffered_root.clone(), tree.node_cache());
        let mut buffered = HitchhikerTree::open(&stored);
        for key in batch {
            buffered = buffered
                .insert(key.clone(), rng.bytes(20), &storage)
                .await?;
        }
        buffered_root = buffered.persist(&mut delta)?;
        storage.flush(&mut delta);
    }
    println!("buffered sealed     {}", hex(buffered_root.as_bytes()));

    let stored = Tree::from_hash_with_cache(buffered_root, tree.node_cache());
    let canonical = HitchhikerTree::open(&stored)
        .canonicalize(&storage, &mut delta)
        .await?;
    storage.flush(&mut delta);
    println!("buffered canonical  {}", hex(canonical.root().as_bytes()));

    println!("{}", dialog_search_tree::audit::report());
    Ok(())
}
