//! Honest block-size census: only blocks REACHABLE from the live root.
//!
//! The earlier node-size sweep censused the whole backend — every block
//! ever written, dead superseded roots and all — which conflates the
//! graveyard with the tree and says nothing about whether the frame
//! ceiling actually bounds LIVE node sizes. This walks the tree from the
//! root and reports sizes per node kind, twice: the buffered operational
//! tree right after the replay (what a replica serves and syncs — its
//! index nodes carry novelty buffers ON TOP of the entry weight the
//! ceiling paces), and the canonical tree after an explicit
//! canonicalize (where the ceiling's 3x bound is the actual claim).
//! The whole-backend graveyard is reported once for contrast.
//!
//! ```sh
//! DIALOG_TREE_MAX_SEGMENT=65536 cargo run --release -p dialog-baseline \
//!   --example live_census -- 10000
//! ```

use dialog_artifacts::{ArtifactStoreMut as _, Artifacts, Datum, IndexRoot, Key, State};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_search_tree::{ArchivedNodeBody, Buffer as TreeBuffer, PersistentNode};
use dialog_storage::{
    Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend as _,
};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

type TreeNode = PersistentNode<Key, State<Datum>>;

fn stats(label: &str, mut sizes: Vec<usize>, ceiling: usize) {
    sizes.sort_unstable();
    let over = sizes
        .iter()
        .filter(|&&s| ceiling > 0 && s > ceiling)
        .count();
    let pick = |p: f64| -> usize {
        if sizes.is_empty() {
            0
        } else {
            sizes[((sizes.len() - 1) as f64 * p).round() as usize]
        }
    };
    println!(
        "  {label}: {} blocks, {:.1} MiB, p50 {} p90 {} p99 {} max {} bytes, {} over the byte ceiling",
        sizes.len(),
        sizes.iter().sum::<usize>() as f64 / (1024.0 * 1024.0),
        pick(0.50),
        pick(0.90),
        pick(0.99),
        sizes.last().copied().unwrap_or(0),
        over,
    );
}

/// Walks the tree from `root`, splitting block sizes by node kind.
/// Spilled value blocks hang off leaf keys and are not walked; this is a
/// census of TREE nodes, the objects the size policy governs.
async fn census(
    inner: &MemoryStorageBackend<Blake3Hash, Vec<u8>>,
    root: Blake3Hash,
    label: &str,
    ceiling: usize,
) -> anyhow::Result<()> {
    let mut stack = vec![root];
    let mut leaves = Vec::new();
    let mut quiet = Vec::new();
    let mut buffered = Vec::new();
    while let Some(hash) = stack.pop() {
        let Some(bytes) = inner.get(&hash).await? else {
            anyhow::bail!("reachable node missing from storage");
        };
        let size = bytes.len();
        let node = TreeNode::new(TreeBuffer::from(bytes));
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                if index.novelty.is_empty() {
                    quiet.push(size);
                } else {
                    buffered.push(size);
                }
                for at in 0..index.len() {
                    stack.push(*index.hash_at(at)?.as_bytes());
                }
            }
            ArchivedNodeBody::Segment(_) => leaves.push(size),
        }
    }
    println!("{label}:");
    stats("leaf segments", leaves, ceiling);
    stats("index (no novelty)", quiet, ceiling);
    stats("index (buffered)", buffered, 0);
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10000);
    let setting = std::env::var("DIALOG_TREE_MAX_SEGMENT").unwrap_or_else(|_| "65536".into());
    let max_segment: usize = setting.parse().unwrap_or(65536);
    // The claim under test: canonical frames stay under
    // frame_ceiling_factor (3) x max_segment. Weight is not bytes
    // (encoding compresses), so bytes past this mark measure how far the
    // WEIGHT clamp lets encoded sizes drift, not a bug per se.
    let ceiling = 3 * max_segment;
    let log = SeLog::load(count)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let inner = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut store = Artifacts::open("census".into(), inner.clone()).await?;
        for commit in &log.transactions {
            store.commit(stream::iter(se_instructions(commit)?)).await?;
        }
        println!(
            "max_segment={setting} txns={} facts={} (byte ceiling reference: {ceiling})",
            log.transactions.len(),
            log.fact_count()
        );

        let tree_root = |revision_bytes: Vec<u8>| async move {
            let root: IndexRoot = CborEncoder.decode(&revision_bytes).await?;
            anyhow::Ok(*root.index())
        };

        let revision = store.revision().await?;
        let bytes = inner
            .get(&revision)
            .await?
            .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
        census(
            &inner,
            tree_root(bytes).await?,
            "buffered operational tree (after replay)",
            ceiling,
        )
        .await?;

        store.canonicalize().await?;
        let revision = store.revision().await?;
        let bytes = inner
            .get(&revision)
            .await?
            .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
        census(&inner, tree_root(bytes).await?, "canonical tree", ceiling).await?;

        // The graveyard, for contrast with what the sweep measured: every
        // block ever written, dead roots and all.
        use dialog_storage::StorageSource as _;
        let all: Vec<usize> = inner
            .read()
            .map(|entry| entry.map(|(_, bytes)| bytes.len()))
            .try_collect()
            .await?;
        println!("whole backend (dead blocks included):");
        stats("all blocks", all, ceiling);
        Ok(())
    })
}
