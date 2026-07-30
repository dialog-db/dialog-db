//! Attribute the at-scale growth in bytes moved per commit.
//!
//! The windowed byte-volume measurement (measure_se_replay) shows both
//! write and read volume per commit growing ~3.5x across a 25K-txn
//! replay even with the novelty byte cap bounding the buffers. This
//! names the growing term: every block written or read is decoded and
//! classified — leaf segment, index node (with its novelty op count),
//! or other (revision/spill blocks) — and each window reports the
//! per-commit volume BY CLASS, alongside a probe of the live root frame
//! (size, novelty ops, links) and the tree depth. Whichever class's
//! bytes track the growth is the mechanism; the classes are designed to
//! separate the candidates (root-frame growth toward S, cascade index
//! rewrites at depth, flush write-amp into ceiling-sized leaves).
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example write_attribution -- 25000 2500
//! ```

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use dialog_artifacts::{ArtifactStoreMut as _, Artifacts, Datum, IndexRoot, Key, State};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_search_tree::{ArchivedNodeBody, Buffer as TreeBuffer, PersistentNode};
use dialog_storage::{Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend};
use futures_util::stream;

type TreeNode = PersistentNode<Key, State<Datum>>;

#[derive(Default, Clone, Copy)]
struct ClassVolume {
    blocks: usize,
    bytes: usize,
}

impl ClassVolume {
    fn add(&mut self, bytes: usize) {
        self.blocks += 1;
        self.bytes += bytes;
    }
}

/// One direction's (write or read) volume, split by block class.
#[derive(Default, Clone, Copy)]
struct Volume {
    leaf: ClassVolume,
    index: ClassVolume,
    other: ClassVolume,
    /// Buffered ops across every index block counted in `index`.
    index_novelty_ops: usize,
}

impl Volume {
    fn classify(&mut self, bytes: &[u8]) {
        let node = TreeNode::new(TreeBuffer::from(bytes.to_vec()));
        match node.body() {
            Ok(ArchivedNodeBody::Segment(_)) => self.leaf.add(bytes.len()),
            Ok(ArchivedNodeBody::Index(index)) => {
                self.index.add(bytes.len());
                self.index_novelty_ops += index.novelty_len();
            }
            Err(_) => self.other.add(bytes.len()),
        }
    }
}

#[derive(Default)]
struct Ledger {
    writes: Volume,
    reads: Volume,
}

/// Proxy backend that decodes and classifies every block moved through it.
#[derive(Clone)]
struct ClassifyingStorage {
    ledger: Arc<Mutex<Ledger>>,
    backend: MemoryStorageBackend<Blake3Hash, Vec<u8>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageBackend for ClassifyingStorage {
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = <MemoryStorageBackend<Blake3Hash, Vec<u8>> as StorageBackend>::Error;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.ledger
            .lock()
            .expect("ledger lock")
            .writes
            .classify(&value);
        self.backend.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let value = self.backend.get(key).await?;
        if let Some(value) = &value {
            self.ledger
                .lock()
                .expect("ledger lock")
                .reads
                .classify(value);
        }
        Ok(value)
    }
}

fn per_commit(class: &ClassVolume, window: usize) -> String {
    format!(
        "{:.1}x{:.0}K",
        class.blocks as f64 / window as f64,
        class.bytes as f64 / window as f64 / 1024.0
    )
}

/// Probes the live tree through the RAW backend (no counter pollution):
/// root block size, root novelty ops, root links, and depth along the
/// leftmost path.
async fn probe(
    inner: &MemoryStorageBackend<Blake3Hash, Vec<u8>>,
    revision: &Blake3Hash,
) -> anyhow::Result<(usize, usize, usize, usize)> {
    use dialog_storage::StorageBackend as _;
    let bytes = inner
        .get(revision)
        .await?
        .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
    let root: IndexRoot = CborEncoder.decode(&bytes).await?;
    let mut hash = *root.index();
    let mut depth = 0usize;
    let mut root_stats = (0usize, 0usize, 0usize);
    loop {
        let Some(bytes) = inner.get(&hash).await? else {
            anyhow::bail!("reachable node missing");
        };
        let size = bytes.len();
        let node = TreeNode::new(TreeBuffer::from(bytes));
        depth += 1;
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                if depth == 1 {
                    root_stats = (size, index.novelty_len(), index.len());
                }
                hash = *index.hash_at(0)?.as_bytes();
            }
            ArchivedNodeBody::Segment(_) => break,
        }
    }
    Ok((root_stats.0, root_stats.1, root_stats.2, depth))
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(25000);
    let window: usize = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2500);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let inner = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let backend = ClassifyingStorage {
            ledger: ledger.clone(),
            backend: inner.clone(),
        };
        let mut store = Artifacts::anonymous(backend).await?;
        let mut committed = 0usize;
        println!(
            "per-commit volume by class (blocks x KiB): W=write R=read; root/depth probed per window"
        );
        println!(
            "{:>7}  {:>10} {:>10} {:>10}  {:>10} {:>10} {:>10}  {:>8} {:>6} {:>5} {:>5}  {:>8}",
            "commits",
            "W leaf",
            "W index",
            "W other",
            "R leaf",
            "R index",
            "R other",
            "root KiB",
            "ops",
            "links",
            "depth",
            "us/txn"
        );
        let mut window_started = std::time::Instant::now();
        for commit in &log.transactions {
            store
                .commit(stream::iter(se_instructions(commit)?))
                .await?;
            committed += 1;
            if committed.is_multiple_of(window) {
                let elapsed = window_started.elapsed().as_micros() as f64 / window as f64;
                let taken = {
                    let mut ledger = ledger.lock().expect("ledger lock");
                    std::mem::take(&mut *ledger)
                };
                let revision = store.revision().await?;
                let (root_size, root_ops, root_links, depth) = probe(&inner, &revision).await?;
                println!(
                    "{committed:>7}  {:>10} {:>10} {:>10}  {:>10} {:>10} {:>10}  {:>8.0} {:>6} {:>5} {:>5}  {:>8.0}",
                    per_commit(&taken.writes.leaf, window),
                    per_commit(&taken.writes.index, window),
                    per_commit(&taken.writes.other, window),
                    per_commit(&taken.reads.leaf, window),
                    per_commit(&taken.reads.index, window),
                    per_commit(&taken.reads.other, window),
                    root_size as f64 / 1024.0,
                    root_ops,
                    root_links,
                    depth,
                    elapsed,
                );
                println!("         edits: {}", dialog_search_tree::edit_audit::report());
                window_started = std::time::Instant::now();
            }
        }
        Ok(())
    })
}
