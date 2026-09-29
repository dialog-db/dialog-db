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

use dialog_baseline::metered::Meter;
use dialog_baseline::nodes::{TreeNode, node};
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_search_tree::{Buffer as TreeBuffer, Load, NodeBody};
use futures_util::stream;

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
        let node = match TreeNode::try_from(TreeBuffer::from(bytes.to_vec())) {
            Ok(node) => node,
            Err(_) => {
                self.other.add(bytes.len());
                return;
            }
        };
        match node.body() {
            NodeBody::Segment(_) => self.leaf.add(bytes.len()),
            NodeBody::Index(index) => {
                self.index.add(bytes.len());
                self.index_novelty_ops += index.novelty_len();
            }
        }
    }
}

#[derive(Default)]
struct Ledger {
    writes: Volume,
    reads: Volume,
}

/// Decodes and classifies every block the branch moves.
#[derive(Clone, Default)]
struct Classifier {
    ledger: Arc<Mutex<Ledger>>,
}

impl Classifier {
    /// Everything classified since the last take.
    fn take(&self) -> Ledger {
        std::mem::take(&mut *self.ledger.lock().expect("ledger lock"))
    }
}

impl Meter for Classifier {
    fn wrote(&self, block: &[u8]) {
        self.ledger
            .lock()
            .expect("ledger lock")
            .writes
            .classify(block);
    }

    fn read(&self, block: &[u8]) {
        self.ledger
            .lock()
            .expect("ledger lock")
            .reads
            .classify(block);
    }
}

fn per_commit(class: &ClassVolume, window: usize) -> String {
    format!(
        "{:.1}x{:.0}K",
        class.blocks as f64 / window as f64,
        class.bytes as f64 / window as f64 / 1024.0
    )
}

/// Probes the live tree: root block size, root novelty ops, root links,
/// and depth along the leftmost path.
async fn probe<Env>(env: &Env, root: Blake3Hash) -> anyhow::Result<(usize, usize, usize, usize)>
where
    Env: Provider<Load> + ConditionalSync,
{
    let mut hash = root;
    let mut depth = 0usize;
    let mut root_stats = (0usize, 0usize, 0usize);
    loop {
        let (node, size) = node(env, &hash).await?;
        depth += 1;
        match node.body() {
            NodeBody::Index(index) => {
                if depth == 1 {
                    root_stats = (size, index.novelty_len(), index.len());
                }
                hash = index.hash_at(0)?.clone();
            }
            NodeBody::Segment(_) => break,
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
        let classifier = Classifier::default();
        let repo = DialogRepo::metered(classifier.clone()).await?;
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
            repo.branch()
                .commit(stream::iter(se_instructions(commit)?))
                .perform(repo.operator())
                .await?;
            committed += 1;
            if committed.is_multiple_of(window) {
                let elapsed = window_started.elapsed().as_micros() as f64 / window as f64;
                let taken = classifier.take();
                let root = repo
                    .root()
                    .expect("the branch has commits, so it has a tree");
                let (root_size, root_ops, root_links, depth) = probe(&repo.index(), root).await?;
                // The probe's own reads are not the commits' traffic.
                classifier.take();
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
                window_started = std::time::Instant::now();
            }
        }
        Ok(())
    })
}
