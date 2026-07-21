//! Node-size capture for a persisted artifact tree.
//!
//! Walks a tree from its root hash through any raw block store and records
//! every node's kind, height, byte size, slot count, and buffered novelty
//! footprint. The point is measurement, not mutation: the walk reads node
//! buffers exactly as a scan would, decodes nothing but structure, and
//! leaves the store untouched.
//!
//! Novelty bytes are measured by re-encoding the index node's links with an
//! empty buffer set (THE canonical byte form, see
//! `PersistentNodeBody::index_from_buffers`) and subtracting: whatever the
//! stored node carries beyond its canonical form is the byte cost of the
//! buffered ops riding on it.

use std::env;

use dialog_search_tree::{Buffer, PersistentNode, PersistentNodeBody};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};

use crate::{Datum, DialogArtifactsError, EMPTY_TREE_HASH, Key, State};

/// Which of the two node forms a walked node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// An index node: routing separators plus child links (and possibly
    /// buffered novelty).
    Index,
    /// A leaf segment: the entries themselves.
    Segment,
}

impl NodeKind {
    /// Short display label.
    pub fn label(&self) -> &'static str {
        match self {
            NodeKind::Index => "index",
            NodeKind::Segment => "segment",
        }
    }
}

/// One walked node's measurements.
#[derive(Debug, Clone)]
pub struct NodeStat {
    /// Index or segment.
    pub kind: NodeKind,
    /// Distance from the leaf layer: 0 is a leaf segment, the root carries
    /// the maximum height. Leaves sit at one depth in this tree, so height
    /// is well defined for every node.
    pub height: usize,
    /// Serialized node size in bytes (the block a fetch pays for).
    pub bytes: usize,
    /// Child count for an index, entry count for a segment.
    pub slots: usize,
    /// Buffered ops riding this node (always 0 for a segment).
    pub novelty_ops: usize,
    /// Bytes this node carries beyond its canonical (novelty-free) encoding.
    pub novelty_bytes: usize,
}

/// Walks the tree rooted at `root` breadth-first and measures every node.
///
/// `root` is the raw 32-byte tree root as carried by a revision; the empty
/// tree yields an empty capture. The store is the same hash-to-block backend
/// the tree persists into, so spilled value blocks and history records
/// outside the tree are never touched.
pub async fn capture<S>(root: &Blake3Hash, store: &S) -> Result<Vec<NodeStat>, DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    let mut stats: Vec<(usize, NodeStat)> = Vec::new();
    if *root == EMPTY_TREE_HASH {
        return Ok(Vec::new());
    }

    let mut frontier: Vec<Blake3Hash> = vec![*root];
    let mut depth = 0usize;
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for hash in &frontier {
            let bytes = store.get(hash).await?.ok_or_else(|| {
                DialogArtifactsError::Tree(format!("tree node missing from store at depth {depth}"))
            })?;
            let size = bytes.len();
            let node = PersistentNode::<Key, State<Datum>>::new(Buffer::from(bytes));
            let stat = if let Ok(index) = node.as_index() {
                for at in 0..index.len() {
                    next.push(*index.hash_at(at)?.as_bytes());
                }
                let novelty_ops = index.novelty_len();
                let novelty_bytes = if novelty_ops == 0 {
                    0
                } else {
                    let canonical = PersistentNodeBody::<State<Datum>>::index_from_buffers(
                        index.links()?,
                        Vec::new(),
                        node.manifest()?,
                    )?
                    .as_bytes()?
                    .len();
                    size.saturating_sub(canonical)
                };
                NodeStat {
                    kind: NodeKind::Index,
                    height: 0,
                    bytes: size,
                    slots: index.len(),
                    novelty_ops,
                    novelty_bytes,
                }
            } else {
                let segment = node.as_segment()?;
                NodeStat {
                    kind: NodeKind::Segment,
                    height: 0,
                    bytes: size,
                    slots: segment.len(),
                    novelty_ops: 0,
                    novelty_bytes: 0,
                }
            };
            stats.push((depth, stat));
        }
        frontier = next;
        depth += 1;
    }

    // Leaves live at the deepest layer, so height = max_depth - depth.
    let max_depth = depth.saturating_sub(1);
    Ok(stats
        .into_iter()
        .map(|(at, mut stat)| {
            stat.height = max_depth - at;
            stat
        })
        .collect())
}

/// Byte-size histogram bucket upper bounds, chosen around the ~50 KB node
/// target: what fraction of nodes (and of bytes) sit far below or far above
/// it is the question the capture answers.
const BUCKETS: [(usize, &str); 5] = [
    (4 * 1024, "<4K"),
    (16 * 1024, "4-16K"),
    (50 * 1024, "16-50K"),
    (100 * 1024, "50-100K"),
    (usize::MAX, "100K+"),
];

fn bucket_of(bytes: usize) -> usize {
    BUCKETS
        .iter()
        .position(|(bound, _)| bytes < *bound)
        .unwrap_or(BUCKETS.len() - 1)
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn summarize(label: &str, group: &str, stats: &[&NodeStat]) {
    if stats.is_empty() {
        return;
    }
    let mut sizes: Vec<usize> = stats.iter().map(|stat| stat.bytes).collect();
    sizes.sort_unstable();
    let count = sizes.len();
    let total: usize = sizes.iter().sum();
    let slots: usize = stats.iter().map(|stat| stat.slots).sum();
    let novelty_ops: usize = stats.iter().map(|stat| stat.novelty_ops).sum();
    let novelty_bytes: usize = stats.iter().map(|stat| stat.novelty_bytes).sum();

    let mut node_hist = [0usize; BUCKETS.len()];
    let mut byte_hist = [0usize; BUCKETS.len()];
    for stat in stats {
        let at = bucket_of(stat.bytes);
        node_hist[at] += 1;
        byte_hist[at] += stat.bytes;
    }
    let hist: Vec<String> = BUCKETS
        .iter()
        .enumerate()
        .map(|(at, (_, name))| {
            format!(
                "{name}:{} nodes/{:.1}% bytes",
                node_hist[at],
                100.0 * byte_hist[at] as f64 / total.max(1) as f64
            )
        })
        .collect();

    eprintln!(
        "TREEDIST {label} {group}: count={count} total_bytes={total} \
         mean={} p10={} p50={} p90={} p99={} min={} max={} \
         slots_mean={:.1} novelty_ops={novelty_ops} novelty_bytes={novelty_bytes}",
        total / count,
        percentile(&sizes, 10.0),
        percentile(&sizes, 50.0),
        percentile(&sizes, 90.0),
        percentile(&sizes, 99.0),
        sizes[0],
        sizes[count - 1],
        slots as f64 / count as f64,
    );
    eprintln!("TREEDIST {label} {group} histogram: [{}]", hist.join(" | "));
}

/// Prints the per-node lines (when `DIALOG_DIST_NODES` is set) and summary
/// distributions for a capture: overall, split by kind, and split by kind
/// and height.
pub fn report(label: &str, stats: &[NodeStat]) {
    if stats.is_empty() {
        eprintln!("TREEDIST {label}: empty tree");
        return;
    }
    if env::var("DIALOG_DIST_NODES").is_ok() {
        for stat in stats {
            eprintln!(
                "TREENODE {label} kind={} height={} bytes={} slots={} \
                 novelty_ops={} novelty_bytes={}",
                stat.kind.label(),
                stat.height,
                stat.bytes,
                stat.slots,
                stat.novelty_ops,
                stat.novelty_bytes
            );
        }
    }

    let all: Vec<&NodeStat> = stats.iter().collect();
    summarize(label, "all", &all);
    for kind in [NodeKind::Index, NodeKind::Segment] {
        let of_kind: Vec<&NodeStat> = stats.iter().filter(|stat| stat.kind == kind).collect();
        summarize(label, kind.label(), &of_kind);
        let max_height = of_kind
            .iter()
            .map(|stat| stat.height)
            .max()
            .unwrap_or_default();
        // Per-height rows only add signal when a kind spans several heights
        // (index nodes do; segments are all height 0).
        if max_height > 0 || kind == NodeKind::Index {
            for height in 0..=max_height {
                let level: Vec<&NodeStat> = of_kind
                    .iter()
                    .filter(|stat| stat.height == height)
                    .copied()
                    .collect();
                summarize(label, &format!("{}/h{height}", kind.label()), &level);
            }
        }
    }
}
