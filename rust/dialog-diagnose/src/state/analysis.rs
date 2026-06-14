//! Tree analysis module for computing prolly tree statistics.

use std::{collections::VecDeque, sync::mpsc::Sender};

use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{CborEncoder, Datum, DialogArtifactsError, Index, KeyBytes, State, Storage};
use dialog_common::{Blake3Hash as NodeHash, NULL_BLAKE3_HASH};
use dialog_search_tree::{
    Accessor, ArchivedNodeBody, Cache, ContentAddressedStorage as TreeStorage, Delta, Node,
};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};

use super::store::WorkerMessage;

type DiagnoseStorage = Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

/// Statistics about the structure and content of a prolly tree.
///
/// These statistics are computed asynchronously to provide insights
/// into the tree's structure without blocking the UI.
#[derive(Default, Debug, Clone)]
pub struct ArtifactsTreeStats {
    /// Maximum depth of the tree
    pub depth: usize,
    /// Total number of entries across all leaf nodes
    pub total_entries: usize,
    /// Distribution of entry counts across depth levels (up to 10 levels)
    pub distribution: [usize; 10],
    /// Smallest segment size found in the tree
    pub minimum_segment_size: usize,
    /// Largest segment size found in the tree
    pub maximum_segment_size: usize,
}

/// Background worker for computing tree statistics.
///
/// This worker performs a breadth-first traversal of the prolly tree
/// to compute various statistics about its structure and contents.
pub struct ArtifactsTreeAnalysis {
    /// The prolly tree index to analyze
    tree: Index,
    /// The storage backend for tree operations
    storage: DiagnoseStorage,
    /// Channel sender for worker messages
    tx: Sender<WorkerMessage>,
}

impl ArtifactsTreeAnalysis {
    /// Creates a new tree analysis worker.
    ///
    /// # Arguments
    ///
    /// * `tree` - The prolly tree index to analyze
    /// * `storage` - The storage backend for tree operations
    /// * `tx` - Channel sender for worker messages
    pub fn new(tree: Index, storage: DiagnoseStorage, tx: Sender<WorkerMessage>) -> Self {
        Self { tree, storage, tx }
    }

    /// Starts the background analysis task.
    ///
    /// This spawns a tokio task that performs a breadth-first traversal
    /// of the tree to compute statistics. The results are sent via the
    /// configured channel when complete.
    pub fn run(&self) {
        let root = self.tree.root().clone();
        if &root == NULL_BLAKE3_HASH {
            return;
        }

        let storage = self.storage.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            let tree_storage = TreeStorage::new(TreeStorageBridge(storage));
            let accessor = Accessor::new(Delta::zero(), Cache::new(), tree_storage);

            let mut stats = ArtifactsTreeStats::default();
            let mut levels = VecDeque::from([vec![root]]);
            let mut segment_sizes: Vec<usize> = vec![];

            while let Some(level) = levels.pop_front() {
                let mut next_level = vec![];

                for hash in level {
                    let node: Node<KeyBytes, State<Datum>> = accessor.get_node(&hash).await?;
                    match node.body()? {
                        ArchivedNodeBody::Index(index) => {
                            for link in index.links.iter() {
                                next_level.push(<&NodeHash>::from(&link.node).clone());
                            }
                        }
                        ArchivedNodeBody::Segment(segment) => {
                            let entry_count = segment.entries.len();

                            segment_sizes.push(entry_count);
                            stats.total_entries += entry_count;
                        }
                    }
                }

                if !next_level.is_empty() {
                    levels.push_back(next_level);
                }

                stats.depth += 1;
            }

            let (minimum_segment_size, maximum_segment_size) =
                segment_sizes
                    .iter()
                    .copied()
                    .fold((None, 0usize), |(min, max), value| {
                        (
                            Some(min.map_or(value, |m: usize| m.min(value))),
                            max.max(value),
                        )
                    });
            let minimum_segment_size = minimum_segment_size.unwrap_or_default();
            let segment_band_width =
                maximum_segment_size.saturating_sub(minimum_segment_size) as f64 / 9.;

            stats.minimum_segment_size = minimum_segment_size;
            stats.maximum_segment_size = maximum_segment_size;

            for size in segment_sizes {
                let normalized = size.saturating_sub(minimum_segment_size);

                let index = (normalized as f64 / segment_band_width).round() as usize;

                stats.distribution[index] += 1;
            }

            tx.send(WorkerMessage::Stats(stats)).unwrap();

            Ok(()) as Result<_, DialogArtifactsError>
        });
    }
}
