//! Tree analysis module for computing prolly tree statistics.

use std::{collections::VecDeque, sync::mpsc::Sender};

use dialog_artifacts::{Datum, DialogArtifactsError, Index, Key};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};

use super::store::WorkerMessage;

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
    tree: Index<Key, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    /// Channel sender for worker messages
    tx: Sender<WorkerMessage>,
}

impl ArtifactsTreeAnalysis {
    /// Creates a new tree analysis worker.
    ///
    /// # Arguments
    ///
    /// * `tree` - The prolly tree index to analyze
    /// * `tx` - Channel sender for worker messages
    pub fn new(
        tree: Index<Key, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tx: Sender<WorkerMessage>,
    ) -> Self {
        Self { tree, tx }
    }

    /// Starts the background analysis task.
    ///
    /// This spawns a tokio task that performs a breadth-first traversal
    /// of the tree to compute statistics. The results are sent via the
    /// configured channel when complete.
    pub fn run(&self) {
        let Some(root) = self.tree.root() else {
            return;
        };

        let root = root.clone();
        let tree = self.tree.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            let mut stats = ArtifactsTreeStats::default();
            let mut levels = VecDeque::from([vec![root.clone()]]);
            let mut segment_sizes = vec![];

            while let Some(level) = levels.pop_front() {
                let mut next_level = vec![];

                for node in level {
                    if node.is_branch() {
                        let mut children = Vec::from(node.load_children(tree.storage()).await?);
                        next_level.append(&mut children);
                    } else {
                        let entries = node.into_entries()?;
                        let entry_count = entries.len();

                        segment_sizes.push(entry_count);
                        stats.total_entries += entry_count;
                    }
                }

                if !next_level.is_empty() {
                    levels.push_back(next_level);
                }

                stats.depth += 1;
            }

            let (minimum_segment_size, maximum_segment_size) =
                segment_sizes.iter().fold((None, 0), |(min, max), value| {
                    (
                        min.or(Some(value)).map(|min| min.min(value)),
                        max.max(*value),
                    )
                });
            let minimum_segment_size = minimum_segment_size.copied().unwrap_or_default();
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
