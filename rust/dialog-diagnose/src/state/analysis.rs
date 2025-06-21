use std::{collections::VecDeque, sync::mpsc::Sender};

use dialog_artifacts::{Datum, DialogArtifactsError, EntityKey, Index};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};

#[derive(Default, Debug, Clone)]
pub struct ArtifactsTreeStats {
    pub depth: usize,
    pub total_entries: usize,
    pub distribution: [usize; 10],
    pub minimum_segment_size: usize,
    pub maximum_segment_size: usize,
}

pub struct ArtifactsTreeAnalysis {
    tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    tx: Sender<ArtifactsTreeStats>,
}

impl ArtifactsTreeAnalysis {
    pub fn new(
        tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tx: Sender<ArtifactsTreeStats>,
    ) -> Self {
        Self { tree, tx }
    }

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

                if next_level.len() > 0 {
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
            let minimum_segment_size = minimum_segment_size.map(|value| *value).unwrap_or_default();
            let segment_band_width =
                maximum_segment_size.saturating_sub(minimum_segment_size) as f64 / 9.;

            stats.minimum_segment_size = minimum_segment_size;
            stats.maximum_segment_size = maximum_segment_size;

            for size in segment_sizes {
                let normalized = size.saturating_sub(minimum_segment_size);

                let index = (normalized as f64 / segment_band_width).round() as usize;

                stats.distribution[index] += 1;
            }

            tx.send(stats).unwrap();

            Ok(()) as Result<_, DialogArtifactsError>
        });
    }
}
