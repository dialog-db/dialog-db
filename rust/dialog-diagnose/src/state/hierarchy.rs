//! Tree hierarchy module for loading and navigating prolly tree nodes.

use std::sync::mpsc::Sender;

use dialog_artifacts::{Datum, DialogArtifactsError, EntityKey, HASH_SIZE, Index, State};
use dialog_prolly_tree::{Block, Entry};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, MemoryStorageBackend};

use super::store::WorkerMessage;

/// Represents a node in the prolly tree hierarchy.
///
/// Tree nodes can be either leaf segments containing actual data entries,
/// or branch nodes containing references to child nodes.
#[derive(Debug)]
pub enum TreeNode {
    /// A leaf segment containing actual data entries
    Segment {
        /// The entries stored in this leaf segment
        entries: Vec<Entry<EntityKey, State<Datum>>>,
    },
    /// A branch node containing references to child nodes
    Branch {
        /// The upper bound key for this branch
        upper_bound: EntityKey,
        /// Hashes of child nodes
        children: Vec<Blake3Hash>,
    },
}

/// Background worker for loading tree node hierarchy data.
///
/// This worker loads individual tree nodes on-demand as the UI navigates
/// the prolly tree structure.
pub struct ArtifactsHierarchy {
    /// The prolly tree index to load nodes from
    tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    /// Channel sender for worker messages
    tx: Sender<WorkerMessage>,
}

impl ArtifactsHierarchy {
    /// Creates a new hierarchy worker.
    ///
    /// # Arguments
    ///
    /// * `tree` - The prolly tree index to load nodes from
    /// * `tx` - Channel sender for worker messages
    pub fn new(
        tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tx: Sender<WorkerMessage>,
    ) -> Self {
        Self { tree, tx }
    }

    /// Looks up a tree node by its hash, loading it in the background.
    ///
    /// This method spawns a background task to load the specified node
    /// and send it via the configured channel when available.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash of the node to look up
    pub fn lookup_node(&self, hash: &Blake3Hash) {
        let tree = self.tree.clone();
        let tx = self.tx.clone();
        let hash = hash.to_owned();

        tokio::spawn(async move {
            let Some(block): Option<Block<HASH_SIZE, EntityKey, State<Datum>, Blake3Hash>> =
                tree.storage().read(&hash).await?
            else {
                // TODO: This should be an error condition
                return Ok(());
            };

            let node = match &block {
                Block::Branch(_) => TreeNode::Branch {
                    upper_bound: block.upper_bound().clone(),
                    children: block
                        .references()?
                        .iter()
                        .map(|reference| reference.hash().to_owned())
                        .collect(),
                },
                Block::Segment(_) => TreeNode::Segment {
                    entries: Vec::from(block.into_entries()?),
                },
            };

            tx.send(WorkerMessage::Node { hash, node }).unwrap();

            Ok(()) as Result<_, DialogArtifactsError>
        });
    }
}
