//! Tree hierarchy module for loading and navigating prolly tree nodes.

use std::sync::mpsc::Sender;

use dialog_artifacts::{CborEncoder, Datum, DialogArtifactsError, Key, State, Storage};
use dialog_search_tree::{
    ArchivedNodeBody, Buffer, Entry, Key as TreeKey, PersistentNode, into_owned,
};
use dialog_storage::{Blake3Hash, MemoryStorageBackend, StorageBackend};

use super::store::WorkerMessage;

type DiagnoseStorage = Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

/// Represents a node in the prolly tree hierarchy.
///
/// Tree nodes can be either leaf segments containing actual data entries,
/// or branch nodes containing references to child nodes.
#[derive(Debug)]
pub enum TreeNode {
    /// A leaf segment containing actual data entries
    Segment {
        /// The entries stored in this leaf segment
        entries: Vec<Entry<Key, State<Datum>>>,
    },
    /// A branch node containing references to child nodes
    Branch {
        /// The separator at each child's left edge (the routing lower
        /// bounds; the leftmost child of a level carries the empty
        /// separator)
        separators: Vec<Vec<u8>>,
        /// Hashes of child nodes
        children: Vec<Blake3Hash>,
    },
}

/// Background worker for loading tree node hierarchy data.
///
/// This worker loads individual tree nodes on-demand as the UI navigates
/// the prolly tree structure.
pub struct ArtifactsHierarchy {
    /// The storage backend for tree operations
    storage: DiagnoseStorage,
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
    pub fn new(storage: DiagnoseStorage, tx: Sender<WorkerMessage>) -> Self {
        Self { storage, tx }
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
        let storage = self.storage.clone();
        let tx = self.tx.clone();
        let hash = hash.to_owned();

        tokio::spawn(async move {
            let Some(bytes) = storage.get(&hash).await? else {
                // TODO: This should be an error condition
                return Ok(());
            };

            let block: PersistentNode<Key, State<Datum>> = PersistentNode::new(Buffer::from(bytes));
            let node = match block.body()? {
                ArchivedNodeBody::Index(index) => {
                    let links = index.links()?;
                    TreeNode::Branch {
                        separators: links.iter().map(|link| link.separator.clone()).collect(),
                        children: links.iter().map(|link| *link.node.as_bytes()).collect(),
                    }
                }
                ArchivedNodeBody::Segment(segment) => {
                    let mut entries = Vec::with_capacity(segment.len());
                    let mut keys = segment.keys::<Key>()?;
                    while let Some((at, key)) = keys.next_key()? {
                        entries.push(Entry {
                            key: <Key as TreeKey>::try_from_bytes(&key)?,
                            value: into_owned::<State<Datum>>(segment.value_at(at)?)?,
                        });
                    }
                    TreeNode::Segment { entries }
                }
            };

            tx.send(WorkerMessage::Node { hash, node }).unwrap();

            Ok(()) as Result<_, DialogArtifactsError>
        });
    }
}
