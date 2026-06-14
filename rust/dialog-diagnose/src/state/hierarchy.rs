//! Tree hierarchy module for loading and navigating prolly tree nodes.

use std::sync::mpsc::Sender;

use dialog_artifacts::{CborEncoder, Datum, DialogArtifactsError, Key, KeyBytes, State, Storage};
use dialog_common::Blake3Hash as NodeHash;
use dialog_search_tree::{ArchivedNodeBody, Buffer, Entry, Node, into_owned};
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
        /// The upper bound key for this branch
        upper_bound: Key,
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

            let block: Node<KeyBytes, State<Datum>> = Node::new(Buffer::from(bytes));
            let node = match block.body()? {
                ArchivedNodeBody::Index(index) => TreeNode::Branch {
                    upper_bound: Key::from(into_owned::<KeyBytes>(
                        &index
                            .links
                            .last()
                            .ok_or_else(|| {
                                DialogArtifactsError::MalformedIndex(
                                    "Index node had no children".into(),
                                )
                            })?
                            .upper_bound,
                    )?),
                    children: index
                        .links
                        .iter()
                        .map(|link| *<&NodeHash>::from(&link.node).as_bytes())
                        .collect(),
                },
                ArchivedNodeBody::Segment(segment) => {
                    let mut entries = Vec::with_capacity(segment.entries.len());
                    for entry in segment.entries.iter() {
                        entries.push(Entry {
                            key: Key::from(into_owned::<KeyBytes>(&entry.key)?),
                            value: into_owned::<State<Datum>>(&entry.value)?,
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
