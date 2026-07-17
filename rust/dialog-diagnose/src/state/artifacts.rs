//! Artifacts cursor for streaming facts data.

use std::{
    pin::Pin,
    sync::{Arc, mpsc::Sender},
};

use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{CborEncoder, DialogArtifactsError, Index, Key, Storage};
use dialog_search_tree::ContentAddressedStorage as TreeStorage;
use dialog_storage::{Blake3Hash, MemoryStorageBackend};
use futures_util::{Stream, TryStreamExt};
use tokio::sync::Mutex;

use super::store::WorkerMessage;

type DiagnoseStorage = Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

/// Internal state for the artifacts cursor.
///
/// Tracks the current position in the stream and completion status.
#[derive(Default)]
pub struct ArtifactsCursorState {
    /// Index of the next item to fetch
    next_index: usize,
    /// Last key processed (for resuming streams)
    last_key: Option<Key>,
    /// Whether the stream has finished
    finished: bool,
}

/// Background worker for streaming facts data from the artifacts database.
///
/// This cursor provides incremental access to facts data, loading it
/// on-demand as the UI requests specific ranges of data.
pub struct ArtifactsCursor {
    /// Shared state for tracking cursor position
    state: Arc<Mutex<ArtifactsCursorState>>,
    /// The prolly tree index containing the facts
    tree: Index,
    /// The storage backend for tree operations
    storage: DiagnoseStorage,
    /// Channel sender for worker messages
    tx: Sender<WorkerMessage>,
}

impl ArtifactsCursor {
    /// Creates a new artifacts cursor.
    ///
    /// # Arguments
    ///
    /// * `tree` - The prolly tree index containing facts data
    /// * `storage` - The storage backend for tree operations
    /// * `tx` - Channel sender for worker messages
    pub fn new(tree: Index, storage: DiagnoseStorage, tx: Sender<WorkerMessage>) -> Self {
        Self {
            state: Default::default(),
            tree,
            storage,
            tx,
        }
    }

    /// Seeks to the specified index in the facts stream.
    ///
    /// This method triggers background loading of facts up to the specified
    /// index if they haven't been loaded yet. Facts are streamed via the
    /// configured channel as they become available.
    ///
    /// # Arguments
    ///
    /// * `index` - Target index to seek to (inclusive)
    pub fn seek(&self, index: usize) {
        let tx = self.tx.clone();
        let state = self.state.clone();
        let tree = self.tree.clone();
        let storage = self.storage.clone();

        tokio::spawn(async move {
            let mut state = state.lock().await;

            if state.finished {
                return Ok(()) as Result<(), DialogArtifactsError>;
            }

            if index < state.next_index {
                return Ok(());
            }

            let tree_storage = TreeStorage::new(TreeStorageBridge(storage));
            let mut stream: Pin<Box<dyn Stream<Item = _> + Send>> = match state.last_key.clone() {
                Some(key) => Box::pin(tree.stream_range(key.., &tree_storage)),
                None => Box::pin(tree.stream(&tree_storage)),
            };

            while let Some(element) = stream.try_next().await? {
                state.last_key = Some(element.key);

                if tx
                    .send(WorkerMessage::Fact {
                        index: state.next_index,
                        data: element.value,
                    })
                    .is_err()
                {
                    break;
                }

                state.next_index += 1;
            }

            Ok(())
        });
    }
}
