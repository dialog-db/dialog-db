use std::{
    pin::Pin,
    sync::{Arc, mpsc::Sender},
};

use dialog_artifacts::{Datum, DialogArtifactsError, EntityKey, Index, State};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};
use futures_util::{Stream, TryStreamExt};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct ArtifactsCursorState {
    next_index: usize,
    last_key: Option<EntityKey>,
    finished: bool,
}

pub struct ArtifactsCursor {
    state: Arc<Mutex<ArtifactsCursorState>>,
    tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    tx: Sender<(usize, State<Datum>)>,
}

impl ArtifactsCursor {
    pub fn new(
        tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tx: Sender<(usize, State<Datum>)>,
    ) -> Self {
        Self {
            state: Default::default(),
            tree,
            tx,
        }
    }

    pub fn seek<'a>(&'a self, index: usize) {
        let tx = self.tx.clone();
        let state = self.state.clone();
        let tree = self.tree.clone();

        tokio::spawn(async move {
            let mut state = state.lock().await;

            if state.finished {
                return Ok(()) as Result<(), DialogArtifactsError>;
            }

            if index < state.next_index {
                return Ok(());
            }

            let mut stream: Pin<Box<dyn Stream<Item = _> + Send>> = match state.last_key.clone() {
                Some(key) => Box::pin(tree.stream_range(key..)),
                None => Box::pin(tree.stream()),
            };

            loop {
                let Some(element) = stream.try_next().await? else {
                    break;
                };

                state.last_key = Some(element.key);

                match tx.send((state.next_index, element.value)) {
                    Ok(_) => (),
                    Err(_) => break,
                }

                state.next_index += 1;
            }

            Ok(())
        });
    }
}
