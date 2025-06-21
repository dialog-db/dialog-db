use std::{collections::VecDeque, sync::mpsc::Sender};

use dialog_artifacts::{
    BRANCH_FACTOR, Datum, DialogArtifactsError, EntityKey, HASH_SIZE, Index, State,
};
use dialog_prolly_tree::{Block, Entry, Node};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, MemoryStorageBackend};

use std::collections::BTreeMap;

pub enum TreeNode {
    Segment {
        entries: Vec<Entry<EntityKey, State<Datum>>>,
    },
    Branch {
        upper_bound: EntityKey,
        children: Vec<Blake3Hash>,
    },
}

pub struct ArtifactsHierarchy {
    tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    tx: Sender<(Blake3Hash, TreeNode)>,
}

impl ArtifactsHierarchy {
    pub fn new(
        tree: Index<EntityKey, Datum, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
        tx: Sender<(Blake3Hash, TreeNode)>,
    ) -> Self {
        Self { tree, tx }
    }

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

            tx.send((hash, node)).unwrap();

            Ok(()) as Result<_, DialogArtifactsError>
        });
    }
}
