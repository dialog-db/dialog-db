use std::collections::{BTreeMap, BTreeSet};

use dialog_artifacts::Artifacts;
use dialog_storage::{Blake3Hash, MemoryStorageBackend};

mod tab;
pub use tab::*;

mod analysis;
pub use analysis::*;

mod artifacts;
pub use artifacts::*;

mod hierarchy;
pub use hierarchy::*;

mod store;
pub use store::*;

use crate::Promise;

#[derive(Default)]
pub struct FactsState {
    pub index: usize,
}

#[derive(Default)]
pub struct TreeState {
    pub root: Blake3Hash,
    pub selected_node: Blake3Hash,
    pub selected_entry: Option<usize>,
    pub expanded: BTreeSet<Blake3Hash>,
}

impl TreeState {
    pub fn select_previous(&mut self, store: &DiagnoseStore) {
        match self.selection_position(store) {
            0 => match self.selection_parent(store) {
                Some((hash, _)) => self.selected_node = *hash,
                None => (),
            },
            index => match self.selection_parent(store) {
                Some((_, TreeNode::Branch { children, .. })) => {
                    if let Some(mut child) = children.get(index.saturating_sub(1)) {
                        while self.expanded.contains(child) {
                            let Promise::Resolved(TreeNode::Branch { children, .. }) =
                                store.node(child)
                            else {
                                break;
                            };
                            let Some(last) = children.last() else {
                                break;
                            };
                            child = last;
                        }
                        self.selected_node = *child;
                    }
                }
                _ => (),
            },
        }
    }

    pub fn select_next(&mut self, store: &DiagnoseStore) {
        if self.expanded.contains(&self.selected_node) {
            match store.node(&self.selected_node) {
                Promise::Resolved(TreeNode::Branch { children, .. }) => {
                    if let Some(child) = children.first() {
                        self.selected_node = *child;
                        return;
                    }
                }
                _ => (),
            }
        }

        let mut cursor = &self.selected_node;

        loop {
            let (parent_hash, siblings) = match store.parent_node_of(cursor) {
                Some((parent_hash, TreeNode::Branch { children, .. })) => (parent_hash, children),
                _ => break,
            };

            match self.position_of(cursor, store) + 1 {
                index if index < siblings.len() => {
                    let Some(hash) = siblings.get(index) else {
                        break;
                    };

                    self.selected_node = *hash;
                    break;
                }
                _ => {
                    cursor = parent_hash;
                }
            }
        }
    }

    fn position_of(&self, hash: &Blake3Hash, store: &DiagnoseStore) -> usize {
        store
            .parent_node_of(hash)
            .map(|(_, node)| match node {
                TreeNode::Branch { children, .. } => children
                    .iter()
                    .enumerate()
                    .find(|(_, candidate)| *candidate == hash)
                    .map(|(index, _)| index)
                    .unwrap_or_default(),
                TreeNode::Segment { .. } => unreachable!("Parent should never be a segment"),
            })
            .unwrap_or_default()
    }

    fn selection_position(&self, store: &DiagnoseStore) -> usize {
        self.position_of(&self.selected_node, store)
    }

    fn selection_parent<'a>(
        &'a self,
        store: &'a DiagnoseStore,
    ) -> Option<(&'a Blake3Hash, &'a TreeNode)> {
        store.parent_node_of(&self.selected_node)
    }
}

pub struct DiagnoseState {
    pub tab: DiagnoseTab,
    pub facts: FactsState,
    pub tree: TreeState,
    pub store: DiagnoseStore,
}

impl DiagnoseState {
    pub async fn new(artifacts: Artifacts<MemoryStorageBackend<[u8; 32], Vec<u8>>>) -> Self {
        let root_hash = artifacts
            .entity_index()
            .read()
            .await
            .hash()
            .expect("Tree is empty!")
            .to_owned();

        Self {
            store: DiagnoseStore::new(artifacts).await,
            tab: Default::default(),
            facts: Default::default(),
            tree: TreeState {
                root: root_hash.clone(),
                selected_node: root_hash,
                selected_entry: None,
                expanded: Default::default(),
            },
        }
    }
}
