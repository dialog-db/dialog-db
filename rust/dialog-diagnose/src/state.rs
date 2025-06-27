//! Application state management for the diagnose TUI.
//!
//! This module contains all the state structures and logic for managing
//! the various components of the diagnose application, including:
//! - Tab navigation state
//! - Facts table state
//! - Tree explorer state
//! - Database store interface

use std::collections::BTreeSet;

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

/// State for the Facts table view.
///
/// Tracks the current selection/cursor position within the facts table.
#[derive(Default)]
pub struct FactsState {
    /// The currently selected row index in the facts table
    pub index: usize,
}

/// State for the Tree explorer view.
///
/// Manages the tree navigation, node selection, and expansion state
/// for exploring the prolly tree structure of the database.
#[derive(Default)]
pub struct TreeState {
    /// Hash of the root node of the tree
    pub root: Blake3Hash,
    /// Hash of the currently selected node
    pub selected_node: Blake3Hash,
    /// Optional index of the selected entry within a node (unused currently)
    pub selected_entry: Option<usize>,
    /// Set of node hashes that are currently expanded in the tree view
    pub expanded: BTreeSet<Blake3Hash>,
}

impl TreeState {
    /// Moves the selection to the previous node in the tree traversal order.
    ///
    /// This method implements tree navigation that moves up the tree hierarchy,
    /// handling both sibling navigation and parent-child relationships.
    pub fn select_previous(&mut self, store: &DiagnoseStore) {
        match self.selection_position(store) {
            0 => {
                if let Some((hash, _)) = self.selection_parent(store) {
                    self.selected_node = *hash
                }
            }
            index => {
                if let Some((_, TreeNode::Branch { children, .. })) = self.selection_parent(store) {
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
            }
        }
    }

    /// Moves the selection to the next node in the tree traversal order.
    ///
    /// This method implements depth-first tree navigation, descending into
    /// expanded nodes and moving to siblings when reaching the end of a branch.
    pub fn select_next(&mut self, store: &DiagnoseStore) {
        if self.expanded.contains(&self.selected_node) {
            if let Promise::Resolved(TreeNode::Branch { children, .. }) =
                store.node(&self.selected_node)
            {
                if let Some(child) = children.first() {
                    self.selected_node = *child;
                    return;
                }
            }
        }

        let mut cursor = &self.selected_node;

        while let Some((
            parent_hash,
            TreeNode::Branch {
                children: siblings, ..
            },
        )) = store.parent_node_of(cursor)
        {
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

    /// Returns the position index of a node within its parent's children list.
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

    /// Returns the position index of the currently selected node within its parent.
    fn selection_position(&self, store: &DiagnoseStore) -> usize {
        self.position_of(&self.selected_node, store)
    }

    /// Returns the parent node hash and tree node of the currently selected node.
    fn selection_parent<'a>(
        &'a self,
        store: &'a DiagnoseStore,
    ) -> Option<(&'a Blake3Hash, &'a TreeNode)> {
        store.parent_node_of(&self.selected_node)
    }
}

/// Main application state that coordinates all UI components.
///
/// This structure holds the state for all tabs and provides the interface
/// to the underlying database store.
pub struct DiagnoseState {
    /// Currently active tab (Facts or Tree)
    pub tab: DiagnoseTab,
    /// State for the facts table view
    pub facts: FactsState,
    /// State for the tree explorer view
    pub tree: TreeState,
    /// Interface to the database store and async data loading
    pub store: DiagnoseStore,
}

impl DiagnoseState {
    /// Creates a new `DiagnoseState` from the given artifacts database.
    ///
    /// This initializes all the state components and sets up the tree view
    /// to start at the root of the prolly tree.
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
                root: root_hash,
                selected_node: root_hash,
                selected_entry: None,
                expanded: Default::default(),
            },
        }
    }
}
