//! Database store interface for the diagnose tool.
//!
//! This module provides the main interface between the UI and the Dialog database,
//! handling asynchronous data loading, caching, and providing access to both
//! facts and tree structure information.

use anyhow::Result;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::mpsc::{Receiver, channel},
};

use dialog_artifacts::{Artifacts, Datum, State};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};

use crate::Promise;

use super::{
    ArtifactsCursor, ArtifactsHierarchy, ArtifactsTreeAnalysis, ArtifactsTreeStats, TreeNode,
};

/// Unified message type for all worker communications.
///
/// This enum consolidates all messages sent from background workers
/// to the main thread, eliminating the need for multiple channels.
#[derive(Debug)]
pub enum WorkerMessage {
    /// A fact has been loaded at the specified index
    Fact {
        /// Index position of the fact
        index: usize,
        /// The loaded fact data
        data: State<Datum>,
    },
    /// Tree analysis statistics have been computed
    Stats(ArtifactsTreeStats),
    /// A tree node has been loaded
    Node {
        /// Hash of the loaded node
        hash: Blake3Hash,
        /// The loaded tree node data
        node: TreeNode,
    },
}

/// Main store for managing database access and caching for the diagnose tool.
///
/// `DiagnoseStore` coordinates between multiple background workers that load
/// different types of data (facts, tree analysis, node hierarchy) and provides
/// a unified interface for the UI to access this data.
pub struct DiagnoseStore {
    /// Worker for loading facts data
    cursor: ArtifactsCursor,
    /// Worker for analyzing tree statistics
    analysis: ArtifactsTreeAnalysis,
    /// Worker for loading tree node hierarchy
    hierarchy: ArtifactsHierarchy,

    /// Unified channel receiver for all worker messages
    message_rx: Receiver<WorkerMessage>,

    /// Cache of loaded facts indexed by position
    facts: BTreeMap<usize, State<Datum>>,
    /// Tree statistics (computed asynchronously)
    stats: Promise<ArtifactsTreeStats>,
    /// Cache of loaded tree nodes indexed by hash
    nodes: BTreeMap<Blake3Hash, TreeNode>,
    /// Mapping from child node hash to parent node hash
    parentage: BTreeMap<Blake3Hash, Blake3Hash>,
}

impl DiagnoseStore {
    /// Creates a new `DiagnoseStore` from the given artifacts database.
    ///
    /// This sets up all the background workers and channels for asynchronous
    /// data loading and initializes the internal caches.
    pub async fn new(artifacts: Artifacts<MemoryStorageBackend<Blake3Hash, Vec<u8>>>) -> Self {
        let tree = artifacts.index().read().await.clone();

        let (tx, message_rx) = channel();
        let cursor = ArtifactsCursor::new(tree.clone(), tx.clone());
        let analysis = ArtifactsTreeAnalysis::new(tree.clone(), tx.clone());
        let hierarchy = ArtifactsHierarchy::new(tree, tx);

        Self {
            message_rx,

            cursor,
            analysis,
            hierarchy,

            facts: Default::default(),
            stats: Promise::Pending,
            nodes: Default::default(),
            parentage: Default::default(),
        }
    }

    /// Synchronizes data from background workers into the local caches.
    ///
    /// This method should be called regularly to pull data from the background
    /// workers into the main thread's cache for UI rendering.
    pub fn sync(&mut self) {
        while let Ok(message) = self.message_rx.try_recv() {
            match message {
                WorkerMessage::Fact { index, data } => {
                    self.facts.insert(index, data);
                }
                WorkerMessage::Stats(stats) => {
                    self.stats = Promise::Resolved(stats);
                }
                WorkerMessage::Node { hash, node } => {
                    match &node {
                        TreeNode::Segment { .. } => (),
                        TreeNode::Branch { children, .. } => {
                            for child in children {
                                self.parentage.insert(*child, hash);
                            }
                        }
                    };
                    self.nodes.insert(hash, node);
                }
            }
        }
    }

    /// Returns the tree statistics, triggering computation if not yet available.
    ///
    /// The statistics are computed asynchronously in the background and cached
    /// once available.
    pub fn stats(&self) -> &Promise<ArtifactsTreeStats> {
        if let Promise::Resolved(_) = &self.stats {
            return &self.stats;
        }

        self.analysis.run();

        &self.stats
    }

    /// Returns facts data for the specified range of indices.
    ///
    /// This method triggers loading of facts if they're not yet cached and
    /// returns a vector of Promise wrappers indicating the loading state of each fact.
    pub fn facts(&self, range: Range<usize>) -> Result<Vec<Promise<&State<Datum>>>> {
        self.cursor.seek(range.end);

        Ok(range
            .map(|index| {
                self.facts
                    .get(&index)
                    .map(Promise::Resolved)
                    .unwrap_or(Promise::Pending)
            })
            .collect())
    }

    /// Returns a tree node by its hash, triggering loading if not yet cached.
    ///
    /// This method initiates background loading of the node if it's not already
    /// available in the cache.
    pub fn node(&self, hash: &Blake3Hash) -> Promise<&TreeNode> {
        self.hierarchy.lookup_node(hash);

        match self.nodes.get(hash) {
            Some(node) => Promise::Resolved(node),
            None => Promise::Pending,
        }
    }

    /// Returns the parent node hash and tree node for the given child node hash.
    ///
    /// This is used for tree navigation to move up the hierarchy.
    pub fn parent_node_of(&self, hash: &Blake3Hash) -> Option<(&Blake3Hash, &TreeNode)> {
        self.parentage
            .get(hash)
            .and_then(|hash| self.nodes.get(hash).map(|node| (hash, node)))
    }
}
