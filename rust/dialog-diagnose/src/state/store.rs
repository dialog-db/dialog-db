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

pub struct DiagnoseStore {
    cursor: ArtifactsCursor,
    analysis: ArtifactsTreeAnalysis,
    hierarchy: ArtifactsHierarchy,

    facts_rx: Receiver<(usize, State<Datum>)>,
    stats_rx: Receiver<ArtifactsTreeStats>,
    nodes_rx: Receiver<(Blake3Hash, TreeNode)>,

    facts: BTreeMap<usize, State<Datum>>,
    stats: Promise<ArtifactsTreeStats>,
    nodes: BTreeMap<Blake3Hash, TreeNode>,
    parentage: BTreeMap<Blake3Hash, Blake3Hash>,
}

impl DiagnoseStore {
    pub async fn new(artifacts: Artifacts<MemoryStorageBackend<Blake3Hash, Vec<u8>>>) -> Self {
        let tree = artifacts.entity_index().read().await.clone();

        // TODO: Unify message channels
        let (tx, facts_rx) = channel();
        let cursor = ArtifactsCursor::new(tree.clone(), tx);

        let (tx, stats_rx) = channel();
        let analysis = ArtifactsTreeAnalysis::new(tree.clone(), tx);

        let (tx, nodes_rx) = channel();
        let hierarchy = ArtifactsHierarchy::new(tree, tx);

        Self {
            facts_rx,
            stats_rx,
            nodes_rx,

            cursor,
            analysis,
            hierarchy,

            facts: Default::default(),
            stats: Promise::Pending,
            nodes: Default::default(),
            parentage: Default::default(),
        }
    }

    pub fn sync(&mut self) {
        while let Some(stats) = self.stats_rx.try_recv().ok() {
            self.stats = Promise::Resolved(stats);
        }

        while let Some((key, value)) = self.facts_rx.try_recv().ok() {
            self.facts.insert(key, value);
        }

        while let Some((hash, node)) = self.nodes_rx.try_recv().ok() {
            match &node {
                TreeNode::Segment { .. } => (),
                TreeNode::Branch { children, .. } => {
                    for child in children {
                        self.parentage.insert(*child, hash.clone());
                    }
                }
            };
            self.nodes.insert(hash, node);
        }
    }

    pub fn stats(&self) -> &Promise<ArtifactsTreeStats> {
        match &self.stats {
            Promise::Resolved(_) => return &self.stats,
            _ => (),
        }

        self.analysis.run();

        &self.stats
    }

    pub fn facts(&self, range: Range<usize>) -> Result<Vec<Promise<&State<Datum>>>> {
        self.cursor.seek(range.end);

        Ok(range
            .map(|index| {
                self.facts
                    .get(&index)
                    .map(|state| Promise::Resolved(state))
                    .unwrap_or(Promise::Pending)
            })
            .collect())
    }

    pub fn node(&self, hash: &Blake3Hash) -> Promise<&TreeNode> {
        self.hierarchy.lookup_node(hash);

        match self.nodes.get(hash) {
            Some(node) => Promise::Resolved(node),
            None => Promise::Pending,
        }
    }

    pub fn parent_node_of(&self, hash: &Blake3Hash) -> Option<(&Blake3Hash, &TreeNode)> {
        self.parentage
            .get(hash)
            .and_then(|hash| self.nodes.get(hash).map(|node| (hash, node)))
    }
}
