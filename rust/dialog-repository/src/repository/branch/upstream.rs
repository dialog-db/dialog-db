use serde::{Deserialize, Serialize};

use super::name::BranchName;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::RemoteName;

/// Upstream represents some branch being tracked.
///
/// The `tree` field stores the upstream's tree root at the time of last
/// sync, used as the base for three-way merge when rebasing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpstreamState {
    /// A local branch upstream
    Local {
        /// Branch name
        branch: BranchName,
        /// Tree root at last sync point
        tree: NodeReference,
    },
    /// A remote branch upstream
    Remote {
        /// Remote name (e.g., "origin")
        name: RemoteName,
        /// Branch name
        branch: BranchName,
        /// Tree root at last sync point
        tree: NodeReference,
    },
}

impl UpstreamState {
    /// Returns the branch name of this upstream.
    pub fn branch(&self) -> &BranchName {
        match self {
            Self::Local { branch, .. } => branch,
            Self::Remote { branch, .. } => branch,
        }
    }

    /// Returns the tree root at the last sync point.
    pub fn tree(&self) -> &NodeReference {
        match self {
            Self::Local { tree, .. } => tree,
            Self::Remote { tree, .. } => tree,
        }
    }

    /// Returns a new upstream with the tree updated to the given value.
    pub fn with_tree(self, tree: NodeReference) -> Self {
        match self {
            Self::Local { branch, .. } => Self::Local { branch, tree },
            Self::Remote { name, branch, .. } => Self::Remote { name, branch, tree },
        }
    }
}
