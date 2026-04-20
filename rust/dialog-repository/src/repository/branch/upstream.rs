use serde::{Deserialize, Serialize};

use crate::repository::tree::TreeReference;

/// Upstream represents some branch being tracked.
///
/// The `tree` field stores the upstream's tree root at the time of last
/// sync, used as the base for three-way merge when rebasing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpstreamState {
    /// A local branch upstream.
    Local {
        /// Branch name.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
    },
    /// A remote branch upstream.
    Remote {
        /// Remote name (e.g., "origin").
        name: String,
        /// Branch name on the remote.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
    },
}

impl UpstreamState {
    /// Returns the branch name of this upstream.
    pub fn branch(&self) -> &str {
        match self {
            Self::Local { branch, .. } => branch,
            Self::Remote { branch, .. } => branch,
        }
    }

    /// Returns the tree root at the last sync point.
    pub fn tree(&self) -> &TreeReference {
        match self {
            Self::Local { tree, .. } => tree,
            Self::Remote { tree, .. } => tree,
        }
    }

    /// Returns a new upstream with the tree updated to the given value.
    pub fn with_tree(self, tree: TreeReference) -> Self {
        match self {
            Self::Local { branch, .. } => Self::Local { branch, tree },
            Self::Remote { name, branch, .. } => Self::Remote { name, branch, tree },
        }
    }
}
