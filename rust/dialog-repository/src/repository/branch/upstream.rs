use serde::{Deserialize, Serialize};

use crate::TreeReference;

/// The persisted form of a branch's upstream tracking state.
///
/// Stored in the branch's `upstream` cell. The `tree` field captures
/// the upstream's tree root at the time of last sync, used as the
/// divergence base for three-way merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Upstream {
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
        remote: String,
        /// Branch name on the remote.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
    },
}

impl Upstream {
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
            Self::Remote { remote, branch, .. } => Self::Remote {
                remote,
                branch,
                tree,
            },
        }
    }
}
