use crate::{Branch, RemoteBranch, TreeReference};
use serde::{Deserialize, Serialize};

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

/// The input shape for [`Branch::set_upstream`](super::Branch::set_upstream).
///
/// Wraps a loaded local or remote branch handle. Convertible into
/// [`Upstream`] (the persisted form) by extracting the names; the
/// stored tree starts at [`TreeReference::default`] (empty) since the
/// divergence point is "anything in the upstream from now on."
///
/// Construct via the `From<&Branch>` and `From<&RemoteBranch>` impls;
/// `branch.set_upstream(&local_or_remote)` invokes them implicitly.
pub enum UpstreamBranch {
    /// A local branch upstream.
    Local(Branch),
    /// A remote branch upstream.
    Remote(RemoteBranch),
}

impl From<&Branch> for UpstreamBranch {
    fn from(branch: &Branch) -> Self {
        UpstreamBranch::Local(branch.clone())
    }
}

impl From<Branch> for UpstreamBranch {
    fn from(branch: Branch) -> Self {
        UpstreamBranch::Local(branch)
    }
}

impl From<&RemoteBranch> for UpstreamBranch {
    fn from(branch: &RemoteBranch) -> Self {
        UpstreamBranch::Remote(branch.clone())
    }
}

impl From<RemoteBranch> for UpstreamBranch {
    fn from(branch: RemoteBranch) -> Self {
        UpstreamBranch::Remote(branch)
    }
}

impl From<UpstreamBranch> for Upstream {
    fn from(source: UpstreamBranch) -> Self {
        match source {
            UpstreamBranch::Local(branch) => Upstream::Local {
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
            UpstreamBranch::Remote(branch) => Upstream::Remote {
                remote: branch.repository().site().name().to_string(),
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
        }
    }
}
