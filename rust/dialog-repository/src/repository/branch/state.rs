use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    string::FromUtf8Error,
};

use crate::repository::node_reference::NodeReference;
use crate::repository::remote::RemoteName;

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchName(String);

impl BranchName {
    /// Creates a new branch name from a string.
    pub fn new(name: String) -> Self {
        BranchName(name)
    }

    /// Returns a reference to the branch name string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl KeyType for BranchName {
    fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for BranchName {
    type Error = FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(BranchName(String::from_utf8(bytes)?))
    }
}

impl Display for BranchName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl From<&BranchName> for BranchName {
    fn from(value: &BranchName) -> Self {
        value.clone()
    }
}

impl From<&str> for BranchName {
    fn from(value: &str) -> Self {
        BranchName(value.to_string())
    }
}

impl From<String> for BranchName {
    fn from(value: String) -> Self {
        BranchName(value)
    }
}

/// Upstream represents some branch being tracked.
///
/// The `tree` field stores the upstream's tree root at the time of last
/// sync — used as the base for three-way merge when rebasing.
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
