use dialog_capability::Did;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    string::FromUtf8Error,
};

use crate::repository::node_reference::NodeReference;
use crate::repository::remote::SiteName;
use crate::repository::revision::Revision;

/// Branch is similar to a git branch and represents a named state of
/// the work that is either diverged or converged from other workstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchState {
    /// Current revision associated with this branch.
    pub revision: Revision,

    /// Root of the search tree branich is based on.
    pub base: NodeReference,

    /// An upstream through which updates get propagated. Branch may
    /// not have an upstream.
    pub upstream: Option<UpstreamState>,
}

impl BranchState {
    /// Create a new fork from the given revision.
    pub fn new(revision: Revision) -> Self {
        Self {
            base: revision.tree.clone(),
            revision,
            upstream: None,
        }
    }

    /// Current revision of this branch.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Upstream branch of this branch.
    pub fn upstream(&self) -> Option<&UpstreamState> {
        self.upstream.as_ref()
    }

    /// Resets the branch to a new revision.
    pub fn reset(&mut self, revision: Revision) -> &mut Self {
        self.revision = revision;
        self
    }
}

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

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpstreamState {
    /// A local branch upstream
    Local {
        /// Branch name
        branch: BranchName,
    },
    /// A remote branch upstream
    Remote {
        /// Remote name (e.g., "origin")
        name: SiteName,
        /// Branch name
        branch: BranchName,
        /// Subject DID of the repository being tracked
        subject: Did,
    },
}

impl UpstreamState {
    /// Returns the branch name of this upstream.
    pub fn branch(&self) -> &BranchName {
        match self {
            Self::Local { branch } => branch,
            Self::Remote { branch, .. } => branch,
        }
    }

    /// Returns the subject DID for remote upstreams, None for local.
    pub fn subject(&self) -> Option<&Did> {
        match self {
            Self::Local { .. } => None,
            Self::Remote { subject, .. } => Some(subject),
        }
    }
}
