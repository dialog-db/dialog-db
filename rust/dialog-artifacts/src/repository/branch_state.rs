use dialog_capability::Did;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use super::node_reference::NodeReference;
use super::revision::Revision;

/// Branch is similar to a git branch and represents a named state of
/// the work that is either diverged or converged from other workstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchState {
    /// Unique identifier of this fork.
    pub id: BranchId,

    /// Free-form human-readable description of this fork.
    pub description: String,

    /// Current revision associated with this branch.
    pub revision: Revision,

    /// Root of the search tree our this revision is based off.
    pub base: NodeReference,

    /// An upstream through which updates get propagated. Branch may
    /// not have an upstream.
    pub upstream: Option<UpstreamState>,
}

impl BranchState {
    /// Create a new fork from the given revision.
    pub fn new(id: BranchId, revision: Revision, description: Option<String>) -> Self {
        Self {
            description: description.unwrap_or_else(|| id.0.clone()),
            base: revision.tree.clone(),
            revision,
            upstream: None,
            id,
        }
    }

    /// Unique identifier of this fork.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Current revision of this branch.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Description of this branch.
    pub fn description(&self) -> &str {
        &self.description
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
pub struct BranchId(pub(crate) String);

impl BranchId {
    /// Creates a new branch identifier from a string.
    pub fn new(id: String) -> Self {
        BranchId(id)
    }

    /// Returns a reference to the branch identifier string.
    pub fn id(&self) -> &String {
        &self.0
    }
}

impl KeyType for BranchId {
    fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for BranchId {
    type Error = std::string::FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(BranchId(String::from_utf8(bytes)?))
    }
}

impl Display for BranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&BranchId> for BranchId {
    fn from(value: &BranchId) -> Self {
        value.clone()
    }
}

impl From<&str> for BranchId {
    fn from(value: &str) -> Self {
        BranchId(value.to_string())
    }
}

impl From<String> for BranchId {
    fn from(value: String) -> Self {
        BranchId(value)
    }
}

/// Upstream represents some branch being tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UpstreamState {
    /// A local branch upstream
    Local {
        /// Branch identifier
        branch: BranchId,
    },
    /// A remote branch upstream
    Remote {
        /// Remote site identifier
        site: super::Site,
        /// Branch identifier
        branch: BranchId,
        /// Subject DID of the repository being tracked
        subject: Did,
    },
}

impl UpstreamState {
    /// Returns the branch identifier of this upstream.
    pub fn id(&self) -> &BranchId {
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
