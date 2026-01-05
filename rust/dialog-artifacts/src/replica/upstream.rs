//! Upstream configuration for branches.
//!
//! An upstream represents another branch (local or remote) that a branch
//! tracks for synchronization purposes.

use serde::{Deserialize, Serialize};

use super::types::{BranchId, Site};

/// Upstream represents some branch being tracked.
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
        site: Site,
        /// Branch identifier
        branch: BranchId,
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

    /// Returns the site if this is a remote upstream.
    pub fn site(&self) -> Option<&Site> {
        match self {
            Self::Local { .. } => None,
            Self::Remote { site, .. } => Some(site),
        }
    }

    /// Returns true if this is a local upstream.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Returns true if this is a remote upstream.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}
