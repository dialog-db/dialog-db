use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::node_reference::NodeReference;

/// A revision represents a concrete state of the repository at a point in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// DID of the repository this revision belongs to.
    pub subject: Did,

    /// DID of the operator (ephemeral session key) that created this revision.
    pub issuer: Did,

    /// DID of the profile (long-lived key) that authorized this revision.
    pub authority: Did,

    /// Root of the search tree at this revision.
    pub tree: NodeReference,

    /// Parent tree roots this revision is based on. Empty for the first revision.
    pub cause: HashSet<NodeReference>,

    /// Period counter. Increments when a different issuer commits (sync boundary).
    pub period: usize,

    /// Moment counter. Increments on each commit within the same period by
    /// the same issuer. Resets to 0 when period advances.
    pub moment: usize,
}

impl Revision {
    /// Creates new revision with an empty tree.
    pub fn new(subject: Did, issuer: Did, authority: Did) -> Self {
        Self {
            subject,
            issuer,
            authority,
            tree: NodeReference::default(),
            period: 0,
            moment: 0,
            cause: HashSet::new(),
        }
    }

    /// DID of the repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// DID of the operator that created this revision.
    pub fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// DID of the profile that authorized this revision.
    pub fn authority(&self) -> &Did {
        &self.authority
    }

    /// Root of the search tree.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Period counter.
    pub fn period(&self) -> &usize {
        &self.period
    }

    /// Moment counter within this period.
    pub fn moment(&self) -> &usize {
        &self.moment
    }

    /// Parent tree roots this revision is based on.
    pub fn cause(&self) -> &HashSet<NodeReference> {
        &self.cause
    }
}
