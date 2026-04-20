use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::tree::TreeReference;

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
    pub tree: TreeReference,

    /// Parent tree roots this revision is based on. Empty for the first revision.
    pub cause: HashSet<TreeReference>,

    /// Period counter. Increments when a different issuer commits (sync boundary).
    pub period: usize,

    /// Moment counter. Increments on each commit within the same period by
    /// the same issuer. Resets to 0 when period advances.
    pub moment: usize,
}
