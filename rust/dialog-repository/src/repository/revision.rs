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

impl Revision {
    /// Build the first revision of a branch, with no causal ancestor and
    /// the logical clock reset to zero.
    pub fn new(tree: TreeReference, subject: Did, issuer: Did, authority: Did) -> Self {
        Self {
            subject,
            issuer,
            authority,
            tree,
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        }
    }

    /// Build the revision that follows `self`, advancing the logical clock.
    ///
    /// When the same issuer commits again the `moment` counter increments;
    /// a different issuer bumps `period` and resets `moment` to zero (this
    /// marks a sync boundary). The previous revision's tree root is
    /// recorded in `cause`.
    pub fn advance(&self, tree: TreeReference, issuer: Did, authority: Did) -> Self {
        let (period, moment) = if self.issuer == issuer {
            (self.period, self.moment + 1)
        } else {
            (self.period + 1, 0)
        };
        Self {
            subject: self.subject.clone(),
            issuer,
            authority,
            tree,
            cause: HashSet::from([self.tree.clone()]),
            period,
            moment,
        }
    }

    /// Build a merge revision combining `self` with `upstream`.
    ///
    /// A merge is a forced sync boundary: the new `period` jumps past both
    /// sides and `moment` resets to zero so later commits from either
    /// issuer compare cleanly against the merged state. The upstream
    /// tree is recorded as the causal ancestor; the branch's own prior
    /// tree is dropped from `cause` because it is now subsumed by the
    /// merged tree.
    pub fn merge(
        &self,
        upstream: &Revision,
        tree: TreeReference,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject: self.subject.clone(),
            issuer,
            authority,
            tree,
            cause: HashSet::from([upstream.tree.clone()]),
            period: self.period.max(upstream.period) + 1,
            moment: 0,
        }
    }
}
