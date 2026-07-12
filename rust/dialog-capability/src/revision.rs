//! Repository revision types: a logical clock plus the search-tree
//! root that together name a repository's state at a point in time.
//!
//! These are plain serde data types with no dependency on the datalog
//! query engine or any storage/transport backend. They live in this
//! light crate (which already owns [`Did`]) so a client that only needs
//! to name or (de)serialize a revision — e.g. a wire DTO decoded by a
//! web page — can do so without linking `dialog-query` or the storage
//! stack. `dialog-repository` re-exports both at their historical paths.

use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::Did;

/// The raw 32-byte Blake3 hash a [`TreeReference`] wraps. Kept as a
/// bare array (not a wrapper type) so the wire form is a plain byte
/// array, matching `dialog_storage::Blake3Hash`.
type TreeHash = [u8; 32];

/// A hash representing an empty (usually newly created) search tree.
///
/// Matches the search tree's null root sentinel
/// (`dialog_common::NULL_BLAKE3_HASH`) byte for byte.
pub const EMPTY_TREE_HASH: TreeHash = [0; 32];

/// Reference to a search tree by its root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TreeReference(TreeHash);

impl TreeReference {
    /// Returns a reference to the underlying hash.
    pub fn hash(&self) -> &TreeHash {
        &self.0
    }
}

impl Default for TreeReference {
    /// By default, a [`TreeReference`] points at the empty search tree.
    fn default() -> Self {
        Self(EMPTY_TREE_HASH)
    }
}

impl Display for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self, f)
    }
}

impl From<TreeHash> for TreeReference {
    fn from(hash: TreeHash) -> Self {
        Self(hash)
    }
}

impl From<TreeReference> for TreeHash {
    fn from(value: TreeReference) -> Self {
        let TreeReference(hash) = value;
        hash
    }
}

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
