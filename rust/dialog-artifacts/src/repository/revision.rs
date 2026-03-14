use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::node_reference::NodeReference;

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// DID of the site where this revision was created. It is expected to be
    /// the DID of a signing principal representing a tool acting on the
    /// author's behalf. In the future we expect to have a signed delegation
    /// chain from user to this site.
    pub issuer: Did,

    /// Reference the root of the search tree.
    pub tree: NodeReference,

    /// Set of parent tree roots this revision is based on. An empty set means
    /// this is the first revision (based on the empty tree).
    ///
    /// Each entry is the tree root of a revision that was merged to produce
    /// this revision's base. Equivalent to `parents` in git commit objects.
    ///
    /// TODO: Store revision metadata as claims in the tree so that a `Revision`
    /// can be reconstructed from a tree root alone, enabling DAG traversal
    /// without external state.
    pub cause: HashSet<NodeReference>,

    /// Period indicating when this revision was created. This MUST be derived
    /// from the `cause`al revisions and it must be greater by one than the
    /// maximum period of the `cause`al revisions that have different `by` from
    /// this revision. More simply we create a new period whenever we
    /// synchronize.
    pub period: usize,

    /// Moment at which this revision was created. It represents a number of
    /// transactions that have being made in this period. If `cause`al revisions
    /// have a revision from same `by` this MUST be value greater by one,
    /// otherwise it should be `0`. This implies that when we sync we increment
    /// `period` and reset `moment` to `0`. And when we create a transaction we
    /// increment `moment` by one and keep the same `period`.
    pub moment: usize,
}

impl Revision {
    /// Creates new revision with an empty tree
    pub fn new(issuer: Did) -> Self {
        Self {
            issuer,
            tree: NodeReference::default(),
            period: 0,
            moment: 0,
            cause: HashSet::new(),
        }
    }

    /// DID of the issuer of this revision.
    pub fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// The component of the [`Revision`] that corresponds to the root of the
    /// search index.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Period when changes have being made
    pub fn period(&self) -> &usize {
        &self.period
    }

    /// Number of transactions made by this issuer since the beginning of
    /// this epoch
    pub fn moment(&self) -> &usize {
        &self.moment
    }

    /// Parent tree roots this revision is based on.
    pub fn cause(&self) -> &HashSet<NodeReference> {
        &self.cause
    }
}
