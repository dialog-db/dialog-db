use crate::TreeReference;
use dialog_artifacts::history::{Edition, Origin, Version};
use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A revision represents a concrete state of the repository at a point in time.
///
/// Causal position is derived from the revision DAG per
/// `notes/version-control.md`: the [`Edition`] is a Lamport timestamp
/// (`max(edition of every revision this one builds on) + 1`), so a higher
/// edition has seen at least as much causal history as any lower one,
/// regardless of which replica produced it — including across repository
/// boundaries. Paired with the [`Origin`] derived from `(issuer, subject)`,
/// it forms a globally unique [`Version`]: two revisions with the same
/// edition but different origins are concurrent by inspection.
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

    /// Causal depth of this revision: `max(cause editions) + 1`, or zero for
    /// the first revision. Isomorphic to a Lamport timestamp.
    pub edition: Edition,
}

impl Revision {
    /// Build the first revision of a branch, with no causal ancestor and
    /// the genesis edition.
    pub fn new(tree: TreeReference, subject: Did, issuer: Did, authority: Did) -> Self {
        Self {
            subject,
            issuer,
            authority,
            tree,
            cause: HashSet::new(),
            edition: Edition::GENESIS,
        }
    }

    /// Build the revision that follows `self`, advancing the edition.
    ///
    /// Whoever advances a revision has, by construction, seen it, so the new
    /// edition is `self + 1` no matter which issuer commits. The previous
    /// revision's tree root is recorded in `cause`.
    pub fn advance(&self, tree: TreeReference, issuer: Did, authority: Did) -> Self {
        Self {
            subject: self.subject.clone(),
            issuer,
            authority,
            tree,
            cause: HashSet::from([self.tree.clone()]),
            edition: self.edition.successor(),
        }
    }

    /// Build a merge revision combining `self` with `upstream`.
    ///
    /// The merge has seen both lineages, so its edition advances past both:
    /// `max(self, upstream) + 1`. The upstream tree is recorded as the
    /// causal ancestor; the branch's own prior tree is dropped from `cause`
    /// because it is now subsumed by the merged tree.
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
            edition: self.edition.max(upstream.edition).successor(),
        }
    }

    /// The [`Origin`] of this revision: the repository-scoped identity of
    /// its issuer, derived from the issuer and subject DIDs. Two origins are
    /// equal exactly when the same issuer committed to the same repository.
    pub fn origin(&self) -> Origin {
        Origin::derive_from_dids(self.issuer.as_str(), self.subject.as_str())
    }

    /// The [`Version`] identifying this revision: its origin paired with its
    /// edition. Versions sort by causal depth, and two versions with the
    /// same edition but different origins identify concurrent revisions.
    pub fn version(&self) -> Version {
        Version::new(self.origin(), self.edition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EMPTY_TREE_HASH;
    use dialog_varsig::did;

    fn tree(seed: u8) -> TreeReference {
        TreeReference::from([seed; 32])
    }

    fn genesis() -> Revision {
        Revision::new(
            TreeReference::from(EMPTY_TREE_HASH),
            did!("test:repository"),
            did!("test:alice"),
            did!("test:profile"),
        )
    }

    #[dialog_common::test]
    fn it_derives_editions_from_the_revision_dag() {
        let base = genesis();
        assert_eq!(base.edition, Edition::GENESIS);

        // Advancing increments the edition no matter which issuer commits:
        // whoever advances a revision has seen it
        let second = base.advance(tree(1), did!("test:alice"), did!("test:profile"));
        assert_eq!(second.edition, Edition::new(1));
        assert!(second.cause.contains(&base.tree));

        let third = second.advance(tree(2), did!("test:bob"), did!("test:profile"));
        assert_eq!(third.edition, Edition::new(2));

        // A merge advances past both lineages
        let concurrent = second.advance(tree(3), did!("test:carol"), did!("test:profile"));
        let merged = third.merge(
            &concurrent,
            tree(4),
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_eq!(merged.edition, Edition::new(3));
    }

    #[dialog_common::test]
    fn it_identifies_concurrent_revisions_by_version() {
        let base = genesis();

        // Two issuers advance from the same base without seeing each other:
        // same edition, different origins — concurrent by inspection
        let left = base.advance(tree(1), did!("test:alice"), did!("test:profile"));
        let right = base.advance(tree(2), did!("test:bob"), did!("test:profile"));
        assert_eq!(left.edition, right.edition);
        assert_ne!(left.origin(), right.origin());
        assert_ne!(left.version(), right.version());

        // The same issuer acting on two different repositories produces two
        // distinct origins
        let elsewhere = Revision::new(
            TreeReference::from(EMPTY_TREE_HASH),
            did!("test:other-repository"),
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_ne!(left.origin(), elsewhere.origin());
    }
}
