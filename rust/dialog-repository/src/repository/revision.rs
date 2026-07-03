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

    /// Name of the branch this revision was minted on. Part of the
    /// revision's [`Origin`] scope: a branch head is an independent
    /// sequential actor, so two branches advanced by the same issuer must
    /// not share an origin — otherwise they could mint colliding versions.
    #[serde(default)]
    pub branch: String,

    /// Root of the search tree at this revision.
    pub tree: TreeReference,

    /// Parent tree roots this revision is based on. Empty for the first revision.
    pub cause: HashSet<TreeReference>,

    /// Causal depth of this revision: `max(cause editions) + 1`, or zero for
    /// the first revision. Isomorphic to a Lamport timestamp.
    pub edition: Edition,

    /// Root of the history index at the most recent revision that recorded
    /// claim lineage (see `dialog_artifacts::history`), or `None` when no
    /// lineage has been recorded yet. Commits made through
    /// [`Branch::commit`](crate::Branch::commit) record every claim's causal
    /// lineage into the history index and update this root; operations that
    /// do not (yet) record lineage carry the previous root forward, so
    /// conflict detection degrades to `IncompleteHistory` for the claims
    /// they produced rather than losing the recorded history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<TreeReference>,
}

impl Revision {
    /// Build the first revision of a branch, with no causal ancestor and
    /// the genesis edition.
    pub fn new(
        tree: TreeReference,
        subject: Did,
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject,
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::new(),
            edition: Edition::GENESIS,
            history: None,
        }
    }

    /// Build the revision that follows `self`, advancing the edition.
    ///
    /// Whoever advances a revision has, by construction, seen it, so the new
    /// edition is `self + 1` no matter which issuer commits. The previous
    /// revision's tree root is recorded in `cause`.
    pub fn advance(
        &self,
        tree: TreeReference,
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject: self.subject.clone(),
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::from([self.tree.clone()]),
            edition: self.edition.successor(),
            history: self.history.clone(),
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
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject: self.subject.clone(),
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::from([upstream.tree.clone()]),
            edition: self.edition.max(upstream.edition).successor(),
            history: self.history.clone(),
        }
    }

    /// The [`Origin`] of this revision: the lineage-scoped identity of its
    /// issuer, derived from the issuer DID, the subject DID, and the branch
    /// the revision was minted on. Two origins are equal exactly when the
    /// same issuer committed to the same branch of the same repository.
    pub fn origin(&self) -> Origin {
        Origin::derive_from_identifiers([
            self.issuer.as_str(),
            self.subject.as_str(),
            self.branch.as_str(),
        ])
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
            "main",
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
        let second = base.advance(tree(1), "main", did!("test:alice"), did!("test:profile"));
        assert_eq!(second.edition, Edition::new(1));
        assert!(second.cause.contains(&base.tree));

        let third = second.advance(tree(2), "main", did!("test:bob"), did!("test:profile"));
        assert_eq!(third.edition, Edition::new(2));

        // A merge advances past both lineages
        let concurrent = second.advance(tree(3), "main", did!("test:carol"), did!("test:profile"));
        let merged = third.merge(
            &concurrent,
            tree(4),
            "main",
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
        let left = base.advance(tree(1), "main", did!("test:alice"), did!("test:profile"));
        let right = base.advance(tree(2), "main", did!("test:bob"), did!("test:profile"));
        assert_eq!(left.edition, right.edition);
        assert_ne!(left.origin(), right.origin());
        assert_ne!(left.version(), right.version());

        // The same issuer acting on two different repositories produces two
        // distinct origins
        let elsewhere = Revision::new(
            TreeReference::from(EMPTY_TREE_HASH),
            did!("test:other-repository"),
            "main",
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_ne!(left.origin(), elsewhere.origin());

        // ... and on two different branches of the same repository: each
        // branch head is an independent sequential actor
        let branched = base.advance(tree(3), "feature", did!("test:alice"), did!("test:profile"));
        assert_ne!(left.origin(), branched.origin());
        assert_ne!(left.version(), branched.version());
    }
}
