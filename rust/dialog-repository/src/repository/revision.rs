use crate::TreeReference;
use crate::schema;
use dialog_artifacts::history::{
    Context, Edition, Origin, REVISION_RECORD_FORMAT, RevisionRecord, Version,
    verify_issuer_signature,
};
use dialog_artifacts::{DialogArtifactsError, Entity};
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

    /// The causal context (per-origin watermark) of this revision's
    /// ancestry, itself included. Publishing it with the head is what
    /// lets a peer read a replica's knowledge without walking its log:
    /// pull seeds its context memo from it, skips upstreams whose
    /// context is included in ours (nothing new), and adopts an
    /// upstream's tree wholesale when its context includes ours and we
    /// have no local novelty. Covered by the head signature (see
    /// [`Revision::payload`]), so adopting it is as trustworthy as
    /// adopting the head. `None` on heads minted before the field
    /// existed; readers fall back to deriving the context by the
    /// ancestry walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// The issuer's Ed25519 signature over [`Revision::payload`], with the
    /// key the issuer DID names (`did:key`). This is what binds the tree
    /// root to the issuer: the in-tree [`RevisionRecord`] signs everything
    /// else a revision states about itself, but cannot contain the root of
    /// the tree it lives in. Empty until the revision is published (the
    /// root is only final at publish time) — see [`Revision::verify`].
    #[serde(default, with = "serde_bytes")]
    pub signature: Vec<u8>,
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
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build the revision that follows `self`, advancing the edition.
    ///
    /// Whoever advances a revision has, by construction, seen it, so the new
    /// edition is `self + 1` no matter which issuer commits. The previous
    /// revision's tree root is recorded in `cause`.
    ///
    /// The subject is the minting branch's repository, passed explicitly
    /// rather than inherited from `self`: a branch may have adopted a head
    /// minted in a *different* repository (a fast-forward pull from a
    /// foreign subject), and commits on top of it belong to this branch's
    /// lineage scope, not the foreign one — otherwise the minted version
    /// would disagree with the version its own data was tagged with.
    pub fn advance(
        &self,
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
            cause: HashSet::from([self.tree.clone()]),
            edition: self.edition.successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build a merge revision combining `self` with `upstream`.
    ///
    /// The merge has seen both lineages, so its edition advances past both:
    /// `max(self, upstream) + 1`. The upstream tree is recorded as the
    /// causal ancestor; the branch's own prior tree is dropped from `cause`
    /// because it is now subsumed by the merged tree.
    ///
    /// As with [`Revision::advance`], the subject is the minting branch's
    /// repository, passed explicitly: either side of the merge may carry a
    /// foreign subject adopted through an earlier pull.
    pub fn merge(
        &self,
        upstream: &Revision,
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
            cause: HashSet::from([upstream.tree.clone()]),
            edition: self.edition.max(upstream.edition).successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// The branch-on-replica entity this revision was minted on: the schema
    /// [`Branch`](crate::schema::Branch) entity, content-derived from the
    /// `(profile, subject)` origin and the branch name — the same entity the
    /// query layer injects overlay facts for on every branch.
    pub fn lineage(&self) -> Entity {
        let origin = schema::Origin::new(self.authority.clone(), self.subject.clone());
        schema::Branch::new(&origin, self.branch.as_str()).this
    }

    /// The [`Origin`] of this revision: the lineage-scoped identity of its
    /// issuer, derived from the schema branch entity (which already folds in
    /// the profile, the subject, and the branch name) and the issuer.
    ///
    /// The branch entity converges across sessions of the same replica, but
    /// a lineage must identify a single sequential actor, so the issuer —
    /// the per-session operator key — disambiguates operators advancing the
    /// same branch.
    pub fn origin(&self) -> Origin {
        Self::origin_of(&self.lineage(), &self.issuer)
    }

    /// The version-control [`Origin`] for the given lineage (branch) entity
    /// advanced by `issuer`. Both identifiers are length-prefixed in the
    /// derivation, keeping it injective.
    pub fn origin_of(lineage: &Entity, issuer: &Did) -> Origin {
        Origin::derive_from_identifiers([lineage.as_str(), issuer.as_str()])
    }

    /// The [`Version`] identifying this revision: its origin paired with its
    /// edition. Versions sort by causal depth, and two versions with the
    /// same edition but different origins identify concurrent revisions.
    pub fn version(&self) -> Version {
        Version::new(self.origin(), self.edition)
    }

    /// The content-derived entity identifying this revision — the entity
    /// onto which commit metadata can be associated, like on any other
    /// entity.
    pub fn entity(&self) -> Entity {
        Self::entity_of(&self.version())
    }

    /// The entity for the revision identified by `version`. Any replica that
    /// knows a revision's version derives the same entity.
    pub fn entity_of(version: &Version) -> Entity {
        version.entity()
    }

    /// This revision's [`RevisionRecord`] — everything the revision states
    /// about itself as one atomic fact, ready to be signed
    /// ([`RevisionRecord::payload`]) and written into the tree
    /// ([`RevisionRecord::entries`]). The record atomically carries the
    /// revision's parents (the DAG edge ancestor traversal follows), its
    /// skip links, and its attribution; individual fields are exposed to
    /// queries through formulas over the record rather than as separate
    /// facts.
    ///
    /// The revision's tree root is deliberately not in the record: the
    /// record lives in that tree, so the root cannot appear inside itself.
    /// The head [`Revision`] carries the root — bound to the issuer by its
    /// own signature over [`Revision::payload`] — and `cause` on the head
    /// carries the parents' roots.
    pub fn record(&self, parents: Vec<Version>, skips: Vec<Version>) -> RevisionRecord {
        RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            lineage: self.lineage(),
            issuer: self.issuer.to_string(),
            authority: self.authority.to_string(),
            parents,
            skips,
            signature: Vec::new(),
        }
    }

    /// The canonical signing payload of this revision: every field except
    /// the signature, deterministically encoded. Variable-width fields are
    /// length-prefixed to keep the encoding injective; the unordered
    /// `cause` set is sorted.
    ///
    /// ```text
    /// (length (8, big-endian) ++ UTF-8) for subject, issuer, authority, branch
    /// tree (32)
    /// cause count (8, big-endian) ++ roots (32 each, sorted)
    /// edition (8, big-endian)
    /// context, when present:
    ///     0x01 ++ entry count (8, big-endian)
    ///          ++ entries (origin (32) ++ edition (8, big-endian), sorted)
    /// ```
    ///
    /// A head without a context appends nothing after the edition (the
    /// pre-context payload shape), and a head with one appends the `0x01`
    /// marker plus the sorted watermark entries. The two shapes differ in
    /// length for any fixed prefix, so the encoding stays injective: a
    /// signature over one can never validate the other.
    pub fn payload(&self) -> Vec<u8> {
        let mut cause: Vec<&TreeReference> = self.cause.iter().collect();
        cause.sort_by(|left, right| left.hash().cmp(right.hash()));

        let mut bytes = Vec::new();
        for field in [
            self.subject.as_str(),
            self.issuer.as_str(),
            self.authority.as_str(),
            self.branch.as_str(),
        ] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes.extend_from_slice(self.tree.hash());
        bytes.extend_from_slice(&(cause.len() as u64).to_be_bytes());
        for tree in cause {
            bytes.extend_from_slice(tree.hash());
        }
        bytes.extend_from_slice(&self.edition.key_bytes());
        if let Some(context) = &self.context {
            bytes.push(0x01);
            bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
            for (origin, edition) in context.iter() {
                bytes.extend_from_slice(&origin.0);
                bytes.extend_from_slice(&edition.key_bytes());
            }
        }
        bytes
    }

    /// Verify that the signature is the issuer's Ed25519 signature over
    /// [`Revision::payload`], resolving the key from the issuer's
    /// `did:key`. This is the check a replica runs before adopting a head
    /// it did not mint (e.g. on pull): a forged or tampered head — wrong
    /// tree root, reattributed issuer, adjusted edition — fails here.
    pub fn verify(&self) -> Result<(), DialogArtifactsError> {
        verify_issuer_signature(self.issuer.as_str(), &self.payload(), &self.signature)
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
        let second = base.advance(
            tree(1),
            did!("test:repository"),
            "main",
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_eq!(second.edition, Edition::new(1));
        assert!(second.cause.contains(&base.tree));

        let third = second.advance(
            tree(2),
            did!("test:repository"),
            "main",
            did!("test:bob"),
            did!("test:profile"),
        );
        assert_eq!(third.edition, Edition::new(2));

        // A merge advances past both lineages
        let concurrent = second.advance(
            tree(3),
            did!("test:repository"),
            "main",
            did!("test:carol"),
            did!("test:profile"),
        );
        let merged = third.merge(
            &concurrent,
            tree(4),
            did!("test:repository"),
            "main",
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_eq!(merged.edition, Edition::new(3));
    }

    /// Adopting a head minted in a foreign repository (a fast-forward
    /// pull across subjects) must not drag later commits into the
    /// foreign lineage scope: advancing mints under the branch's own
    /// subject, so the version agrees with the one the commit path
    /// derives — and tags data with — up front.
    #[dialog_common::test]
    fn it_scopes_advances_to_the_minting_subject() {
        let foreign = Revision::new(
            TreeReference::from(EMPTY_TREE_HASH),
            did!("test:alice-repository"),
            "main",
            did!("test:alice"),
            did!("test:profile"),
        );

        // Bob's branch adopted the foreign head; his next commit mints
        // under his own repository subject.
        let ours = foreign.advance(
            tree(1),
            did!("test:bob-repository"),
            "main",
            did!("test:bob"),
            did!("test:profile"),
        );
        assert_eq!(ours.subject, did!("test:bob-repository"));
        assert_ne!(
            ours.origin(),
            foreign
                .advance(
                    tree(1),
                    did!("test:alice-repository"),
                    "main",
                    did!("test:bob"),
                    did!("test:profile"),
                )
                .origin(),
            "the subject participates in the lineage scope"
        );

        // Same for a merge on top of an adopted head.
        let merged = ours.merge(
            &foreign,
            tree(2),
            did!("test:bob-repository"),
            "main",
            did!("test:bob"),
            did!("test:profile"),
        );
        assert_eq!(merged.subject, did!("test:bob-repository"));
    }

    #[dialog_common::test]
    fn it_identifies_concurrent_revisions_by_version() {
        let base = genesis();

        // Two issuers advance from the same base without seeing each other:
        // same edition, different origins — concurrent by inspection
        let left = base.advance(
            tree(1),
            did!("test:repository"),
            "main",
            did!("test:alice"),
            did!("test:profile"),
        );
        let right = base.advance(
            tree(2),
            did!("test:repository"),
            "main",
            did!("test:bob"),
            did!("test:profile"),
        );
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
        let branched = base.advance(
            tree(3),
            did!("test:repository"),
            "feature",
            did!("test:alice"),
            did!("test:profile"),
        );
        assert_ne!(left.origin(), branched.origin());
        assert_ne!(left.version(), branched.version());
    }
}
