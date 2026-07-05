use crate::TreeReference;
use crate::schema::{self, EntityExt as _};
use dialog_artifacts::history::{
    Cause, Claim, Edition, Origin, REVISION_ATTRIBUTE, Record, SKIP_ATTRIBUTE, Version,
};
use dialog_artifacts::{Attribute, DialogArtifactsError, Entity, Value};
use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::FromStr;

/// Canonical dag-cbor input for deriving a revision's entity from its
/// [`Version`] (see [`EntityExt`](crate::schema::EntityExt)). The version is
/// globally unique, so two replicas that know a revision's version converge
/// on the same entity — and can attach (or query) metadata for it without
/// holding the revision itself.
#[derive(Serialize)]
enum RevisionHash<'a> {
    Revision { origin: &'a [u8], edition: u64 },
}

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
        Entity::of(&RevisionHash::Revision {
            origin: version.origin.key_bytes().as_slice(),
            edition: version.edition.value(),
        })
    }

    /// The history records describing this revision:
    ///
    /// - its DAG edge — a `dialog.db/revision` claim on the branch lineage
    ///   entity whose value is the revision entity and whose cause lists the
    ///   parent revision versions (what
    ///   [`common_ancestor`](dialog_artifacts::history::common_ancestor)
    ///   traverses),
    /// - its skip links — one `dialog.db/skip` claim per level, whose cause
    ///   leaps 2^level first-parent steps back (computed by
    ///   [`extend_skips`](dialog_artifacts::history::extend_skips); empty
    ///   for genesis and merge revisions), and
    /// - its attribute claims on the revision entity (edition, branch,
    ///   issuer, authority, and one `cause` per parent revision entity), so
    ///   the revision is describable and joinable like any other entity.
    ///
    /// The revision's tree root is deliberately not among the claims: the
    /// records themselves live in that tree, so the root cannot appear
    /// inside itself. The head [`Revision`] carries the root; `cause` on
    /// the head carries the parents' roots.
    pub fn records(
        &self,
        parents: impl IntoIterator<Item = Version>,
        skips: &[(u32, Version)],
    ) -> Result<Vec<(Version, Record)>, DialogArtifactsError> {
        // Derive the schema entities once: `version()` and `entity()` each
        // recompute the lineage (two content-derived entities) on their own.
        let lineage = self.lineage();
        let version = Version::new(Self::origin_of(&lineage, &self.issuer), self.edition);
        let this = Self::entity_of(&version);
        let parents: Vec<Version> = parents.into_iter().collect();

        let mut records = Vec::with_capacity(5 + parents.len() + skips.len());
        records.push((
            version,
            Record::Assert(Claim {
                the: Attribute::from_str(REVISION_ATTRIBUTE)?,
                of: lineage.clone(),
                is: Value::Entity(this.clone()),
                cause: parents.iter().copied().collect(),
            }),
        ));

        for (level, target) in skips {
            records.push((
                version,
                Record::Assert(Claim {
                    the: Attribute::from_str(SKIP_ATTRIBUTE)?,
                    of: this.clone(),
                    is: Value::UnsignedInt(u128::from(*level)),
                    cause: Cause::from(*target),
                }),
            ));
        }

        let mut attribute = |the: &str, is: Value| -> Result<(), DialogArtifactsError> {
            records.push((
                version,
                Record::Assert(Claim {
                    the: Attribute::from_str(the)?,
                    of: this.clone(),
                    is,
                    cause: Cause::genesis(),
                }),
            ));
            Ok(())
        };

        attribute(
            "dialog.revision/edition",
            Value::UnsignedInt(u128::from(self.edition.value())),
        )?;
        attribute("dialog.revision/branch", Value::Entity(lineage))?;
        attribute(
            "dialog.revision/issuer",
            Value::Entity(Entity::from_str(self.issuer.as_str())?),
        )?;
        attribute(
            "dialog.revision/authority",
            Value::Entity(Entity::from_str(self.authority.as_str())?),
        )?;
        for parent in &parents {
            attribute(
                "dialog.revision/cause",
                Value::Entity(Self::entity_of(parent)),
            )?;
        }

        Ok(records)
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
