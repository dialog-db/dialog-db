//! The half of a revision's behaviour that needs types living above
//! `dialog-capability`.
//!
//! [`Revision`] itself is the identity and clock: its fields are built from
//! the [`history`](dialog_capability::history) types, so it lives in
//! `dialog-capability` (which `dialog-artifacts` depends on). Deriving a
//! revision's lineage needs this crate's [`schema`], and its in-tree record
//! needs `dialog_artifacts::history::RevisionRecord` — both above that
//! layer. They are supplied here as an extension trait.

use dialog_artifacts::Entity;
use dialog_artifacts::history::{REVISION_RECORD_FORMAT, RevisionRecord};
use dialog_capability::history::{Origin, Version};
use dialog_capability::{Did, Revision};

use crate::schema;

/// Revision behaviour that depends on the repository schema and the in-tree
/// revision record.
pub trait RevisionExt {
    /// The branch-on-replica entity this revision was minted on: the schema
    /// [`Branch`](crate::schema::Branch) entity, content-derived from the
    /// `(profile, subject)` origin and the branch name — the same entity the
    /// query layer injects overlay facts for on every branch.
    fn lineage(&self) -> Entity;

    /// The [`Origin`] of this revision: the lineage-scoped identity of its
    /// issuer, derived from the schema branch entity (which already folds in
    /// the profile, the subject, and the branch name) and the issuer.
    ///
    /// The branch entity converges across sessions of the same replica, but
    /// a lineage must identify a single sequential actor, so the issuer —
    /// the per-session operator key — disambiguates operators advancing the
    /// same branch.
    fn origin(&self) -> Origin;

    /// The [`Version`] identifying this revision: its origin paired with its
    /// edition.
    fn version(&self) -> Version;

    /// The content-derived entity identifying this revision — the entity
    /// onto which commit metadata can be associated, like on any other
    /// entity.
    fn entity(&self) -> Entity;

    /// This revision's [`RevisionRecord`] — everything the revision states
    /// about itself as one atomic fact, ready to be signed and written into
    /// the tree.
    ///
    /// The revision's tree root is deliberately not in the record: the
    /// record lives in that tree, so the root cannot appear inside itself.
    fn record(&self, parents: Vec<Version>, skips: Vec<Version>) -> RevisionRecord;
}

/// The version-control [`Origin`] for the given lineage (branch) entity
/// advanced by `issuer`. Both identifiers are length-prefixed in the
/// derivation, keeping it injective.
pub fn origin_of(lineage: &Entity, issuer: &Did) -> Origin {
    Origin::derive_from_identifiers([lineage.as_str(), issuer.as_str()])
}

/// The entity for the revision identified by `version`. Any replica that
/// knows a revision's version derives the same entity.
pub fn entity_of(version: &Version) -> Entity {
    version
        .entity_did()
        .parse()
        .expect("a did:key URI formed from a 32-byte hash is always a valid entity")
}

impl RevisionExt for Revision {
    fn lineage(&self) -> Entity {
        let origin = schema::Origin::new(self.authority.clone(), self.subject.clone());
        schema::Branch::new(&origin, self.branch.as_str()).this
    }

    fn origin(&self) -> Origin {
        origin_of(&self.lineage(), &self.issuer)
    }

    fn version(&self) -> Version {
        self.version_with(self.origin())
    }

    fn entity(&self) -> Entity {
        entity_of(&self.version())
    }

    fn record(&self, parents: Vec<Version>, skips: Vec<Version>) -> RevisionRecord {
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
}
