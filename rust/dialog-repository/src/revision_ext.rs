//! The half of a revision's behaviour that needs types living above
//! `dialog-capability`.
//!
//! [`Revision`] itself is the identity and clock: its fields are built from
//! the [`history`](dialog_capability::history) types, so it lives in
//! `dialog-capability` (which `dialog-artifacts` depends on). Deriving a
//! revision's branch entity needs this crate's [`schema`], and its in-tree
//! record needs `dialog_artifacts::history::RevisionRecord` — both above
//! that layer. They are supplied here as an extension trait.

use dialog_artifacts::Entity;
use dialog_artifacts::history::{REVISION_RECORD_FORMAT, RevisionRecord};
use dialog_capability::history::{Origin, Version};
use dialog_capability::{Did, Revision};

use crate::schema;

/// The branch entity for the branch named `name` on the replica
/// `(subject, profile)`: the schema [`Branch`](crate::schema::Branch)
/// entity, content-derived from the [`Replica`](crate::schema::Replica)
/// and the name. This is the opaque identifier published heads carry in
/// place of the name — every copy of the branch derives the same one.
pub fn branch_of(subject: &Did, profile: &Did, name: &str) -> Entity {
    schema::Branch::new(schema::Replica::new(profile.clone(), subject.clone()), name).this
}

/// Revision behaviour that depends on the repository schema and the in-tree
/// revision record. The identity half — [`Revision::origin`] and
/// [`Revision::version`] — moved onto `Revision` itself once heads began
/// carrying their branch identifier; what remains here needs `Entity` or
/// the record type, which live above `dialog-capability`.
pub trait RevisionExt {
    /// The branch entity this revision was minted on, parsed from the
    /// identifier the head carries (see [`branch_of`]).
    fn branch_entity(&self) -> Entity;

    /// The content-derived entity identifying this revision — the entity
    /// onto which commit metadata can be associated, like on any other
    /// entity.
    fn entity(&self) -> Entity;

    /// This revision's [`RevisionRecord`] — everything the revision states
    /// about itself as one atomic fact, ready to be signed and written into
    /// the tree.
    ///
    /// The `authority` (the profile the issuer acts for) is passed in: the
    /// head no longer carries it — its identity is the branch identifier
    /// plus the issuer — but the record keeps the attribution readable.
    /// The revision's tree root is deliberately not in the record: the
    /// record lives in that tree, so the root cannot appear inside itself.
    fn record(&self, authority: &Did, parents: Vec<Version>, skips: Vec<Version>)
    -> RevisionRecord;
}

/// The version-control [`Origin`] for the given branch entity advanced by
/// `issuer`. Both identifiers are length-prefixed in the derivation,
/// keeping it injective.
pub fn origin_of(branch: &Entity, issuer: &Did) -> Origin {
    Origin::derive_from_identifiers([branch.as_str(), issuer.as_str()])
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
    fn branch_entity(&self) -> Entity {
        self.branch
            .parse()
            .expect("a head's branch identifier is a schema branch entity URI")
    }

    fn entity(&self) -> Entity {
        entity_of(&self.version())
    }

    fn record(
        &self,
        authority: &Did,
        parents: Vec<Version>,
        skips: Vec<Version>,
    ) -> RevisionRecord {
        RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            branch: self.branch_entity(),
            issuer: self.issuer.to_string(),
            authority: authority.to_string(),
            parents,
            skips,
            signature: Vec::new(),
        }
    }
}
