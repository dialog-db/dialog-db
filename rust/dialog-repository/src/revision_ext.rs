//! Schema-dependent revision helpers.
//!
//! [`Revision`](dialog_artifacts::Revision) lives in `dialog-artifacts`
//! with the rest of the version-control types, and carries its whole
//! identity inline (branch entity + issuer), so its behaviour is
//! inherent. What remains here is the one derivation that needs this
//! crate's [`schema`]: minting the branch entity from a replica and a
//! name.

use dialog_artifacts::Entity;
use dialog_capability::Did;
use dialog_capability::history::Origin;

use crate::schema;

/// The branch entity for the branch named `name` on the replica
/// `(subject, profile)`: the schema [`Branch`](crate::schema::Branch)
/// entity, content-derived from the [`Replica`](crate::schema::Replica)
/// and the name. This is the opaque identifier published heads carry in
/// place of the name — every copy of the branch derives the same one.
pub fn branch_of(subject: &Did, profile: &Did, name: &str) -> Entity {
    schema::Branch::new(schema::Replica::new(profile.clone(), subject.clone()), name).this
}

/// The version-control [`Origin`] for the given branch entity advanced by
/// `issuer`. Both identifiers are length-prefixed in the derivation,
/// keeping it injective.
pub fn origin_of(branch: &Entity, issuer: &Did) -> Origin {
    Origin::derive_from_identifiers([branch.as_str(), issuer.as_str()])
}
