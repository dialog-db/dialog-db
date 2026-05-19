//! [`Origin`] — this device's view of a repository.
//!
//! Identity is `(profile, subject)`: two devices on the same profile
//! converge on the same `Origin.this`; two profiles viewing the same
//! repository diverge. The [`Branch`](crate::schema::Branch) entity is
//! `(origin, name)`-derived, so `Origin.this` is what anchors a
//! branch to its containing view.

// The `#[derive(Concept)]` and `#[derive(Attribute)]` macros generate
// helper types and associated functions without doc comments. Suppress
// the crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use dialog_artifacts::Entity;
use dialog_query::Concept;
use dialog_varsig::Did;
use serde::Serialize;

use crate::schema::Branch;
use crate::schema::domain::branch::Name as BranchName;
use crate::schema::domain::origin::{Profile, Subject};
use crate::schema::prelude::*;

/// This device's view of a specific repository.
///
/// The `this` entity is content-derived from the `(profile, subject)`
/// pair (see [`This`]), so:
///
/// - two devices holding the same profile converge on the same
///   origin entity for a given repository, and
/// - different profiles produce different origin entities even when
///   pointing at the same repository.
///
/// # Redundant by design
///
/// The [`Subject`] and [`Profile`] attributes carry the same two DIDs
/// that went into hashing the entity. The redundancy is intentional:
/// the hash is one-way, so without these attributes it would be
/// impossible to answer queries like "find the origin this profile
/// has for subject X" — you would need to know both inputs upfront
/// and re-hash to locate the entity.
///
/// # No name field
///
/// Dialog's `Origin` carries identity (`subject`, `profile`) only.
/// If a downstream system wants to attach a display name to the same
/// `Origin.this`, it can assert `dialog.meta/name` on the entity —
/// that attribute composes at query time without affecting identity.
///
/// # Constructing
///
/// [`Origin::new`] takes the profile and subject DIDs and derives
/// every field consistently:
///
/// ```no_run
/// use dialog_varsig::Did;
/// use dialog_repository::schema::Origin;
/// # fn example(profile: Did, subject: Did) -> Origin {
/// Origin::new(profile, subject)
/// # }
/// ```
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Origin {
    /// The origin's entity. Derived from `(profile, subject)`.
    pub this: Entity,
    /// Reference to the repository this origin is a view of.
    pub subject: Subject,
    /// Reference to the profile that owns this origin.
    pub profile: Profile,
}

/// Hash input for [`Origin::this`].
///
/// The single-variant enum shape tags the CBOR encoding with the
/// concept name: two inputs with the same data but different
/// concepts (e.g. an origin and a branch that happened to share
/// field shapes) produce distinct hashes.
#[derive(Debug, Clone, Serialize)]
enum This<'a> {
    Origin { subject: &'a Did, profile: &'a Did },
}

impl Origin {
    /// Build an origin concept from a profile DID and a subject DID.
    pub fn new(profile: Did, subject: Did) -> Self {
        Self {
            this: Entity::of(&This::Origin {
                subject: &subject,
                profile: &profile,
            }),
            subject: Subject(subject.this()),
            profile: Profile(profile.this()),
        }
    }

    /// The origin's entity.
    pub fn this(&self) -> &Entity {
        &self.this
    }

    /// Create a [`Branch`] concept rooted at this origin.
    pub fn branch(&self, name: impl Into<BranchName>) -> Branch {
        Branch::new(self, name)
    }
}

impl AsRef<Entity> for Origin {
    fn as_ref(&self) -> &Entity {
        &self.this
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::did;

    #[dialog_common::test]
    async fn same_inputs_same_entity() {
        let a = Origin::new(did!("test:p"), did!("test:r"));
        let b = Origin::new(did!("test:p"), did!("test:r"));
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_profile_different_entity() {
        let a = Origin::new(did!("test:p1"), did!("test:r"));
        let b = Origin::new(did!("test:p2"), did!("test:r"));
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_subject_different_entity() {
        let a = Origin::new(did!("test:p"), did!("test:r1"));
        let b = Origin::new(did!("test:p"), did!("test:r2"));
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn subject_and_profile_reflect_inputs() {
        let profile = did!("test:profile-x");
        let subject = did!("test:repo-y");
        let origin = Origin::new(profile.clone(), subject.clone());
        assert_eq!(origin.profile.0.to_string(), profile.as_str());
        assert_eq!(origin.subject.0.to_string(), subject.as_str());
    }
}
