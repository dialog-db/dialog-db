//! [`Replica`] — this device's view of a repository.

// The `#[derive(Concept)]` and `#[derive(Attribute)]` macros generate
// helper types and associated functions without doc comments. Suppress
// the crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use crate::SiteAddress;
use dialog_artifacts::Entity;
use dialog_query::Concept;
use dialog_varsig::Did;
use serde::Serialize;

use crate::schema::Branch;
use crate::schema::Remote;
use crate::schema::domain::branch::Name as BranchName;
use crate::schema::domain::remote::{Address, Name as RemoteName};
use crate::schema::domain::replica::{Name, Profile, Subject};
use crate::schema::prelude::*;

/// A replica — this device's view of a specific repository.
///
/// The `this` entity is content-derived from the `(profile, subject)`
/// pair (see [`This`]), so:
///
/// - two devices holding the same profile converge on the same
///   replica entity for a given repository, and
/// - different profiles produce different replica entities even when
///   pointing at the same repository.
///
/// The concept lives on the repository's meta branch. It is
/// typically the first thing asserted when a repository is opened
/// locally: writing the replica record announces "this profile has
/// a local view of this repository" and anchors subsequent
/// per-replica facts (branches, upstream configuration, etc.).
///
/// # Redundant by design
///
/// The [`Subject`] and [`Profile`] attributes carry the same two DIDs
/// that went into hashing the entity. The redundancy is intentional:
/// the hash is a one-way function, so without these attributes it
/// would be impossible to answer queries like "find the replica this
/// profile has for subject X" — you would need to know both inputs
/// upfront and re-hash to locate the entity. The stored attributes
/// make the relationships discoverable through normal queries.
///
/// # Constructing
///
/// [`Replica::new`] takes the profile and subject DIDs plus a name
/// and derives every field consistently:
///
/// ```no_run
/// use dialog_varsig::Did;
/// use dialog_repository::schema::Replica;
/// # fn example(profile: Did, subject: Did) -> Replica {
/// Replica::new(profile, subject, "home")
/// # }
/// ```
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Replica {
    /// The replica's entity. Derived from `(profile, subject)`.
    pub this: Entity,
    /// Human-readable name for the repository on this replica.
    pub name: Name,
    /// Reference to the repository this replica is a view of.
    pub subject: Subject,
    /// Reference to the profile that owns this replica.
    pub profile: Profile,
}

/// Hash input for [`Replica::this`].
///
/// The single-variant enum shape tags the CBOR encoding with the
/// concept name: two inputs with the same data but different
/// concepts (e.g. a replica and a branch that happened to share
/// field shapes) produce distinct hashes.
#[derive(Debug, Clone, Serialize)]
enum This<'a> {
    Replica { subject: &'a Did, profile: &'a Did },
}

impl Replica {
    /// Build a replica concept from a profile DID, a subject DID,
    /// and a name.
    ///
    /// Derives `this` from `(profile, subject)` and fills in the
    /// `subject` and `profile` attributes from the same DIDs so
    /// every field is consistent with the entity hash. `name`
    /// takes anything convertible into [`Name`] — e.g. a `&str`
    /// — so callers don't have to wrap string literals.
    pub fn new(profile: Did, subject: Did, name: impl Into<Name>) -> Self {
        Self {
            this: Entity::of(&This::Replica {
                subject: &subject,
                profile: &profile,
            }),
            subject: Subject(subject.this()),
            profile: Profile(profile.this()),
            name: name.into(),
        }
    }

    /// The replica's entity.
    pub fn this(&self) -> &Entity {
        &self.this
    }

    /// Create a [`Branch`] concept on this replica.
    ///
    /// `name` is anything convertible into a
    /// [`branch::Name`](crate::schema::domain::branch::Name),
    /// matching the [`Branch::new`] signature.
    pub fn branch(&self, name: impl Into<BranchName>) -> Branch {
        Branch::new(self, name)
    }

    /// Create a [`Remote`] concept on this replica.
    ///
    /// `name` accepts anything convertible into [`Name`]; the
    /// [`SiteAddress`] is encoded into an [`Address`] internally
    /// (we can't surface that as a `From` impl without clashing
    /// with the blanket one the `Attribute` derive emits — see
    /// [`Address::encode`]).
    pub fn remote(
        &self,
        name: impl Into<RemoteName>,
        subject: Did,
        address: &SiteAddress,
    ) -> Remote {
        Remote::new(self, subject, Address::encode(address), name)
    }
}

impl AsRef<Entity> for Replica {
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

    fn named(tag: &str) -> Name {
        Name(tag.into())
    }

    #[dialog_common::test]
    async fn same_origin_same_entity() {
        let a = Replica::new(did!("test:p"), did!("test:r"), named("home"));
        let b = Replica::new(did!("test:p"), did!("test:r"), named("home"));
        assert_eq!(a.this.to_string(), b.this.to_string());
    }

    #[dialog_common::test]
    async fn different_profile_different_entity() {
        let a = Replica::new(did!("test:p1"), did!("test:r"), named("home"));
        let b = Replica::new(did!("test:p2"), did!("test:r"), named("home"));
        assert_ne!(a.this.to_string(), b.this.to_string());
    }

    #[dialog_common::test]
    async fn name_does_not_affect_entity() {
        // The entity is derived from (profile, subject) alone, so
        // renaming a replica does not produce a new entity — it
        // produces a new name attribute on the existing one.
        let a = Replica::new(did!("test:p"), did!("test:r"), named("home"));
        let b = Replica::new(did!("test:p"), did!("test:r"), named("pictures"));
        assert_eq!(a.this.to_string(), b.this.to_string());
    }

    #[dialog_common::test]
    async fn subject_and_profile_reflect_inputs() {
        let profile = did!("test:profile-x");
        let subject = did!("test:repo-y");
        let replica = Replica::new(profile.clone(), subject.clone(), named("n"));
        assert_eq!(replica.profile.0.to_string(), profile.as_str());
        assert_eq!(replica.subject.0.to_string(), subject.as_str());
    }
}
