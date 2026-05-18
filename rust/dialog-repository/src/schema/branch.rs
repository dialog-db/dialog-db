//! [`Branch`] — a branch within a replica of a repository.
//!
//! Coexists with [`crate::Branch`] (the persistent handle). Both
//! describe "the branch named X on this replica" but the schema
//! concept is a *fact set* asserted into a layer and queried back,
//! while the handle is the imperative API. Always disambiguate via
//! `crate::schema::Branch` in code that uses both.

// The `#[derive(Concept)]` and `#[derive(Attribute)]` macros generate
// helper types and associated functions without doc comments. Suppress
// the crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use dialog_artifacts::Entity;
use dialog_query::Concept;
use serde::Serialize;

use crate::schema::domain::branch::{Name, Origin};
use crate::schema::prelude::*;

/// Hash input for [`Branch::this`].
///
/// `Branch` identity is `(replica, name)`: the branch named `"main"`
/// on one replica is distinct from the branch named `"main"` on a
/// different replica (whether that's a different profile's view of
/// the same repository, or an entirely different repository).
///
/// The single-variant enum shape tags the CBOR encoding with the
/// concept name, so a branch and a remote with the same
/// `(origin, name)` pair hash to different entities.
///
/// Not stored — constructed transiently inside [`Branch::new`] so
/// the hash can be computed.
#[derive(Serialize)]
enum This<'a> {
    Branch { origin: &'a Entity, name: &'a str },
}

/// A branch within a replica.
///
/// The `this` entity is content-derived from the replica's entity
/// and the branch name, so:
///
/// - the same replica + the same branch name always yields the same
///   `Branch` entity (devices sharing a profile converge on the same
///   `Replica.this`, and therefore the same `Branch.this`), and
/// - different replicas — or different names within one replica —
///   yield different entities.
///
/// # Redundant by design
///
/// The `name` and `origin` attributes duplicate information that
/// went into the hash. The hash is one-way, so without these
/// attributes there would be no way to answer "which branches are
/// on this replica" or "which branches belong to this repository"
/// without knowing the inputs upfront.
///
/// # Constructing
///
/// [`Branch::new`] takes any [`AsRef<Entity>`] origin (a [`Replica`]
/// for a local branch, a [`Remote`] for a remote-side branch) plus
/// a name, and derives every field consistently:
///
/// ```no_run
/// use dialog_varsig::did;
/// use dialog_repository::schema::{Branch, Replica};
/// let replica = Replica::new(
///     did!("test:profile"),
///     did!("test:repo"),
///     "home",
/// );
/// let main = Branch::new(&replica, "main");
/// ```
///
/// [`Replica`]: crate::schema::Replica
/// [`Remote`]: crate::schema::Remote
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Branch {
    /// The branch's entity. Derived from `(origin, name)`.
    pub this: Entity,
    /// The branch's name on this origin.
    pub name: Name,
    /// The origin (replica or remote) this branch lives on.
    pub origin: Origin,
}

impl AsRef<Entity> for Branch {
    fn as_ref(&self) -> &Entity {
        &self.this
    }
}

impl Branch {
    /// Build a branch concept from an owning entity and a name.
    ///
    /// The `origin` argument can be anything that views as an
    /// [`Entity`] — a [`Replica`](crate::schema::Replica) (for a
    /// local branch) or a [`Remote`](crate::schema::Remote) (for a
    /// remote-side branch) both work via their `AsRef<Entity>`
    /// impls. `name` takes anything convertible into [`Name`] —
    /// e.g. a `&str` — so callers don't have to wrap string
    /// literals. Derives `this` from `(origin, name)` and stores
    /// `origin` as an attribute so every field is consistent with
    /// the entity hash.
    pub fn new(origin: impl AsRef<Entity>, name: impl Into<Name>) -> Self {
        let origin = origin.as_ref();
        let name = name.into();
        Self {
            this: Entity::of(&This::Branch {
                origin,
                name: &name.0,
            }),
            origin: Origin::from(origin.clone()),
            name,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::schema::Replica;
    use dialog_varsig::did;

    #[dialog_common::test]
    async fn same_replica_same_name_same_entity() {
        let r = Replica::new(did!("test:p"), did!("test:r"), "home");
        let a = Branch::new(&r, "main");
        let b = Branch::new(&r, "main");
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_name_different_entity() {
        let r = Replica::new(did!("test:p"), did!("test:r"), "home");
        let a = Branch::new(&r, "main");
        let b = Branch::new(&r, "meta");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_replica_different_entity() {
        let r1 = Replica::new(did!("test:p1"), did!("test:r"), "home");
        let r2 = Replica::new(did!("test:p2"), did!("test:r"), "home");
        let a = Branch::new(&r1, "main");
        let b = Branch::new(&r2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_repo_different_entity() {
        let r1 = Replica::new(did!("test:p"), did!("test:r1"), "home");
        let r2 = Replica::new(did!("test:p"), did!("test:r2"), "home");
        let a = Branch::new(&r1, "main");
        let b = Branch::new(&r2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn attributes_reflect_replica() {
        let r = Replica::new(did!("test:p"), did!("test:r"), "home");
        let b = Branch::new(&r, "main");
        assert_eq!(b.origin.0, r.this);
    }

    #[dialog_common::test]
    async fn replica_name_does_not_affect_branch_entity() {
        // The replica's display name is not part of Replica.this, so
        // renaming the replica doesn't change the branch entity.
        let home = Replica::new(did!("test:p"), did!("test:r"), "home");
        let pics = Replica::new(did!("test:p"), did!("test:r"), "pictures");
        let a = Branch::new(&home, "main");
        let b = Branch::new(&pics, "main");
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn branch_on_replica_and_remote_differ() {
        // A `Branch` is polymorphic over its origin — the same name
        // on a replica vs. on a remote still produces distinct
        // entities because the origin entities themselves differ.
        use crate::schema::Remote;
        use crate::schema::domain::remote::Address;
        let replica = Replica::new(did!("test:p"), did!("test:r"), "home");
        let remote = Remote::new(
            &replica,
            did!("test:repo"),
            Address(b"addr".to_vec()),
            "origin",
        );
        let local = Branch::new(&replica, "main");
        let tracking = Branch::new(&remote, "main");
        assert_ne!(local.this, tracking.this);
        assert_eq!(local.origin.0, replica.this);
        assert_eq!(tracking.origin.0, remote.this);
    }
}
