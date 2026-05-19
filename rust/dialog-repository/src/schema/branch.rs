//! [`Branch`] — a branch within an origin (a device's view of a
//! repository).
//!
//! Coexists with [`crate::Branch`] (the persistent handle). Both
//! describe "the branch named X on this origin" but the schema
//! concept is a *fact set* synthesized at query time, while the
//! handle is the imperative API. Always disambiguate via
//! `crate::schema::Branch` in code that uses both.

// The `#[derive(Concept)]` and `#[derive(Attribute)]` macros generate
// helper types and associated functions without doc comments. Suppress
// the crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use dialog_artifacts::Entity;
use dialog_query::Concept;
use serde::Serialize;

use crate::schema::domain::branch::{Moment, Name, Origin, Period, Tree};
use crate::schema::prelude::*;

/// Hash input for [`Branch::this`].
///
/// `Branch` identity is `(origin, name)`: the branch named `"main"`
/// on one origin is distinct from the branch named `"main"` on a
/// different origin.
///
/// Not stored — used only inside [`Branch::new`] to compute the
/// entity hash.
#[derive(Serialize)]
enum This<'a> {
    Branch { origin: &'a Entity, name: &'a str },
}

/// A branch within an origin.
///
/// `this` is content-derived from `(origin, name)`:
///
/// - the same origin + the same branch name always yields the same
///   `Branch` entity (devices sharing a profile converge on the same
///   `Origin.this`, and therefore the same `Branch.this`), and
/// - different origins — or different names within one origin —
///   yield different entities.
///
/// # Redundant by design
///
/// `name` and `origin` duplicate information that went into the
/// hash. The hash is one-way, so without these attributes there
/// would be no way to answer "which branches are on this origin"
/// without knowing the inputs upfront.
///
/// # Constructing
///
/// [`Branch::new`] takes any [`AsRef<Entity>`] origin (typically a
/// [`Origin`](crate::schema::Origin)) plus a name:
///
/// ```no_run
/// use dialog_varsig::did;
/// use dialog_repository::schema::{Branch, Origin};
/// let origin = Origin::new(did!("test:profile"), did!("test:repo"));
/// let main = Branch::new(&origin, "main");
/// ```
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Branch {
    /// The branch's entity. Derived from `(origin, name)`.
    pub this: Entity,
    /// The branch's name on this origin.
    pub name: Name,
    /// The origin this branch lives on.
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
    /// `origin` is anything that views as an [`Entity`] — typically
    /// an [`Origin`](crate::schema::Origin) via its `AsRef<Entity>`
    /// impl. `name` is anything convertible into [`Name`]. Derives
    /// `this` from `(origin, name)` and stores `origin` as an
    /// attribute so every field is consistent with the entity hash.
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

/// The current revision of a branch.
///
/// Attached to the same entity as [`Branch`] (`this == Branch.this`)
/// — separate concept rather than fields on `Branch` because a
/// freshly-opened branch with no commits has no revision yet, and
/// dialog concepts require every field to be present. The optionality
/// falls out of presence/absence of the fact: if a `BranchRevision`
/// is asserted, the branch is at that revision; if not, it's empty.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BranchRevision {
    /// The branch entity (same as [`Branch::this`]).
    pub this: Entity,
    /// Tree hash of the current revision, base58-encoded.
    pub tree: Tree,
    /// Logical-clock period component.
    pub period: Period,
    /// Logical-clock moment component.
    pub moment: Moment,
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::schema::Origin as OriginConcept;
    use dialog_varsig::did;

    #[dialog_common::test]
    async fn same_origin_same_name_same_entity() {
        let o = OriginConcept::new(did!("test:p"), did!("test:r"));
        let a = Branch::new(&o, "main");
        let b = Branch::new(&o, "main");
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_name_different_entity() {
        let o = OriginConcept::new(did!("test:p"), did!("test:r"));
        let a = Branch::new(&o, "main");
        let b = Branch::new(&o, "meta");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_origin_different_entity() {
        let o1 = OriginConcept::new(did!("test:p1"), did!("test:r"));
        let o2 = OriginConcept::new(did!("test:p2"), did!("test:r"));
        let a = Branch::new(&o1, "main");
        let b = Branch::new(&o2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn different_repo_different_entity() {
        let o1 = OriginConcept::new(did!("test:p"), did!("test:r1"));
        let o2 = OriginConcept::new(did!("test:p"), did!("test:r2"));
        let a = Branch::new(&o1, "main");
        let b = Branch::new(&o2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    async fn attributes_reflect_origin() {
        let o = OriginConcept::new(did!("test:p"), did!("test:r"));
        let b = Branch::new(&o, "main");
        assert_eq!(b.origin.0, o.this);
    }

    #[dialog_common::test]
    async fn branch_revision_attaches_to_branch_entity() {
        let o = OriginConcept::new(did!("test:p"), did!("test:r"));
        let b = Branch::new(&o, "main");
        let rev = BranchRevision {
            this: b.this.clone(),
            tree: Tree("zSomeHash".into()),
            period: Period(1),
            moment: Moment(42),
        };
        assert_eq!(rev.this, b.this);
    }
}
