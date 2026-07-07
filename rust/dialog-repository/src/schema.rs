//! Typed schema for the facts a dialog-db repository writes about itself.
//!
//! Each branch carries a small, fixed set of facts describing its own
//! structure — the [`Origin`] (this device's view of the repository),
//! the [`Branch`] (name + origin), and the current [`BranchRevision`]
//! when one exists. These facts are **synthesized at query time** from
//! the branch handle plus the operator's identity (via
//! [`Identify`](dialog_effects::authority::Identify)); they never live
//! in the branch's persistent tree, which means user
//! [`Transaction`](crate::repository::branch::Transaction)s cannot
//! write or retract them.
//!
//! # Entity identity
//!
//! Two complementary identity schemes:
//!
//! - **Intrinsic** — for entities with their own cryptographic identity
//!   (profiles, repository subjects). The entity URI is just the DID;
//!   use [`DidExt::this`].
//!
//! - **Content-derived** — for entities defined by their inputs (an
//!   origin is `(profile, subject)`, a branch is `(origin, name)`). The
//!   entity URI is `did:key:z6Mk<base58(blake3(dag-cbor(inputs)))>`;
//!   use [`EntityExt::of`]. Two parties independently describing the
//!   same logical entity converge on the same URI.
//!
//! # Concept namespacing
//!
//! Per-concept attribute namespaces — [`branch`] holds the
//! `dialog.branch/*` attributes for [`Branch`] + [`BranchRevision`];
//! [`origin`] holds the `dialog.origin/*` attributes for [`Origin`].
//! Separating them keeps a `Branch:` query from cross-matching an
//! `Origin:` entity even though both could carry similar attribute
//! names.

use base58::ToBase58;
use dialog_artifacts::Entity;
use dialog_common::Blake3Hash;
use dialog_query::{Attribute, Concept};
use dialog_varsig::Did;
use serde::Serialize;

/// Derive an [`Entity`] from a serializable value.
///
/// `Entity` itself has no awareness of the content-derivation scheme
/// the schema uses. [`EntityExt::of`] hashes the dag-cbor encoding of
/// `value` and formats the result as a `did:key:z6Mk<base58>` URI.
///
/// # Canonical encoding
///
/// The hash is taken over `serde_ipld_dagcbor` bytes, so the resulting
/// entity depends only on the value's semantic content. Field
/// ordering, integer width, and map key sorting are fixed by the
/// dag-cbor specification, so independent implementations that
/// serialize the same logical value converge on the same entity.
///
/// # DID-key shape
///
/// The `did:key:z6Mk` prefix reuses the multibase/multicodec shape
/// dialog-db already uses for randomly generated entity URIs. The
/// `6Mk` prefix nominally indicates ed25519 key material, but nothing
/// in dialog-db enforces that the bytes actually *are* an ed25519
/// public key, so the same shape works for arbitrary 32-byte hashes.
/// If a future version of dialog-db begins validating the multicodec
/// prefix, this is the one place that would need to change.
pub trait EntityExt {
    /// Derive an `Entity` from the dag-cbor encoding of `value`.
    fn of<T: Serialize>(value: &T) -> Entity;
}

impl EntityExt for Entity {
    fn of<T: Serialize>(value: &T) -> Entity {
        let bytes = serde_ipld_dagcbor::to_vec(value)
            .expect("dag-cbor encoding should not fail for schema types");
        let hash = Blake3Hash::hash(&bytes);
        let encoded = hash.as_bytes().as_ref().to_base58();
        format!("did:key:z6Mk{encoded}")
            .parse()
            .expect("did:key URI formed from a 32-byte hash is always valid")
    }
}

/// View a [`Did`] as the entity it identifies.
///
/// DIDs and entities share the `did:method:identifier` URI shape, so
/// a DID string always parses as a valid [`Entity`]. Dialog treats
/// the two as distinct concerns — "a cryptographic identifier"
/// vs. "the subject of artifacts" — but when a schema concept's
/// identity *is* a DID (a profile, a repository subject), the DID is
/// also the concept's `this` entity.
pub trait DidExt {
    /// Produce the [`Entity`] this DID identifies.
    fn this(&self) -> Entity;
}

impl DidExt for Did {
    fn this(&self) -> Entity {
        self.as_str()
            .parse()
            .expect("DID string is always a valid Entity URI")
    }
}

/// Attribute newtypes for [`Branch`] / [`BranchRevision`] entities.
///
/// All attributes here live under the `dialog.branch` domain. The
/// kebab-cased struct name becomes the relation name —
/// [`Name`](branch::Name) → `dialog.branch/name`, etc.
pub mod branch {
    use super::{Attribute, Entity};

    /// `dialog.branch/name` — the human-readable branch name.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Name(
        /// The branch name string.
        pub String,
    );

    /// `dialog.branch/origin` — points at the
    /// [`Origin`](super::Origin) entity this branch lives on.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Origin(
        /// The origin entity URI.
        pub Entity,
    );

    /// `dialog.branch/tree` — the current revision's tree hash,
    /// base58-encoded.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Tree(
        /// Base58-encoded Blake3 hash of the current revision's tree
        /// root.
        pub String,
    );

    /// `dialog.branch/edition` — causal depth of the current revision.
    ///
    /// A Lamport timestamp derived from the revision DAG:
    /// `max(cause editions) + 1`, or zero for the first revision.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Edition(
        /// Edition of the revision's logical clock.
        pub u128,
    );

    /// `dialog.branch/revision` — the content-derived entity of the
    /// current revision: the join key from "where is this branch now?"
    /// to everything recorded about that revision (see
    /// [`RevisionRecord`](dialog_artifacts::history::RevisionRecord)).
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Revision(
        /// The revision entity URI.
        pub Entity,
    );
}

/// Attribute newtypes for [`Origin`] entities.
///
/// All attributes here live under the `dialog.origin` domain.
/// No `Name` field — dialog's `Origin` is identity-only. Downstream
/// code that wants a display name can additionally assert
/// `dialog.meta/name` on the same `Origin.this`.
pub mod origin {
    use super::{Attribute, Entity};

    /// `dialog.origin/subject` — the repository this origin views.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.origin")]
    pub struct Subject(
        /// The repository subject entity (its DID as Entity).
        pub Entity,
    );

    /// `dialog.origin/profile` — the profile that owns this origin.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.origin")]
    pub struct Profile(
        /// The profile entity (its DID as Entity).
        pub Entity,
    );
}

/// Attribute newtypes for the [`Revision`] / [`RevisionParent`]
/// concepts.
///
/// All attributes here live under the `dialog.revision` domain — and
/// none of them is ever stored. A revision describes itself with one
/// atomic `dialog.db/revision` record fact; these attributes are the
/// *conclusion shape* of the built-in rules (see
/// [`rules::revision_rule`](crate::rules)) that project the record's
/// fields at query time, verification included.
pub mod revision {
    use super::{Attribute, Entity};

    /// `dialog.revision/lineage` — the branch lineage entity the
    /// revision was minted on (a [`Branch`](super::Branch) entity).
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.revision")]
    pub struct Lineage(
        /// The lineage (branch) entity.
        pub Entity,
    );

    /// `dialog.revision/issuer` — the operator DID (as entity) that
    /// minted the revision.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.revision")]
    pub struct Issuer(
        /// The issuer entity (the operator's DID).
        pub Entity,
    );

    /// `dialog.revision/authority` — the profile DID (as entity) that
    /// authorized the revision.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.revision")]
    pub struct Authority(
        /// The authority entity (the profile's DID).
        pub Entity,
    );

    /// `dialog.revision/edition` — the revision's causal depth
    /// (a Lamport timestamp), derived from its parents.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.revision")]
    pub struct Edition(
        /// The revision's edition.
        pub u64,
    );

    /// `dialog.revision/parent` — a parent revision's entity; one per
    /// parent (two for a merge), so cardinality-many.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.revision")]
    #[cardinality(many)]
    pub struct Parent(
        /// A parent revision's entity.
        pub Entity,
    );
}

/// Attribute newtypes for the [`Session`] concept.
///
/// `Profile` / `Operator` are cardinality-one (one per session); the
/// `Branch` attribute is cardinality-many — asserted once per layered
/// branch the session is reading from. `Branch` deliberately isn't a
/// field on the [`Session`] concept (concept fields are cardinality-
/// one); query it separately as a standalone attribute on `db:session`
/// to enumerate the branches in scope.
pub mod session {
    use super::{Attribute, Entity};

    /// `dialog.session/profile` — the profile DID, as Entity.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.session")]
    pub struct Profile(
        /// Profile entity (the operator's `Identify`d profile DID).
        pub Entity,
    );

    /// `dialog.session/operator` — the operator DID, as Entity.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.session")]
    pub struct Operator(
        /// Operator entity (the operator's own DID — the
        /// session/ephemeral key, not the profile).
        pub Entity,
    );

    /// `dialog.session/branch` — cardinality-many; one assertion per
    /// branch the session has in scope (primary + each `.join(&b)`-ed
    /// branch).
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.session")]
    #[cardinality(many)]
    pub struct Branch(
        /// The branch entity (the `Branch.this` for that branch under
        /// the session's current origin).
        pub Entity,
    );
}

/// Hash input for [`Origin::this`].
///
/// The single-variant enum shape tags the CBOR encoding with the
/// concept name: two inputs with the same data but different
/// concepts produce distinct hashes.
#[derive(Debug, Clone, Serialize)]
enum OriginHash<'a> {
    Origin { subject: &'a Did, profile: &'a Did },
}

/// Hash input for [`Branch::this`].
///
/// `Branch` identity is `(origin, name)`. The concept-tag variant
/// keeps a branch and an origin with the same field shapes from
/// hashing to the same entity.
#[derive(Serialize)]
enum BranchHash<'a> {
    Branch { origin: &'a Entity, name: &'a str },
}

/// This device's view of a specific repository.
///
/// `this` is content-derived from `(profile, subject)` (see
/// [`OriginHash`]), so:
///
/// - two devices holding the same profile converge on the same
///   origin entity for a given repository, and
/// - different profiles produce different origin entities even when
///   pointing at the same repository.
///
/// # Redundant by design
///
/// [`origin::Subject`] and [`origin::Profile`] carry the same two
/// DIDs that went into the hash. The hash is one-way, so without
/// these attributes it would be impossible to answer "find the
/// origin this profile has for subject X" without re-hashing every
/// candidate. The attributes make the relationships discoverable
/// through normal queries.
///
/// # No name field
///
/// Dialog's `Origin` carries identity (`subject`, `profile`) only.
/// Downstream that wants a display name can assert `dialog.meta/name`
/// on the same `Origin.this`; that attribute composes at query time
/// without affecting identity.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Origin {
    /// The origin's entity. Derived from `(profile, subject)`.
    pub this: Entity,
    /// Reference to the repository this origin is a view of.
    pub subject: origin::Subject,
    /// Reference to the profile that owns this origin.
    pub profile: origin::Profile,
}

impl Origin {
    /// Build an origin concept from a profile DID and a subject DID.
    pub fn new(profile: Did, subject: Did) -> Self {
        Self {
            this: Entity::of(&OriginHash::Origin {
                subject: &subject,
                profile: &profile,
            }),
            subject: origin::Subject(subject.this()),
            profile: origin::Profile(profile.this()),
        }
    }
}

impl AsRef<Entity> for Origin {
    fn as_ref(&self) -> &Entity {
        &self.this
    }
}

/// A branch within an origin.
///
/// `this` is content-derived from `(origin, name)`. Devices sharing
/// a profile converge on the same `Origin.this`, and therefore the
/// same `Branch.this` — so the schema concept naturally describes
/// "the same branch" across devices.
///
/// # Coexistence with `crate::Branch`
///
/// Coexists with [`crate::Branch`] (the persistent handle). Both
/// describe "the branch named X on this origin" but the schema
/// concept is a *fact set* synthesized at query time, while the
/// handle is the imperative API. Always disambiguate via
/// `crate::schema::Branch` in code that uses both.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Branch {
    /// The branch's entity. Derived from `(origin, name)`.
    pub this: Entity,
    /// The branch's name on this origin.
    pub name: branch::Name,
    /// The origin this branch lives on.
    pub origin: branch::Origin,
}

impl Branch {
    /// Build a branch concept from an owning entity and a name.
    ///
    /// `origin` is anything that views as an [`Entity`] — typically
    /// an [`Origin`] via its `AsRef<Entity>` impl. Derives `this`
    /// from `(origin, name)` and stores `origin` as an attribute so
    /// every field is consistent with the entity hash.
    pub fn new(origin: impl AsRef<Entity>, name: impl Into<branch::Name>) -> Self {
        let origin = origin.as_ref();
        let name = name.into();
        Self {
            this: Entity::of(&BranchHash::Branch {
                origin,
                name: &name.0,
            }),
            origin: branch::Origin::from(origin.clone()),
            name,
        }
    }
}

impl AsRef<Entity> for Branch {
    fn as_ref(&self) -> &Entity {
        &self.this
    }
}

/// The current revision of a branch.
///
/// Attached to the same entity as [`Branch`] (`this == Branch.this`)
/// — a separate concept rather than fields on `Branch` because a
/// freshly-opened branch with no commits has no revision yet, and
/// dialog concepts require every field to be present. The
/// optionality falls out of presence/absence of the fact: if a
/// `BranchRevision` is asserted, the branch is at that revision; if
/// not, it's empty.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BranchRevision {
    /// The branch entity (same as [`Branch::this`]).
    pub this: Entity,
    /// Tree hash of the current revision, base58-encoded.
    pub tree: branch::Tree,
    /// Causal depth of the revision (Lamport timestamp).
    pub edition: branch::Edition,
    /// The revision entity — the join key to the revision's recorded
    /// metadata.
    pub revision: branch::Revision,
}

/// What a revision states about itself, projected from its signed
/// record.
///
/// `this` is the content-derived revision entity (the same entity the
/// overlay's [`BranchRevision::revision`] points at). The fields are
/// never stored as facts: built-in rules derive them at query time
/// from the branch's `dialog.db/revision` record fact via the
/// `dialog/revision` formula, which refuses records that don't carry
/// a valid issuer signature — forged attribution never surfaces in a
/// query result. The DAG edge (one row per parent) is the separate
/// cardinality-many [`RevisionParent`].
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Revision {
    /// The revision entity, derivable by any replica from the version.
    pub this: Entity,
    /// The branch lineage the revision was minted on.
    pub lineage: revision::Lineage,
    /// The operator DID (as entity) that minted the revision.
    pub issuer: revision::Issuer,
    /// The profile DID (as entity) that authorized it.
    pub authority: revision::Authority,
    /// The revision's causal depth.
    pub edition: revision::Edition,
}

/// One edge of the revision DAG: `this` revision was minted on top of
/// `parent`. Cardinality-many — a merge revision yields two rows; a
/// genesis revision yields none. Derived at query time from the same
/// signed record as [`Revision`].
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RevisionParent {
    /// The revision entity.
    pub this: Entity,
    /// A parent revision's entity.
    pub parent: revision::Parent,
}

/// What this query session is reading from.
///
/// Asserted on the fixed `db:session` entity by the auto-injection
/// path before every query. Carries the profile and operator DIDs
/// (as Entities) so a query can ask "who am I, and which operator
/// session am I in?" without reaching for env-specific accessors.
///
/// `Session` is cardinality-one on its three fields. The per-branch
/// listing — which branches are in this session's scope — lives on
/// the same entity as a cardinality-many [`session::Branch`]
/// attribute, queried separately when you need it.
///
/// Across multiple branches in one session you can still have only
/// one profile and one operator, so those go in the concept. Origin
/// is per-branch (different branches may live on different repos), so
/// it doesn't belong here — query the per-branch
/// [`Branch.origin`](Branch) instead.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Session {
    /// The fixed session entity: `db:session`.
    pub this: Entity,
    /// The profile DID, as Entity. From `Identify` on the operator.
    pub profile: session::Profile,
    /// The operator DID, as Entity. From `Identify` on the operator.
    pub operator: session::Operator,
}

impl Session {
    /// The conventional entity URI for the session concept.
    /// Always `db:session` — sessions don't get distinct identities;
    /// there's exactly one per query.
    pub fn entity() -> Entity {
        "db:session"
            .parse()
            .expect("db:session is a valid entity URI")
    }
}

/// One `(session, branch)` membership fact — the cardinality-many
/// counterpart to [`Session`].
///
/// [`Session`] holds the cardinality-one identity (`profile`,
/// `operator`); the set of branches in scope is cardinality-many, so
/// it can't be a `Session` field. `SessionBranch` carries one branch
/// entity per instance, all sharing `this == Session::entity()`.
/// Asserting N of them records N branches; a `Query<SessionBranch>`
/// yields one row per branch.
#[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionBranch {
    /// The session entity — always `db:session`, same as
    /// [`Session::this`].
    pub this: Entity,
    /// One branch in the session's scope.
    pub branch: session::Branch,
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::did;

    #[dialog_common::test]
    fn it_derives_same_entity_for_same_value() {
        assert_eq!(Entity::of(&"hello"), Entity::of(&"hello"));
    }

    #[dialog_common::test]
    fn it_derives_different_entities_for_different_values() {
        assert_ne!(Entity::of(&"alice"), Entity::of(&"bob"));
    }

    #[dialog_common::test]
    fn it_produces_did_key_uris() {
        let e = Entity::of(&"anything");
        assert!(e.to_string().starts_with("did:key:z6Mk"));
    }

    #[dialog_common::test]
    fn it_preserves_uri_through_did_this() {
        let d = did!("key:z6MkTestEntity");
        assert_eq!(d.this().to_string(), d.as_str());
    }

    #[dialog_common::test]
    fn it_derives_same_origin_for_same_profile_and_subject() {
        let a = Origin::new(did!("test:p"), did!("test:r"));
        let b = Origin::new(did!("test:p"), did!("test:r"));
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_derives_different_origins_for_different_profiles() {
        let a = Origin::new(did!("test:p1"), did!("test:r"));
        let b = Origin::new(did!("test:p2"), did!("test:r"));
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_derives_different_origins_for_different_subjects() {
        let a = Origin::new(did!("test:p"), did!("test:r1"));
        let b = Origin::new(did!("test:p"), did!("test:r2"));
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_reflects_subject_and_profile_on_origin_attributes() {
        let profile = did!("test:profile-x");
        let subject = did!("test:repo-y");
        let origin = Origin::new(profile.clone(), subject.clone());
        assert_eq!(origin.profile.0.to_string(), profile.as_str());
        assert_eq!(origin.subject.0.to_string(), subject.as_str());
    }

    #[dialog_common::test]
    fn it_derives_same_branch_for_same_origin_and_name() {
        let o = Origin::new(did!("test:p"), did!("test:r"));
        let a = Branch::new(&o, "main");
        let b = Branch::new(&o, "main");
        assert_eq!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_derives_different_branches_for_different_names() {
        let o = Origin::new(did!("test:p"), did!("test:r"));
        let a = Branch::new(&o, "main");
        let b = Branch::new(&o, "meta");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_derives_different_branches_for_different_origins() {
        let o1 = Origin::new(did!("test:p1"), did!("test:r"));
        let o2 = Origin::new(did!("test:p2"), did!("test:r"));
        let a = Branch::new(&o1, "main");
        let b = Branch::new(&o2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_derives_different_branches_for_different_repos() {
        let o1 = Origin::new(did!("test:p"), did!("test:r1"));
        let o2 = Origin::new(did!("test:p"), did!("test:r2"));
        let a = Branch::new(&o1, "main");
        let b = Branch::new(&o2, "main");
        assert_ne!(a.this, b.this);
    }

    #[dialog_common::test]
    fn it_reflects_origin_on_branch_attribute() {
        let o = Origin::new(did!("test:p"), did!("test:r"));
        let b = Branch::new(&o, "main");
        assert_eq!(b.origin.0, o.this);
    }

    #[dialog_common::test]
    fn it_attaches_branch_revision_to_branch_entity() {
        let o = Origin::new(did!("test:p"), did!("test:r"));
        let b = Branch::new(&o, "main");
        let rev = BranchRevision {
            this: b.this.clone(),
            tree: branch::Tree("zSomeHash".into()),
            edition: branch::Edition(42),
            revision: branch::Revision("test:revision".parse().expect("valid entity")),
        };
        assert_eq!(rev.this, b.this);
    }
}
