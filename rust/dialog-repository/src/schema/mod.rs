//! Typed schema for the facts a dialog-db repository writes about itself.
//!
//! Each branch has a small, fixed set of facts describing its own
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
//! - **Intrinsic** — for entities that have their own cryptographic
//!   identity (profiles, repository subjects). The entity URI is just
//!   the DID; use [`prelude::DidExt::this`].
//!
//! - **Content-derived** — for entities defined by their inputs (an
//!   origin is `(profile, subject)`, a branch is `(origin, name)`).
//!   The entity URI is `did:key:z6Mk<base58(blake3(dag-cbor(inputs)))>`;
//!   use [`prelude::EntityExt::of`]. Two parties independently
//!   describing the same logical entity converge on the same URI.
//!
//! # Concept namespacing
//!
//! Per-concept attribute namespaces under [`domain`] —
//! `dialog.branch/*` for [`Branch`] + [`BranchRevision`],
//! `dialog.origin/*` for [`Origin`] — so a `Branch:` query never
//! matches an `Origin:` entity even though both could carry similar
//! attribute names.
//!
//! Cross-cutting attributes any entity might carry (a published
//! [`meta::Name`], a human [`meta::Description`]) live under the
//! shared `dialog.meta` / `dialog.name` namespaces.
//!
//! # Naming note
//!
//! [`Branch`] (the schema concept) coexists with
//! [`crate::Branch`] (the persistent handle). They share a name on
//! purpose — both describe "the branch named X on this origin" —
//! but the schema concept is a *fact set* synthesized at query time,
//! while the handle is the imperative API. Code that uses both should
//! disambiguate via `crate::schema::Branch` vs the bare `Branch`.

pub mod prelude;

pub mod domain;

pub mod meta;

pub mod origin;
pub use origin::*;

pub mod branch;
pub use branch::*;
