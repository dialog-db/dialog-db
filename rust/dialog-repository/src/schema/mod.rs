//! Typed schema for the facts a dialog-db repository writes about itself.
//!
//! Mirrors the equivalent module in
//! [`tonk-schema`](https://github.com/tonk-labs/tonk/tree/staging/rust/tonk-schema/src)
//! — backported here so `dialog-repository` can describe its own
//! branches, remotes, and replicas without an external dep. Each
//! repository has a meta-branch (or layer) carrying these concepts:
//! its replicas, the branches on each replica, the remotes those
//! replicas track, etc.
//!
//! # Entity identity
//!
//! Two complementary identity schemes:
//!
//! - **Intrinsic** — for entities that have their own cryptographic
//!   identity (profiles, repository subjects). The entity URI is just
//!   the DID; use [`prelude::DidExt::this`].
//!
//! - **Content-derived** — for entities defined by their inputs (a
//!   replica is `(profile, subject)`, a branch is `(replica, name)`).
//!   The entity URI is `did:key:z6Mk<base58(blake3(dag-cbor(inputs)))>`;
//!   use [`prelude::EntityExt::of`]. Two parties independently
//!   describing the same logical entity converge on the same URI.
//!
//! # Concept namespacing
//!
//! Each concept's attributes live in their own
//! [`domain`](crate::schema::domain) submodule
//! (`xyz.tonk.replica`, `xyz.tonk.branch`, `xyz.tonk.remote`) so a
//! `Branch:` query never matches a `Remote:` entity even though both
//! carry a `name` and an `origin`.
//!
//! Cross-cutting attributes that any entity might carry —
//! human-readable [`meta::Description`], a published
//! [`meta::Name`] — live under the shared `dialog.meta` /
//! `dialog.name` namespaces.
//!
//! # Naming note
//!
//! [`Branch`] (the schema concept) coexists with
//! [`crate::Branch`] (the persistent handle). They share a name on
//! purpose — both describe "the branch named X on this replica" —
//! but the schema concept is a *fact set* you assert into a layer
//! and query back, while the handle is the imperative API for
//! reading/writing the branch's storage. Code that uses both should
//! disambiguate via `crate::schema::Branch` vs the bare `Branch`.

pub mod prelude;

pub mod domain;

pub mod meta;

pub mod replica;
pub use replica::*;

pub mod branch;
pub use branch::*;

pub mod remote;
pub use remote::*;

pub mod tracking_branch;
pub use tracking_branch::*;
