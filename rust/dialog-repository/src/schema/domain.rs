//! Per-concept attribute types in the `xyz.tonk.*` sub-domains.
//!
//! Each concept owns its own attribute namespace
//! (`xyz.tonk.replica`, `xyz.tonk.branch`, `xyz.tonk.remote`) so
//! its descriptor never matches entities of another shape — a
//! `Branch:` query would otherwise return [`Remote`] entities
//! since both have a `name` and an `origin` claim under the
//! shared `xyz.tonk` namespace.
//!
//! [`TrackingBranch`] reuses the `xyz.tonk.branch` namespace
//! because a tracking branch *is* a local branch with one extra
//! relation; its entities should still surface in a `branch:`
//! query.
//!
//! [`Remote`]: crate::schema::Remote
//! [`TrackingBranch`]: crate::schema::TrackingBranch

// The `#[derive(Attribute)]` macro generates helper types and
// associated functions without doc comments. Suppress the
// crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use crate::SiteAddress;
use dialog_artifacts::Entity;
use dialog_query::Attribute;
use std::convert::Infallible;

/// Attributes that live on [`Replica`] entities only.
///
/// [`Replica`]: crate::schema::Replica
pub mod replica {
    use super::{Attribute, Entity};

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.replica")]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.replica")]
    pub struct Subject(pub Entity);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.replica")]
    pub struct Profile(pub Entity);
}

/// Attributes that live on [`Branch`] entities (and
/// [`TrackingBranch`], which extends `Branch`).
///
/// [`Branch`]: crate::schema::Branch
/// [`TrackingBranch`]: crate::schema::TrackingBranch
pub mod branch {
    use super::{Attribute, Entity};

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.branch")]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.branch")]
    pub struct Origin(pub Entity);

    /// The upstream branch a local branch is tracking. Direction-
    /// explicit counterpart to [`Origin`]: asserting
    /// `local -upstream-> remote_branch` records that the local
    /// branch tracks the remote branch.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.branch")]
    pub struct Upstream(pub Entity);
}

/// Attributes that live on [`Remote`] entities only.
///
/// [`Remote`]: crate::schema::Remote
pub mod remote {
    use super::{Attribute, Entity, Infallible, SiteAddress};

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.remote")]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.remote")]
    pub struct Origin(pub Entity);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.remote")]
    pub struct Subject(pub Entity);

    /// Serialized [`SiteAddress`] bytes — the opaque payload a
    /// remote uses to locate a peer.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("xyz.tonk.remote")]
    pub struct Address(pub Vec<u8>);

    impl Address {
        /// Encode a [`SiteAddress`] as dag-cbor bytes.
        pub fn encode(address: &SiteAddress) -> Self {
            let bytes = serde_ipld_dagcbor::to_vec(address)
                .expect("SiteAddress is serde-serializable and dag-cbor-compatible");
            Self(bytes)
        }

        /// Decode the stored dag-cbor bytes back into a
        /// [`SiteAddress`].
        pub fn decode(&self) -> Result<SiteAddress, serde_ipld_dagcbor::DecodeError<Infallible>> {
            serde_ipld_dagcbor::from_slice(&self.0)
        }
    }
}
