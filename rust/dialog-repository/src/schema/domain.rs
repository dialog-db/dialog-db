//! Per-concept attribute types under the `dialog.*` namespace.
//!
//! Each concept owns its own attribute slot (`dialog.origin`,
//! `dialog.branch`) so its descriptor never cross-matches against
//! entities of another shape.
//!
//! [`Branch`]: crate::schema::Branch
//! [`BranchRevision`]: crate::schema::BranchRevision
//! [`Origin`]: crate::schema::Origin

// The `#[derive(Attribute)]` macro generates helper types and
// associated functions without doc comments. Suppress the
// crate-level `missing_docs` lint for this module so the macros
// compile under `-D warnings`.
#![allow(missing_docs)]

use dialog_artifacts::Entity;
use dialog_query::Attribute;

/// Attributes that live on [`Origin`](crate::schema::Origin) entities.
///
/// No `Name` here — an origin in this schema doesn't carry a display
/// name. Downstream code that wants one can additionally assert
/// `dialog.meta/name` on the same `Origin.this`; it composes at query
/// time without participating in identity.
pub mod origin {
    use super::{Attribute, Entity};

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.origin")]
    pub struct Subject(pub Entity);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.origin")]
    pub struct Profile(pub Entity);
}

/// Attributes that live on [`Branch`] entities — both the identity
/// fields ([`Name`], [`Origin`]) and the per-revision fields carried
/// by [`BranchRevision`] ([`Tree`], [`Period`], [`Moment`]).
///
/// `BranchRevision` reuses the branch entity (`this`) for its own
/// `this`, so its attributes live in the same namespace.
///
/// [`Branch`]: crate::schema::Branch
/// [`BranchRevision`]: crate::schema::BranchRevision
pub mod branch {
    use super::{Attribute, Entity};

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Name(pub String);

    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Origin(pub Entity);

    /// Current revision's tree hash, base58-encoded.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Tree(pub String);

    /// Logical-clock period component of the current revision.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Period(pub u128);

    /// Logical-clock moment component of the current revision.
    #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
    #[domain("dialog.branch")]
    pub struct Moment(pub u128);
}
