//! The version-control identity and clock types: the plain data a revision
//! is named by.
//!
//! These live in this light crate rather than in `dialog-artifacts` because
//! [`Revision`](crate::Revision) is built from them — `dialog-artifacts`
//! depends on this crate, so the types its revision fields need must sit at
//! or below this layer. They carry no dependency on the search tree, the
//! storage stack, or the query engine: an [`Origin`] is a Blake3 hash, an
//! [`Edition`] is a counter, a [`Version`] pairs them, and a [`Context`] is a
//! per-origin watermark map.
//!
//! The tree-facing half of version control (the `RevisionRecord` a revision
//! writes into the artifact tree, and the history key encodings) stays in
//! `dialog-artifacts`, which is where the `Artifact`/`Key`/`Datum` types it
//! is made of live.

mod context;
mod edition;
mod origin;
mod principal;
mod version;

pub use context::*;
pub use edition::*;
pub use origin::*;
pub use principal::*;
pub use version::*;

/// Failures the version-control identity types can raise.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    /// A signature did not verify, or an issuer DID was not a usable
    /// `did:key`.
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// A byte reference was not a valid version or origin encoding.
    #[error("Invalid reference: {0}")]
    InvalidReference(String),

    /// A causal context could not be derived because an ancestor revision
    /// was missing from the local history.
    #[error("Incomplete history: {0}")]
    IncompleteHistory(String),
}
