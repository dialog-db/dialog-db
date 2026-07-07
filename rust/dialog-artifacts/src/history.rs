//! Version control primitives grounded in the revision DAG.
//!
//! This module implements the causal encoding described in
//! `notes/version-control.md`. Instead of deriving causal position from a
//! logical counter that is local to a repository's synchronization history
//! (see `notes/divergence-clock.md`), causal position is derived from the
//! structure of the revision DAG itself:
//!
//! - [`Edition`] is the count of revisions in the causal chain leading to a
//!   revision — a Lamport timestamp, comparable across repository boundaries.
//! - [`Origin`] is a repository-scoped identity for an issuer, derived as
//!   `Blake3(issuer + subject)`.
//! - [`Version`] pairs the two into a compact revision identifier that sorts
//!   naturally by causal depth.
//! - [`Cause`] is a set of [`Version`]s identifying the prior claims (or
//!   parent revisions) superseded by a claim (or revision).
//! - [`Revision`] is a signed, content-addressed record of a commit.
//! - [`causality`] implements the tiered conflict detection over a
//!   [`History`] index, determining whether two claims on the same
//!   `(entity, attribute)` are causally ordered or concurrent.

mod edition;
pub use edition::*;

mod origin;
pub use origin::*;

mod version;
pub use version::*;

mod cause;
pub use cause::*;

mod principal;
pub use principal::*;

mod revision;
pub use revision::*;

mod claim;
pub use claim::*;

#[cfg(test)]
mod key;
#[cfg(test)]
pub use key::*;

mod causality;
pub use causality::*;

// An in-memory `History` used by the unit tests in this module. The durable
// implementation is [`TreeHistory`], which reads the history region of the
// artifact tree itself (see [`crate::history_key`] for the key layout).
#[cfg(test)]
mod memory;
#[cfg(test)]
pub use memory::*;

mod record;
pub use record::*;

mod query;
pub use query::*;

mod skip;
pub use skip::*;

mod log;
pub use log::*;

mod revision_record;
pub use revision_record::*;

/// The attribute under which a repository's revision lineage claims are
/// recorded. The claim's entity is the repository DID and its value is the
/// content-addressed entity of the [`Revision`].
pub const REVISION_ATTRIBUTE: &str = "dialog.db/revision";

#[cfg(test)]
mod tests;
