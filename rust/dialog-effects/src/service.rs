//! Why a request was not carried out.

use thiserror::Error;

/// A request that could not be carried out for a reason that is not an
/// access decision.
///
/// Authorization failures are *not* here: they are
/// [`AuthorizeError`](dialog_capability::access::AuthorizeError), which
/// already names them (revoked, expired, audience mismatch, unproven
/// subject, and the rest), and effect errors carry that type directly
/// rather than restating it.
///
/// Version conflicts are not here either: they are
/// [`MemoryError::VersionMismatch`](crate::memory::MemoryError), which
/// carries the versions themselves and is what every caller already
/// matches on.
///
/// What is left over is the handful of ways a request fails when
/// authority was never in question and no state moved. Deliberately not
/// a status code or a response: a caller holding one of these has no way
/// to learn what carried the request, because that is never what it
/// needs to decide.
#[derive(Clone, Debug, Error, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum Rejection {
    /// Temporarily unable to serve. Retryable as-is.
    #[error("Temporarily unable to serve the request: {reason}")]
    Unavailable {
        /// Why, as far as the responder would say.
        reason: String,
    },

    /// Not carried out, for a reason this version does not recognize.
    ///
    /// The honest variant. Folding an unrecognized failure into a named
    /// one would claim knowledge we do not have, and would silently
    /// change meaning the day the responder starts saying something new.
    /// `detail` is kept for logs; matching on it is a mistake, because
    /// what lands here is exactly what has no agreed meaning.
    #[error("The request was not carried out: {detail}")]
    Unclassified {
        /// Whatever came back, verbatim and bounded.
        detail: String,
    },
}

impl Rejection {
    /// Whether retrying the identical request could succeed.
    ///
    /// True only for [`Rejection::Unavailable`]. A conflict needs the
    /// request re-formed against current state, and an unrecognized
    /// failure is not a known-transient one.
    pub fn is_transient(&self) -> bool {
        matches!(self, Rejection::Unavailable { .. })
    }
}
