//! Temporal errors.

use super::timestamp::{SystemTime, Timestamp};
use thiserror::Error;

/// An error expressing when a time is larger than 2⁵³ seconds past the Unix epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("Time out of JsTime (2⁵³) range: {:?}", tried)]
pub struct OutOfRangeError {
    /// The [`SystemTime`] that is outside of the [`JsTime`] range (2⁵³).
    pub tried: SystemTime,
}

/// An error expressing when a time is larger than 2⁵³ seconds past the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NumberIsNotATimestamp {
    /// The [`Ipld`] number that is outside of the [`JsTime`] range.
    #[error("Cannot convert IPLD number to JsTime (2⁵³) range: {0}")]
    TriedIpldInt(i128),

    /// A [`SystemTime`] is outside of the [`JsTime`] range.
    #[error(transparent)]
    TriedSystemTime(#[from] OutOfRangeError),
}

/// An error expressing when a time is not within the bounds of a UCAN.
///
/// Each variant carries the bound it failed against and the instant it
/// was judged at. A caller answering this to its own clients needs both:
/// "expired" alone cannot say when, so it cannot be reported as anything
/// more useful than a generic refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Error)]
pub enum TimeBoundError {
    /// The UCAN has expired.
    #[error("Expired at {}, checked at {}", expiration.to_unix(), at.to_unix())]
    Expired {
        /// The bound that had already passed.
        expiration: Timestamp,
        /// The instant the check was made at.
        at: Timestamp,
    },

    /// The UCAN is not yet valid, but will be in the future.
    #[error("Not valid before {}, checked at {}", not_before.to_unix(), at.to_unix())]
    NotYetValid {
        /// The bound that has not been reached yet.
        not_before: Timestamp,
        /// The instant the check was made at.
        at: Timestamp,
    },
}

/// The UCAN has expired.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Error)]
#[error("Expired")]
pub struct Expired;
