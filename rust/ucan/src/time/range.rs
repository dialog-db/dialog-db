//! Time range for UCAN validity windows.

use super::timestamp::Timestamp;
use std::{
    fmt,
    ops::{Bound, RangeBounds},
};

/// A time range representing the intersection of all validity windows
/// in a UCAN delegation chain.
///
/// `not_before` is the latest `nbf` across all delegations (lower bound).
/// `expiration` is the earliest `exp` across all delegations and the invocation (upper bound).
///
/// Uses [`Bound::Unbounded`] for open ends and [`Bound::Included`] for set bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// Earliest time this range is valid (latest `nbf` in the chain).
    pub not_before: Bound<Timestamp>,

    /// Latest time this range is valid (earliest `exp` in the chain).
    pub expiration: Bound<Timestamp>,
}

impl TimeRange {
    /// An unbounded time range (no constraints).
    #[must_use]
    pub const fn unbounded() -> Self {
        Self {
            not_before: Bound::Unbounded,
            expiration: Bound::Unbounded,
        }
    }

    /// Creates a time range from optional `not_before` and `expiration` bounds.
    #[must_use]
    pub const fn new(not_before: Option<Timestamp>, expiration: Option<Timestamp>) -> Self {
        Self {
            not_before: match not_before {
                Some(t) => Bound::Included(t),
                None => Bound::Unbounded,
            },
            expiration: match expiration {
                Some(t) => Bound::Included(t),
                None => Bound::Unbounded,
            },
        }
    }

    /// Returns `true` if this range is non-empty (a valid time exists within it).
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match (self.not_before, self.expiration) {
            (Bound::Included(nbf), Bound::Included(exp)) => nbf <= exp,
            _ => true,
        }
    }

    /// Compute the intersection of two time ranges.
    ///
    /// The resulting `not_before` is the later of the two lower bounds,
    /// and the resulting `expiration` is the earlier of the two upper bounds.
    #[must_use]
    pub fn intersect(self, other: Self) -> Self {
        let not_before = match (self.not_before, other.not_before) {
            (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b)),
            (Bound::Included(a), Bound::Unbounded) => Bound::Included(a),
            (Bound::Unbounded, Bound::Included(b)) => Bound::Included(b),
            (Bound::Unbounded, Bound::Unbounded) => Bound::Unbounded,
            // Excluded bounds are not used in UCAN, but handle gracefully
            (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b)),
            (Bound::Excluded(a), Bound::Included(b)) if a >= b => Bound::Excluded(a),
            (Bound::Included(a), Bound::Excluded(b)) if b >= a => Bound::Excluded(b),
            (Bound::Excluded(a), _) => Bound::Excluded(a),
            (_, Bound::Excluded(b)) => Bound::Excluded(b),
        };
        let expiration = match (self.expiration, other.expiration) {
            (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.min(b)),
            (Bound::Included(a), Bound::Unbounded) => Bound::Included(a),
            (Bound::Unbounded, Bound::Included(b)) => Bound::Included(b),
            (Bound::Unbounded, Bound::Unbounded) => Bound::Unbounded,
            (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.min(b)),
            (Bound::Excluded(a), Bound::Included(b)) if a <= b => Bound::Excluded(a),
            (Bound::Included(a), Bound::Excluded(b)) if b <= a => Bound::Excluded(b),
            (Bound::Excluded(a), _) => Bound::Excluded(a),
            (_, Bound::Excluded(b)) => Bound::Excluded(b),
        };
        Self {
            not_before,
            expiration,
        }
    }
}

impl RangeBounds<Timestamp> for TimeRange {
    fn start_bound(&self) -> Bound<&Timestamp> {
        match &self.not_before {
            Bound::Included(t) => Bound::Included(t),
            Bound::Excluded(t) => Bound::Excluded(t),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Timestamp> {
        match &self.expiration {
            Bound::Included(t) => Bound::Included(t),
            Bound::Excluded(t) => Bound::Excluded(t),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.not_before {
            Bound::Included(nbf) | Bound::Excluded(nbf) => write!(f, "{}", nbf.to_unix())?,
            Bound::Unbounded => {}
        }
        write!(f, "..")?;
        match self.expiration {
            Bound::Included(exp) => write!(f, "={}", exp.to_unix()),
            Bound::Excluded(exp) => write!(f, "{}", exp.to_unix()),
            Bound::Unbounded => Ok(()),
        }
    }
}
