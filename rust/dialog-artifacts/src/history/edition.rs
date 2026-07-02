use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

/// Count of revisions in the causal chain leading to a revision.
///
/// An [`Edition`] is isomorphic to a Lamport timestamp: a revision's edition
/// is `max(edition of every version in its cause) + 1`, and a genesis
/// revision (with an empty cause) has edition `0`. This gives editions a
/// useful property: a higher edition has seen at least as much causal history
/// as any lower one, regardless of which repository it came from.
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct Edition(u64);

/// The byte width of an [`Edition`] in key encodings
pub const EDITION_LENGTH: usize = 8;

impl Edition {
    /// The edition of a genesis revision (one with an empty cause)
    pub const GENESIS: Edition = Edition(0);

    /// Create an [`Edition`] from its numeric value
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// The numeric value of this [`Edition`]
    pub fn value(&self) -> u64 {
        self.0
    }

    /// The [`Edition`] that immediately follows this one
    pub fn successor(&self) -> Edition {
        Edition(self.0.saturating_add(1))
    }

    /// The big-endian byte representation of this [`Edition`], suitable for
    /// use as a key component (lexicographic order matches numeric order)
    pub fn key_bytes(&self) -> [u8; EDITION_LENGTH] {
        self.0.to_be_bytes()
    }

    /// Reconstruct an [`Edition`] from its key byte representation
    pub fn from_key_bytes(bytes: [u8; EDITION_LENGTH]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl From<u64> for Edition {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Edition> for u64 {
    fn from(value: Edition) -> Self {
        value.0
    }
}

impl Display for Edition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
