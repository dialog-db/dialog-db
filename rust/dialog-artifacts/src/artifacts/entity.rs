use std::fmt::Display;

use crate::{make_reference, make_seed, reference_type};
use dialog_storage::Blake3Hash;

/// An [`Entity`] is the subject part of a semantic triple. Internally, an
/// [`Entity`] is represented as a unique 32-byte hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(Blake3Hash);

impl Default for Entity {
    fn default() -> Self {
        Self::new()
    }
}

reference_type!(Entity);

impl Entity {
    /// Generate a new, unique [`Entity`].
    pub fn new() -> Self {
        Self(make_reference(make_seed()))
    }
}

impl Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{}...",
            self.0
                .iter()
                .take(6)
                .map(|byte| format!("{:X}", byte))
                .collect::<Vec<String>>()
                .concat()
        )
    }
}
