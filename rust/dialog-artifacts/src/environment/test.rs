//! Test utilities — in-memory volatile environment via builder.

use dialog_storage::provider::Volatile;

use super::builder::Builder;

impl Builder<Volatile> {
    /// Create a builder backed by in-memory volatile storage.
    pub fn volatile() -> Self {
        Builder::new(Volatile::new())
    }
}
