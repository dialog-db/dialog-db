use dialog_capability::{Capability, Policy};
use dialog_effects::memory as fx;

use crate::RemoteAddress;
use crate::repository::cell::Cell;

use super::Space;

/// Remote-scoped memory namespace (`remote/{name}`).
///
/// Provides access to remote address configuration and branch cells.
#[derive(Debug, Clone)]
pub struct RemoteMemory(Capability<fx::Space>);

impl From<Space> for RemoteMemory {
    fn from(space: Space) -> Self {
        Self(space.0)
    }
}

impl RemoteMemory {
    /// Remote name, derived from the space path.
    pub fn name(&self) -> &str {
        fx::Space::of(&self.0)
            .space
            .strip_prefix("remote/")
            .unwrap_or("")
    }

    /// The underlying space capability.
    pub fn capability(&self) -> Capability<fx::Space> {
        self.0.clone()
    }

    /// Cell for the remote address configuration.
    pub fn address(&self) -> Cell<RemoteAddress> {
        Cell::from_capability(self.cell_capability("address"))
    }

    /// Create a [`Cell`] within this remote's space.
    pub fn cell<T>(&self, name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, name: impl Into<String>) -> Capability<fx::Cell> {
        self.0.clone().attenuate(fx::Cell::new(name))
    }
}
