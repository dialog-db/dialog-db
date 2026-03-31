//! Remote repository — a loaded remote with address and branch navigation.

use dialog_capability::{Capability, Did, Policy};
use dialog_effects::memory as fx;

use super::state::RemoteName;
use crate::RemoteAddress;
use crate::repository::cell::{Cell, Retain};
use crate::repository::revision::Revision;

/// A loaded remote repository.
///
/// Holds the retained address and a memory space capability scoped to
/// `remote/{name}`, used for branch revision cells.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    address: Retain<RemoteAddress>,
    capability: Capability<fx::Space>,
}

impl RemoteRepository {
    /// Construct from a retained address cell and its site space capability.
    pub(crate) fn new(address: Retain<RemoteAddress>, capability: Capability<fx::Space>) -> Self {
        Self {
            address,
            capability,
        }
    }

    /// Local name for this remote.
    pub fn name(&self) -> RemoteName {
        fx::Space::of(&self.capability)
            .space
            .strip_prefix("remote/")
            .unwrap_or("")
            .into()
    }

    /// The subject DID of the remote repository.
    pub fn did(&self) -> Did {
        self.address.get().subject.clone()
    }

    /// The full remote address (site + subject).
    pub fn address(&self) -> RemoteAddress {
        self.address.get().clone()
    }

    /// A cell for a branch revision at this remote.
    ///
    /// Path: `remote/{name}/branch/{branch}/revision`
    pub fn branch_revision(&self, branch: &str) -> Cell<Revision> {
        let cap = self
            .capability
            .clone()
            .attenuate(fx::Cell::new(format!("branch/{branch}/revision")));
        Cell::from_capability(cap)
    }
}
