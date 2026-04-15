use dialog_capability::{Capability, Did, Policy};
use dialog_effects::memory as fx;
use dialog_effects::memory::prelude::SpaceExt;

use super::address::SiteAddress;
use super::create::CreateRemote;
use super::load::LoadRemote;
use crate::RemoteAddress;
use crate::RemoteName;
use crate::repository::cell::Cell;

/// A reference to a named remote within a repository.
///
/// Wraps a `Capability<fx::Space>` scoped to `remote/{name}`.
/// The subject DID is derived from the capability chain.
#[derive(Debug, Clone)]
pub struct RemoteReference(Capability<fx::Space>);

impl From<Capability<fx::Space>> for RemoteReference {
    fn from(space: Capability<fx::Space>) -> Self {
        Self(space)
    }
}

impl From<RemoteReference> for Capability<fx::Space> {
    fn from(reference: RemoteReference) -> Self {
        reference.0
    }
}

impl RemoteReference {
    /// The subject DID this remote belongs to.
    pub fn subject(&self) -> &Did {
        self.0.subject()
    }

    /// Name of this remote, extracted from the space path.
    pub fn name(&self) -> RemoteName {
        fx::Space::of(&self.0)
            .space
            .strip_prefix("remote/")
            .unwrap_or("")
            .into()
    }

    /// Cell for the remote address configuration.
    pub fn address(&self) -> Cell<RemoteAddress> {
        Cell::from_capability(self.0.clone().cell("address"))
    }

    /// Create a typed cell within this remote's space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.0.clone().cell(cell_name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, cell_name: impl Into<String>) -> Capability<fx::Cell> {
        self.0.clone().cell(cell_name)
    }

    /// Create a new remote with a site address.
    ///
    /// Uses the repository's own DID as the subject. Call `.subject(did)`
    /// on the returned builder to target a different repository.
    pub fn create(self, address: impl Into<SiteAddress>) -> CreateRemote {
        let subject = self.0.subject().clone();
        let remote = RemoteAddress::new(address.into(), subject);
        CreateRemote::new(self, remote)
    }

    /// Load an existing remote.
    pub fn load(self) -> LoadRemote {
        LoadRemote::new(self)
    }
}
