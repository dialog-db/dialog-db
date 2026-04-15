use dialog_capability::{Capability, Policy};
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
pub struct RemoteReference(Capability<fx::Space>);

impl From<Capability<fx::Space>> for RemoteReference {
    fn from(space: Capability<fx::Space>) -> Self {
        Self(space)
    }
}

impl RemoteReference {
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

    /// The underlying space capability.
    pub fn capability(&self) -> Capability<fx::Space> {
        self.0.clone()
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
