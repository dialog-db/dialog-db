use dialog_varsig::Principal;

use super::create::CreateRemote;
use super::load::LoadRemote;
use crate::RemoteAddress;
use crate::repository::memory::Site;
use crate::{RemoteName, Repository};

/// A reference to a named remote within a repository.
///
/// Wraps a `Site` (memory space scoped to `remote/{name}`).
/// Call `.create(address)` or `.load()`.
pub struct RemoteSelector(pub(crate) Site);

impl RemoteSelector {
    /// Name of this remote.
    pub fn name(&self) -> RemoteName {
        self.0.name().into()
    }

    /// Create a new remote with the given address.
    pub fn create(self, address: RemoteAddress) -> CreateRemote {
        CreateRemote::new(self.0, address)
    }

    /// Load an existing remote.
    pub fn load(self) -> LoadRemote {
        LoadRemote::from(self.0)
    }
}

impl<C: Principal> Repository<C> {
    /// Get a remote reference for the given name.
    ///
    /// Call `.create(address)` or `.load()` on the returned reference.
    pub fn remote(&self, name: impl Into<RemoteName>) -> RemoteSelector {
        let name = name.into();
        let space = self.memory().space(&format!("remote/{}", name.as_str()));
        RemoteSelector(Site::from(space))
    }
}
