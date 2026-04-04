use dialog_capability::Did;
use dialog_varsig::Principal;

use super::address::SiteAddress;
use super::create::CreateRemote;
use super::load::LoadRemote;
use crate::RemoteAddress;
use crate::repository::memory::Site;
use crate::{RemoteName, Repository};

/// A reference to a named remote within a repository.
///
/// Wraps a `Site` (memory space scoped to `remote/{name}`) and the
/// repository's default subject DID.
pub struct RemoteSelector {
    pub(crate) site: Site,
    pub(crate) subject: Did,
}

impl RemoteSelector {
    /// Name of this remote.
    pub fn name(&self) -> RemoteName {
        self.site.name().into()
    }

    /// Create a new remote with a site address.
    ///
    /// Uses the repository's own DID as the subject. Call `.subject(did)`
    /// on the returned builder to target a different repository.
    pub fn create(self, address: impl Into<SiteAddress>) -> CreateRemote {
        let remote = RemoteAddress::new(address.into(), self.subject);
        CreateRemote::new(self.site, remote)
    }

    /// Load an existing remote.
    pub fn load(self) -> LoadRemote {
        LoadRemote::from(self.site)
    }
}

impl<C: Principal> Repository<C> {
    /// Get a remote reference for the given name.
    ///
    /// Call `.create(address)` or `.load()` on the returned reference.
    pub fn remote(&self, name: impl Into<RemoteName>) -> RemoteSelector {
        let name = name.into();
        let space = self.memory().space(&format!("remote/{}", name.as_str()));
        RemoteSelector {
            site: Site::from(space),
            subject: self.did(),
        }
    }
}
