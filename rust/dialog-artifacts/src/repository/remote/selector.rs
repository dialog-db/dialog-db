use crate::repository::cell::Cell;
use crate::{CreateSite, LoadSite, RemoteAddress, Repository, SiteName};
use dialog_varsig::Principal;

/// A reference to a named remote site within a repository.
///
/// Call `.add(address)` to create or `.load()` to load an existing remote.
pub struct SiteSelector(Cell<RemoteAddress>);

impl SiteSelector {
    /// Name of the remote site.
    pub fn name(&self) -> SiteName {
        self.0.name().into()
    }

    /// Add a new remote with the given address.
    pub fn create(self, address: RemoteAddress) -> CreateSite {
        CreateSite::new(self.0, address)
    }

    /// Load an existing remote.
    pub fn load(self) -> LoadSite {
        LoadSite::from(self.0)
    }
}

impl<C: Principal> Repository<C> {
    /// Get a site reference for the given remote name.
    ///
    /// Call `.add(address)` or `.load()` on the returned reference.
    pub fn site(&self, name: impl Into<SiteName>) -> SiteSelector {
        let cell = self.memory().space("site").cell(name.into().as_str());
        SiteSelector(cell)
    }
}

impl From<Cell<RemoteAddress>> for SiteSelector {
    fn from(cell: Cell<RemoteAddress>) -> Self {
        Self(cell)
    }
}
