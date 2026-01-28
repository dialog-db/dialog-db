//! Remote repository reference.

use dialog_capability::Did;

use super::{Operator, PlatformBackend, PlatformStorage, RemoteBranch, RemoteState, Site};

/// A reference to a repository on a remote site.
///
/// This is a builder step for accessing remote branches.
#[derive(Clone)]
pub struct RemoteRepository<Backend: PlatformBackend> {
    /// The subject DID identifying the repository owner.
    subject: Did,
    /// The remote site name.
    site_name: Site,
    /// Storage for persistence (cloned, cheap).
    storage: PlatformStorage<Backend>,
    /// Issuer for signing requests.
    issuer: Operator,
    /// The remote state (credentials).
    state: Option<RemoteState>,
}

impl<Backend: PlatformBackend> RemoteRepository<Backend> {
    /// Create a new remote repository reference.
    pub(super) fn new(
        site_name: Site,
        subject: Did,
        storage: PlatformStorage<Backend>,
        issuer: Operator,
        state: Option<RemoteState>,
    ) -> Self {
        Self {
            subject,
            site_name,
            storage,
            issuer,
            state,
        }
    }

    /// Returns the subject DID identifying the repository owner.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Returns the remote site name.
    pub fn site_name(&self) -> &Site {
        &self.site_name
    }
}

impl<Backend: PlatformBackend + 'static> RemoteRepository<Backend> {
    /// Reference a branch within this remote repository.
    pub fn branch(&self, name: impl Into<String>) -> RemoteBranch<Backend> {
        RemoteBranch::reference(
            name.into(),
            self.site_name.clone(),
            self.subject.clone(),
            self.storage.clone(),
            self.issuer.clone(),
            self.state.clone(),
        )
    }
}
