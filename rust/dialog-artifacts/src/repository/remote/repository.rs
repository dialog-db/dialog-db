//! Remote repository reference.

use dialog_capability::Did;

use super::{Credentials, PlatformBackend, PlatformStorage, RemoteBranch, RemoteCredentials, Site};

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
    issuer: Credentials,
    /// Credentials for connecting to the remote.
    credentials: Option<RemoteCredentials>,
}

impl<Backend: PlatformBackend + 'static> RemoteRepository<Backend> {
    /// Create a new remote repository reference.
    pub(super) fn new(
        site_name: Site,
        subject: Did,
        storage: PlatformStorage<Backend>,
        issuer: Credentials,
        credentials: Option<RemoteCredentials>,
    ) -> Self {
        Self {
            subject,
            site_name,
            storage,
            issuer,
            credentials,
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

    /// Reference a branch within this remote repository.
    pub fn branch(&self, name: impl Into<String>) -> RemoteBranch<Backend> {
        RemoteBranch::reference(
            name.into(),
            self.site_name.clone(),
            self.subject.clone(),
            self.storage.clone(),
            self.issuer.clone(),
            self.credentials.clone(),
        )
    }
}
