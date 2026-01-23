pub use super::Replica;
use super::remote::Site;
use super::{PlatformBackend, RemoteSite, RemoteState, ReplicaError};

/// Manages remote repositories for synchronization.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Remotes<Backend: PlatformBackend> {
    /// Adds a new remote repository with the given name and address.
    async fn add_remote(&mut self, remote: RemoteState) -> Result<Site, ReplicaError>;
    /// Loads an existing remote repository by name.
    async fn load_remote(&mut self, site: &Site) -> Result<RemoteState, ReplicaError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Backend: PlatformBackend + 'static> Remotes<Backend> for Replica<Backend> {
    /// Adds a new remote repository with the given name and address.
    async fn add_remote(&mut self, state: RemoteState) -> Result<Site, ReplicaError> {
        let site = state.site.clone();
        RemoteSite::add(
            state,
            self.storage().clone(),
            self.issuer().clone(),
            self.subject().clone(),
        )
        .await?;
        Ok(site)
    }

    async fn load_remote(&mut self, site: &Site) -> Result<RemoteState, ReplicaError> {
        let remote = RemoteSite::load(
            site,
            self.storage().clone(),
            self.issuer().clone(),
            self.subject().clone(),
        )
        .await?;
        remote.state().ok_or_else(|| ReplicaError::RemoteNotFound {
            remote: site.clone(),
        })
    }
}
