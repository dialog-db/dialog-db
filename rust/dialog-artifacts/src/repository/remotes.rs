pub use super::Repository;
use super::remote::Site;
use super::{OperatingAuthority, PlatformBackend, RemoteSite, RemoteState, RepositoryError};
use dialog_common::ConditionalSync;

/// Manages remote sites used for synchronization. Repository (a.k.a Replica)
/// may have zero or more sites configured that can be used to obtain references
/// to remote branches which in turn can be configured as upstream of the local
/// branches.
///
/// Trait is meant to be implemented by `Repository` or other similar abstraction
/// that needs to manage remotes e.g. `SigningAuthority` could potentially implement
/// `Remotes` to have remotes configured level higher.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Remotes<Backend: PlatformBackend> {
    /// Adds a new remote site with the given name and credentials.
    async fn add_remote(&mut self, remote: RemoteState) -> Result<Site, RepositoryError>;
    /// Loads already added remote site by name.
    async fn load_remote(&mut self, site: &Site) -> Result<RemoteState, RepositoryError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Backend: PlatformBackend + 'static, A: OperatingAuthority + ConditionalSync + 'static>
    Remotes<Backend> for Repository<Backend, A>
{
    async fn add_remote(&mut self, state: RemoteState) -> Result<Site, RepositoryError> {
        let site = state.site.clone();
        RemoteSite::add(state, self.issuer().clone(), self.storage().clone()).await?;
        Ok(site)
    }

    async fn load_remote(&mut self, site: &Site) -> Result<RemoteState, RepositoryError> {
        let remote = RemoteSite::load(site, self.issuer().clone(), self.storage().clone()).await?;
        remote
            .state()
            .ok_or_else(|| RepositoryError::RemoteNotFound {
                remote: site.clone(),
            })
    }
}
