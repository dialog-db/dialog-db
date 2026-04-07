//! MountedSpace: a composed set of providers for a mounted space.
//!
//! Routes capabilities to the appropriate provider field.

use dialog_capability::Provider;
use dialog_capability::access::{AuthorizeError, Claim, Protocol, Save as AccessSave};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::{archive, credential, memory};

/// Trait for types that can serve as a mounted space provider.
pub trait SpaceProvider:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + Provider<credential::Load>
    + Provider<credential::Save>
    + ConditionalSend
    + ConditionalSync
    + Clone
    + 'static
{
}

impl<T> SpaceProvider for T where
    T: Provider<archive::Get>
        + Provider<archive::Put>
        + Provider<memory::Resolve>
        + Provider<memory::Publish>
        + Provider<memory::Retract>
        + Provider<credential::Load>
        + Provider<credential::Save>
        + ConditionalSend
        + ConditionalSync
        + Clone
        + 'static
{
}

/// A composed set of providers for a single mounted space.
#[derive(Clone, dialog_capability::Provider)]
pub struct MountedSpace<Archive, Memory, Cred, Permit> {
    /// Archive provider. TODO: Split into separate Index and Blob providers.
    #[provide(dialog_effects::archive::Get, dialog_effects::archive::Put)]
    pub archive: Archive,

    /// Memory provider.
    #[provide(
        dialog_effects::memory::Resolve,
        dialog_effects::memory::Publish,
        dialog_effects::memory::Retract
    )]
    pub memory: Memory,

    /// Credential provider.
    #[provide(dialog_effects::credential::Load, dialog_effects::credential::Save)]
    pub credential: Cred,

    /// Permit (delegation proof) provider.
    pub permit: Permit,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Archive, Memory, Cred, Permit, P> Provider<Claim<P>>
    for MountedSpace<Archive, Memory, Cred, Permit>
where
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Proof: Clone + ConditionalSend + ConditionalSync,
    P::ProofChain: ConditionalSend,
    Permit: Provider<Claim<P>> + ConditionalSend + ConditionalSync,
    Archive: ConditionalSend + ConditionalSync,
    Memory: ConditionalSend + ConditionalSync,
    Cred: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<Claim<P>>,
    ) -> Result<P::ProofChain, AuthorizeError> {
        self.permit.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Archive, Memory, Cred, Permit, P> Provider<AccessSave<P>>
    for MountedSpace<Archive, Memory, Cred, Permit>
where
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Permit: Provider<AccessSave<P>> + ConditionalSend + ConditionalSync,
    Archive: ConditionalSend + ConditionalSync,
    Memory: ConditionalSend + ConditionalSync,
    Cred: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: dialog_capability::Capability<AccessSave<P>>,
    ) -> Result<(), AuthorizeError> {
        self.permit.execute(input).await
    }
}

/// Resource<Location> for MountedSpace: each field opens from the same Location.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, M, C, P> crate::resource::Resource<dialog_effects::storage::Location>
    for MountedSpace<A, M, C, P>
where
    A: crate::resource::Resource<dialog_effects::storage::Location> + ConditionalSend,
    M: crate::resource::Resource<dialog_effects::storage::Location> + ConditionalSend,
    C: crate::resource::Resource<dialog_effects::storage::Location> + ConditionalSend,
    P: crate::resource::Resource<dialog_effects::storage::Location> + ConditionalSend,
    A::Error: std::fmt::Display,
    M::Error: std::fmt::Display,
    C::Error: std::fmt::Display,
    P::Error: std::fmt::Display,
{
    type Error = dialog_effects::storage::StorageError;

    async fn open(
        location: &dialog_effects::storage::Location,
    ) -> Result<Self, dialog_effects::storage::StorageError> {
        use dialog_effects::storage::StorageError;
        Ok(MountedSpace {
            archive: A::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            memory: M::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            credential: C::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            permit: P::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
        })
    }
}
