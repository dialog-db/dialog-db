//! Space: a composed set of providers for a mounted space.
//!
//! Routes capabilities to the appropriate provider field.

use dialog_capability::access::{AuthorizeError, Protocol, Prove, Retain as AccessRetain};
use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::storage::{Location, StorageError};
use dialog_effects::{archive, credential, memory};

use std::fmt::Display;

use crate::resource::Resource;

/// Trait for types that can serve as a mounted space provider.
pub trait SpaceProvider:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + Provider<credential::Load<dialog_credentials::Credential>>
    + Provider<credential::Save<dialog_credentials::Credential>>
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
        + Provider<credential::Load<dialog_credentials::Credential>>
        + Provider<credential::Save<dialog_credentials::Credential>>
        + ConditionalSend
        + ConditionalSync
        + Clone
        + 'static
{
}

/// A composed set of providers for a single mounted space.
#[derive(Clone, dialog_capability::Provider)]
pub struct Space<Archive, Memory, Credential, Certificate> {
    /// Archive provider.
    #[provide(archive::Get, archive::Put)]
    pub archive: Archive,

    /// Memory provider.
    #[provide(memory::Resolve, memory::Publish, memory::Retract)]
    pub memory: Memory,

    /// Credential provider.
    #[provide(credential::Load<dialog_credentials::Credential>, credential::Save<dialog_credentials::Credential>)]
    pub credential: Credential,

    /// Certificate provider manual implementation are used because of the
    /// complex generics
    pub certificate: Certificate,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Archive, Memory, Credential, Certificate, P> Provider<Prove<P>>
    for Space<Archive, Memory, Credential, Certificate>
where
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    Certificate: Provider<Prove<P>> + ConditionalSend + ConditionalSync,
    Archive: ConditionalSend + ConditionalSync,
    Memory: ConditionalSend + ConditionalSync,
    Credential: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Prove<P>>) -> Result<P::Proof, AuthorizeError> {
        input.perform(&self.certificate).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Archive, Memory, Credential, Certificate, P> Provider<AccessRetain<P>>
    for Space<Archive, Memory, Credential, Certificate>
where
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Certificate: Provider<AccessRetain<P>> + ConditionalSend + ConditionalSync,
    Archive: ConditionalSend + ConditionalSync,
    Memory: ConditionalSend + ConditionalSync,
    Credential: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<AccessRetain<P>>) -> Result<(), AuthorizeError> {
        input.perform(&self.certificate).await
    }
}

/// Resource<Location> for Space: each field opens from the same Location.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, M, C, P> Resource<Location> for Space<A, M, C, P>
where
    A: Resource<Location> + ConditionalSend,
    M: Resource<Location> + ConditionalSend,
    C: Resource<Location> + ConditionalSend,
    P: Resource<Location> + ConditionalSend,
    A::Error: Display,
    M::Error: Display,
    C::Error: Display,
    P::Error: Display,
{
    type Error = StorageError;

    async fn open(location: &Location) -> Result<Self, StorageError> {
        Ok(Space {
            archive: A::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            memory: M::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            credential: C::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
            certificate: P::open(location)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?,
        })
    }
}
