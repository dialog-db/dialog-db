//! Space: a composed set of providers created from a Location.
//!
//! A Space is the product of mounting a Location with a set of factories.
//! It routes capabilities to the appropriate provider.

use dialog_capability::access::{AuthorizeError, Claim, Protocol, Save as AccessSave};
use dialog_common::{ConditionalSend, ConditionalSync};

use super::location::Location;

/// Factory that creates a provider from a location.
///
/// Each backend type (FileStore, IndexedDb, Volatile) implements this
/// with its own address resolution logic.
pub trait Factory: ConditionalSend + ConditionalSync {
    /// The provider this factory creates.
    type Provider: ConditionalSend + ConditionalSync;

    /// Create a provider for the given location.
    fn create(&self, location: &Location) -> Self::Provider;
}

/// A composed set of providers for a single mounted location.
///
/// Routes capabilities to the appropriate provider field via
/// `#[derive(Provider)]`. Permit routing for `Claim<P>`/`Save<P>` is
/// handled by manual impls since they're generic over `Protocol`.
#[derive(dialog_capability::Provider)]
pub struct Space<Archive, Memory, Cred, Permit> {
    // TODO: Split archive into separate Index and Blob providers
    // (archive::Index::Get/Put and archive::Blob::Get/Put)
    /// Archive operations (content-addressed index and blob storage).
    #[provide(dialog_effects::archive::Get, dialog_effects::archive::Put)]
    pub archive: Archive,

    /// Memory cell operations.
    #[provide(
        dialog_effects::memory::Resolve,
        dialog_effects::memory::Publish,
        dialog_effects::memory::Retract
    )]
    pub memory: Memory,

    /// Credential load/save.
    #[provide(dialog_effects::credential::Load, dialog_effects::credential::Save)]
    pub credential: Cred,

    /// Delegation proof storage.
    pub permit: Permit,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Archive, Memory, Cred, Permit, P> dialog_capability::Provider<Claim<P>>
    for Space<Archive, Memory, Cred, Permit>
where
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Proof: Clone + ConditionalSend + ConditionalSync,
    P::ProofChain: ConditionalSend,
    Permit: dialog_capability::Provider<Claim<P>> + ConditionalSend + ConditionalSync,
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
impl<Archive, Memory, Cred, Permit, P> dialog_capability::Provider<AccessSave<P>>
    for Space<Archive, Memory, Cred, Permit>
where
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Permit: dialog_capability::Provider<AccessSave<P>> + ConditionalSend + ConditionalSync,
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
