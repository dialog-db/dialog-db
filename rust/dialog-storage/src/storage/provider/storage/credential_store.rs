//! The credential store: where signing keys are kept, apart from the
//! storage spaces live in.
//!
//! A [`Storage`](super::Storage) keeps no signing key: it holds a space's
//! data and its public identity, and whoever holds it is given nothing to
//! sign with. Keys are opened from a [`CredentialStore`] instead, by
//! whoever builds a peer, and the peer is handed the key it acts with.
//!
//! The store is laid out like a storage, one space per key, but under
//! names of its own (`{name}.credentials`), so a key and the space of the
//! same name never share a directory or a database.

use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Policy, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::storage::LocationExt as _;
use dialog_effects::{credential, storage};

use super::loader::Loader;
use super::router::Router;
use crate::provider::SpaceProvider;
use crate::resource::{Pool, Resource};

/// A store of signing keys, opened by name.
///
/// [`OpenCredential`](https://docs.rs/dialog-identity) loads, creates, or
/// opens a key in it. Like a [`Storage`](super::Storage), it belongs to a
/// system: the application opens keys from it acting as that system and
/// hands a peer the key it acts with; no peer holds the store. Cloning
/// yields a second handle onto the same keys.
pub struct CredentialStore<S: Clone> {
    loader: Loader<S>,
    router: Router<S>,
    /// The system this store belongs to. Only its DID is kept.
    system: Option<Did>,
}

impl<S: Clone> Clone for CredentialStore<S> {
    fn clone(&self) -> Self {
        Self {
            loader: self.loader.clone(),
            router: self.router.clone(),
            system: self.system.clone(),
        }
    }
}

impl<S: Clone> CredentialStore<S> {
    /// A store with nothing opened yet.
    pub fn new() -> Self {
        let spaces = Arc::new(Pool::new());
        Self {
            loader: Loader::new(Arc::clone(&spaces)),
            router: Router::new(spaces),
            system: None,
        }
    }

    /// This store, owned by `system`.
    pub fn owned_by(mut self, system: Did) -> Self {
        self.system = Some(system);
        self
    }

    /// The system this store belongs to, when it has one.
    pub fn system(&self) -> Option<&Did> {
        self.system.as_ref()
    }
}

impl<S: Clone> Default for CredentialStore<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Where the key named by `location` is kept: beside the space of the
/// same name, never in it.
fn keyed(location: &storage::Location) -> storage::Location {
    storage::Location::new(
        location.directory.clone(),
        format!("{}.credentials", location.name),
    )
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Load> for CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<storage::Load>,
    ) -> Result<Credential, storage::StorageError> {
        let location = keyed(storage::Location::of(&input));
        Subject::from(input.subject().clone())
            .attenuate(storage::Storage)
            .attenuate(location)
            .load()
            .perform(&self.loader)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Create> for CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<storage::Create>,
    ) -> Result<Credential, storage::StorageError> {
        let location = keyed(storage::Location::of(&input));
        let credential = storage::Create::of(&input).credential.clone();
        Subject::from(input.subject().clone())
            .attenuate(storage::Storage)
            .attenuate(location)
            .create(credential)
            .perform(&self.loader)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<credential::Load<Credential>> for CredentialStore<S>
where
    S: Clone + ConditionalSync,
    Router<S>: Provider<credential::Load<Credential>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<Credential>>,
    ) -> Result<Credential, credential::CredentialError> {
        input.perform(&self.router).await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_credential;
    use crate::provider::storage::{Storage, VolatileSpace};
    use dialog_effects::storage::Storage as StorageFx;
    use dialog_varsig::Principal as _;

    /// A key is kept in the credential store, and loads back whole: the
    /// signer, not its verifier.
    #[dialog_common::test]
    async fn it_keeps_a_signing_key() {
        let credentials = CredentialStore::<VolatileSpace>::new();
        let key = test_credential().await;

        StorageFx::profile("alice")
            .create(key.clone())
            .perform(&credentials)
            .await
            .unwrap();
        let loaded = StorageFx::profile("alice")
            .load()
            .perform(&credentials)
            .await
            .unwrap();
        assert!(matches!(loaded, Credential::Signer(_)));
        assert_eq!(loaded.did(), key.did());
    }

    /// A key and the space of the same name are kept apart: the space
    /// holds its verifier, the credential store the key.
    #[dialog_common::test]
    async fn it_keeps_a_key_apart_from_the_space_of_its_name() {
        let credentials = CredentialStore::<VolatileSpace>::new();
        let storage = Storage::<VolatileSpace>::volatile();
        let key = test_credential().await;

        StorageFx::profile("alice")
            .create(key.clone())
            .perform(&credentials)
            .await
            .unwrap();
        let space = StorageFx::profile("alice")
            .create(key.clone())
            .perform(&storage)
            .await
            .unwrap();

        assert!(matches!(space, Credential::Verifier(_)));
        let kept = StorageFx::profile("alice")
            .load()
            .perform(&credentials)
            .await
            .unwrap();
        assert!(matches!(kept, Credential::Signer(_)));
    }
}
