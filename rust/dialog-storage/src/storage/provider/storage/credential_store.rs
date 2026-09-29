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
use dialog_capability::{Capability, Did, Policy, Provider, Subject, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::LocationExt as _;
use dialog_effects::{credential, storage};
use dialog_varsig::Principal as _;

use super::Storage;

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

/// What a key's name ends with where the store keeps it, so a storage
/// tells a key apart from a space and refuses to mount one as the other.
pub(super) const SUFFIX: &str = ".credentials";

/// Where the key named by `location` is kept: beside the space of the
/// same name, never in it.
fn keyed(location: &storage::Location) -> storage::Location {
    storage::Location::new(
        location.directory.clone(),
        format!("{}{SUFFIX}", location.name),
    )
}

impl<S> CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend + ConditionalSync,
    S::Error: Display,
    Router<S>: Provider<credential::Save<Credential>>,
    Self: ConditionalSend + ConditionalSync,
{
    /// Move the signing key the space at `location` in `storage` kept
    /// from before the credential store into this store, leaving the
    /// space its verifier. Idempotent: a space that holds only a verifier
    /// is left alone, and the key the store already has is returned.
    ///
    /// The one way a key leaves a space. It is not an effect any peer
    /// performs; the application runs it once, as the system, when it
    /// brings a storage from before the credential store up to date.
    pub async fn adopt_from(
        &self,
        storage: &Storage<S>,
        location: &storage::Location,
    ) -> Result<Credential, storage::StorageError> {
        let at = Subject::from(did!("local:storage"))
            .attenuate(storage::Storage)
            .attenuate(location.clone());
        // The loader itself, not the storage: the storage hands a space
        // over as its verifier, and this is where the key is taken out.
        let held = at.clone().load().perform(&storage.loader).await?;
        let Credential::Signer(signer) = &held else {
            return Subject::from(did!("local:storage"))
                .attenuate(storage::Storage)
                .attenuate(keyed(location))
                .load()
                .perform(&self.loader)
                .await;
        };
        let kept = Subject::from(did!("local:storage"))
            .attenuate(storage::Storage)
            .attenuate(keyed(location))
            .create(held.clone())
            .perform(&self.loader)
            .await;
        let kept = match kept {
            Ok(kept) => kept,
            Err(storage::StorageError::AlreadyExists(_)) => {
                Subject::from(did!("local:storage"))
                    .attenuate(storage::Storage)
                    .attenuate(keyed(location))
                    .load()
                    .perform(&self.loader)
                    .await?
            }
            Err(error) => return Err(error),
        };
        if kept.did() != held.did() {
            return Err(storage::StorageError::Storage(format!(
                "the store keeps {} under {}, not the space's key {}",
                kept.did(),
                location.name,
                held.did()
            )));
        }
        // Only once the store has the key does the space give it up.
        Subject::from(held.did())
            .credential()
            .key(credential::SELF)
            .save(Credential::from(signer.signer().verifier()))
            .perform(&storage.router)
            .await
            .map_err(|error| storage::StorageError::Storage(error.to_string()))?;
        Ok(kept)
    }
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

/// A key retracted from the store is gone: the store held its only copy.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<credential::Retract<Credential>> for CredentialStore<S>
where
    S: Clone + ConditionalSync,
    Router<S>: Provider<credential::Retract<Credential>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Retract<Credential>>,
    ) -> Result<(), credential::CredentialError> {
        let subject = input.subject().clone();
        let own = credential::Key::of(&input).address == credential::SELF;
        input.perform(&self.router).await?;
        // A space whose own key is gone names nothing: its location is
        // free for the next key created under the same name.
        if own {
            self.loader.unmount(&subject);
        }
        Ok(())
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

    /// A space from before the credential store holds its signing key.
    /// Adopting it moves the key into the store and leaves the space its
    /// verifier, and adopting again finds the key already there.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_adopts_the_key_a_legacy_space_kept() {
        use crate::provider::FileSystem;
        use crate::provider::storage::NativeSpace;
        use crate::resource::Resource as _;
        use dialog_effects::storage::{Directory, Location};

        let root = tempfile::tempdir().unwrap();
        let base = Directory::At(root.path().to_string_lossy().into_owned());
        let location = Location::new(base, "home");
        let key = test_credential().await;
        key.did()
            .credential()
            .key(credential::SELF)
            .save(key.clone())
            .perform(&FileSystem::open(&location).await.unwrap())
            .await
            .unwrap();

        let storage = Storage::<NativeSpace>::default();
        let credentials = CredentialStore::<NativeSpace>::default();
        let adopted = credentials.adopt_from(&storage, &location).await.unwrap();
        assert!(matches!(adopted, Credential::Signer(_)));
        assert_eq!(adopted.did(), key.did());

        let again = credentials.adopt_from(&storage, &location).await.unwrap();
        assert!(matches!(again, Credential::Signer(_)));
        assert_eq!(again.did(), key.did());

        let space = Subject::from(did!("local:storage"))
            .attenuate(storage::Storage)
            .attenuate(location.clone())
            .load()
            .perform(&storage)
            .await
            .unwrap();
        assert!(matches!(space, Credential::Verifier(_)));
        let kept = FileSystem::open(&location).await.unwrap();
        let raw = key
            .did()
            .credential()
            .key(credential::SELF)
            .load()
            .perform(&kept)
            .await
            .unwrap();
        assert!(
            matches!(raw, Credential::Verifier(_)),
            "the space still holds its signing key"
        );
        let stored = StorageFx::profile("home")
            .load()
            .perform(&credentials)
            .await;
        assert!(stored.is_err() || matches!(stored, Ok(Credential::Signer(_))));
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
