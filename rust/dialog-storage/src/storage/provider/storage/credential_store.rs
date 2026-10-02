//! The credential store: where signing keys are kept, apart from the
//! storage spaces live in.
//!
//! A [`Storage`](super::Storage) keeps no signing key: it holds a space's
//! data and its public identity, and whoever holds it is given nothing to
//! sign with. Keys are opened from a [`CredentialStore`] instead, by
//! whoever builds a peer, and the peer is handed the key it acts with.
//!
//! The store keeps every key of a directory in one space of that
//! directory, named [`VAULT`], each under its own name. A key and the
//! space of the same name never share a directory or a database, and a
//! directory holds one store however many keys it keeps.

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

use crate::provider::SpaceProvider;
use crate::resource::{Pool, Resource};

/// The name of the space a directory's keys are kept in, so a storage
/// tells it apart from a space of its own and refuses to mount it.
pub(super) const VAULT: &str = "dialog.credential";

/// Where a key the store handed out is kept: the directory, and the name
/// it is kept under there.
type Kept = (storage::Directory, String);

/// A store of signing keys, opened by name.
///
/// [`OpenCredential`](https://docs.rs/dialog-identity) loads, creates, or
/// opens a key in it. Like a [`Storage`](super::Storage), it belongs to a
/// system: the application opens keys from it acting as that system and
/// hands a peer the key it acts with; no peer holds the store. Cloning
/// yields a second handle onto the same keys.
pub struct CredentialStore<S: Clone> {
    /// The space each directory keeps its keys in, once opened.
    vaults: Arc<Pool<String, S>>,
    /// Where each key this store handed out is kept, by the key's DID.
    kept: Arc<Pool<Did, Kept>>,
    /// The system this store belongs to. Only its DID is kept.
    system: Option<Did>,
}

impl<S: Clone> Clone for CredentialStore<S> {
    fn clone(&self) -> Self {
        Self {
            vaults: Arc::clone(&self.vaults),
            kept: Arc::clone(&self.kept),
            system: self.system.clone(),
        }
    }
}

impl<S: Clone> CredentialStore<S> {
    /// A store with nothing opened yet.
    pub fn new() -> Self {
        Self {
            vaults: Arc::new(Pool::new()),
            kept: Arc::new(Pool::new()),
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

/// The space the keys of `directory` are kept in.
fn vault(directory: &storage::Directory) -> storage::Location {
    storage::Location::new(directory.clone(), VAULT)
}

/// The key named `name` as a space keeps it: under its own name, where a
/// space's own identity is kept under [`credential::SELF`].
fn key(name: &str) -> Capability<credential::Key> {
    Subject::from(did!("local:storage")).credential().key(name)
}

fn failed(error: impl Display) -> storage::StorageError {
    storage::StorageError::Storage(error.to_string())
}

impl<S> CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend,
    S::Error: Display,
{
    /// The space `directory` keeps its keys in. With `create` it is
    /// brought into being when absent; without, a directory that keeps no
    /// keys yet answers `None` and nothing is created.
    async fn vault(
        &self,
        directory: &storage::Directory,
        create: bool,
    ) -> Result<Option<S>, storage::StorageError> {
        let location = vault(directory);
        let pooled = format!("{directory:?}");
        if let Some(space) = self.vaults.get(&pooled) {
            return Ok(Some(space));
        }
        let space = if create {
            S::open(&location).await.map_err(failed)?
        } else {
            match S::load(&location).await {
                Ok(space) => space,
                Err(error) if S::is_not_found(&error) => return Ok(None),
                Err(error) => return Err(failed(error)),
            }
        };
        self.vaults.insert(pooled, space.clone());
        Ok(Some(space))
    }

    /// The key kept at `location`, or `None` when the directory keeps no
    /// key under that name.
    async fn load(
        &self,
        location: &storage::Location,
    ) -> Result<Option<Credential>, storage::StorageError> {
        let Some(space) = self.vault(&location.directory, false).await? else {
            return Ok(None);
        };
        match key(&location.name).load().perform(&space).await {
            Ok(credential) => {
                self.kept.insert(
                    credential.did(),
                    (location.directory.clone(), location.name.clone()),
                );
                Ok(Some(credential))
            }
            Err(credential::CredentialError::NotFound(_)) => Ok(None),
            Err(error) => Err(failed(error)),
        }
    }

    /// Keep `credential` at `location`, refusing a name that already
    /// keeps a key.
    async fn create(
        &self,
        location: &storage::Location,
        credential: Credential,
    ) -> Result<Credential, storage::StorageError> {
        if self.load(location).await?.is_some() {
            return Err(storage::StorageError::AlreadyExists(format!(
                "{:?}/{}",
                location.directory, location.name
            )));
        }
        let Some(space) = self.vault(&location.directory, true).await? else {
            return Err(failed("the credential store could not be opened"));
        };
        key(&location.name)
            .save(credential.clone())
            .perform(&space)
            .await
            .map_err(failed)?;
        self.kept.insert(
            credential.did(),
            (location.directory.clone(), location.name.clone()),
        );
        Ok(credential)
    }

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
    ) -> Result<Credential, storage::StorageError>
    where
        super::router::Router<S>: Provider<credential::Save<Credential>>,
        Self: ConditionalSend + ConditionalSync,
    {
        let at = Subject::from(did!("local:storage"))
            .attenuate(storage::Storage)
            .attenuate(location.clone());
        // The loader itself, not the storage: the storage hands a space
        // over as its verifier, and this is where the key is taken out.
        let held = at.clone().load().perform(&storage.loader).await?;
        let Credential::Signer(signer) = &held else {
            return self.load(location).await?.ok_or_else(|| {
                storage::StorageError::NotFound(format!("no key is kept under {}", location.name))
            });
        };
        let kept = match self.create(location, held.clone()).await {
            Ok(kept) => kept,
            Err(storage::StorageError::AlreadyExists(_)) => {
                self.load(location).await?.ok_or_else(|| {
                    storage::StorageError::NotFound(format!(
                        "no key is kept under {}",
                        location.name
                    ))
                })?
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
            .map_err(failed)?;
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
        let location = storage::Location::of(&input);
        self.load(location).await?.ok_or_else(|| {
            storage::StorageError::NotFound(format!("no key is kept under {}", location.name))
        })
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
        let location = storage::Location::of(&input);
        let credential = storage::Create::of(&input).credential.clone();
        self.create(location, credential).await
    }
}

/// The key a store handed out, read again by the key's own DID.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<credential::Load<Credential>> for CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Load<Credential>>,
    ) -> Result<Credential, credential::CredentialError> {
        let missing = || credential::CredentialError::NotFound(input.subject().to_string());
        let (directory, name) = self.kept.get(input.subject()).ok_or_else(missing)?;
        self.load(&storage::Location::new(directory, name))
            .await
            .map_err(|error| credential::CredentialError::Storage(error.to_string()))?
            .ok_or_else(missing)
    }
}

/// A key retracted from the store is gone: the store held its only copy.
/// The name it was kept under is free for the next key created there.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<credential::Retract<Credential>> for CredentialStore<S>
where
    S: Clone + SpaceProvider + Resource<storage::Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<credential::Retract<Credential>>,
    ) -> Result<(), credential::CredentialError> {
        // Retracting a key the store never handed out retracts nothing.
        let Some((directory, name)) = self.kept.get(input.subject()) else {
            return Ok(());
        };
        let space = self
            .vault(&directory, false)
            .await
            .map_err(|error| credential::CredentialError::Storage(error.to_string()))?;
        if let Some(space) = space {
            key(&name).retract().perform(&space).await?;
        }
        self.kept.remove(input.subject());
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

    /// Every key of a directory is kept in the directory's one credential
    /// space: keeping a second key adds nothing beside it. The volatile
    /// store keeps nothing on disk to count;
    /// `it_keeps_every_key_of_a_directory_in_one_database` pins the same
    /// in the browser.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_keeps_every_key_of_a_directory_in_one_space() {
        use crate::provider::storage::NativeSpace;
        use dialog_effects::storage::{Directory, Location};

        let root = tempfile::tempdir().unwrap();
        let base = Directory::At(root.path().to_string_lossy().into_owned());
        let credentials = CredentialStore::<NativeSpace>::new();
        let at = |name: &str| {
            Subject::from(did!("local:storage"))
                .attenuate(storage::Storage)
                .attenuate(Location::new(base.clone(), name))
        };

        let alice = test_credential().await;
        let bob = test_credential().await;
        at("alice")
            .create(alice.clone())
            .perform(&credentials)
            .await
            .unwrap();
        at("bob")
            .create(bob.clone())
            .perform(&credentials)
            .await
            .unwrap();

        let entries: Vec<String> = std::fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec![VAULT.to_string()]);

        // A fresh handle on the store reads each key back under its name.
        let reopened = CredentialStore::<NativeSpace>::new();
        let loaded = at("alice").load().perform(&reopened).await.unwrap();
        assert_eq!(loaded.did(), alice.did());
        let loaded = at("bob").load().perform(&reopened).await.unwrap();
        assert_eq!(loaded.did(), bob.did());
        assert!(matches!(
            at("carol").load().perform(&reopened).await,
            Err(storage::StorageError::NotFound(_))
        ));
    }

    /// In the browser every key of a directory is a row of one database,
    /// and no key gets a database of its own.
    #[cfg(target_arch = "wasm32")]
    #[dialog_common::test]
    async fn it_keeps_every_key_of_a_directory_in_one_database() {
        use crate::helpers::unique_name;
        use crate::provider::indexeddb::database_exists;
        use crate::provider::storage::WebSpace;
        use dialog_effects::storage::{Directory, Location};

        let path = unique_name("credentials");
        let base = Directory::At(path.clone());
        let credentials = CredentialStore::<WebSpace>::new();
        let at = |name: &str| {
            Subject::from(did!("local:storage"))
                .attenuate(storage::Storage)
                .attenuate(Location::new(base.clone(), name))
        };

        let alice = test_credential().await;
        let bob = test_credential().await;
        at("alice")
            .create(alice.clone())
            .perform(&credentials)
            .await
            .unwrap();
        at("bob")
            .create(bob.clone())
            .perform(&credentials)
            .await
            .unwrap();

        assert!(database_exists(&format!("{path}/{VAULT}")).await.unwrap());
        for name in ["alice", "bob", "alice.credentials", "bob.credentials"] {
            assert!(
                !database_exists(&format!("{path}/{name}")).await.unwrap(),
                "{name} got a database of its own"
            );
        }

        let reopened = CredentialStore::<WebSpace>::new();
        let loaded = at("alice").load().perform(&reopened).await.unwrap();
        assert_eq!(loaded.did(), alice.did());
        let loaded = at("bob").load().perform(&reopened).await.unwrap();
        assert_eq!(loaded.did(), bob.did());
    }

    /// Loading a key from a directory that keeps none creates nothing
    /// there: the store comes into being with its first key.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_creates_nothing_to_look_for_a_key() {
        use crate::provider::storage::NativeSpace;
        use dialog_effects::storage::{Directory, Location};

        let root = tempfile::tempdir().unwrap();
        let base = Directory::At(root.path().to_string_lossy().into_owned());
        let credentials = CredentialStore::<NativeSpace>::new();
        let missing = Subject::from(did!("local:storage"))
            .attenuate(storage::Storage)
            .attenuate(Location::new(base, "nobody"))
            .load()
            .perform(&credentials)
            .await;
        assert!(matches!(missing, Err(storage::StorageError::NotFound(_))));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
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
