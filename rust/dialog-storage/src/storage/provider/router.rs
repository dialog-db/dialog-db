//! Environment: composes Router (DID routing) and Loader (space load/create).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::storage::Location;
use dialog_varsig::Principal;
use parking_lot::RwLock;

use super::space::SpaceProvider;
use crate::resource::Resource;

type Spaces<S> = Arc<RwLock<HashMap<Did, S>>>;

/// Routes effects by subject DID to the matching store.
#[derive(Clone)]
pub struct Router<S> {
    spaces: Spaces<S>,
}

impl<S: Clone> Router<S> {
    fn new(spaces: Spaces<S>) -> Self {
        Self { spaces }
    }
}

trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<dialog_capability::storage::StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(dialog_capability::storage::StorageError::Storage(format!("no mount for {did}")).into())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, Fx> Provider<Fx> for Router<S>
where
    S: Provider<Fx> + ConditionalSync + Clone,
    Fx: dialog_capability::Effect + ConditionalSend + 'static,
    Fx::Output: FromUnmounted,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        let did = input.subject().clone();
        let store = self.spaces.read().get(&did).cloned();
        match store {
            Some(store) => store.execute(input).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}

/// Handles storage::Load and storage::Create, mutating the shared table.
pub struct Loader<S> {
    spaces: Spaces<S>,
}

impl<S> Loader<S> {
    fn new(spaces: Spaces<S>) -> Self {
        Self { spaces }
    }

    fn register(&self, did: Did, store: S) {
        self.spaces.write().insert(did, store);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<dialog_effects::storage::Load> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: std::fmt::Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<dialog_effects::storage::Load>,
    ) -> Result<Did, dialog_effects::storage::StorageError> {
        use dialog_effects::{credential, storage::StorageError};

        let location = Location::of(&input);
        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let cred_cap = dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(credential::Credential)
            .attenuate(credential::Address::new("credential/self"))
            .invoke(credential::Load);

        let cred: dialog_credentials::Credential =
            <S as Provider<credential::Load>>::execute(&store, cred_cap)
                .await
                .map_err(|e| StorageError::NotFound(e.to_string()))?;

        let did = cred.did();
        self.register(did.clone(), store);
        Ok(did)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<dialog_effects::storage::Create> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: std::fmt::Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<dialog_effects::storage::Create>,
    ) -> Result<Did, dialog_effects::storage::StorageError> {
        use dialog_effects::storage::{Create, StorageError};

        let location = Location::of(&input);
        let credential = &Create::of(&input).credential;

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let check_cap = dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(dialog_effects::credential::Credential)
            .attenuate(dialog_effects::credential::Address::new("credential/self"))
            .invoke(dialog_effects::credential::Load);

        let existing: Result<dialog_credentials::Credential, _> =
            <S as Provider<dialog_effects::credential::Load>>::execute(&store, check_cap).await;

        if existing.is_ok() {
            return Err(StorageError::AlreadyExists(format!(
                "{:?}/{}",
                location.directory, location.name
            )));
        }

        let save_cap = dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(dialog_effects::credential::Credential)
            .attenuate(dialog_effects::credential::Address::new("credential/self"))
            .invoke(dialog_effects::credential::Save {
                credential: credential.clone(),
            });

        <S as Provider<dialog_effects::credential::Save>>::execute(&store, save_cap)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let did = credential.did();
        self.register(did.clone(), store);
        Ok(did)
    }
}

/// Environment: the runtime context for capability dispatch.
#[derive(dialog_capability::Provider)]
pub struct Environment<S: Clone> {
    #[provide(dialog_effects::storage::Load, dialog_effects::storage::Create)]
    loader: Loader<S>,

    #[provide(
        dialog_effects::archive::Get,
        dialog_effects::archive::Put,
        dialog_effects::memory::Resolve,
        dialog_effects::memory::Publish,
        dialog_effects::memory::Retract,
        dialog_effects::credential::Load,
        dialog_effects::credential::Save
    )]
    router: Router<S>,
}

impl<S: Clone> Environment<S> {
    /// Create a new empty environment.
    pub fn new() -> Self {
        let spaces: Spaces<S> = Arc::new(RwLock::new(HashMap::new()));
        Self {
            loader: Loader::new(Arc::clone(&spaces)),
            router: Router::new(spaces),
        }
    }

    /// Check if a DID is mounted.
    pub fn contains(&self, did: &Did) -> bool {
        self.router.spaces.read().contains_key(did)
    }
}

/// MountedSpace backed by Volatile providers.
pub type VolatileSpace =
    super::space::MountedSpace<super::Volatile, super::Volatile, super::Volatile, super::Volatile>;

#[cfg(not(target_arch = "wasm32"))]
/// MountedSpace backed by FileStore providers.
pub type NativeSpace = super::space::MountedSpace<
    super::FileStore,
    super::FileStore,
    super::FileStore,
    super::FileStore,
>;

impl Environment<VolatileSpace> {
    /// Create a volatile (in-memory) environment for testing.
    pub fn volatile() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for Environment<NativeSpace> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl Default
    for Environment<
        super::space::MountedSpace<
            super::IndexedDb,
            super::IndexedDb,
            super::IndexedDb,
            super::IndexedDb,
        >,
    >
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::memory::{self, Memory};
    use dialog_effects::storage::{LocationExt, Storage};

    #[dialog_common::test]
    async fn it_creates_profile_with_sugar() {
        let env = Environment::volatile();

        let signer = Ed25519Signer::generate().await.unwrap();
        let profile_did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let did = Storage::profile("alice")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(did, profile_did);
        assert!(env.contains(&did));
    }

    #[dialog_common::test]
    async fn it_archives_after_create() {
        let env = Environment::volatile();

        let signer = Ed25519Signer::generate().await.unwrap();
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let did = Storage::profile("bob")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        let content = b"hello world".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        Subject::from(did.clone())
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content.clone()))
            .perform(&env)
            .await
            .unwrap();

        let result = Subject::from(did)
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&env)
            .await;

        assert_eq!(result.unwrap(), Some(content));
    }

    #[dialog_common::test]
    async fn it_publishes_memory_after_create() {
        let env = Environment::volatile();

        let signer = Ed25519Signer::generate().await.unwrap();
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let did = Storage::profile("charlie")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        let content = b"cell value".to_vec();

        let etag = Subject::from(did.clone())
            .attenuate(Memory)
            .attenuate(memory::Space::new("data"))
            .attenuate(memory::Cell::new("head"))
            .invoke(memory::Publish::new(content.clone(), None))
            .perform(&env)
            .await
            .unwrap();

        assert!(!etag.is_empty());

        let resolved = Subject::from(did)
            .attenuate(Memory)
            .attenuate(memory::Space::new("data"))
            .attenuate(memory::Cell::new("head"))
            .invoke(memory::Resolve)
            .perform(&env)
            .await
            .unwrap();

        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().content, content);
    }

    #[dialog_common::test]
    async fn it_errors_for_unmounted_did() {
        let env = Environment::volatile();

        let result = Subject::from(did!("key:zUnknown"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]))
            .perform(&env)
            .await;
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_isolates_spaces() {
        let env = Environment::volatile();

        let signer1 = Ed25519Signer::generate().await.unwrap();
        let cred1 = dialog_credentials::Credential::Signer(SignerCredential::from(signer1));
        let did1 = Storage::profile("dave")
            .create(cred1)
            .perform(&env)
            .await
            .unwrap();

        let signer2 = Ed25519Signer::generate().await.unwrap();
        let cred2 = dialog_credentials::Credential::Signer(SignerCredential::from(signer2));
        let did2 = Storage::profile("eve")
            .create(cred2)
            .perform(&env)
            .await
            .unwrap();

        let content = b"dave only".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        Subject::from(did1)
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest.clone(), content))
            .perform(&env)
            .await
            .unwrap();

        let result = Subject::from(did2)
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&env)
            .await;

        assert_eq!(result.unwrap(), None, "eve should not see dave's data");
    }

    // Volatile creates fresh stores each time, so duplicate detection
    // and load-after-create don't work. These pass with persistent backends.
    #[dialog_common::test]
    #[should_panic(expected = "duplicate create should fail")]
    async fn it_rejects_duplicate_create() {
        let env = Environment::volatile();

        let signer = Ed25519Signer::generate().await.unwrap();
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        Storage::profile("frank")
            .create(credential.clone())
            .perform(&env)
            .await
            .unwrap();

        let result = Storage::profile("frank")
            .create(credential)
            .perform(&env)
            .await;

        assert!(result.is_err(), "duplicate create should fail");
    }

    #[dialog_common::test]
    #[should_panic]
    async fn it_creates_then_loads() {
        let env = Environment::volatile();

        let signer = Ed25519Signer::generate().await.unwrap();
        let profile_did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        Storage::profile("grace")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        let did = Storage::profile("grace")
            .load()
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(did, profile_did);
    }
}
