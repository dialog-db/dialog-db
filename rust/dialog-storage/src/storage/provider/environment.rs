//! Environment: composes Router (DID routing) and Loader (space load/create).

use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Policy, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::storage::Location;
use dialog_varsig::Principal;

use super::space::SpaceProvider;
use crate::resource::{Pool, Resource};
use dialog_capability::StorageError;

/// Routes effects by subject DID to the matching store.
#[derive(Clone)]
pub struct Router<S> {
    spaces: Arc<Pool<Did, S>>,
}

impl<S> Router<S> {
    fn new(spaces: Arc<Pool<Did, S>>) -> Self {
        Self { spaces }
    }
}

trait FromUnmounted {
    fn unmounted(did: &Did) -> Self;
}

impl<T, E: From<StorageError>> FromUnmounted for Result<T, E> {
    fn unmounted(did: &Did) -> Self {
        Err(StorageError::Storage(format!("no mount for {did}")).into())
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
        let store = self.spaces.get(&did);
        match store {
            Some(store) => input.perform(&store).await,
            None => Fx::Output::unmounted(&did),
        }
    }
}

/// Handles storage::Load and storage::Create, mutating the shared table.
///
/// Maintains a location -> DID mapping so that loading the same location
/// twice returns the existing DID (important for non-persistent backends).
pub struct Loader<S> {
    spaces: Arc<Pool<Did, S>>,
    mounts: Pool<String, Did>,
}

impl<S> Loader<S> {
    fn new(spaces: Arc<Pool<Did, S>>) -> Self {
        Self {
            spaces,
            mounts: Pool::new(),
        }
    }

    fn register(&self, did: Did, location_key: String, store: S) {
        self.mounts.insert(location_key, did.clone());
        self.spaces.insert(did, store);
    }

    fn mounted_did(&self, key: &String) -> Option<Did> {
        self.mounts.get(key)
    }
}

fn location_key(location: &Location) -> String {
    format!("{:?}/{}", location.directory, location.name)
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
    ) -> Result<dialog_credentials::Credential, dialog_effects::storage::StorageError> {
        use dialog_effects::{credential, storage::StorageError};

        let location = Location::of(&input);
        let key = location_key(location);

        // Return existing credential if this location is already mounted
        if let Some(did) = self.mounted_did(&key) {
            let store = self.spaces.get(&did);
            if let Some(store) = store {
                let cred_cap =
                    dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
                        .attenuate(credential::Credential)
                        .attenuate(credential::Address::new("credential/self"))
                        .invoke(credential::Load);
                return cred_cap
                    .perform(&store)
                    .await
                    .map_err(|e| StorageError::NotFound(e.to_string()));
            }
        }

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let cred_cap = dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(credential::Credential)
            .attenuate(credential::Address::new("credential/self"))
            .invoke(credential::Load);

        let cred: dialog_credentials::Credential = cred_cap
            .perform(&store)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        let did = cred.did();
        self.register(did, key, store);
        Ok(cred)
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
    ) -> Result<dialog_credentials::Credential, dialog_effects::storage::StorageError> {
        use dialog_effects::storage::{Create, StorageError};

        let location = Location::of(&input);
        let credential = Create::of(&input).credential.clone();
        let key = location_key(location);

        // Check if this location is already mounted
        if self.mounted_did(&key).is_some() {
            return Err(StorageError::AlreadyExists(key));
        }

        // Check if this DID is already mounted
        let did = credential.did();
        if self.spaces.contains(&did) {
            return Err(StorageError::AlreadyExists(format!("{did}")));
        }

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let save_cap = dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(dialog_effects::credential::Credential)
            .attenuate(dialog_effects::credential::Address::new("credential/self"))
            .invoke(dialog_effects::credential::Save {
                credential: credential.clone(),
            });

        save_cap
            .perform(&store)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        self.register(did, key, store);
        Ok(credential)
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

use dialog_capability::access::{
    AuthorizeError, Protocol, Prove as AccessClaim, Retain as AccessRetain,
};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, P> Provider<AccessClaim<P>> for Environment<S>
where
    S: Clone + ConditionalSync,
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    Router<S>: Provider<AccessClaim<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<AccessClaim<P>>) -> Result<P::Proof, AuthorizeError> {
        input.perform(&self.router).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, P> Provider<AccessRetain<P>> for Environment<S>
where
    S: Clone + ConditionalSync,
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Router<S>: Provider<AccessRetain<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<AccessRetain<P>>) -> Result<(), AuthorizeError> {
        input.perform(&self.router).await
    }
}

impl<S: Clone> Environment<S> {
    /// Create a new empty environment.
    pub fn new() -> Self {
        let spaces = Arc::new(Pool::new());
        Self {
            loader: Loader::new(Arc::clone(&spaces)),
            router: Router::new(spaces),
        }
    }

    /// Check if a DID is mounted.
    pub fn contains(&self, did: &Did) -> bool {
        self.router.spaces.contains(did)
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

    /// Helper: create a credential and return (credential, expected_did).
    async fn test_credential() -> (dialog_credentials::Credential, Did) {
        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));
        (credential, did)
    }

    #[dialog_common::test]
    async fn it_creates_profile_with_sugar() {
        let env = Environment::volatile();
        let (credential, expected_did) = test_credential().await;

        let cred = Storage::profile("alice")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(cred.did(), expected_did);
        assert!(env.contains(&cred.did()));
    }

    #[dialog_common::test]
    async fn it_archives_after_create() {
        let env = Environment::volatile();
        let (credential, _) = test_credential().await;

        let cred = Storage::profile("bob")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();
        let did = cred.did();

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
        let (credential, _) = test_credential().await;

        let did = Storage::profile("charlie")
            .create(credential)
            .perform(&env)
            .await
            .unwrap()
            .did();

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

        let (cred1, _) = test_credential().await;
        let did1 = Storage::profile("dave")
            .create(cred1)
            .perform(&env)
            .await
            .unwrap()
            .did();

        let (cred2, _) = test_credential().await;
        let did2 = Storage::profile("eve")
            .create(cred2)
            .perform(&env)
            .await
            .unwrap()
            .did();

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

    #[dialog_common::test]
    async fn it_rejects_duplicate_create() {
        let env = Environment::volatile();
        let (credential, _) = test_credential().await;

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
    async fn it_creates_then_loads() {
        let env = Environment::volatile();
        let (credential, expected_did) = test_credential().await;

        Storage::profile("grace")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        let loaded = Storage::profile("grace")
            .load()
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(loaded.did(), expected_did);
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod native_tests {
        use super::*;
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};

        fn unique_name(prefix: &str) -> String {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let ts = time::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("{prefix}-{ts}-{seq}")
        }

        type NativeEnv = Environment<NativeSpace>;

        #[dialog_common::test]
        async fn it_creates_and_loads_on_filesystem() {
            let env = NativeEnv::default();
            let name = unique_name("fs-create-load");

            let (credential, expected_did) = super::test_credential().await;

            let cred = Storage::temp(&name)
                .create(credential)
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(cred.did(), expected_did);

            let loaded = Storage::temp(&name).load().perform(&env).await.unwrap();
            assert_eq!(loaded.did(), expected_did);
        }

        #[dialog_common::test]
        async fn it_persists_archive_on_filesystem() {
            let env = NativeEnv::default();
            let name = unique_name("fs-archive");

            let (credential, _) = super::test_credential().await;

            let did = Storage::temp(&name)
                .create(credential)
                .perform(&env)
                .await
                .unwrap()
                .did();

            let content = b"persistent data".to_vec();
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
        async fn it_rejects_duplicate_create_on_filesystem() {
            let env = NativeEnv::default();
            let name = unique_name("fs-dup");

            let (credential, _) = super::test_credential().await;

            Storage::temp(&name)
                .create(credential.clone())
                .perform(&env)
                .await
                .unwrap();

            let result = Storage::temp(&name).create(credential).perform(&env).await;
            assert!(result.is_err(), "duplicate create should fail");
        }
    }
}
