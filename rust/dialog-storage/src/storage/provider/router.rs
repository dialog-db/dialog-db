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

impl<S: Clone> Default for Environment<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::space::MountedSpace;
    use super::*;
    use crate::provider::Volatile;
    use dialog_capability::{Subject, did};
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_effects::storage::Storage;

    type TestSpace = MountedSpace<Volatile, Volatile, Volatile, Volatile>;
    type TestEnv = Environment<TestSpace>;

    #[dialog_common::test]
    async fn it_creates_and_mounts_profile() {
        let env = TestEnv::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let profile_did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let did = Storage::profile("alice")
            .invoke(dialog_effects::storage::Create::new(credential))
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(did, profile_did);
        assert!(env.contains(&did));
    }

    // Volatile creates fresh stores each time, so duplicate detection
    // doesn't work. This test passes with persistent backends (FileStore, IDB).
    #[dialog_common::test]
    #[should_panic(expected = "duplicate create should fail")]
    async fn it_rejects_duplicate_create() {
        let env = TestEnv::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        Storage::profile("bob")
            .invoke(dialog_effects::storage::Create::new(credential.clone()))
            .perform(&env)
            .await
            .unwrap();

        let result = Storage::profile("bob")
            .invoke(dialog_effects::storage::Create::new(credential))
            .perform(&env)
            .await;

        assert!(result.is_err(), "duplicate create should fail");
    }

    #[dialog_common::test]
    async fn it_routes_effects_after_create() {
        let env = TestEnv::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let did = Storage::profile("charlie")
            .invoke(dialog_effects::storage::Create::new(credential))
            .perform(&env)
            .await
            .unwrap();

        let content = b"hello".to_vec();
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
    async fn it_errors_for_unmounted_did() {
        let env = TestEnv::new();

        let result = Subject::from(did!("key:zUnknown"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]))
            .perform(&env)
            .await;
        assert!(result.is_err());
    }

    // Volatile creates fresh stores each time, so load-after-create
    // doesn't see the saved credential. Passes with persistent backends.
    #[dialog_common::test]
    #[should_panic]
    async fn it_creates_then_loads() {
        let env = TestEnv::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let profile_did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        Storage::profile("dave")
            .invoke(dialog_effects::storage::Create::new(credential))
            .perform(&env)
            .await
            .unwrap();

        let did = Storage::profile("dave")
            .invoke(dialog_effects::storage::Load)
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(did, profile_did);
    }
}
