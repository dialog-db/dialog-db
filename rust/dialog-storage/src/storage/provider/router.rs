//! Environment: routes effects by DID, handles space load/create.

use std::collections::HashMap;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use parking_lot::RwLock;

/// Environment: the runtime context for capability dispatch.
///
/// Routes effects by subject DID to the matching store `S`.
/// `S` is typically a [`MountedSpace`](super::space::MountedSpace) but
/// can be any type implementing the required Provider traits.
pub struct Environment<S> {
    spaces: RwLock<HashMap<Did, S>>,
}

impl<S> Environment<S> {
    /// Create a new empty environment.
    pub fn new() -> Self {
        Self {
            spaces: RwLock::new(HashMap::new()),
        }
    }

    /// Register a store for a DID.
    pub fn register(&self, did: Did, store: S) {
        self.spaces.write().insert(did, store);
    }

    /// Check if a DID is mounted.
    pub fn contains(&self, did: &Did) -> bool {
        self.spaces.read().contains_key(did)
    }
}

impl<S> Default for Environment<S> {
    fn default() -> Self {
        Self::new()
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

/// Blanket Provider impl: routes effects by subject DID to the matching store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, Fx> Provider<Fx> for Environment<S>
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

/// Volatile-backed environment.
pub type VolatileEnvironment = Environment<
    super::space::MountedSpace<super::Volatile, super::Volatile, super::Volatile, super::Volatile>,
>;

impl VolatileEnvironment {
    /// Create a new volatile environment for testing.
    pub fn volatile() -> Self {
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
    use dialog_effects::credential as cred_fx;
    use dialog_varsig::Principal;

    async fn seed_and_register(env: &VolatileEnvironment) -> Did {
        let provider = Volatile::new();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = Principal::did(&signer);
        let credential = dialog_credentials::Credential::Signer(SignerCredential::from(signer));

        let save_cap = Subject::from(did!("local:storage"))
            .attenuate(cred_fx::Credential)
            .attenuate(cred_fx::Address::new("credential"))
            .invoke(cred_fx::Save { credential });
        save_cap.perform(&provider).await.unwrap();

        let space = MountedSpace {
            archive: provider.clone(),
            memory: provider.clone(),
            credential: provider.clone(),
            permit: provider,
        };
        env.register(did.clone(), space);
        did
    }

    #[dialog_common::test]
    async fn it_routes_archive_effects_by_did() {
        let env = VolatileEnvironment::volatile();
        let did = seed_and_register(&env).await;

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
    async fn it_errors_for_unmounted_did() {
        let env = VolatileEnvironment::volatile();

        let result = Subject::from(did!("key:zUnknown"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new([0u8; 32]))
            .perform(&env)
            .await;
        assert!(result.is_err(), "should error for unmounted DID");
    }

    #[dialog_common::test]
    async fn it_isolates_spaces_by_did() {
        let env = VolatileEnvironment::volatile();
        let did1 = seed_and_register(&env).await;
        let did2 = seed_and_register(&env).await;

        let content = b"did1 data".to_vec();
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
        assert_eq!(result.unwrap(), None, "did2 should not see did1's data");
    }
}
