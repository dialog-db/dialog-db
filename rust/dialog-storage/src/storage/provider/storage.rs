//! Storage: composes Router (DID routing) and Loader (space load/create).

mod loader;
#[cfg(not(target_arch = "wasm32"))]
mod native;
mod router;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;

use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::access::{AuthorizeError, Protocol, Prove, Retain};
use dialog_capability::{Capability, Did, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::credential::Secret;
use dialog_effects::{archive, credential, memory, storage};

use loader::Loader;
use router::Router;

use crate::provider::{Space, Volatile};
use crate::resource::Pool;

#[cfg(not(target_arch = "wasm32"))]
pub use native::NativeSpace;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web::WebSpace;

/// Storage: the runtime context for capability dispatch.
#[derive(Provider)]
pub struct Storage<S: Clone> {
    #[provide(storage::Load, storage::Create)]
    loader: Loader<S>,

    #[provide(
        archive::Get,
        archive::Put,
        memory::Resolve,
        memory::Publish,
        memory::Retract,
        credential::Load<Credential>,
        credential::Save<Credential>,
        credential::Load<Secret>,
        credential::Save<Secret>
    )]
    router: Router<S>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, P> Provider<Prove<P>> for Storage<S>
where
    S: Clone + ConditionalSync,
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    Router<S>: Provider<Prove<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Prove<P>>) -> Result<P::Proof, AuthorizeError> {
        input.perform(&self.router).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, P> Provider<Retain<P>> for Storage<S>
where
    S: Clone + ConditionalSync,
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Router<S>: Provider<Retain<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Retain<P>>) -> Result<(), AuthorizeError> {
        input.perform(&self.router).await
    }
}

impl<S: Clone> Storage<S> {
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

/// Space backed by Volatile providers.
pub type VolatileSpace = Space<Volatile, Volatile, Volatile, Volatile>;

impl Storage<VolatileSpace> {
    /// Create a volatile (in-memory) environment for testing.
    pub fn volatile() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_credential;
    use dialog_capability::did;
    use dialog_common::Blake3Hash;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{LocationExt, Storage as StorageFx};
    use dialog_varsig::Principal;

    #[dialog_common::test]
    async fn it_creates_profile_with_sugar() {
        let env = Storage::volatile();
        let credential = test_credential().await;
        let expected_did = credential.did();

        let cred = StorageFx::profile("alice")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(cred.did(), expected_did);
        assert!(env.contains(&cred.did()));
    }

    #[dialog_common::test]
    async fn it_archives_after_create() {
        let env = Storage::volatile();
        let credential = test_credential().await;

        let cred = StorageFx::profile("bob")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();
        let did = cred.did();

        let content = b"hello world".to_vec();
        let digest = Blake3Hash::hash(&content);

        did.clone()
            .archive()
            .catalog("index")
            .put(digest.clone(), content.clone())
            .perform(&env)
            .await
            .unwrap();

        let result = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&env)
            .await;

        assert_eq!(result.unwrap(), Some(content));
    }

    #[dialog_common::test]
    async fn it_publishes_memory_after_create() {
        let env = Storage::volatile();
        let credential = test_credential().await;

        let did = StorageFx::profile("charlie")
            .create(credential)
            .perform(&env)
            .await
            .unwrap()
            .did();

        let content = b"cell value".to_vec();

        let etag = did
            .clone()
            .memory()
            .space("data")
            .cell("head")
            .publish(content.clone(), None)
            .perform(&env)
            .await
            .unwrap();

        assert!(!etag.is_empty());

        let resolved = did
            .memory()
            .space("data")
            .cell("head")
            .resolve()
            .perform(&env)
            .await
            .unwrap();

        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().content, content);
    }

    #[dialog_common::test]
    async fn it_errors_for_unmounted_did() {
        let env = Storage::volatile();

        let result = did!("key:zUnknown")
            .archive()
            .catalog("index")
            .get([0u8; 32])
            .perform(&env)
            .await;
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_isolates_spaces() {
        let env = Storage::volatile();

        let did1 = StorageFx::profile("dave")
            .create(test_credential().await)
            .perform(&env)
            .await
            .unwrap()
            .did();

        let did2 = StorageFx::profile("eve")
            .create(test_credential().await)
            .perform(&env)
            .await
            .unwrap()
            .did();

        let content = b"dave only".to_vec();
        let digest = Blake3Hash::hash(&content);

        did1.archive()
            .catalog("index")
            .put(digest.clone(), content)
            .perform(&env)
            .await
            .unwrap();

        let result = did2
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&env)
            .await;

        assert_eq!(result.unwrap(), None, "eve should not see dave's data");
    }

    #[dialog_common::test]
    async fn it_rejects_duplicate_create() {
        let env = Storage::volatile();
        let credential = test_credential().await;

        StorageFx::profile("frank")
            .create(credential.clone())
            .perform(&env)
            .await
            .unwrap();

        let result = StorageFx::profile("frank")
            .create(credential)
            .perform(&env)
            .await;

        assert!(result.is_err(), "duplicate create should fail");
    }

    #[dialog_common::test]
    async fn it_creates_then_loads() {
        let env = Storage::volatile();
        let credential = test_credential().await;
        let expected_did = credential.did();

        StorageFx::profile("grace")
            .create(credential)
            .perform(&env)
            .await
            .unwrap();

        let loaded = StorageFx::profile("grace")
            .load()
            .perform(&env)
            .await
            .unwrap();

        assert_eq!(loaded.did(), expected_did);
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod native_tests {
        use super::*;
        use crate::helpers::unique_name;

        #[dialog_common::test]
        async fn it_creates_and_loads_on_filesystem() {
            let env = Storage::temp();
            let name = unique_name("fs-create-load");

            let credential = test_credential().await;
            let expected_did = credential.did();

            let cred = StorageFx::profile(&name)
                .create(credential)
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(cred.did(), expected_did);

            let loaded = StorageFx::profile(&name)
                .load()
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(loaded.did(), expected_did);
        }

        #[dialog_common::test]
        async fn it_persists_archive_on_filesystem() {
            let env = Storage::temp();
            let name = unique_name("fs-archive");

            let credential = test_credential().await;

            let did = StorageFx::profile(&name)
                .create(credential)
                .perform(&env)
                .await
                .unwrap()
                .did();

            let content = b"persistent data".to_vec();
            let digest = Blake3Hash::hash(&content);

            did.clone()
                .archive()
                .catalog("index")
                .put(digest.clone(), content.clone())
                .perform(&env)
                .await
                .unwrap();

            let result = did
                .archive()
                .catalog("index")
                .get(digest)
                .perform(&env)
                .await;

            assert_eq!(result.unwrap(), Some(content));
        }

        #[dialog_common::test]
        async fn it_rejects_duplicate_create_on_filesystem() {
            let env = Storage::temp();
            let name = unique_name("fs-dup");

            let credential = test_credential().await;

            StorageFx::profile(&name)
                .create(credential.clone())
                .perform(&env)
                .await
                .unwrap();

            let result = StorageFx::profile(&name)
                .create(credential)
                .perform(&env)
                .await;
            assert!(result.is_err(), "duplicate create should fail");
        }
    }
}
