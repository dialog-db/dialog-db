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
pub use web::{WebOpfsSpace, WebSpace};

/// Storage: the runtime context for capability dispatch.
#[derive(Provider)]
pub struct Storage<S: Clone> {
    #[provide(storage::Load, storage::Create)]
    loader: Loader<S>,

    #[provide(
        archive::Get,
        archive::Put,
        archive::Import,
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
    use dialog_common::{Blake3Hash, Buffer};
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
            .put(Buffer::from(content.clone()))
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
            .put(Buffer::from(content))
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

        // `load` is "open an existing space, fail if absent" — it must
        // never bring a space into being. A `load` of a name that was
        // never created has to error AND leave nothing behind on disk
        // (the bug it guards: a missing-name `load` materializing an
        // empty backing store, e.g. a stray IndexedDB database).
        #[dialog_common::test]
        async fn it_does_not_create_on_load_of_missing_space() {
            use std::path::PathBuf;

            let env = Storage::temp();
            let name = unique_name("fs-load-missing");

            // Where `TempFileSystem` roots a `Directory::Profile` space.
            let root: PathBuf = std::env::temp_dir()
                .join("dialog")
                .join("profile")
                .join(&name);
            assert!(!root.exists(), "precondition: space must not exist yet");

            let result = StorageFx::profile(&name).load().perform(&env).await;

            assert!(result.is_err(), "load of a missing space must fail");
            assert!(
                !root.exists(),
                "load must not create the space directory: {}",
                root.display(),
            );
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
                .put(Buffer::from(content.clone()))
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    mod web_tests {
        use super::*;
        use crate::helpers::unique_name;

        // `Storage::opfs()` mounts a Space that keeps archive and memory in OPFS
        // and credential/certificate on IndexedDB. These exercise the full
        // mixed-backend round-trip: create (credential -> IndexedDB), archive a
        // blob and publish a cell (-> OPFS), read them back, then reload the
        // profile (credential read back from IndexedDB).
        #[dialog_common::test]
        async fn it_round_trips_archive_and_memory_on_opfs() {
            let env = Storage::opfs();
            let name = unique_name("opfs-roundtrip");

            let did = StorageFx::profile(&name)
                .create(test_credential().await)
                .perform(&env)
                .await
                .unwrap()
                .did();

            // Archive (-> OPFS).
            let content = b"opfs archive blob".to_vec();
            let digest = Blake3Hash::hash(&content);
            did.clone()
                .archive()
                .catalog("index")
                .put(Buffer::from(content.clone()))
                .perform(&env)
                .await
                .unwrap();
            let got = did
                .clone()
                .archive()
                .catalog("index")
                .get(digest)
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(got, Some(content));

            // Memory (-> OPFS).
            let cell = b"opfs cell value".to_vec();
            did.clone()
                .memory()
                .space("data")
                .cell("head")
                .publish(cell.clone(), None)
                .perform(&env)
                .await
                .unwrap();
            let resolved = did
                .memory()
                .space("data")
                .cell("head")
                .resolve()
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(resolved.unwrap().content, cell);
        }

        #[dialog_common::test]
        async fn it_loads_the_credential_from_indexeddb() {
            let env = Storage::opfs();
            let name = unique_name("opfs-credential");
            let credential = test_credential().await;
            let expected_did = credential.did();

            StorageFx::profile(&name)
                .create(credential)
                .perform(&env)
                .await
                .unwrap();

            // The credential lives on IndexedDB; loading the profile reads it
            // back, proving the credential slot is wired to IndexedDB while
            // archive/memory above used OPFS.
            let loaded = StorageFx::profile(&name)
                .load()
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(loaded.did(), expected_did);
        }

        #[dialog_common::test]
        async fn it_enforces_cas_on_opfs_memory() {
            use dialog_effects::memory::MemoryError;

            let env = Storage::opfs();
            let did = StorageFx::profile(&unique_name("opfs-cas"))
                .create(test_credential().await)
                .perform(&env)
                .await
                .unwrap()
                .did();

            did.clone()
                .memory()
                .space("data")
                .cell("head")
                .publish(b"first".to_vec(), None)
                .perform(&env)
                .await
                .unwrap();

            // A second IfNoneMatch publish must fail: the cell already exists.
            let result = did
                .memory()
                .space("data")
                .cell("head")
                .publish(b"second".to_vec(), None)
                .perform(&env)
                .await;
            assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
        }

        #[dialog_common::test]
        async fn it_isolates_spaces_on_opfs() {
            let env = Storage::opfs();
            let alice = StorageFx::profile(&unique_name("opfs-alice"))
                .create(test_credential().await)
                .perform(&env)
                .await
                .unwrap()
                .did();
            let bob = StorageFx::profile(&unique_name("opfs-bob"))
                .create(test_credential().await)
                .perform(&env)
                .await
                .unwrap()
                .did();

            let content = b"alice only".to_vec();
            let digest = Blake3Hash::hash(&content);
            alice
                .archive()
                .catalog("index")
                .put(Buffer::from(content))
                .perform(&env)
                .await
                .unwrap();

            let seen = bob
                .archive()
                .catalog("index")
                .get(digest)
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(seen, None, "bob's OPFS space must not see alice's blob");
        }

        #[dialog_common::test]
        async fn it_round_trips_a_large_blob_on_opfs() {
            // A megabyte blob — the large-payload case OPFS is meant to serve.
            let env = Storage::opfs();
            let did = StorageFx::profile(&unique_name("opfs-large"))
                .create(test_credential().await)
                .perform(&env)
                .await
                .unwrap()
                .did();

            let content: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();
            let digest = Blake3Hash::hash(&content);
            did.clone()
                .archive()
                .catalog("index")
                .put(Buffer::from(content.clone()))
                .perform(&env)
                .await
                .unwrap();
            let got = did
                .archive()
                .catalog("index")
                .get(digest)
                .perform(&env)
                .await
                .unwrap();
            assert_eq!(got, Some(content));
        }

        // `load` is "open an existing space, fail if absent" — it must never
        // bring a space into being. On IndexedDB a space is one database, so a
        // `load` of a never-created profile has to error AND leave no database
        // behind (the bug it guards: a missing-name load materializing a stray
        // IndexedDB database, e.g. the literal `{subject}` DB an unbound
        // template once produced).
        #[dialog_common::test]
        async fn it_does_not_create_on_load_of_missing_space() {
            use crate::provider::storage::WebSpace;

            let env: Storage<WebSpace> = Storage::default();
            let name = unique_name("idb-load-missing");
            // `Directory::Profile` names the database `"{name}.profile"`.
            let db_name = format!("{name}.profile");

            assert!(
                !idb_database_exists(&db_name).await,
                "precondition: database must not exist yet",
            );

            let result = StorageFx::profile(&name).load().perform(&env).await;

            assert!(result.is_err(), "load of a missing space must fail");
            assert!(
                !idb_database_exists(&db_name).await,
                "load must not create the IndexedDB database `{db_name}`",
            );
        }

        /// True if an IndexedDB database with this name currently exists.
        ///
        /// Calls `indexedDB.databases()` (which lists without opening, so it
        /// never creates one) off the worker/window global — the test runs in
        /// a dedicated worker where `window` is absent, so the factory is read
        /// from `self`, not `window`.
        async fn idb_database_exists(name: &str) -> bool {
            use js_sys::{Array, Function, Promise, Reflect};
            use wasm_bindgen::{JsCast, JsValue};
            use wasm_bindgen_futures::JsFuture;

            let global = js_sys::global();
            let factory = Reflect::get(&global, &JsValue::from_str("indexedDB"))
                .expect("global indexedDB");
            let databases: Function = Reflect::get(&factory, &JsValue::from_str("databases"))
                .expect("indexedDB.databases")
                .unchecked_into();
            let promise: Promise = databases
                .call0(&factory)
                .expect("databases() call")
                .unchecked_into();
            let list = JsFuture::from(promise).await.expect("databases() resolves");

            Array::from(&list).iter().any(|info| {
                Reflect::get(&info, &JsValue::from_str("name"))
                    .ok()
                    .and_then(|n| n.as_string())
                    .as_deref()
                    == Some(name)
            })
        }
    }
}
