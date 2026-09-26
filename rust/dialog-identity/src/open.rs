//! Opening the signing credential a peer is built over: load it, create
//! it, or do whichever applies.

use dialog_capability::{Capability, Provider, Subject, did};
use dialog_common::ConditionalSync;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt};

use crate::IdentityError;

enum OpenMode {
    OpenOrCreate,
    Load,
    Create,
}

/// Command to open, load, or create a signing credential at a named
/// location.
///
/// What it yields is a peer's identity; `dialog-peer` builds the peer
/// over it.
pub struct OpenCredential {
    name: String,
    directory: Directory,
    mode: OpenMode,
}

impl OpenCredential {
    /// Open the credential named `name`: load it, or generate and
    /// persist one if none is there.
    pub fn open(name: impl Into<String>) -> Self {
        Self::new(name.into(), OpenMode::OpenOrCreate)
    }

    /// Load the credential named `name`, failing if none is there.
    pub fn load(name: impl Into<String>) -> Self {
        Self::new(name.into(), OpenMode::Load)
    }

    /// Create the credential named `name`, failing if one is there.
    pub fn create(name: impl Into<String>) -> Self {
        Self::new(name.into(), OpenMode::Create)
    }

    fn new(name: String, mode: OpenMode) -> Self {
        Self {
            name,
            directory: Directory::Profile,
            mode,
        }
    }

    /// Set the directory the credential lives in.
    ///
    /// Defaults to `Directory::Profile` (the platform profile directory).
    /// Use `Directory::Temp` for testing or ephemeral credentials.
    pub fn at(mut self, directory: Directory) -> Self {
        self.directory = directory;
        self
    }

    fn location(&self) -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(Location::new(self.directory.clone(), &self.name))
    }

    /// Execute against a storage provider.
    pub async fn perform<Env>(self, env: &Env) -> Result<SignerCredential, IdentityError>
    where
        Env: Provider<storage_fx::Load> + Provider<storage_fx::Create> + ConditionalSync,
    {
        let credential = match self.mode {
            OpenMode::Load => self
                .location()
                .load()
                .perform(env)
                .await
                .map_err(|e| match e {
                    storage_fx::StorageError::NotFound(_) => IdentityError::NotFound,
                    e => IdentityError::Storage(e.to_string()),
                })?,
            OpenMode::Create => self
                .location()
                .create(generate().await?)
                .perform(env)
                .await
                .map_err(|e| match e {
                    storage_fx::StorageError::AlreadyExists(_) => IdentityError::AlreadyExists,
                    e => IdentityError::Storage(e.to_string()),
                })?,
            OpenMode::OpenOrCreate => match self.location().load().perform(env).await {
                Ok(credential) => credential,
                Err(storage_fx::StorageError::NotFound(_)) => self
                    .location()
                    .create(generate().await?)
                    .perform(env)
                    .await
                    .map_err(|e| IdentityError::Storage(e.to_string()))?,
                Err(error) => return Err(IdentityError::Storage(error.to_string())),
            },
        };

        match credential {
            Credential::Signer(signer) => Ok(signer),
            Credential::Verifier(_) => Err(IdentityError::Key(
                "the credential at this location is verifier-only".into(),
            )),
        }
    }
}

async fn generate() -> Result<Credential, IdentityError> {
    let signer = Ed25519Signer::generate()
        .await
        .map_err(|e| IdentityError::Key(e.to_string()))?;
    Ok(Credential::Signer(SignerCredential::from(signer)))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::Principal as _;
    use std::sync::atomic::{AtomicBool, Ordering};

    use dialog_effects::storage::{Create, Load, StorageError};
    use dialog_storage::provider::storage::Storage;

    struct FailingLoad {
        create_called: AtomicBool,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Load> for FailingLoad {
        async fn execute(&self, _input: Capability<Load>) -> Result<Credential, StorageError> {
            Err(StorageError::Storage("database unavailable".into()))
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Create> for FailingLoad {
        async fn execute(&self, _input: Capability<Create>) -> Result<Credential, StorageError> {
            self.create_called.store(true, Ordering::SeqCst);
            Err(StorageError::AlreadyExists("Profile/tonk".into()))
        }
    }

    #[dialog_common::test]
    async fn it_opens_a_credential() {
        let storage = Storage::volatile();

        let credential = OpenCredential::open("alice")
            .perform(&storage)
            .await
            .unwrap();
        assert!(!credential.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_opens_the_same_credential_twice() {
        let storage = Storage::volatile();

        let first = OpenCredential::open("bob").perform(&storage).await.unwrap();
        let second = OpenCredential::open("bob").perform(&storage).await.unwrap();

        assert_eq!(first.did(), second.did());
    }

    #[dialog_common::test]
    async fn it_does_not_create_after_a_storage_load_failure() {
        let storage = FailingLoad {
            create_called: AtomicBool::new(false),
        };

        let result = OpenCredential::open("tonk").perform(&storage).await;

        assert!(matches!(
            result,
            Err(IdentityError::Storage(message)) if message.contains("database unavailable")
        ));
        assert!(
            !storage.create_called.load(Ordering::SeqCst),
            "a backend failure must not fall through to credential creation"
        );
    }

    #[dialog_common::test]
    async fn it_creates_then_loads() {
        let storage = Storage::volatile();

        let created = OpenCredential::create("charlie")
            .perform(&storage)
            .await
            .unwrap();
        let loaded = OpenCredential::load("charlie")
            .perform(&storage)
            .await
            .unwrap();

        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_fails_to_create_a_duplicate() {
        let storage = Storage::volatile();

        OpenCredential::create("dave")
            .perform(&storage)
            .await
            .unwrap();
        let result = OpenCredential::create("dave").perform(&storage).await;

        assert!(matches!(result, Err(IdentityError::AlreadyExists)));
    }

    #[dialog_common::test]
    async fn it_fails_to_load_a_missing_credential() {
        let storage = Storage::volatile();

        let result = OpenCredential::load("missing").perform(&storage).await;
        assert!(matches!(result, Err(IdentityError::NotFound)));
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_across_directories() {
        let storage = Storage::volatile();

        let profile = OpenCredential::open("same-name")
            .at(Directory::Profile)
            .perform(&storage)
            .await
            .unwrap();
        let temp = OpenCredential::open("same-name")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        assert_ne!(
            profile.did(),
            temp.did(),
            "the same name in different directories names different credentials"
        );
    }

    #[dialog_common::test]
    async fn it_creates_and_loads_at_temp() {
        let storage = Storage::volatile();

        let created = OpenCredential::create("temp-load")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();
        let loaded = OpenCredential::load("temp-load")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_does_not_find_a_temp_credential_in_the_default_directory() {
        let storage = Storage::volatile();

        OpenCredential::create("only-in-temp")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        let result = OpenCredential::load("only-in-temp").perform(&storage).await;
        assert!(
            result.is_err(),
            "a credential created at temp is not found in the default directory"
        );
    }
}
