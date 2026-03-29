//! Profile — a named identity backed by a signing credential.

use crate::operator::OperatorBuilder;
use crate::storage::LocationExt;
use dialog_capability::storage::{Load, Location, Mount, Save, Storage as StorageCap};
use dialog_capability::{Capability, Policy, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_storage::provider::Address;
use dialog_varsig::{Did, Principal};

/// An opened profile — holds a signing credential and knows where it lives.
#[derive(Debug, Clone)]
pub struct Profile {
    credential: SignerCredential,
    location: Capability<Location<Address>>,
}

impl Profile {
    /// Open a profile — loads existing or creates new.
    pub fn open(location: Capability<Location<Address>>) -> OpenProfile {
        OpenProfile {
            location,
            mode: OpenMode::OpenOrCreate,
        }
    }

    /// Load an existing profile — fails if not found.
    pub fn load(location: Capability<Location<Address>>) -> OpenProfile {
        OpenProfile {
            location,
            mode: OpenMode::Load,
        }
    }

    /// Create a new profile — fails if one already exists.
    pub fn create(location: Capability<Location<Address>>) -> OpenProfile {
        OpenProfile {
            location,
            mode: OpenMode::Create,
        }
    }

    /// The profile's DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The signing credential.
    pub fn credential(&self) -> &SignerCredential {
        &self.credential
    }

    /// The storage location capability for this profile.
    pub fn location(&self) -> &Capability<Location<Address>> {
        &self.location
    }

    /// Start building an operator from this profile.
    pub fn operator(&self, context: impl Into<Vec<u8>>) -> OperatorBuilder {
        OperatorBuilder::new(self, context.into())
    }
}

impl Principal for Profile {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

enum OpenMode {
    OpenOrCreate,
    Load,
    Create,
}

/// Command to open, load, or create a profile.
pub struct OpenProfile {
    location: Capability<Location<Address>>,
    mode: OpenMode,
}

impl OpenProfile {
    /// Execute against storage.
    ///
    /// Reads credentials from `{location}/credential/profile`.
    /// Mounts the profile DID at `{location}` in the storage store table.
    pub async fn perform<S>(self, storage: &S) -> Result<Profile, ProfileError>
    where
        S: Provider<Load<Credential, Address>>
            + Provider<Save<Credential, Address>>
            + Provider<Mount<Address>>
            + ConditionalSync,
    {
        let location = self.location;

        let cred_location = location
            .resolve("credential/profile")
            .map_err(|e| ProfileError::Storage(e.to_string()))?;

        let credential = match self.mode {
            OpenMode::Load => {
                let cred = cred_location
                    .load::<Credential>()
                    .perform(storage)
                    .await
                    .map_err(|e| ProfileError::Storage(e.to_string()))?;

                match cred {
                    Credential::Signer(signer) => signer,
                    Credential::Verifier(_) => {
                        return Err(ProfileError::Key(
                            "profile credential is verifier-only".into(),
                        ));
                    }
                }
            }
            OpenMode::Create => {
                let existing = cred_location
                    .clone()
                    .load::<Credential>()
                    .perform(storage)
                    .await;

                if existing.is_ok() {
                    return Err(ProfileError::AlreadyExists);
                }

                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| ProfileError::Key(e.to_string()))?;
                let credential = SignerCredential::from(signer);

                cred_location
                    .save(Credential::Signer(credential.clone()))
                    .perform(storage)
                    .await
                    .map_err(|e| ProfileError::Storage(e.to_string()))?;

                credential
            }
            OpenMode::OpenOrCreate => {
                let load = cred_location
                    .clone()
                    .load::<Credential>()
                    .perform(storage)
                    .await;

                match load {
                    Ok(cred) => match cred {
                        Credential::Signer(signer) => signer,
                        Credential::Verifier(_) => {
                            return Err(ProfileError::Key(
                                "profile credential is verifier-only".into(),
                            ));
                        }
                    },
                    Err(_) => {
                        let signer = Ed25519Signer::generate()
                            .await
                            .map_err(|e| ProfileError::Key(e.to_string()))?;
                        let credential = SignerCredential::from(signer);

                        cred_location
                            .save(Credential::Signer(credential.clone()))
                            .perform(storage)
                            .await
                            .map_err(|e| ProfileError::Storage(e.to_string()))?;

                        credential
                    }
                }
            }
        };

        // Mount the profile DID at the root location
        let address = Location::of(&location).address().clone();
        StorageCap::mount(credential.did(), address)
            .perform(storage)
            .await
            .map_err(|e| ProfileError::Storage(e.to_string()))?;

        Ok(Profile {
            credential,
            location,
        })
    }
}

/// Errors that can occur when opening a profile.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// Storage operation failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key generation or import failed.
    #[error("Key error: {0}")]
    Key(String),

    /// Profile already exists (for create).
    #[error("Profile already exists")]
    AlreadyExists,

    /// Profile not found (for load).
    #[error("Profile not found")]
    NotFound,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;

    fn unique_location(prefix: &str) -> Capability<Location<Address>> {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        Storage::temp(&format!("{prefix}-{id}-{seq}"))
    }

    #[dialog_common::test]
    async fn create_then_load_mounts_did() {
        let storage = Storage::temp_storage();
        let location = unique_location("create-load");

        let created = Profile::create(location.clone())
            .perform(&storage)
            .await
            .unwrap();
        assert!(storage.stores().contains(&created.did()));

        let loaded = Profile::load(location).perform(&storage).await.unwrap();
        assert_eq!(created.did(), loaded.did());
        assert!(storage.stores().contains(&loaded.did()));
    }

    #[dialog_common::test]
    async fn open_creates_when_missing() {
        let storage = Storage::temp_storage();

        let profile = Profile::open(unique_location("open-create"))
            .perform(&storage)
            .await
            .unwrap();

        assert!(!profile.did().to_string().is_empty());
        assert!(storage.stores().contains(&profile.did()));
    }

    #[dialog_common::test]
    async fn open_loads_when_existing() {
        let storage = Storage::temp_storage();
        let location = unique_location("open-load");

        let first = Profile::open(location.clone())
            .perform(&storage)
            .await
            .unwrap();

        let second = Profile::open(location).perform(&storage).await.unwrap();

        assert_eq!(first.did(), second.did());
        assert!(storage.stores().contains(&second.did()));
    }

    #[dialog_common::test]
    async fn create_fails_when_existing() {
        let storage = Storage::temp_storage();
        let location = unique_location("create-dup");

        Profile::create(location.clone())
            .perform(&storage)
            .await
            .unwrap();

        let result = Profile::create(location).perform(&storage).await;
        assert!(
            matches!(result, Err(ProfileError::AlreadyExists)),
            "creating an existing profile should fail"
        );
    }

    #[dialog_common::test]
    async fn load_fails_when_missing() {
        let storage = Storage::temp_storage();

        let result = Profile::load(unique_location("load-missing"))
            .perform(&storage)
            .await;

        assert!(result.is_err());
    }
}
