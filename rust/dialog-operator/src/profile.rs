//! Profile — a named identity backed by a signing credential.

pub mod access;

use crate::operator::OperatorBuilder;
use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::{self as storage_fx, LocationExt};
use dialog_ucan::DelegationChain;
use dialog_varsig::{Did, Principal};

/// An opened profile — holds a signing credential.
#[derive(Debug, Clone)]
pub struct Profile {
    credential: SignerCredential,
}

impl Profile {
    /// Open a profile — loads existing or creates new.
    pub fn open(name: impl Into<String>) -> OpenProfile {
        OpenProfile {
            name: name.into(),
            mode: OpenMode::OpenOrCreate,
        }
    }

    /// Load an existing profile — fails if not found.
    pub fn load(name: impl Into<String>) -> OpenProfile {
        OpenProfile {
            name: name.into(),
            mode: OpenMode::Load,
        }
    }

    /// Create a new profile — fails if one already exists.
    pub fn create(name: impl Into<String>) -> OpenProfile {
        OpenProfile {
            name: name.into(),
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

    /// Store a delegation chain under this profile's DID.
    pub fn save(&self, chain: DelegationChain) -> SaveDelegation {
        SaveDelegation {
            did: self.did(),
            chain,
        }
    }

    /// Get an access handle for claiming and delegating capabilities.
    pub fn access(&self) -> access::Access<'_> {
        access::Access::new(&self.credential)
    }

    /// Derive an operator from this profile with the given context seed.
    pub fn derive(&self, context: impl Into<Vec<u8>>) -> OperatorBuilder {
        OperatorBuilder::new(self, context.into())
    }
}

impl From<&Profile> for Capability<Subject> {
    fn from(p: &Profile) -> Self {
        Subject::from(p.credential.did()).into()
    }
}

impl Principal for Profile {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

impl TryFrom<dialog_credentials::Credential> for Profile {
    type Error = ProfileError;

    fn try_from(credential: dialog_credentials::Credential) -> Result<Self, ProfileError> {
        match credential {
            dialog_credentials::Credential::Signer(s) => Ok(Profile { credential: s }),
            dialog_credentials::Credential::Verifier(_) => Err(ProfileError::Key(
                "profile credential is verifier-only".into(),
            )),
        }
    }
}

/// Command to store a delegation chain under a profile's DID.
pub struct SaveDelegation {
    did: Did,
    chain: DelegationChain,
}

use dialog_capability::access::Save as AccessSave;
use dialog_capability_ucan::Ucan;
type SaveUcan = AccessSave<Ucan>;

impl SaveDelegation {
    /// Execute against the environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), ProfileError>
    where
        Env: Provider<SaveUcan> + ConditionalSync,
    {
        use dialog_capability::access::Permit;

        Subject::from(self.did)
            .attenuate(Permit)
            .invoke(dialog_capability::access::Save::<Ucan>::new(self.chain))
            .perform(env)
            .await
            .map_err(|e| ProfileError::Storage(e.to_string()))
    }
}

enum OpenMode {
    OpenOrCreate,
    Load,
    Create,
}

/// Command to open, load, or create a profile.
pub struct OpenProfile {
    name: String,
    mode: OpenMode,
}

impl OpenProfile {
    /// Execute against an environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<Profile, ProfileError>
    where
        Env: Provider<storage_fx::Load> + Provider<storage_fx::Create> + ConditionalSync,
    {
        let credential = match self.mode {
            OpenMode::Load => storage_fx::Storage::profile(&self.name)
                .load()
                .perform(env)
                .await
                .map_err(|e| ProfileError::Storage(e.to_string()))?,
            OpenMode::Create => {
                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| ProfileError::Key(e.to_string()))?;
                let credential =
                    dialog_credentials::Credential::Signer(SignerCredential::from(signer));

                storage_fx::Storage::profile(&self.name)
                    .create(credential)
                    .perform(env)
                    .await
                    .map_err(|e| ProfileError::Storage(e.to_string()))?
            }
            OpenMode::OpenOrCreate => {
                let load_result = storage_fx::Storage::profile(&self.name)
                    .load()
                    .perform(env)
                    .await;

                match load_result {
                    Ok(cred) => cred,
                    Err(_) => {
                        let signer = Ed25519Signer::generate()
                            .await
                            .map_err(|e| ProfileError::Key(e.to_string()))?;
                        let credential =
                            dialog_credentials::Credential::Signer(SignerCredential::from(signer));

                        storage_fx::Storage::profile(&self.name)
                            .create(credential)
                            .perform(env)
                            .await
                            .map_err(|e| ProfileError::Storage(e.to_string()))?
                    }
                }
            }
        };

        Profile::try_from(credential)
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
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_storage::provider::environment::Environment;
    use dialog_storage::provider::environment::VolatileSpace;

    type TestEnv = Environment<VolatileSpace>;

    #[dialog_common::test]
    async fn it_opens_profile() {
        let env = TestEnv::new();

        let profile = Profile::open("alice").perform(&env).await.unwrap();
        assert!(!profile.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_opens_same_profile_twice() {
        let env = TestEnv::new();

        let first = Profile::open("bob").perform(&env).await.unwrap();
        let second = Profile::open("bob").perform(&env).await.unwrap();

        assert_eq!(first.did(), second.did());
    }

    #[dialog_common::test]
    async fn it_creates_then_loads() {
        let env = TestEnv::new();

        let created = Profile::create("charlie").perform(&env).await.unwrap();
        let loaded = Profile::load("charlie").perform(&env).await.unwrap();

        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_fails_to_create_duplicate() {
        let env = TestEnv::new();

        Profile::create("dave").perform(&env).await.unwrap();
        let result = Profile::create("dave").perform(&env).await;

        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_fails_to_load_missing() {
        let env = TestEnv::new();

        let result = Profile::load("missing").perform(&env).await;
        assert!(result.is_err());
    }
}
