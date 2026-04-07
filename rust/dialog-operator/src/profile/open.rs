use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::{self as storage_fx, LocationExt};

use super::{Profile, ProfileError};

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
    pub(super) fn open(name: String) -> Self {
        Self {
            name,
            mode: OpenMode::OpenOrCreate,
        }
    }

    pub(super) fn load(name: String) -> Self {
        Self {
            name,
            mode: OpenMode::Load,
        }
    }

    pub(super) fn create(name: String) -> Self {
        Self {
            name,
            mode: OpenMode::Create,
        }
    }

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
