use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::{self as storage_fx, Directory, Location, LocationExt};

use super::{Profile, ProfileError};

enum OpenMode {
    OpenOrCreate,
    Load,
    Create,
}

/// Command to open, load, or create a profile.
pub struct OpenProfile {
    name: String,
    directory: Directory,
    mode: OpenMode,
}

impl OpenProfile {
    pub(super) fn open(name: String) -> Self {
        Self {
            name,
            directory: Directory::Profile,
            mode: OpenMode::OpenOrCreate,
        }
    }

    pub(super) fn load(name: String) -> Self {
        Self {
            name,
            directory: Directory::Profile,
            mode: OpenMode::Load,
        }
    }

    pub(super) fn create(name: String) -> Self {
        Self {
            name,
            directory: Directory::Profile,
            mode: OpenMode::Create,
        }
    }

    /// Set the directory for this profile.
    ///
    /// Defaults to `Directory::Profile` (the platform profile directory).
    /// Use `Directory::Temp` for testing or ephemeral profiles.
    pub fn at(mut self, directory: Directory) -> Self {
        self.directory = directory;
        self
    }

    fn location(&self) -> dialog_capability::Capability<Location> {
        dialog_capability::Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(Location::new(self.directory.clone(), &self.name))
    }

    /// Execute against a storage provider.
    pub async fn perform<Env>(self, env: &Env) -> Result<Profile, ProfileError>
    where
        Env: Provider<storage_fx::Load> + Provider<storage_fx::Create> + ConditionalSync,
    {
        let credential = match self.mode {
            OpenMode::Load => self
                .location()
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

                self.location()
                    .create(credential)
                    .perform(env)
                    .await
                    .map_err(|e| ProfileError::Storage(e.to_string()))?
            }
            OpenMode::OpenOrCreate => {
                let load_result = self.location().load().perform(env).await;

                match load_result {
                    Ok(cred) => cred,
                    Err(_) => {
                        let signer = Ed25519Signer::generate()
                            .await
                            .map_err(|e| ProfileError::Key(e.to_string()))?;
                        let credential =
                            dialog_credentials::Credential::Signer(SignerCredential::from(signer));

                        self.location()
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
