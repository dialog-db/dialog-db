//! Profile — a named identity backed by a signing credential.

use crate::operator::OperatorBuilder;
use dialog_capability::storage::{Load, Location, Save};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_storage::provider::{Address, Store};
use dialog_varsig::{Did, Principal};

/// An opened profile — holds a signing credential and knows where it lives.
#[derive(Debug, Clone)]
pub struct Profile {
    credential: SignerCredential,
    name: String,
    location: Capability<Location<Address>>,
}

impl Profile {
    /// Start building a profile open command.
    pub fn named(name: impl Into<String>) -> ProfileBuilder {
        ProfileBuilder { name: name.into() }
    }

    /// The profile name.
    pub fn name(&self) -> &str {
        &self.name
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
    pub fn operator(&self, store: Store, context: impl Into<Vec<u8>>) -> OperatorBuilder {
        OperatorBuilder::new(self, store, context.into())
    }
}

impl Principal for Profile {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

/// Builder for opening a profile.
pub struct ProfileBuilder {
    name: String,
}

impl ProfileBuilder {
    /// Set the storage location for this profile.
    pub fn open(self, location: Capability<Location<Address>>) -> OpenProfile {
        OpenProfile {
            name: self.name,
            location,
        }
    }
}

/// Command to open a profile — loads or creates the signing key.
pub struct OpenProfile {
    name: String,
    location: Capability<Location<Address>>,
}

impl OpenProfile {
    /// Execute against a storage provider.
    pub async fn perform<S>(self, storage: &S) -> Result<Profile, ProfileError>
    where
        S: Provider<Load<Credential, Address>>
            + Provider<Save<Credential, Address>>
            + ConditionalSync,
    {
        let location = self.location;

        let load = location.clone().load::<Credential>().perform(storage).await;

        let credential = match load {
            Ok(credential) => match credential {
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

                location
                    .clone()
                    .save(Credential::Signer(credential.clone()))
                    .perform(storage)
                    .await
                    .map_err(|e| ProfileError::Storage(e.to_string()))?;

                credential
            }
        };

        Ok(Profile {
            name: self.name,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::storage::Storage;
    use dialog_storage::provider::{FileSystem, fs};

    fn temp_location() -> Capability<Location<Address>> {
        use dialog_common::time;
        let id = format!(
            "dialog-{}",
            time::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let address = fs::Address::temp().resolve(&id).unwrap();
        Storage::locate(Address::FileSystem(address))
    }

    #[dialog_common::test]
    async fn it_opens_profile() {
        let location = temp_location();

        let profile = Profile::named("personal")
            .open(location)
            .perform(&FileSystem)
            .await
            .unwrap();

        assert!(!profile.did().to_string().is_empty());
        assert_eq!(profile.name(), "personal");
    }

    #[dialog_common::test]
    async fn it_reopens_same_profile() {
        let location = temp_location();

        let first = Profile::named("work")
            .open(location.clone())
            .perform(&FileSystem)
            .await
            .unwrap();
        let second = Profile::named("work")
            .open(location)
            .perform(&FileSystem)
            .await
            .unwrap();

        assert_eq!(first.did(), second.did());
    }
}
