//! Profile — a named identity backed by a signing credential.

pub mod access;
mod error;
mod open;
mod save;
mod space;

pub use error::ProfileError;
pub use open::OpenProfile;
pub use save::SaveDelegation;
pub use space::SpaceHandle;

use crate::operator::OperatorBuilder;
use base58::ToBase58;
use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_credentials::SignerCredential;
use dialog_effects::credential::prelude::*;
use dialog_effects::credential::{self, CredentialError, Secret};
use dialog_ucan::UcanDelegation;
use dialog_varsig::{Did, Principal};
use std::marker::PhantomData;

/// An opened profile — holds a signing credential.
#[derive(Debug, Clone)]
pub struct Profile {
    credential: SignerCredential,
}

impl Profile {
    /// Open a profile — loads existing or creates new.
    pub fn open(name: impl Into<String>) -> OpenProfile {
        OpenProfile::open(name.into())
    }

    /// Load an existing profile — fails if not found.
    pub fn load(name: impl Into<String>) -> OpenProfile {
        OpenProfile::load(name.into())
    }

    /// Create a new profile — fails if one already exists.
    pub fn create(name: impl Into<String>) -> OpenProfile {
        OpenProfile::create(name.into())
    }

    /// The profile's DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The signing credential.
    pub fn signer(&self) -> &SignerCredential {
        &self.credential
    }

    /// Get a credential handle for saving/loading site secrets.
    pub fn credential(&self) -> CredentialHandle {
        CredentialHandle { did: self.did() }
    }

    /// Store a delegation chain under this profile's DID.
    pub fn save(&self, chain: UcanDelegation) -> SaveDelegation {
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

    /// Get a handle to a named repository space under this profile.
    ///
    /// The returned handle can open, load, or create a repository
    /// through an operator that verifies the profile DID.
    pub fn repository(&self, name: impl Into<String>) -> SpaceHandle {
        SpaceHandle {
            profile_did: self.did(),
            name: name.into(),
        }
    }
}

/// Handle for credential operations scoped to a profile's DID.
///
/// Created via [`Profile::credential()`].
pub struct CredentialHandle {
    did: Did,
}

impl CredentialHandle {
    /// Select a site credential by address.
    ///
    /// The address is serialized and blake3-hashed to derive the credential
    /// store key, matching the Operator's fork authorization lookup.
    pub fn site(self, address: &impl serde::Serialize) -> CredentialSite {
        let bytes = serde_ipld_dagcbor::to_vec(address).expect("address must be serializable");
        let key = blake3::hash(&bytes).as_bytes().to_base58();
        CredentialSite { did: self.did, key }
    }
}

/// A site credential handle ready for save/load operations.
///
/// Created via [`CredentialHandle::site()`].
pub struct CredentialSite {
    did: Did,
    key: String,
}

impl CredentialSite {
    /// Save a site credential to the credential store.
    ///
    /// The credential is converted to [`Secret`] via [`TryInto`] during
    /// [`perform()`](SaveSiteCredential::perform).
    pub fn save<T: TryInto<Secret>>(self, credential: T) -> SaveSiteCredential<T> {
        SaveSiteCredential {
            did: self.did,
            key: self.key,
            credential,
        }
    }

    /// Load a site credential from the credential store.
    ///
    /// The loaded [`Secret`] is converted to `T` via [`TryFrom`] during
    /// [`perform()`](LoadSiteCredential::perform).
    pub fn load<T: TryFrom<Secret>>(self) -> LoadSiteCredential<T> {
        LoadSiteCredential {
            did: self.did,
            key: self.key,
            _marker: PhantomData,
        }
    }
}

/// Saves a site credential. Created via [`CredentialSite::save()`].
pub struct SaveSiteCredential<T> {
    did: Did,
    key: String,
    credential: T,
}

impl<T> SaveSiteCredential<T>
where
    T: TryInto<Secret>,
    T::Error: Into<CredentialError>,
{
    /// Serialize the credential and save it to the store.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), CredentialError>
    where
        Env: Provider<credential::Save<Secret>> + ConditionalSync,
    {
        let secret = self.credential.try_into().map_err(Into::into)?;
        self.did
            .credential()
            .site(&self.key)
            .save(secret)
            .perform(env)
            .await
    }
}

/// Loads a site credential. Created via [`CredentialSite::load()`].
pub struct LoadSiteCredential<T> {
    did: Did,
    key: String,
    _marker: PhantomData<T>,
}

impl<T> LoadSiteCredential<T>
where
    T: TryFrom<Secret>,
    T::Error: Into<CredentialError>,
{
    /// Load the credential from the store and deserialize it.
    pub async fn perform<Env>(self, env: &Env) -> Result<T, CredentialError>
    where
        Env: Provider<credential::Load<Secret>> + ConditionalSync,
    {
        let secret = self
            .did
            .credential()
            .site(&self.key)
            .load()
            .perform(env)
            .await?;
        secret.try_into().map_err(Into::into)
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

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_storage::provider::storage::Storage;

    #[dialog_common::test]
    async fn it_opens_profile() {
        let storage = Storage::volatile();

        let profile = Profile::open("alice").perform(&storage).await.unwrap();
        assert!(!profile.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_opens_same_profile_twice() {
        let storage = Storage::volatile();

        let first = Profile::open("bob").perform(&storage).await.unwrap();
        let second = Profile::open("bob").perform(&storage).await.unwrap();

        assert_eq!(first.did(), second.did());
    }

    #[dialog_common::test]
    async fn it_creates_then_loads() {
        let storage = Storage::volatile();

        let created = Profile::create("charlie").perform(&storage).await.unwrap();
        let loaded = Profile::load("charlie").perform(&storage).await.unwrap();

        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_fails_to_create_duplicate() {
        let storage = Storage::volatile();

        Profile::create("dave").perform(&storage).await.unwrap();
        let result = Profile::create("dave").perform(&storage).await;

        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_fails_to_load_missing() {
        let storage = Storage::volatile();

        let result = Profile::load("missing").perform(&storage).await;
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn it_opens_profile_at_temp() {
        use dialog_effects::storage::Directory;

        let storage = Storage::volatile();

        let profile = Profile::open("temp-alice")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        assert!(!profile.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn it_isolates_profiles_across_directories() {
        use dialog_effects::storage::Directory;

        let storage = Storage::volatile();

        let profile = Profile::open("same-name")
            .at(Directory::Profile)
            .perform(&storage)
            .await
            .unwrap();

        let temp = Profile::open("same-name")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        assert_ne!(
            profile.did(),
            temp.did(),
            "same name in different directories should produce different profiles"
        );
    }

    #[dialog_common::test]
    async fn it_creates_and_loads_at_temp() {
        use dialog_effects::storage::Directory;

        let storage = Storage::volatile();

        let created = Profile::create("temp-load")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        let loaded = Profile::load("temp-load")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_does_not_find_temp_profile_in_default_directory() {
        use dialog_effects::storage::Directory;

        let storage = Storage::volatile();

        Profile::create("only-in-temp")
            .at(Directory::Temp)
            .perform(&storage)
            .await
            .unwrap();

        let result = Profile::load("only-in-temp").perform(&storage).await;
        assert!(
            result.is_err(),
            "profile created at temp should not be found in default directory"
        );
    }
}
