//! Site secrets stored under a DID: remote credentials, handoff material
//! and the like, until sealed facts replace them.

use dialog_capability::{Provider, SiteId};
use dialog_common::ConditionalSync;
use dialog_effects::credential::prelude::*;
use dialog_effects::credential::{self, CredentialError, Secret};
use dialog_varsig::Did;
use std::marker::PhantomData;

/// Handle for site-secret operations scoped to one DID.
///
/// Created via `Peer::secrets` in `dialog-peer`.
pub struct CredentialHandle {
    did: Did,
}

impl CredentialHandle {
    /// Site secrets stored under `did`.
    pub fn new(did: Did) -> Self {
        Self { did }
    }

    /// Select a site credential by address identifier.
    pub fn site(self, id: impl Into<SiteId>) -> CredentialSite {
        CredentialSite {
            did: self.did,
            key: id.into(),
        }
    }
}

/// A site credential handle ready for save/load operations.
///
/// Created via [`CredentialHandle::site()`].
pub struct CredentialSite {
    did: Did,
    key: SiteId,
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

    /// Remove a site credential from the credential store.
    ///
    /// Idempotent: retracting an address that holds nothing succeeds.
    pub fn retract(self) -> RetractSiteCredential {
        RetractSiteCredential {
            did: self.did,
            key: self.key,
        }
    }
}

/// Saves a site credential. Created via [`CredentialSite::save()`].
pub struct SaveSiteCredential<T> {
    did: Did,
    key: SiteId,
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
    key: SiteId,
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

/// Removes a site credential. Created via [`CredentialSite::retract()`].
pub struct RetractSiteCredential {
    did: Did,
    key: SiteId,
}

impl RetractSiteCredential {
    /// Remove the credential from the store.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), CredentialError>
    where
        Env: Provider<credential::Retract<Secret>> + ConditionalSync,
    {
        self.did
            .credential()
            .site(&self.key)
            .retract()
            .perform(env)
            .await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_varsig::Principal as _;
    use crate::OpenCredential;
    use dialog_storage::provider::storage::Storage;

    #[dialog_common::test]
    async fn it_saves_retracts_and_reloads_a_site_secret() {
        let storage = Storage::volatile();
        let credential = OpenCredential::open("site-retract")
            .perform(&storage)
            .await
            .unwrap();
        let secrets = || CredentialHandle::new(credential.did());

        secrets()
            .site("example.com")
            .save(Secret::from(vec![1u8, 2, 3]))
            .perform(&storage)
            .await
            .unwrap();

        secrets()
            .site("example.com")
            .retract()
            .perform(&storage)
            .await
            .unwrap();

        let result = secrets()
            .site("example.com")
            .load::<Secret>()
            .perform(&storage)
            .await;

        assert!(matches!(result, Err(CredentialError::NotFound(_))));
    }
}
