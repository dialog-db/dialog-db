//! Credential Load/Save for filesystem storage.
//!
//! Layout: `{space_root}/credential/{address}`

use super::{FileSystem, FileSystemError, FileSystemHandle};
use base58::ToBase58;
use dialog_capability::{Capability, Provider};
use dialog_credentials::{Credential, CredentialExport};
use dialog_effects::credential::prelude::{
    LoadCredentialExt, LoadGrantExt, LoadSecretExt, SaveCredentialExt, SaveGrantExt, SaveSecretExt,
};
use dialog_effects::credential::{CredentialError, Grant, Load, Save, Secret};

const CREDENTIAL: &str = "credential";
const KEY: &str = "key";
const SITE: &str = "site";
const GRANT: &str = "grant";

impl FileSystem {
    /// Returns the handle for a key credential at the given address.
    /// Layout: `{space_root}/credential/key/{address}`
    pub fn credential_key(&self, address: &str) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(CREDENTIAL)?.resolve(KEY)?.resolve(address)
    }

    /// Returns the handle for a site secret at the given address.
    /// Layout: `{space_root}/credential/site/{hash(address)}`
    pub fn credential_site(&self, address: &str) -> Result<FileSystemHandle, FileSystemError> {
        let key = blake3::hash(address.as_bytes()).as_bytes().to_base58();
        self.resolve(CREDENTIAL)?.resolve(SITE)?.resolve(&key)
    }

    /// Returns the handle for a directory grant at the given site address.
    /// Layout: `{space_root}/credential/grant/{hash(address)}`. A separate
    /// namespace from `site` so a grant and an opaque secret for the same
    /// site don't collide.
    pub fn credential_grant(&self, address: &str) -> Result<FileSystemHandle, FileSystemError> {
        let key = blake3::hash(address.as_bytes()).as_bytes().to_base58();
        self.resolve(CREDENTIAL)?.resolve(GRANT)?.resolve(&key)
    }
}

#[async_trait::async_trait]
impl Provider<Load<Credential>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let handle = self.credential_key(input.address())?;
        let data = handle.read().await?;
        let export = CredentialExport::try_from(data)
            .map_err(|e| CredentialError::Corrupted(e.to_string()))?;

        Credential::import(export)
            .await
            .map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}

#[async_trait::async_trait]
impl Provider<Save<Credential>> for FileSystem {
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), CredentialError> {
        let handle = self.credential_key(input.address())?;
        let export = input
            .credential()
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        handle.write(export.as_bytes()).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Provider<Load<Secret>> for FileSystem {
    async fn execute(&self, input: Capability<Load<Secret>>) -> Result<Secret, CredentialError> {
        let handle = self.credential_site(input.address().as_str())?;
        let data = handle.read().await?;
        Ok(Secret::from(data))
    }
}

#[async_trait::async_trait]
impl Provider<Save<Secret>> for FileSystem {
    async fn execute(&self, input: Capability<Save<Secret>>) -> Result<(), CredentialError> {
        let handle = self.credential_site(input.address().as_str())?;
        handle.write(input.secret().as_bytes()).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Provider<Load<Grant>> for FileSystem {
    async fn execute(&self, input: Capability<Load<Grant>>) -> Result<Grant, CredentialError> {
        let handle = self.credential_grant(input.address().as_str())?;
        let data = handle.read().await?;
        let path = String::from_utf8(data)
            .map_err(|e| CredentialError::Corrupted(format!("grant path is not UTF-8: {e}")))?;
        Ok(Grant::path(path))
    }
}

#[async_trait::async_trait]
impl Provider<Save<Grant>> for FileSystem {
    async fn execute(&self, input: Capability<Save<Grant>>) -> Result<(), CredentialError> {
        let handle = self.credential_grant(input.address().as_str())?;
        handle.write(input.grant().as_path().as_bytes()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{test_credential, unique_did, unique_name};
    use crate::resource::Resource;
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};
    use dialog_varsig::Principal;

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credential() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-cred-not-found"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        let result = did.credential().key("self").load().perform(&provider).await;

        assert!(result.is_err());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_saves_and_loads_credential() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-cred-save-load"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let cred = test_credential().await;
        let expected_did = cred.did();

        // Save
        did.clone()
            .credential()
            .key("self")
            .save(cred)
            .perform(&provider)
            .await?;

        // Load
        let loaded = did
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;

        assert_eq!(loaded.did(), expected_did);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_overwrites_credential_at_same_address() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-cred-overwrite"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        let cred1 = test_credential().await;
        let cred2 = test_credential().await;
        let expected_did = cred2.did();

        // Save first credential
        did.clone()
            .credential()
            .key("self")
            .save(cred1)
            .perform(&provider)
            .await?;

        // Save second credential at same address
        did.clone()
            .credential()
            .key("self")
            .save(cred2)
            .perform(&provider)
            .await?;

        // Load should return second credential
        let loaded = did
            .credential()
            .key("self")
            .load()
            .perform(&provider)
            .await?;

        assert_eq!(loaded.did(), expected_did);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_address() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-cred-isolate"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        let cred1 = test_credential().await;
        let cred2 = test_credential().await;
        let expected_did1 = cred1.did();
        let expected_did2 = cred2.did();

        // Save to different addresses
        did.clone()
            .credential()
            .key("addr1")
            .save(cred1)
            .perform(&provider)
            .await?;

        did.clone()
            .credential()
            .key("addr2")
            .save(cred2)
            .perform(&provider)
            .await?;

        // Load from each address
        let loaded1 = did
            .clone()
            .credential()
            .key("addr1")
            .load()
            .perform(&provider)
            .await?;

        let loaded2 = did
            .credential()
            .key("addr2")
            .load()
            .perform(&provider)
            .await?;

        assert_eq!(loaded1.did(), expected_did1);
        assert_eq!(loaded2.did(), expected_did2);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_saves_and_loads_a_directory_grant() -> anyhow::Result<()> {
        use dialog_capability::SiteId;
        use dialog_effects::credential::Grant;

        let location = StorageLocation::new(Directory::Temp, unique_name("fs-grant-save-load"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;
        let site = SiteId::from("did:key:zVault");

        did.clone()
            .credential()
            .site(site.clone())
            .save_grant(Grant::path("/path/to/vault"))
            .perform(&provider)
            .await?;

        let loaded = did
            .credential()
            .site(site)
            .load_grant()
            .perform(&provider)
            .await?;

        assert_eq!(loaded.as_path(), "/path/to/vault");
        Ok(())
    }
}
