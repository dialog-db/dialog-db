//! Credential Load/Save for filesystem storage.
//!
//! Layout: `{space_root}/credential/{address}`

use dialog_capability::{Capability, Policy, Provider};
use dialog_credentials::{Credential, CredentialExport};
use dialog_effects::credential::{self, CredentialError};

use super::{FileSystem, FileSystemError, FileSystemHandle};

const CREDENTIAL: &str = "credential";

impl FileSystem {
    /// Returns the handle for a credential at the given address.
    pub(super) fn credential(&self, address: &str) -> Result<FileSystemHandle, FileSystemError> {
        self.resolve(CREDENTIAL)?.resolve(address)
    }
}

#[async_trait::async_trait]
impl Provider<credential::Load> for FileSystem {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Credential, CredentialError> {
        let address = &credential::Name::of(&input).name;
        let handle = self.credential(address)?;
        let data = handle.read().await?;
        let export = CredentialExport::try_from(data)
            .map_err(|e| CredentialError::Corrupted(e.to_string()))?;

        Credential::import(export)
            .await
            .map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}

#[async_trait::async_trait]
impl Provider<credential::Save> for FileSystem {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let name = &credential::Name::of(&input).name;
        let cred = &credential::Save::of(&input).credential;
        let handle = self.credential(name)?;
        let export = cred
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        handle.write(export.as_bytes()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::Resource;
    use dialog_capability::Did;
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_effects::prelude::*;
    use dialog_effects::storage::{Directory, Location as StorageLocation};
    use dialog_varsig::Principal;

    fn unique_name(prefix: &str) -> String {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{ts}-{seq}")
    }

    async fn unique_did() -> Did {
        let signer = Ed25519Signer::generate().await.unwrap();
        Principal::did(&signer)
    }

    async fn test_credential() -> dialog_credentials::Credential {
        let signer = Ed25519Signer::generate().await.unwrap();
        dialog_credentials::Credential::Signer(SignerCredential::from(signer))
    }

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credential() -> anyhow::Result<()> {
        let location = StorageLocation::new(Directory::Temp, unique_name("fs-cred-not-found"));
        let provider = FileSystem::open(&location).await?;
        let did = unique_did().await;

        let result = did.credential("self").load().perform(&provider).await;

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
            .credential("self")
            .save(cred)
            .perform(&provider)
            .await?;

        // Load
        let loaded = did.credential("self").load().perform(&provider).await?;

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
            .credential("self")
            .save(cred1)
            .perform(&provider)
            .await?;

        // Save second credential at same address
        did.clone()
            .credential("self")
            .save(cred2)
            .perform(&provider)
            .await?;

        // Load should return second credential
        let loaded = did.credential("self").load().perform(&provider).await?;

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
            .credential("addr1")
            .save(cred1)
            .perform(&provider)
            .await?;

        did.clone()
            .credential("addr2")
            .save(cred2)
            .perform(&provider)
            .await?;

        // Load from each address
        let loaded1 = did
            .clone()
            .credential("addr1")
            .load()
            .perform(&provider)
            .await?;

        let loaded2 = did.credential("addr2").load().perform(&provider).await?;

        assert_eq!(loaded1.did(), expected_did1);
        assert_eq!(loaded2.did(), expected_did2);
        Ok(())
    }
}
