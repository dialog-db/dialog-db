//! Credential capability provider for filesystem.
//!
//! Stores credentials as exported bytes under `{subject}/credentials/{name}`.

use super::{FileSystem, FileSystemError};
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::credential::{
    self, CredentialError, CredentialExport, Identity, LoadCapability, SaveCapability,
};

impl From<FileSystemError> for CredentialError {
    fn from(e: FileSystemError) -> Self {
        CredentialError::Storage(e.to_string())
    }
}

#[async_trait]
impl Provider<credential::Load> for FileSystem {
    async fn execute(
        &self,
        input: Capability<credential::Load>,
    ) -> Result<Option<Identity>, CredentialError> {
        let subject = input.subject().into();
        let name = input.name();

        let location = self
            .credentials(&subject)?
            .resolve(name)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        match location.read().await {
            Ok(data) => {
                let export = CredentialExport::try_from(data)
                    .map_err(|e| CredentialError::Corrupted(e.to_string()))?;
                let credential = Identity::import(export)
                    .await
                    .map_err(|e| CredentialError::Corrupted(e.to_string()))?;
                Ok(Some(credential))
            }
            Err(_) => Ok(None),
        }
    }
}

#[async_trait]
impl Provider<credential::Save> for FileSystem {
    async fn execute(&self, input: Capability<credential::Save>) -> Result<(), CredentialError> {
        let subject = input.subject().into();
        let name = input.name();
        let credential = input.credential();

        let location = self
            .credentials(&subject)?
            .resolve(name)
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        let export = credential
            .export()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        location
            .write(export.as_bytes())
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject};
    use dialog_effects::credential;
    use dialog_varsig::Principal;

    fn temp_filesystem() -> (tempfile::TempDir, FileSystem) {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileSystem::mount(dir.path().to_path_buf()).unwrap();
        (dir, fs)
    }

    fn unique_subject(prefix: &str) -> Subject {
        let did: Did = format!(
            "did:test:{}-{}",
            prefix,
            dialog_common::time::now()
                .duration_since(dialog_common::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
        .parse()
        .unwrap();
        Subject::from(did)
    }

    fn build_load_cap(subject: Subject, name: &str) -> Capability<credential::Load> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Load)
    }

    fn build_save_cap(
        subject: Subject,
        name: &str,
        credential: Identity,
    ) -> Capability<credential::Save> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Name::new(name))
            .invoke(credential::Save::new(credential))
    }

    async fn make_signer_credential() -> Identity {
        let signer = dialog_credentials::Ed25519Signer::generate().await.unwrap();
        Identity::from(signer)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_credentials() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-missing");

        let result = <FileSystem as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "s3-bucket"),
        )
        .await?;

        assert!(result.is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_credentials() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-roundtrip");
        let cred = make_signer_credential().await;
        let expected_did = cred.did();

        <FileSystem as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3-bucket", cred),
        )
        .await?;

        let result = <FileSystem as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "s3-bucket"),
        )
        .await?;

        let loaded = result.expect("credential should exist");
        assert_eq!(loaded.did(), expected_did);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_name() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-isolation");

        let cred_a = make_signer_credential().await;
        let cred_b = make_signer_credential().await;
        let did_a = cred_a.did();
        let did_b = cred_b.did();

        <FileSystem as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-a", cred_a),
        )
        .await?;
        <FileSystem as Provider<credential::Save>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-b", cred_b),
        )
        .await?;

        let result1 = <FileSystem as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject.clone(), "bucket-a"),
        )
        .await?;
        let result2 = <FileSystem as Provider<credential::Load>>::execute(
            &provider,
            build_load_cap(subject, "bucket-b"),
        )
        .await?;

        assert_eq!(result1.unwrap().did(), did_a);
        assert_eq!(result2.unwrap().did(), did_b);

        Ok(())
    }
}
