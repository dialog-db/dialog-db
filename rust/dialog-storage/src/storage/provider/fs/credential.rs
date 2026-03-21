//! Credential capability provider for filesystem.
//!
//! Stores credentials as JSON files under `{subject}/credentials/{address_id}`.

use super::{FileSystem, FileSystemError};
use async_trait::async_trait;
use dialog_capability::credential::{self, CredentialError};
use dialog_capability::{Capability, Policy, Provider};
use dialog_common::ConditionalSend;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::PathBuf;

impl From<FileSystemError> for CredentialError {
    fn from(e: FileSystemError) -> Self {
        CredentialError::NotFound(e.to_string())
    }
}

/// Resolve the filesystem path for a credential address.
fn credential_path(
    fs: &FileSystem,
    subject: &dialog_capability::Did,
    address_id: &str,
) -> Result<PathBuf, FileSystemError> {
    let location = fs.credentials(subject)?;
    let file_location = location.resolve(address_id)?;
    file_location.try_into()
}

#[async_trait]
impl<C> Provider<credential::Retrieve<C>> for FileSystem
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(
        &self,
        input: Capability<credential::Retrieve<C>>,
    ) -> Result<C, CredentialError> {
        let subject = input.subject().into();
        let address_id = credential::Retrieve::<C>::of(&input)
            .address
            .id()
            .to_string();

        let path = credential_path(self, &subject, &address_id)?;

        match tokio::fs::read(&path).await {
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|e| CredentialError::NotFound(format!("deserialization error: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(CredentialError::NotFound(
                format!("no credentials at '{address_id}'"),
            )),
            Err(e) => Err(CredentialError::NotFound(e.to_string())),
        }
    }
}

#[async_trait]
impl<C> Provider<credential::Save<C>> for FileSystem
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    async fn execute(&self, input: Capability<credential::Save<C>>) -> Result<(), CredentialError> {
        let subject = input.subject().into();
        let effect = credential::Save::<C>::of(&input);
        let address_id = effect.address.id().to_string();
        let value = serde_json::to_vec(&effect.credentials)
            .map_err(|e| CredentialError::NotFound(format!("serialization error: {e}")))?;

        let location = self.credentials(&subject)?;
        location.ensure_dir().await?;

        let path = credential_path(self, &subject, &address_id)?;

        // Ensure parent directory exists (address_id might contain path separators)
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
        }

        tokio::fs::write(&path, &value)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Did, Subject, credential};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestCredentials {
        access_key: String,
        secret_key: String,
    }

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

    fn build_retrieve_cap<C: Serialize + DeserializeOwned + ConditionalSend + 'static>(
        subject: Subject,
        address_id: &str,
    ) -> Capability<credential::Retrieve<C>> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Retrieve {
                address: credential::Address::new(address_id),
            })
    }

    fn build_save_cap<C: Serialize + DeserializeOwned + ConditionalSend + 'static>(
        subject: Subject,
        address_id: &str,
        credentials: C,
    ) -> Capability<credential::Save<C>> {
        subject
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Save {
                address: credential::Address::new(address_id),
                credentials,
            })
    }

    #[dialog_common::test]
    async fn it_returns_not_found_for_missing_credentials() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-missing");

        let result: Result<TestCredentials, _> =
            <FileSystem as Provider<credential::Retrieve<TestCredentials>>>::execute(
                &provider,
                build_retrieve_cap(subject, "s3://my-bucket"),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(CredentialError::NotFound(_))));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_credentials() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-roundtrip");
        let creds = TestCredentials {
            access_key: "AKIA123".to_string(),
            secret_key: "secret456".to_string(),
        };

        <FileSystem as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "s3://my-bucket", creds.clone()),
        )
        .await?;

        let result: TestCredentials = <FileSystem as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "s3://my-bucket")
        )
        .await?;

        assert_eq!(result, creds);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_credentials_by_address() -> anyhow::Result<()> {
        let (_dir, provider) = temp_filesystem();
        let subject = unique_subject("fs-cred-isolation");

        let creds1 = TestCredentials {
            access_key: "key1".to_string(),
            secret_key: "secret1".to_string(),
        };
        let creds2 = TestCredentials {
            access_key: "key2".to_string(),
            secret_key: "secret2".to_string(),
        };

        <FileSystem as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-a", creds1.clone()),
        )
        .await?;
        <FileSystem as Provider<credential::Save<TestCredentials>>>::execute(
            &provider,
            build_save_cap(subject.clone(), "bucket-b", creds2.clone()),
        )
        .await?;

        let result1: TestCredentials = <FileSystem as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject.clone(), "bucket-a")
        )
        .await?;
        let result2: TestCredentials = <FileSystem as Provider<
            credential::Retrieve<TestCredentials>,
        >>::execute(
            &provider, build_retrieve_cap(subject, "bucket-b")
        )
        .await?;

        assert_eq!(result1, creds1);
        assert_eq!(result2, creds2);

        Ok(())
    }
}
