use std::fmt::Display;

use super::Storage;
use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::repository::{
    Credential, CredentialExport, Load, LoadCapability, RepositoryError, Save, SaveCapability,
};
use dialog_storage::provider::FileSystem;

fn to_err(e: impl Display) -> RepositoryError {
    RepositoryError::Storage(e.to_string())
}

#[async_trait]
impl Provider<Load> for Storage<'_, FileSystem> {
    async fn execute(
        &self,
        input: Capability<Load>,
    ) -> Result<Option<Credential>, RepositoryError> {
        let name = input.name();
        let location = self
            .resolve(name)
            .and_then(|loc| loc.resolve("credentials")?.resolve("self"))
            .map_err(to_err)?;

        match location.read().await {
            Ok(data) => {
                let export = CredentialExport::try_from(data)
                    .map_err(|e| RepositoryError::Corrupted(e.to_string()))?;
                let credential = Credential::import(export)
                    .await
                    .map_err(|e| RepositoryError::Corrupted(e.to_string()))?;
                Ok(Some(credential))
            }
            Err(_) => Ok(None),
        }
    }
}

#[async_trait]
impl Provider<Save> for Storage<'_, FileSystem> {
    async fn execute(&self, input: Capability<Save>) -> Result<(), RepositoryError> {
        let name = input.name();
        let credential = input.credential();
        let location = self
            .resolve(name)
            .and_then(|loc| loc.resolve("credentials")?.resolve("self"))
            .map_err(to_err)?;

        let export = credential.export().await.map_err(to_err)?;
        location.write(export.as_bytes()).await.map_err(to_err)
    }
}
