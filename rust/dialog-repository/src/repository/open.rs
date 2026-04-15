use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space as space_fx;

use super::Repository;
use super::error::RepositoryError;

/// Command to open (load-or-create) a repository.
///
/// Returns `Repository<Credential>` since the loaded credential
/// may be verifier-only.
pub struct OpenRepository(pub Capability<space_fx::Space>);

impl OpenRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, RepositoryError>
    where
        Env: Provider<space_fx::Load> + Provider<space_fx::Create> + ConditionalSync,
    {
        use dialog_effects::space::SpaceExt as _;

        let load_result = self.0.clone().load().perform(env).await;

        let credential = match load_result {
            Ok(cred) => cred,
            Err(_) => {
                let signer = dialog_credentials::Ed25519Signer::generate()
                    .await
                    .map_err(|e| RepositoryError::StorageError(e.to_string()))?;
                let cred = Credential::Signer(SignerCredential::from(signer));

                self.0.create(cred).perform(env).await?
            }
        };

        Ok(Repository::from(credential))
    }
}
