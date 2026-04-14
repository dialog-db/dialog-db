use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space as space_fx;

use super::Repository;
use super::error::RepositoryError;

/// Command to create a new repository.
///
/// Returns `Repository<SignerCredential>` since a freshly generated
/// credential always has a private key.
pub struct CreateRepository(pub(super) Capability<space_fx::Space>);

impl CreateRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, RepositoryError>
    where
        Env: Provider<space_fx::Create> + ConditionalSync,
    {
        use dialog_effects::space::SpaceExt as _;

        let signer = dialog_credentials::Ed25519Signer::generate()
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;
        let cred = Credential::Signer(SignerCredential::from(signer));

        let credential = self.0.create(cred).perform(env).await?;
        Repository::try_from(credential)
    }
}
