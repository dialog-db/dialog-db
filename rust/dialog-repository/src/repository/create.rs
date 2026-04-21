use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space;
use dialog_effects::space::SpaceExt;

use super::Repository;
use crate::CreateRepositoryError;

/// Command to create a new repository.
///
/// Returns `Repository<SignerCredential>` since a freshly generated
/// credential always has a private key.
pub struct CreateRepository(pub Capability<space::Space>);

impl CreateRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, CreateRepositoryError>
    where
        Env: Provider<space::Create> + ConditionalSync,
    {
        let signer = SignerCredential::from(Ed25519Signer::generate().await?);
        self.0
            .create(Credential::Signer(signer.clone()))
            .perform(env)
            .await?;
        Ok(Repository::from(signer))
    }
}
