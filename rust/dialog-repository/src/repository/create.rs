use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space;
use dialog_effects::space::SpaceExt;

use super::Repository;
use super::error::RepositoryError;

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
    ) -> Result<Repository<SignerCredential>, RepositoryError>
    where
        Env: Provider<space::Create> + ConditionalSync,
    {
        let signer = Ed25519Signer::generate().await?;
        let credential = Credential::Signer(SignerCredential::from(signer));
        self.0.create(credential).perform(env).await?.try_into()
    }
}
