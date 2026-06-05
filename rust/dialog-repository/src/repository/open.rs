use crate::{OpenRepositoryError, Repository};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space::{self, SpaceExt};

/// Command to open (load-or-create) a repository.
///
/// Returns `Repository<Credential>` since the loaded credential
/// may be verifier-only.
pub struct OpenRepository(pub Capability<space::Space>);

impl OpenRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, OpenRepositoryError>
    where
        Env: Provider<space::Load> + Provider<space::Create> + ConditionalSync,
    {
        let credential = match self.0.clone().load().perform(env).await {
            Ok(credential) => credential,
            Err(_) => {
                let signer = Ed25519Signer::generate().await?;
                let credential = Credential::Signer(SignerCredential::from(signer));
                self.0.create(credential).perform(env).await?
            }
        };

        Ok(Repository::from(credential))
    }
}
