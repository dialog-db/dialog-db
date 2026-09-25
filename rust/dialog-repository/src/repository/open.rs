use crate::registry::RegistryEnv;
use crate::repository::upgrade::stamp;
use crate::{OpenRepositoryError, Repository, RepositoryMemoryExt as _, UpgradeError};
use dialog_capability::{Capability, Provider};
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::memory::List;
use dialog_effects::space::{self, SpaceExt};

/// Command to open (load-or-create) a repository.
///
/// Opening an existing repository upgrades its storage to the current
/// layout, and creating one records that layout.
///
/// Opening also opens the repository's registry branch and leaves it
/// held by the environment, so listing branches and resolving where
/// their upstreams live read a warm branch rather than opening one.
///
/// Returns `Repository<Credential>` since the loaded credential
/// may be verifier-only.
pub struct OpenRepository(pub Capability<space::Space>);

impl OpenRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, OpenRepositoryError>
    where
        Env: Provider<space::Load> + Provider<space::Create> + Provider<List> + RegistryEnv,
    {
        let repository = match self.0.clone().load().perform(env).await {
            Ok(credential) => {
                let repository = Repository::from(credential);
                repository.upgrade().perform(env).await?;
                repository
            }
            Err(_) => {
                let signer = Ed25519Signer::generate().await?;
                let credential = Credential::Signer(SignerCredential::from(signer));
                let repository = Repository::from(self.0.create(credential).perform(env).await?);
                stamp(&repository.subject(), env)
                    .await
                    .map_err(UpgradeError::from)?;
                repository
            }
        };

        repository.subject().registry().open().perform(env).await?;
        Ok(repository)
    }
}
