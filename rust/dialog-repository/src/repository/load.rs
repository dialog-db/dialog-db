use crate::registry::RegistryEnv;
use crate::{LoadRepositoryError, Repository, RepositoryMemoryExt as _};
use dialog_capability::{Capability, Provider};
use dialog_effects::space::{self, SpaceExt};

/// Command to load an existing repository.
///
/// Loading also opens the repository's registry branch and leaves it
/// held by the environment, as [`OpenRepository`](crate::OpenRepository)
/// does.
///
/// Returns `Repository<Credential>` since the credential
/// may be verifier-only.
pub struct LoadRepository(pub Capability<space::Space>);

impl LoadRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, LoadRepositoryError>
    where
        Env: Provider<space::Load> + RegistryEnv,
    {
        let repository = Repository::from(self.0.load().perform(env).await?);
        repository.subject().registry().open().perform(env).await?;
        Ok(repository)
    }
}
