use crate::{LoadRepositoryError, Repository};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::space::{self, SpaceExt};

/// Command to load an existing repository.
///
/// Returns `Repository<Credential>` since the credential
/// may be verifier-only.
pub struct LoadRepository(pub Capability<space::Space>);

impl LoadRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, LoadRepositoryError>
    where
        Env: Provider<space::Load> + ConditionalSync,
    {
        Ok(Repository::from(self.0.load().perform(env).await?))
    }
}
