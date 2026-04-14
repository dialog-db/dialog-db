use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::space as space_fx;

use super::Repository;
use super::error::RepositoryError;

/// Command to load an existing repository.
///
/// Returns `Repository<Credential>` since the credential
/// may be verifier-only.
pub struct LoadRepository(pub(super) Capability<space_fx::Space>);

impl LoadRepository {
    /// Execute against an operator.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, RepositoryError>
    where
        Env: Provider<space_fx::Load> + ConditionalSync,
    {
        use dialog_effects::space::SpaceExt as _;

        let credential = self.0.load().perform(env).await?;
        Ok(Repository::from(credential))
    }
}
