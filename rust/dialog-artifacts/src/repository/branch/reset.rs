use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::Branch;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command struct for resetting a branch to a given revision.
pub struct Reset<'a> {
    branch: &'a Branch,
    revision: Revision,
}

impl<'a> Reset<'a> {
    pub(super) fn new(branch: &'a Branch, revision: Revision) -> Self {
        Self { branch, revision }
    }
}

impl Reset<'_> {
    /// Execute the reset operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory_fx::Publish>,
    {
        self.branch
            .revision
            .publish(Some(self.revision), env)
            .await?;

        Ok(())
    }
}
