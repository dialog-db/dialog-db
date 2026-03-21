use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::Branch;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command struct for resetting a branch to a given revision.
pub struct Reset<'a, Store> {
    branch: &'a Branch<Store>,
    revision: Revision,
}

impl<'a, Store> Reset<'a, Store> {
    pub(super) fn new(branch: &'a Branch<Store>, revision: Revision) -> Self {
        Self { branch, revision }
    }
}

impl<Store> Reset<'_, Store> {
    /// Execute the reset operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory_fx::Publish>,
    {
        self.branch.revision.publish(self.revision, env).await?;

        Ok(())
    }
}
