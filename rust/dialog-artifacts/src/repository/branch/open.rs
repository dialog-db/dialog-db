use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::cell::{Cell, CellOr};
use crate::repository::error::RepositoryError;
use crate::repository::memory::{Authorization, Memory, Trace};
use crate::repository::revision::Revision;

/// Command to open a branch, creating it with defaults if it doesn't exist.
pub struct Open<Store> {
    session: Authorization<Store>,
    subject: Did,
    memory: Memory,
    trace: Trace,
}

impl<Store> Open<Store> {
    pub(crate) fn new(
        session: Authorization<Store>,
        subject: Did,
        memory: Memory,
        trace: Trace,
    ) -> Self {
        Self {
            session,
            subject,
            memory,
            trace,
        }
    }
}

impl<Store> Open<Store> {
    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch<Store>, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let default_revision = Revision::new(self.session.did());
        let revision: CellOr<Revision> = self.trace.cell("revision").or(default_revision);
        revision.get_or_init(env).await?;

        let upstream: Cell<Option<UpstreamState>> = self.trace.cell("upstream");
        upstream.resolve(env).await?;

        Ok(Branch {
            session: self.session,
            subject: self.subject,
            memory: self.memory,
            trace: self.trace,
            revision,
            upstream,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use crate::repository::Repository;
    use crate::repository::node_reference::NodeReference;
    use dialog_storage::provider::Volatile;

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

        let branch = repo.open_branch("main").perform(&env).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert_eq!(branch.revision().tree(), &NodeReference::default());
        Ok(())
    }
}
