use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::state::BranchState;
use super::Branch;
use crate::repository::error::RepositoryError;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;

/// Command struct for advancing a branch to a new revision with explicit base.
pub struct Advance {
    pub(super) branch: Branch,
    pub(super) revision: Revision,
    pub(super) base: NodeReference,
}

impl Advance {
    /// Execute the advance operation, returning the updated branch.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Publish>,
    {
        let branch = self.branch;

        let new_state = BranchState {
            revision: self.revision,
            base: self.base,
            ..branch.state()
        };

        branch.cell.publish(new_state, env).await?;

        Ok(branch)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use super::super::Branch;
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::node_reference::NodeReference;
    use crate::repository::revision::Revision;
    use futures_util::stream;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[dialog_common::test]
    async fn it_advances_with_explicit_base() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(dialog_storage::provider::Volatile::new()));

        let issuer = test_issuer().await;

        let branch = Branch::open("main", issuer.clone(), test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        // Commit something to create a non-empty tree
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let instructions = stream::iter(vec![Instruction::Assert(artifact)]);
        let (branch, _hash) = branch.commit(instructions).perform(env.clone()).await?;

        // Advance to a new revision with a different base
        let new_revision = Revision::new(issuer.did());
        let explicit_base = NodeReference::default();

        let branch = branch
            .advance(new_revision.clone(), explicit_base.clone())
            .perform(&mut *env.lock().await)
            .await?;

        // Verify the branch has the new revision and explicit base
        assert_eq!(branch.revision(), new_revision);
        assert_eq!(branch.base(), explicit_base);

        Ok(())
    }
}
