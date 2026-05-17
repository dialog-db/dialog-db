use crate::{
    Branch, CommitError, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Revision, TreeReference, Upstream,
};
use dialog_artifacts::{Instruction, tree};
use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::{Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_prolly_tree::{EMPT_TREE_HASH, Tree};
use futures_util::Stream;

/// Command that commits a stream of changes (assert/retract) to a branch.
///
/// Created by [`Branch::commit`]. Execute with `.perform(&env)`.
pub struct Commit<'a, Changes> {
    branch: &'a Branch,
    changes: Changes,
}

impl<'a, Changes> Commit<'a, Changes> {
    fn new(branch: &'a Branch, changes: Changes) -> Self {
        Self { branch, changes }
    }
}

impl Branch {
    /// Commit a stream of changes to this branch.
    pub fn commit<Changes>(&self, changes: Changes) -> Commit<'_, Changes> {
        Commit::new(self, changes)
    }
}

impl<Changes> Commit<'_, Changes>
where
    Changes: Stream<Item = Instruction> + ConditionalSend,
{
    /// Execute the commit, returning the newly-published [`Revision`].
    ///
    /// Load the branch's current search tree, apply every change in the
    /// stream to the three (entity / attribute / value) indexes, then
    /// publish a new [`Revision`] to the branch's revision cell with the
    /// updated logical clock.
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let changes = self.changes;
        let base_revision = branch.revision();

        // If the branch tracks a remote upstream, commits must be able
        // to read remote-only blocks on demand (pull only merges the
        // tree metadata, not every block). `NetworkedIndex` falls back
        // to the remote when a block is missing locally.
        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => {
                branch.subject().remote(name).load().perform(env).await.ok()
            }
            _ => None,
        };
        let mut store = NetworkedIndex::new(env, branch.archive().index(), remote);

        // Walk forward from the current revision's tree root, or from
        // the empty tree if the branch has no commits yet.
        let base_tree_hash = base_revision
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPT_TREE_HASH);

        let mut tree: Index = Tree::from_hash(&base_tree_hash, &store).await?;

        // Drain the change stream into the tree. All EAV/AEV/VAE writes,
        // cardinality-one supersession, and retraction live in the shared
        // helper so branch commits and in-memory layers agree exactly on
        // key layout.
        tree::apply(&mut tree, &mut store, changes).await?;

        // `tree.hash()` returns `None` only when the tree is empty, which
        // we represent as the canonical empty-tree hash.
        let tree = TreeReference::from(tree.hash().copied().unwrap_or(EMPT_TREE_HASH));

        // Discover who we are so the revision can be attributed to the
        // correct profile / operator. The subject comes from the branch
        // itself, not the identity chain.
        let authority = Identify.perform(env).await?;
        let issuer = authority.did();
        let profile = authority.profile().clone();

        let revision = match base_revision {
            Some(base) => base.advance(tree, issuer, profile),
            None => Revision::new(tree, branch.of().clone(), issuer, profile),
        };

        branch
            .revision
            .publish(revision.clone())
            .perform(env)
            .await?;

        Ok(revision)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::TreeReference;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
    use futures_util::{StreamExt, stream};

    #[dialog_common::test]
    async fn it_commits_and_selects() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: Value::String("Alice".to_string()),
            cause: None,
        };

        let instructions = stream::iter(vec![Instruction::Assert(artifact.clone())]);

        let revision = branch.commit(instructions).perform(&operator).await?;
        assert_ne!(revision.tree, TreeReference::default());

        // Select should find the artifact
        let selector = ArtifactSelector::new().the("user/name".parse()?);
        let stream = branch.claims().select(selector).perform(&operator).await?;
        tokio::pin!(stream);

        let results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].the, artifact.the);
        assert_eq!(results[0].is, artifact.is);

        Ok(())
    }
}
