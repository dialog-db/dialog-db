use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::Tree;
use dialog_storage::Blake3Hash;
use futures_util::{StreamExt, TryStreamExt};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::state::BranchState;
use super::{Branch, Index};
use crate::artifacts::{Artifact, Datum, Instruction};
use crate::repository::archive::ContentAddressedStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, Key, KeyView, KeyViewConstruct,
    KeyViewMut, State, ValueKey,
};

/// Command struct for committing instructions to a branch.
pub struct Commit<I> {
    pub(super) branch: Branch,
    pub(super) instructions: I,
}

impl<I> Commit<I>
where
    I: futures_util::Stream<Item = Instruction> + ConditionalSend,
{
    /// Execute the commit operation, returning the updated branch and tree hash.
    ///
    /// Takes `Arc<Mutex<Env>>` because the prolly tree requires an owned
    /// `ContentAddressedStorage` implementation via `ContentAddressedStore`.
    pub async fn perform<Env>(
        self,
        env: Arc<Mutex<Env>>,
    ) -> Result<(Branch, Blake3Hash), DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let instructions = self.instructions;
        let base_revision = branch.revision();

        let archive =
            ContentAddressedStore::new(env.clone(), branch.archive().index());

        // Load tree from current revision hash
        let mut tree: Index<Env> = Tree::from_hash(base_revision.tree().hash(), archive)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load tree: {:?}", e)))?;

        // Apply instructions
        tokio::pin!(instructions);

        while let Some(instruction) = instructions.next().await {
            match instruction {
                Instruction::Assert(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    let datum = Datum::from(artifact);

                    if let Some(cause) = &datum.cause {
                        let ancestor_key = {
                            let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
                                .set_entity(entity_key.entity())
                                .set_attribute(entity_key.attribute())
                                .into_key();
                            let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
                                .set_entity(entity_key.entity())
                                .set_attribute(entity_key.attribute())
                                .into_key();

                            let search_stream = tree.stream_range(search_start..search_end);

                            let mut ancestor_key = None;

                            tokio::pin!(search_stream);

                            while let Some(candidate) = search_stream.try_next().await? {
                                if let State::Added(current_element) = candidate.value {
                                    let current_artifact = Artifact::try_from(current_element)?;
                                    let current_artifact_reference =
                                        crate::artifacts::Cause::from(&current_artifact);

                                    if cause == &current_artifact_reference {
                                        ancestor_key = Some(candidate.key);
                                        break;
                                    }
                                }
                            }

                            ancestor_key
                        };

                        if let Some(key) = ancestor_key {
                            let entity_key = EntityKey(key);
                            let value_key: ValueKey<Key> = ValueKey::from_key(&entity_key);
                            let attribute_key: AttributeKey<Key> =
                                AttributeKey::from_key(&entity_key);

                            tree.delete(&entity_key.into_key()).await?;
                            tree.delete(&value_key.into_key()).await?;
                            tree.delete(&attribute_key.into_key()).await?;
                        }
                    }

                    tree.set(entity_key.into_key(), State::Added(datum.clone()))
                        .await?;
                    tree.set(attribute_key.into_key(), State::Added(datum.clone()))
                        .await?;
                    tree.set(value_key.into_key(), State::Added(datum)).await?;
                }
                Instruction::Retract(fact) => {
                    let entity_key = EntityKey::from(&fact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    tree.set(entity_key.into_key(), State::Removed).await?;
                    tree.set(attribute_key.into_key(), State::Removed).await?;
                    tree.set(value_key.into_key(), State::Removed).await?;
                }
            }
        }

        // Get the tree hash
        let tree_hash = *tree
            .hash()
            .ok_or_else(|| DialogArtifactsError::Storage("Failed to get tree hash".to_string()))?;

        let tree_reference = NodeReference(tree_hash);

        // Calculate new period and moment
        let issuer_did = branch.issuer.did();
        let (period, moment) = {
            let base_period = *base_revision.period();
            let base_moment = *base_revision.moment();
            let base_issuer = base_revision.issuer();

            if base_issuer == &issuer_did {
                (base_period, base_moment + 1)
            } else {
                (base_period + 1, 0)
            }
        };

        let new_revision = Revision {
            issuer: issuer_did,
            tree: tree_reference,
            cause: HashSet::from([base_revision.edition().map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to create edition: {:?}", e))
            })?]),
            period,
            moment,
        };

        // Update branch state
        let new_state = BranchState {
            revision: new_revision,
            ..branch.state()
        };

        // Publish updated state
        let mut env = env.lock().await;
        branch
            .cell
            .publish(new_state, &mut *env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok((branch, tree_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{test_issuer, test_subject};
    use super::super::Branch;
    use crate::artifacts::{Artifact, ArtifactSelector, Instruction};
    use dialog_prolly_tree::EMPT_TREE_HASH;
    use futures_util::{StreamExt, stream};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[dialog_common::test]
    async fn it_commits_and_selects() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(dialog_storage::provider::Volatile::new()));

        let branch = Branch::open("main", test_issuer().await, test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };

        let instructions = stream::iter(vec![Instruction::Assert(artifact.clone())]);

        let (branch, hash) = branch.commit(instructions).perform(env.clone()).await?;
        assert_ne!(hash, EMPT_TREE_HASH);

        // Select should find the artifact
        let selector = ArtifactSelector::new().the("user/name".parse()?);
        let stream = branch.select(selector).perform(env.clone()).await?;
        tokio::pin!(stream);

        let results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].the, artifact.the);
        assert_eq!(results[0].is, artifact.is);

        Ok(())
    }
}
