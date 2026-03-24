use dialog_capability::{Provider, Subject, credential};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::Tree;
use dialog_storage::Blake3Hash;
use futures_util::{StreamExt, TryStreamExt};
use std::collections::HashSet;

use super::{Branch, Index};
use crate::artifacts::{Artifact, Cause, Datum, Instruction};
use crate::repository::archive::ContentAddressedStore;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, Key, KeyView, KeyViewConstruct,
    KeyViewMut, State, ValueKey,
};

/// Command struct for committing instructions to a branch.
pub struct Commit<'a, I> {
    branch: &'a Branch,
    instructions: I,
}

impl<'a, I> Commit<'a, I> {
    pub(super) fn new(branch: &'a Branch, instructions: I) -> Self {
        Self {
            branch,
            instructions,
        }
    }
}

impl<I> Commit<'_, I>
where
    I: futures_util::Stream<Item = Instruction> + ConditionalSend,
{
    /// Execute the commit operation, returning the tree hash.
    pub async fn perform<Env>(self, env: &Env) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<credential::Identify>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let instructions = self.instructions;
        let base_revision = branch.revision();

        let mut store = ContentAddressedStore::new(env, branch.archive().index());

        // Load tree from current revision hash (empty tree if no revision yet)
        let base_tree_hash = base_revision
            .as_ref()
            .map(|rev| *rev.tree().hash())
            .unwrap_or(dialog_prolly_tree::EMPT_TREE_HASH);

        let mut tree: Index = Tree::from_hash(&base_tree_hash, &store)
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

                            let search_stream = tree.stream_range(search_start..search_end, &store);

                            let mut ancestor_key = None;

                            tokio::pin!(search_stream);

                            while let Some(candidate) = search_stream.try_next().await? {
                                if let State::Added(current_element) = candidate.value {
                                    let current_artifact = Artifact::try_from(current_element)?;
                                    let current_artifact_reference = Cause::from(&current_artifact);

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

                            tree.delete(&entity_key.into_key(), &mut store).await?;
                            tree.delete(&value_key.into_key(), &mut store).await?;
                            tree.delete(&attribute_key.into_key(), &mut store).await?;
                        }
                    }

                    tree.set(
                        entity_key.into_key(),
                        State::Added(datum.clone()),
                        &mut store,
                    )
                    .await?;
                    tree.set(
                        attribute_key.into_key(),
                        State::Added(datum.clone()),
                        &mut store,
                    )
                    .await?;
                    tree.set(value_key.into_key(), State::Added(datum), &mut store)
                        .await?;
                }
                Instruction::Retract(fact) => {
                    let entity_key = EntityKey::from(&fact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    tree.set(entity_key.into_key(), State::Removed, &mut store)
                        .await?;
                    tree.set(attribute_key.into_key(), State::Removed, &mut store)
                        .await?;
                    tree.set(value_key.into_key(), State::Removed, &mut store)
                        .await?;
                }
            }
        }

        // Get the tree hash
        let tree_hash = *tree
            .hash()
            .ok_or_else(|| DialogArtifactsError::Storage("Failed to get tree hash".to_string()))?;

        let tree_reference = NodeReference::from(tree_hash);

        // Discover operator identity from the environment
        let identify_cap = Subject::from(branch.subject().clone())
            .attenuate(credential::Credential)
            .invoke(credential::Identify);
        let identity = <Env as Provider<credential::Identify>>::execute(env, identify_cap)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Identify failed: {}", e)))?;
        let issuer_did = identity.operator;

        // Calculate new period and moment
        let (period, moment, cause) = match &base_revision {
            Some(rev) => {
                let (p, m) = if rev.issuer() == &issuer_did {
                    (*rev.period(), *rev.moment() + 1)
                } else {
                    (*rev.period() + 1, 0)
                };
                (p, m, HashSet::from([rev.tree().clone()]))
            }
            None => (0, 0, HashSet::new()),
        };

        let new_revision = Revision {
            issuer: issuer_did,
            tree: tree_reference,
            cause,
            period,
            moment,
        };

        // Publish updated revision
        branch
            .revision
            .publish(Some(new_revision), env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        Ok(tree_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{TestEnv, test_subject};
    use crate::artifacts::{Artifact, ArtifactSelector, Instruction};
    use crate::repository::Repository;
    use dialog_prolly_tree::EMPT_TREE_HASH;
    use futures_util::{StreamExt, stream};

    #[dialog_common::test]
    async fn it_commits_and_selects() -> anyhow::Result<()> {
        let env = TestEnv::new();

        let repo = Repository::new(test_subject());
        let branch = repo.open_branch("main").perform(&env).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };

        let instructions = stream::iter(vec![Instruction::Assert(artifact.clone())]);

        let hash = branch.commit(instructions).perform(&env).await?;
        assert_ne!(hash, EMPT_TREE_HASH);

        // Select should find the artifact
        let selector = ArtifactSelector::new().the("user/name".parse()?);
        let stream = branch.select(selector).perform(&env).await?;
        tokio::pin!(stream);

        let results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].the, artifact.the);
        assert_eq!(results[0].is, artifact.is);

        Ok(())
    }
}
