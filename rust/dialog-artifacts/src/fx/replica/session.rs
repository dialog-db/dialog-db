//! Session provides query and transaction capabilities for a branch.
//!
//! A session wraps a branch and provides the `ArtifactStore` and `ArtifactStoreMut`
//! traits for querying and transacting artifacts.

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSend;
use dialog_prolly_tree::{Entry, GeometricDistribution, Tree, EMPT_TREE_HASH};
use dialog_storage::{Blake3Hash, ContentAddressedStorage};
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::artifacts::selector::Constrained;
use crate::artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Datum, Instruction, MatchCandidate,
};
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, KeyView, KeyViewConstruct, KeyViewMut,
    State, ValueKey,
};

use super::issuer::Issuer;
use super::types::{NodeReference, Revision};

/// Type alias for the prolly tree index used in sessions.
pub type SessionIndex<S> = Tree<GeometricDistribution, crate::Key, State<Datum>, Blake3Hash, S>;

/// A session provides query and transaction capabilities for a branch.
///
/// The session wraps a prolly tree for efficient artifact storage and
/// implements `ArtifactStore` and `ArtifactStoreMut` for querying and
/// committing changes.
///
/// # Type Parameters
///
/// - `S`: The storage backend implementing `ContentAddressedStorage`
#[derive(Clone)]
pub struct BranchSession<S>
where
    S: ContentAddressedStorage<Hash = [u8; 32]> + Clone + Send + Sync + 'static,
    S::Error: Into<DialogArtifactsError>,
{
    /// The issuer for signing commits.
    issuer: Issuer,
    /// Current revision (for commit ancestry).
    revision: Revision,
    /// The prolly tree holding artifacts.
    tree: Arc<RwLock<SessionIndex<S>>>,
    /// Callback to save the new revision after commit.
    on_commit: Option<Arc<dyn Fn(Revision) + Send + Sync>>,
}

impl<S> BranchSession<S>
where
    S: ContentAddressedStorage<Hash = [u8; 32]> + Clone + Send + Sync + 'static,
    S::Error: Into<DialogArtifactsError>,
{
    /// Create a new session with the given storage and revision.
    ///
    /// This is an async constructor because it needs to load the tree from storage.
    pub async fn new(
        issuer: Issuer,
        revision: Revision,
        storage: S,
    ) -> Result<Self, DialogArtifactsError> {
        let tree_hash = *revision.tree().hash();
        let tree = if tree_hash == EMPT_TREE_HASH {
            Tree::new(storage)
        } else {
            Tree::from_hash(&tree_hash, storage)
                .await
                .map_err(|e| DialogArtifactsError::Storage(e.to_string()))?
        };

        Ok(Self {
            issuer,
            revision,
            tree: Arc::new(RwLock::new(tree)),
            on_commit: None,
        })
    }

    /// Set a callback to be invoked after a successful commit.
    ///
    /// The callback receives the new revision and can be used to
    /// persist it back to the branch.
    pub fn on_commit<F>(mut self, f: F) -> Self
    where
        F: Fn(Revision) + Send + Sync + 'static,
    {
        self.on_commit = Some(Arc::new(f));
        self
    }

    /// Get the current revision.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Get the issuer.
    pub fn issuer(&self) -> &Issuer {
        &self.issuer
    }

    /// Get access to the underlying tree for testing purposes.
    ///
    /// This is primarily for testing remote block fetching and caching.
    #[cfg(test)]
    pub async fn tree_for_testing(&self) -> SessionIndex<S> {
        self.tree.read().await.clone()
    }

    /// Get access to the underlying storage for remote configuration.
    ///
    /// This allows updating remotes on the store after the session is created.
    pub async fn storage(&self) -> S {
        self.tree.read().await.storage().clone()
    }
}

// Implement ArtifactStore for BranchSession
impl<S> ArtifactStore for BranchSession<S>
where
    S: ContentAddressedStorage<Hash = [u8; 32]> + Clone + Send + Sync + 'static,
    S::Error: Into<DialogArtifactsError> + Send,
{
    fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static + ConditionalSend
    {
        let tree = self.tree.clone();

        try_stream! {
            let tree = tree.read().await.clone();

            if selector.entity().is_some() {
                let start = <EntityKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.value().is_some() {
                let start = <ValueKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.attribute().is_some() {
                let start = <AttributeKey<crate::Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<crate::Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else {
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
        }
    }
}

// Implement ArtifactStoreMut for BranchSession
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> ArtifactStoreMut for BranchSession<S>
where
    S: ContentAddressedStorage<Hash = [u8; 32]> + Clone + Send + Sync + 'static,
    S::Error: Into<DialogArtifactsError> + Send,
{
    async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: Stream<Item = Instruction> + ConditionalSend,
    {
        use std::collections::HashSet;

        let base_revision = self.revision.clone();

        let transaction_result = async {
            let mut tree = self.tree.write().await;

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
                                let search_start = <EntityKey<crate::Key> as KeyViewConstruct>::min()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute())
                                    .into_key();
                                let search_end = <EntityKey<crate::Key> as KeyViewConstruct>::max()
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
                                let value_key: ValueKey<crate::Key> = ValueKey::from_key(&entity_key);
                                let attribute_key: AttributeKey<crate::Key> =
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

            let tree_hash = *tree.hash().ok_or_else(|| {
                DialogArtifactsError::Storage("Failed to get tree hash".to_string())
            })?;

            let tree_reference = NodeReference::new(tree_hash);

            let (period, moment) = {
                let base_period = *base_revision.period();
                let base_moment = *base_revision.moment();
                let base_issuer = base_revision.issuer();

                if base_issuer == self.issuer.principal() {
                    (base_period, base_moment + 1)
                } else {
                    (base_period + 1, 0)
                }
            };

            #[allow(clippy::clone_on_copy)]
            let new_revision = Revision {
                issuer: self.issuer.principal().clone(),
                tree: tree_reference.clone(),
                cause: HashSet::from([base_revision.edition().expect("Failed to create edition")]),
                period,
                moment,
            };

            // Update internal revision
            self.revision = new_revision.clone();

            // Invoke callback if set
            if let Some(callback) = &self.on_commit {
                callback(new_revision);
            }

            Ok(tree_hash)
        }
        .await;

        match transaction_result {
            Ok(hash) => Ok(hash),
            Err(error) => {
                // Rollback: reset tree to base revision's tree hash
                self.tree
                    .write()
                    .await
                    .set_hash(Some(*base_revision.tree().hash()))
                    .await?;

                Err(error)
            }
        }
    }
}
