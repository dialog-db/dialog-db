use dialog_capability::{Did, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive;
use dialog_effects::memory;
use dialog_prolly_tree::{
    EMPT_TREE_HASH, Entry, GeometricDistribution, Node, Tree, TreeDifference,
};
use dialog_storage::Blake3Hash;
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::artifacts::selector::Constrained;
use crate::artifacts::{Artifact, ArtifactSelector, Datum, Instruction, MatchCandidate};
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, FromKey, Key, KeyView, KeyViewConstruct,
    KeyViewMut, State, ValueKey,
};

use super::archive::CapabilityArchive;
use super::branch_state::{BranchId, BranchState};
use super::cell::Cell;
use super::credentials::Credentials;
use super::error::RepositoryError;
use super::node_reference::NodeReference;
use super::revision::Revision;

/// Type alias for the prolly tree index backed by capability-based archive.
pub type Index<Env> = Tree<
    GeometricDistribution,
    Key,
    State<Datum>,
    Blake3Hash,
    CapabilityArchive<Env>,
>;

/// A branch represents a named line of development within a repository.
///
/// This is the capability-based version of Branch — it has no `Backend` parameter.
/// All effectful operations return command structs whose `.perform(env)` method
/// executes the effects. The `Env` is never captured in persistent structs
/// (except via the `CapabilityArchive` CAS adapter for the prolly tree).
pub struct Branch {
    issuer: Credentials,
    id: BranchId,
    subject: Did,
    state: BranchState,
    cell: Cell,
    catalog: String,
    edition: Option<Vec<u8>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Branch")
            .field("id", &self.id)
            .field("issuer", &self.issuer.did())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch identifier.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns the DID of the authority issuing changes on this branch.
    pub fn did(&self) -> Did {
        self.issuer.did()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> &Revision {
        &self.state.revision
    }

    /// Returns the base tree reference for this branch.
    pub fn base(&self) -> &NodeReference {
        &self.state.base
    }

    /// Returns the branch state.
    pub fn state(&self) -> &BranchState {
        &self.state
    }

    /// Returns the issuer.
    pub fn issuer(&self) -> &Credentials {
        &self.issuer
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Returns a description of this branch.
    pub fn description(&self) -> &str {
        &self.state.description
    }

    /// Logical time on this branch
    pub fn occurence(&self) -> super::occurence::Occurence {
        self.state.revision.clone().into()
    }
}

impl Branch {
    /// Create a command to open (load or create) a branch.
    pub fn open(id: impl Into<BranchId>, issuer: Credentials, subject: Did) -> Open {
        let id = id.into();
        Open {
            id,
            issuer,
            subject,
            create_if_missing: true,
        }
    }

    /// Create a command to load an existing branch (error if not found).
    pub fn load(id: impl Into<BranchId>, issuer: Credentials, subject: Did) -> Open {
        let id = id.into();
        Open {
            id,
            issuer,
            subject,
            create_if_missing: false,
        }
    }

    /// Create a command to commit instructions to this branch.
    pub fn commit<I>(self, instructions: I) -> Commit<I> {
        Commit {
            branch: self,
            instructions,
        }
    }

    /// Create a command to select artifacts from this branch.
    pub fn select(&self, selector: ArtifactSelector<Constrained>) -> Select {
        Select {
            subject: self.subject.clone(),
            state: self.state.clone(),
            catalog: self.catalog.clone(),
            selector,
        }
    }

    /// Create a command to reset the branch to a given revision.
    pub fn reset(self, revision: Revision) -> Reset {
        Reset {
            branch: self,
            revision,
        }
    }

    /// Create a command to advance the branch to a new revision with an
    /// explicit base tree. Used after merge operations where `base` should
    /// be set to the upstream's tree (what we synced from) while `revision`
    /// is the merged result.
    pub fn advance(self, revision: Revision, base: NodeReference) -> Advance {
        Advance {
            branch: self,
            revision,
            base,
        }
    }

    /// Create a command to pull changes from a local upstream branch.
    ///
    /// This performs a three-way merge:
    /// 1. Loads the upstream tree (their changes)
    /// 2. Computes local changes since last pull
    /// 3. Integrates local changes into upstream tree
    /// 4. Creates a new revision
    pub fn pull(self, upstream_revision: Revision) -> PullLocal {
        PullLocal {
            branch: self,
            upstream_revision,
        }
    }
}

/// Command struct for opening/loading a branch.
pub struct Open {
    id: BranchId,
    issuer: Credentials,
    subject: Did,
    create_if_missing: bool,
}

impl Open {
    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory::Resolve> + Provider<memory::Publish>,
    {
        let space = self.subject.to_string();
        let cell_name = format!("local/{}", self.id);
        let cell = Cell::new(
            Subject::from(self.subject.clone()),
            &space,
            &cell_name,
        );

        // Try to resolve existing branch state
        let resolved: Option<(BranchState, Vec<u8>)> = cell.resolve().perform(env).await?;

        let (state, edition) = match resolved {
            Some((state, edition)) => (state, Some(edition)),
            None if self.create_if_missing => {
                // Create default state
                let state = BranchState::new(
                    self.id.clone(),
                    Revision::new(self.issuer.did()),
                    None,
                );
                let edition = cell.publish(&state, None)?.perform(env).await?;
                (state, Some(edition))
            }
            None => {
                return Err(RepositoryError::BranchNotFound {
                    id: self.id.clone(),
                });
            }
        };

        let catalog = "index".to_string();

        Ok(Branch {
            id: self.id,
            issuer: self.issuer,
            subject: self.subject,
            state,
            cell,
            catalog,
            edition,
        })
    }
}

/// Command struct for committing instructions to a branch.
pub struct Commit<I> {
    branch: Branch,
    instructions: I,
}

impl<I> Commit<I>
where
    I: Stream<Item = Instruction> + ConditionalSend,
{
    /// Execute the commit operation, returning the updated branch and tree hash.
    ///
    /// Takes `Arc<Mutex<Env>>` because the prolly tree requires an owned
    /// `ContentAddressedStorage` implementation via `CapabilityArchive`.
    pub async fn perform<Env>(
        self,
        env: Arc<Mutex<Env>>,
    ) -> Result<(Branch, Blake3Hash), DialogArtifactsError>
    where
        Env: Provider<archive::Get>
            + Provider<archive::Put>
            + Provider<memory::Resolve>
            + Provider<memory::Publish>
            + ConditionalSync
            + 'static,
    {
        let mut branch = self.branch;
        let instructions = self.instructions;
        let base_revision = branch.state.revision.clone();

        let archive = CapabilityArchive::new(
            env.clone(),
            Subject::from(branch.subject.clone()),
            &branch.catalog,
        );

        // Load tree from current revision hash
        let mut tree: Index<Env> = Tree::from_hash(
            base_revision.tree().hash(),
            archive,
        )
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
        let tree_hash = *tree.hash().ok_or_else(|| {
            DialogArtifactsError::Storage("Failed to get tree hash".to_string())
        })?;

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
            ..branch.state.clone()
        };

        // Publish updated state
        let mut env = env.lock().await;
        let new_edition = branch
            .cell
            .publish(&new_state, branch.edition.clone())
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?
            .perform(&mut *env)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

        branch.state = new_state;
        branch.edition = Some(new_edition);

        Ok((branch, tree_hash))
    }
}

/// Command struct for selecting artifacts from a branch.
pub struct Select {
    subject: Did,
    state: BranchState,
    catalog: String,
    selector: ArtifactSelector<Constrained>,
}

impl Select {
    /// Execute the select operation, returning a stream of matching artifacts.
    ///
    /// Takes `Arc<Mutex<Env>>` because the prolly tree requires an owned
    /// `ContentAddressedStorage` implementation via `CapabilityArchive`.
    pub async fn perform<Env>(
        self,
        env: Arc<Mutex<Env>>,
    ) -> Result<
        impl Stream<Item = Result<Artifact, DialogArtifactsError>>,
        DialogArtifactsError,
    >
    where
        Env: Provider<archive::Get>
            + Provider<archive::Put>
            + ConditionalSync
            + 'static,
    {
        let archive = CapabilityArchive::new(
            env,
            Subject::from(self.subject.clone()),
            &self.catalog,
        );

        let tree: Index<Env> = Tree::from_hash(
            self.state.revision.tree.hash(),
            archive,
        )
        .await
        .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load tree: {:?}", e)))?;

        let selector = self.selector;

        Ok(async_stream::try_stream! {
            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });
                tokio::pin!(stream);

                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.value().is_some() {
                let start = <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });
                tokio::pin!(stream);

                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.attribute().is_some() {
                let start = <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });
                tokio::pin!(stream);

                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else {
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
        })
    }
}

/// Command struct for resetting a branch to a given revision.
pub struct Reset {
    branch: Branch,
    revision: Revision,
}

impl Reset {
    /// Execute the reset operation, returning the updated branch.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        let mut branch = self.branch;
        let revision = self.revision;

        let new_state = BranchState {
            revision: revision.clone(),
            base: revision.tree.clone(),
            ..branch.state.clone()
        };

        let new_edition = branch
            .cell
            .publish(&new_state, branch.edition.clone())?
            .perform(env)
            .await?;

        branch.state = new_state;
        branch.edition = Some(new_edition);

        Ok(branch)
    }
}

/// Command struct for advancing a branch to a new revision with explicit base.
pub struct Advance {
    branch: Branch,
    revision: Revision,
    base: NodeReference,
}

impl Advance {
    /// Execute the advance operation, returning the updated branch.
    pub async fn perform<Env>(self, env: &mut Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        let mut branch = self.branch;

        let new_state = BranchState {
            revision: self.revision,
            base: self.base,
            ..branch.state.clone()
        };

        let new_edition = branch
            .cell
            .publish(&new_state, branch.edition.clone())?
            .perform(env)
            .await?;

        branch.state = new_state;
        branch.edition = Some(new_edition);

        Ok(branch)
    }
}

/// Command struct for pulling from a local upstream revision.
///
/// This performs a three-way merge between the current branch, the base
/// (last sync point), and the upstream revision.
pub struct PullLocal {
    branch: Branch,
    upstream_revision: Revision,
}

impl PullLocal {
    /// Execute the pull operation, returning the updated branch and the
    /// new revision (or None if no changes).
    pub async fn perform<Env>(
        self,
        env: Arc<Mutex<Env>>,
    ) -> Result<(Branch, Option<Revision>), DialogArtifactsError>
    where
        Env: Provider<archive::Get>
            + Provider<archive::Put>
            + Provider<memory::Resolve>
            + Provider<memory::Publish>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let upstream_revision = self.upstream_revision;

        // If upstream revision's tree matches our base, nothing to do
        if &branch.state.base == upstream_revision.tree() {
            return Ok((branch, None));
        }

        let archive = CapabilityArchive::new(
            env.clone(),
            Subject::from(branch.subject.clone()),
            &branch.catalog,
        );

        // Load upstream tree
        let mut target: Index<Env> =
            Tree::from_hash(upstream_revision.tree.hash(), archive.clone())
                .await
                .map_err(|e| {
                    DialogArtifactsError::Storage(format!(
                        "Failed to load upstream tree: {:?}",
                        e
                    ))
                })?;

        // Load base tree (state at last sync)
        let base: Index<Env> =
            Tree::from_hash(branch.state.base.hash(), archive.clone())
                .await
                .map_err(|e| {
                    DialogArtifactsError::Storage(format!(
                        "Failed to load base tree: {:?}",
                        e
                    ))
                })?;

        // Load current tree
        let current: Index<Env> =
            Tree::from_hash(branch.state.revision.tree.hash(), archive)
                .await
                .map_err(|e| {
                    DialogArtifactsError::Storage(format!(
                        "Failed to load current tree: {:?}",
                        e
                    ))
                })?;

        // Compute local changes: what operations transform base into current
        let changes = base.differentiate(&current);

        // Integrate local changes into upstream tree
        target.integrate(changes).await.map_err(|e| {
            DialogArtifactsError::Storage(format!(
                "Failed to integrate changes: {:?}",
                e
            ))
        })?;

        // Get the hash of the integrated tree
        let hash = target.hash().cloned().unwrap_or(EMPT_TREE_HASH);

        // Check if integration actually changed the tree
        if &hash == upstream_revision.tree.hash() {
            // No local changes were integrated — adopt upstream directly
            let mut env = env.lock().await;
            let branch = branch
                .reset(upstream_revision.clone())
                .perform(&mut *env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            Ok((branch, Some(upstream_revision)))
        } else {
            // Create new revision with integrated changes
            let issuer_did = branch.issuer.did();
            let new_revision = Revision {
                issuer: issuer_did,
                tree: NodeReference(hash),
                cause: HashSet::from([upstream_revision.edition().map_err(|e| {
                    DialogArtifactsError::Storage(format!("Failed to create edition: {:?}", e))
                })?]),
                period: upstream_revision
                    .period
                    .max(branch.state.revision.period)
                    + 1,
                moment: 0,
            };

            // Advance branch to merged revision with upstream's tree as base
            let mut env = env.lock().await;
            let branch = branch
                .advance(new_revision.clone(), upstream_revision.tree.clone())
                .perform(&mut *env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("{:?}", e)))?;

            Ok((branch, Some(new_revision)))
        }
    }
}

/// Create a stream of novel nodes representing local changes since the last sync.
///
/// These are tree nodes that exist in the current tree but not in the base tree.
/// Used during push to send only the new nodes to the remote.
pub fn novelty<Env>(
    base_hash: Blake3Hash,
    current_hash: Blake3Hash,
    env: Arc<Mutex<Env>>,
    subject: Did,
    catalog: String,
) -> impl Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>>
where
    Env: Provider<archive::Get>
        + Provider<archive::Put>
        + ConditionalSync
        + 'static,
{
    async_stream::try_stream! {
        let archive = CapabilityArchive::new(
            env,
            Subject::from(subject),
            catalog,
        );

        let base: Index<Env> = Tree::from_hash(&base_hash, archive.clone())
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
            })?;

        let current: Index<Env> = Tree::from_hash(&current_hash, archive)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
            })?;

        let difference = TreeDifference::compute(&base, &current)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to compute diff: {:?}", e))
            })?;

        for await node in difference.novel_nodes() {
            yield node.map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load node: {:?}", e))
            })?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    fn test_subject() -> Did {
        "did:test:branch-cap".parse().unwrap()
    }

    async fn test_issuer() -> Credentials {
        Credentials::from_passphrase("test").await.unwrap()
    }

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let mut env = Volatile::new();

        let branch = Branch::open("main", test_issuer().await, test_subject())
            .perform(&mut env)
            .await?;

        assert_eq!(branch.id().id(), "main");
        assert_eq!(branch.revision().tree(), &NodeReference::default());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_existing_branch() -> anyhow::Result<()> {
        let mut env = Volatile::new();
        let issuer = test_issuer().await;

        // First open creates
        let _ = Branch::open("main", issuer.clone(), test_subject())
            .perform(&mut env)
            .await?;

        // Load should find it
        let branch = Branch::load("main", issuer, test_subject())
            .perform(&mut env)
            .await?;

        assert_eq!(branch.id().id(), "main");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> anyhow::Result<()> {
        let mut env = Volatile::new();

        let result = Branch::load("nonexistent", test_issuer().await, test_subject())
            .perform(&mut env)
            .await;

        assert!(matches!(result, Err(RepositoryError::BranchNotFound { .. })));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(Volatile::new()));

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

        let results: Vec<_> = stream
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].the, artifact.the);
        assert_eq!(results[0].is, artifact.is);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_advances_with_explicit_base() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(Volatile::new()));

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
        assert_eq!(branch.revision(), &new_revision);
        assert_eq!(branch.base(), &explicit_base);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(Volatile::new()));

        let issuer = test_issuer().await;

        let branch = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        // Pull with upstream at same base — should be a no-op
        let upstream_revision = Revision::new(issuer.did());

        let (branch, pulled) = branch
            .pull(upstream_revision)
            .perform(env.clone())
            .await?;

        assert!(pulled.is_none(), "No changes expected when base matches");
        assert_eq!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> anyhow::Result<()> {
        let env = Arc::new(Mutex::new(Volatile::new()));

        let issuer = test_issuer().await;

        // Create "main" branch and commit something
        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: crate::Value::String("Main data".to_string()),
            cause: None,
        };
        let (main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(env.clone())
            .await?;

        let main_revision = main.revision().clone();

        // Create "feature" branch (empty, base = empty tree)
        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&mut *env.lock().await)
            .await?;

        // Pull main's revision into feature (no local changes)
        let (feature, pulled) = feature
            .pull(main_revision.clone())
            .perform(env.clone())
            .await?;

        assert!(pulled.is_some());
        // Since feature had no local changes, it should adopt main's revision
        assert_eq!(feature.revision().tree(), main_revision.tree());

        Ok(())
    }
}
