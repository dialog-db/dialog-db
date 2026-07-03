use crate::schema;
use crate::{
    Branch, CommitError, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference, Upstream,
};
use dialog_artifacts::history::{Edition, HistoryStore, Record, Version};
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::{DialogArtifactsError, Instruction};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::Delta;
use futures_util::{Stream, StreamExt, stream};

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
            + Provider<Import>
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
        // Checkpoint the head: capture the version we build this commit on top
        // of, so the publish below CAS's against it. A concurrent commit or
        // pull that advances the head while we apply changes then makes this
        // publish fail with `VersionMismatch` rather than silently overwriting
        // it — the caller refreshes and retries. See `Cell::checkpoint`.
        let head = branch.revision.checkpoint();
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

        // Discover who we are up front: the revision is attributed to the
        // profile / operator, and the commit's `Version` — the identifier
        // every datum and history record it writes is tagged with — derives
        // from the issuer and the subject. The subject comes from the
        // branch itself, not the identity chain.
        let authority = Identify.perform(env).await?;
        let issuer = authority.did();
        let profile = authority.profile().clone();

        let edition = base_revision
            .as_ref()
            .map(|base| base.edition.successor())
            .unwrap_or(Edition::GENESIS);
        let lineage = schema::Branch::new(
            &schema::Origin::new(profile.clone(), branch.of().clone()),
            branch.name(),
        )
        .this;
        let origin = Revision::origin_of(&lineage, &issuer);
        let version = Version::new(origin, edition);

        // Walk forward from the current revision's tree root, or from
        // the empty tree if the branch has no commits yet.
        let base_tree_hash = base_revision
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);

        let mut tree = Index::from_hash(NodeHash::from(base_tree_hash));

        // Derive each instruction's history record against the pre-commit
        // tree, so the record's cause lists the versions of the claims the
        // instruction supersedes. This needs the instructions twice (once to
        // derive, once to apply), hence the collect.
        let instructions: Vec<Instruction> = changes.collect().await;
        let mut records: Vec<(Version, Record)> = Vec::with_capacity(instructions.len() + 1);
        for instruction in &instructions {
            let artifact = match instruction {
                Instruction::Assert(artifact)
                | Instruction::Replace(artifact)
                | Instruction::Retract(artifact) => artifact,
            };
            let current = tree
                .select_data(store.clone(), &artifact.of, &artifact.the)
                .await?;
            records.push((version, Record::derive(instruction, &current)));
        }
        // Drain the change stream into the tree. EAV/AEV/VAE writes,
        // cardinality-one supersession, and retraction live in the
        // shared `ArtifactTreeExt::apply_versioned` so the key layout stays
        // uniform; every asserted datum is tagged with this commit's
        // version. The batch's new nodes accumulate in `delta`, which we
        // flush below.
        let mut delta = Delta::zero();
        tree.apply_versioned(
            &mut store,
            &mut delta,
            Some(version),
            stream::iter(instructions),
        )
        .await?;

        // Persist the tree's pending nodes before referencing the root in
        // a revision; a revision must only point at durable blocks. The
        // empty tree's root is the canonical empty-tree hash already. The
        // whole flush travels as one `Import` invocation; block buffers are
        // reference-counted, so nothing is copied on the way in, and
        // providers with native batching persist it in a single round trip
        // (one IndexedDB transaction).
        branch
            .archive()
            .index()
            .import(delta.flush().map(|(_, buffer)| buffer))
            .perform(env)
            .await
            .map_err(DialogArtifactsError::from)?;

        let tree = TreeReference::from(*tree.root().as_bytes());

        let parent = base_revision.as_ref().map(Revision::version);
        let base_history = base_revision.as_ref().and_then(|base| base.history.clone());

        let mut revision = match base_revision {
            Some(base) => base.advance(tree, branch.name(), issuer, profile),
            None => Revision::new(tree, branch.of().clone(), branch.name(), issuer, profile),
        };
        debug_assert_eq!(revision.version(), version);

        // Record the commit's claim lineage, the revision's DAG edge on the
        // branch lineage entity, and the revision's own attribute claims
        // into the history index, continuing from the most recent recorded
        // root. This is what powers claim-level conflict detection: see
        // `dialog_artifacts::history::causality`.
        records.extend(revision.records(parent)?);
        let mut history = match &base_history {
            Some(root) => HistoryStore::from_hash(root.hash(), store.clone()),
            None => HistoryStore::new(store.clone()),
        };
        history.record_all(records).await?;
        revision.history = history.hash().map(TreeReference::from);

        head.publish(revision.clone(), env).await?;

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

    /// A commit made through one handle, while another handle to the same
    /// branch commits from a stale snapshot, must not be silently lost — the
    /// stale commit fails loudly, and refreshing then re-committing reconciles.
    ///
    /// `commit` checkpoints the head it builds on, then publishes CAS'd against
    /// that version. Two independent handles (the shape the service worker hits
    /// once `/transact` no longer serializes writes under a single lock) both
    /// snapshot the same head; one commits and advances storage, so the other's
    /// publish CAS fails with a `VersionMismatch` rather than overwriting the
    /// first commit with a tree built from the now-stale snapshot. Recovery is
    /// refresh + re-commit.
    #[dialog_common::test]
    async fn it_fails_a_commit_racing_another_then_reconciles_on_refresh() -> Result<()> {
        use crate::PublishError;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Two independent handles to the same branch — both snapshot the same
        // (empty) head at open time.
        let writer_a = repo.branch("main").open().perform(&operator).await?;
        let writer_b = repo.branch("main").open().perform(&operator).await?;

        // A commits first, advancing the head in storage. B's cache is now
        // stale.
        writer_a
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:a".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // B commits from its stale snapshot — must fail loudly, not silently
        // drop A's commit.
        let raced = writer_b
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:b".parse()?,
                is: Value::String("Bob".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await;
        assert!(
            matches!(
                raced,
                Err(crate::CommitError::Publish(
                    PublishError::VersionMismatch { .. }
                ))
            ),
            "a commit racing another must fail with a version mismatch; got {raced:?}"
        );

        // Recovery: refresh B's view of the head, then re-commit.
        writer_b.refresh(&operator).await?;
        writer_b
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:b".parse()?,
                is: Value::String("Bob".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // Both commits survive.
        let fresh = repo.branch("main").open().perform(&operator).await?;
        let results: Vec<_> = fresh
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results.len(),
            2,
            "both racing commits must survive recovery"
        );

        Ok(())
    }
}

#[cfg(test)]
mod history_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::history::{Causality, History as _, causality, common_ancestor};
    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    fn title(of: &str, value: &str) -> Artifact {
        Artifact {
            the: "post/title".parse().unwrap(),
            of: of.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        }
    }

    /// Commits record claim lineage into the history index: a replacement's
    /// record supersedes the claim it replaced, detectable via the tiered
    /// conflict detection, and every revision's DAG edge is recorded.
    #[dialog_common::test]
    async fn it_records_claim_lineage_across_commits() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;
        assert!(first.history.is_some(), "commit must record history");

        // Refresh so the next commit builds on the published head.
        branch.refresh(&operator).await?;
        let second = branch
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hi",
            ))]))
            .perform(&operator)
            .await?;

        branch.refresh(&operator).await?;
        let history = branch.history(&operator);

        // Both claims are recorded, and the replacement's cause lists the
        // version of the claim it superseded.
        let records = history.records().await?;
        let claims: Vec<_> = records
            .iter()
            .filter(|(_, record)| record.claim().the.to_string() == "post/title")
            .collect();
        assert_eq!(claims.len(), 2);
        let (first_version, first_record) = claims[0];
        let (second_version, second_record) = claims[1];
        assert_eq!(*first_version, first.version());
        assert_eq!(*second_version, second.version());
        assert!(first_record.claim().cause.is_genesis());
        assert!(second_record.claim().cause.contains(&first.version()));

        // Tier 1 conflict detection over the branch's durable history.
        assert_eq!(
            causality(
                (second_record.claim(), second_version),
                (first_record.claim(), first_version),
                &history
            )
            .await?,
            Causality::Supersedes
        );

        // The revision DAG edges are recorded too: each revision's lineage
        // claim is present, attached to the branch lineage entity and
        // pointing at the content-derived revision entity...
        let edge = history.revision_at(&second.version()).await?;
        assert_eq!(history.revision_at(&first.version()).await?.len(), 1);
        assert_eq!(edge.len(), 1);
        assert_eq!(edge[0].of, second.lineage());
        assert_eq!(edge[0].is, Value::Entity(second.entity()));

        // ... and the revision entity is describable like any other entity:
        // its attribute claims are recorded on it
        let described = history
            .claims_at(
                &second.version(),
                &second.entity(),
                &"dialog.revision/edition".parse()?,
            )
            .await?;
        assert_eq!(described.len(), 1);
        assert_eq!(
            described[0].is,
            Value::UnsignedInt(u128::from(second.edition.value()))
        );
        assert_eq!(
            common_ancestor(&second.version(), &first.version(), &history).await?,
            Some(first.version())
        );

        Ok(())
    }

    /// Data committed through a branch is tagged with the revision's
    /// version, so later commits can derive what they supersede.
    #[dialog_common::test]
    async fn it_tags_committed_data_with_the_revision_version() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let revision = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        // Read the datum back through the artifact tree and check the tag.
        use crate::{Index, NetworkedIndex, RepositoryArchiveExt as _};
        use dialog_artifacts::tree::ArtifactTreeExt as _;
        use dialog_common::Blake3Hash as NodeHash;

        let store = NetworkedIndex::new(&operator, branch.archive().index(), None);
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
        let data = tree
            .select_data(store, &"post:1".parse()?, &"post/title".parse()?)
            .await?;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].version, Some(revision.version()));

        Ok(())
    }
}
