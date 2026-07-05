use crate::schema;
use crate::{
    Branch, CommitError, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference,
};
use dialog_artifacts::history::{Edition, TreeHistory, Version, extend_skips};
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
        let upstreams = branch.upstreams();
        let remote = match upstreams.remote_name() {
            Some(name) => branch
                .subject()
                .remote(name.to_string())
                .load()
                .perform(env)
                .await
                .ok(),
            None => None,
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
            schema::Origin::new(profile.clone(), branch.of().clone()),
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

        // Drain the change stream into the tree. EAV/AEV/VAE writes,
        // cardinality-one supersession, retraction — and, because the
        // writes are version-tagged, each instruction's history record,
        // whose cause lists the versions of the exact claims the write
        // superseded — all live in the shared
        // `ArtifactTreeExt::apply_versioned` so the key layout stays
        // uniform. Data and history land in the same tree: one root covers
        // both. The batch's new nodes accumulate in `delta`.
        let mut delta = Delta::zero();
        let changed = tree
            .apply_versioned(&mut store, &mut delta, Some(version), changes)
            .await?;

        // A batch that left the indexes untouched (e.g. a transaction
        // re-asserting metadata that is already in place) is a no-op:
        // keep the current revision rather than minting one that differs
        // only by edition. Only a branch with no revision at all still
        // publishes, to establish its genesis.
        if !changed && let Some(base) = base_revision {
            return Ok(base);
        }

        // Mint the revision (the placeholder tree root is replaced below,
        // after its own records are in the tree) and record its DAG edge on
        // the branch lineage entity, its skip links, plus its attribute
        // claims on the revision entity, in the same batch delta. None of
        // those records depend on the final root — a root cannot appear
        // inside itself.
        //
        // The skip table (logarithmic leaps through the revision DAG for
        // `common_ancestor` — see `dialog_artifacts::history::extend_skips`)
        // is lifted from the parent's recorded table, read out of the base
        // tree through the branch's shared node cache.
        let parent = base_revision.as_ref().map(Revision::version);
        let skips = match &parent {
            Some(parent) => {
                let history = TreeHistory::from_root_with_cache(
                    &base_tree_hash,
                    store.clone(),
                    branch.node_cache(),
                );
                extend_skips(&history, parent).await?
            }
            None => Vec::new(),
        };
        let mut revision = match base_revision {
            Some(base) => base.advance(TreeReference::default(), branch.name(), issuer, profile),
            None => Revision::new(
                TreeReference::default(),
                branch.of().clone(),
                branch.name(),
                issuer,
                profile,
            ),
        };
        debug_assert_eq!(revision.version(), version);
        let entries = revision
            .records(parent, &skips)?
            .into_iter()
            .map(|(version, record)| record.into_entry(&version))
            .collect();
        tree.record(&mut store, &mut delta, entries).await?;

        // Persist the tree's pending nodes before referencing the root in
        // a revision; a revision must only point at durable blocks. The
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

        revision.tree = TreeReference::from(*tree.root().as_bytes());

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

        // A third commit records its skip table: the level-1 link leaps two
        // first-parent steps back, from the third revision to the first.
        branch.refresh(&operator).await?;
        let third = branch
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hello",
            ))]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let history = branch.history(&operator);
        let skips = history.skips_at(&third.version()).await?;
        assert_eq!(skips.len(), 1);
        assert_eq!(skips[0].is, Value::UnsignedInt(1));
        assert!(skips[0].cause.contains(&first.version()));

        Ok(())
    }

    /// A commit whose instruction stream is empty (or entirely no-op)
    /// leaves the branch exactly where it was: same revision, same
    /// edition, no new history.
    #[dialog_common::test]
    async fn it_keeps_the_revision_for_an_empty_commit() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let unchanged = branch
            .commit(stream::iter(Vec::<Instruction>::new()))
            .perform(&operator)
            .await?;
        assert_eq!(unchanged, first, "an empty commit mints no revision");
        assert_eq!(branch.revision(), Some(first));

        Ok(())
    }

    /// A no-op commit from a stale handle must not silently succeed: the
    /// instructions were judged no-op against a snapshot another writer
    /// has since superseded, so "nothing to do" may be wrong at the
    /// current head. Here the stale handle re-asserts a value the head
    /// has retracted in the meantime — succeeding silently would lose
    /// the re-assertion.
    ///
    /// KNOWN BUG: the no-op early return in `Commit::perform` skips the
    /// head CAS entirely, so the staleness this test provokes goes
    /// undetected and the re-assertion is lost. Candidate fix: before
    /// returning the base revision, verify the head cell's version still
    /// matches the checkpoint (a resolve + compare — not a same-value
    /// publish, which would bump the cell version and make concurrent
    /// no-ops fail each other spuriously) and surface the usual
    /// `VersionMismatch` when it moved, so callers refresh and retry
    /// exactly like the non-no-op race.
    #[ignore = "no-op commits skip the head CAS and can silently lose a stale re-assertion"]
    #[dialog_common::test]
    async fn it_does_not_treat_a_stale_snapshot_as_a_noop() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let seed = repo.branch("main").open().perform(&operator).await?;
        seed.commit(stream::iter(vec![Instruction::Assert(title(
            "post:1", "Hej",
        ))]))
        .perform(&operator)
        .await?;

        // Both handles snapshot the head with the title asserted.
        let retractor = repo.branch("main").open().perform(&operator).await?;
        let reasserter = repo.branch("main").open().perform(&operator).await?;

        // One handle retracts the title, advancing the head.
        retractor
            .commit(stream::iter(vec![Instruction::Retract(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        // The other re-asserts the same value from its stale snapshot,
        // where the write looks like a cardinality-one no-op. It must not
        // report success while leaving the title retracted at the head.
        let raced = reasserter
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await;

        use futures_util::StreamExt as _;
        let head = repo.branch("main").load().perform(&operator).await?;
        let reasserted = !head
            .claims()
            .select(
                dialog_artifacts::ArtifactSelector::new()
                    .the("post/title".parse()?)
                    .of("post:1".parse()?),
            )
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .is_empty();

        assert!(
            raced.is_err() || reasserted,
            "a stale no-op either fails loudly (so the caller refreshes \
             and retries) or lands the re-assertion; it silently did \
             neither: {raced:?}"
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
