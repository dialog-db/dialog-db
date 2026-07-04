use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{BlobChange, BlobIndexExt as _, blob_changes};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{BlobError, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, TreeDifference};
use futures_util::{StreamExt as _, TryStreamExt as _};

use crate::{
    Branch, Index, LocalIndex, PushError, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Revision, Upstream, UpstreamBranch,
};

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
    to: Option<Upstream>,
}

impl<'a> Push<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch, to: None }
    }

    /// Push to the given branch instead of the default upstream.
    ///
    /// Accepts either a `&Branch` or a `&RemoteBranch` — the same inputs as
    /// [`Branch::set_upstream`]. If the target is already tracked, its
    /// recorded sync base drives the fast-forward check and the novelty
    /// upload; otherwise the empty base does (only a target with no
    /// revision of its own accepts such a push), and a successful push
    /// starts tracking the target — without changing the default upstream.
    pub fn to(mut self, source: impl Into<UpstreamBranch>) -> Self {
        self.to = Some(Upstream::from(source.into()));
        self
    }
}

impl Branch {
    /// Create a command to push local changes to the upstream branch.
    ///
    /// Targets the default upstream; chain [`Push::to`] to push to another
    /// tracked (or brand-new) upstream instead.
    pub fn push(&self) -> Push<'_> {
        Push::new(self)
    }
}

impl Push<'_> {
    /// Execute the push operation.
    ///
    /// Push is fast-forward only:
    ///
    /// - `Ok(Some(revision))` — pushed; upstream now at `revision`.
    /// - `Ok(None)` — nothing to push (branch has no local revision).
    /// - `Err(PushError::NonFastForward)` — upstream has moved since
    ///   the last sync; pull to integrate before pushing again.
    ///
    /// For remote upstream, novel tree blocks are uploaded before the
    /// revision is published.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PushError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, Publish>>
            + Provider<Fork<RemoteSite, BlobImport>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;

        // Select the upstream entry to push to: the default when no
        // explicit target was given, otherwise the tracked entry for that
        // target — or, for a target not tracked yet, a fresh entry whose
        // empty sync base only fast-forwards onto an empty target.
        let upstreams = branch.upstreams();
        let upstream_state = match self.to {
            None => upstreams.default_upstream().cloned().ok_or_else(|| {
                PushError::BranchHasNoUpstream {
                    branch: branch.name().to_string(),
                }
            })?,
            Some(target) => {
                if let Upstream::Local { branch: name, .. } = &target
                    && name == branch.name()
                {
                    return Err(PushError::UpstreamIsItself {
                        branch: branch.name().to_string(),
                    });
                }
                upstreams.find(&target).cloned().unwrap_or(target)
            }
        };

        let revision = match branch.revision() {
            Some(revision) => revision,
            None => return Ok(None),
        };
        let base = upstream_state.tree().clone();

        // Nothing new to push: the local head already equals the recorded
        // upstream sync point. Without this guard every sync tick re-publishes
        // the revision pointer to the remote (an ongoing `branch/*/revision`
        // PUT) and re-fetches + diffs the upstream for an empty novelty set,
        // even when no commit has landed since the last push. Short-circuit so
        // an idle branch does no push I/O.
        if revision.tree == base {
            return Ok(Some(revision));
        }

        match &upstream_state {
            Upstream::Local {
                branch: upstream_name,
                ..
            } => {
                let target = branch
                    .subject()
                    .branch(upstream_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                let current = target.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                target.reset(revision.clone()).perform(env).await?;
            }
            Upstream::Remote {
                remote: remote_name,
                branch: upstream_branch_name,
                ..
            } => {
                let remote = branch
                    .subject()
                    .remote(remote_name.clone())
                    .load()
                    .perform(env)
                    .await?;

                let upstream = remote
                    .branch(upstream_branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                // Refresh the cache from the remote so our divergence
                // check sees the latest upstream tree, not whatever
                // was in our last snapshot.
                upstream.fetch().perform(env).await?;

                let current = upstream.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                // Upload tree nodes present in our current tree but not
                // in the base, so the remote can hydrate the new tree
                // before we publish the revision pointing at it.
                let index = branch.archive().index();
                let store = LocalIndex::new(env, index.clone());
                let base_tree = Index::from_hash(NodeHash::from(*base.hash()));
                let current_tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
                let tree_store = TreeStorage::new(TreeStorageBridge(store));
                let difference =
                    TreeDifference::compute(&base_tree, &current_tree, &tree_store, &tree_store)
                        .await?;
                let novelty = difference.novel_nodes().map_err(Into::into);
                let remote_archive = remote.archive();
                let remote_index = remote_archive.index();
                let upload = remote_index.upload(novelty).perform(env);
                // Boxed because the upload future carries the full
                // stream type and produces large futures.
                Box::pin(upload).await?;

                // Ship blobs newly referenced since the sync checkpoint. The
                // entry-level view of the same differential, restricted to the
                // BLOB tag, names exactly what the remote lacks under
                // fast-forward. Bytes must land on the remote before we publish
                // a revision that references them, so a failed upload here
                // aborts the push with the revision still unpublished.
                let blob_store = LocalIndex::new(env, index.clone());
                let current_index = Index::from_hash(NodeHash::from(*revision.tree.hash()));
                let address = remote.address();
                let mut changes = std::pin::pin!(blob_changes(
                    Index::from_hash(NodeHash::from(*base.hash())),
                    Index::from_hash(NodeHash::from(*revision.tree.hash())),
                    blob_store.clone(),
                ));
                while let Some(change) = changes.next().await {
                    // Removals ship nothing; the remote keeps its bytes.
                    let BlobChange::Added(hash) = change? else {
                        continue;
                    };
                    let digest = dialog_common::Blake3Hash::from(hash);
                    // Size from the current tree's blob index (no byte fetch).
                    let record = current_index
                        .get_blob(&blob_store, &hash)
                        .await?
                        .ok_or_else(|| {
                            BlobError::ExecutionError(format!(
                                "blob {digest:?} referenced by the tree but absent from its index"
                            ))
                        })?;
                    // Local bytes -> remote import sink. Mirrors the remote
                    // `Read` fork in `branch/blob.rs` and `RemotePut`'s `Put`
                    // fork in `remote/archive.rs`, substituting the blob
                    // `Import` effect (single-part on the current providers).
                    let mut source = branch
                        .archive()
                        .blob()
                        .read(digest.clone())
                        .perform(env)
                        .await?;
                    let mut sink = address
                        .subject
                        .clone()
                        .archive()
                        .blob()
                        .import(digest.clone(), record.size)
                        .fork(address.site())
                        .perform(env)
                        .await?;
                    while let Some(chunk) = source.next().await? {
                        sink.write_all(&chunk).await?;
                    }
                    sink.finish().await?;
                }

                upstream.publish(revision.clone()).perform(env).await?;
            }
        }

        // Advance this upstream's recorded sync point to the just-pushed
        // tree. A target pushed explicitly for the first time gets tracked
        // here (appended, not made the default).
        let mut upstreams = branch.upstreams();
        upstreams.upsert(upstream_state.with_tree(revision.tree.clone()));
        branch.upstream.publish(upstreams).perform(env).await?;

        Ok(Some(revision))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::PushError;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        let feature_revision = feature.revision().expect("feature should have a revision");

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_some());

        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let main_rev = main_reloaded
            .revision()
            .expect("main should have a revision after push");
        assert_eq!(main_rev.tree, feature_revision.tree);

        Ok(())
    }

    /// A second push with no intervening commit is a no-op: the local head
    /// already equals the recorded upstream sync point, so it returns the
    /// current revision without re-publishing. Guards the ongoing-`revision`-PUT
    /// regression where an idle sync tick re-pushed on every drain.
    #[dialog_common::test]
    async fn it_is_a_noop_when_nothing_new_to_push() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:123".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let revision = feature.revision().expect("feature should have a revision");

        // First push lands the commit.
        let first = feature.push().perform(&operator).await?;
        assert_eq!(
            first.map(|r| r.tree),
            Some(revision.tree.clone()),
            "first push lands the local head"
        );

        // Second push, with no new commit, is a no-op that still reports the
        // current revision.
        let second = feature.push().perform(&operator).await?;
        assert_eq!(
            second.map(|r| r.tree),
            Some(revision.tree),
            "second push with nothing new returns the current revision as a no-op"
        );

        Ok(())
    }

    /// A branch can push to an upstream other than its default: the target
    /// advances, starts being tracked with its own sync base, and the
    /// default stays put.
    #[dialog_common::test]
    async fn it_pushes_to_a_non_default_upstream_and_tracks_it() -> Result<()> {
        use crate::Upstream;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let backup = repo.branch("backup").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:123".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let revision = feature.revision().expect("feature has a revision");

        // Bare push targets the default upstream (main)...
        feature.push().perform(&operator).await?;
        let main = repo.branch("main").load().perform(&operator).await?;
        assert_eq!(main.revision().map(|r| r.tree), Some(revision.tree.clone()));

        // ... and an explicit push targets another branch entirely.
        let pushed = feature.push().to(&backup).perform(&operator).await?;
        assert!(pushed.is_some());
        let backup = repo.branch("backup").load().perform(&operator).await?;
        assert_eq!(
            backup.revision().map(|r| r.tree),
            Some(revision.tree.clone())
        );

        // Backup is now tracked with its own sync base; main stays default.
        let upstreams = feature.upstreams();
        assert_eq!(upstreams.iter().count(), 2);
        assert!(matches!(
            upstreams.default_upstream(),
            Some(Upstream::Local { branch, .. }) if branch == "main"
        ));
        assert!(upstreams.iter().any(|entry| matches!(
            entry,
            Upstream::Local { branch, tree } if branch == "backup" && *tree == revision.tree
        )));

        // Pushing to the branch itself is refused.
        let selfish = feature.push().to(&feature).perform(&operator).await;
        assert!(matches!(selfish, Err(PushError::UpstreamIsItself { .. })));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_non_fast_forward_on_local_upstream_diverged() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@example.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = feature.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::NonFastForward { .. })),
            "Push should fail with NonFastForward when diverged, got: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_pushing_branch_without_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        let result = branch.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::BranchHasNoUpstream { .. })),
            "Push should fail with BranchHasNoUpstream, got: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_pushing_empty_branch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_none(), "Push with no revision should return None");

        Ok(())
    }
}
