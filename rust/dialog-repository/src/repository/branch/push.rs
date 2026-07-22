use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{BlobChange, BlobIndexExt as _, ShipmentRef, shipment_refs};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{BlobError, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, TreeDifference};
use dialog_storage::StorageBackend as _;
use futures_util::{StreamExt as _, TryStreamExt as _};

use crate::{
    Branch, Index, LocalIndex, PushError, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Revision, Upstream,
};

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
}

impl<'a> Push<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// Create a command to push local changes to the upstream branch.
    ///
    /// Reads the upstream configuration from branch state and dispatches
    /// to local or remote push logic.
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
        let upstream_state = branch
            .upstream()
            .ok_or_else(|| PushError::BranchHasNoUpstream {
                branch: branch.name().to_string(),
            })?;

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

                // Ship the blocks the tree nodes reference but the node upload
                // does not carry: blob bytes and spilled value blocks. Both
                // are surfaced by ONE entry-level drain of the SAME
                // differential the node upload just walked (`shipment_refs`),
                // so the changed paths are read once per push instead of once
                // per concern. Bytes must land on the remote before we publish
                // a revision that references them, so a failed upload here
                // aborts the push with the revision still unpublished.
                let blob_store = LocalIndex::new(env, index.clone());
                let current_index = Index::from_hash(NodeHash::from(*revision.tree.hash()));
                let address = remote.address();
                let mut refs = std::pin::pin!(shipment_refs(&difference));
                while let Some(shipment) = refs.next().await {
                    match shipment? {
                        // Removals ship nothing; the remote keeps its bytes.
                        ShipmentRef::Blob(BlobChange::Removed(_)) => {}
                        ShipmentRef::Blob(BlobChange::Added(hash)) => {
                            let digest = dialog_common::Blake3Hash::from(hash);
                            // Size from the current tree's blob index (no byte
                            // fetch).
                            let record = current_index
                                .get_blob(&blob_store, &hash)
                                .await?
                                .ok_or_else(|| {
                                    BlobError::ExecutionError(format!(
                                        "blob {digest:?} referenced by the tree but absent from its index"
                                    ))
                                })?;
                            // Local bytes -> remote import sink. Mirrors the
                            // remote `Read` fork in `branch/blob.rs` and
                            // `RemotePut`'s `Put` fork in `remote/archive.rs`,
                            // substituting the blob `Import` effect
                            // (single-part on the current providers).
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
                        // A value larger than the inline threshold lives as a
                        // content-addressed block (addressed by its 32-byte
                        // value reference) in the same store as the tree
                        // nodes. Local bytes -> remote block put, mirroring
                        // the novel node upload.
                        ShipmentRef::SpilledValue(reference) => {
                            let bytes = blob_store.get(&reference).await?.ok_or_else(|| {
                                BlobError::ExecutionError(format!(
                                    "spilled value block {reference:?} referenced by the tree but absent from the local archive"
                                ))
                            })?;
                            remote_index.put(Buffer::from(bytes)).perform(env).await?;
                        }
                    }
                }

                upstream.publish(revision.clone()).perform(env).await?;
            }
        }

        // Advance our recorded sync point to the just-pushed tree.
        branch
            .upstream
            .publish(upstream_state.with_tree(revision.tree.clone()))
            .perform(env)
            .await?;

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
    use futures_util::{StreamExt as _, stream};

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

    /// Pushing a spilling value ships its block to the local upstream, a
    /// spilled value shared by many facts ships once, and a re-push with
    /// nothing new is a no-op (no re-upload).
    #[dialog_common::test]
    async fn it_pushes_spilled_value_blocks_once() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let big = "z".repeat(inline_n + 1);
        let value = Value::String(big);

        // Two facts share the same large value -> one spilled block.
        feature
            .commit(stream::iter(vec![
                Instruction::Assert(Artifact {
                    the: "doc/body".parse()?,
                    of: "doc:a".parse()?,
                    is: value.clone(),
                    cause: None,
                }),
                Instruction::Assert(Artifact {
                    the: "doc/body".parse()?,
                    of: "doc:b".parse()?,
                    is: value.clone(),
                    cause: None,
                }),
            ]))
            .perform(&operator)
            .await?;

        let first = feature.push().perform(&operator).await?;
        assert!(first.is_some(), "the first push lands the commit");

        // The main branch (the upstream) can now read both facts back,
        // reconstructing the shared spilled value from the shipped block.
        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let results: Vec<_> = main_reloaded
            .claims()
            .select(dialog_artifacts::ArtifactSelector::new().the("doc/body".parse()?))
            .perform(&operator)
            .await?
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;
        assert_eq!(
            results.len(),
            2,
            "both facts hydrate from the shipped block"
        );
        assert!(
            results.iter().all(|r| r.is == value),
            "the shared spilled value reconstructs for both facts"
        );

        // A re-push with nothing new is a no-op.
        let second = feature.push().perform(&operator).await?;
        assert_eq!(
            second.map(|r| r.tree),
            first.map(|r| r.tree),
            "a re-push with nothing new returns the same revision"
        );

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
