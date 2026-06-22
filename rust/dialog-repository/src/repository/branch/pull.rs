use dialog_artifacts::DialogArtifactsError;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, Delta};

use crate::{
    Branch, EMPTY_TREE_HASH, Index, NetworkedIndex, PublishError, PullError, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference, Upstream,
};

/// Command struct for pulling from upstream (auto-dispatches local/remote).
pub struct Pull<'a> {
    branch: &'a Branch,
}

impl<'a> Pull<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// Pull from the configured upstream.
    pub fn pull(&self) -> Pull<'_> {
        Pull::new(self)
    }
}

impl Pull<'_> {
    /// Execute the pull operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
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
        let upstream = branch
            .upstream()
            .ok_or_else(|| PullError::BranchHasNoUpstream {
                branch: branch.name().to_string(),
            })?;

        // Resolve the upstream's current revision and, when the
        // upstream is remote, keep a handle so the merge can fall back
        // to the remote archive for blocks that aren't local.
        let (upstream_revision, remote) = match upstream {
            Upstream::Local { branch: id, .. } => {
                let upstream_branch = branch.subject().branch(id).load().perform(env).await?;
                (upstream_branch.revision(), None)
            }
            Upstream::Remote {
                remote: name,
                branch: branch_name,
                ..
            } => {
                let remote = branch.subject().remote(name).load().perform(env).await?;
                let upstream = remote.branch(branch_name).open().perform(env).await?;
                (upstream.fetch().perform(env).await?, Some(remote))
            }
        };

        // Upstream has never received a revision yet — nothing to
        // merge in, so the pull is a no-op.
        let Some(upstream_revision) = upstream_revision else {
            return Ok(None);
        };

        // `base` is the upstream tree at our last sync point (the
        // divergence marker). If it equals the upstream's current
        // tree, the upstream hasn't moved and there's nothing to pull.
        let base = branch
            .upstream()
            .map(|u| u.tree().clone())
            .unwrap_or_default();

        if base == upstream_revision.tree {
            return Ok(None);
        }

        // Checkpoint the head cell up front, capturing the version we read the
        // local revision at. The merge below is computed from this snapshot;
        // publishing through the checkpoint CAS's against *this* version, not
        // whatever the cell holds at publish time. So a commit that advances
        // the head mid-pull makes our publish fail rather than silently adopt
        // the new version and drop the commit (see `Cell::checkpoint`).
        let head = branch.revision.checkpoint();
        let local_revision = branch.revision();
        let local_tree_hash = local_revision
            .as_ref()
            .map(|revision| *revision.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);

        // `NetworkedIndex` reads from the local archive first and,
        // when the upstream is remote, falls back to the remote
        // archive for blocks that haven't been replicated. With
        // `remote: None` it degrades to a plain local index.
        let store = NetworkedIndex::new(env, branch.archive().index(), remote);

        // The three trees: last-sync base, local current, and the
        // upstream revision we're merging in. Hydration is lazy; blocks
        // load on demand as the differential walks them.
        let base = Index::from_hash(NodeHash::from(*base.hash()));
        let local = Index::from_hash(NodeHash::from(local_tree_hash));
        let mut merged = Index::from_hash(NodeHash::from(*upstream_revision.tree.hash()));

        // Replay local changes (base → local) on top of the upstream
        // tree to produce the merged tree. The differential only reads
        // blocks on paths where base and local actually differ.
        let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
        let local_changes = base.differentiate(&local, &tree_store, &tree_store);
        let mut delta = Delta::zero();
        merged = Box::pin(merged.edit().integrate(local_changes, &tree_store))
            .await?
            .persist(&mut delta)?;

        // Persist the merged tree's pending nodes to the local archive
        // before referencing its root in a revision. The whole flush
        // travels as one `Import` invocation: block buffers are
        // reference-counted (nothing is copied on the way in) and
        // providers with native batching persist it in a single round
        // trip.
        branch
            .archive()
            .index()
            .import(delta.flush().map(|(_, buffer)| buffer))
            .perform(env)
            .await
            .map_err(DialogArtifactsError::from)?;

        let merged_tree = TreeReference::from(*merged.root().as_bytes());

        let new_revision = match local_revision {
            // Merging produced the upstream tree verbatim
            // (fast-forward): adopt the upstream revision — there's
            // nothing novel to attribute.
            _ if merged_tree == upstream_revision.tree => upstream_revision.clone(),
            // Branch has no prior revision; adopt the upstream
            // revision directly (its identity still applies).
            None => upstream_revision.clone(),
            // Real three-way merge: mint a revision attributed to the
            // current authority combining both sides.
            Some(local) => {
                let authority = Identify.perform(env).await?;
                local.merge(
                    &upstream_revision,
                    merged_tree,
                    authority.did(),
                    authority.profile().clone(),
                )
            }
        };

        // Publish the merged revision as the branch's new head, through the
        // checkpoint — so the CAS is against the version we merged from. If a
        // commit advanced the head while we were merging, this fails with
        // `VersionMismatch` instead of silently overwriting that commit (our
        // merge was computed from a now-stale snapshot, so it must not land).
        // On success the checkpoint updates the shared cache, so the branch
        // handle sees the new head. The caller refreshes and re-pulls to
        // reconcile a mismatch.
        head.publish(new_revision.clone(), env).await?;

        // Advance the recorded sync base to the upstream tree we just merged
        // in, so the next pull/push uses it as the divergence marker.
        // Checkpointed just before the write, so its CAS is against the marker
        // as it stands now.
        //
        // The head publish above and this write are not one atomic step: a
        // concurrent pull could land its own (head + sync-base) pair in
        // between. If it did, the marker moved and our write would clobber a
        // consistent pair back to a stale base — so on a mismatch we DON'T
        // propagate the error. The other pull already established a valid
        // (head, base); we yield to it and return the head as it now stands,
        // rather than the revision we published (which has been superseded).
        if let Some(upstream) = branch.upstream() {
            let marker = branch.upstream.checkpoint();
            let publish = marker
                .publish(upstream.with_tree(upstream_revision.tree.clone()), env)
                .await;

            if let Err(PublishError::VersionMismatch { .. }) = publish {
                // Re-read the head a concurrent pull left in place and return
                // it. It must differ from what we published — if it matched,
                // the marker moved without the head, which would be an
                // inconsistent state we don't expect.
                let current = branch.revision();
                debug_assert!(
                    current.as_ref() != Some(&new_revision),
                    "upstream marker moved but head did not — inconsistent sync state"
                );
                return Ok(current);
            }
            publish?;
        }

        Ok(Some(new_revision))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:seed".parse()?,
            is: Value::String("Seed".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after pull");
        assert_eq!(feature_rev.tree, main_revision.tree);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after merge");
        assert_ne!(feature_rev.tree, main_revision.tree);
        Ok(())
    }

    /// A commit made through one branch handle, while a pull is being computed
    /// through *another handle to the same branch*, must not be silently lost —
    /// the pull fails loudly, and refreshing then re-pulling reconciles both
    /// changes.
    ///
    /// Each `open()` of a branch produces an independent handle whose revision
    /// cell caches the head it saw at open time. `pull` checkpoints its handle's
    /// head up front and, after merging, publishes the result CAS'd against the
    /// checkpointed version. If another handle commits in between, the storage
    /// head advances past the checkpoint, so the publish fails with a
    /// `VersionMismatch` rather than overwriting the commit with a tree built
    /// from the stale snapshot.
    ///
    /// This is the real shape in the service worker: the auto-sync pull and a
    /// local commit run against handles that don't share a revision-cache view.
    /// The recovery is exactly what a consumer does: `refresh` the handle to
    /// pick up the current head, then re-pull — the re-pull merges from the
    /// now-current snapshot and reuses the blocks the first attempt already
    /// fetched.
    #[dialog_common::test]
    async fn it_fails_a_pull_racing_a_commit_then_reconciles_on_refresh() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Upstream `main` has a change to pull in.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        // Two independent handles to the same `feature` branch — like the two
        // call sites in the worker that don't share a revision-cache view. Both
        // snapshot the same (empty) feature head at open time.
        let feature_pull = repo.branch("feature").open().perform(&operator).await?;
        feature_pull.set_upstream(&main).perform(&operator).await?;
        let feature_commit = repo.branch("feature").open().perform(&operator).await?;

        // A local commit lands through the *other* handle, advancing the
        // feature head in storage. `feature_pull`'s cache is now stale.
        feature_commit
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // The pull handle, unaware of that commit, pulls from upstream. It must
        // fail loudly (version mismatch) rather than silently drop the commit.
        let raced = feature_pull.pull().perform(&operator).await;
        assert!(
            matches!(
                raced,
                Err(crate::PullError::Publish(
                    crate::PublishError::VersionMismatch { .. }
                ))
            ),
            "a pull racing a commit must fail with a version mismatch, not drop the commit; got {raced:?}"
        );

        // Recovery: refresh the stale handle to pick up the current head, then
        // re-pull. This reconciles upstream with the raced commit.
        feature_pull.refresh(&operator).await?;
        feature_pull.pull().perform(&operator).await?;

        // Both changes are now present on the branch.
        let feature = repo.branch("feature").open().perform(&operator).await?;
        let committed: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/email".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(committed.len(), 1, "the raced commit must survive recovery");
        assert_eq!(committed[0].is, Value::String("feature@test.com".into()));

        let pulled: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            pulled.len(),
            1,
            "the pulled upstream change must survive too"
        );

        Ok(())
    }
}
