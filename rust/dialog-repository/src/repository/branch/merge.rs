//! Merging a commit that lost the race for its branch's head.
//!
//! A commit builds on the head it read (the base) and publishes against
//! it. When another writer advanced the head first, the commit's revision
//! is already durable, just not the head. Merging keeps that revision
//! exactly as minted, editions and records included, and publishes a
//! merge of it with the head that won: the same merge a pull performs
//! between peers, with the roles fixed by the race. The base is known
//! exactly, the losing side is one commit, and both sides are local, so
//! nothing is fetched and no merge direction is chosen.
//!
//! Only a revision the winning head has not seen can be kept. A writer
//! racing itself on one branch mints the same edition twice, and the
//! loser's version is already taken; that race is refused instead.

use std::collections::BTreeSet;
use std::mem;
use std::sync::{Arc, Mutex};

use dialog_artifacts::DialogArtifactsError;
use dialog_artifacts::history::{Context, RevisionRecord};
use dialog_artifacts::merge;
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_capability::Provider;
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt as _};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, Delta};

use crate::{Branch, CommitError, Index, NetworkedIndex, PublishError, Revision, TreeReference};

/// How many times merging tries to publish its merge before giving
/// up: the merge itself can lose to yet another writer.
const RETRY_LIMIT: usize = 3;

/// Publish a merge of `mine`, a revision minted on `base` whose publish
/// lost, with whatever head `branch` holds now.
pub(crate) async fn merge_with_winner<Env>(
    branch: &Branch,
    base: Option<Revision>,
    mine: Revision,
    lost: PublishError,
    env: &Env,
) -> Result<Revision, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
{
    let mut lost = lost;
    for _ in 0..RETRY_LIMIT {
        branch.revision.resolve().perform(env).await?;
        let head = branch.revision.checkpoint();
        let Some(theirs) = branch.revision() else {
            // The head was emptied under the commit; there is nothing to
            // merge with, and adopting over a deletion is not ours to do.
            return Err(lost.into());
        };
        let Some(context) = context_of(branch, &theirs).await else {
            // A head minted before heads carried their context: its
            // ancestry is not known without a walk, so it is not merged.
            return Err(lost.into());
        };
        if context.observes(&mine.version()) {
            // The winner already holds this version: the same writer raced
            // itself, and the edition this commit minted is taken.
            return Err(lost.into());
        }

        let (merged, merged_context) =
            merged(branch, base.as_ref(), &mine, &theirs, context, env).await?;
        match head.publish(merged.clone(), env).await {
            Ok(()) => {
                branch.contexts().insert(merged.version(), merged_context);
                return Ok(merged);
            }
            Err(error @ PublishError::VersionMismatch { .. }) => lost = error,
            Err(error) => return Err(error.into()),
        }
    }
    Err(lost.into())
}

/// The context of `head`: the one it published, or the one remembered
/// for it.
async fn context_of(branch: &Branch, head: &Revision) -> Option<Context> {
    match &head.context {
        Some(context) => Some(context.clone()),
        None => branch.contexts().cached(&head.version()).await,
    }
}

/// Mint a revision whose tree is `theirs` with what `mine` changed since
/// `base` integrated onto it, and whose parents are both, with its
/// context.
async fn merged<Env>(
    branch: &Branch,
    base: Option<&Revision>,
    mine: &Revision,
    theirs: &Revision,
    context: Context,
    env: &Env,
) -> Result<(Revision, Context), CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
{
    let mut store = NetworkedIndex::new(env, branch.archive().index(), branch.fallback());
    let tree = |reference: &TreeReference| {
        Index::from_hash_with_cache(NodeHash::from(*reference.hash()), branch.node_cache())
    };
    let base_tree = tree(&base.map(|base| base.tree.clone()).unwrap_or_default());
    let mine_tree = tree(&mine.tree);
    let mut merged = tree(&theirs.tree);

    // What this commit changed since its base, history first so its
    // supersession records retire the claims they cover before its data
    // lands, screened against the winner as a pull screens an upstream.
    let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
    let screen_store = TreeStorage::new(TreeStorageBridge(store.clone()));
    let history_scope = merge::history_scope();
    let data_scope = merge::data_scope();
    let history = merge::screen_history(
        base_tree.differentiate_within_with(
            &mine_tree,
            &history_scope,
            &tree_store,
            &tree_store,
            dialog_search_tree::Prefetch::Eager,
        ),
        tree(&theirs.tree),
        screen_store,
    );
    let observed = Arc::new(Mutex::new(BTreeSet::new()));
    let data = merge::screen_data(
        merge::observe_revisions(
            base_tree.differentiate_within_with(
                &mine_tree,
                &data_scope,
                &tree_store,
                &tree_store,
                dialog_search_tree::Prefetch::Eager,
            ),
            observed.clone(),
        ),
        context.clone(),
    );
    let mut delta = Delta::zero();
    merged = Box::pin(
        merged
            .edit()
            .integrate(futures_util::StreamExt::chain(history, data), &tree_store),
    )
    .await?
    .persist(&mut delta)?;

    let mut merged_context = context;
    merged_context.absorb(mem::take(
        &mut *observed
            .lock()
            .expect("the revision observer mutex is never poisoned"),
    ));

    // The merge revision, minted as pull mints one: no skip table, the
    // record signed before it enters the tree, the head once its root is
    // final.
    let authority = Identify.perform(env).await?;
    let branch_entity = crate::branch_of(branch.of(), authority.profile(), branch.name());
    let mut revision = theirs.merge(
        mine,
        TreeReference::default(),
        branch_entity,
        authority.did(),
    );
    let mut record = RevisionRecord::create(
        &revision,
        authority.profile(),
        vec![theirs.version(), mine.version()],
        Vec::new(),
    );
    record.signature = Attest::new(record.payload()?).perform(env).await?;
    let manifest = merged.format_manifest(store.clone(), &delta).await?;
    merged
        .record(&mut store, &mut delta, record.entries(&manifest)?)
        .await?;
    revision.tree = TreeReference::from(*merged.root().as_bytes());
    merged_context.record(revision.version());
    revision.context = Some(merged_context.clone());
    revision.signature = Attest::new(revision.payload()).perform(env).await?;

    branch
        .archive()
        .index()
        .import(delta.flush().map(|(_, buffer)| buffer))
        .perform(env)
        .await
        .map_err(DialogArtifactsError::from)?;
    Ok((revision, merged_context))
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::registry::RegistryEnv;
    use crate::{Branch, CommitError, PublishError, PullError};
    use anyhow::Result;
    use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use futures_util::{StreamExt, stream};

    fn name(of: &str, is: &str) -> Result<Instruction> {
        Ok(Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: of.parse()?,
            is: Value::String(is.to_string()),
            cause: None,
        }))
    }

    async fn names<Env>(branch: &Branch, env: &Env) -> Result<Vec<Value>>
    where
        Env: RegistryEnv,
    {
        let mut values: Vec<Value> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .to_owned()
            .perform(env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|artifact| artifact.map(|artifact| artifact.is))
            .collect::<Result<_, _>>()?;
        values.sort_by_key(|value| format!("{value:?}"));
        Ok(values)
    }

    /// A commit that loses the race to another writer, asked to merge,
    /// is kept as it was minted and merged with the head that won: both
    /// writes survive and the head's parents are both revisions.
    #[dialog_common::test]
    async fn it_merges_a_commit_that_lost_to_another_writer() -> Result<()> {
        let (worker, peer) = test_session_with_peer().await;
        let repo = test_repo(&worker, &peer).await;
        let first = repo.branch("main").open().perform(&worker).await?;
        let second = repo.branch("main").open().perform(&peer).await?;

        let won = first
            .commit(stream::iter(vec![name("user:a", "Alice")?]))
            .perform(&worker)
            .await?;
        let merged = second
            .commit(stream::iter(vec![name("user:b", "Bob")?]))
            .merge()
            .perform(&peer)
            .await?;

        assert_ne!(merged.version(), won.version());
        assert_eq!(second.revision(), Some(merged.clone()));
        let fresh = repo.branch("main").open().perform(&peer).await?;
        assert_eq!(fresh.revision(), Some(merged));
        assert_eq!(
            names(&fresh, &peer).await?,
            vec![Value::String("Alice".into()), Value::String("Bob".into())]
        );
        Ok(())
    }

    /// Without `merge`, the same race is refused as before.
    #[dialog_common::test]
    async fn it_refuses_a_lost_race_unless_asked_to_merge() -> Result<()> {
        let (worker, peer) = test_session_with_peer().await;
        let repo = test_repo(&worker, &peer).await;
        let first = repo.branch("main").open().perform(&worker).await?;
        let second = repo.branch("main").open().perform(&peer).await?;

        first
            .commit(stream::iter(vec![name("user:a", "Alice")?]))
            .perform(&worker)
            .await?;
        let raced = second
            .commit(stream::iter(vec![name("user:b", "Bob")?]))
            .perform(&peer)
            .await;
        assert!(
            matches!(
                raced,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "{raced:?}"
        );
        Ok(())
    }

    /// A second handle of the same writer that lost the race builds on
    /// the head its writer moved rather than merging with it: its
    /// writer's own commit is not a concurrent change, and both writes
    /// land.
    #[dialog_common::test]
    async fn it_builds_on_a_head_its_own_writer_moved() -> Result<()> {
        let (worker, peer) = test_session_with_peer().await;
        let repo = test_repo(&worker, &peer).await;
        let first = repo.branch("main").open().perform(&worker).await?;
        let second = repo.branch("main").open().perform(&worker).await?;

        let won = first
            .commit(stream::iter(vec![name("user:a", "Alice")?]))
            .perform(&worker)
            .await?;
        let landed = second
            .commit(stream::iter(vec![name("user:b", "Bob")?]))
            .merge()
            .perform(&worker)
            .await?;

        assert_ne!(landed.version(), won.version());
        let fresh = repo.branch("main").open().perform(&worker).await?;
        assert_eq!(fresh.revision(), Some(landed));
        assert_eq!(
            names(&fresh, &worker).await?,
            vec![Value::String("Alice".into()), Value::String("Bob".into())]
        );
        Ok(())
    }

    /// A session pulling a branch in the background while it commits to
    /// the same branch writes under one origin both times. The two take
    /// turns rather than minting the same edition twice, so both land.
    #[dialog_common::test]
    async fn it_lands_a_commit_and_a_pull_of_one_session_together() -> Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let repo = test_repo(&session, &peer).await;
        let main = repo.branch("main").open().perform(&session).await?;
        main.commit(stream::iter(vec![name("user:a", "Alice")?]))
            .perform(&session)
            .await?;

        let syncing = repo.branch("feature").open().perform(&session).await?;
        syncing.pull_from(&main).perform(&session).await?;
        syncing.pull().perform(&session).await?;
        let writing = repo.branch("feature").open().perform(&session).await?;

        main.commit(stream::iter(vec![name("user:b", "Bob")?]))
            .perform(&session)
            .await?;
        let (pulled, committed) = futures_util::join!(
            syncing.pull().perform(&session),
            writing
                .commit(stream::iter(vec![name("user:c", "Carol")?]))
                .merge()
                .perform(&session),
        );
        committed?;
        // A pull whose head moved under it is refused, as it always is, and
        // pulled again from the head it missed.
        if let Err(error) = pulled {
            assert!(
                matches!(
                    error,
                    PullError::Publish(PublishError::VersionMismatch { .. })
                ),
                "{error:?}"
            );
            syncing.refresh(&session).await?;
            syncing.pull().perform(&session).await?;
        }

        let fresh = repo.branch("feature").open().perform(&session).await?;
        assert_eq!(
            names(&fresh, &session).await?,
            vec![
                Value::String("Alice".into()),
                Value::String("Bob".into()),
                Value::String("Carol".into())
            ]
        );
        Ok(())
    }
}
