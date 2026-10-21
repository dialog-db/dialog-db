//! Moving what a branch stores to the shape this release writes.
//!
//! A release before this one keyed each number type in its own band of
//! the value index. This release keys every number in one band, in
//! dialog-number's encoding ([`NUMBER_BAND`]), and writes into no tree
//! laid out the earlier way: a commit on such a branch fails with
//! [`CommitError::NeedsMigration`]. The tree's manifest names the band
//! it is laid out in, and the field is critical, so a release before the
//! band refuses a migrated tree by name rather than misreading it.
//!
//! [`Branch::migrate_storage`] is the rewrite: every entry of the head's
//! tree, facts and history alike, re-keyed under the band (see
//! [`rekey_tree`]) into a tree created under the same manifest with the
//! band set, published as a revision that descends from the head. The
//! rewrite is deterministic, so two replicas that migrate the same head
//! produce the same tree, and nothing is lost: a datum names its cause
//! and a history record carries its claim, and neither depends on the
//! band.
//!
//! [`Branch::migrate`] is the whole move for a branch an earlier release
//! wrote: the storage rewrite, then [`Branch::upgrade_rules`], which
//! re-installs rules under the identities this release gives them.

use dialog_artifacts::tree::rekey::rekey_tree;
use dialog_artifacts::{ArchiveDelta, DialogArtifactsError, NUMBER_BAND};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSync, Holds};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::Manifest;
use futures_util::stream;

use crate::repository::archive::networked::Reads;
use crate::repository::archive::persist;
use crate::{Branch, CommitError, Index, NetworkedIndex, RemoteSite, Revision, RulesUpgraded};

/// What [`Branch::migrate`] did.
#[derive(Debug, Clone, Default)]
pub struct Migrated {
    /// The revision the storage rewrite published, or `None` when the
    /// head's tree was already laid out the way this release writes.
    pub storage: Option<Revision>,
    /// What the rule upgrade did.
    pub rules: RulesUpgraded,
}

impl Migrated {
    /// Whether anything moved.
    pub fn is_empty(&self) -> bool {
        self.storage.is_none()
            && self.rules.reinstalled.is_empty()
            && self.rules.rewritten.is_empty()
            && self.rules.remarked.is_empty()
    }
}

impl Branch {
    /// The value encoding the head's tree is laid out in: [`NUMBER_BAND`]
    /// for a tree this release wrote, `0` for one an earlier release
    /// wrote, and `None` for a branch with no revision.
    pub async fn storage_encoding<Env>(&self, env: &Env) -> Result<Option<u32>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let Some(revision) = self.revision() else {
            return Ok(None);
        };
        let store = NetworkedIndex::new(env, self.archive().index(), self.fallback());
        let tree =
            Index::from_hash_with_cache(NodeHash::from(*revision.tree.hash()), self.node_cache());
        let manifest = tree
            .manifest(&store)
            .await
            .map_err(DialogArtifactsError::from)?;
        Ok(Some(manifest.value_encoding))
    }

    /// Whether the head's tree is laid out the way an earlier release
    /// wrote, so that a write fails with [`CommitError::NeedsMigration`]
    /// until [`migrate`](Self::migrate) runs.
    pub async fn needs_migration<Env>(&self, env: &Env) -> Result<bool, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        Ok(self
            .storage_encoding(env)
            .await?
            .is_some_and(|encoding| encoding != NUMBER_BAND))
    }

    /// Rewrite the head's tree under `value_encoding` and publish the
    /// rewrite as a revision. [`NUMBER_BAND`] is the encoding this release
    /// writes, and [`migrate_storage`](Self::migrate_storage) is the
    /// rewrite to it; `0` lays the tree out the way the release before the
    /// band did, which is how a test produces a branch as that release
    /// left it.
    pub fn rekey_storage(&self, value_encoding: u32) -> RekeyStorage<'_> {
        RekeyStorage {
            branch: self,
            value_encoding,
        }
    }

    /// Rewrite the head's tree under the band this release writes. See
    /// the [module documentation](self).
    pub fn migrate_storage(&self) -> RekeyStorage<'_> {
        self.rekey_storage(NUMBER_BAND)
    }

    /// Move everything an earlier release stored on this branch to the
    /// shape this release reads and writes: the storage rewrite, then
    /// the rule upgrade. See the [module documentation](self).
    pub fn migrate(&self) -> Migrate<'_> {
        Migrate { branch: self }
    }
}

/// Command created by [`Branch::rekey_storage`].
pub struct RekeyStorage<'a> {
    branch: &'a Branch,
    value_encoding: u32,
}

impl RekeyStorage<'_> {
    /// Rewrite the head's tree and publish the rewrite. `None` when the
    /// branch has no revision or its tree is already laid out under the
    /// encoding asked for.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, CommitError>
    where
        Env: Provider<BlobImport>
            + Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let Some(revision) = branch.revision() else {
            return Ok(None);
        };
        let store = NetworkedIndex::erased(env, branch.archive().index(), branch.fallback());
        let tree =
            Index::from_hash_with_cache(NodeHash::from(*revision.tree.hash()), branch.node_cache());
        let manifest = tree
            .manifest(&store)
            .await
            .map_err(DialogArtifactsError::from)?;
        if manifest.value_encoding == self.value_encoding {
            return Ok(None);
        }
        let target = Manifest {
            value_encoding: self.value_encoding,
            ..manifest
        };
        // The rewrite's nodes are persisted before the revision that
        // names them is minted: a revision points only at durable blocks.
        let mut delta = ArchiveDelta::zero();
        let rewritten = rekey_tree(&tree, &store, &mut delta, target, branch.node_cache()).await?;
        persist(
            &branch.archive().index(),
            &branch.node_cache(),
            &mut delta,
            env,
        )
        .await?;
        // Boxed: the commit is a large future, and this one runs once.
        let revision = Box::pin(
            branch
                .commit(stream::empty())
                .rebuilding(rewritten)
                .allow_empty()
                .perform(env),
        )
        .await?;
        Ok(Some(revision))
    }
}

/// Command created by [`Branch::migrate`].
pub struct Migrate<'a> {
    branch: &'a Branch,
}

impl Migrate<'_> {
    /// Rewrite the storage, then upgrade the rules, and publish both.
    pub async fn perform<Env>(self, env: &Env) -> Result<Migrated, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let storage = Box::pin(self.branch.migrate_storage().perform(env)).await?;
        let rules = self.branch.upgrade_rules().perform(env).await?;
        Ok(Migrated { storage, rules })
    }
}

/// `tree`, laid out in the band this release writes: the tree itself when
/// its manifest names the band, else its rewrite under the band, persisted
/// into the branch's archive and memoized by root in the caches the
/// environment holds for the repository.
///
/// A pull that merges aligns its sync base, the upstream head and the
/// local head before it diffs or grafts them, so a tree a release before
/// the band wrote (an old client's head, or the sync base left by one)
/// meets the others in one encoding: the diff of two trees in different
/// encodings finds every number changed. The rewrite is deterministic,
/// so the rewrite of a base and of the head a replica minted over the
/// same rewrite share their unchanged subtrees, and the graft adopts them
/// by hash as it would any other. A pull that adopts a head by root reads
/// nothing and adopts it as it is; the next read or write that opens the
/// branch migrates it.
pub(crate) async fn aligned<Env>(
    branch: &Branch,
    tree: Index,
    store: &NetworkedIndex<'_, dyn Reads>,
    env: &Env,
) -> Result<Index, DialogArtifactsError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<BlobImport>
        + Provider<BlobRead>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let manifest = tree.manifest(store).await?;
    if manifest.value_encoding == NUMBER_BAND {
        return Ok(tree);
    }
    let root = tree.root().clone();
    if let Some(rewritten) = branch.rewrites().get_cached(&root) {
        return Ok(Index::from_hash_with_cache(rewritten, branch.node_cache()));
    }
    let target = Manifest {
        value_encoding: NUMBER_BAND,
        ..manifest
    };
    let mut delta = ArchiveDelta::zero();
    let rewritten = rekey_tree(&tree, store, &mut delta, target, branch.node_cache()).await?;
    persist(
        &branch.archive().index(),
        &branch.node_cache(),
        &mut delta,
        env,
    )
    .await?;
    branch.rewrites().insert(root, rewritten.root().clone());
    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{
        ArtifactSelector, Entity, NUMBER_BAND, Pick, Relation as ArtifactsRelation, Value,
    };
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Relation;
    use dialog_query::{AttributeStatement, Cardinality};
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::TryStreamExt as _;

    use crate::helpers::test_repo;
    use crate::{Branch, CommitError};

    type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

    fn size() -> ArtifactsRelation {
        "item/size".parse().expect("a relation")
    }

    fn statement(of: &Entity, is: Value, pick: Pick) -> AttributeStatement {
        AttributeStatement {
            the: Relation::from(size()),
            of: of.clone(),
            is,
            cause: None,
            cardinality: Some(if pick == Pick::All {
                Cardinality::Many
            } else {
                Cardinality::One
            }),
            pick: Some(pick),
        }
    }

    async fn commit(
        branch: &Branch,
        operator: &Operator,
        facts: Vec<AttributeStatement>,
    ) -> Result<()> {
        let mut transaction = branch.transaction();
        for fact in facts {
            transaction = transaction.assert(fact);
        }
        transaction.commit().publish().perform(operator).await?;
        branch.refresh(operator).await?;
        Ok(())
    }

    /// Every `item/size` the branch holds, sorted by its number.
    async fn sizes(branch: &Branch, operator: &Operator) -> Result<Vec<(Entity, Value)>> {
        let rows = branch
            .claims()
            .select(ArtifactSelector::new().the(size()))
            .perform(operator)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut sizes: Vec<(Entity, Value)> = rows
            .into_iter()
            .map(|view| view.to_owned().map(|fact| (fact.of, fact.is)))
            .collect::<Result<_, _>>()?;
        sizes.sort_by_key(|(entity, _)| entity.to_string());
        Ok(sizes)
    }

    /// A branch as the release before the band left it: its head's tree
    /// keys each number type in its own band. Facts of every number type,
    /// a succession and a retraction give it history to carry across.
    async fn legacy_branch(operator: &Operator, branch: &Branch) -> Result<Vec<(Entity, Value)>> {
        let items: Vec<Entity> = (0..4).map(|_| Entity::new()).collect::<Result<_, _>>()?;
        commit(
            branch,
            operator,
            vec![
                statement(&items[0], Value::UnsignedInt(3), Pick::All),
                statement(&items[1], Value::SignedInt(-2), Pick::All),
                statement(&items[2], Value::Float(2.5), Pick::All),
                statement(&items[3], Value::UnsignedInt(10), Pick::Max),
            ],
        )
        .await?;
        // A succession under `max` and a retraction, so the history and
        // coverage regions hold records the rewrite must carry.
        commit(
            branch,
            operator,
            vec![statement(&items[3], Value::UnsignedInt(40), Pick::Max)],
        )
        .await?;
        branch
            .transaction()
            .retract(statement(&items[2], Value::Float(2.5), Pick::All))
            .commit()
            .publish()
            .perform(operator)
            .await?;
        branch.refresh(operator).await?;
        let before = sizes(branch, operator).await?;

        let rewritten = branch.rekey_storage(0).perform(operator).await?;
        assert!(
            rewritten.is_some(),
            "the head is rewritten under the per-type bands"
        );
        branch.refresh(operator).await?;
        assert_eq!(branch.storage_encoding(operator).await?, Some(0));
        assert_eq!(
            sizes(branch, operator).await?,
            before,
            "the rewrite keeps every fact"
        );
        Ok(before)
    }

    /// A branch the release before the band wrote is refused a write
    /// until it is migrated, with nothing written by the refusal; the
    /// migration rewrites its head under the band, keeps every fact, and
    /// a second run finds nothing to do.
    #[dialog_common::test]
    async fn a_branch_an_earlier_release_wrote_is_migrated_once() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(branch.storage_encoding(&operator).await?, None);
        let before = legacy_branch(&operator, &branch).await?;
        assert!(branch.needs_migration(&operator).await?);
        let head = branch.revision();

        let refused = commit(
            &branch,
            &operator,
            vec![statement(&Entity::new()?, Value::UnsignedInt(7), Pick::All)],
        )
        .await
        .expect_err("a write into a tree the earlier release laid out is refused");
        assert!(
            matches!(
                refused.downcast_ref::<CommitError>(),
                Some(CommitError::NeedsMigration(_))
            ),
            "{refused:?}"
        );
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), head, "a refused write moves nothing");
        assert_eq!(sizes(&branch, &operator).await?, before);

        let migrated = branch.migrate().perform(&operator).await?;
        branch.refresh(&operator).await?;
        assert!(migrated.storage.is_some(), "the storage is rewritten");
        assert!(!branch.needs_migration(&operator).await?);
        assert_eq!(branch.storage_encoding(&operator).await?, Some(NUMBER_BAND));
        assert_eq!(
            sizes(&branch, &operator).await?,
            before,
            "every fact survives"
        );
        assert_eq!(
            branch.revision().map(|revision| revision.version()),
            migrated.storage.map(|revision| revision.version()),
            "the migration is the head"
        );

        let again = branch.migrate().perform(&operator).await?;
        assert!(again.is_empty(), "a second run finds nothing: {again:?}");

        commit(
            &branch,
            &operator,
            vec![statement(&Entity::new()?, Value::UnsignedInt(7), Pick::All)],
        )
        .await?;
        assert_eq!(sizes(&branch, &operator).await?.len(), before.len() + 1);
        Ok(())
    }

    /// A replica on this release pulls an upstream a replica on the
    /// release before the band wrote: first with nothing of its own, which
    /// adopts the head as it is (no block is read) and leaves the
    /// migration to the next read that opens the branch; then with novelty
    /// of its own while the upstream moved again the earlier way, which
    /// merges through the rewrite, so the earlier replica's later writes
    /// are not lost and the merged head is laid out in the band.
    #[dialog_common::test]
    async fn it_pulls_an_upstream_an_earlier_release_wrote() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        // `main` is the earlier replica: its head is laid out the earlier
        // way, with facts of every number type and history.
        let main = repo.branch("main").open().perform(&operator).await?;
        let first = legacy_branch(&operator, &main).await?;

        // `feature` tracks it and has nothing of its own: a first contact.
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;
        feature.refresh(&operator).await?;
        assert_eq!(
            feature.storage_encoding(&operator).await?,
            Some(0),
            "a fast-forward adopts the head as it is, reading nothing"
        );
        assert!(feature.needs_migration(&operator).await?);
        assert_eq!(sizes(&feature, &operator).await?, first);
        feature.migrate().perform(&operator).await?;
        feature.refresh(&operator).await?;
        assert_eq!(
            feature.storage_encoding(&operator).await?,
            Some(NUMBER_BAND)
        );

        // The earlier replica writes more, still the earlier way: its
        // head moves through the band and back, as an old client's
        // commit leaves it.
        main.migrate_storage().perform(&operator).await?;
        main.refresh(&operator).await?;
        let late = Entity::new()?;
        commit(
            &main,
            &operator,
            vec![statement(&late, Value::UnsignedInt(99), Pick::All)],
        )
        .await?;
        main.rekey_storage(0).perform(&operator).await?;
        main.refresh(&operator).await?;
        assert_eq!(main.storage_encoding(&operator).await?, Some(0));

        // This replica writes too, so the pull merges both sides.
        let own = Entity::new()?;
        commit(
            &feature,
            &operator,
            vec![statement(&own, Value::Float(0.5), Pick::All)],
        )
        .await?;
        feature.pull().perform(&operator).await?;
        feature.refresh(&operator).await?;
        assert_eq!(
            feature.storage_encoding(&operator).await?,
            Some(NUMBER_BAND)
        );
        let merged = sizes(&feature, &operator).await?;
        assert_eq!(merged.len(), first.len() + 2, "{merged:?}");
        assert!(
            merged
                .iter()
                .any(|(of, is)| of == &late && is.same_number(&Value::UnsignedInt(99))),
            "the earlier replica's later write arrived: {merged:?}"
        );
        assert!(
            merged
                .iter()
                .any(|(of, is)| of == &own && is.same_number(&Value::Float(0.5))),
            "this replica's own write stands: {merged:?}"
        );
        for (of, is) in &first {
            assert!(
                merged.iter().any(|(o, i)| o == of && i.same_number(is)),
                "{of} {is:?} survives the merge"
            );
        }
        Ok(())
    }
}
