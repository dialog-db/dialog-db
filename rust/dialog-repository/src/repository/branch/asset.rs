//! Assets: content a line stores in its blob store and records as a fact.
//!
//! An [`Asset`] names content by its hash and size, and its entity is
//! `asset:<hash>`. Asserting an asset on a transaction stores it: the commit
//! writes the bytes through the blob store and records
//! `asset:<hash> dialog.asset/size <size>` in the same revision as the
//! transaction's other facts, so those facts may point at the asset's entity.
//! Retracting the asset retracts that fact, dropping the line's reference to
//! the bytes. Push ships an asset's bytes with the revision that records it,
//! and a replica hydrates them through the entity like any blob.
//!
//! Content held in memory is asserted directly. Content too large to hold is
//! imported first: [`Branch::asset`] takes a stream of chunks, and its
//! [`import`](AssetStream::import) streams them into the blob store and
//! returns the asset naming them, which is then asserted:
//!
//! ```no_run
//! # use dialog_artifacts::Asset;
//! # use dialog_effects::blob::BlobError;
//! # use dialog_repository::Branch;
//! # async fn example<Env>(
//! #     branch: &Branch,
//! #     env: &Env,
//! #     photo: Vec<u8>,
//! #     chunks: futures_util::stream::Iter<std::vec::IntoIter<Result<Vec<u8>, BlobError>>>,
//! # ) -> anyhow::Result<()>
//! # where
//! #     Env: dialog_capability::Provider<dialog_effects::blob::Write>
//! #         + dialog_common::ConditionalSync
//! #         + 'static,
//! # {
//! let photo = Asset::from(photo);
//! let video = branch.asset(chunks).import().perform(env).await?;
//! let transaction = branch.transaction().assert(photo).assert(video);
//! # let _ = transaction;
//! # Ok(())
//! # }
//! ```
//!
//! Recording and dropping an asset are both a transaction's job:
//! `tx.assert(asset)` records it and `tx.retract(asset)` drops it.

use crate::repository::archive::networked::write_blob;
use crate::repository::branch::blob::index_store;
use crate::repository::source::SourceRef;
use crate::{Branch, CommitError, Hydrate, Index, Snapshot};
use dialog_artifacts::{Asset, AssetChange, BlobIndexExt as _, Instruction};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{
    BlobError, Import as BlobImport, Read as BlobRead, Size as BlobSize, Write as BlobWrite,
};
use dialog_effects::memory::Resolve;
use futures_util::{Stream, StreamExt};

impl Branch {
    /// Content to import into this branch's blob store as an asset: a
    /// stream of byte chunks. See [`AssetStream::import`].
    pub fn asset<S>(&self, chunks: S) -> AssetStream<'_, S> {
        AssetStream {
            source: SourceRef::from(self),
            chunks,
        }
    }
}

impl Snapshot {
    /// Content to import into this snapshot's blob store as an asset: a
    /// stream of byte chunks. See [`AssetStream::import`].
    pub fn asset<S>(&self, chunks: S) -> AssetStream<'_, S> {
        AssetStream {
            source: SourceRef::from(self),
            chunks,
        }
    }
}

/// A stream of byte chunks bound to a line's blob store, ready to import as
/// an asset. Created by [`Branch::asset`] or [`Snapshot::asset`].
pub struct AssetStream<'a, S> {
    source: SourceRef<'a>,
    chunks: S,
}

impl<'a, S> AssetStream<'a, S> {
    /// Import the chunks into the line's blob store as an [`Asset`].
    ///
    /// The import only writes bytes: it records nothing and advances no
    /// line, so it works on a snapshot as on a branch. Assert the returned
    /// asset on a transaction to record it, in the same revision as facts
    /// pointing at its entity.
    pub fn import(self) -> ImportAsset<'a, S> {
        ImportAsset {
            source: self.source,
            chunks: self.chunks,
        }
    }
}

/// Stream bytes into a line's blob store as an [`Asset`]. Created by
/// [`AssetStream::import`].
pub struct ImportAsset<'a, S> {
    source: SourceRef<'a>,
    chunks: S,
}

impl<S> ImportAsset<'_, S>
where
    S: Stream<Item = Result<Vec<u8>, BlobError>> + ConditionalSend + Unpin,
{
    /// Execute the import, returning the asset naming the stored bytes.
    ///
    /// The bytes stream through the blob store, which hashes them as they
    /// are written, so they are never held whole in memory.
    pub async fn perform<Env>(mut self, env: &Env) -> Result<Asset, CommitError>
    where
        Env: Provider<BlobWrite> + ConditionalSync + 'static,
    {
        let mut sink = self.source.archive().blob().write().perform(env).await?;
        let mut size: u64 = 0;
        while let Some(chunk) = self.chunks.next().await {
            let chunk = chunk?;
            size += chunk.len() as u64;
            sink.write_all(&chunk).await?;
        }
        let hash = sink.finish().await?;
        Ok(Asset::stored(*hash.as_bytes(), size))
    }
}

/// Store the assets a transaction changes and return the facts recording
/// them, for the commit to apply under machinery scope (see
/// [`Commit::with_machinery`](crate::Commit)).
///
/// An imported asset carrying its bytes is imported into `source`'s blob
/// store under the asset's hash and size; the store verifies the bytes
/// against both and keeps nothing on a mismatch, which fails the commit. An
/// imported asset naming stored bytes, such as an upload's, is checked to be
/// reachable at the size it names. Either way the bytes are durable before
/// the revision recording them is minted, the order
/// [`WriteBlob`](crate::WriteBlob) keeps. Each import yields its
/// `dialog.asset/size` fact.
///
/// A discard is keyed on the hash alone: it retracts the size the line
/// records for that hash, whatever size the discarded asset names, and
/// yields nothing when the line records none.
pub(crate) async fn store_assets<Env>(
    source: SourceRef<'_>,
    changes: Vec<AssetChange>,
    env: &Env,
) -> Result<Vec<Instruction>, CommitError>
where
    Env: Provider<BlobImport>
        + Provider<BlobRead>
        + Provider<BlobSize>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let mut instructions = Vec::with_capacity(changes.len());
    for change in changes {
        match change {
            AssetChange::Import(asset) => {
                match asset.content() {
                    Some(content) => write_asset(source, &asset, content, env).await?,
                    None => check_stored_asset(source, &asset, env).await?,
                }
                // A replace, not an assert: an asset has one size, and
                // replacing a fact with the value it already has is a no-op,
                // so re-asserting a recorded asset mints nothing.
                instructions.push(Instruction::Assert(
                    asset.fact()?,
                    dialog_artifacts::Policy::Last,
                ));
            }
            AssetChange::Discard(asset) => {
                if let Some(size) = recorded_size(source, asset.hash(), env).await? {
                    instructions.push(Instruction::Retract(
                        Asset::stored(*asset.hash(), size).fact()?,
                    ));
                }
            }
        }
    }
    Ok(instructions)
}

/// The size `source`'s current tree records for the asset `hash`, if it
/// records that asset.
pub(crate) async fn recorded_size<Env>(
    source: SourceRef<'_>,
    hash: &dialog_storage::Blake3Hash,
    env: &Env,
) -> Result<Option<u64>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let Some(revision) = source.revision() else {
        return Ok(None);
    };
    let store = index_store(source, env).await;
    Ok(Index::from_hash(NodeHash::from(*revision.tree.hash()))
        .asset_size(&store, hash)
        .await?)
}

/// Import `content` into `source`'s blob store under `asset`'s hash and
/// size. The store verifies the bytes as they land and keeps nothing when
/// they hash to anything else.
async fn write_asset<Env>(
    source: SourceRef<'_>,
    asset: &Asset,
    content: &[u8],
    env: &Env,
) -> Result<(), CommitError>
where
    Env: Provider<BlobImport> + ConditionalSync + 'static,
{
    write_blob(
        env,
        &source.archive().index(),
        &Blake3Hash::from(*asset.hash()),
        content,
    )
    .await?;
    Ok(())
}

/// Check that a stored asset's bytes are reachable from `source` at the
/// size it names, before any revision records it.
///
/// Reachable means one of two things. The local blob store holds the bytes:
/// it keys them by their verified hash, so presence proves the hash, and it
/// reports their length without the bytes being read. Or the line already
/// records this asset at this size, so its bytes are held by reference and
/// hydrate from the remote on demand, as after a pull; re-asserting such an
/// asset changes nothing. Anything else, bytes this replica has never held
/// for an asset its line never recorded, fails the commit rather than
/// recording content nobody here can read.
async fn check_stored_asset<Env>(
    source: SourceRef<'_>,
    asset: &Asset,
    env: &Env,
) -> Result<(), CommitError>
where
    Env: Provider<BlobRead>
        + Provider<BlobSize>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Hydrate>
        + ConditionalSync
        + 'static,
{
    let digest = Blake3Hash::from(*asset.hash());
    let local = source
        .archive()
        .blob()
        .size(digest.clone())
        .perform(env)
        .await?;
    let held = match local {
        Some(size) => size,
        None => match recorded_size(source, asset.hash(), env).await? {
            Some(size) => size,
            None => return Err(BlobError::NotFound(digest.to_string()).into()),
        },
    };
    if held != asset.size() {
        return Err(BlobError::SizeMismatch {
            digest: digest.to_string(),
            expected: asset.size(),
            held,
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{Counting, test_repo};
    use crate::repository::source::SourceRef;
    use crate::{Blob, Branch, CommitError};
    use anyhow::Result;
    use dialog_artifacts::{
        ASSET_SIZE, Artifact, ArtifactSelector, Asset, Attribute, Changes, DialogArtifactsError,
        Entity, Update as _, Value,
    };
    use dialog_effects::blob::{BlobError, BlobReader};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_peer::{Peer, Session};
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::{StreamExt as _, stream};

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    fn avatar() -> Attribute {
        "profile/avatar".parse().expect("valid attribute")
    }

    fn alice() -> Entity {
        "user:alice".parse().expect("valid entity")
    }

    /// A fact batch pointing `user:alice`'s avatar at `content`.
    fn avatar_of_alice(content: Entity) -> Changes {
        let mut facts = Changes::new();
        facts.associate(
            avatar(),
            alice(),
            Value::Entity(content),
            dialog_artifacts::Policy::Last,
        );
        facts
    }

    fn chunked(payload: &[u8], size: usize) -> Vec<Result<Vec<u8>, BlobError>> {
        payload.chunks(size).map(|c| Ok(c.to_vec())).collect()
    }

    /// The facts `branch` holds about `entity`.
    async fn facts_of(
        branch: &Branch,
        operator: &Peer<VolatileSpace, Session>,
        entity: &Entity,
    ) -> Result<Vec<Artifact>> {
        Ok(branch
            .claims()
            .select(ArtifactSelector::new().of(entity.clone()))
            .to_owned()
            .perform(operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// The size an asset's fact records on `branch`, if it records one.
    async fn recorded_size(
        branch: &Branch,
        operator: &Peer<VolatileSpace, Session>,
        asset: &Asset,
    ) -> Result<Option<Value>> {
        Ok(facts_of(branch, operator, &asset.entity()?)
            .await?
            .into_iter()
            .find(|fact| fact.the.as_str() == ASSET_SIZE)
            .map(|fact| fact.is))
    }

    /// Asserting an asset stores its bytes and records it as a fact on its
    /// entity, in the same revision as a fact pointing at that entity. The
    /// entity then resolves to the bytes and their size.
    #[dialog_common::test]
    async fn it_stores_an_asserted_asset() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload: Vec<u8> = (0..20_000u32).map(|i| (i % 241) as u8).collect();
        let photo = Asset::from(payload.clone());
        let entity = photo.entity()?;

        branch
            .transaction()
            .assert(photo.clone())
            .assert(avatar_of_alice(entity.clone()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let avatar_facts = facts_of(&branch, &operator, &alice()).await?;
        assert_eq!(avatar_facts.len(), 1);
        assert_eq!(avatar_facts[0].is, Value::Entity(entity.clone()));
        assert_eq!(
            recorded_size(&branch, &operator, &photo).await?,
            Some(Value::UnsignedInt(payload.len() as u128))
        );

        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(&operator)
                .await?,
            Some(payload.len() as u64)
        );
        let reader = Blob::from(entity)
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload);
        Ok(())
    }

    /// An import streams bytes into the blob store and records nothing;
    /// asserting the asset it returns records it.
    #[dialog_common::test]
    async fn it_records_an_imported_asset_when_asserted() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 233) as u8).collect();
        let video = branch
            .asset(stream::iter(chunked(&payload, 8192)))
            .import()
            .perform(&operator)
            .await?;
        assert_eq!(video.hash(), Asset::from(payload.clone()).hash());
        assert_eq!(video.size(), payload.len() as u64);
        assert_eq!(video.content(), None, "an import holds no bytes in memory");
        assert_eq!(branch.revision(), None, "an import records nothing");

        branch
            .transaction()
            .assert(video.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            recorded_size(&branch, &operator, &video).await?,
            Some(Value::UnsignedInt(payload.len() as u128))
        );
        let reader = Blob::from(video.entity()?)
            .read((&branch).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, payload);
        Ok(())
    }

    /// A stored asset naming a size its bytes do not have fails the commit,
    /// so a recorded size is always the stored bytes' length.
    #[dialog_common::test]
    async fn it_refuses_a_stored_asset_of_the_wrong_size() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let imported = branch
            .asset(stream::iter(chunked(b"eleven bytes", 4)))
            .import()
            .perform(&operator)
            .await?;
        let misstated = Asset::stored(*imported.hash(), imported.size() + 1);

        let refused = branch
            .transaction()
            .assert(misstated)
            .commit()
            .publish()
            .perform(&operator)
            .await;
        assert!(
            matches!(
                refused,
                Err(CommitError::Blob(BlobError::SizeMismatch { expected, held, .. }))
                    if expected == imported.size() + 1 && held == imported.size()
            ),
            "a misstated size fails the commit, got {refused:?}"
        );
        assert_eq!(branch.revision(), None, "the head does not move");
        Ok(())
    }

    /// Checking a stored asset's size asks the blob store for the size and
    /// never reads the bytes back.
    #[dialog_common::test]
    async fn it_checks_a_stored_asset_without_reading_its_bytes() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 227) as u8).collect();
        let imported = branch
            .asset(stream::iter(chunked(&payload, 4096)))
            .import()
            .perform(&operator)
            .await?;

        let counting = Counting::new(operator.clone());
        branch
            .transaction()
            .assert(imported.clone())
            .commit()
            .publish()
            .perform(&counting)
            .await?;

        assert_eq!(
            counting.count("blob::Read"),
            0,
            "the check reads no bytes: {:?}",
            counting.snapshot()
        );
        assert_eq!(counting.count("blob::Size"), 1);
        assert_eq!(
            recorded_size(&branch, &operator, &imported).await?,
            Some(Value::UnsignedInt(payload.len() as u128))
        );
        Ok(())
    }

    /// Carried bytes are imported under the asset's hash, so bytes that hash
    /// to anything else are refused by the store as they land, and nothing
    /// is kept under either hash.
    #[dialog_common::test]
    async fn it_refuses_carried_bytes_that_do_not_hash_to_the_asset() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let declared = Asset::from(b"the declared bytes".to_vec());
        let carried = b"some other bytes".to_vec();
        let refused =
            super::write_asset(SourceRef::from(&branch), &declared, &carried, &operator).await;

        assert!(
            matches!(
                refused,
                Err(CommitError::Blob(BlobError::DigestMismatch { .. }))
            ),
            "got {refused:?}"
        );
        for hash in [*declared.hash(), *Asset::from(carried).hash()] {
            let held = branch
                .archive()
                .index()
                .archive()
                .blob()
                .size(hash)
                .perform(&operator)
                .await?;
            assert_eq!(held, None, "nothing is stored under either hash");
        }
        Ok(())
    }

    /// An asset dispatched as a transient would never commit, so its bytes
    /// and fact would be dropped: the commit is refused instead, and
    /// nothing in it lands.
    #[dialog_common::test]
    async fn it_refuses_an_asset_dispatched_as_a_transient() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let head = branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let asset = Asset::from(b"a transient asset".to_vec());
        let refused = branch
            .transaction()
            .assert(avatar_of_alice(asset.entity()?))
            .dispatch(asset.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await;

        assert!(
            matches!(
                refused,
                Err(CommitError::Artifact(
                    DialogArtifactsError::AssetsUnsupported(_)
                ))
            ),
            "got {refused:?}"
        );
        assert_eq!(branch.revision(), Some(head), "the head does not move");
        assert_eq!(recorded_size(&branch, &operator, &asset).await?, None);
        let held = branch
            .archive()
            .index()
            .archive()
            .blob()
            .size(*asset.hash())
            .perform(&operator)
            .await?;
        assert_eq!(held, None, "the asset's bytes were not stored");
        Ok(())
    }

    /// A stored asset whose bytes the blob store does not hold fails the
    /// commit instead of recording content nobody can read.
    #[dialog_common::test]
    async fn it_refuses_a_stored_asset_the_store_does_not_hold() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let refused = branch
            .transaction()
            .assert(Asset::stored([5u8; 32], 10))
            .commit()
            .publish()
            .perform(&operator)
            .await;
        assert!(matches!(
            refused,
            Err(CommitError::Blob(BlobError::NotFound(_)))
        ));
        Ok(())
    }

    /// An unreachable asset fails the whole transaction: the facts asserted
    /// beside it are not recorded, the head does not move, and nothing about
    /// the asset is recorded, even on a line that already has history.
    #[dialog_common::test]
    async fn it_refuses_the_whole_transaction_for_an_unreachable_asset() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let head = branch.revision();

        let unreachable = Asset::stored([6u8; 32], 10);
        let refused = branch
            .transaction()
            .assert(unreachable.clone())
            .assert(avatar_of_alice(unreachable.entity()?))
            .commit()
            .publish()
            .perform(&operator)
            .await;

        assert!(matches!(
            refused,
            Err(CommitError::Blob(BlobError::NotFound(_)))
        ));
        assert_eq!(branch.revision(), head, "the head does not move");
        assert_eq!(recorded_size(&branch, &operator, &unreachable).await?, None);
        let avatar_facts = facts_of(&branch, &operator, &alice()).await?;
        assert_eq!(
            avatar_facts.iter().map(|fact| &fact.is).collect::<Vec<_>>(),
            vec![&Value::Entity(alice())],
            "the fact pointing at the asset was not recorded"
        );
        Ok(())
    }

    /// A staged batch refuses an unreachable asset at the link that asserts
    /// it, before anything publishes.
    #[dialog_common::test]
    async fn it_refuses_an_unreachable_asset_in_a_staged_batch() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let batch = branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .perform(&operator)
            .await?;
        let refused = batch
            .transaction()
            .assert(Asset::stored([7u8; 32], 10))
            .commit()
            .perform(&operator)
            .await;

        assert!(matches!(
            refused,
            Err(CommitError::Blob(BlobError::NotFound(_)))
        ));
        assert_eq!(branch.revision(), None, "nothing published");
        Ok(())
    }

    /// A snapshot transaction refuses an unreachable asset and stays where
    /// it was.
    #[dialog_common::test]
    async fn it_refuses_an_unreachable_asset_on_a_snapshot() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let snapshot = branch.snapshot().expect("a first commit");
        let before = snapshot.revision();

        let refused = snapshot
            .transaction()
            .assert(Asset::stored([8u8; 32], 10))
            .commit()
            .perform(&operator)
            .await;

        assert!(matches!(
            refused,
            Err(CommitError::Blob(BlobError::NotFound(_)))
        ));
        assert_eq!(snapshot.revision(), before);
        Ok(())
    }

    /// Retracting an asset retracts its fact, so the line no longer vouches
    /// for the bytes; the bytes themselves stay in the store for a later
    /// collection to reclaim.
    #[dialog_common::test]
    async fn it_drops_an_asset_when_retracted() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let photo = Asset::from(b"retract me".to_vec());
        branch
            .transaction()
            .assert(photo.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .retract(photo.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(recorded_size(&branch, &operator, &photo).await?, None);
        assert_eq!(
            Blob::from(photo.entity()?)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );
        Ok(())
    }

    /// A discard is keyed on the hash: retracting an asset named at a size
    /// other than the one recorded still retracts the recorded fact.
    #[dialog_common::test]
    async fn it_discards_an_asset_by_its_hash_whatever_size_is_named() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let photo = Asset::from(b"discard me by hash".to_vec());
        branch
            .transaction()
            .assert(photo.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .retract(Asset::stored(*photo.hash(), photo.size() + 7))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(recorded_size(&branch, &operator, &photo).await?, None);
        assert_eq!(
            Blob::from(photo.entity()?)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );
        Ok(())
    }

    /// `Blob::retract` on an asset's entity drops the asset, as a
    /// transaction's `retract(asset)` would: the asset is recorded by a fact,
    /// not by a blob-index entry.
    #[dialog_common::test]
    async fn it_retracts_an_asset_through_blob_retract() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let photo = Asset::from(b"retract me through the blob api".to_vec());
        branch
            .transaction()
            .assert(photo.clone())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        Blob::from(photo.entity()?)
            .retract((&branch).into())
            .perform(&operator)
            .await?;

        assert_eq!(recorded_size(&branch, &operator, &photo).await?, None);
        assert_eq!(
            Blob::from(photo.entity()?)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None
        );
        Ok(())
    }

    /// Retracting an asset the line never recorded is a no-op: no
    /// reachability check applies to a retraction, and retracting a fact that
    /// is not there changes nothing, so the head does not move.
    #[dialog_common::test]
    async fn it_retracts_an_unrecorded_asset_as_a_no_op() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let head = branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let after = branch
            .transaction()
            .retract(Asset::stored([9u8; 32], 10))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(after, head, "retracting an unrecorded asset is a no-op");
        Ok(())
    }

    /// Asserting an asset the line already records changes nothing, so a
    /// transaction doing only that keeps the head.
    #[dialog_common::test]
    async fn it_mints_nothing_for_an_asset_already_recorded() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .transaction()
            .assert(Asset::from(b"same bytes".to_vec()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let second = branch
            .transaction()
            .assert(Asset::from(b"same bytes".to_vec()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(second, first, "re-asserting an asset is not a change");
        Ok(())
    }

    /// An application cannot write an asset's fact itself: only a commit
    /// that stored the bytes records one.
    #[dialog_common::test]
    async fn it_refuses_an_asset_fact_written_by_an_application() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut forged = Changes::new();
        let fact = Asset::from(b"forged".to_vec()).fact()?;
        forged.associate(
            fact.the,
            fact.of,
            Value::UnsignedInt(1_000_000),
            dialog_artifacts::Policy::All,
        );
        let refused = branch
            .transaction()
            .assert(forged)
            .commit()
            .publish()
            .perform(&operator)
            .await;
        assert!(matches!(
            refused,
            Err(CommitError::Artifact(
                DialogArtifactsError::ReservedAttribute(_)
            ))
        ));
        Ok(())
    }

    /// Assets asserted across a chained batch are all recorded by the time
    /// it publishes.
    #[dialog_common::test]
    async fn it_stores_assets_through_a_staged_batch() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = Asset::from(b"first".to_vec());
        let second = Asset::from(b"second".to_vec());

        let batch = branch
            .transaction()
            .assert(first.clone())
            .commit()
            .perform(&operator)
            .await?;
        batch
            .transaction()
            .assert(second.clone())
            .commit()
            .perform(&operator)
            .await?
            .publish()
            .perform(&operator)
            .await?;

        for asset in [first, second] {
            assert_eq!(
                Blob::from(asset.entity()?)
                    .size((&branch).into())
                    .perform(&operator)
                    .await?,
                Some(asset.size())
            );
        }
        Ok(())
    }

    /// A snapshot transaction stores assets on the snapshot's own lineage.
    #[dialog_common::test]
    async fn it_stores_an_asset_through_a_snapshot_transaction() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(avatar_of_alice(alice()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let snapshot = branch.snapshot().expect("a first commit");

        let asset = Asset::from(b"snapshot bytes".to_vec());
        snapshot
            .transaction()
            .assert(asset.clone())
            .commit()
            .perform(&operator)
            .await?;

        let reader = Blob::from(asset.entity()?)
            .read((&snapshot).into())
            .perform(&operator)
            .await?;
        assert_eq!(drain(reader).await, b"snapshot bytes".to_vec());
        assert_eq!(
            Blob::from(asset.entity()?)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            None,
            "the branch never saw the snapshot's commit"
        );
        Ok(())
    }

    /// A batch integrated into a transaction brings its asset changes along.
    #[dialog_common::test]
    async fn it_carries_assets_through_integrate() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let asset = Asset::from(b"integrated".to_vec());
        let mut batch = avatar_of_alice(asset.entity()?);
        batch.import(asset.clone());

        branch
            .transaction()
            .integrate(batch)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            Blob::from(asset.entity()?)
                .size((&branch).into())
                .perform(&operator)
                .await?,
            Some(10)
        );
        Ok(())
    }
}
