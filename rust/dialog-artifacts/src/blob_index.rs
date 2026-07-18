//! The blob index: intrinsic, content-derived metadata for referenced blobs.
//!
//! The blob index is the fifth ordering carried in the artifact tree (see
//! [`BlobKey`]). Each entry maps a blob hash to a small, content-derived
//! [`BlobRecord`] — currently the blob's size — which drives replication (the
//! tree differential identifies newly referenced blobs) and answers intrinsic
//! queries such as `blob/size` without fetching the blob itself.
//!
//! The record rides the tree's shared `State<Datum>` value: blob keys occupy a
//! tag range disjoint from the EAV/AEV/VAE indexes, so a blob entry's `Datum`
//! is never seen by the fact scan. The encoding is hidden behind
//! [`BlobRecord`]'s conversions so callers deal in `{version, size}`, not raw
//! `Datum` fields, and a leading version byte lets the record grow.

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_search_tree::{Buffer, Change, ContentAddressedStorage, Delta, TreeDifference};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;

use crate::{
    BLOB_KEY_TAG, BlobKey, Datum, DialogArtifactsError, Key, KeyBytes, State,
    tree::{ArtifactTree, TreeStorageBridge},
};

/// Current [`BlobRecord`] encoding version.
pub const BLOB_RECORD_VERSION: u8 = 1;

/// Number of bytes in the version-1 record encoding: one version byte plus a
/// big-endian `u64` size.
const BLOB_RECORD_V1_LEN: usize = 1 + 8;

/// Intrinsic, content-derived metadata stored for a blob in the blob index.
///
/// Versioned so new intrinsic fields can be added without a tree-wide
/// migration: a reader switches on the leading version byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobRecord {
    /// Encoding version of this record.
    pub version: u8,
    /// Total size of the blob in bytes.
    pub size: u64,
}

impl BlobRecord {
    /// A record for a blob of `size` bytes, at the current encoding version.
    pub fn new(size: u64) -> Self {
        Self {
            version: BLOB_RECORD_VERSION,
            size,
        }
    }

    /// Encode this record as the tree value stored against a blob key.
    fn into_state(self) -> State<Datum> {
        let mut value = Vec::with_capacity(BLOB_RECORD_V1_LEN);
        value.push(self.version);
        value.extend_from_slice(&self.size.to_be_bytes());
        // Blob entries carry only the record in `value`; the artifact-shaped
        // fields are canonical empties and are never read (blob keys never
        // reach the fact scan).
        State::Added(Datum {
            entity: String::new(),
            attribute: String::new(),
            value_type: 0,
            value,
            cause: None,
            version: None,
            supersedes: Vec::new(),
            retraction: false,
        })
    }

    /// Decode a blob record from a tree value. `Removed` (a retracted entry)
    /// decodes to `None`.
    fn from_state(state: &State<Datum>) -> Result<Option<Self>, DialogArtifactsError> {
        let datum = match state {
            State::Added(datum) => datum,
            State::Removed => return Ok(None),
        };
        let bytes = datum.value.as_slice();
        match bytes.first().copied() {
            Some(BLOB_RECORD_VERSION) if bytes.len() == BLOB_RECORD_V1_LEN => {
                let size = u64::from_be_bytes(
                    bytes[1..BLOB_RECORD_V1_LEN]
                        .try_into()
                        .expect("checked length"),
                );
                Ok(Some(Self {
                    version: BLOB_RECORD_VERSION,
                    size,
                }))
            }
            Some(version) => Err(DialogArtifactsError::MalformedIndex(format!(
                "unsupported blob record version {version} ({} bytes)",
                bytes.len()
            ))),
            None => Err(DialogArtifactsError::MalformedIndex(
                "empty blob record".to_string(),
            )),
        }
    }
}

/// A change to the blob index between two tree versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobChange {
    /// A blob newly referenced in the target tree — a candidate to upload.
    Added(Blake3Hash),
    /// A blob reference removed in the target tree.
    Removed(Blake3Hash),
}

impl BlobChange {
    /// The blob hash this change concerns.
    pub fn hash(&self) -> &Blake3Hash {
        match self {
            BlobChange::Added(hash) | BlobChange::Removed(hash) => hash,
        }
    }
}

/// Stream the blob-index differences between two tree versions, in hash order.
///
/// Runs the search-tree differential and keeps only entries in the `BLOB` tag
/// range, so the result names exactly the blobs added or removed between
/// `checkpoint` and `current` — the set push must ship (additions) without
/// re-reading subtrees that did not change. Both trees must be readable from
/// `store`.
pub fn blob_changes<'s, S>(
    checkpoint: ArtifactTree,
    current: ArtifactTree,
    store: S,
) -> impl Stream<Item = Result<BlobChange, DialogArtifactsError>> + 's + ConditionalSend
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + ConditionalSync
        + 's,
{
    let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
    try_stream! {
        let difference = TreeDifference::compute(&checkpoint, &current, &storage, &storage).await?;
        let changes = difference.changes();
        tokio::pin!(changes);
        for await change in changes {
            let (entry, removed) = match change? {
                Change::Add(entry) => (entry, false),
                Change::Remove(entry) => (entry, true),
            };
            let key = Key::from(entry.key);
            if key.tag() != BLOB_KEY_TAG {
                continue;
            }
            let hash = BlobKey(key).blob_hash();
            // Decoding rejects a malformed record; a `None` decode is a
            // retraction tombstone, which is a reference only when removed.
            match (removed, BlobRecord::from_state(&entry.value)?) {
                (false, Some(_)) => yield BlobChange::Added(hash),
                (true, _) => yield BlobChange::Removed(hash),
                (false, None) => {}
            }
        }
    }
}

/// Blob-index operations on an [`ArtifactTree`].
///
/// An extension trait for the same reason as
/// [`ArtifactTreeExt`](crate::tree::ArtifactTreeExt): `ArtifactTree` aliases a
/// foreign `PersistentTree`. Writes follow the same contract — new nodes
/// accumulate in the caller-owned `delta`, which the caller flushes and
/// persists when minting a revision.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BlobIndexExt {
    /// Record a blob reference in the index (idempotent: re-recording the same
    /// `(hash, record)` is a no-op write).
    async fn put_blob<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        hash: &Blake3Hash,
        record: BlobRecord,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

    /// Look up a blob's record, or `None` if it is not in the index.
    async fn get_blob<S>(
        &self,
        store: &S,
        hash: &Blake3Hash,
    ) -> Result<Option<BlobRecord>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

    /// Whether the index references a blob.
    async fn has_blob<S>(&self, store: &S, hash: &Blake3Hash) -> Result<bool, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        Ok(self.get_blob(store, hash).await?.is_some())
    }

    /// Stream every referenced blob and its record, in hash order.
    ///
    /// Consumes `self` (the tree is moved into the returned stream to pin its
    /// root); `store` backs it.
    fn list_blobs<'s, S>(
        self,
        store: S,
    ) -> impl Stream<Item = Result<(Blake3Hash, BlobRecord), DialogArtifactsError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobIndexExt for ArtifactTree {
    async fn put_blob<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        hash: &Blake3Hash,
        record: BlobRecord,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        let key = KeyBytes::from(BlobKey::new(hash).into_key());
        let transient = self
            .edit()
            .insert(key, record.into_state(), &storage)
            .await?;
        *self = transient.persist(delta)?;
        Ok(())
    }

    async fn get_blob<S>(
        &self,
        store: &S,
        hash: &Blake3Hash,
    ) -> Result<Option<BlobRecord>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        let key = KeyBytes::from(BlobKey::new(hash).into_key());
        match self.get(&key, &storage).await? {
            Some(state) => BlobRecord::from_state(&state),
            None => Ok(None),
        }
    }

    fn list_blobs<'s, S>(
        self,
        store: S,
    ) -> impl Stream<Item = Result<(Blake3Hash, BlobRecord), DialogArtifactsError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        let tree: ArtifactTree = self;
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        try_stream! {
            let range = KeyBytes::from(BlobKey::min().into_key())
                ..=KeyBytes::from(BlobKey::max().into_key());
            let stream = tree.stream_range(range, &storage);
            tokio::pin!(stream);
            for await item in stream {
                let entry = item?;
                if let Some(record) = BlobRecord::from_state(&entry.value)? {
                    let hash = BlobKey(Key::from(entry.key)).blob_hash();
                    yield (hash, record);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::TryStreamExt;

    fn hash(seed: u8) -> Blake3Hash {
        [seed; 32]
    }

    #[dialog_common::test]
    async fn it_round_trips_a_blob_record() -> Result<(), DialogArtifactsError> {
        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();
        let mut tree = ArtifactTree::empty();

        tree.put_blob(&mut store, &mut delta, &hash(1), BlobRecord::new(4096))
            .await?;
        for (_, buffer) in delta.flush() {
            store
                .set(*buffer.blake3_hash().as_bytes(), buffer.as_ref().to_vec())
                .await?;
        }

        assert_eq!(
            tree.get_blob(&store, &hash(1)).await?,
            Some(BlobRecord::new(4096))
        );
        assert!(tree.has_blob(&store, &hash(1)).await?);
        assert_eq!(tree.get_blob(&store, &hash(2)).await?, None);
        assert!(!tree.has_blob(&store, &hash(2)).await?);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_lists_blobs_in_hash_order() -> Result<(), DialogArtifactsError> {
        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();
        let mut tree = ArtifactTree::empty();

        for seed in [3u8, 1, 2] {
            tree.put_blob(
                &mut store,
                &mut delta,
                &hash(seed),
                BlobRecord::new(seed as u64),
            )
            .await?;
            for (_, buffer) in delta.flush() {
                store
                    .set(*buffer.blake3_hash().as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }
        }

        let listed: Vec<_> = tree.list_blobs(store).try_collect().await?;
        assert_eq!(
            listed,
            vec![
                (hash(1), BlobRecord::new(1)),
                (hash(2), BlobRecord::new(2)),
                (hash(3), BlobRecord::new(3)),
            ]
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_detects_newly_referenced_blobs_from_the_differential()
    -> Result<(), DialogArtifactsError> {
        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();

        let persist = |store: &mut MemoryStorageBackend<Blake3Hash, Vec<u8>>,
                       delta: &mut Delta<NodeHash, Buffer>| {
            let buffers: Vec<_> = delta.flush().collect();
            let store = store.clone();
            async move {
                let mut store = store;
                for (_, buffer) in buffers {
                    store
                        .set(*buffer.blake3_hash().as_bytes(), buffer.as_ref().to_vec())
                        .await?;
                }
                Ok::<_, DialogArtifactsError>(())
            }
        };

        // Checkpoint references blob A.
        let mut checkpoint = ArtifactTree::empty();
        checkpoint
            .put_blob(&mut store, &mut delta, &hash(1), BlobRecord::new(10))
            .await?;
        persist(&mut store, &mut delta).await?;

        // Current adds blob B and keeps A.
        let mut current = checkpoint.clone();
        current
            .put_blob(&mut store, &mut delta, &hash(2), BlobRecord::new(20))
            .await?;
        persist(&mut store, &mut delta).await?;

        let changes: Vec<_> = blob_changes(checkpoint, current, store)
            .try_collect()
            .await?;
        assert_eq!(changes, vec![BlobChange::Added(hash(2))]);
        Ok(())
    }
}
