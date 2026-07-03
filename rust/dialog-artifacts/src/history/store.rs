use std::str::FromStr;

use dialog_common::{Blake3Hash as NodeHash, Buffer, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_search_tree::{Change, ContentAddressedStorage as NodeStorage, Delta, PersistentTree};
use dialog_storage::{Blake3Hash, CborEncoder, DialogStorageError, Encoder, StorageBackend};
use futures_util::TryStreamExt;

use crate::tree::TreeStorageBridge;
use crate::{Attribute, DialogArtifactsError, Entity, Value};

use super::{
    Claim, HISTORY_KEY_LENGTH, History, HistoryKey, REVISION_ATTRIBUTE, Record, Revision, Version,
};

/// The search tree used to persist the history index. Keys are the raw
/// [`HistoryKey`] bytes; values are CBOR-encoded [`Record`]s.
pub type HistoryIndex = PersistentTree<[u8; HISTORY_KEY_LENGTH], Vec<u8>>;

/// A durable [`History`] index backed by a
/// [`dialog_search_tree::PersistentTree`] over content-addressed storage.
///
/// Claims are keyed by `/edition/origin/entity/attribute/value_hash` (see
/// [`HistoryKey`]), so the index is content-addressed, deterministic for the
/// same set of claims, and synchronizable with the same machinery as the
/// artifact indexes. The tree is immutable: mutations accumulate in a
/// caller-owned delta and become durable when [`HistoryStore::persist`]
/// flushes them to storage — until then they are queryable but not stored,
/// which is what makes transactional rollback (see [`HistoryStore::reset`])
/// cheap.
pub struct HistoryStore<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    index: HistoryIndex,
    storage: NodeStorage<TreeStorageBridge<Backend>>,
    delta: Delta<NodeHash, Buffer>,
    encoder: CborEncoder,
}

impl<Backend> HistoryStore<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    /// Initialize a new, empty [`HistoryStore`] over the given storage
    /// backend
    pub fn new(backend: Backend) -> Self {
        Self {
            index: PersistentTree::empty(),
            storage: NodeStorage::new(TreeStorageBridge(backend)),
            delta: Delta::zero(),
            encoder: CborEncoder,
        }
    }

    /// Hydrate a [`HistoryStore`] from the root hash of a previously
    /// persisted history index
    pub fn from_hash(hash: &Blake3Hash, backend: Backend) -> Self {
        Self {
            index: PersistentTree::from_hash(NodeHash::from(*hash)),
            storage: NodeStorage::new(TreeStorageBridge(backend)),
            delta: Delta::zero(),
            encoder: CborEncoder,
        }
    }

    /// The root hash of the history index, or `None` when it is empty.
    /// Pending (unpersisted) changes are reflected.
    pub fn hash(&self) -> Option<Blake3Hash> {
        let root = self.index.root();
        if root == NULL_BLAKE3_HASH {
            None
        } else {
            Some(*root.as_bytes())
        }
    }

    /// Reset the index to a previously persisted root (or to empty),
    /// discarding any pending changes
    pub fn reset(&mut self, hash: Option<&Blake3Hash>) {
        self.index = match hash {
            Some(hash) => PersistentTree::from_hash(NodeHash::from(*hash)),
            None => PersistentTree::empty(),
        };
        self.delta = Delta::zero();
    }

    /// Flush all pending changes to storage, making the current root durable.
    /// Returns the persisted root hash, or `None` when the index is empty.
    pub async fn persist(&mut self) -> Result<Option<Blake3Hash>, DialogArtifactsError> {
        let pending = self.delta.flush().collect::<Vec<_>>();
        for (digest, buffer) in pending {
            self.storage.store(buffer.into_vec(), &digest).await?;
        }
        Ok(self.hash())
    }

    /// Adopt every record present in the history index rooted at `other`
    /// that this index lacks.
    ///
    /// Records are immutable and uniquely keyed, so adoption is a union:
    /// entries only this index has are kept, never removed. Only blocks on
    /// paths where the two indexes differ are read, so shared history (from
    /// common ancestry) costs nothing. This is the replication step of a
    /// merge: after adopting the remote side's history, conflict detection
    /// keeps working across the sync boundary.
    pub async fn adopt(&mut self, other: &Blake3Hash) -> Result<(), DialogArtifactsError> {
        let other = HistoryIndex::from_hash(NodeHash::from(*other));

        let additions = {
            let changes = self
                .index
                .differentiate(&other, &self.storage, &self.storage);
            tokio::pin!(changes);

            let mut additions = Vec::new();
            while let Some(change) = changes.try_next().await? {
                if let Change::Add(entry) = change {
                    additions.push(entry);
                }
            }
            additions
        };

        if additions.is_empty() {
            return Ok(());
        }

        let mut transient = self.index.edit();
        for entry in additions {
            transient = transient
                .insert(entry.key, entry.value, &self.storage)
                .await?;
        }
        self.index = transient.persist(&mut self.delta)?;
        self.persist().await?;

        Ok(())
    }

    /// Whether the given claim, produced by the revision identified by
    /// `version`, is already recorded
    pub async fn contains(
        &self,
        version: &Version,
        claim: &Claim,
    ) -> Result<bool, DialogArtifactsError> {
        Ok(self
            .index
            .get(HistoryKey::new(version, claim).as_bytes(), &self.storage)
            .await?
            .is_some())
    }

    /// Record a batch of claims, each paired with the [`Version`] of the
    /// revision that produced it, as a single tree edit.
    ///
    /// The batch's new tree nodes are flushed to storage before returning so
    /// that subsequent edits can read the tree spine back. Storage is
    /// content-addressed, so nodes written by a transaction that later rolls
    /// back (see [`HistoryStore::reset`]) are unreferenced but harmless:
    /// durability is conferred by whichever root the caller ultimately
    /// points at.
    pub async fn record_all<Records>(
        &mut self,
        records: Records,
    ) -> Result<(), DialogArtifactsError>
    where
        Records: IntoIterator<Item = (Version, Record)>,
    {
        let mut transient = self.index.edit();
        let mut inserted = false;

        for (version, record) in records {
            let (_, bytes) = self.encoder.encode(&record).await?;
            transient = transient
                .insert(
                    *HistoryKey::new(&version, record.claim()).as_bytes(),
                    bytes,
                    &self.storage,
                )
                .await?;
            inserted = true;
        }

        if inserted {
            self.index = transient.persist(&mut self.delta)?;
            self.persist().await?;
        }

        Ok(())
    }

    /// Record a claim produced by the revision identified by `version`
    pub async fn record(
        &mut self,
        version: &Version,
        record: Record,
    ) -> Result<(), DialogArtifactsError> {
        self.record_all([(*version, record)]).await
    }

    /// Record the lineage claim for the given revision: a claim under the
    /// repository DID whose value is the revision's content-addressed entity
    /// and whose cause lists the parent revision versions
    pub async fn record_revision(
        &mut self,
        revision: &Revision,
    ) -> Result<(), DialogArtifactsError> {
        let (version, record) = revision_record(revision)?;
        self.record(&version, record).await
    }

    /// The recorded revision lineage claims for the repository identified by
    /// `subject`, in a total order consistent with causality (ascending by
    /// version; no revision appears before one of its ancestors)
    pub async fn revisions(
        &self,
        subject: &Entity,
    ) -> Result<Vec<(Version, Claim)>, DialogArtifactsError> {
        let subject = subject.key_bytes();
        let attribute = Attribute::from_str(REVISION_ATTRIBUTE)?;

        let stream = self.index.stream(&self.storage);
        tokio::pin!(stream);

        let mut revisions = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            let key = HistoryKey::from(entry.key);
            if key.entity_bytes() == subject.as_slice()
                && key.attribute_bytes() == attribute.key_bytes().as_slice()
            {
                let record: Record = self.encoder.decode(&entry.value).await?;
                revisions.push((key.version(), record.claim().clone()));
            }
        }

        Ok(revisions)
    }

    /// Every record in the history index, in key order. This is the export
    /// used to replicate history between repositories.
    pub async fn records(&self) -> Result<Vec<(Version, Record)>, DialogArtifactsError> {
        let stream = self.index.stream(&self.storage);
        tokio::pin!(stream);

        let mut records = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            let record: Record = self.encoder.decode(&entry.value).await?;
            records.push((HistoryKey::from(entry.key).version(), record));
        }

        Ok(records)
    }
}

impl<Backend> History for HistoryStore<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        let (min, max) = HistoryKey::claim_range(version, of, the);

        let stream = self
            .index
            .stream_range(*min.as_bytes()..=*max.as_bytes(), &self.storage);
        tokio::pin!(stream);

        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            let record: Record = self.encoder.decode(&entry.value).await?;
            claims.push(record.claim().clone());
        }

        Ok(claims)
    }

    async fn revision_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        let attribute = Attribute::from_str(REVISION_ATTRIBUTE)?;
        let (min, max) = HistoryKey::version_range(version);

        let stream = self
            .index
            .stream_range(*min.as_bytes()..=*max.as_bytes(), &self.storage);
        tokio::pin!(stream);

        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if HistoryKey::from(entry.key).attribute_bytes() == attribute.key_bytes().as_slice() {
                let record: Record = self.encoder.decode(&entry.value).await?;
                claims.push(record.claim().clone());
            }
        }

        Ok(claims)
    }
}

/// The lineage record for a revision: a claim under the repository DID whose
/// value is the revision's content-addressed entity and whose cause lists
/// the parent revision versions, paired with the revision's [`Version`]
pub fn revision_record(revision: &Revision) -> Result<(Version, Record), DialogArtifactsError> {
    Ok((
        revision.version(),
        Record::Assert(Claim {
            the: Attribute::from_str(REVISION_ATTRIBUTE)?,
            of: revision.subject().clone(),
            is: Value::Entity(revision.entity()?),
            cause: revision.cause().clone(),
        }),
    ))
}
