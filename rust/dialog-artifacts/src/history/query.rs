use std::str::FromStr;

use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::ContentAddressedStorage as NodeStorage;
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::TryStreamExt;

use crate::Value;
use crate::history::VersionExt as _;
use crate::tree::ArtifactTreeExt as _;
use crate::tree::{ArtifactTree, SpillCache, TreeStorageBridge, fetch_spilled_cached, spill_cache};
use crate::{
    Attribute, DialogArtifactsError, Entity, State, history_claim_range, history_key_version,
    history_region_range,
};

use super::{Claim, History, REVISION_ATTRIBUTE, Record, RevisionRecord, Version};

/// Read access to the history region of an artifact tree.
///
/// History records live in the same search tree as the index entries (under
/// [`HISTORY_KEY_TAG`](crate::HISTORY_KEY_TAG)), so a tree root identifies
/// the data *and* its recorded lineage: there is no separate history root to
/// carry, replicate, or merge — pulling a tree pulls its history, and
/// merging trees unions their histories.
pub struct TreeHistory<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    tree: ArtifactTree,
    store: S,
    storage: NodeStorage<TreeStorageBridge<S>>,
    /// Memoized verified records, keyed by version. A version's record
    /// is immutable (two records claiming one version is protocol
    /// corruption), so entries never invalidate; a hit skips the tree
    /// read, the decode, and the signature verification. Share one
    /// cache across readers with
    /// [`with_record_cache`](TreeHistory::with_record_cache).
    records: dialog_search_tree::Cache<Version, RevisionRecord>,
    /// Cached spilled value blocks, so a claim on a value that spilled
    /// out of its key reads its archive block once across lookups.
    spill: SpillCache,
}

impl<S> TreeHistory<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    /// Read history from the given artifact tree
    pub fn new(tree: ArtifactTree, store: S) -> Self
    where
        S: Clone,
    {
        Self {
            records: dialog_search_tree::Cache::new(),
            tree,
            store: store.clone(),
            storage: NodeStorage::new(TreeStorageBridge(store)),
            spill: spill_cache(),
        }
    }

    /// Read history from the artifact tree rooted at `root`
    pub fn from_root(root: &Blake3Hash, store: S) -> Self {
        Self::new(ArtifactTree::from_hash(NodeHash::from(*root)), store)
    }

    /// Attach a shared verified-record memo (see the `records` field).
    pub fn with_record_cache(
        mut self,
        records: dialog_search_tree::Cache<Version, RevisionRecord>,
    ) -> Self {
        self.records = records;
        self
    }

    /// Read history from the artifact tree rooted at `root`, sharing the
    /// given node cache: repeated history lookups (e.g. the frontier reads
    /// of [`common_ancestor`](super::common_ancestor), or the skip table
    /// construction of [`extend_skips`](super::extend_skips)) re-walk the
    /// same tree spine, and content-addressed keys make sharing the cache
    /// with other readers of the same store safe.
    pub fn from_root_with_cache(
        root: &Blake3Hash,
        store: S,
        cache: dialog_search_tree::Cache<NodeHash, dialog_search_tree::Buffer>,
    ) -> Self {
        Self::new(
            ArtifactTree::from_hash_with_cache(NodeHash::from(*root), cache),
            store,
        )
    }

    /// Every record in the history region, in key order (ascending by
    /// version; no record appears before one produced by an ancestor
    /// revision)
    pub async fn records(&self) -> Result<Vec<(Version, Record)>, DialogArtifactsError> {
        let (min, max) = history_region_range();
        let stream = self.tree.stream_range(min..=max, &self.storage);
        tokio::pin!(stream);

        let mut records = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                let spilled = fetch_spilled_cached(&self.store, &self.spill, &entry.key).await?;
                records.push((
                    history_key_version(&entry.key)?,
                    Record::try_from_key_datum_with_value(&entry.key, datum, spilled)?,
                ));
            }
        }
        Ok(records)
    }
}

impl<S> History for TreeHistory<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + ConditionalSync,
{
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        let (min, max) = history_claim_range(version, of, the);
        let stream = self.tree.stream_range(min..=max, &self.storage);
        tokio::pin!(stream);

        // The key is lossless, so `history_claim_range` is exact on
        // `(version, entity, attribute)`: every hit belongs to this claim and
        // needs no re-check (the truncated key this replaces did).
        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                let spilled = fetch_spilled_cached(&self.store, &self.spill, &entry.key).await?;
                claims.push(
                    Record::try_from_key_datum_with_value(&entry.key, datum, spilled)?
                        .claim()
                        .clone(),
                );
            }
        }
        Ok(claims)
    }

    async fn revision_record(
        &self,
        version: &Version,
    ) -> Result<Option<RevisionRecord>, DialogArtifactsError> {
        // A version's record is immutable, so a memoized hit needs no
        // read and no re-verification.
        if let Some(record) = self
            .records
            .get_or_fetch::<_, DialogArtifactsError>(version, async |_| Ok(None))
            .await?
        {
            return Ok(Some(record));
        }

        // The record is a fact on the version-derived entity under the
        // reserved attribute, stored in the attribute-ordered index
        // only (the one ordering the query layer's projection rules
        // scan). Scan the (attribute, entity) prefix there; fall back
        // to the entity-ordered index for trees written before records
        // were single-ordered.
        let of = version.entity();
        let the = Attribute::from_str(REVISION_ATTRIBUTE)?;
        let candidates = self
            .tree
            .clone()
            .select_record(self.store.clone(), &of, &the)
            .await?;
        // Tree blocks may have arrived from an untrusted peer; a record
        // only counts if it vouches for itself — issuer signature valid,
        // and derived version matching the slot it was found at. A
        // candidate that fails is SKIPPED, not fatal: a hostile peer can
        // plant a second, garbage-signed record at a genuine slot (same
        // lineage/issuer/parents derive the same version), and one bad
        // candidate must not veto the good one. Only when no candidate
        // verifies does the first failure surface.
        let mut failure = None;
        for artifact in candidates {
            if let Value::Record(bytes) = &artifact.is {
                let verified = RevisionRecord::try_from_bytes(bytes)
                    .and_then(|record| record.verify(version).map(|()| record));
                match verified {
                    Ok(record) => {
                        self.records.insert(*version, record.clone());
                        return Ok(Some(record));
                    }
                    Err(error) => failure = failure.or(Some(error)),
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }
}
