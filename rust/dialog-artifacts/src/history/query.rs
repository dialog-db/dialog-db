use std::str::FromStr;

use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::ContentAddressedStorage as NodeStorage;
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::TryStreamExt;

use crate::tree::{ArtifactTree, TreeStorageBridge};
use crate::{
    Attribute, DialogArtifactsError, Entity, State, history_attribute_range, history_claim_range,
    history_key_version, history_region_range,
};

use super::{Claim, History, REVISION_ATTRIBUTE, Record, SKIP_ATTRIBUTE, Version};

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
    storage: NodeStorage<TreeStorageBridge<S>>,
}

impl<S> TreeHistory<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    /// Read history from the given artifact tree
    pub fn new(tree: ArtifactTree, store: S) -> Self {
        Self {
            tree,
            storage: NodeStorage::new(TreeStorageBridge(store)),
        }
    }

    /// Read history from the artifact tree rooted at `root`
    pub fn from_root(root: &Blake3Hash, store: S) -> Self {
        Self::new(ArtifactTree::from_hash(NodeHash::from(*root)), store)
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

    /// Every claim recorded by the revision identified by `version` under
    /// the given attribute. The attribute head leads the entity head in
    /// history keys, so this is an exact range scan over just the matches
    /// — not a walk over everything the revision recorded — which keeps
    /// each ancestor-traversal step O(result) even for revisions minted by
    /// huge commits.
    async fn claims_by_attribute(
        &self,
        version: &Version,
        attribute: &str,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        let attribute = Attribute::from_str(attribute)?;
        let (min, max) = history_attribute_range(version, &attribute);
        let stream = self.tree.stream_range(
            crate::KeyBytes::from(min)..=crate::KeyBytes::from(max),
            &self.storage,
        );
        tokio::pin!(stream);

        // The range is exact for attributes that fit the raw head (both
        // callers' attributes do); the re-check guards a longer attribute
        // sharing a head, per the history key contract.
        let attribute = attribute.to_string();
        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                if datum.attribute != attribute {
                    continue;
                }
                claims.push(Record::try_from_datum(datum)?.claim().clone());
            }
        }
        Ok(claims)
    }

    /// Every record in the history region, in key order (ascending by
    /// version; no record appears before one produced by an ancestor
    /// revision)
    pub async fn records(&self) -> Result<Vec<(Version, Record)>, DialogArtifactsError> {
        let (min, max) = history_region_range();
        let stream = self.tree.stream_range(
            crate::KeyBytes::from(min)..=crate::KeyBytes::from(max),
            &self.storage,
        );
        tokio::pin!(stream);

        let mut records = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                records.push((
                    history_key_version(&entry.key)?,
                    Record::try_from_datum(datum)?,
                ));
            }
        }
        Ok(records)
    }
}

impl<S> History for TreeHistory<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        let (min, max) = history_claim_range(version, of, the);
        let stream = self.tree.stream_range(
            crate::KeyBytes::from(min)..=crate::KeyBytes::from(max),
            &self.storage,
        );
        tokio::pin!(stream);

        // The range spans the keys' raw entity/attribute heads, which may
        // be shared beyond their truncation point; re-check the stored
        // record before decoding it (see `history_claim_range`).
        let the = the.to_string();
        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                if datum.entity != of.as_str() || datum.attribute != the {
                    continue;
                }
                claims.push(Record::try_from_datum(datum)?.claim().clone());
            }
        }
        Ok(claims)
    }

    async fn revision_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        self.claims_by_attribute(version, REVISION_ATTRIBUTE).await
    }

    async fn skips_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        self.claims_by_attribute(version, SKIP_ATTRIBUTE).await
    }
}
