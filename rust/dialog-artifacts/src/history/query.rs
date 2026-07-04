use std::str::FromStr;

use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::ContentAddressedStorage as NodeStorage;
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::TryStreamExt;

use crate::tree::{ArtifactTree, TreeStorageBridge};
use crate::{
    Attribute, DialogArtifactsError, Entity, State, history_attribute_hash, history_claim_range,
    history_key_attribute_hash, history_key_version, history_region_range, history_version_range,
};

use super::{Claim, History, REVISION_ATTRIBUTE, Record, Version};

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

        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                claims.push(Record::try_from_datum(datum)?.claim().clone());
            }
        }
        Ok(claims)
    }

    async fn revision_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        let attribute = history_attribute_hash(&Attribute::from_str(REVISION_ATTRIBUTE)?);
        let (min, max) = history_version_range(version);
        let stream = self.tree.stream_range(
            crate::KeyBytes::from(min)..=crate::KeyBytes::from(max),
            &self.storage,
        );
        tokio::pin!(stream);

        let mut claims = Vec::new();
        while let Some(entry) = stream.try_next().await? {
            if history_key_attribute_hash(&entry.key) != attribute.as_slice() {
                continue;
            }
            if let State::Added(datum) = entry.value {
                claims.push(Record::try_from_datum(datum)?.claim().clone());
            }
        }
        Ok(claims)
    }
}
