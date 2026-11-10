use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::tree::fetch_spilled;
use dialog_corpus::{
    Artifact, DialogCorpusError, EntityKey, Exporter, Key, KeyViewConstruct, State,
};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
use dialog_search_tree::nodes;
use futures_util::TryStreamExt;

use crate::repository::archive::erased;
use crate::{Branch, Index, RemoteSite};

/// Command struct for exporting all artifacts from a branch.
pub struct Export<'a, E> {
    branch: &'a Branch,
    exporter: E,
}

impl<'a, E> Export<'a, E> {
    pub(super) fn new(branch: &'a Branch, exporter: E) -> Self {
        Self { branch, exporter }
    }
}

impl<E: Exporter> Export<'_, E> {
    /// Execute the export, writing all artifacts to the exporter.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DialogCorpusError>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let mut exporter = self.exporter;

        let archive = ArchiveScope::new(branch.subject());
        let catalog = archive.index();
        let store = erased(env);

        let tree = match branch.revision() {
            Some(revision) => Index::at(catalog, NodeHash::from(*revision.tree.hash())),
            // No revision, no tree: the export streams nothing.
            None => Index::empty_in(catalog),
        };

        let range = <EntityKey<Key> as KeyViewConstruct>::min().into_key()
            ..=<EntityKey<Key> as KeyViewConstruct>::max().into_key();

        // Keep the raw backend to fetch spilled value blocks by reference; the
        // bridge below only reads tree nodes.
        let raw_store = store;
        let tree_store = store;
        let stream = tree.stream_range(range, &tree_store);
        tokio::pin!(stream);

        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = &entry.value {
                let spilled = fetch_spilled(&raw_store, &archive, &entry.key).await?;
                let artifact = Artifact::from_key_datum_with_value(&entry.key, datum, spilled)?;
                exporter.write(&artifact).await?;
            }
        }

        exporter.close().await?;

        Ok(())
    }
}
