//! The branch diagnose explores.
//!
//! Diagnose loads its facts into a branch of a fresh in-memory repository,
//! the way an application stores them, and explores the tree that branch
//! commits: facts, history region and all.

use std::sync::Arc;

use async_trait::async_trait;
use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{
    Artifact, Datum, DialogArtifactsError, Index, Instruction, Key, LoadBlob, State,
};
use dialog_capability::{Provider, Subject};
use dialog_common::{Blake3Hash, Buffer, ConditionalSend};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::storage::Location;
use dialog_peer::helpers::{open_peer, test_storage, unique_name};
use dialog_peer::{Peer, Session};
use dialog_repository::{LocalIndex, RepositoryExt as _};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock, Nodes};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{Stream, StreamExt as _};

/// The session diagnose reads its repository through.
type DiagnoseSession = Peer<VolatileSpace, Session>;

/// Loads the explored branch's tree nodes and spilled values from its
/// archive catalog, through the session the branch was loaded by.
///
/// It keeps the nodes it reads for the whole session in one node cache, so
/// every view (the hierarchy, the analysis, the artifact listing) reads a
/// node from the archive once. Clones share the session and the cache, so
/// every background worker can hold one.
#[derive(Clone)]
pub struct Blocks {
    session: Arc<DiagnoseSession>,
    catalog: CatalogScope,
    nodes: ArtifactNodeCache,
}

impl Blocks {
    fn index(&self) -> LocalIndex<'_, DiagnoseSession> {
        LocalIndex::new(&self.session, self.catalog.clone(), self.nodes.clone())
    }
}

/// Tree nodes are read through the session's node cache.
impl Nodes<Key, State<Datum>> for Blocks {
    fn kept(&self) -> Option<&ArtifactNodeCache> {
        Some(&self.nodes)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlock> for Blocks {
    async fn execute(&self, load: LoadBlock) -> Result<Option<Buffer>, DialogSearchTreeError> {
        load.perform(&self.index()).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlob> for Blocks {
    async fn execute(&self, load: LoadBlob) -> Result<Option<Buffer>, DialogArtifactsError> {
        load.perform(&self.index()).await
    }
}

/// The tree a branch committed, and where its blocks load from.
pub struct Explored {
    /// The committed tree.
    pub tree: Index,
    /// Where the tree's nodes and spilled values load from.
    pub blocks: Blocks,
}

/// Commits `artifacts` to the main branch of a fresh in-memory repository,
/// canonicalized so the tree is the one the fact set determines, and returns
/// the tree the branch committed.
pub async fn explore<Artifacts>(artifacts: Artifacts) -> anyhow::Result<Explored>
where
    Artifacts: Stream<Item = Artifact> + ConditionalSend,
{
    let storage = test_storage().await;
    let peer = open_peer(storage, Location::profile(unique_name("diagnose"))).await?;
    let session = peer
        .session(b"diagnose")
        .space(peer.state())
        .allow(Subject::any())
        .await?;
    let repository = peer
        .space(unique_name("repo"))
        .open()
        .perform(&session)
        .await?;
    let branch = repository.branch("main").open().perform(&session).await?;
    let changes = artifacts
        .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect();
    branch
        .transaction()
        .integrate(changes)
        .commit()
        .canonicalize()
        .publish()
        .perform(&session)
        .await?;

    let branch = repository.branch("main").load().perform(&session).await?;
    let tree = match branch.revision() {
        Some(revision) => Index::from_hash(Blake3Hash::from(*revision.tree.hash())),
        None => Index::empty(),
    };
    Ok(Explored {
        tree,
        blocks: Blocks {
            catalog: branch.archive().index(),
            session: Arc::new(session),
            nodes: ArtifactNodeCache::new(),
        },
    })
}
