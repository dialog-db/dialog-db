//! The branch diagnose explores.
//!
//! Diagnose loads its facts into a branch of a fresh in-memory repository,
//! the way an application stores them, and explores the tree that branch
//! commits: facts, history region and all.

use std::sync::Arc;

use dialog_capability::Subject;
use dialog_common::{Blake3Hash, ConditionalSend};
use dialog_corpus::{Fact, Index, Instruction};
use dialog_effects::storage::Location;
use dialog_peer::helpers::{open_peer, test_storage, unique_name};
use dialog_peer::{Peer, Session};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{Stream, StreamExt as _};

/// The session diagnose reads its repository through: what every view
/// reads the explored tree's nodes and spilled values through, by
/// performing loads on it. It holds the repository's node cache, so every
/// view (the hierarchy, the analysis, the fact listing) reads a node
/// from the archive once. Shared behind an [`Arc`], so every background
/// worker can hold it.
pub type DiagnoseSession = Peer<VolatileSpace, Session>;

/// The tree a branch committed, and the session its blocks load through.
pub struct Explored {
    /// The committed tree, a root under the branch's catalog.
    pub tree: Index,
    /// The session the tree's nodes and spilled values load through.
    pub session: Arc<DiagnoseSession>,
}

/// Commits `facts` to the main branch of a fresh in-memory repository,
/// canonicalized so the tree is the one the fact set determines, and returns
/// the tree the branch committed.
pub async fn explore<Facts>(facts: Facts) -> anyhow::Result<Explored>
where
    Facts: Stream<Item = Fact> + ConditionalSend,
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
    let changes = facts
        .map(|fact| Instruction::Assert(fact, dialog_corpus::Pick::All))
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
    let catalog = branch.archive().index();
    let tree = match branch.revision() {
        Some(revision) => Index::at(catalog, Blake3Hash::from(*revision.tree.hash())),
        None => Index::empty_in(catalog),
    };
    Ok(Explored {
        tree,
        session: Arc::new(session),
    })
}
