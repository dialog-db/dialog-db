//! Syncing a session: the primary and every durable layer that has
//! upstreams, pulled and pushed in the order that keeps the primary's
//! pointer facts naming revisions of the layers it holds.

use dialog_capability::{Fork, Provider};
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Publish;
use thiserror::Error;

use crate::placement::{Pointer, pointers, tree_revision};
use crate::repository::branch::resolve::resolve;
use crate::repository::source::Source;
use crate::{
    Branch, CommitError, Download, PullEnv, PullError, PushError, RemoteSite,
    ResolveUpstreamsError, Session, SessionEnv,
};

/// What [`Session::sync`] needs of its environment: what a session's
/// dispatch needs, and what a pull and a push need.
pub trait SyncEnv:
    SessionEnv
    + PullEnv
    + Provider<Fork<RemoteSite, Get>>
    + Provider<Fork<RemoteSite, Put>>
    + Provider<Fork<RemoteSite, Publish>>
    + Provider<Fork<RemoteSite, BlobImport>>
    + Provider<Fork<RemoteSite, BlobRead>>
{
}

impl<T> SyncEnv for T where
    T: SessionEnv
        + PullEnv
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, Publish>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>
{
}

/// Why a [`Session::sync`] stopped.
#[derive(Error, Debug)]
pub enum SyncError {
    /// The upstreams of the primary or of a durable layer could not be
    /// resolved.
    #[error(transparent)]
    Upstreams(#[from] ResolveUpstreamsError),
    /// Pulling the primary or a durable layer failed. What was pulled
    /// before it stays pulled.
    #[error(transparent)]
    Pull(#[from] PullError),
    /// Recording, in the primary, where the pulls left the durable layers
    /// failed.
    #[error(transparent)]
    Commit(#[from] CommitError),
    /// Pushing the primary or a durable layer failed. What was pushed
    /// before it stays pushed.
    #[error(transparent)]
    Push(#[from] PushError),
}

/// Command syncing a [`Session`]. Built by [`Session::sync`]; run with
/// [`perform`](Self::perform).
pub struct SyncSession<'a> {
    session: &'a Session,
    download: Download,
}

impl Session {
    /// Pull and push the primary and every durable layer that has
    /// upstreams (the ones its branch pulls from and pushes to), bringing
    /// local what `download` names of each head a pull adopts (see
    /// [`Branch::pull`]). A memory layer has none and is not synced.
    ///
    /// The order keeps the primary's pointer facts naming revisions of
    /// the layers it holds: the layers are pulled before the primary, so
    /// a pointer the primary pulls names a layer revision already
    /// pulled; a layer the pulls left at a revision the primary does not
    /// point at gets its pointer fact written, as a dispatch through the
    /// session; and the primary is pushed after the layers, so a replica
    /// that pulls a pointer finds the layer revision it names upstream.
    pub fn sync(&self, download: Download) -> SyncSession<'_> {
        SyncSession {
            session: self,
            download,
        }
    }
}

impl SyncSession<'_> {
    /// Pull every durable layer with upstreams, then the primary; record
    /// the layers' revisions in the primary where they moved; push every
    /// durable layer with upstreams, then the primary.
    pub async fn perform<Env: SyncEnv>(self, env: &Env) -> Result<(), SyncError> {
        let session = self.session;
        let primary = session.primary();
        let layers: Vec<&Branch> = session
            .durables()
            .iter()
            .map(|(_, branch)| branch)
            .collect();
        for branch in layers.iter().copied().chain([primary]) {
            resolve(branch, env).await?;
        }
        for branch in layers.iter().copied().chain([primary]) {
            if !branch.pulls().is_empty() {
                Box::pin(branch.pull(self.download.clone()).perform(env)).await?;
            }
        }
        let pointed = pointers(&Source::from(primary.clone()), env).await?;
        let mut moved = dialog_corpus::Changes::new();
        for (layer, branch) in session.durables() {
            let Some(revision) = branch.revision() else {
                continue;
            };
            let revision = tree_revision(&revision);
            let recorded = pointed
                .iter()
                .any(|(pointed, at)| pointed == layer && *at == revision);
            if !recorded {
                dialog_corpus::Statement::assert(
                    Pointer {
                        layer: layer.clone(),
                        revision,
                    },
                    &mut moved,
                );
            }
        }
        if !moved.is_empty() {
            Box::pin(session.transact(moved).perform(env)).await?;
        }
        for branch in layers.into_iter().chain([primary]) {
            if !branch.pushes().is_empty() {
                Box::pin(branch.push().perform(env)).await?;
            }
        }
        Ok(())
    }
}
