//! Syncing a stack: the primary and every durable layer that has
//! upstreams, pulled and pushed in the order that keeps the primary's
//! pointer facts naming revisions of the layers it holds.

use dialog_capability::{Fork, Provider};
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Publish;
use thiserror::Error;

use crate::placement::{Pointer, pointers, revision_entity};
use crate::repository::branch::resolve::resolve;
use crate::repository::source::Source;
use dialog_corpus::Entity;

use crate::{
    Branch, CommitError, Download, PullEnv, PullError, PushError, RemoteSite,
    ResolveUpstreamsError, Stack, StackEnv,
};

/// What [`Stack::sync`] needs of its environment: what a stack's
/// dispatch needs, and what a pull and a push need.
pub trait SyncEnv:
    StackEnv
    + PullEnv
    + Provider<Fork<RemoteSite, Get>>
    + Provider<Fork<RemoteSite, Put>>
    + Provider<Fork<RemoteSite, Publish>>
    + Provider<Fork<RemoteSite, BlobImport>>
    + Provider<Fork<RemoteSite, BlobRead>>
{
}

impl<T> SyncEnv for T where
    T: StackEnv
        + PullEnv
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, Publish>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>
{
}

/// Why a [`Stack::sync`] stopped.
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

/// Command syncing a [`Stack`]. Built by [`Stack::sync`]; run with
/// [`perform`](Self::perform).
pub struct SyncStack<'a> {
    stack: &'a Stack,
    download: Download,
}

impl Stack {
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
    /// stack; and the primary is pushed after the layers, so a replica
    /// that pulls a pointer finds the layer revision it names upstream.
    ///
    /// After the pulls, and again after the pushes, the stack records
    /// in `session:` the revision row of every layer that stands
    /// somewhere other than its row says (the primary's
    /// `dialog.branch/revision`, a memory layer's `dialog.layer/revision`),
    /// as a dispatch does once it publishes, and only those: the
    /// dispatch that writes the pointer facts records them, and with no
    /// pointer to write but a layer the pulls moved, an empty dispatch
    /// does. Each such dispatch takes the moved rows as its stimulus, so
    /// a rule premised on the primary's `dialog.branch/revision` fires
    /// when a pull moved the primary, as it fires when a commit did, and
    /// one premised on a durable layer's pointer fact fires when the
    /// dispatch writes it. A sync after which no layer stands anywhere
    /// new dispatches nothing and writes nothing into `session:`.
    pub fn sync(&self, download: Download) -> SyncStack<'_> {
        SyncStack {
            stack: self,
            download,
        }
    }
}

impl SyncStack<'_> {
    /// Pull every durable layer with upstreams, then the primary; record
    /// the layers' revisions in the primary where they moved, and in the
    /// stack's own layer those that moved; push every durable layer
    /// with upstreams, then the primary; and record in the stack's own
    /// layer the revisions the pushes moved.
    pub async fn perform<Env: SyncEnv>(self, env: &Env) -> Result<(), SyncError> {
        let stack = self.stack;
        let primary = stack.primary();
        let layers: Vec<&Branch> = stack.durables().iter().map(|(_, branch)| branch).collect();
        for branch in layers.iter().copied().chain([primary]) {
            resolve(branch, env).await?;
        }
        for branch in layers.iter().copied().chain([primary]) {
            if !branch.pulls().is_empty() {
                Box::pin(branch.pull(self.download.clone()).perform(env)).await?;
            }
        }
        let entities: Vec<&Entity> = stack
            .durables()
            .iter()
            .filter_map(|(layer, _)| stack.branch_of(layer))
            .collect();
        let pointed = pointers(&Source::from(primary.clone()), entities, env).await?;
        let mut moved = dialog_corpus::Changes::new();
        for (layer, branch) in stack.durables() {
            let (Some(revision), Some(entity)) = (branch.revision(), stack.branch_of(layer)) else {
                continue;
            };
            let revision = revision_entity(&revision);
            let recorded = pointed
                .iter()
                .any(|(pointed, at)| pointed == entity && *at == revision);
            if !recorded {
                dialog_corpus::Statement::assert(
                    Pointer {
                        branch: entity.clone(),
                        revision,
                    },
                    &mut moved,
                );
            }
        }
        // One dispatch writes the pointers the pulls moved and records,
        // in `session:`, the revisions they moved; with no pointer to
        // write, it runs only when a revision moved, to record it.
        if !moved.is_empty() || stack.unrecorded() {
            Box::pin(stack.transact(moved).perform(env)).await?;
        }
        for branch in layers.into_iter().chain([primary]) {
            if !branch.pushes().is_empty() {
                Box::pin(branch.push().perform(env)).await?;
            }
        }
        if stack.unrecorded() {
            Box::pin(stack.transact(dialog_corpus::Changes::new()).perform(env)).await?;
        }
        Ok(())
    }
}
