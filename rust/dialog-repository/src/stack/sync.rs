//! Syncing a stack: the primary pulled and pushed, and the trees it
//! points its durable layers at carried with it.

use dialog_capability::{Fork, Provider};
use dialog_corpus::Changes;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Publish;
use thiserror::Error;

use crate::erased;
use crate::repository::branch::resolve::resolve;
use crate::stack::durable::{merge, pointed};

use crate::{
    CommitError, Download, PullEnv, PullError, PushError, RemoteSite, ResolveUpstreamsError, Stack,
    StackEnv,
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
    /// The primary's upstreams could not be resolved.
    #[error(transparent)]
    Upstreams(#[from] ResolveUpstreamsError),
    /// Pulling the primary, or the trees it points the durable layers at,
    /// failed. The primary stays where it was.
    #[error(transparent)]
    Pull(#[from] PullError),
    /// Merging a durable layer two replicas moved, or recording in the
    /// primary where it stands, failed.
    #[error(transparent)]
    Commit(#[from] CommitError),
    /// Pushing the primary, or the trees it points the durable layers at,
    /// failed. What was pushed before it stays pushed.
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
    /// Pull and push the primary, with the trees it points the durable
    /// layers at, bringing local what `download` names of the head a pull
    /// adopts and of every tree that head points a durable layer at (see
    /// [`Branch::pull`](crate::Branch::pull)). A durable layer has no
    /// head and no upstreams of its own: the primary's carry it. A memory
    /// layer is not synced.
    ///
    /// The pull downloads the pointed trees before the primary's head
    /// moves, so a replica that goes offline after it reads every layer.
    /// When the pull merged two primaries that each moved a durable
    /// layer, the primary's pointer cells hold a claim of each, and
    /// `last` would pick one: instead this replica's changes to the layer
    /// since the two met are replayed onto the other's tree, as a branch
    /// pull rebases local commits over pulled ones, our writes winning on
    /// the cells they touch and theirs standing elsewhere, and the
    /// primary is pointed at the merged tree alone, as a dispatch through
    /// the stack. The push uploads the pointed trees before the primary's
    /// head, so a replica that pulls the head finds every tree it points
    /// at upstream.
    ///
    /// After the pull, and again after the push, the stack records in
    /// `session:` the revision row of every layer that stands somewhere
    /// other than its row says (the primary's `dialog.branch/revision`, a
    /// memory layer's `dialog.layer/revision`), as a dispatch does once
    /// it publishes, and only those: the dispatch that writes a merged
    /// pointer records them, and with none to write but a layer the pull
    /// moved, an empty dispatch does. Each such dispatch takes the moved
    /// rows as its stimulus, so a rule premised on the primary's
    /// `dialog.branch/revision` fires when a pull moved the primary, as it
    /// fires when a commit did. A sync after which no layer stands
    /// anywhere new dispatches nothing and writes nothing into
    /// `session:`.
    pub fn sync(&self, download: Download) -> SyncStack<'_> {
        SyncStack {
            stack: self,
            download,
        }
    }
}

impl SyncStack<'_> {
    /// Pull the primary with the trees it points at; merge every durable
    /// layer the pull left with more than one pointer; record what moved
    /// in the stack's own layer; push the primary with the trees it points
    /// at; and record in the stack's own layer what the push moved.
    pub async fn perform<Env: SyncEnv>(self, env: &Env) -> Result<(), SyncError> {
        let stack = self.stack;
        let primary = stack.primary();
        let durables = stack.durables();
        let layers = durables.layers().to_vec();
        resolve(primary, env).await?;
        // Where the primary pointed the layers before the pull: the
        // changes of ours a merge replays are the ones since there.
        let before = durables.resolve(primary.revision().as_ref(), env).await?;
        let head = primary.revision();
        if !primary.pulls().is_empty() {
            Box::pin(
                primary
                    .pull(self.download.clone())
                    .pointing(layers.clone())
                    .perform(env),
            )
            .await?;
        }
        let mut merged = Changes::new();
        if let Some(pulled) = primary.revision()
            && primary.revision() != head
        {
            let archive = durables.archive();
            for (at, layer) in layers.iter().enumerate() {
                let claims = pointed(&archive, &pulled.tree, layer, erased(env))
                    .await
                    .map_err(CommitError::from)?;
                let ours = before.revisions.get(at).cloned().flatten();
                merged.merge(Box::pin(merge(durables, layer, &claims, ours.as_ref(), env)).await?);
            }
        }
        stack.repoint(env).await?;
        // One dispatch writes the merged pointers and records, in
        // `session:`, the revisions the pull moved; with no pointer to
        // write, it runs only when a revision moved, to record it.
        if !merged.is_empty() || stack.unrecorded() {
            let repointed = !merged.is_empty();
            Box::pin(stack.assert(merged).perform(env)).await?;
            // The merged pointers were written as facts of the primary,
            // not by a write into the layers: read them back.
            if repointed {
                durables.refresh(primary.revision().as_ref(), env).await?;
            }
        }
        if !primary.pushes().is_empty() {
            Box::pin(primary.push().pointing(layers).perform(env)).await?;
        }
        if stack.unrecorded() {
            Box::pin(stack.invocation().perform(env)).await?;
        }
        Ok(())
    }
}
