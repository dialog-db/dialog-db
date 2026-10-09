//! Materialize a branch's content locally.
//!
//! A pull adopts an upstream head by reference: subtrees the local
//! replica never changed stay remote and hydrate lazily as reads touch
//! them, and blob bytes replicate on first read. That laziness is the
//! right default for sync cost, but sometimes the caller wants the
//! opposite guarantee — everything the head references present locally,
//! before going offline or before latency-sensitive reads (the
//! authorization walk over an access branch is the motivating case).
//!
//! [`Branch::download`] provides it: one walk of the current revision
//! through the snapshot export machinery with download reach, fetching
//! every block and blob the revision references that the local store
//! does not hold, caching each as it lands. [`Pull::download`] chains
//! the two acts — adopt the upstream head, then materialize it.

use core::ops::RangeInclusive;

use dialog_artifacts::merge::data_scope;
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_effects::peer::Connect;
use futures_util::StreamExt as _;

use crate::repository::snapshot::Snapshot;
use crate::{
    Branch, DownloadError, Pull, PullError, RemoteFallback, RemoteSite, Revision, Upstream,
};

/// [`data_scope`] as the byte ranges the traversal takes: the operational
/// indexes and the blob index, with history and coverage left out.
fn operational_scope() -> Vec<RangeInclusive<Vec<u8>>> {
    data_scope()
        .map(|range| {
            let (start, end) = range.into_inner();
            Vec::from(start)..=Vec::from(end)
        })
        .to_vec()
}

/// Command struct for materializing a branch's content locally. Created
/// by [`Branch::download`].
pub struct Download<'a> {
    branch: &'a Branch,
    from: Option<Upstream>,
    revision: Option<Revision>,
    scope: Option<Vec<RangeInclusive<Vec<u8>>>>,
}

impl Branch {
    /// Fetch every block and blob the current revision references that
    /// the local store does not hold, from the branch's upstream.
    ///
    /// A no-op without a remote upstream (a local upstream shares the
    /// store, so there is nothing to fetch) or before the first
    /// revision.
    pub fn download(&self) -> Download<'_> {
        Download {
            branch: self,
            from: None,
            revision: None,
            scope: None,
        }
    }
}

impl Download<'_> {
    /// Materialize `revision` instead of the branch's current head —
    /// how a caller downloads a prepared-but-not-yet-adopted revision
    /// so the head never advances past the blocks the store holds.
    pub fn of(mut self, revision: Revision) -> Self {
        self.revision = Some(revision);
        self
    }

    /// Materialize only the key regions in `scope`, leaving the rest of
    /// the tree by reference. See [`SnapshotExport::within`].
    pub fn within(mut self, scope: impl Into<Vec<RangeInclusive<Vec<u8>>>>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Materialize only what reads and authorization touch: the
    /// entity/attribute/value indexes and the blob index, with their
    /// spilled values and referenced blobs.
    ///
    /// This is the download a profile wants. The regions it skips are
    /// history and coverage, which no read path can reach — a selector
    /// only ever ranges over the three data orderings — while they are
    /// also the only regions that grow with every edit ever made rather
    /// than with the live fact count.
    ///
    /// The revision DAG is *not* in what it skips: revision records are
    /// ordinary facts in the data indexes, so ancestry, `log`, and skip
    /// tables keep working. And a history record that turns out to be
    /// needed hydrates from the upstream on demand rather than failing.
    pub fn operational(self) -> Self {
        self.within(operational_scope())
    }

    /// Materialize the branch's current revision locally.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DownloadError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let Some(revision) = self.revision.or_else(|| branch.revision()) else {
            return Ok(());
        };
        let fallback = match self.from {
            Some(upstream) => upstream.fallback(),
            None => branch.fallback(),
        };
        let remote = match fallback {
            RemoteFallback::Remote(remote) => remote,
            RemoteFallback::None => return Ok(()),
            RemoteFallback::Unavailable { remote, reason } => {
                return Err(DownloadError::Unreachable {
                    upstream: remote,
                    reason,
                });
            }
        };

        // One walk with download reach: a read-miss falls through to the
        // remote and is cached locally on the way through, and a missing
        // blob is imported (digest-verified) before its reader is served.
        // The items themselves carry nothing the local store does not
        // already hold by the time they are yielded, so draining the
        // stream is the whole job.
        let export = Snapshot::new(branch.subject(), revision)
            .export()
            .download(remote);
        let export = match self.scope {
            Some(scope) => export.within(scope),
            None => export,
        };
        let items = export.perform(env);
        futures_util::pin_mut!(items);
        while let Some(item) = items.next().await {
            item.map_err(DownloadError::Snapshot)?;
        }
        Ok(())
    }
}

/// Command struct for a pull followed by a download of the adopted head.
/// Created by [`Pull::download`].
pub struct PullDownload<'a>(Pull<'a>, Option<Vec<RangeInclusive<Vec<u8>>>>);

impl<'a> Pull<'a> {
    /// After the pull, fetch every block and blob the adopted head
    /// references that the local store does not hold.
    ///
    /// The pull itself stays byte-frugal — it adopts by reference — and
    /// this chains the materialization the caller opted into. The login
    /// flow is the motivating case: pull the account's access branch,
    /// download it, and proofs read entirely locally.
    pub fn download(self) -> PullDownload<'a> {
        PullDownload(self, None)
    }
}

impl<'a> PullDownload<'a> {
    /// Materialize only the key regions in `scope`. See
    /// [`Download::within`].
    pub fn within(mut self, scope: impl Into<Vec<RangeInclusive<Vec<u8>>>>) -> Self {
        self.1 = Some(scope.into());
        self
    }

    /// Materialize only the operational regions. See
    /// [`Download::operational`].
    pub fn operational(self) -> Self {
        self.within(operational_scope())
    }

    /// Pull with the materialization ORDERED BEFORE the head advance:
    /// prepare the merge, download every block and blob the prepared
    /// revision references, and only then commit the cell advance. A
    /// failed download — offline included — leaves the local revision
    /// untouched, so the branch can never point at blocks the store
    /// lacks: there is no window in which a crash or a lost connection
    /// strands a by-reference head. Returns what the pull returned.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + Provider<Connect>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let branch = self.0.branch();
        let from = self.0.source().cloned();
        // Boxed: the prepare future carries the whole pull machinery and
        // trips the large-futures lint inline.
        let prepared = Box::pin(self.0.prepare(env)).await?;
        if let Some(revision) = prepared.revision().cloned() {
            Download {
                branch,
                from,
                revision: Some(revision),
                scope: self.1,
            }
            .perform(env)
            .await?;
        }
        prepared.commit(env).await
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::future::{Future as _, poll_fn};
    use std::task::Poll;

    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_artifacts::{Artifact, Instruction, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use futures_util::stream;

    fn name(of: &str, is: &str) -> Result<Instruction> {
        Ok(Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: of.parse()?,
                is: Value::String(is.to_string()),
                cause: None,
            },
            dialog_artifacts::Policy::All,
        ))
    }

    /// Yields to the executor once, so whatever runs alongside the caller
    /// gets a turn before it resumes.
    async fn yield_once() {
        let mut yielded = false;
        poll_fn(move |context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await
    }

    /// A pull that downloads what it adopts lands under the branch's
    /// write lock, as a plain pull does: while another handle of this
    /// writer holds the lock, the pull waits for it instead of publishing
    /// past it.
    ///
    /// The race the lock prevents -- a merging commit minting on the head
    /// this pull is about to move, both under one origin -- turns on the
    /// interleaving of awaits inside the two operations, which nothing
    /// outside them can fix. So the lock itself is probed: held by the
    /// test, it must hold the pull.
    #[dialog_common::test]
    async fn it_lands_a_downloading_pull_under_the_write_lock() -> Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let repo = test_repo(&session, &peer).await;
        let main = repo.branch("main").open().perform(&session).await?;
        main.commit(stream::iter(vec![name("user:a", "Alice")?]))
            .perform(&session)
            .await?;
        let syncing = repo.branch("feature").open().perform(&session).await?;
        syncing.pull_from(&main).perform(&session).await?;

        let writing = repo.branch("feature").open().perform(&session).await?;
        let writer = writing.writer();
        let held = writer.lock().await;

        let mut pulling = Box::pin(syncing.pull().download().perform(&session));
        for _ in 0..1000 {
            let landed = poll_fn(|context| {
                Poll::Ready(match pulling.as_mut().poll(context) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                })
            })
            .await;
            if let Some(result) = landed {
                panic!("the pull landed past the write lock another handle held: {result:?}");
            }
            yield_once().await;
        }

        drop(held);
        let pulled = pulling.await?;
        assert_eq!(
            pulled,
            main.revision(),
            "released, the pull adopts main's head"
        );
        Ok(())
    }
}
