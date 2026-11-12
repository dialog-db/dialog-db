//! Bring a branch's content local.
//!
//! A replica holds what its reads reached and its writes minted, and the
//! rest of its head by reference: a block it lacks is fetched from the
//! branch's upstream when a read first asks for it. How much of a
//! revision is brought local ahead of that is a decision, and
//! [`Download`] is its vocabulary. A pull is asked it every time
//! ([`Branch::pull`] takes one) and fetches what it names before the head
//! moves; [`Branch::download`] asks it of the head a branch already
//! stands on, or of a revision it is about to adopt.

use core::ops::RangeInclusive;
use dialog_corpus::tree::CorpusNode;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::blob::Load as BlobLoad;
use dialog_search_tree::nodes;

use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_corpus::Index;
use dialog_corpus::merge::data_scope;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Resolve;
use futures_util::StreamExt as _;

use crate::HeldCaches;
use crate::repository::archive::routes;
use crate::repository::snapshot::Snapshot;
use crate::{Branch, DownloadError, RemoteFallback, RemoteSite, Revision, TreeReference, Upstream};

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

/// How much of a revision is brought local, beyond what the replica
/// already holds.
///
/// Every pull names one, so that how much a pull fetches is decided where
/// the pull is written. There is no default: the right answer differs by
/// branch, and a wrong one is either a replica that waits on the network
/// for what it showed a moment ago, or one that pays for a space it never
/// looked at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Download {
    /// Nothing. A head is adopted by reference, and a block is fetched
    /// when a read first asks for it.
    ///
    /// The cheapest pull, and the one for a replica that is always
    /// connected or does not mind a read waiting. After it, a read or a
    /// write the replica answered from its own blocks can wait on the
    /// remote, wherever the upstream changed a leaf it holds.
    MinimalRanges,
    /// What this replica holds of its head, as the revision has it: the
    /// key ranges its queries read and its commits wrote.
    ///
    /// The pull for a replica that works offline. A replica holds the
    /// leaves its reads reached and its writes minted. A head adopted by
    /// reference leaves every leaf the upstream changed on the remote,
    /// held before or not, so a read or a write that was answered at once
    /// waits the next time, and fails without a network. This fetches
    /// those leaves first, and only those: the head's tree and the
    /// revision's are walked in step
    /// ([`PersistentTree::follow`](dialog_search_tree::PersistentTree::follow)),
    /// and a node of the revision is fetched only where its keys meet a
    /// leaf the replica holds. A range the replica never read stays by
    /// reference however much of it changed, so the cost follows what
    /// changed in what this replica uses.
    ///
    /// It brings tree nodes, not what they point at: a spilled value or
    /// a blob a changed leaf newly references is fetched when it is
    /// read. Of the head a branch already stands on it fetches nothing,
    /// and a branch with no head holds nothing to bring along.
    QueriedRanges,
    /// What reads and authorization touch: the entity, attribute and
    /// value orders and the blob index, with their spilled values and
    /// referenced blobs.
    ///
    /// The download a profile wants. What it leaves by reference is
    /// history and coverage, which no read path can reach, and which are
    /// the only regions that grow with every edit ever made and not with
    /// the live fact count. The revision DAG is not among them: revision
    /// records are ordinary facts in the data orders, so ancestry, `log`
    /// and skip tables keep working. A history record that turns out to
    /// be needed is fetched on demand.
    Operational,
    /// The key ranges given, with their spilled values and referenced
    /// blobs, the rest left by reference. See
    /// [`SnapshotExport::within`](crate::SnapshotExport::within).
    Ranges(Vec<RangeInclusive<Vec<u8>>>),
    /// Every block and blob the revision references.
    Everything,
}

/// Command struct for bringing a branch's content local. Created by
/// [`Branch::download`].
pub struct DownloadBranch<'a> {
    branch: &'a Branch,
    from: Option<Upstream>,
    revision: Option<Revision>,
    extent: Download,
}

impl Branch {
    /// Fetch what `extent` names of the current revision that the local
    /// store does not hold, from the branch's upstream.
    ///
    /// A no-op without a remote upstream (a local upstream shares the
    /// store, so there is nothing to fetch) or before the first
    /// revision.
    pub fn download(&self, extent: Download) -> DownloadBranch<'_> {
        DownloadBranch {
            branch: self,
            from: None,
            revision: None,
            extent,
        }
    }
}

impl<'a> DownloadBranch<'a> {
    /// The download of `revision` from `from`, as a pull makes it of the
    /// head it prepared.
    pub(crate) fn prepared(
        branch: &'a Branch,
        from: Upstream,
        revision: Revision,
        extent: Download,
    ) -> Self {
        Self {
            branch,
            from: Some(from),
            revision: Some(revision),
            extent,
        }
    }

    /// Download `revision` instead of the branch's current head: how a
    /// caller brings local a revision it prepared and has not adopted,
    /// so the head never advances past the blocks the store holds.
    pub fn of(mut self, revision: Revision) -> Self {
        self.revision = Some(revision);
        self
    }

    /// Fetch what the extent names.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DownloadError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, BlobRead>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let scope = match self.extent {
            Download::MinimalRanges => return Ok(()),
            Download::Everything => None,
            Download::Operational => Some(operational_scope()),
            Download::Ranges(scope) => Some(scope),
            Download::QueriedRanges => {
                let (Some(held), Some(revision)) = (branch.revision(), self.revision) else {
                    return Ok(());
                };
                if held.tree == revision.tree {
                    return Ok(());
                }
                // The revision may come from an upstream the branch does
                // not track: lend its route, so what the walk misses is
                // fetched from the peer that holds it.
                let fallback = match self.from {
                    Some(upstream) => upstream.fallback(),
                    None => branch.fallback(),
                };
                let _loan = routes::lend(branch.routes(), branch.of(), &fallback);
                let subject = branch.subject();
                let at =
                    |tree: &TreeReference| Index::at(subject.clone(), NodeHash::from(*tree.hash()));
                at(&revision.tree)
                    .follow(&at(&held.tree), env)
                    .await
                    .map_err(DownloadError::Tree)?;
                return Ok(());
            }
        };
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
        let export = Snapshot::cold(branch.subject(), revision, &HeldCaches::of(env))
            .export()
            .download(remote);
        let export = match scope {
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

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::future::{Future as _, poll_fn};
    use std::task::Poll;

    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_corpus::{Fact, Instruction, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use futures_util::stream;

    fn name(of: &str, is: &str) -> Result<Instruction> {
        Ok(Instruction::Assert(
            Fact {
                the: "user/name".parse()?,
                of: of.parse()?,
                is: Value::String(is.to_string()),
                cause: None,
            },
            dialog_corpus::Pick::All,
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

        let mut pulling = Box::pin(syncing.pull(crate::Download::Everything).perform(&session));
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
