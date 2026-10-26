use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::Rejection;
use dialog_effects::archive::{Get as ArchiveGet, Put as ArchivePut};
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::{Publish, Resolve};

use super::context::{full_context, head_observes};
use crate::{Branch, NetworkedIndex, PublishError, Revision};

/// Command that resets a branch to a given revision.
///
/// Reset moves a branch forward only: to a revision that holds the
/// branch's head, as a head established elsewhere does (a push, a pull on
/// another handle). A reset to a revision that does not hold the head is
/// refused with [`PublishError::Rejected`] carrying
/// [`Rejection::Unsupported`], and the head does not move.
///
/// Moving a branch back, or onto another writer's line that lacks this
/// writer's commits, and then committing, breaks what every causal context
/// assumes: that one origin's revisions form one chain. Below the writer's
/// last revision the next commit re-mints a version already used; above
/// it, on a line that lacks that revision, the commit forks the origin.
/// Either way a context that holds one revision claims the other, and a
/// merge drops writes as already seen (#682). To go back, start a new
/// branch from the older revision; to take another line's work, pull it.
pub struct Reset<'a> {
    branch: &'a Branch,
    revision: Revision,
}

impl<'a> Reset<'a> {
    fn new(branch: &'a Branch, revision: Revision) -> Self {
        Self { branch, revision }
    }
}

impl Branch {
    /// Create a command to move the branch forward to a given revision,
    /// one that holds its head (see [`Reset`]).
    pub fn reset(&self, revision: Revision) -> Reset<'_> {
        Reset::new(self, revision)
    }
}

impl Reset<'_> {
    /// Execute the reset operation.
    ///
    /// The head moves under the branch's write lock, as it does for a
    /// commit or a pull: a writer of this branch minting on the head in
    /// this process sees it move before or after, never in between.
    ///
    /// Whether the revision holds the head is one entry of its context:
    /// the memo, its edition when the head is of the same origin, or one
    /// point read of its region, through the store that serves the
    /// branch's blocks. A branch with no head, or a revision of the head's
    /// own tree, moves at no read.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishError>
    where
        Env: Provider<Publish>
            + Provider<BlobRead>
            + Provider<ArchiveGet>
            + Provider<ArchivePut>
            + Provider<Resolve>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let writer = self.branch.writer();
        let _writing = writer.lock().await;
        if let Some(head) = self.branch.revision()
            && head.tree != self.revision.tree
            && !holds(self.branch, &self.revision, &head, env).await?
        {
            return Err(PublishError::Rejected(Rejection::Unsupported {
                reason: format!(
                    "branch {} cannot be reset to a revision that does not hold its head \
                     ({}@{}): a reset only moves a branch forward. Pull the work instead, \
                     or start a new branch from that revision",
                    self.branch.name(),
                    head.edition,
                    head.origin(),
                ),
            }));
        }
        self.branch
            .revision
            .publish(self.revision)
            .perform(env)
            .await
    }

    /// Move the head to the revision without checking that it holds the
    /// head.
    ///
    /// For a caller that has established that already (a push checks the
    /// target's head against its own before moving it), and for tests that
    /// rewind a branch on purpose: a release before #682 resets any way, so
    /// its replicas still produce the heads those tests build, and pulls and
    /// pushes on this release have to meet them.
    pub(crate) async fn perform_unchecked<Env>(self, env: &Env) -> Result<(), PublishError>
    where
        Env: Provider<Publish>,
    {
        let writer = self.branch.writer();
        let _writing = writer.lock().await;
        self.branch
            .revision
            .publish(self.revision)
            .perform(env)
            .await
    }
}

/// Whether `revision` holds `head`: its context observes `head`'s version.
async fn holds<Env>(
    branch: &Branch,
    revision: &Revision,
    head: &Revision,
    env: &Env,
) -> Result<bool, PublishError>
where
    Env: Provider<BlobRead>
        + Provider<ArchiveGet>
        + Provider<ArchivePut>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
{
    let version = head.version();
    let store = NetworkedIndex::erased(env, branch.archive().index(), branch.fallback());
    let unreadable = |error: dialog_artifacts::DialogArtifactsError| {
        PublishError::Storage(format!(
            "could not tell whether the revision holds branch {}'s head: {error}",
            branch.name()
        ))
    };
    if let Some(seen) = head_observes(
        revision,
        &version,
        &branch.contexts(),
        &branch.records(),
        &store,
        &branch.node_cache(),
    )
    .await
    .map_err(unreadable)?
    {
        return Ok(seen);
    }
    let history = branch.history(env);
    let context = full_context(
        revision,
        &branch.contexts(),
        &store,
        &branch.node_cache(),
        &history,
    )
    .await
    .map_err(unreadable)?;
    Ok(context.observes(&version))
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;

    use dialog_artifacts::history::Edition;
    use dialog_capability::Subject;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::{RepositoryMemoryExt, Revision, TreeReference};

    /// The head cell takes the revision. A bare storage provider cannot
    /// read an archive, so it moves the head without the check; the checked
    /// path is covered with sessions in `pull::sync_base_tests`.
    #[dialog_common::test]
    async fn it_sets_revision() -> Result<()> {
        let provider = Volatile::new();
        let subject = Subject::from(did!("key:zBranchResetTest"));

        let branch = subject.branch("main").open().perform(&provider).await?;
        assert!(branch.revision().is_none());

        let revision = Revision {
            branch: "branch:main".parse()?,
            issuer: subject.did().clone(),
            tree: TreeReference::from([7u8; 32]),
            edition: Edition::GENESIS,
            context: None,
            region: false,
            count: None,
            signature: Vec::new(),
        };
        branch
            .reset(revision.clone())
            .perform_unchecked(&provider)
            .await?;

        assert_eq!(branch.revision(), Some(revision));
        Ok(())
    }
}
