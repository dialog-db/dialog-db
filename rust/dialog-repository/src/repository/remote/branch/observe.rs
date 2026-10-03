//! Observe command for remote branches.

use super::RemoteEdition;
use crate::{ConnectedBranch, ObserveRemoteBranchError, Revision};
use dialog_artifacts::DialogArtifactsError;
use dialog_capability::Provider;
use dialog_capability::identity::Precedence;
use dialog_common::ConditionalSync;
use dialog_effects::memory::Publish;

/// Command to record a head of the remote branch that was observed
/// rather than fetched: delivered by a subscription, say, or by a peer.
///
/// An observed head may arrive late, after a newer one was fetched or
/// observed, so it is recorded only when it is ahead of the head already
/// known (see [`Revision::precedence`]). Recording it makes it the head a
/// pull or push [assuming upstream](crate::Pull::assuming_upstream)
/// acts on, without a round trip to the remote.
pub struct ObserveRemoteBranch<'a> {
    branch: &'a ConnectedBranch,
    edition: RemoteEdition,
}

/// What observing a head did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation {
    /// The head is ahead of the one known, and is now the one known.
    Advanced(Revision),
    /// The head is the one known or an earlier one, and was ignored.
    Stale,
    /// Neither the head nor the one known built on the other, so which is
    /// newer is not known. Nothing was recorded; fetch to learn where the
    /// remote stands.
    Diverged,
}

impl<'a> ObserveRemoteBranch<'a> {
    /// Create a new observe command.
    pub fn new(branch: &'a ConnectedBranch, edition: RemoteEdition) -> Self {
        Self { branch, edition }
    }

    /// Execute the observation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Observation, ObserveRemoteBranchError>
    where
        Env: Provider<Publish> + ConditionalSync,
    {
        let observed = &self.edition.content;
        observed.verify().map_err(DialogArtifactsError::from)?;

        if let Some(known) = self.branch.revision() {
            match observed.precedence(&known) {
                Precedence::After => {}
                Precedence::Same | Precedence::Before => return Ok(Observation::Stale),
                Precedence::Concurrent => return Ok(Observation::Diverged),
            }
        }

        let revision = observed.clone();
        self.branch
            .cache()
            .publish(self.edition.clone())
            .perform(env)
            .await?;
        self.branch.upstream().reset(self.edition);
        Ok(Observation::Advanced(revision))
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Observation;
    use crate::helpers::{connect, test_repo};
    use crate::{
        Branch, ConnectedBranch, ObserveRemoteBranchError, RemoteEdition, ResolveEnv, Revision,
        TreeReference, Upstream,
    };
    use anyhow::Result;
    use dialog_artifacts::{Artifact, DialogArtifactsError, Entity, Instruction, Value};
    use dialog_effects::memory::Version;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_remote_s3::Address;
    use futures_util::stream;

    /// A remote nothing answers at: a test that reached it would fail
    /// rather than wait.
    fn unreachable() -> Address {
        Address::builder("http://127.0.0.1:1")
            .region("us-east-1")
            .bucket("bucket")
            .build()
            .unwrap()
    }

    fn edition(revision: &Revision, version: &[u8]) -> RemoteEdition {
        RemoteEdition {
            content: revision.clone(),
            version: Version::from(version),
        }
    }

    /// The remote branch `branch` pulls from, as its pull reaches it: the
    /// handle whose recorded head a pull assuming the upstream merges.
    async fn upstream_of<Env>(branch: &Branch, env: &Env) -> Result<ConnectedBranch>
    where
        Env: ResolveEnv,
    {
        let Some(Upstream::Remote { remote, branch, .. }) = branch.pulls().iter().next().cloned()
        else {
            anyhow::bail!("the branch pulls from no remote");
        };
        Ok(remote.branch(branch).open().perform(env).await?)
    }

    async fn commit<Env>(branch: &Branch, name: &str, env: &Env) -> Result<Revision>
    where
        Env: ResolveEnv,
    {
        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: format!("user:{name}").parse()?,
                is: Value::String(name.into()),
                cause: None,
            })]))
            .perform(env)
            .await?;
        Ok(branch.revision().expect("a commit leaves a head"))
    }

    /// A head observed after a newer one was observed is ignored, so a
    /// pull assuming the upstream keeps its sync point at the newer head
    /// and a push assuming it has nothing to send. Recording the stale
    /// head would move the sync point back to it, and the push would
    /// then try to upload to the remote.
    #[dialog_common::test]
    async fn it_ignores_a_head_observed_after_a_newer_one() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        // Two heads the remote branch took in turn, minted here.
        let writer = repo.branch("writer").open().perform(&operator).await?;
        let first = commit(&writer, "first", &operator).await?;
        let second = commit(&writer, "second", &operator).await?;

        let origin = connect("origin", unreachable(), repo.did(), &operator).await?;
        let main = repo.branch("main").open().perform(&operator).await?;
        let target = origin.branch("main").open().perform(&operator).await?;
        main.set_upstream(target).perform(&operator).await?;
        let remote = upstream_of(&main, &operator).await?;

        // The newer head is observed first, and pulled.
        let observed = remote
            .observe(edition(&second, b"second"))
            .perform(&operator)
            .await?;
        assert_eq!(observed, Observation::Advanced(second.clone()));
        main.pull().assuming_upstream().perform(&operator).await?;
        assert_eq!(
            main.revision().map(|head| head.tree),
            Some(second.tree.clone())
        );

        // The older head arrives late.
        let remote = upstream_of(&main, &operator).await?;
        let late = remote
            .observe(edition(&first, b"first"))
            .perform(&operator)
            .await?;

        main.pull().assuming_upstream().perform(&operator).await?;
        let pushed = main.push().assuming_upstream().perform(&operator).await?;
        assert_eq!(
            pushed.map(|head| head.tree),
            Some(second.tree.clone()),
            "nothing to push: the sync point is still the newer head"
        );
        assert_eq!(late, Observation::Stale);
        let remote = upstream_of(&main, &operator).await?;
        assert_eq!(remote.revision(), Some(second));
        Ok(())
    }

    /// A head that neither built on the one known nor was built on by it
    /// is not recorded: which is newer is not known.
    #[dialog_common::test]
    async fn it_records_nothing_for_a_head_built_apart() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let ours = repo.branch("ours").open().perform(&operator).await?;
        let theirs = repo.branch("theirs").open().perform(&operator).await?;
        let known = commit(&ours, "ours", &operator).await?;
        let apart = commit(&theirs, "theirs", &operator).await?;

        let origin = connect("origin", unreachable(), repo.did(), &operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        remote
            .observe(edition(&known, b"known"))
            .perform(&operator)
            .await?;
        let observed = remote
            .observe(edition(&apart, b"apart"))
            .perform(&operator)
            .await?;

        assert_eq!(observed, Observation::Diverged);
        let remote = origin.branch("main").open().perform(&operator).await?;
        assert_eq!(remote.revision(), Some(known));
        Ok(())
    }

    /// A head its issuer did not sign is refused, and nothing is recorded.
    #[dialog_common::test]
    async fn it_refuses_an_unsigned_head() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let origin = connect("origin", unreachable(), repo.did(), &operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        let forged = Revision::new(
            TreeReference::from([9u8; 32]),
            "branch:main".parse::<Entity>()?,
            operator.did(),
        );
        let observed = remote
            .observe(edition(&forged, b"forged"))
            .perform(&operator)
            .await;

        assert!(
            matches!(
                observed,
                Err(ObserveRemoteBranchError::Artifact(
                    DialogArtifactsError::InvalidSignature(_)
                ))
            ),
            "an unsigned head must be refused; got {observed:?}"
        );
        let remote = origin.branch("main").open().perform(&operator).await?;
        assert_eq!(remote.revision(), None);
        Ok(())
    }
}
