//! Watch command for remote branches.

use async_stream::stream;
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::{self, Publish};
use futures_util::Stream;

use super::RemoteEdition;
use crate::{ConnectedBranch, Observation, RemoteSite, WatchRemoteBranchError};

/// Command to follow the remote branch's head as it moves.
///
/// The remote answers the head when the watch begins, then each head the
/// branch takes, and each is recorded as an observed head is (see
/// [`ConnectedBranch::observe`]): only when it is ahead of the one known.
/// A pull or push assuming the upstream then acts on it with no round
/// trip of its own.
///
/// A remote that cannot follow a cell refuses the watch with
/// `Rejection::Unsupported`, and the branch is fetched instead.
pub struct WatchRemoteBranch<'a> {
    branch: &'a ConnectedBranch,
}

impl<'a> WatchRemoteBranch<'a> {
    /// Create a new watch command.
    pub fn new(branch: &'a ConnectedBranch) -> Self {
        Self { branch }
    }

    /// Begin the watch, answering the observations of the heads the remote
    /// delivers, until the watch ends.
    ///
    /// Refused here when the remote does not begin it. Once begun, a head
    /// that cannot be read or recorded is answered as an error and the
    /// watch goes on; it ends when the remote stops answering, the last
    /// item saying why when it was not the remote's doing to end it
    /// cleanly.
    pub async fn perform<'e, Env>(
        self,
        env: &'e Env,
    ) -> Result<
        impl Stream<Item = Result<Observation, WatchRemoteBranchError>> + 'e,
        WatchRemoteBranchError,
    >
    where
        Env: Provider<Fork<RemoteSite, memory::Watch>> + Provider<Publish> + ConditionalSync,
    {
        let branch = self.branch.clone();
        let connection = branch.repository().connection(env);
        let mut editions =
            Provider::<memory::Watch>::execute(&connection, branch.upstream().watch()).await?;
        Ok(stream! {
            loop {
                let state = match editions.next().await {
                    Ok(Some(state)) => state,
                    Ok(None) => break,
                    Err(error) => {
                        yield Err(WatchRemoteBranchError::from(error));
                        break;
                    }
                };
                // A branch the remote has no head for yet has nothing to
                // record.
                let Some(edition) = state else { continue };
                let revision = match branch.upstream().decode(&edition.content).await {
                    Ok(revision) => revision,
                    Err(error) => {
                        yield Err(error.into());
                        continue;
                    }
                };
                let observed = branch
                    .observe(RemoteEdition {
                        content: revision,
                        version: edition.version,
                    })
                    .perform(env)
                    .await;
                yield observed.map_err(WatchRemoteBranchError::from);
            }
        })
    }
}
