use dialog_capability::{Fork, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::memory::{Publish, Resolve};

use super::{Branch, UpstreamState};
use crate::repository::error::RepositoryError;
use crate::repository::memory::RepositoryMemoryExt;
use crate::repository::remote::RemoteSite;
use crate::repository::revision::Revision;

/// Command struct for fetching the upstream branch's current revision.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote fetch logic.
///
/// Does NOT modify local state (only reads from upstream).
pub struct Fetch<'a> {
    branch: &'a Branch,
}

impl<'a> Fetch<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch }
    }
}

impl Branch {
    /// Create a command to fetch the upstream branch's current revision.
    ///
    /// Does NOT modify local state, only reads from upstream.
    pub fn fetch(&self) -> Fetch<'_> {
        Fetch::new(self)
    }
}

impl Fetch<'_> {
    /// Execute the fetch operation, returning the upstream revision.
    ///
    /// Returns `None` if the upstream has no revision yet.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Resolve>
            + Provider<Publish>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync,
    {
        let upstream =
            self.branch
                .upstream()
                .ok_or_else(|| RepositoryError::BranchHasNoUpstream {
                    name: self.branch.name().to_string(),
                })?;

        match &upstream {
            UpstreamState::Local { branch: name, .. } => {
                let upstream = Subject::from(self.branch.subject().clone())
                    .branch(name.clone())
                    .load()
                    .perform(env)
                    .await?;
                Ok(upstream.revision())
            }
            UpstreamState::Remote {
                name,
                branch: branch_name,
                ..
            } => {
                let remote_repo = Subject::from(self.branch.subject().clone())
                    .remote(name.clone())
                    .load()
                    .perform(env)
                    .await?;
                let remote_branch = remote_repo
                    .branch(branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;
                remote_branch.fetch().perform(env).await
            }
        }
    }
}
