use crate::{Branch, FetchError, RemoteSite, RepositoryMemoryExt, Revision, Upstream};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::{Publish, Resolve};

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
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, FetchError>
    where
        Env: Provider<Resolve>
            + Provider<Publish>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync,
    {
        let upstream = self
            .branch
            .upstream()
            .ok_or_else(|| FetchError::BranchHasNoUpstream {
                branch: self.branch.name().to_string(),
            })?;

        match &upstream {
            Upstream::Local { branch: name, .. } => {
                let upstream = self
                    .branch
                    .subject()
                    .branch(name.clone())
                    .load()
                    .perform(env)
                    .await?;
                Ok(upstream.revision())
            }
            Upstream::Remote {
                remote: name,
                branch: branch_name,
                ..
            } => {
                let remote_repo = self
                    .branch
                    .subject()
                    .remote(name.clone())
                    .load()
                    .perform(env)
                    .await?;
                let remote_branch = remote_repo
                    .branch(branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;
                Ok(remote_branch.fetch().perform(env).await?)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_fetches_local_upstream_revision() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let fetched = feature.fetch().perform(&operator).await?;

        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().tree, main_revision.tree);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_modify_local_state_on_fetch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let feature_revision_before = feature.revision();

        let _fetched = feature.fetch().perform(&operator).await?;

        // Fetch should not modify local state
        assert_eq!(feature.revision(), feature_revision_before);

        Ok(())
    }
}
