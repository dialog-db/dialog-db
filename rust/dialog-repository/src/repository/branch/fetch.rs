use futures_util::future::join_all;

use super::resolve::resolve;
use crate::registry::RegistryEnv;
use crate::{Branch, FetchError, RepositoryMemoryExt, Revision, Upstream};

/// Command fetching the current revision of every branch a branch pulls
/// from.
///
/// Does NOT modify the branch (only reads its upstreams, and caches
/// what each peer's branch is at).
pub struct Fetch<'a> {
    branch: &'a Branch,
}

/// What an upstream was at when fetched.
#[derive(Debug, Clone)]
pub struct Fetched {
    /// The upstream fetched from.
    pub upstream: Upstream,
    /// Its current revision, or `None` if it has none yet.
    pub revision: Option<Revision>,
}

impl Branch {
    /// Create a command to fetch every upstream this branch pulls from.
    ///
    /// Does NOT modify local state, only reads from upstreams.
    pub fn fetch(&self) -> Fetch<'_> {
        Fetch { branch: self }
    }
}

impl Fetch<'_> {
    /// Fetch every upstream this branch pulls from, concurrently.
    pub async fn perform<Env: RegistryEnv>(self, env: &Env) -> Result<Vec<Fetched>, FetchError> {
        let branch = self.branch;
        resolve(branch, env).await?;
        let upstreams = branch.pulls();
        if upstreams.is_empty() {
            return Err(FetchError::BranchHasNoUpstream {
                branch: branch.name().to_string(),
            });
        }
        // Every upstream is fetched, whether or not another can be
        // reached: one that cannot does not hide what the rest are at.
        let results = join_all(
            upstreams
                .iter()
                .map(|upstream| async move { (upstream, fetch_one(branch, upstream, env).await) }),
        )
        .await;
        let total = results.len();
        let mut fetched = Vec::new();
        let mut unreached = Vec::new();
        for (upstream, result) in results {
            match result {
                Ok(revision) => fetched.push(Fetched {
                    upstream: upstream.clone(),
                    revision,
                }),
                Err(error) => unreached.push((upstream.target(), error)),
            }
        }
        match unreached.len() {
            0 => Ok(fetched),
            failed if failed == total => Err(unreached.remove(0).1),
            _ => Err(FetchError::Partial { fetched, unreached }),
        }
    }
}

/// The current revision of `upstream`: read locally for a branch on this
/// replica, fetched from its peer for one on another.
pub(crate) async fn fetch_one<Env: RegistryEnv>(
    branch: &Branch,
    upstream: &Upstream,
    env: &Env,
) -> Result<Option<Revision>, FetchError> {
    match upstream {
        Upstream::Local { branch: name, .. } => {
            let upstream = branch
                .subject()
                .branch(name.clone())
                .load()
                .perform(env)
                .await?;
            Ok(upstream.revision())
        }
        Upstream::Remote {
            remote,
            branch: name,
            ..
        } => {
            let remote_branch = remote.branch(name.clone()).open().perform(env).await?;
            Ok(remote_branch.fetch().perform(env).await?)
        }
        Upstream::Unreachable { target, reason, .. } => Err(FetchError::Unreachable {
            upstream: target.to_string(),
            reason: reason.clone(),
        }),
    }
}
#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_operator::helpers::test_operator_with_profile;

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

        let fetched = feature
            .fetch()
            .perform(&operator)
            .await?
            .into_iter()
            .next()
            .and_then(|fetched| fetched.revision);

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
