//! Recording what a branch pulls from and pushes to.

use dialog_artifacts::Changes;
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_query::Statement as _;

use super::resolve::resolve;
use crate::registry::{RegistryEnv, apply, pull, push};
use crate::schema::Replica;
use crate::{Branch, RepositoryMemoryExt as _, SetUpstreamError, UpstreamBranch};

/// Which relations a [`SetUpstream`] records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Pull,
    Push,
    Both,
}

/// Command recording a branch to pull from, push to, or both. Created
/// by [`Branch::set_upstream`], [`Branch::pull_from`], and
/// [`Branch::push_to`].
pub struct SetUpstream<'a> {
    branch: &'a Branch,
    target: UpstreamBranch,
    direction: Direction,
}

impl Branch {
    /// Record `target` as a branch this one both pulls from and pushes
    /// to, the way a git upstream is tracked in both directions.
    ///
    /// Accepts a local [`Branch`] or a [`RemoteBranch`](crate::RemoteBranch)
    /// opened at a peer. A branch may track several: a bare
    /// [`pull`](Self::pull) takes from every one and a bare
    /// [`push`](Self::push) goes to every one.
    pub fn set_upstream(&self, target: impl Into<UpstreamBranch>) -> SetUpstream<'_> {
        SetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Both,
        }
    }

    /// Record `target` as a branch this one pulls from.
    pub fn pull_from(&self, target: impl Into<UpstreamBranch>) -> SetUpstream<'_> {
        SetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Pull,
        }
    }

    /// Record `target` as a branch this one pushes to.
    pub fn push_to(&self, target: impl Into<UpstreamBranch>) -> SetUpstream<'_> {
        SetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Push,
        }
    }
}

impl SetUpstream<'_> {
    /// Record the relations in the registry, and bring this branch's
    /// routes up to date with them.
    pub async fn perform<Env: RegistryEnv>(self, env: &Env) -> Result<(), SetUpstreamError> {
        let branch = self.branch;
        let operator = Identify.perform(env).await?;
        let local = Replica::new(operator.profile().clone(), branch.of().clone());
        let this = local.branch(branch.name());

        let mut changes = Changes::new();
        let target = match &self.target {
            UpstreamBranch::Local(target) => {
                if target.name() == branch.name() && target.of() == branch.of() {
                    return Err(SetUpstreamError::UpstreamIsItself {
                        branch: branch.name().to_string(),
                    });
                }
                local.branch(target.name())
            }
            UpstreamBranch::Remote(target) => {
                // The tracked branch and its replica are recorded with the
                // relation, so the rule resolving it can place it.
                target.repository().replica().assert(&mut changes);
                target.concept()
            }
        };
        target.clone().assert(&mut changes);
        if matches!(self.direction, Direction::Pull | Direction::Both) {
            pull(&this, &target).assert(&mut changes);
        }
        if matches!(self.direction, Direction::Push | Direction::Both) {
            push(&this, &target).assert(&mut changes);
        }

        let registry = branch.subject().registry().open().perform(env).await?;
        apply(&registry, changes, env).await?;
        resolve(branch, env).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{connect, test_repo};
    use crate::{SetUpstreamError, Upstream};
    use anyhow::Result;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_remote_s3::Address;

    fn site() -> Address {
        Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()
            .expect("valid address")
    }

    /// A local upstream is pulled from and pushed to, by name.
    #[dialog_common::test]
    async fn it_sets_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let main = repo.branch("main").open().perform(&operator).await?;

        feature.set_upstream(&main).perform(&operator).await?;

        for upstreams in [feature.pulls(), feature.pushes()] {
            assert!(matches!(
                upstreams.iter().next(),
                Some(Upstream::Local { branch, .. }) if branch == "main"
            ));
        }
        Ok(())
    }

    /// A branch at a peer resolves to that peer's repository and the
    /// branch there.
    #[dialog_common::test]
    async fn it_sets_remote_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = connect(&repo, "origin", site(), repo.did(), &operator).await?;
        let remote_main = origin.branch("main").open().perform(&operator).await?;

        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.set_upstream(&remote_main).perform(&operator).await?;

        assert!(matches!(
            branch.pulls().iter().next(),
            Some(Upstream::Remote { remote, branch, .. })
                if remote.same(&origin) && branch == "main"
        ));
        Ok(())
    }

    /// The resolved upstream is kept in the branch's tracking cell, so a
    /// branch reopened from storage knows it without asking the registry.
    #[dialog_common::test]
    async fn it_persists_remote_upstream_across_reload() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = connect(&repo, "origin", site(), repo.did(), &operator).await?;
        let remote_main = origin.branch("main").open().perform(&operator).await?;

        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.set_upstream(&remote_main).perform(&operator).await?;

        let reopened = repo.branch("main").open().perform(&operator).await?;
        assert!(matches!(
            reopened.pulls().iter().next(),
            Some(Upstream::Remote { remote, branch, .. })
                if remote.same(&origin) && branch == "main"
        ));
        Ok(())
    }

    /// Setting a second upstream keeps the first: a branch pulls from
    /// and pushes to every one, with none singled out.
    #[dialog_common::test]
    async fn it_tracks_every_upstream_it_is_given() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let dev = repo.branch("dev").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;

        feature.set_upstream(&main).perform(&operator).await?;
        feature.set_upstream(&dev).perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let mut names: Vec<String> = feature
            .pulls()
            .iter()
            .filter_map(|upstream| match upstream {
                Upstream::Local { branch, .. } => Some(branch.clone()),
                _ => None,
            })
            .collect();
        names.sort();
        assert_eq!(names, vec!["dev".to_string(), "main".to_string()]);
        assert_eq!(feature.pushes().iter().count(), 2);
        Ok(())
    }

    /// `pull_from` and `push_to` each record one direction only.
    #[dialog_common::test]
    async fn it_records_one_direction_at_a_time() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let backup = repo.branch("backup").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;

        feature.pull_from(&main).perform(&operator).await?;
        feature.push_to(&backup).perform(&operator).await?;

        assert!(matches!(
            feature.pulls().iter().collect::<Vec<_>>().as_slice(),
            [Upstream::Local { branch, .. }] if branch == "main"
        ));
        assert!(matches!(
            feature.pushes().iter().collect::<Vec<_>>().as_slice(),
            [Upstream::Local { branch, .. }] if branch == "backup"
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_setting_upstream_to_self() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let result = branch.set_upstream(&branch).perform(&operator).await;

        assert!(matches!(
            result,
            Err(SetUpstreamError::UpstreamIsItself { .. })
        ));

        Ok(())
    }
}
