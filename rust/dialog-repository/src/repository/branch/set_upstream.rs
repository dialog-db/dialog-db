//! Recording what a branch pulls from and pushes to.

use dialog_artifacts::Changes;
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_query::Statement as _;

use super::resolve::resolve;
use crate::ResolveEnv;
use crate::registry::{apply, pull, push};
use crate::schema::{Branch as BranchConcept, Replica};
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

/// Command removing a branch this one pulls from, pushes to, or both.
/// Created by [`Branch::unset_upstream`], [`Branch::stop_pulling_from`],
/// and [`Branch::stop_pushing_to`].
pub struct UnsetUpstream<'a> {
    branch: &'a Branch,
    target: UpstreamBranch,
    direction: Direction,
}

impl Branch {
    /// Record `target` as a branch this one both pulls from and pushes
    /// to, the way a git upstream is tracked in both directions.
    ///
    /// Accepts a local [`Branch`] or a [`ConnectedBranch`](crate::ConnectedBranch)
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

impl Branch {
    /// Stop pulling from and pushing to `target`: the relations
    /// [`set_upstream`](Self::set_upstream) recorded are retracted, and
    /// every other upstream is kept. The tree this branch was last in
    /// sync with `target` at stays recorded: it says where content the
    /// branch adopted came from, and is the base to sync from should
    /// `target` be set again.
    pub fn unset_upstream(&self, target: impl Into<UpstreamBranch>) -> UnsetUpstream<'_> {
        UnsetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Both,
        }
    }

    /// Stop pulling from `target`.
    pub fn stop_pulling_from(&self, target: impl Into<UpstreamBranch>) -> UnsetUpstream<'_> {
        UnsetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Pull,
        }
    }

    /// Stop pushing to `target`.
    pub fn stop_pushing_to(&self, target: impl Into<UpstreamBranch>) -> UnsetUpstream<'_> {
        UnsetUpstream {
            branch: self,
            target: target.into(),
            direction: Direction::Push,
        }
    }
}

/// The registry's concepts of `branch` and of the `target` it relates
/// to.
async fn related<Env: ResolveEnv>(
    branch: &Branch,
    target: &UpstreamBranch,
    env: &Env,
) -> Result<(BranchConcept, BranchConcept), SetUpstreamError> {
    let operator = Identify.perform(env).await?;
    let local = Replica::new(operator.profile().clone(), branch.of().clone());
    let this = local.branch(branch.name());
    let target = match target {
        UpstreamBranch::Local(target) => {
            if target.of() != branch.of() {
                return Err(SetUpstreamError::ForeignLocalUpstream {
                    branch: branch.name().to_string(),
                    target: target.name().to_string(),
                });
            }
            if target.name() == branch.name() {
                return Err(SetUpstreamError::UpstreamIsItself {
                    branch: branch.name().to_string(),
                });
            }
            local.branch(target.name())
        }
        UpstreamBranch::Remote(target) => target.concept(),
    };
    Ok((this, target))
}

impl SetUpstream<'_> {
    /// Record the relations in the registry, and bring this branch's
    /// routes up to date with them.
    pub async fn perform<Env: ResolveEnv>(self, env: &Env) -> Result<(), SetUpstreamError> {
        let branch = self.branch;
        let (this, target) = related(branch, &self.target, env).await?;

        let mut changes = Changes::new();
        if let UpstreamBranch::Remote(remote) = &self.target {
            // The tracked branch and its replica are recorded with the
            // relation, so the rule resolving it can place it.
            remote.repository().replica().assert(&mut changes);
        }
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

impl UnsetUpstream<'_> {
    /// Retract the relations from the registry, and bring this branch's
    /// routes up to date without them. The tracked branch stays
    /// recorded, since another branch may still relate to it.
    pub async fn perform<Env: ResolveEnv>(self, env: &Env) -> Result<(), SetUpstreamError> {
        let branch = self.branch;
        let (this, target) = related(branch, &self.target, env).await?;

        let mut changes = Changes::new();
        if matches!(self.direction, Direction::Pull | Direction::Both) {
            pull(&this, &target).retract(&mut changes);
        }
        if matches!(self.direction, Direction::Push | Direction::Both) {
            push(&this, &target).retract(&mut changes);
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
    use dialog_peer::helpers::test_session_with_peer;
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
        let (operator, profile) = test_session_with_peer().await;
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = connect("origin", site(), repo.did(), &operator).await?;
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = connect("origin", site(), repo.did(), &operator).await?;
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
        let (operator, profile) = test_session_with_peer().await;
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
        let (operator, profile) = test_session_with_peer().await;
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

    /// Unsetting an upstream retracts it and keeps every other one, and
    /// a branch reopened from storage no longer tracks it.
    #[dialog_common::test]
    async fn it_unsets_one_upstream_and_keeps_the_rest() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let origin = connect("origin", site(), repo.did(), &operator).await?;
        let remote_main = origin.branch("main").open().perform(&operator).await?;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature
            .set_upstream(&remote_main)
            .perform(&operator)
            .await?;

        feature
            .unset_upstream(&remote_main)
            .perform(&operator)
            .await?;

        let reopened = repo.branch("feature").open().perform(&operator).await?;
        for branch in [&feature, &reopened] {
            for upstreams in [branch.pulls(), branch.pushes()] {
                assert!(matches!(
                    upstreams.iter().collect::<Vec<_>>().as_slice(),
                    [Upstream::Local { branch, .. }] if branch == "main"
                ));
            }
        }
        Ok(())
    }

    /// Stopping one direction leaves the other in place.
    #[dialog_common::test]
    async fn it_stops_one_direction_at_a_time() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        feature.stop_pushing_to(&main).perform(&operator).await?;
        assert_eq!(feature.pushes().iter().count(), 0);
        assert_eq!(feature.pulls().iter().count(), 1);

        feature.stop_pulling_from(&main).perform(&operator).await?;
        assert_eq!(feature.upstreams().iter().count(), 0);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_setting_upstream_to_self() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
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
