//! Resolving where a branch's upstreams live.
//!
//! Which branches a branch pulls from and pushes to is recorded in the
//! registry. Resolving them -- to a local branch by name, or to a peer's
//! repository and the addresses the peer is reached at -- takes one query
//! per direction through the built-in upstream rules, and the result is
//! cached in the branch's tracking cell, stamped with the registry
//! revision it was resolved at. A sync re-resolves only when the registry
//! has moved since.

use std::sync::Arc;

use dialog_artifacts::Entity;
use dialog_capability::{Did, Subject};
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_query::{Output as _, Query, Term};

use crate::registry::RegistryEnv;
use crate::schema::{
    BranchPull, BranchPush, Peer, PeerAddress, PullUpstream, PushUpstream, Replica,
};
use crate::{Branch, REGISTRY, RepositoryMemoryExt as _, ResolveUpstreamsError, Resolved, Route};

/// The registry branch of `subject`, held warm by `env`: opened the first
/// time it is asked for, and reused after, with its head re-read so
/// writes through other handles are seen.
pub(crate) async fn registry<Env: RegistryEnv>(
    subject: &Subject,
    env: &Env,
) -> Result<Branch, crate::ResolveError> {
    let key = format!("dialog.registry:{}", subject.did());
    let held = env
        .held(&key)
        .and_then(|held| held.downcast_ref::<Branch>().cloned());
    if let Some(registry) = held {
        registry.refresh(env).await?;
        return Ok(registry);
    }
    let registry = subject.branch(REGISTRY).open().perform(env).await?;
    env.hold(key, Arc::new(registry.clone()));
    Ok(registry)
}

/// Bring `branch`'s cached routes up to date with the registry: a no-op
/// while the registry has not moved since they were resolved.
pub(crate) async fn resolve<Env: RegistryEnv>(
    branch: &Branch,
    env: &Env,
) -> Result<(), ResolveUpstreamsError> {
    if branch.name() == REGISTRY {
        return Ok(());
    }
    let registry = registry(&branch.subject(), env).await?;
    let at = registry.revision();
    if matches!(&branch.tracked().resolved, Some(resolved) if resolved.at == at) {
        return Ok(());
    }

    let operator = Identify.perform(env).await?;
    let local = Replica::new(operator.profile().clone(), branch.of().clone());
    let this = local.branch(branch.name()).this;
    let resolved = Resolved {
        at,
        pulls: pulls(&registry, &local, &this, env).await?,
        pushes: pushes(&registry, &local, &this, env).await?,
    };

    // Other syncs write this cell too, recording how far they got. On a
    // conflict, re-read it and lay the routes over what is there now.
    let cell = branch.tracking();
    for _ in 0..2 {
        let mut tracking = branch.tracked();
        tracking.resolved = Some(resolved.clone());
        match cell.checkpoint().publish(tracking, env).await {
            Ok(()) => return Ok(()),
            Err(crate::PublishError::VersionMismatch { .. }) => {
                cell.resolve().perform(env).await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Where each branch `this` pulls from lives.
async fn pulls<Env: RegistryEnv>(
    registry: &Branch,
    local: &Replica,
    this: &Entity,
    env: &Env,
) -> Result<Vec<Route>, ResolveUpstreamsError> {
    let related: Vec<BranchPull> = Box::pin(
        registry
            .query()
            .select(Query::<BranchPull> {
                this: this.clone().into(),
                pull: Term::var("pull"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    if related.is_empty() {
        return Ok(Vec::new());
    }
    let upstreams: Vec<PullUpstream> = Box::pin(
        registry
            .query()
            .select(Query::<PullUpstream> {
                this: this.clone().into(),
                upstream: Term::var("upstream"),
                name: Term::var("name"),
                subject: Term::var("subject"),
                peer: Term::var("peer"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let found = upstreams
        .into_iter()
        .map(|row| (row.upstream.0, row.name.0, row.subject.0, row.peer.0))
        .collect();
    let targets = related.into_iter().map(|row| row.pull.0).collect();
    routes(registry, local, targets, found, env).await
}

/// Where each branch `this` pushes to lives.
async fn pushes<Env: RegistryEnv>(
    registry: &Branch,
    local: &Replica,
    this: &Entity,
    env: &Env,
) -> Result<Vec<Route>, ResolveUpstreamsError> {
    let related: Vec<BranchPush> = Box::pin(
        registry
            .query()
            .select(Query::<BranchPush> {
                this: this.clone().into(),
                push: Term::var("push"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    if related.is_empty() {
        return Ok(Vec::new());
    }
    let upstreams: Vec<PushUpstream> = Box::pin(
        registry
            .query()
            .select(Query::<PushUpstream> {
                this: this.clone().into(),
                upstream: Term::var("upstream"),
                name: Term::var("name"),
                subject: Term::var("subject"),
                peer: Term::var("peer"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let found = upstreams
        .into_iter()
        .map(|row| (row.upstream.0, row.name.0, row.subject.0, row.peer.0))
        .collect();
    let targets = related.into_iter().map(|row| row.push.0).collect();
    routes(registry, local, targets, found, env).await
}

/// A route for each of `targets`: local when the rule placed it on this
/// replica, at its peer's addresses when on another's, and unreachable
/// when the rule could not place it or its peer has nowhere to be
/// reached -- so a relation never silently drops out.
async fn routes<Env: RegistryEnv>(
    registry: &Branch,
    local: &Replica,
    targets: Vec<Entity>,
    found: Vec<(Entity, String, Entity, Entity)>,
    env: &Env,
) -> Result<Vec<Route>, ResolveUpstreamsError> {
    let mut routes = Vec::with_capacity(targets.len());
    for target in targets {
        let Some((_, branch, subject, peer)) = found.iter().find(|(entity, ..)| *entity == target)
        else {
            routes.push(Route::Unreachable {
                target,
                reason: "the branch and its replica are not recorded".into(),
            });
            continue;
        };
        if *peer == local.peer.0 && *subject == local.subject.0 {
            routes.push(Route::Local {
                branch: branch.clone(),
            });
            continue;
        }
        let Ok(subject) = subject.to_string().parse::<Did>() else {
            routes.push(Route::Unreachable {
                target,
                reason: format!("{subject} is not a repository DID"),
            });
            continue;
        };
        let (name, addresses) = reach(registry, peer, env).await?;
        if addresses.is_empty() {
            routes.push(Route::Unreachable {
                target,
                reason: format!(
                    "peer {} has no address",
                    name.as_deref().unwrap_or(&peer.to_string())
                ),
            });
            continue;
        }
        routes.push(Route::Remote {
            peer: peer.clone(),
            name,
            addresses,
            subject,
            branch: branch.clone(),
        });
    }
    Ok(routes)
}

/// The name a peer is known by and the addresses it is reached at.
async fn reach<Env: RegistryEnv>(
    registry: &Branch,
    peer: &Entity,
    env: &Env,
) -> Result<(Option<String>, Vec<crate::SiteAddress>), ResolveUpstreamsError> {
    let names: Vec<Peer> = Box::pin(
        registry
            .query()
            .select(Query::<Peer> {
                this: peer.clone().into(),
                name: Term::var("name"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let rows: Vec<PeerAddress> = Box::pin(
        registry
            .query()
            .select(Query::<PeerAddress> {
                this: peer.clone().into(),
                address: Term::var("address"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    let addresses = rows
        .iter()
        .map(PeerAddress::site)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((names.into_iter().next().map(|row| row.name.0), addresses))
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{registry, resolve};
    use crate::helpers::test_repo;
    use crate::registry::{apply, pull};
    use crate::schema::{Peer, Replica};
    use crate::{PullError, Route, Upstream};
    use anyhow::Result;
    use dialog_artifacts::Changes;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::Statement as _;
    use dialog_varsig::did;

    /// Routes are cached against the registry revision they were resolved
    /// at: an upstream recorded through another handle is picked up the
    /// next time this one resolves, because the registry moved.
    #[dialog_common::test]
    async fn it_resolves_again_when_the_registry_moves() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let dev = repo.branch("dev").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.pull_from(&main).perform(&operator).await?;
        assert_eq!(feature.pulls().iter().count(), 1);

        let other = repo.branch("feature").open().perform(&operator).await?;
        other.pull_from(&dev).perform(&operator).await?;

        resolve(&feature, &operator).await?;
        feature.refresh(&operator).await?;
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
        Ok(())
    }

    /// A branch pulled from whose peer has nowhere to be reached resolves
    /// as unreachable rather than dropping out, and pulling from it says
    /// why.
    #[dialog_common::test]
    async fn it_keeps_an_unreachable_upstream_and_says_why() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let feature = repo.branch("feature").open().perform(&operator).await?;

        // A pull relation to a branch on a peer with no address.
        let local = Replica::new(profile.did(), repo.did());
        let replica = Peer::new(&did!("web:nowhere.example"), "nowhere").repository(repo.did());
        let target = replica.branch("main");
        let mut changes = Changes::new();
        replica.assert(&mut changes);
        target.clone().assert(&mut changes);
        pull(&local.branch("feature"), &target).assert(&mut changes);
        apply(
            &registry(&repo.subject(), &operator).await?,
            changes,
            &operator,
        )
        .await?;

        resolve(&feature, &operator).await?;
        let routes = feature.tracked().resolved.expect("resolved").pulls;
        assert!(
            matches!(routes.as_slice(), [Route::Unreachable { target: entity, .. }] if *entity == target.this),
            "{routes:?}"
        );

        let pulled = feature.pull().perform(&operator).await;
        assert!(
            matches!(pulled, Err(PullError::Unreachable { .. })),
            "{pulled:?}"
        );
        Ok(())
    }
}
