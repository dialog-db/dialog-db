//! Resolving where a branch's upstreams live.
//!
//! Which branches a branch pulls from and pushes to is recorded in the
//! registry. Resolving them, to a local branch by name or to a replica at
//! a peer, takes one query per direction through the built-in upstream
//! rules. Where a peer is reached is the host's business: its contacts,
//! found by connecting to the peer. The result is cached in the branch's
//! tracking cell, stamped with the registry revision it was resolved at,
//! and a remote route keeps the addresses its peer was reached at then,
//! so a branch opened cold can still read what it holds by reference. A
//! sync re-resolves when the registry has moved since, or when a peer is
//! now reached somewhere else.

use dialog_artifacts::Entity;
use dialog_capability::Provider;
use dialog_capability::{Did, Subject};
use dialog_effects::MethodExt as _;
use dialog_effects::authority::{Identify, OperatorExt as _};
use dialog_effects::peer::prelude::*;
use dialog_effects::peer::{Connect, PeerError};
use dialog_query::{Output as _, Query, Term};

use crate::registry::RegistryEnv;
use crate::schema::{BranchPull, BranchPush, PullUpstream, PushUpstream, Replica};
use crate::{
    Branch, REGISTRY, RepositoryMemoryExt as _, ResolveUpstreamsError, Resolved, Route,
    SiteAddress, site_address,
};

/// The environment resolving runs against: the registry, and the host's
/// connections to peers.
pub trait ResolveEnv: RegistryEnv + Provider<Connect> {}

impl<T: RegistryEnv + Provider<Connect>> ResolveEnv for T {}

/// Bring `branch`'s cached routes up to date with the registry: a no-op
/// while the registry has not moved since they were resolved.
pub(crate) async fn resolve<Env: ResolveEnv>(
    branch: &Branch,
    env: &Env,
) -> Result<(), ResolveUpstreamsError> {
    if branch.name() == REGISTRY {
        return Ok(());
    }
    let registry = branch.subject().registry().open().perform(env).await?;
    let at = registry.revision();
    let operator = Identify.perform(env).await?;
    let host = Subject::from(operator.profile().clone());
    if let Some(resolved) = &branch.tracked().resolved
        && resolved.at == at
        && current(branch, resolved, &host, env).await?
    {
        return Ok(());
    }

    let local = Replica::new(operator.profile().clone(), branch.of().clone());
    let this = local.branch(branch.name()).this;
    let resolved = Resolved {
        at,
        pulls: pulls(branch, &registry, &host, &local, &this, env).await?,
        pushes: pushes(branch, &registry, &host, &local, &this, env).await?,
    };

    // Other syncs write this cell too, recording how far they got. On a
    // conflict, re-read it and lay the routes over what is there now,
    // until they land: every caller reads the routes from the cell after
    // this, so answering before they are recorded would have it act on
    // whatever the last writer left -- routes resolved before the
    // registry moved. A cell that keeps moving is given up on, saying so.
    let cell = branch.tracking();
    for _ in 0..ATTEMPTS {
        let mut tracking = branch.tracked();
        // What the registry says at one revision may still resolve to
        // different routes, when the host's contacts moved: only the
        // same routes already recorded are taken as landed.
        if tracking.resolved.as_ref() == Some(&resolved) {
            return Ok(());
        }
        tracking.resolved = Some(resolved.clone());
        match cell.checkpoint().publish(tracking, env).await {
            Ok(()) => return Ok(()),
            Err(crate::PublishError::VersionMismatch { .. }) => {
                cell.resolve().perform(env).await?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ResolveUpstreamsError::Contended {
        branch: branch.name().to_string(),
        attempts: ATTEMPTS,
    })
}

/// How many times the routes are laid over a tracking cell that moved
/// before resolving gives up on it.
const ATTEMPTS: usize = 8;

/// Where each branch `this` pulls from lives.
async fn pulls<Env: ResolveEnv>(
    branch: &Branch,
    registry: &Branch,
    host: &Subject,
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
    routes(branch, host, local, targets, found, env).await
}

/// Where each branch `this` pushes to lives.
async fn pushes<Env: ResolveEnv>(
    branch: &Branch,
    registry: &Branch,
    host: &Subject,
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
    routes(branch, host, local, targets, found, env).await
}

/// Whether the remote routes in `resolved` still lead where their peers
/// are reached. One that no longer does, and one that could not be
/// reached before, may be reachable at a new address now.
async fn current<Env: ResolveEnv>(
    branch: &Branch,
    resolved: &Resolved,
    host: &Subject,
    env: &Env,
) -> Result<bool, ResolveUpstreamsError> {
    for route in resolved.pulls.iter().chain(resolved.pushes.iter()) {
        match route {
            Route::Local { .. } => {}
            Route::Unreachable { .. } => return Ok(false),
            Route::Remote {
                peer, addresses, ..
            } => match reach(branch, host, peer, env).await? {
                Some(now) if now == *addresses => {}
                _ => return Ok(false),
            },
        }
    }
    Ok(true)
}

/// A route for each of `targets`: local when the rule placed it on this
/// replica, at its peer's addresses when on another's, and unreachable
/// when the rule could not place it or the host cannot reach its peer --
/// so a relation never silently drops out.
async fn routes<Env: ResolveEnv>(
    source: &Branch,
    host: &Subject,
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
        let Some(addresses) = reach(source, host, peer, env).await? else {
            routes.push(Route::Unreachable {
                target,
                reason: format!("no address is known for peer {peer}"),
            });
            continue;
        };
        routes.push(Route::Remote {
            peer: peer.clone(),
            name: None,
            addresses,
            subject,
            branch: branch.clone(),
        });
    }
    Ok(routes)
}

/// Where the host reaches `peer`, or `None` when it has no address for it.
/// `branch` keeps the connection's record of which address answered, so
/// its remotes fail over as the connection does.
async fn reach<Env: ResolveEnv>(
    branch: &Branch,
    host: &Subject,
    peer: &Entity,
    env: &Env,
) -> Result<Option<Vec<SiteAddress>>, ResolveUpstreamsError> {
    let connection = match host
        .clone()
        .reader()
        .peers()
        .connect(peer.clone())
        .perform(env)
        .await
    {
        Ok(connection) => connection,
        Err(PeerError::Unreachable { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    branch.connected(peer.clone(), connection.answers());
    Ok(Some(
        connection
            .addresses()
            .iter()
            .map(site_address)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{ResolveEnv, resolve};
    use crate::helpers::{connect, flaky_session_with_peer, test_repo};
    use crate::registry::{apply, pull};
    use crate::schema::Replica;
    use crate::{
        Branch, PullError, Repository, RepositoryMemoryExt as _, ResolveUpstreamsError, Route,
        SiteAddress, Upstream, contact,
    };
    use anyhow::Result;
    use dialog_artifacts::Changes;
    use dialog_credentials::Credential;
    use dialog_peer::PeerSpace;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::Statement as _;
    use dialog_remote_ucan::UcanAddress;
    use dialog_varsig::did;

    /// Routes that resolve to `main` and `dev` on this replica: the
    /// second is recorded straight into the registry, so the branch's
    /// tracking cell still holds the routes from when only `main` was.
    async fn two_upstreams_one_recorded<S: PeerSpace, Env>(
        repo: &Repository<Credential>,
        peer: &dialog_peer::Peer<S>,
        env: &Env,
    ) -> Result<Branch>
    where
        Env: ResolveEnv,
    {
        let main = repo.branch("main").open().perform(env).await?;
        let feature = repo.branch("feature").open().perform(env).await?;
        feature.pull_from(&main).perform(env).await?;
        assert_eq!(feature.pulls().iter().count(), 1);

        let local = Replica::new(peer.did(), repo.did());
        let dev = local.branch("dev");
        let mut changes = Changes::new();
        dev.clone().assert(&mut changes);
        pull(&local.branch("feature"), &dev).assert(&mut changes);
        apply(
            &repo.subject().registry().open().perform(env).await?,
            changes,
            env,
        )
        .await?;
        Ok(feature)
    }

    /// Other syncs write the tracking cell too. Losing it to them,
    /// however many times running, does not leave the routes
    /// unrecorded: resolve lays them over what is there until they
    /// land, and only then answers -- a caller reading the routes after
    /// a resolve that answered without recording them would act on
    /// whatever the last writer left, routes resolved before the
    /// registry moved.
    #[dialog_common::test]
    async fn it_records_its_routes_however_often_the_cell_moves() -> Result<()> {
        let (operator, profile) = flaky_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let feature = two_upstreams_one_recorded(&repo, &profile, &operator).await?;

        let memory = profile
            .storage()
            .space(&repo.did())
            .expect("mounted")
            .memory;
        memory.lose_next_publishes("branch/feature", "tracking", 3);
        resolve(&feature, &operator).await?;

        let registry = repo.subject().registry().open().perform(&operator).await?;
        let resolved = feature.tracked().resolved.expect("resolved");
        assert_eq!(
            resolved.at,
            registry.revision(),
            "the routes were resolved at the registry's head"
        );
        assert_eq!(resolved.pulls.len(), 2, "{:?}", resolved.pulls);
        assert_eq!(memory.publishes("branch/feature", "tracking"), 4);

        let reopened = repo.branch("feature").open().perform(&operator).await?;
        assert_eq!(
            reopened.tracked().resolved,
            Some(resolved),
            "the routes were recorded, not just cached"
        );
        Ok(())
    }

    /// A cell that never stops moving is given up on, saying so, rather
    /// than answering with the routes unrecorded.
    #[dialog_common::test]
    async fn it_gives_up_on_a_cell_that_never_stops_moving() -> Result<()> {
        let (operator, profile) = flaky_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let feature = two_upstreams_one_recorded(&repo, &profile, &operator).await?;

        let memory = profile
            .storage()
            .space(&repo.did())
            .expect("mounted")
            .memory;
        memory.lose_publishes("branch/feature", "tracking", 0..);
        let resolved = resolve(&feature, &operator).await;
        assert!(
            matches!(resolved, Err(ResolveUpstreamsError::Contended { .. })),
            "{resolved:?}"
        );
        Ok(())
    }

    /// Routes are cached against the registry revision they were resolved
    /// at: an upstream recorded through another handle is picked up the
    /// next time this one resolves, because the registry moved.
    #[dialog_common::test]
    async fn it_resolves_again_when_the_registry_moves() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
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
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let feature = repo.branch("feature").open().perform(&operator).await?;

        // A pull relation to a branch on a peer with no address.
        let local = Replica::new(profile.did(), repo.did());
        let replica = Replica::new(did!("web:nowhere.example"), repo.did());
        let target = replica.branch("main");
        let mut changes = Changes::new();
        replica.assert(&mut changes);
        target.clone().assert(&mut changes);
        pull(&local.branch("feature"), &target).assert(&mut changes);
        apply(
            &repo.subject().registry().open().perform(&operator).await?,
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

    /// Where a peer is reached is the host's, not the registry's: a
    /// peer given a new address is picked up the next time the branch
    /// resolves, though the registry has not moved.
    #[dialog_common::test]
    async fn it_resolves_again_when_a_peer_is_reached_elsewhere() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let first = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let second = SiteAddress::from(UcanAddress::new("https://backup.tonk.network/ucan/"));

        let origin = connect("origin", first.clone(), repo.did(), &operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.set_upstream(&remote).perform(&operator).await?;
        let reached = |branch: &crate::Branch| match branch.tracked().resolved {
            Some(resolved) => match resolved.pulls.as_slice() {
                [Route::Remote { addresses, .. }] => addresses.clone(),
                routes => panic!("one remote route, got {routes:?}"),
            },
            None => panic!("resolved"),
        };
        assert_eq!(reached(&branch), vec![first.clone()]);

        let at = repo
            .subject()
            .registry()
            .open()
            .perform(&operator)
            .await?
            .revision();
        contact(origin.peer().clone())
            .add_address(second.clone())
            .perform(&operator)
            .await?;
        resolve(&branch, &operator).await?;
        let mut expected = vec![first, second];
        let mut addresses = reached(&branch);
        expected.sort_by_key(|site| format!("{site:?}"));
        addresses.sort_by_key(|site| format!("{site:?}"));
        assert_eq!(addresses, expected);
        assert_eq!(
            repo.subject()
                .registry()
                .open()
                .perform(&operator)
                .await?
                .revision(),
            at,
            "the registry did not move"
        );
        Ok(())
    }
}
