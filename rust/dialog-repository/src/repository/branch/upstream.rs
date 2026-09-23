//! What a branch pulls from and pushes to.
//!
//! Which branches those are is recorded as `dialog.branch/pull` and
//! `dialog.branch/push` facts in the registry, and resolved into
//! [`Upstream`]s when the branch is opened. The one thing kept per
//! branch is how far it has synced with each: the tree it was last in
//! sync at, the base the next merge with that upstream runs from, in the
//! branch's `sync` cell.

use crate::RemoteFallback;
use crate::{Branch, RemoteBranch, RemoteRepository, Revision, SiteAddress, TreeReference};
use dialog_artifacts::Entity;
use dialog_capability::{Did, Subject};
use serde::{Deserialize, Serialize};

pub(crate) mod legacy;

/// Which branch an upstream is, as the sync record keys it: a branch on
/// this replica by name, a branch at a peer by entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    /// A branch on this replica.
    Local(String),
    /// A branch on a peer's replica.
    Remote(Entity),
}

/// An upstream a branch pulls from or pushes to, with the tree the branch
/// was last in sync with it at.
#[derive(Debug, Clone)]
pub enum Upstream {
    /// A branch on this replica.
    Local {
        /// The branch name.
        branch: String,
        /// The tree at the last sync.
        tree: TreeReference,
    },
    /// A branch of a repository held at a peer.
    Remote {
        /// The repository, at the peer.
        remote: RemoteRepository,
        /// The branch name there.
        branch: String,
        /// The tree at the last sync.
        tree: TreeReference,
    },
    /// A branch whose peer could not be resolved when the branch was
    /// opened. Syncing with it fails, saying why; reading around it does
    /// not.
    Unreachable {
        /// The branch entity.
        target: Entity,
        /// Why it could not be resolved.
        reason: String,
        /// The tree at the last sync.
        tree: TreeReference,
    },
}

impl Upstream {
    /// The tree at the last sync with this upstream.
    pub fn tree(&self) -> &TreeReference {
        match self {
            Self::Local { tree, .. }
            | Self::Remote { tree, .. }
            | Self::Unreachable { tree, .. } => tree,
        }
    }

    /// This upstream, last in sync at `tree`.
    pub fn with_tree(self, tree: TreeReference) -> Self {
        match self {
            Self::Local { branch, .. } => Self::Local { branch, tree },
            Self::Remote { remote, branch, .. } => Self::Remote {
                remote,
                branch,
                tree,
            },
            Self::Unreachable { target, reason, .. } => Self::Unreachable {
                target,
                reason,
                tree,
            },
        }
    }

    /// Which branch this is, as the sync record keys it.
    pub fn target(&self) -> Target {
        match self {
            Self::Local { branch, .. } => Target::Local(branch.clone()),
            Self::Remote { remote, branch, .. } => {
                Target::Remote(remote.replica().branch(branch.as_str()).this)
            }
            Self::Unreachable { target, .. } => Target::Remote(target.clone()),
        }
    }

    /// Where this upstream lives, as a route to record.
    pub(crate) fn route(&self) -> Route {
        match self {
            Self::Local { branch, .. } => Route::Local {
                branch: branch.clone(),
            },
            Self::Remote { remote, branch, .. } => Route::Remote {
                peer: remote.peer().clone(),
                name: remote.label(),
                addresses: remote.addresses().to_vec(),
                subject: remote.did(),
                branch: branch.clone(),
            },
            Self::Unreachable { target, reason, .. } => Route::Unreachable {
                target: target.clone(),
                reason: reason.clone(),
            },
        }
    }

    /// Whether two upstreams are the same branch, whatever their sync
    /// state.
    pub fn same_target(&self, other: &Upstream) -> bool {
        self.target() == other.target()
    }

    /// Where a read this upstream holds by reference falls back to.
    pub(crate) fn fallback(&self) -> RemoteFallback {
        match self {
            Self::Local { .. } => RemoteFallback::None,
            Self::Remote { remote, .. } => RemoteFallback::Remote(remote.clone()),
            Self::Unreachable { target, reason, .. } => RemoteFallback::Unavailable {
                remote: target.to_string(),
                reason: reason.clone(),
            },
        }
    }
}

/// A set of upstreams.
#[derive(Debug, Clone, Default)]
pub struct Upstreams(Vec<Upstream>);

impl Upstreams {
    /// The entry for the same branch as `target`, if present.
    pub fn find(&self, target: &Upstream) -> Option<&Upstream> {
        self.0.iter().find(|entry| entry.same_target(target))
    }

    /// Iterate over the upstreams.
    pub fn iter(&self) -> impl Iterator<Item = &Upstream> {
        self.0.iter()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Add `upstream` unless the same branch is already here.
    pub(crate) fn insert(&mut self, upstream: Upstream) {
        if self.find(&upstream).is_none() {
            self.0.push(upstream);
        }
    }

    /// Where reads held by reference fall back to: the first upstream
    /// at a peer, reachable or not.
    pub(crate) fn fallback(&self) -> RemoteFallback {
        self.0
            .iter()
            .map(Upstream::fallback)
            .find(|fallback| !matches!(fallback, RemoteFallback::None))
            .unwrap_or(RemoteFallback::None)
    }
}

impl FromIterator<Upstream> for Upstreams {
    fn from_iter<I: IntoIterator<Item = Upstream>>(iter: I) -> Self {
        let mut upstreams = Self::default();
        for upstream in iter {
            upstreams.insert(upstream);
        }
        upstreams
    }
}

/// What a branch's `tracking` cell holds: its upstreams as last resolved
/// from the registry, and how far it has synced with each.
///
/// The routes are a cache of the registry's `dialog.branch/pull` and
/// `dialog.branch/push` facts, resolved to where each upstream lives. A
/// sync that finds the registry moved since they were resolved resolves
/// them again. Reads that fall back to an upstream use them as they
/// stand, so a branch opened cold still knows where the content it holds
/// by reference lives.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Tracking {
    /// The routes, and the registry revision they were resolved at;
    /// `None` until first resolved.
    pub resolved: Option<Resolved>,
    /// The tree each upstream was last in sync at.
    pub synced: Vec<Synced>,
}

/// A branch's routes, resolved from the registry at a revision.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resolved {
    /// The registry revision they were resolved at.
    pub at: Option<Revision>,
    /// Where each branch pulled from lives.
    pub pulls: Vec<Route>,
    /// Where each branch pushed to lives.
    pub pushes: Vec<Route>,
}

/// Where an upstream lives, as resolved from the registry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Route {
    /// A branch on this replica.
    Local {
        /// The branch name.
        branch: String,
    },
    /// A branch of a repository held at a peer.
    Remote {
        /// The peer.
        peer: Entity,
        /// The peer's local name, if it has one.
        name: Option<String>,
        /// Where the peer is reached.
        addresses: Vec<SiteAddress>,
        /// The repository, at the peer.
        subject: Did,
        /// The branch name there.
        branch: String,
    },
    /// A branch whose peer could not be resolved.
    Unreachable {
        /// The branch entity.
        target: Entity,
        /// Why it could not be resolved.
        reason: String,
    },
}

impl Route {
    /// The upstream this route leads to, last in sync at `tree`, with a
    /// remote's state cached under `host`.
    pub(crate) fn upstream(&self, host: &Subject, tree: TreeReference) -> Upstream {
        match self {
            Self::Local { branch } => Upstream::Local {
                branch: branch.clone(),
                tree,
            },
            Self::Remote {
                peer,
                name,
                addresses,
                subject,
                branch,
            } => Upstream::Remote {
                remote: RemoteRepository::new(
                    host.clone(),
                    peer.clone(),
                    name.clone(),
                    addresses.clone(),
                    subject.clone(),
                ),
                branch: branch.clone(),
                tree,
            },
            Self::Unreachable { target, reason } => Upstream::Unreachable {
                target: target.clone(),
                reason: reason.clone(),
                tree,
            },
        }
    }
}

/// One upstream's sync state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Synced {
    /// The upstream.
    pub target: Target,
    /// The tree at the last sync.
    pub tree: TreeReference,
    /// Where it lives, as it was reached. Kept so the branch knows where
    /// content it adopted came from even when it synced with a branch
    /// once, without tracking it: pushing that content on needs to know
    /// which peers can serve it.
    pub route: Route,
}

impl Tracking {
    /// The tree recorded for `target`, if it has been synced.
    pub fn get(&self, target: &Target) -> Option<&TreeReference> {
        self.synced
            .iter()
            .find(|synced| synced.target == *target)
            .map(|synced| &synced.tree)
    }

    /// The tree to merge with `target` from: the one recorded, or the
    /// empty tree.
    pub fn tree(&self, target: &Target) -> TreeReference {
        self.get(target).cloned().unwrap_or_default()
    }

    /// Record that `upstream` was last in sync at its tree.
    pub fn record(&mut self, upstream: &Upstream) {
        let target = upstream.target();
        let tree = upstream.tree().clone();
        let route = upstream.route();
        match self
            .synced
            .iter_mut()
            .find(|synced| synced.target == target)
        {
            Some(synced) => {
                synced.tree = tree;
                synced.route = route;
            }
            None => self.synced.push(Synced {
                target,
                tree,
                route,
            }),
        }
    }

    /// Every branch this one has synced with, tracked or not: where the
    /// content it holds came from.
    pub(crate) fn synced_with(&self, host: &Subject) -> Upstreams {
        self.synced
            .iter()
            .map(|synced| synced.route.upstream(host, synced.tree.clone()))
            .collect()
    }

    /// The upstreams pulled from, each with its sync state.
    pub(crate) fn pulls(&self, host: &Subject) -> Upstreams {
        self.upstreams_of(host, |resolved| &resolved.pulls)
    }

    /// The upstreams pushed to, each with its sync state.
    pub(crate) fn pushes(&self, host: &Subject) -> Upstreams {
        self.upstreams_of(host, |resolved| &resolved.pushes)
    }

    fn upstreams_of(&self, host: &Subject, routes: impl Fn(&Resolved) -> &Vec<Route>) -> Upstreams {
        let Some(resolved) = self.resolved.as_ref() else {
            return Upstreams::default();
        };
        routes(resolved)
            .iter()
            .map(|route| {
                let upstream = route.upstream(host, TreeReference::default());
                let tree = self.tree(&upstream.target());
                upstream.with_tree(tree)
            })
            .collect()
    }
}

/// The input shape for [`Branch::set_upstream`](super::Branch::set_upstream)
/// and the one-off targets of a pull or push.
///
/// Wraps a local branch or a branch at a peer. Convertible into an
/// [`Upstream`] never synced with, whose merge runs from the empty tree.
pub enum UpstreamBranch {
    /// A local branch. Both variants are boxed: the handles are large
    /// and this enum is a short-lived constructor argument.
    Local(Box<Branch>),
    /// A branch at a peer.
    Remote(Box<RemoteBranch>),
}

impl From<&Branch> for UpstreamBranch {
    fn from(branch: &Branch) -> Self {
        UpstreamBranch::Local(Box::new(branch.clone()))
    }
}

impl From<Branch> for UpstreamBranch {
    fn from(branch: Branch) -> Self {
        UpstreamBranch::Local(Box::new(branch))
    }
}

impl From<&RemoteBranch> for UpstreamBranch {
    fn from(branch: &RemoteBranch) -> Self {
        UpstreamBranch::Remote(Box::new(branch.clone()))
    }
}

impl From<RemoteBranch> for UpstreamBranch {
    fn from(branch: RemoteBranch) -> Self {
        UpstreamBranch::Remote(Box::new(branch))
    }
}

impl From<UpstreamBranch> for Upstream {
    fn from(source: UpstreamBranch) -> Self {
        match source {
            UpstreamBranch::Local(branch) => Upstream::Local {
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
            UpstreamBranch::Remote(branch) => Upstream::Remote {
                remote: branch.repository().clone(),
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{Tracking, Upstream};
    use crate::TreeReference;

    /// The sync record keeps one tree per upstream, replacing it in
    /// place, and answers the empty tree for an upstream never synced.
    #[dialog_common::test]
    fn it_records_one_tree_per_upstream() {
        let develop = |tree| Upstream::Local {
            branch: "develop".into(),
            tree: TreeReference::from(tree),
        };
        let main = |tree| Upstream::Local {
            branch: "main".into(),
            tree: TreeReference::from(tree),
        };
        let mut syncs = Tracking::default();

        assert_eq!(
            syncs.tree(&main([0; 32]).target()),
            TreeReference::default()
        );
        syncs.record(&main([1; 32]));
        syncs.record(&develop([2; 32]));
        syncs.record(&main([3; 32]));

        assert_eq!(syncs.synced.len(), 2);
        assert_eq!(
            syncs.get(&main([0; 32]).target()),
            Some(&TreeReference::from([3; 32]))
        );
        assert_eq!(
            syncs.get(&develop([0; 32]).target()),
            Some(&TreeReference::from([2; 32]))
        );
    }
}
