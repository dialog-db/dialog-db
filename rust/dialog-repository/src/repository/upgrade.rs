//! Upgrading a repository's local storage to the current layout.
//!
//! What a release keeps locally, and where, changes over time. A
//! repository records the layout version its storage is at in one cell,
//! `dialog/version`, which no upgrade ever moves: every release can find
//! it before it knows anything else about the layout. Storage without
//! the cell is at version 0, the layout from before versioning.
//!
//! Opening or loading a repository upgrades it, and creating one records
//! the current version, so storage is never read in an older layout.
//! [`Repository::upgrade`] runs every step from the recorded version up
//! to [`VERSION`] and records the new version last. A step asserts the same facts however often it runs, so an
//! upgrade interrupted before it records its version simply runs again.
//!
//! # Steps
//!
//! - **0 → 1**: remotes and upstreams move from cells into facts. Each
//!   remote becomes a contact of the host doing the upgrade, reached at
//!   the address its cell holds and named as the remote was; each
//!   upstream becomes a pull and a push relation in the
//!   [`REGISTRY`](crate::REGISTRY) to the branch it tracked. The cells,
//!   and `credential/key/self`, are left in place for one version and
//!   removed by the next.

use dialog_artifacts::Changes;
use dialog_capability::{Capability, Did, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::MethodExt as _;
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::prelude::{
    ListSpaceExt as _, MemoryExt as _, SpaceExt as _, SpaceScope,
};
use dialog_effects::memory::{List, Publish};
use dialog_query::Statement as _;
use dialog_varsig::Principal;

use super::branch::upstream::legacy;
use super::peer::host;
use crate::registry::{RegistryEnv, apply, pull, push};
use crate::schema::{DidExt as _, Replica};
use crate::{
    AddAddressError, Branch, Cell, PeersEnv, PublishError, RemoteAddress, RemoteEdition,
    Repository, RepositoryMemoryExt as _, Resolved, Route, SiteAddress, Tracking, UpgradeError,
    contact, peer_did,
};
use dialog_artifacts::Entity;
use dialog_effects::peer::prelude::*;

/// The layout version this release stores in.
pub const VERSION: u32 = 1;

/// The space and cell holding the layout version. Fixed for good: this
/// is what every release reads first.
const SPACE: &str = "dialog";
const CELL: &str = "version";

/// What an upgrade did: the version the storage was at, and the one it
/// is at now. Equal when there was nothing to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Upgraded {
    /// The version the storage was at.
    pub from: u32,
    /// The version the storage is at now.
    pub to: u32,
}

impl<C: Principal> Repository<C> {
    /// Upgrade this repository's local storage to the current layout.
    pub fn upgrade(&self) -> Upgrade {
        Upgrade {
            subject: self.subject(),
        }
    }
}

/// Command to upgrade a repository's local storage. Created by
/// [`Repository::upgrade`].
pub struct Upgrade {
    subject: Subject,
}

impl Upgrade {
    /// Run every step from the recorded version to [`VERSION`], then
    /// record it.
    ///
    /// The version is published against the one just read, so of two
    /// upgrades racing, the second is refused rather than recording over
    /// the first.
    pub async fn perform<Env>(self, env: &Env) -> Result<Upgraded, UpgradeError>
    where
        Env: RegistryEnv + PeersEnv + Provider<List>,
    {
        let cell: Cell<u32> = SpaceScope::new(self.subject.clone(), SPACE)
            .cell(CELL)
            .into();
        cell.resolve().perform(env).await?;
        let from = cell.content().unwrap_or(0);

        if from > VERSION {
            return Err(UpgradeError::Newer {
                found: from,
                supported: VERSION,
            });
        }
        if from == VERSION {
            return Ok(Upgraded { from, to: from });
        }

        let operator = Identify.perform(env).await?;
        let registry = self.subject.registry().open().perform(env).await?;

        if from < 1 {
            carry_over(&self.subject, &registry, &operator, env).await?;
        }

        cell.publish(VERSION).perform(env).await?;
        Ok(Upgraded { from, to: VERSION })
    }
}

/// Record that storage this release just created is at [`VERSION`], so
/// no upgrade ever runs over it.
pub(crate) async fn stamp<Env>(subject: &Subject, env: &Env) -> Result<(), PublishError>
where
    Env: Provider<Publish> + ConditionalSync,
{
    let cell: Cell<u32> = SpaceScope::new(subject.clone(), SPACE).cell(CELL).into();
    cell.publish(VERSION).perform(env).await
}

/// Step 0 → 1: carry remotes and upstreams out of their cells into
/// facts, found by listing what is stored under `remote/` and `branch/`.
///
/// Each remote becomes a peer. Each upstream becomes pull and push
/// relations to the branch it tracked, with that branch and its replica
/// recorded so the relations resolve. The remote head a branch cached
/// moves to its entity-keyed cell, and each branch's tracking cell is
/// written in full: its routes, resolved at the registry revision this
/// step leaves, and the tree it was last in sync with each upstream at.
async fn carry_over<Env>(
    subject: &Subject,
    registry: &Branch,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<(), UpgradeError>
where
    Env: RegistryEnv + PeersEnv + Provider<List>,
{
    let local = Replica::new(operator.profile().clone(), registry.of().clone());
    let remotes = stored(subject, "remote", "/address", env).await?;
    let branches = stored(subject, "branch", "/upstream", env).await?;

    let mut peers: Vec<Carried> = Vec::new();
    let mut changes = Changes::new();
    let mut tracked: Vec<(String, Vec<Route>, Tracking)> = Vec::new();

    for name in &branches {
        let cell = subject.branch(name.as_str()).legacy_upstream();
        cell.resolve()
            .perform(env)
            .await
            .map_err(|source| UpgradeError::Upstream {
                name: name.clone(),
                source,
            })?;
        let tracking = local.branch(name.as_str());
        let mut routes = Vec::new();
        let mut state = Tracking::default();

        for upstream in cell.content().unwrap_or_default().iter() {
            let (target, route) = match upstream {
                legacy::Upstream::Local { branch, .. } => {
                    let target = local.branch(branch.as_str());
                    target.clone().assert(&mut changes);
                    (
                        target.clone(),
                        Route::Local {
                            branch: branch.clone(),
                        },
                    )
                }
                legacy::Upstream::Remote { remote, branch, .. } => {
                    // An upstream naming a remote with no address was
                    // already unreachable; there is no peer to carry it to.
                    let Some(address) = load(subject, remote, env).await? else {
                        continue;
                    };
                    let carried = carry(&mut peers, remote, address)?;
                    let replica = Replica::new(carried.peer.clone(), carried.subject.clone());
                    let target = replica.branch(branch.as_str());
                    replica.assert(&mut changes);
                    target.clone().assert(&mut changes);
                    rehome(subject, remote, branch, &target.this, env).await?;
                    (
                        target.clone(),
                        Route::Remote {
                            peer: carried.peer.this(),
                            name: Some(remote.clone()),
                            addresses: vec![carried.site.clone()],
                            subject: carried.subject.clone(),
                            branch: branch.clone(),
                        },
                    )
                }
            };
            pull(&tracking, &target).assert(&mut changes);
            push(&tracking, &target).assert(&mut changes);
            state.record(&route.upstream(subject, upstream.tree().clone()));
            routes.push(route);
        }
        tracked.push((name.clone(), routes, state));
    }

    for name in &remotes {
        if let Some(address) = load(subject, name, env).await? {
            carry(&mut peers, name, address)?;
        }
    }

    // Contacts are the host's, and every repository's remote is usually
    // named origin: a name another peer already has is not given again,
    // so looking a peer up by it stays unambiguous.
    for carried in peers {
        let entity = carried.peer.this();
        let known = host(env)
            .await?
            .reader()
            .peers()
            .find(carried.name.clone())
            .perform(env)
            .await
            .map_err(AddAddressError::from)?;
        let contact = contact(&carried.peer).add_address(carried.site);
        let contact = if known.iter().all(|peer| *peer == entity) {
            contact.name(carried.name)
        } else {
            contact
        };
        contact.perform(env).await?;
    }

    apply(registry, changes, env).await?;

    for (name, routes, mut state) in tracked {
        state.resolved = Some(Resolved {
            at: registry.revision(),
            pulls: routes.clone(),
            pushes: routes,
        });
        let cell = subject.branch(name.as_str()).tracking();
        cell.resolve().perform(env).await?;
        cell.publish(state).perform(env).await?;
    }
    Ok(())
}

/// Move the head a branch cached for `branch` on the remote `remote`
/// to the cell keyed by that branch's entity, unless one is there.
async fn rehome<Env: RegistryEnv>(
    subject: &Subject,
    remote: &str,
    branch: &str,
    entity: &Entity,
    env: &Env,
) -> Result<(), UpgradeError> {
    let from: Cell<RemoteEdition> = SpaceScope::new(subject.clone(), format!("remote/{remote}"))
        .cell(format!("branch/{branch}/revision"))
        .into();
    from.resolve().perform(env).await?;
    let Some(edition) = from.content() else {
        return Ok(());
    };
    let to: Cell<RemoteEdition> = SpaceScope::new(subject.clone(), format!("upstream/{entity}"))
        .cell("revision")
        .into();
    to.resolve().perform(env).await?;
    if to.content().is_none() {
        to.publish(edition).perform(env).await?;
    }
    Ok(())
}

/// The names under `space` that hold a cell ending in `suffix`: the
/// remotes with an `address`, the branches with an `upstream`.
async fn stored<Env>(
    subject: &Subject,
    space: &str,
    suffix: &str,
    env: &Env,
) -> Result<Vec<String>, UpgradeError>
where
    Env: Provider<List>,
{
    Ok(subject
        .clone()
        .reader()
        .memory()
        .space(space)
        .list()
        .perform(env)
        .await?
        .into_iter()
        .filter_map(|path| path.strip_suffix(suffix).map(str::to_string))
        .collect())
}

/// A remote being carried over: the peer it becomes, the address that
/// peer is reached at, and the subject the remote holds a replica of.
#[derive(Clone)]
struct Carried {
    name: String,
    peer: Did,
    site: SiteAddress,
    subject: Did,
}

/// Carry the remote `name` over once, however many upstreams name it.
fn carry(
    peers: &mut Vec<Carried>,
    name: &str,
    address: RemoteAddress,
) -> Result<Carried, UpgradeError> {
    if let Some(carried) = peers.iter().find(|carried| carried.name == name) {
        return Ok(carried.clone());
    }
    let carried = Carried {
        name: name.to_string(),
        peer: peer_did(&address.address)?,
        site: address.address,
        subject: address.subject,
    };
    peers.push(carried.clone());
    Ok(carried)
}

/// The address a remote's legacy cell holds, if it has one.
async fn load<Env: RegistryEnv>(
    subject: &Subject,
    name: &str,
    env: &Env,
) -> Result<Option<RemoteAddress>, UpgradeError> {
    let cell: Cell<RemoteAddress> = SpaceScope::new(subject.clone(), format!("remote/{name}"))
        .cell("address")
        .into();
    cell.resolve()
        .perform(env)
        .await
        .map_err(|source| UpgradeError::Remote {
            name: name.to_string(),
            source,
        })?;
    Ok(cell.content())
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{CELL, SPACE, Upgraded, VERSION};
    use crate::Repository;
    use crate::RepositoryExt as _;
    use crate::helpers::test_repo;
    use crate::repository::branch::resolve::resolve;
    use crate::schema::{BranchPull, BranchPush, DidExt as _, Replica};
    use crate::{
        Cell, REGISTRY, RemoteAddress, RemoteEdition, RepositoryMemoryExt as _, Route, SiteAddress,
        Target, TreeReference, UpgradeError, site_address,
    };
    use dialog_artifacts::Instruction;
    use dialog_capability::Provider;
    use dialog_capability::Subject;
    use dialog_common::ConditionalSync;
    use dialog_credentials::Credential;
    use dialog_effects::MethodExt as _;
    use dialog_effects::memory::Version;
    use dialog_effects::memory::prelude::{CellScope, SpaceScope};
    use dialog_effects::memory::{Resolve, Retract};
    use dialog_effects::peer::prelude::*;
    use dialog_identity::SpaceHandle;
    use dialog_peer::helpers::{test_session_with_peer, unique_name};
    use dialog_query::{Output as _, Query, Term};
    use dialog_remote_ucan::UcanAddress;
    use dialog_varsig::did;
    use futures_util::stream;

    /// Take out the version `repo` recorded when it was created, leaving
    /// its storage as it was before versioning.
    async fn unversion<Env>(repo: &Repository<Credential>, env: &Env) -> anyhow::Result<()>
    where
        Env: Provider<Resolve> + Provider<Retract> + ConditionalSync,
    {
        let version: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        version.resolve().perform(env).await?;
        version.retract().perform(env).await?;
        Ok(())
    }

    /// Storage from before versioning is at version 0, and upgrading it
    /// carries the remotes and upstreams stored in cells over into facts.
    ///
    /// The cells are written from the bytes earlier releases wrote, not
    /// by encoding today's types, so the test keeps reading what is
    /// actually on disk even after those types change. Both upstream
    /// shapes are covered: the list of entries, and the single bare
    /// entry older releases wrote.
    #[dialog_common::test]
    async fn it_upgrades_remotes_and_upstreams_from_cells() -> anyhow::Result<()> {
        // `remote/origin/address`: a UCAN service at
        // https://tonk.network/ucan/ holding the repository below.
        const REMOTE: &str = "a26761646472657373a1645563616ea168656e64706f696e74781a68747470733a2f2f746f6e6b2e6e6574776f726b2f7563616e2f677375626a65637478386469643a6b65793a7a364d6b68615867425a44766f74446b4c353235376661697a74694769433251744b4c4770626e6e4547746132646f4b";
        // `branch/main/upstream`: `main` on origin, then local `develop`.
        const MANY: &str = "82a16652656d6f7465a3647472656598200707070707070707070707070707070707070707070707070707070707070707666272616e6368646d61696e6672656d6f7465666f726967696ea1654c6f63616ca2647472656598200000000000000000000000000000000000000000000000000000000000000000666272616e636867646576656c6f70";
        // `branch/draft/upstream`, in the older single-entry shape:
        // `draft` on origin.
        const ONE: &str = "a16652656d6f7465a3647472656598200909090909090909090909090909090909090909090909090909090909090909666272616e63686564726166746672656d6f7465666f726967696e";

        fn bytes(hex: &str) -> Vec<u8> {
            (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("valid hex"))
                .collect()
        }

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        unversion(&repo, &operator).await?;
        let held = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");

        for (space, cell, content) in [
            ("remote/origin", "address", REMOTE),
            ("branch/main", "upstream", MANY),
            ("branch/draft", "upstream", ONE),
        ] {
            CellScope::new(Subject::from(repo.did()), space, cell)
                .publish(bytes(content), None)
                .perform(&operator)
                .await?;
        }

        // The head `main` last saw on origin, cached by remote name.
        let scratch = repo.branch("scratch").open().perform(&operator).await?;
        let seen = scratch
            .commit(stream::iter(Vec::<Instruction>::new()))
            .allow_empty()
            .perform(&operator)
            .await?;
        let cached = RemoteEdition {
            content: seen.clone(),
            version: Version::from(b"seen".as_slice()),
        };
        let legacy: Cell<RemoteEdition> =
            SpaceScope::new(Subject::from(repo.did()), "remote/origin")
                .cell("branch/main/revision")
                .into();
        legacy.publish(cached.clone()).perform(&operator).await?;

        assert_eq!(
            repo.upgrade().perform(&operator).await?,
            Upgraded {
                from: 0,
                to: VERSION
            }
        );

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;

        // The remote is a contact of the host, named after it,
        // identified by its origin, reached at the address its cell held.
        let host = crate::host(&operator).await?;
        let named = host
            .clone()
            .reader()
            .peers()
            .find("origin")
            .perform(&operator)
            .await?;
        let origin = did!("web:tonk.network");
        assert_eq!(named, vec![origin.this()], "one remote, one contact");
        let connection = host
            .reader()
            .peers()
            .connect(origin.this())
            .perform(&operator)
            .await?;
        let sites = connection
            .addresses()
            .iter()
            .map(site_address)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            sites,
            vec![SiteAddress::from(UcanAddress::new(
                "https://tonk.network/ucan/"
            ))]
        );

        // Each upstream is pulled from and pushed to, by entity: on the
        // peer's replica of the repository it holds, or on this one.
        let local = Replica::new(profile.did(), repo.did());
        let remote = Replica::new(origin, held);
        let tracked = async |name: &str| -> anyhow::Result<(Vec<_>, Vec<_>)> {
            let this = local.branch(name).this;
            let mut pulls: Vec<_> = registry
                .query()
                .select(Query::<BranchPull> {
                    this: this.clone().into(),
                    pull: Term::var("pull"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|row: BranchPull| row.pull.0)
                .collect();
            let mut pushes: Vec<_> = registry
                .query()
                .select(Query::<BranchPush> {
                    this: this.into(),
                    push: Term::var("push"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|row: BranchPush| row.push.0)
                .collect();
            pulls.sort();
            pushes.sort();
            Ok((pulls, pushes))
        };

        let mut main = vec![remote.branch("main").this, local.branch("develop").this];
        main.sort();
        assert_eq!(tracked("main").await?, (main.clone(), main.clone()));
        let draft = vec![remote.branch("draft").this];
        assert_eq!(tracked("draft").await?, (draft.clone(), draft));

        // Each branch's tracking cell holds its routes and the trees its
        // cells recorded, so it knows where its upstreams live, and how far
        // it synced with each, before it next pulls.
        let origin_main = remote.branch("main").this;
        let main_tracking = repo
            .branch("main")
            .open()
            .perform(&operator)
            .await?
            .tracked();
        let routes = main_tracking
            .resolved
            .clone()
            .expect("routes resolved")
            .pulls;
        assert!(
            routes.iter().any(|route| matches!(
                route,
                Route::Remote { name, branch, .. } if name.as_deref() == Some("origin") && branch == "main"
            )),
            "{routes:?}"
        );
        assert!(
            routes.contains(&Route::Local {
                branch: "develop".into()
            }),
            "{routes:?}"
        );
        assert_eq!(
            main_tracking.get(&Target::Remote(origin_main.clone())),
            Some(&TreeReference::from([7; 32]))
        );
        assert_eq!(
            main_tracking.get(&Target::Local("develop".into())),
            Some(&TreeReference::default())
        );
        let draft_tracking = repo
            .branch("draft")
            .open()
            .perform(&operator)
            .await?
            .tracked();
        assert_eq!(
            draft_tracking.get(&Target::Remote(remote.branch("draft").this)),
            Some(&TreeReference::from([9; 32]))
        );

        // The head cached by remote name moved to the cell keyed by the
        // remote branch's entity.
        let rehomed: Cell<RemoteEdition> =
            SpaceScope::new(Subject::from(repo.did()), format!("upstream/{origin_main}"))
                .cell("revision")
                .into();
        rehomed.resolve().perform(&operator).await?;
        assert_eq!(rehomed.content(), Some(cached));

        // Upgrading again finds nothing to do.
        assert_eq!(
            repo.upgrade().perform(&operator).await?,
            Upgraded {
                from: VERSION,
                to: VERSION
            }
        );
        assert_eq!(tracked("main").await?, (main.clone(), main));

        Ok(())
    }

    /// A repository that has nothing stored in cells upgrades to the
    /// current version all the same, and records it.
    #[dialog_common::test]
    async fn it_upgrades_a_repository_with_nothing_to_carry() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        unversion(&repo, &operator).await?;

        assert_eq!(
            repo.upgrade().perform(&operator).await?,
            Upgraded {
                from: 0,
                to: VERSION
            }
        );

        let cell: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        cell.resolve().perform(&operator).await?;
        assert_eq!(cell.content(), Some(VERSION));
        Ok(())
    }

    /// Storage a newer release upgraded is left alone: this release
    /// does not know its layout, so it refuses rather than misreads it.
    #[dialog_common::test]
    async fn it_refuses_storage_from_a_newer_release() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let cell: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        cell.resolve().perform(&operator).await?;
        cell.publish(VERSION + 1).perform(&operator).await?;

        let refused = repo.upgrade().perform(&operator).await;
        assert!(
            matches!(refused, Err(UpgradeError::Newer { found, supported })
                if found == VERSION + 1 && supported == VERSION),
            "{refused:?}"
        );
        Ok(())
    }

    /// A repository created by this release is already at the current
    /// layout, so nothing a later upgrade could carry over exists.
    #[dialog_common::test]
    async fn it_creates_a_repository_at_the_current_version() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let version: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        version.resolve().perform(&operator).await?;
        assert_eq!(version.content(), Some(VERSION));
        Ok(())
    }

    /// Opening a repository whose storage predates versioning upgrades
    /// it: its upstreams are pulled from without anyone calling
    /// `upgrade`.
    #[dialog_common::test]
    async fn it_upgrades_a_repository_when_it_opens() -> anyhow::Result<()> {
        // `branch/draft/upstream`, in the older single-entry shape:
        // `draft` on origin.
        const ONE: &str = "a16652656d6f7465a3647472656598200909090909090909090909090909090909090909090909090909090909090909666272616e63686564726166746672656d6f7465666f726967696e";
        // `remote/origin/address`: a UCAN service at
        // https://tonk.network/ucan/ holding the repository below.
        const REMOTE: &str = "a26761646472657373a1645563616ea168656e64706f696e74781a68747470733a2f2f746f6e6b2e6e6574776f726b2f7563616e2f677375626a65637478386469643a6b65793a7a364d6b68615867425a44766f74446b4c353235376661697a74694769433251744b4c4770626e6e4547746132646f4b";

        fn bytes(hex: &str) -> Vec<u8> {
            (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("valid hex"))
                .collect()
        }

        let (operator, profile) = test_session_with_peer().await;
        let name = unique_name("legacy");
        let handle = || SpaceHandle {
            peer: profile.did(),
            name: name.clone(),
        };
        let repo = handle().open().perform(&operator).await?;

        // What an earlier release left: no version, cells for a remote
        // and an upstream.
        let version: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        version.resolve().perform(&operator).await?;
        if version.content().is_some() {
            version.retract().perform(&operator).await?;
        }
        for (space, cell, content) in [
            ("remote/origin", "address", REMOTE),
            ("branch/draft", "upstream", ONE),
        ] {
            CellScope::new(Subject::from(repo.did()), space, cell)
                .publish(bytes(content), None)
                .perform(&operator)
                .await?;
        }

        let reopened = handle().open().perform(&operator).await?;
        assert_eq!(reopened.did(), repo.did());

        version.resolve().perform(&operator).await?;
        assert_eq!(version.content(), Some(VERSION), "opening upgraded it");
        let draft = reopened.branch("draft").open().perform(&operator).await?;
        resolve(&draft, &operator).await?;
        assert_eq!(draft.pulls().iter().count(), 1, "draft pulls from origin");
        Ok(())
    }

    /// Every repository's legacy remote is usually named "origin", and
    /// contacts are the host's, not a repository's. A name already given
    /// to another peer is not given again, so looking a peer up by it
    /// stays unambiguous.
    #[dialog_common::test]
    async fn it_names_a_carried_peer_only_if_the_name_is_free() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        for endpoint in ["https://one.example/ucan/", "https://two.example/ucan/"] {
            let repo = test_repo(&operator, &profile).await;
            unversion(&repo, &operator).await?;
            let origin: Cell<RemoteAddress> =
                SpaceScope::new(Subject::from(repo.did()), "remote/origin")
                    .cell("address")
                    .into();
            origin
                .publish(RemoteAddress {
                    address: UcanAddress::new(endpoint).into(),
                    subject: repo.did(),
                })
                .perform(&operator)
                .await?;
            repo.upgrade().perform(&operator).await?;
        }

        let named = Subject::from(profile.did())
            .reader()
            .peers()
            .find("origin")
            .perform(&operator)
            .await?;
        assert_eq!(named.len(), 1, "one peer is known as origin: {named:?}");
        Ok(())
    }
}
