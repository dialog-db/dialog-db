//! Upgrading a repository's local storage to the current layout.
//!
//! What a release keeps locally, and where, changes over time. A
//! repository records the layout version its storage is at in one cell,
//! `dialog/version`, which no upgrade ever moves: every release can find
//! it before it knows anything else about the layout. Storage without
//! the cell is at version 0, the layout from before versioning.
//!
//! Upgrading is explicit. [`Repository::upgrade`] runs every step from
//! the recorded version up to [`VERSION`] and records the new version
//! last. A step asserts the same facts however often it runs, so an
//! upgrade interrupted before it records its version simply runs again.
//!
//! # Steps
//!
//! - **0 → 1**: remotes and upstreams move from cells into facts in the
//!   [`REGISTRY`](crate::REGISTRY). Each remote becomes a
//!   [`Peer`](crate::schema::Peer) at the address its cell holds; each
//!   upstream becomes a pull and a push relation to the branch it
//!   tracked. The cells, and `credential/key/self`, are left in place
//!   for one version and removed by the next.

use dialog_artifacts::Changes;
use dialog_capability::{Capability, Did, Provider, Subject};
use dialog_effects::MethodExt as _;
use dialog_effects::authority::{Identify, Operator, OperatorExt as _};
use dialog_effects::memory::List;
use dialog_effects::memory::prelude::{
    ListSpaceExt as _, MemoryExt as _, SpaceExt as _, SpaceScope,
};
use dialog_query::Statement as _;
use dialog_varsig::Principal;

use crate::registry::{RegistryEnv, apply, pull, push};
use crate::schema::{Peer, PeerAddress, Replica};
use crate::{
    Branch, Cell, REGISTRY, RemoteAddress, Repository, RepositoryMemoryExt as _, UpgradeError,
    Upstream,
};

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
        Env: RegistryEnv + Provider<List>,
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
        let registry = self.subject.branch(REGISTRY).open().perform(env).await?;

        if from < 1 {
            carry_over(&self.subject, &registry, &operator, env).await?;
        }

        cell.publish(VERSION).perform(env).await?;
        Ok(Upgraded { from, to: VERSION })
    }
}

/// Step 0 → 1: carry remotes and upstreams out of their cells into
/// facts, found by listing what is stored under `remote/` and `branch/`.
async fn carry_over<Env>(
    subject: &Subject,
    registry: &Branch,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<(), UpgradeError>
where
    Env: RegistryEnv + Provider<List>,
{
    let local = Replica::new(operator.profile().clone(), registry.of().clone());
    let remotes = stored(subject, "remote", "/address", env).await?;
    let branches = stored(subject, "branch", "/upstream", env).await?;

    let mut peers: Vec<Carried> = Vec::new();
    let mut changes = Changes::new();

    for name in &branches {
        let cell = subject.branch(name.as_str()).upstream();
        cell.resolve()
            .perform(env)
            .await
            .map_err(|source| UpgradeError::Upstream {
                name: name.clone(),
                source,
            })?;
        let tracking = local.branch(name.as_str());

        for upstream in cell.content().unwrap_or_default().iter() {
            let tracked = match upstream {
                Upstream::Local { branch, .. } => local.branch(branch.as_str()),
                Upstream::Remote { remote, branch, .. } => {
                    let address = load(subject, remote, env).await?;
                    let (peer, of) = carry(&mut peers, remote, address)?;
                    peer.repository(of).branch(branch.as_str())
                }
            };
            pull(&tracking, &tracked).assert(&mut changes);
            push(&tracking, &tracked).assert(&mut changes);
        }
    }

    for name in &remotes {
        let address = load(subject, name, env).await?;
        carry(&mut peers, name, address)?;
    }

    for carried in peers {
        carried.peer.assert(&mut changes);
        carried.address.assert(&mut changes);
    }

    apply(registry, changes, env).await?;
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
struct Carried {
    name: String,
    peer: Peer,
    address: PeerAddress,
    subject: Did,
}

/// Carry the remote `name` over once, however many upstreams name it,
/// answering the peer it became and the subject it holds.
fn carry(
    peers: &mut Vec<Carried>,
    name: &str,
    address: RemoteAddress,
) -> Result<(Peer, Did), UpgradeError> {
    if let Some(carried) = peers.iter().find(|carried| carried.name == name) {
        return Ok((carried.peer.clone(), carried.subject.clone()));
    }
    let peer = Peer::at(name, &address.address)?;
    peers.push(Carried {
        name: name.to_string(),
        address: PeerAddress::new(&peer.this, &address.address)?,
        peer: peer.clone(),
        subject: address.subject.clone(),
    });
    Ok((peer, address.subject))
}

/// The address a remote's cell holds.
async fn load<Env: RegistryEnv>(
    subject: &Subject,
    name: &str,
    env: &Env,
) -> Result<RemoteAddress, UpgradeError> {
    let remote = subject
        .remote(name)
        .load()
        .perform(env)
        .await
        .map_err(|source| UpgradeError::Remote {
            name: name.to_string(),
            source,
        })?;
    Ok(remote.address())
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::{CELL, SPACE, Upgraded, VERSION};
    use crate::helpers::test_repo;
    use crate::schema::{BranchPull, BranchPush, Peer, PeerAddress, Replica};
    use crate::{Cell, REGISTRY, RepositoryMemoryExt as _, SiteAddress, UpgradeError};
    use dialog_capability::Subject;
    use dialog_effects::memory::prelude::{CellScope, SpaceScope};
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::{Output as _, Query, Term};
    use dialog_remote_ucan::UcanAddress;
    use dialog_varsig::did;

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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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

        // The remote is a peer named after it, identified by its origin,
        // reached at the address its cell held.
        let peers: Vec<Peer> = registry
            .query()
            .select(Query::<Peer> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(peers.len(), 1, "one remote, one peer");
        let origin = peers.into_iter().next().expect("one peer");
        assert_eq!(origin.this.to_string(), "did:web:tonk.network");
        assert_eq!(origin.name.0, "origin");

        let addresses: Vec<PeerAddress> = registry
            .query()
            .select(Query::<PeerAddress> {
                this: origin.this.clone().into(),
                address: Term::var("address"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let sites = addresses
            .iter()
            .map(PeerAddress::site)
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
        let remote = origin.repository(held);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let cell: Cell<u32> = SpaceScope::new(Subject::from(repo.did()), SPACE)
            .cell(CELL)
            .into();
        cell.publish(VERSION + 1).perform(&operator).await?;

        let refused = repo.upgrade().perform(&operator).await;
        assert!(
            matches!(refused, Err(UpgradeError::Newer { found, supported })
                if found == VERSION + 1 && supported == VERSION),
            "{refused:?}"
        );
        Ok(())
    }
}
