//! The branch registry: where a branch's existence is recorded.
//!
//! A branch exists as a set of memory cells. The fact that it exists is
//! what makes it findable, and that fact lives in a branch of its own,
//! named [`REGISTRY`](crate::REGISTRY).
//!
//! # Why this lives here
//!
//! A branch records itself with `dialog.branch/*` facts, and the
//! `dialog.` namespace is reserved for machinery-written facts --
//! revision records, delegation records -- so that application writes
//! cannot forge them. Recording a branch is the same kind of write, so
//! it goes through the same [`machinery`](super::Commit::machinery)
//! scope, beside the delegation records that already use it.

use dialog_artifacts::{Changes, Entity, Statement};
use dialog_capability::{Capability, Did, Fork, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, Operator, OperatorExt as _};
use dialog_effects::memory::{Publish, Resolve};
use futures_util::stream;

use crate::schema::{
    ActiveBranch, Branch as BranchConcept, BranchUpstream, Peer, PeerAddress, Replica,
};
use crate::{
    Branch, CommitError, MigrateError, RemoteAddress, RemoteSite, RepositoryMemoryExt as _,
    Upstream,
};

/// The environment a registry write runs against.
pub trait RegistryEnv:
    Provider<Get>
    + Provider<Put>
    + Provider<Import>
    + Provider<Resolve>
    + Provider<Publish>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<crate::Hydrate>
    + Provider<dialog_artifacts::Preload>
    + Provider<dialog_artifacts::Speculation>
    + Provider<Fork<RemoteSite, Resolve>>
    + ConditionalSync
    + 'static
{
}

impl<T> RegistryEnv for T where
    T: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static
{
}

/// Record `name` as a branch of the replica `operator` views.
///
/// Idempotent: the fact is derived from `(replica, name)`, so recording
/// the same branch twice converges on one record rather than
/// conflicting.
pub async fn record<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
) -> Result<(), CommitError> {
    write(registry, operator, name, env, Written::Asserted).await
}

/// Forget `name`, so it stops being listed.
///
/// The caller retracts the branch's cells first: this half is what a
/// listing reads, so retracting it last means a failure part-way leaves
/// cells nothing points at rather than a branch that lists but cannot
/// be opened.
pub async fn forget<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
) -> Result<(), CommitError> {
    write(registry, operator, name, env, Written::Retracted).await
}

/// Which way the record goes.
#[derive(Clone, Copy)]
enum Written {
    Asserted,
    Retracted,
}

async fn write<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
    written: Written,
) -> Result<(), CommitError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());
    let record = BranchConcept::new(&replica, name);

    let mut changes = Changes::new();
    match written {
        Written::Asserted => record.assert(&mut changes),
        Written::Retracted => record.retract(&mut changes),
    }

    apply(registry, changes, env).await
}

/// Switch the replica `operator` views to the branch `branch`.
///
/// Records which branch is active as a cardinality-one fact on the
/// replica, so switching again supersedes the previous one. The branch
/// is named by its entity rather than a name because it need not be on
/// this replica: it is not looked up, only pointed at.
pub async fn switch<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    branch: &Entity,
    env: &Env,
) -> Result<(), CommitError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());
    let active = ActiveBranch {
        this: replica.this,
        branch: branch.clone().into(),
    };

    let mut changes = Changes::new();
    active.assert(&mut changes);

    apply(registry, changes, env).await
}

/// Record `peer` and the addresses it is reached at.
///
/// Converges: the peer is its DID, so recording the same peer again adds
/// nothing, and a new address joins the ones already recorded.
pub async fn add_peer<Env: RegistryEnv>(
    registry: &Branch,
    peer: &Peer,
    addresses: &[PeerAddress],
    env: &Env,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    peer.clone().assert(&mut changes);
    for address in addresses {
        address.clone().assert(&mut changes);
    }
    apply(registry, changes, env).await
}

/// Record that `branch` tracks `upstream`: a branch on this replica, or
/// one on a peer's, each named by entity.
pub async fn set_upstream<Env: RegistryEnv>(
    registry: &Branch,
    branch: &BranchConcept,
    upstream: &BranchConcept,
    env: &Env,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    BranchUpstream {
        this: branch.this.clone(),
        upstream: upstream.this.clone().into(),
    }
    .assert(&mut changes);
    apply(registry, changes, env).await
}

/// Carry remotes and upstreams recorded in cells over into facts.
///
/// Memory cannot enumerate its spaces, so the caller names what to
/// carry over: the `remotes` it knows, and the `branches` whose
/// upstreams to read. A remote an upstream points at is carried over
/// too, named or not.
///
/// Each remote becomes a [`Peer`] named after it, at the address its
/// cell holds; each upstream becomes a `dialog.branch/upstream` fact on
/// the tracking branch, pointing at the tracked branch by entity. The
/// cells are left in place.
///
/// Converges: running it again asserts the same facts.
pub async fn migrate<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    remotes: &[&str],
    branches: &[&str],
    env: &Env,
) -> Result<(), MigrateError> {
    let subject = Subject::from(registry.of().clone());
    let local = Replica::new(operator.profile().clone(), registry.of().clone());

    let mut peers: Vec<Carried> = Vec::new();
    let mut changes = Changes::new();

    for name in branches {
        let cell = subject.branch(*name).upstream();
        cell.resolve()
            .perform(env)
            .await
            .map_err(|source| MigrateError::Upstream {
                name: name.to_string(),
                source,
            })?;
        let tracking = local.branch(*name);

        for upstream in cell.content().unwrap_or_default().iter() {
            let tracked = match upstream {
                Upstream::Local { branch, .. } => local.branch(branch.as_str()),
                Upstream::Remote { remote, branch, .. } => {
                    let address = load(&subject, remote, env).await?;
                    let (peer, of) = carry(&mut peers, remote, address)?;
                    peer.repository(of).branch(branch.as_str())
                }
            };
            BranchUpstream {
                this: tracking.this.clone(),
                upstream: tracked.this.into(),
            }
            .assert(&mut changes);
        }
    }

    for name in remotes {
        let address = load(&subject, name, env).await?;
        carry(&mut peers, name, address)?;
    }

    for carried in peers {
        carried.peer.assert(&mut changes);
        carried.address.assert(&mut changes);
    }

    apply(registry, changes, env).await?;
    Ok(())
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
) -> Result<(Peer, Did), MigrateError> {
    if let Some(carried) = peers.iter().find(|carried| carried.name == name) {
        return Ok((carried.peer.clone(), carried.subject.clone()));
    }
    let peer = Peer::at(name, &address.address)?;
    peers.push(Carried {
        name: name.to_string(),
        address: PeerAddress::new(&peer, &address.address)?,
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
) -> Result<RemoteAddress, MigrateError> {
    let remote = subject
        .remote(name)
        .load()
        .perform(env)
        .await
        .map_err(|source| MigrateError::Remote {
            name: name.to_string(),
            source,
        })?;
    Ok(remote.address())
}

/// Commit `changes` to the registry under the machinery scope, which
/// is what lets them write the reserved `dialog.` namespace.
async fn apply<Env: RegistryEnv>(
    registry: &Branch,
    changes: Changes,
    env: &Env,
) -> Result<(), CommitError> {
    let instructions = changes.into_instructions();
    if instructions.is_empty() {
        return Ok(());
    }

    Box::pin(
        registry
            .commit(stream::iter(instructions))
            .machinery()
            .allow_empty()
            .perform(env),
    )
    .await?;

    Ok(())
}

/// Every branch recorded on the replica `operator` views.
///
/// The registry itself comes back among them without ever having been
/// recorded: its fact is synthesized into the query's overlay, which is
/// what keeps a registry from having to exist before it can be created.
pub async fn list<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<Vec<BranchConcept>, dialog_query::EvaluationError> {
    use dialog_query::{Output as _, Query, Term};

    let replica = Replica::new(operator.profile().clone(), registry.of().clone());

    Box::pin(
        registry
            .query()
            .select(Query::<BranchConcept> {
                this: Term::var("this"),
                name: Term::var("name"),
                replica: replica.this.clone().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::{REGISTRY, RepositoryMemoryExt};
    use dialog_capability::Subject;
    use dialog_effects::authority::Identify;
    use dialog_operator::helpers::test_operator_with_profile;

    /// A recorded branch is listed; the registry lists itself without
    /// ever having been recorded.
    #[dialog_common::test]
    async fn it_records_and_lists() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let identity = Identify.perform(&operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;

        super::record(&registry, &identity, "feature", &operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        let names: Vec<String> = super::list(&registry, &identity, &operator)
            .await?
            .into_iter()
            .map(|branch| branch.name.0)
            .collect();

        assert!(
            names.contains(&"feature".to_string()),
            "a recorded branch is listed: {names:?}"
        );
        assert!(
            names.contains(&REGISTRY.to_string()),
            "the registry lists itself: {names:?}"
        );

        Ok(())
    }

    /// The active branch is written under the name tonk reads it by,
    /// `dialog.replica/active-branch`, on the replica's entity. The
    /// name is the contract, so it is checked against the stored claim
    /// rather than through the concept that also defines it.
    #[dialog_common::test]
    async fn it_records_the_active_branch_by_name() -> anyhow::Result<()> {
        use crate::schema::{Branch as BranchConcept, Replica};
        use dialog_artifacts::{ArtifactSelector, Value};
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let identity = Identify.perform(&operator).await?;
        let replica = Replica::new(profile.did(), repo.did());
        let feature = BranchConcept::new(&replica, "feature").this;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        super::switch(&registry, &identity, &feature, &operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .load()
            .perform(&operator)
            .await?;
        let select = registry.claims().select(
            ArtifactSelector::new()
                .the("dialog.replica/active-branch".parse()?)
                .of(replica.this.clone()),
        );
        let store = crate::NetworkedIndex::new(&operator, select.catalog(), None);
        let rows: Vec<_> = select.execute(store).await?.collect::<Vec<_>>().await;

        assert_eq!(rows.len(), 1, "one active branch recorded: {}", rows.len());
        let artifact = rows.into_iter().next().expect("one row")?;
        assert_eq!(artifact.to_owned()?.is, Value::Entity(feature));
        Ok(())
    }

    /// Remotes and upstreams stored in cells before peers existed are
    /// carried over into facts.
    ///
    /// The cells are written from the bytes earlier releases wrote, not
    /// by encoding today's types, so the test keeps reading what is
    /// actually on disk even after those types change. Both upstream
    /// shapes are covered: the list of entries, and the single bare
    /// entry older releases wrote.
    #[dialog_common::test]
    async fn it_migrates_remotes_and_upstreams_from_cells() -> anyhow::Result<()> {
        use crate::SiteAddress;
        use crate::schema::{BranchUpstream, Peer, PeerAddress, Replica};
        use dialog_effects::memory::prelude::CellScope;
        use dialog_query::{Output as _, Query, Term};
        use dialog_remote_ucan::UcanAddress;
        use dialog_varsig::did;

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
        let identity = Identify.perform(&operator).await?;
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

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        super::migrate(&registry, &identity, &[], &["main", "draft"], &operator).await?;

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

        // Each upstream points at the tracked branch by entity: on the
        // peer's replica of the repository it holds, or on this one.
        let local = Replica::new(profile.did(), repo.did());
        let remote = origin.repository(held);
        let tracked = async |name: &str| -> anyhow::Result<Vec<_>> {
            let mut upstreams: Vec<_> = registry
                .query()
                .select(Query::<BranchUpstream> {
                    this: local.branch(name).this.into(),
                    upstream: Term::var("upstream"),
                })
                .perform(&operator)
                .try_vec()
                .await?
                .into_iter()
                .map(|row: BranchUpstream| row.upstream.0)
                .collect();
            upstreams.sort();
            Ok(upstreams)
        };

        let mut main = vec![remote.branch("main").this, local.branch("develop").this];
        main.sort();
        assert_eq!(tracked("main").await?, main);
        assert_eq!(tracked("draft").await?, vec![remote.branch("draft").this]);

        // Carrying over again changes nothing.
        super::migrate(
            &registry,
            &identity,
            &["origin"],
            &["main", "draft"],
            &operator,
        )
        .await?;
        assert_eq!(tracked("main").await?, main);

        Ok(())
    }

    /// A peer is added with its address, and a local branch is set to
    /// track a branch on the peer's replica, navigated to from the peer.
    #[dialog_common::test]
    async fn it_tracks_a_branch_on_a_peer() -> anyhow::Result<()> {
        use crate::SiteAddress;
        use crate::schema::{BranchUpstream, Peer, PeerAddress, Replica};
        use dialog_query::{Output as _, Query, Term};
        use dialog_remote_ucan::UcanAddress;
        use dialog_varsig::did;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;

        let address = SiteAddress::from(UcanAddress::new("https://tonk.network/ucan/"));
        let peer = Peer::at("tonk", &address)?;
        super::add_peer(
            &registry,
            &peer,
            &[PeerAddress::new(&peer, &address)?],
            &operator,
        )
        .await?;

        let local = Replica::new(profile.did(), repo.did()).branch("main");
        let remote = peer.repository(did!("key:zAlice")).branch("main");
        super::set_upstream(&registry, &local, &remote, &operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        let upstreams: Vec<BranchUpstream> = registry
            .query()
            .select(Query::<BranchUpstream> {
                this: local.this.clone().into(),
                upstream: Term::var("upstream"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            upstreams
                .into_iter()
                .map(|row| row.upstream.0)
                .collect::<Vec<_>>(),
            vec![remote.this]
        );
        Ok(())
    }
}
