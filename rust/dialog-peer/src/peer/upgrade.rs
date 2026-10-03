//! Bringing a peer's records up to date: numbered steps an application
//! registers, run once each when the peer is built.
//!
//! What a peer keeps in its space, and how, changes over time, and so
//! does what an application keeps there. A repository's own layout is
//! upgraded when it opens (`dialog-repository`'s `upgrade`); this is the
//! layer above it, for the records a peer and its application keep: a
//! version the peer's home records in one cell, `dialog/peer`, and steps
//! numbered from it. The builder runs every registered step whose
//! version is above the recorded one, in order, and records each
//! version as its step completes. A step asserts the same facts however
//! often it runs, so one interrupted before its version is recorded
//! simply runs again next time.
//!
//! An application registers its own steps with
//! [`PeerBuilder::upgrade`](super::PeerBuilder::upgrade): moving records
//! it kept in a layout of its own into the peer's, say. Steps run for a
//! peer acting as itself; a session runs none, since it writes nothing to
//! its peer's space.
//!
//! What lives below the peer is not a step here. A key a space kept from
//! before the credential store is moved by
//! [`CredentialStore::adopt_from`](dialog_storage::provider::storage::CredentialStore::adopt_from),
//! which the application runs before it opens the peer's credential.

use std::future::Future;
use std::pin::Pin;

use dialog_capability::Subject;
use dialog_effects::memory::prelude::SpaceScope;
use dialog_repository::Cell;

use super::{Local, Peer, PeerError, PeerSpace};

/// The space and cell in a peer's home recording the version its records
/// are at. Fixed for good: this is read before anything else.
const SPACE: &str = "dialog";
const CELL: &str = "peer";

/// The future a step runs.
#[cfg(not(target_arch = "wasm32"))]
pub type StepFuture<'a> = Pin<Box<dyn Future<Output = Result<(), PeerError>> + Send + 'a>>;
/// The future a step runs (single-threaded wasm form).
#[cfg(target_arch = "wasm32")]
pub type StepFuture<'a> = Pin<Box<dyn Future<Output = Result<(), PeerError>> + 'a>>;

#[cfg(not(target_arch = "wasm32"))]
type StepFn<S> = Box<dyn for<'a> Fn(&'a Peer<S, Local>) -> StepFuture<'a> + Send + Sync>; // bare-send-ok: type-erased closure needs real auto-trait bounds
#[cfg(target_arch = "wasm32")]
type StepFn<S> = Box<dyn for<'a> Fn(&'a Peer<S, Local>) -> StepFuture<'a>>;

/// One step of a peer's upgrade: the version it brings the records to,
/// and what it does to get there.
pub struct Step<S: Clone> {
    version: u32,
    name: String,
    run: StepFn<S>,
}

impl<S: Clone> Step<S> {
    /// A step to `version`, named for what it does, running `run` over
    /// the peer. Versions are the application's own sequence, and every
    /// step must assert the same facts however often it runs.
    pub fn new<F>(version: u32, name: impl Into<String>, run: F) -> Self
    where
        F: for<'a> Fn(&'a Peer<S, Local>) -> StepFuture<'a> + StepBound,
    {
        Self {
            version,
            name: name.into(),
            run: Box::new(run),
        }
    }

    /// The version this step brings the records to.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// What this step does.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The auto-trait bounds a step's closure needs on each target.
#[cfg(not(target_arch = "wasm32"))]
pub trait StepBound: Send + Sync + 'static {} // bare-send-ok: mirrors StepFn
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + 'static> StepBound for T {} // bare-send-ok: mirrors StepFn
/// The auto-trait bounds a step's closure needs on each target.
#[cfg(target_arch = "wasm32")]
pub trait StepBound: 'static {}
#[cfg(target_arch = "wasm32")]
impl<T: 'static> StepBound for T {}

/// What an upgrade did: the version the records were at, and the one
/// they are at now. Equal when there was nothing to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Upgraded {
    /// The version the records were at.
    pub from: u32,
    /// The version the records are at now.
    pub to: u32,
}

impl<S: PeerSpace> Peer<S, Local> {
    /// The version this peer's records are at: what the last completed
    /// step recorded, or 0 for a peer no step has run for.
    pub async fn version(&self) -> Result<u32, PeerError> {
        let cell = self.version_cell();
        cell.resolve()
            .perform(self)
            .await
            .map_err(|error| PeerError::Upgrade(error.to_string()))?;
        Ok(cell.content().unwrap_or(0))
    }

    fn version_cell(&self) -> Cell<u32> {
        SpaceScope::new(Subject::from(self.home().clone()), SPACE)
            .cell(CELL)
            .into()
    }

    /// Run every step in `steps` whose version is above the recorded
    /// one, lowest first, recording each version as its step completes.
    pub(crate) async fn upgrade(&self, steps: &[&Step<S>]) -> Result<Upgraded, PeerError> {
        let mut pending: Vec<&Step<S>> = steps.to_vec();
        pending.sort_by_key(|step| step.version);
        let cell = self.version_cell();
        cell.resolve()
            .perform(self)
            .await
            .map_err(|error| PeerError::Upgrade(error.to_string()))?;
        let from = cell.content().unwrap_or(0);
        let mut at = from;
        for step in pending {
            if step.version <= at {
                continue;
            }
            (step.run)(self).await.map_err(|error| {
                PeerError::Upgrade(format!(
                    "step {} ({}) failed: {error}",
                    step.version, step.name
                ))
            })?;
            cell.publish(step.version)
                .perform(self)
                .await
                .map_err(|error| {
                    PeerError::Upgrade(format!(
                        "step {} ({}) ran but its version was not recorded: {error}",
                        step.version, step.name
                    ))
                })?;
            at = step.version;
        }
        Ok(Upgraded { from, to: at })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::Step;
    use crate::helpers::{test_credential_store, test_grant, test_peer, test_storage, unique_name};
    use crate::{OpenCredential, Peer, PeerError};
    use dialog_effects::storage::Location;
    use dialog_repository::Repository;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_varsig::Principal as _;

    /// A step that counts how often it ran.
    fn counting(version: u32, ran: &Arc<AtomicUsize>) -> Step<VolatileSpace> {
        let ran = Arc::clone(ran);
        Step::new(version, format!("count-{version}"), move |_peer| {
            let ran = Arc::clone(&ran);
            Box::pin(async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })
    }

    /// A registered step runs once for a peer, not again when the peer
    /// is built again over the same storage, and a later step runs only
    /// from where the records are.
    #[dialog_common::test]
    async fn it_runs_each_step_once() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let location = Location::temp(unique_name("upgrade"));
        let credential = OpenCredential::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&test_credential_store())
            .await?;
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));

        let peer = Peer::new(credential.clone())
            .at(location.clone())
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage.clone())
            .grant(test_grant().await)
            .upgrade(counting(1, &first))
            .await?;
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(peer.version().await?, 1);

        let peer = Peer::new(credential.clone())
            .at(location.clone())
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage.clone())
            .grant(test_grant().await)
            .upgrade(counting(2, &second))
            .upgrade(counting(1, &first))
            .await?;
        assert_eq!(first.load(Ordering::SeqCst), 1, "a step ran again");
        assert_eq!(second.load(Ordering::SeqCst), 1);
        assert_eq!(peer.version().await?, 2);
        Ok(())
    }

    /// A step that fails records no version, so it runs again next time,
    /// and the steps above it wait.
    #[dialog_common::test]
    async fn it_runs_a_failed_step_again() -> anyhow::Result<()> {
        let storage = test_storage().await;
        let location = Location::temp(unique_name("upgrade-retry"));
        let credential = OpenCredential::open(location.name.clone())
            .at(location.directory.clone())
            .perform(&test_credential_store())
            .await?;
        let later = Arc::new(AtomicUsize::new(0));
        let failing: Step<VolatileSpace> = Step::new(1, "fails", |_peer| {
            Box::pin(async { Err(PeerError::Upgrade("not yet".to_string())) })
        });

        let refused = Peer::new(credential.clone())
            .at(location.clone())
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage.clone())
            .grant(test_grant().await)
            .upgrade(failing)
            .upgrade(counting(2, &later))
            .await;
        assert!(matches!(refused, Err(PeerError::Upgrade(_))), "{refused:?}");
        assert_eq!(
            later.load(Ordering::SeqCst),
            0,
            "a later step ran past a failure"
        );

        let ran = Arc::new(AtomicUsize::new(0));
        let peer = Peer::new(credential.clone())
            .at(location)
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage)
            .grant(test_grant().await)
            .upgrade(counting(1, &ran))
            .upgrade(counting(2, &later))
            .await?;
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        assert_eq!(later.load(Ordering::SeqCst), 1);
        assert_eq!(peer.version().await?, 2);
        Ok(())
    }

    /// A peer's storage from before the credential store, the layout the
    /// `tonk-2026-09-25.1` release wrote, keeps working: the home space
    /// holds the peer's signing key, and its repository keeps its remote
    /// and upstream in cells. Bringing it up to date is three moves an
    /// application makes in order: the key into the credential store
    /// (the peer's DID does not change), the repository's cells into
    /// facts (when the repository opens), and the application's own
    /// records by the steps it registers (here: an account the peer acts
    /// for, which the old layout had no notion of).
    ///
    /// The cells are written from the bytes that release wrote, not by
    /// encoding today's types, so this keeps reading what is on disk.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_brings_storage_from_before_the_credential_store_up_to_date() -> anyhow::Result<()> {
        use crate::helpers::{test_custodian, test_system};
        use crate::{Allowance, SpaceVaultExt as _};
        use dialog_capability::Subject;
        use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
        use dialog_effects::credential::{SELF, prelude::*};
        use dialog_effects::memory::prelude::CellScope;
        use dialog_effects::storage::{Directory, Location};
        use dialog_query::{Output as _, Query, Term};
        use dialog_repository::{RepositoryExt as _, schema::BranchPull};
        use dialog_repository::{Upgraded as RepositoryUpgraded, VERSION};
        use dialog_storage::provider::FileSystem;
        use dialog_storage::provider::storage::{CredentialStore, NativeSpace, Storage};
        use dialog_storage::resource::Resource as _;

        // `remote/origin/address`: a UCAN service at
        // https://tonk.network/ucan/ holding the repository below.
        const REMOTE: &str = "a26761646472657373a1645563616ea168656e64706f696e74781a68747470733a2f2f746f6e6b2e6e6574776f726b2f7563616e2f677375626a65637478386469643a6b65793a7a364d6b68615867425a44766f74446b4c353235376661697a74694769433251744b4c4770626e6e4547746132646f4b";
        // `branch/main/upstream`: `main` on origin, then local `develop`.
        const MANY: &str = "82a16652656d6f7465a3647472656598200707070707070707070707070707070707070707070707070707070707070707666272616e6368646d61696e6672656d6f7465666f726967696ea1654c6f63616ca2647472656598200000000000000000000000000000000000000000000000000000000000000000666272616e636867646576656c6f70";
        fn bytes(hex: &str) -> Vec<u8> {
            (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).expect("valid hex"))
                .collect()
        }

        // The storage as that release left it.
        let root = tempfile::tempdir()?;
        let base = Directory::At(root.path().to_string_lossy().into_owned());
        let location = Location::new(base.clone(), "home");
        let key = SignerCredential::from(Ed25519Signer::generate().await?);
        let home = FileSystem::open(&location).await?;
        key.did()
            .credential()
            .key(SELF)
            .save(Credential::Signer(key.clone()))
            .perform(&home)
            .await?;
        for (space, cell, content) in [
            ("remote/origin", "address", REMOTE),
            ("branch/main", "upstream", MANY),
        ] {
            CellScope::new(Subject::from(key.did()), space, cell)
                .publish(bytes(content), None)
                .perform(&home)
                .await?;
        }
        drop(home);

        // 1. The key moves into the credential store; the peer's DID stays.
        let system = test_system().await;
        let storage = Storage::<NativeSpace>::default().owned_by(system.did());
        let credentials = CredentialStore::<NativeSpace>::default();
        credentials.adopt_from(&storage, &location).await?;
        let credential = crate::OpenCredential::load("home")
            .at(base.clone())
            .perform(&credentials)
            .await?;
        assert_eq!(credential.did(), key.did(), "the peer's identity changed");
        assert!(
            matches!(
                storage.identity(&key.did()).await,
                Some(Credential::Verifier(_))
            ),
            "the home still holds its signing key"
        );

        // 3. The application's own step: this peer has an account now.
        let onboarded = Arc::new(AtomicUsize::new(0));
        let onboard = {
            let onboarded = Arc::clone(&onboarded);
            Step::<NativeSpace>::new(1, "onboard", move |peer| {
                let onboarded = Arc::clone(&onboarded);
                Box::pin(async move {
                    let custodian = test_custodian(peer)
                        .await
                        .map_err(|error| PeerError::Upgrade(error.to_string()))?;
                    let account = peer
                        .state()
                        .vault("account")
                        .open()
                        .via(&custodian)
                        .perform(peer)
                        .await
                        .map_err(|error| PeerError::Upgrade(error.to_string()))?;
                    account
                        .add(custodian.did())
                        .perform(peer)
                        .await
                        .map_err(|error| PeerError::Upgrade(error.to_string()))?;
                    account
                        .delegate(peer.did())
                        .perform(peer)
                        .await
                        .map_err(|error| PeerError::Upgrade(error.to_string()))?;
                    onboarded.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let peer = Peer::new(credential.clone())
            .at(location.clone())
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage.clone())
            .grant(Allowance::storage(&system))
            .base(base.clone())
            .upgrade(onboard)
            .await?;
        assert_eq!(onboarded.load(Ordering::SeqCst), 1);
        assert_eq!(peer.version().await?, 1);
        assert!(
            peer.authority().await.is_ok(),
            "the peer acts for no account"
        );

        // 2. The repository's cells became facts when it opened.
        let repository = peer
            .space(key.did().to_string())
            .open()
            .perform(&peer)
            .await?;
        assert_eq!(
            repository.upgrade().perform(&peer).await?,
            RepositoryUpgraded {
                from: VERSION,
                to: VERSION
            },
            "the repository was not upgraded when it opened"
        );
        let registry = repository.branch("meta").open().perform(&peer).await?;
        let pulls: Vec<BranchPull> = registry
            .query()
            .select(Query::<BranchPull> {
                this: Term::var("this"),
                pull: Term::var("pull"),
            })
            .perform(&peer)
            .try_vec()
            .await?;
        assert!(
            !pulls.is_empty(),
            "main's upstream was not carried into the registry"
        );

        // Opened again, nothing runs twice.
        let again = Peer::new(credential.clone())
            .at(location)
            .space(Repository::from(credential.did()).branch("main"))
            .with(storage)
            .grant(Allowance::storage(&system))
            .base(base)
            .upgrade({
                let onboarded = Arc::clone(&onboarded);
                Step::<NativeSpace>::new(1, "onboard", move |_peer| {
                    let onboarded = Arc::clone(&onboarded);
                    Box::pin(async move {
                        onboarded.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                })
            })
            .await?;
        assert_eq!(onboarded.load(Ordering::SeqCst), 1, "the step ran again");
        assert_eq!(again.version().await?, 1);
        Ok(())
    }

    /// A session runs no step: it writes nothing to its peer's space.
    #[dialog_common::test]
    async fn it_runs_no_step_for_a_session() -> anyhow::Result<()> {
        let peer = test_peer().await;
        let ran = Arc::new(AtomicUsize::new(0));
        let _session = peer
            .session(b"upgrade")
            .space(peer.state())
            .upgrade(counting(1, &ran))
            .await?;
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        assert_eq!(peer.version().await?, 0);
        Ok(())
    }
}
