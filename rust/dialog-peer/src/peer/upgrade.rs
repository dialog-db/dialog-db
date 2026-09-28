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
