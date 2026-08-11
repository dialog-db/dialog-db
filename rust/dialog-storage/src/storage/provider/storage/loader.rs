//! Space loading and creation.
//!
//! Handles `storage::Load` and `storage::Create` effects, managing
//! location-to-DID mappings and space provider lifecycle.

use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Policy, Provider, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::prelude::*;
use dialog_effects::{credential, storage};
use dialog_varsig::Principal;
use storage::{Location, StorageError};

use crate::provider::SpaceProvider;
use crate::resource::{Pool, Resource};

/// What a location is currently known to be.
///
/// The state a plain `Option<Did>` cannot express is [`Mount::Claimed`]:
/// a `Create` has passed its checks but has not registered yet, because
/// it is still `await`ing an `open` and a credential write. Modelling
/// that window explicitly is what lets a second `Create` refuse instead
/// of racing through the same gap.
#[derive(Clone)]
enum Mount {
    /// A `Create` is in flight for this location. Nothing is registered.
    Claimed,
    /// The location resolves to this DID and its store is in `spaces`.
    Mounted(Did),
}

impl Mount {
    /// The DID this location resolves to, if creation finished.
    fn did(&self) -> Option<&Did> {
        match self {
            Mount::Claimed => None,
            Mount::Mounted(did) => Some(did),
        }
    }
}

/// Handles storage::Load and storage::Create, mutating the shared table.
///
/// Maintains a location -> DID mapping so that loading the same location
/// twice returns the existing DID (important for non-persistent backends).
///
/// Both tables are shared behind an `Arc`, so a clone of a loader
/// resolves locations through the same mapping and mounts into the same
/// pool. Cloning to get a second handle onto one set of spaces is the
/// point; a clone with its own mounts table would hand the same location
/// two DIDs. Sharing also means two handles contend for the same claim,
/// which is what keeps concurrent creates across clones exclusive.
pub struct Loader<S> {
    spaces: Arc<Pool<Did, S>>,
    /// Location key -> [`Mount`]. An absent entry means "never attempted";
    /// the two present states are the enum's job to distinguish.
    mounts: Arc<Pool<String, Mount>>,
}

impl<S> Clone for Loader<S> {
    fn clone(&self) -> Self {
        Self {
            spaces: Arc::clone(&self.spaces),
            mounts: Arc::clone(&self.mounts),
        }
    }
}

impl<S> Loader<S> {
    pub fn new(spaces: Arc<Pool<Did, S>>) -> Self {
        Self {
            spaces,
            mounts: Arc::new(Pool::new()),
        }
    }

    /// Claim `key` for creation, or report what is already there.
    ///
    /// `Create` checks the mounts table, then `await`s an `open` and a
    /// credential write before it registers. Publishing [`Mount::Claimed`]
    /// before the first `await` is what closes that window: a second
    /// `Create` for the same location sees the claim and refuses, instead
    /// of passing the same checks and registering a second DID for one
    /// location. Claiming and checking are one locked step inside `Pool`,
    /// so there is no gap between them to race through.
    ///
    /// The returned [`Claim`] releases the location when dropped, so an
    /// early return (or a panic) cannot leave a claim behind that would
    /// make the location permanently uncreatable. Call [`Claim::mount`] to
    /// turn it into a registered mount instead.
    fn claim<'a>(&'a self, key: &str) -> Result<Claim<'a, S>, StorageError> {
        match self.mounts.claim(key.to_string(), Mount::Claimed) {
            // We published the claim; the location is ours to create.
            None => Ok(Claim {
                mounts: &self.mounts,
                spaces: &self.spaces,
                key: key.to_string(),
                held: true,
            }),
            Some(_) => Err(StorageError::AlreadyExists(key.to_string())),
        }
    }

    /// Record a location that was discovered rather than created.
    ///
    /// `Load` has no claim to promote: it resolves an existing space, and
    /// two concurrent loads of one location converge on the same DID (they
    /// read it out of the store), so the last writer winning is harmless.
    fn register(&self, did: Did, location_key: String, store: S) {
        self.mounts
            .insert(location_key, Mount::Mounted(did.clone()));
        self.spaces.insert(did, store);
    }

    fn lookup(&self, key: &String) -> Option<Did> {
        self.mounts.get(key).as_ref().and_then(Mount::did).cloned()
    }
}

/// An in-flight claim on a location, held for the duration of a `Create`.
///
/// Exists so the [`Mount::Claimed`] window cannot outlive the attempt that
/// opened it: dropping this releases the location, whether the `Create`
/// returned early, failed, or panicked. Only [`Claim::mount`] converts it
/// into a lasting [`Mount::Mounted`].
struct Claim<'a, S> {
    mounts: &'a Pool<String, Mount>,
    spaces: &'a Pool<Did, S>,
    key: String,
    held: bool,
}

impl<S> Claim<'_, S> {
    /// Promote the claim to a mount: the location resolves to `did` from
    /// here on, and its store becomes routable.
    fn mount(mut self, did: Did, store: S) {
        self.mounts
            .insert(self.key.clone(), Mount::Mounted(did.clone()));
        self.spaces.insert(did, store);
        // Registered, so `drop` must not take the entry back out.
        self.held = false;
    }
}

impl<S> Drop for Claim<'_, S> {
    fn drop(&mut self) {
        if self.held {
            self.mounts.remove(&self.key);
        }
    }
}

fn location_key(location: &Location) -> String {
    format!("{:?}/{}", location.directory, location.name)
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Load> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<storage::Load>) -> Result<Credential, StorageError> {
        let location = Location::of(&input);
        let key = location_key(location);

        // Return existing credential if this location is already mounted
        if let Some(did) = self.lookup(&key)
            && let Some(store) = self.spaces.get(&did)
        {
            return did!("local:storage")
                .credential()
                .key(credential::SELF)
                .load()
                .perform(&store)
                .await
                .map_err(|e| StorageError::NotFound(e.to_string()));
        }

        // `load`, not `open`: a `storage::Load` of a space that was never
        // created must fail without materializing its backing store (an
        // `open` here would, on IndexedDB, create the database into being).
        let store = S::load(location)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        let cred: Credential = did!("local:storage")
            .credential()
            .key(credential::SELF)
            .load()
            .perform(&store)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        let did = cred.did();
        self.register(did, key, store);
        Ok(cred)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Create> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<storage::Create>,
    ) -> Result<Credential, StorageError> {
        let location = Location::of(&input);
        let cred = storage::Create::of(&input).credential.clone();
        let key = location_key(location);

        // Claim the location before the first `await`. This both rejects a
        // create of an already-mounted location and shuts out a concurrent
        // create of the same one, which would otherwise pass these checks
        // during the `open`/`save` window below and register a second DID.
        // Every early return from here releases the claim when `claim` drops.
        let claim = self.claim(&key)?;

        // Check if this DID is already mounted
        let did = cred.did();
        if self.spaces.contains(&did) {
            return Err(StorageError::AlreadyExists(format!("{did}")));
        }

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        did!("local:storage")
            .credential()
            .key(credential::SELF)
            .save(cred.clone())
            .perform(&store)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        claim.mount(did, store);
        Ok(cred)
    }
}
