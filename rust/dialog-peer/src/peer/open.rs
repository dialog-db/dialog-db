//! [`OpenPeer`]: a root peer over the credential at a location.
//!
//! The common case composed once: the credential is opened from the space
//! at a location, through [`OpenCredential`], and the peer opened over it
//! with that space as its home. The peer itself is not coupled to where
//! its credential lives; this is the convenience for when the two share
//! one space, which every root peer today does.

use dialog_capability::Provider;
use dialog_effects::storage::{self as storage_fx, Directory, Location};
use dialog_identity::OpenCredential;
use dialog_network::Network;
use dialog_storage::provider::storage::Storage;
use dialog_varsig::Principal as _;

use super::{Peer, PeerError, PeerSpace, Runtime};

enum Mode {
    Open,
    Load,
    Create,
}

/// Opens a root peer over the credential at a location.
///
/// `open` loads the credential or generates and persists one; `load`
/// fails when none is there; `create` fails when one is.
pub struct OpenPeer {
    location: Location,
    mode: Mode,
    network: Network,
    runtime: Runtime,
    base: Option<Directory>,
    branch: Option<String>,
}

impl OpenPeer {
    /// Open the peer at `location`: load its credential, or generate and
    /// persist one if none is there.
    pub fn open(location: Location) -> Self {
        Self::new(location, Mode::Open)
    }

    /// Load the peer at `location`, failing if no credential is there.
    pub fn load(location: Location) -> Self {
        Self::new(location, Mode::Load)
    }

    /// Create the peer at `location`, failing if a credential is already
    /// there.
    pub fn create(location: Location) -> Self {
        Self::new(location, Mode::Create)
    }

    fn new(location: Location, mode: Mode) -> Self {
        Self {
            location,
            mode,
            network: Network::default(),
            runtime: Runtime::default(),
            base: None,
            branch: None,
        }
    }

    /// The network fork invocations dispatch through.
    pub fn network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    /// The runtime the peer performs through.
    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = runtime;
        self
    }

    /// The directory space names resolve against. Defaults to the
    /// location's directory.
    pub fn base(mut self, directory: Directory) -> Self {
        self.base = Some(directory);
        self
    }

    /// The state branch. Defaults to `main`.
    pub fn branch(mut self, name: impl Into<String>) -> Self {
        self.branch = Some(name.into());
        self
    }

    /// Open the credential in `storage` and the peer over it.
    pub async fn perform<S>(self, storage: &Storage<S>) -> Result<Peer<S>, PeerError>
    where
        S: PeerSpace,
        Storage<S>: Provider<storage_fx::Load> + Provider<storage_fx::Create>,
    {
        let name = self.location.name.clone();
        let command = match self.mode {
            Mode::Open => OpenCredential::open(name),
            Mode::Load => OpenCredential::load(name),
            Mode::Create => OpenCredential::create(name),
        };
        let credential = command
            .at(self.location.directory.clone())
            .perform(storage)
            .await
            .map_err(|error| PeerError::Open(error.to_string()))?;

        let mut builder = Peer::open(credential.did())
            .credential(credential)
            .storage(storage.clone())
            .network(self.network)
            .runtime(self.runtime)
            .base(self.base.unwrap_or(self.location.directory));
        if let Some(branch) = self.branch {
            builder = builder.branch(branch);
        }
        builder.await
    }
}
