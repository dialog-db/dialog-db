//! Concrete environment composition for the repository layer.
//!
//! Uses [`Environment<Local, Remote>`] to compose local storage with a
//! remote network provider. The [`Network`] router handles unified address
//! dispatch via its generated [`NetworkAddress`] enum.

#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::Volatile;
#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::network::emulator::Route;
use dialog_storage::provider::{FileSystem, Network};

pub use dialog_effects::environment::Environment;
pub use dialog_storage::provider::network::NetworkAddress;

use crate::repository::credentials::Credentials;

/// Concrete address type for remote operations.
pub type RemoteAddress = NetworkAddress<Credentials>;

/// Native environment: filesystem local storage with network remote.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnvironment<Issuer> = Environment<FileSystem, Network<Issuer>>;

/// Web environment: IndexedDB local storage with network remote.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type WebEnvironment<Issuer> = Environment<dialog_storage::provider::IndexedDb, Network<Issuer>>;

/// Test environment: in-memory local storage with emulated remote keyed by
/// the generated [`NetworkAddress`].
#[cfg(any(test, feature = "helpers"))]
pub type TestEnvironment = Environment<Volatile, Route<RemoteAddress>>;
