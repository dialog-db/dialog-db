//! Concrete environment composition for the repository layer.
//!
//! Uses [`Environment<Local, Remote, Credentials>`] to compose local storage,
//! remote network provider, and credential capabilities. The [`Network`]
//! router handles unified address dispatch via its generated [`NetworkAddress`]
//! enum.

#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::Volatile;
#[cfg(any(test, feature = "helpers"))]
use dialog_storage::provider::network::emulator::Route;
use dialog_storage::provider::{FileSystem, Network};

pub use dialog_effects::environment::Environment;
pub use dialog_storage::provider::network::NetworkAddress;

use crate::repository::credentials::Credentials;

/// Concrete address type for remote operations.
pub type RemoteAddress = NetworkAddress;

/// Native environment: filesystem local storage with network remote
/// and operator credentials.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnvironment = Environment<FileSystem, Network, Credentials>;

/// Web environment: IndexedDB local storage with network remote
/// and operator credentials.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type WebEnvironment = Environment<dialog_storage::provider::IndexedDb, Network, Credentials>;

/// Test environment: in-memory local storage with emulated remote keyed by
/// the generated [`NetworkAddress`].
///
/// Uses `()` for credentials by default. For tests that need credential
/// effects, use `Environment<Volatile, Route<RemoteAddress>, Credentials>`.
#[cfg(any(test, feature = "helpers"))]
pub type TestEnvironment = Environment<Volatile, Route<RemoteAddress>>;
