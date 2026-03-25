//! Concrete environment composition for the repository layer.
//!
//! Use [`open`] to bootstrap a fully-configured environment from a
//! [`Profile`](crate::Profile) descriptor.

mod error;
mod provider;

pub use provider::Environment;

use crate::remote::Remote;

pub use error::OpenError;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web::*;

/// The platform-specific environment type.
///
/// On native: `Environment<Credentials, FileSystem, Remote>`
/// On web: `Environment<Credentials, IndexedDb, Remote>`
#[cfg(not(target_arch = "wasm32"))]
pub type DialogEnvironment = NativeEnvironment;

/// The platform-specific environment type.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type DialogEnvironment = WebEnvironment;

#[cfg(any(test, feature = "helpers"))]
mod test;
#[cfg(any(test, feature = "helpers"))]
pub use test::*;
