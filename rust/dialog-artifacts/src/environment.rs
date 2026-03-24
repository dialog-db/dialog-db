//! Concrete environment composition for the repository layer.
//!
//! Use [`open`] to bootstrap a fully-configured environment from a
//! [`Profile`](crate::Profile) descriptor.

pub use dialog_effects::environment::Environment;

mod error;

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

#[cfg(any(test, feature = "helpers"))]
mod test;
#[cfg(any(test, feature = "helpers"))]
pub use test::*;
