//! Operator — an operating environment built from a Profile.
//!
//! The Operator is a type alias for `Environment<Credentials, Compositor, Remote>`.
//! Build one via `Profile::operator()`.

mod builder;
#[cfg(test)]
mod test;

pub use builder::{NetworkBuilder, OperatorBuilder, OperatorError};

use crate::Credentials;
use crate::environment::Environment;
use crate::remote::Remote;
use dialog_storage::provider::Compositor;

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes authority credentials, a [`Compositor`] for DID-routed storage,
/// and a remote provider.
pub type Operator = Environment<Credentials, Compositor, Remote>;
