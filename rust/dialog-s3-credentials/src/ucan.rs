//! UCAN-based authorization.
//!
//! This module provides UCAN (User Controlled Authorization Networks) support:
//!
//! ## Client-side (making requests)
//!
//! - [`Credentials`] - Credentials that delegate to an external access service
//! - [`OperatorIdentity`] - Operator identity for signing invocations
//! - [`DelegationChain`] - Chain of delegations proving authority
//!
//! ## Server-side (handling requests)
//!
//! - [`UcanAuthorizer`] - Wraps credentials to handle UCAN invocations and authorize requests
//! - [`InvocationChain`] - Parsed UCAN container with invocation and delegation chain

mod authorization;
mod container;
mod credentials;
pub mod delegation;
mod invocation;
mod provider;

pub use authorization::UcanAuthorization;
pub use container::Container;
pub use credentials::Credentials;
pub use delegation::DelegationChain;
pub use invocation::InvocationChain;
pub use provider::UcanAuthorizer;

/// Test helpers for creating UCAN delegations.
/// Only available with the `helpers` feature.
#[cfg(feature = "helpers")]
pub mod test_helpers {
    pub use super::delegation::helpers::{create_delegation, generate_signer};
}
