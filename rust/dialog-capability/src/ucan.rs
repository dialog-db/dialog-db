//! UCAN bridge types and authorization utilities.
//!
//! When the `ucan` feature is enabled this module provides:
//!
//! - IPLD parameter collection from capability chains
//! - [`Ucan`] — authorization format producing signed UCAN invocations
//! - [`UcanInvocation`] — a signed UCAN invocation (the authorization proof)
//! - [`Issuer`] — adapts credential effects to UCAN's Signer interface
//! - [`authorize`] — builds a UCAN invocation from a capability and delegation chain

mod authorize;
pub mod claim;
pub mod delegation;
mod invocation;
pub mod issuer;
mod parameters;

pub use authorize::authorize;
pub use claim::claim;
pub use delegation::import_delegation_chain;
pub use invocation::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{Args, Parameters, parameters, parameters_to_args};

use crate::Constraint;
use crate::credential::AuthorizationFormat;

/// UCAN authorization format — produces a signed invocation chain.
///
/// Used with [`credential::Authorize<Fx, Ucan>`](crate::credential::Authorize)
/// to produce [`UcanInvocation`] as the authorization proof.
pub struct Ucan;

impl AuthorizationFormat for Ucan {
    type Authorization<Fx: Constraint> = UcanInvocation;
}
