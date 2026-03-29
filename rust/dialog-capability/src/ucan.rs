//! UCAN bridge types and authorization utilities.
//!
//! When the `ucan` feature is enabled this module provides:
//!
//! - IPLD parameter collection from capability chains
//! - [`Ucan`] — authorization format producing signed UCAN invocations
//! - [`UcanInvocation`] — a signed UCAN invocation (the authorization proof)
//! - [`Issuer`] — adapts credential effects to UCAN's Signer interface
//! - [`authorize`] — builds a UCAN invocation from a capability and delegation chain

mod access;
pub mod claim;
pub mod delegation;
mod invocation;
pub mod issuer;
mod parameters;
mod scope;

pub use claim::{claim, find_chain};
pub use delegation::import_delegation_chain;
pub use invocation::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};

/// UCAN authorization format — produces a signed invocation chain.
///
/// Implements [`Protocol`](crate::access::Protocol) for environments
/// that provide identity, signing, and storage effects. Produces
/// [`UcanInvocation`] as the authorization proof.
pub struct Ucan;
