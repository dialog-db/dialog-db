//! UCAN authorization protocol implementation.
//!
//! Provides the [`Ucan`] protocol type and proof chain types.
//! Delegation is handled via
//! [`profile.access().claim().delegate()`](dialog_operator::profile::access).

mod access;
mod invocation;
pub mod parameters;
pub mod scope;

pub use access::{
    UcanAuthorization, UcanDelegation, UcanPermit, UcanPermit as UcanProofChain, UcanProof,
};
pub use invocation::UcanInvocation;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};

/// UCAN authorization protocol marker.
///
/// Implements [`Protocol`](dialog_capability::access::Protocol) for UCAN-based
/// authorization with Ed25519 signatures.
pub struct Ucan;
