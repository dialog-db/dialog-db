//! UCAN bridge types and authorization utilities.
//!
//! When the `ucan` feature is enabled this module provides:
//!
//! - IPLD parameter collection from capability chains
//! - [`Ucan`] authorization format producing signed UCAN invocations
//! - [`UcanInvocation`] signed UCAN invocation (the authorization proof)
//! - [`Issuer`] adapter bridging credential effects to UCAN signing
//! - [`authorize`](claim::claim) builds a UCAN invocation from a capability and delegation chain

mod access;
mod builder;
pub mod claim;
pub mod delegation;
mod invocation;
pub mod issuer;
mod parameters;
mod scope;

pub use builder::{DelegateRequest, InvokeRequest, IssuerUnset};
pub use claim::find_chain;
pub use delegation::import_delegation_chain;
pub use invocation::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};

use crate::{Ability, Capability, Effect};

/// UCAN authorization format producing signed invocation chains.
///
/// Implements [`Protocol`](crate::access::Protocol) for environments
/// that provide identity, signing, and storage effects.
///
/// Also provides builder APIs for delegations and invocations:
/// - [`Ucan::delegate()`] to build and sign a delegation chain
/// - [`Ucan::invoke()`] to build and sign an invocation chain
pub struct Ucan;

impl Ucan {
    /// Start building a delegation for the given capability.
    pub fn delegate(capability: &impl Ability) -> DelegateRequest<IssuerUnset> {
        DelegateRequest::new(capability)
    }

    /// Start building a signed invocation for an effect capability.
    ///
    /// Projects the effect through [`Claim`](crate::Claim) so that
    /// payload fields are replaced with checksums in the invocation arguments.
    pub fn invoke<Fx>(capability: &Capability<Fx>) -> InvokeRequest<IssuerUnset>
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        InvokeRequest::new(capability)
    }
}
