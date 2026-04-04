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
mod builder;
pub mod claim;
pub mod delegation;
pub mod issuer;
pub mod parameters;
pub mod scope;

pub use access::{UcanAuthorization, UcanPermit as UcanProofChain, UcanPermit, UcanProof};
pub use builder::{InvokeRequest, IssuerUnset};
pub use claim::{claim, find_chain};
pub use delegation::import_delegation_chain;
pub use dialog_capability::ucan::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};

use dialog_capability::{Ability, Capability, Effect};

/// UCAN authorization format — produces a signed invocation chain.
///
/// Implements [`Protocol`](dialog_capability::access::Protocol) for environments
/// that provide identity, signing, and storage effects. Produces
/// [`UcanInvocation`] as the authorization proof.
///
/// Provides [`Ucan::invoke()`] for building signed invocations.
/// For delegations, use [`profile.access().claim(&cap).delegate(&audience)`](dialog_operator::profile::access).
pub struct Ucan;

impl Ucan {
    /// Start building a signed invocation for an effect capability.
    ///
    /// Projects the effect through [`Claim`](dialog_capability::Claim) so that
    /// payload fields are replaced with checksums in the invocation arguments.
    pub fn invoke<Fx>(capability: &Capability<Fx>) -> InvokeRequest<IssuerUnset>
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        InvokeRequest::new(capability)
    }
}
