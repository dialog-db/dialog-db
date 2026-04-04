//! UCAN authorization protocol implementation.
//!
//! Provides the [`Ucan`] protocol type, proof chain types, and the
//! invocation builder. Delegation is handled via
//! [`profile.access().claim().delegate()`](dialog_operator::profile::access).

mod access;
mod builder;
mod invocation;
pub mod issuer;
pub mod parameters;
pub mod scope;

pub use access::{UcanAuthorization, UcanPermit, UcanPermit as UcanProofChain, UcanProof};
pub use builder::{InvokeRequest, IssuerUnset};
pub use invocation::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};

use dialog_capability::{Ability, Capability, Effect};

/// UCAN authorization format.
///
/// Implements [`Protocol`](dialog_capability::access::Protocol) for UCAN-based
/// authorization. Provides [`Ucan::invoke()`] for building signed invocations.
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
