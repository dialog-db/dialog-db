//! UCAN bridge types and authorization utilities.
//!
//! When the `ucan` feature is enabled this module provides:
//!
//! - [`Scope`] and [`Parameters`] for IPLD parameter collection
//! - [`Issuer`] adapter bridging credential effects to UCAN signing
//! - [`UcanInvocation`] wrapper for signed UCAN invocation chains

mod invocation;
pub mod issuer;
mod parameters;
mod scope;

pub use invocation::UcanInvocation;
pub use issuer::Issuer;
pub use parameters::{parameters, parameters_to_args, parameters_to_policy};
pub use scope::{Args, Parameters, Scope};
