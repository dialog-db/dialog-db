//! UCAN-based authorization for S3-compatible storage.
//!
//! This crate provides UCAN (User Controlled Authorization Networks) support:
//!
//! ## Client-side (making requests)
//!
//! - [`UcanAuthorization`] - Authorization material wrapping a signed UCAN chain
//! - [`UcanAddress`] - Access service endpoint
//!
//! ## Server-side (handling requests)
//!
//! - [`UcanAuthorizer`] - Verifies UCAN invocations and produces presigned URLs
//! - [`InvocationChain`] - Parsed UCAN container with invocation and delegation chain

mod authorizer;
mod provider;
pub mod site;

pub use authorizer::UcanAuthorizer;
pub use site::{Ucan, UcanAddress, UcanAuthorization, UcanClaim, UcanInvocation, UcanSite};

// Re-export container types from dialog-ucan
pub use dialog_ucan_core::{Container, ContainerError, DelegationChain, InvocationChain};

#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
