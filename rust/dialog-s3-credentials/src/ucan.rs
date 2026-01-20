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

mod credentials;
mod provider;

pub use credentials::{
    Credentials, CredentialsBuilder, DelegationChain, IntoUcanArgs, OperatorIdentity,
    generate_signer,
};
pub use provider::{InvocationChain, UcanAuthorizer};
