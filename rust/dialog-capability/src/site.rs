//! Site trait for declaring remote execution targets.
//!
//! A [`Site`] is a marker trait that declares what authorization format
//! and address type are needed for a target location.
//!
//! No methods — all execution logic lives in [`Fork`](crate::fork::Fork)
//! and [`Provider`](crate::Provider) impls.

use crate::access::Protocol;
use dialog_common::ConditionalSend;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Associates an address type with its corresponding site.
///
/// This trait allows inferring the site type from an address type,
/// enabling ergonomic `.fork(address)` calls without explicit site type parameters.
pub trait SiteAddress: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static {
    /// The site type this address belongs to.
    type Site: Site<Address = Self>;
}

/// Pure site marker — declares types needed for remote execution.
///
/// No methods. Configuration (address) is carried by
/// [`Fork`](crate::fork::Fork) at execution time.
///
/// Credentials are the address's concern — e.g. S3 `Address` carries
/// `Option<S3Credentials>` directly.
pub trait Site: Clone + ConditionalSend + 'static {
    /// The access protocol used by this site.
    type Protocol: Protocol;

    /// The address type for this site (serializable for storage/transport).
    type Address: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static;
}
