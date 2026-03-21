//! Site trait for declaring remote execution targets.
//!
//! A [`Site`] is a marker trait that declares what credential type,
//! authorization format, and address type are needed for a target location.
//! No methods — all execution logic lives in [`Fork`](crate::fork::Fork)
//! and [`Provider`](crate::Provider) impls.

use crate::credential::{Addressable, Allow, AuthorizationFormat};
use dialog_common::ConditionalSend;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Associates an address type with its corresponding site.
///
/// This trait allows inferring the site type from an address type,
/// enabling ergonomic `.fork(address)` calls without explicit site type parameters.
pub trait SiteAddress: Clone + ConditionalSend + 'static {
    /// The site type this address belongs to.
    type Site: Site<Address = Self>;
}

/// Pure site marker — declares types needed for remote execution.
///
/// No methods. Configuration (address, credentials) is carried by
/// [`ForkInvocation`](crate::fork::ForkInvocation) at execution time.
pub trait Site: Clone + ConditionalSend + 'static {
    /// The credential type needed to execute at this site.
    type Credentials: Serialize + DeserializeOwned + ConditionalSend + 'static;

    /// The authorization format this site requires.
    ///
    /// - `Allow` for sites that just need permission (S3, Local)
    /// - UCAN format for sites that need a signed invocation chain
    type Format: AuthorizationFormat;

    /// The address type for this site (used for credential lookup).
    type Address: Addressable<Self::Credentials> + Clone + ConditionalSend + 'static;
}

/// Local site — no remote backend needed.
///
/// Used for operations that execute directly without remote authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, serde::Deserialize)]
pub struct Local;

impl Site for Local {
    type Credentials = ();
    type Format = Allow;
    type Address = ();
}
