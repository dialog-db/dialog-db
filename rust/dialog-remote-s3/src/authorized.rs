//! Authorized operation ready for HTTP execution.
//!
//! [`Authorized<Fx>`] pairs a [`Permit`] (presigned HTTP request) with a
//! [`Capability<Fx>`] (the typed operation). `Provider<Authorized<Fx>>` impls
//! on [`Http`](crate::s3::Http) execute the HTTP call and interpret the response.
//!
//! Both direct-S3 (`Provider<Fork<S3, Fx>>`) and UCAN (`Provider<Fork<UcanSite, Fx>>`)
//! paths produce an `Authorized<Fx>` after their respective authorization step,
//! then delegate to the shared `Http` execution layer.

use crate::permit::Permit;
use dialog_capability::command::Command;
use dialog_capability::{Capability, Constraint, Effect};

/// A pre-authorized operation ready for HTTP execution.
///
/// Combines a [`Permit`] (the presigned HTTP request) with the
/// [`Capability<Fx>`] (carrying the typed effect parameters).
pub struct Authorized<Fx: Effect> {
    /// The presigned HTTP request (URL + method + headers).
    pub permit: Permit,
    /// The capability with effect-specific parameters.
    pub capability: Capability<Fx>,
}

impl<Fx: Effect> Authorized<Fx>
where
    Fx::Of: Constraint,
{
    /// Create a new authorized operation.
    pub fn new(permit: Permit, capability: Capability<Fx>) -> Self {
        Self { permit, capability }
    }
}

impl<Fx: Effect> Command for Authorized<Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Fx::Output;
}
