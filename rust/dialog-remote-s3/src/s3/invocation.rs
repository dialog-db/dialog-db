//! S3 invocation ready for HTTP execution.
//!
//! [`S3Invocation<Fx>`] pairs a [`Permit`] (presigned HTTP request) with a
//! [`Capability<Fx>`] (the typed operation). The site provider builds this
//! after authorizing via [`S3Authorization::grant`](super::S3Authorization::grant),
//! then executes the HTTP request.

use crate::Permit;
use dialog_capability::Command;
use dialog_capability::{Capability, Constraint, Effect, Provider};

/// A pre-authorized S3 operation ready for HTTP execution.
///
/// Combines a [`Permit`] (the presigned HTTP request) with the
/// [`Capability<Fx>`] (carrying the typed effect parameters).
pub struct S3Invocation<Fx: Effect> {
    /// The capability with effect-specific parameters.
    pub capability: Capability<Fx>,
    /// The presigned HTTP request (URL + method + headers).
    pub permit: Permit,
}

impl<Fx: Effect> S3Invocation<Fx>
where
    Fx::Of: Constraint,
{
    /// Create a new S3 invocation.
    pub fn new(permit: Permit, capability: Capability<Fx>) -> Self {
        Self { permit, capability }
    }

    /// Execute this invocation against a provider.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: Provider<Self>,
    {
        env.execute(self).await
    }
}

impl<Fx: Effect> Command for S3Invocation<Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Fx::Output;
}
