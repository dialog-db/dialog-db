//! FS invocation ready for execution.
//!
//! Mirrors [`dialog_remote_s3::S3Invocation`]. Pairs an [`FsPermit`] (handle
//! id + captured request) with a [`Capability<Fx>`] (the typed operation).
//! Produced by [`FsAuthorization::redeem`](super::FsAuthorization::redeem)
//! followed by [`FsPermit::invoke`](super::FsPermit::invoke).

use super::FsPermit;
use dialog_capability::Command;
use dialog_capability::{Capability, Constraint, Effect, Provider};

/// A pre-authorized FS operation ready for execution.
pub struct FsInvocation<Fx: Effect> {
    /// The capability with effect-specific parameters.
    pub capability: Capability<Fx>,
    /// The permit naming the registered handle and the captured request.
    pub permit: FsPermit,
}

impl<Fx: Effect> FsInvocation<Fx>
where
    Fx::Of: Constraint,
{
    /// Construct a new invocation.
    pub fn new(permit: FsPermit, capability: Capability<Fx>) -> Self {
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

impl<Fx: Effect> Command for FsInvocation<Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Fx::Output;
}
