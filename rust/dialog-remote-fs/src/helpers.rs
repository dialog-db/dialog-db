//! Test helpers for FS-remote.
//!
//! [`FsNetwork`] drives `Fork<Fs, Fx>` through the real
//! [`SiteFork::authorize`](dialog_capability::SiteFork) path — including the
//! subject-verification against the directory's stored credential — without a
//! full Operator. On native `authorize` needs no environment (it opens the
//! directory straight from the `file:` URL address), so `FsNetwork` is a unit
//! env.

use async_trait::async_trait;
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Ability, Constraint, Effect, Fork, ForkInvocation, Provider, SiteFork};
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::fs::Fs;

/// Flatten an [`AuthorizeError`] into an effect's `Result` output, the way the
/// Operator's fork dispatch does (its equivalent helper is private).
trait FromAuthError {
    fn from_auth_error(error: AuthorizeError) -> Self;
}

impl<T, E: From<AuthorizeError>> FromAuthError for Result<T, E> {
    fn from_auth_error(error: AuthorizeError) -> Self {
        Err(E::from(error))
    }
}

/// Test environment for FS-remote fork execution. Authorizes each fork against
/// the directory its address names, then performs it.
#[derive(Clone, Copy, Default)]
pub struct FsNetwork;

impl FsNetwork {
    /// Create a test network.
    pub fn new() -> Self {
        Self
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx> Provider<Fork<Fs, Fx>> for FsNetwork
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Fx::Output: ConditionalSend + FromAuthError,
    Fork<Fs, Fx>: ConditionalSend,
    crate::fs::FsFork<Fx>: SiteFork<Self, Site = Fs, Effect = Fx> + ConditionalSend,
    ForkInvocation<Fs, Fx>: ConditionalSend,
    dialog_capability::Capability<Fx>: Ability,
    Fs: Provider<ForkInvocation<Fs, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, fork: Fork<Fs, Fx>) -> Fx::Output {
        let site_fork = crate::fs::FsFork::from(fork);
        match site_fork.authorize(self).await {
            Ok(invocation) => invocation.perform(&Fs).await,
            Err(error) => Fx::Output::from_auth_error(error),
        }
    }
}
