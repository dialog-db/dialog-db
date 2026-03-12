//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] pairs a local storage provider with a remote provider,
//! implementing [`Provider<Fx>`] for local effects and
//! [`Provider<RemoteInvocation<Fx, Address>>`] for remote effects via two
//! non-overlapping blanket impls.
//!
//! The remote provider's [`ProviderRoute::Address`] determines which address
//! type the environment can dispatch to.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::remote::RemoteInvocation;

/// Generic environment that delegates local effects to `Local` and
/// remote effects to `Remote`.
///
/// The remote address type is derived from `Remote`'s [`ProviderRoute`]
/// implementation.
pub struct Environment<Local, Remote: ProviderRoute> {
    /// Provider for local effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

impl<Local, Remote: ProviderRoute> Environment<Local, Remote> {
    /// Create a new environment from local and remote providers.
    pub fn new(local: Local, remote: Remote) -> Self {
        Self { local, remote }
    }
}

/// Blanket local dispatch: any [`Effect`] delegates to `self.local`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Local, Remote, Fx> Provider<Fx> for Environment<Local, Remote>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Local: Provider<Fx> + ConditionalSync,
    Remote: ProviderRoute + ConditionalSync,
{
    async fn execute(&self, input: Capability<Fx>) -> Fx::Output {
        self.local.execute(input).await
    }
}

/// Blanket remote dispatch: [`RemoteInvocation<Fx, Address>`] delegates
/// to `self.remote`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Local, Remote, Fx> Provider<RemoteInvocation<Fx, Remote::Address>>
    for Environment<Local, Remote>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Local: ConditionalSync,
    Remote: ProviderRoute + Provider<RemoteInvocation<Fx, Remote::Address>> + ConditionalSync,
    Remote::Address: ConditionalSend + 'static,
{
    async fn execute(&self, input: RemoteInvocation<Fx, Remote::Address>) -> Fx::Output {
        self.remote.execute(input).await
    }
}
