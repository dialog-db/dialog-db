//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Local` — storage effects (archive, memory)
//! - `Remote` — remote invocations via [`RemoteInvocation`]
//! - `Credentials` — credential effects (identify, sign)
//!
//! Storage and credential effects are routed via `#[derive(Provider)]`.
//! Remote effects use a manual blanket impl keyed on [`RemoteInvocation`].

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::remote::RemoteInvocation;

/// Generic environment that delegates:
/// - Storage effects to `Local`
/// - Remote invocations to `Remote`
/// - Credential effects to `Credentials`
#[derive(Provider)]
pub struct Environment<Local, Remote: ProviderRoute, Credentials = ()> {
    #[provide(crate::credential::Identify, crate::credential::Sign)]
    /// Provider for credential effects.
    pub credentials: Credentials,
    #[provide(
        crate::archive::Get,
        crate::archive::Put,
        crate::memory::Resolve,
        crate::memory::Publish,
        crate::memory::Retract
    )]
    /// Provider for local storage effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

impl<Local, Remote: ProviderRoute> Environment<Local, Remote> {
    /// Create a new environment from local and remote providers (no credentials).
    pub fn new(local: Local, remote: Remote) -> Self {
        Self {
            local,
            remote,
            credentials: (),
        }
    }
}

impl<Local, Remote: ProviderRoute, Credentials> Environment<Local, Remote, Credentials> {
    /// Create a new environment from local, remote, and credential providers.
    pub fn with_credentials(local: Local, remote: Remote, credentials: Credentials) -> Self {
        Self {
            local,
            remote,
            credentials,
        }
    }
}

/// Blanket remote dispatch: [`RemoteInvocation<Fx, Address>`] delegates
/// to `self.remote`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Local, Remote, Credentials, Fx> Provider<RemoteInvocation<Fx, Remote::Address>>
    for Environment<Local, Remote, Credentials>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Local: ConditionalSync,
    Credentials: ConditionalSync,
    Remote: ProviderRoute + Provider<RemoteInvocation<Fx, Remote::Address>> + ConditionalSync,
    Remote::Address: ConditionalSend + 'static,
{
    async fn execute(&self, input: RemoteInvocation<Fx, Remote::Address>) -> Fx::Output {
        self.remote.execute(input).await
    }
}
