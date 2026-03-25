//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Authority` — identity and signing (identify, sign)
//! - `Local` — storage effects (archive, memory, credentials)
//! - `Remote` — remote invocations (S3, UCAN, etc.)
//!
//! Authority and local effects are routed via `#[derive(Provider)]`.
//! Authorization is handled by blanket impls in `dialog-capability`.
//! Credential store effects are routed to the local storage provider.
//! Remote invocations (`Fork<S, Fx>`) are routed to `Remote`.

use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::authority;
use dialog_capability::credential::{self, CredentialError, List, Retrieve, Save};
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::site::Site;
use dialog_capability::{Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::{archive, memory, storage};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Generic environment that delegates:
/// - Authority effects (identify, sign) to `Authority`
/// - Storage effects to `Local`
/// - Remote invocations to `Remote`
#[derive(Provider)]
pub struct Environment<Authority, Local, Remote> {
    #[provide(authority::Identify, authority::Sign)]
    /// Provider for authority effects (identity + signing).
    pub authority: Authority,
    #[provide(
        archive::Get,
        archive::Put,
        memory::Resolve,
        memory::Publish,
        memory::Retract,
        storage::Get,
        storage::Set,
        storage::Delete,
        storage::List
    )]
    /// Provider for local storage effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

impl<Authority, Local, Remote> Environment<Authority, Local, Remote> {
    /// Create a new environment.
    pub fn new(authority: Authority, local: Local, remote: Remote) -> Self {
        Self {
            local,
            authority,
            remote,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Authority, Local, Remote, C> Provider<Retrieve<C>> for Environment<Authority, Local, Remote>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
    Capability<Retrieve<C>>: ConditionalSend,
    Retrieve<C>: ConditionalSend + 'static,
    Local: Provider<Retrieve<C>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Retrieve<C>>) -> Result<C, CredentialError> {
        self.local.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Authority, Local, Remote, C> Provider<Save<C>> for Environment<Authority, Local, Remote>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
    Capability<Save<C>>: ConditionalSend,
    Save<C>: ConditionalSend + 'static,
    Local: Provider<Save<C>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Save<C>>) -> Result<(), CredentialError> {
        self.local.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Authority, Local, Remote, C> Provider<List<C>> for Environment<Authority, Local, Remote>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
    Capability<List<C>>: ConditionalSend,
    List<C>: ConditionalSend + 'static,
    Local: Provider<List<C>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<List<C>>,
    ) -> Result<Vec<credential::Address<C>>, CredentialError> {
        self.local.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Authority, Local, Remote, S, Fx> Provider<Fork<S, Fx>>
    for Environment<Authority, Local, Remote>
where
    S: Site,
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    ForkInvocation<S, Fx>: ConditionalSend,
    Remote: Provider<Fork<S, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: ForkInvocation<S, Fx>) -> Fx::Output {
        self.remote.execute(input).await
    }
}
