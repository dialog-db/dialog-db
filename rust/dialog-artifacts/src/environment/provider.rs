//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Credentials` — credential effects (identify, sign, authorize)
//! - `Local` — storage effects (archive, memory)
//! - `Remote` — remote invocations (S3, UCAN, etc.)
//!
//! Local and credential effects are routed via `#[derive(Provider)]`.
//! Authorization is handled by blanket impls in `dialog-capability`.
//! Credential store effects are routed to the local storage provider.
//! Remote invocations (`Fork<S, Fx>`) are routed to `Remote`.

use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::credential::{self, CredentialError, Import, List, Retrieve, Save};
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::site::Site;
use dialog_capability::{Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::{archive, memory, storage};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Generic environment that delegates:
/// - Credential effects to `Credentials`
/// - Storage effects to `Local`
/// - Remote invocations to `Remote`
#[derive(Provider)]
pub struct Environment<Credentials, Local, Remote> {
    #[provide(credential::Identify, credential::Sign)]
    /// Provider for credential effects.
    pub credentials: Credentials,
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

impl<Credentials, Local, Remote> Environment<Credentials, Local, Remote> {
    /// Create a new environment from credential, local storage, and remote providers.
    pub fn new(credentials: Credentials, local: Local, remote: Remote) -> Self {
        Self {
            local,
            credentials,
            remote,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Credentials, Local, Remote, C> Provider<Retrieve<C>>
    for Environment<Credentials, Local, Remote>
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
impl<Credentials, Local, Remote, C> Provider<Save<C>> for Environment<Credentials, Local, Remote>
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
impl<Credentials, Local, Remote, C> Provider<List<C>> for Environment<Credentials, Local, Remote>
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
impl<Credentials, Local, Remote, Material> Provider<Import<Material>>
    for Environment<Credentials, Local, Remote>
where
    Material: Serialize + DeserializeOwned + ConditionalSend + 'static,
    Capability<Import<Material>>: ConditionalSend,
    Credentials: Provider<Import<Material>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Import<Material>>) -> Result<(), CredentialError> {
        self.credentials.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Credentials, Local, Remote, S, Fx> Provider<Fork<S, Fx>>
    for Environment<Credentials, Local, Remote>
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
