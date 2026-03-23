//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Local` — storage effects (archive, memory)
//! - `Credentials` — credential effects (identify, sign, authorize)
//! - `Remote` — remote invocations (S3, UCAN, etc.)
//!
//! Local and credential effects are routed via `#[derive(Provider)]`.
//! Authorization is handled by blanket impls in `dialog-capability`.
//! Credential store effects are routed to the appropriate provider.
//! Remote invocations (`Fork<S, Fx>`) are routed to `Remote`.

use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::credential::{CredentialError, Import, List, Retrieve, Save};
use dialog_capability::{Constraint, Effect};

/// Generic environment that delegates:
/// - Storage effects to `Local`
/// - Credential effects (including authorization) to `Credentials`
/// - Remote invocations to `Remote`
#[derive(Provider)]
pub struct Environment<Local, Credentials = (), Remote = ()> {
    #[provide(crate::credential::Identify, crate::credential::Sign)]
    /// Provider for credential effects.
    pub credentials: Credentials,
    #[provide(
        crate::archive::Get,
        crate::archive::Put,
        crate::memory::Resolve,
        crate::memory::Publish,
        crate::memory::Retract,
        crate::storage::Get,
        crate::storage::Set,
        crate::storage::Delete,
        crate::storage::List
    )]
    /// Provider for local storage effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

// Route Retrieve<C> to self.local for any credential type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, C> Provider<Retrieve<C>>
    for Environment<Local, Credentials, Remote>
where
    C: serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<Retrieve<C>>: dialog_common::ConditionalSend,
    Retrieve<C>: dialog_common::ConditionalSend + 'static,
    Local: Provider<Retrieve<C>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<Retrieve<C>>) -> Result<C, CredentialError> {
        self.local.execute(input).await
    }
}

// Route Save<C> to self.local for any credential type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, C> Provider<Save<C>> for Environment<Local, Credentials, Remote>
where
    C: serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<Save<C>>: dialog_common::ConditionalSend,
    Save<C>: dialog_common::ConditionalSend + 'static,
    Local: Provider<Save<C>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<Save<C>>) -> Result<(), CredentialError> {
        self.local.execute(input).await
    }
}

// Route List<C> to self.local for any credential type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, C> Provider<List<C>> for Environment<Local, Credentials, Remote>
where
    C: serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<List<C>>: dialog_common::ConditionalSend,
    List<C>: dialog_common::ConditionalSend + 'static,
    Local: Provider<List<C>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<List<C>>,
    ) -> Result<Vec<dialog_capability::credential::Address<C>>, CredentialError> {
        self.local.execute(input).await
    }
}

// Route Import<Material> to self.credentials for any material type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, Material> Provider<Import<Material>>
    for Environment<Local, Credentials, Remote>
where
    Material:
        serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<Import<Material>>: dialog_common::ConditionalSend,
    Credentials: Provider<Import<Material>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<Import<Material>>) -> Result<(), CredentialError> {
        self.credentials.execute(input).await
    }
}

impl<Local> Environment<Local> {
    /// Create a new environment from a local provider (no credentials).
    pub fn new(local: Local) -> Self {
        Self {
            local,
            credentials: (),
            remote: (),
        }
    }
}

impl<Local, Credentials> Environment<Local, Credentials> {
    /// Create a new environment from local and credential providers.
    pub fn with_credentials(local: Local, credentials: Credentials) -> Self {
        Self {
            local,
            credentials,
            remote: (),
        }
    }
}

impl<Local, Credentials, Remote> Environment<Local, Credentials, Remote> {
    /// Create a new environment from local, credential, and remote providers.
    pub fn with_remote(local: Local, credentials: Credentials, remote: Remote) -> Self {
        Self {
            local,
            credentials,
            remote,
        }
    }
}

// Route Fork<S, Fx> to Environment's remote field for all site types.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, S, Fx> Provider<dialog_capability::fork::Fork<S, Fx>>
    for Environment<Local, Credentials, Remote>
where
    S: dialog_capability::site::Site,
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    dialog_capability::fork::ForkInvocation<S, Fx>: dialog_common::ConditionalSend,
    Remote: Provider<dialog_capability::fork::Fork<S, Fx>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: dialog_capability::fork::ForkInvocation<S, Fx>) -> Fx::Output {
        self.remote.execute(input).await
    }
}
