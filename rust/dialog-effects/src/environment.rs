//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Local` — storage effects (archive, memory)
//! - `Credentials` — credential effects (identify, sign, authorize)
//! - `Remote` — remote invocations (S3, UCAN, etc.)
//!
//! Local and credential effects are routed via `#[derive(Provider)]`.
//! [`Authorize`] and credential store effects are routed to `Credentials`
//! via blanket impls. Remote invocations (`Fork<S, Fx>`) are routed to
//! `Remote` via blanket impls defined alongside each site type.
//!
//! [`Authorize`]: dialog_capability::credential::Authorize

use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::credential::{
    Authorization, AuthorizationFormat, Authorize, AuthorizeError, CredentialError, Get, Import,
    Set,
};
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
        crate::memory::Retract
    )]
    /// Provider for local storage effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

// Route Authorize<Fx, F> to self.credentials.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, Fx, F> Provider<Authorize<Fx, F>>
    for Environment<Local, Credentials, Remote>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    F: AuthorizationFormat,
    Capability<Fx>: dialog_common::ConditionalSend,
    Authorize<Fx, F>: dialog_common::ConditionalSend + 'static,
    Credentials: Provider<Authorize<Fx, F>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Fx, F>>,
    ) -> Result<Authorization<Fx, F>, AuthorizeError> {
        self.credentials.execute(input).await
    }
}

// Route Get<C> to self.credentials for any credential type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, C> Provider<Get<C>> for Environment<Local, Credentials, Remote>
where
    C: serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<Get<C>>: dialog_common::ConditionalSend,
    Get<C>: dialog_common::ConditionalSend + 'static,
    Credentials: Provider<Get<C>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<Get<C>>) -> Result<C, CredentialError> {
        self.credentials.execute(input).await
    }
}

// Route Set<C> to self.credentials for any credential type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, C> Provider<Set<C>> for Environment<Local, Credentials, Remote>
where
    C: serde::Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    Capability<Set<C>>: dialog_common::ConditionalSend,
    Set<C>: dialog_common::ConditionalSend + 'static,
    Credentials: Provider<Set<C>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<Set<C>>) -> Result<(), CredentialError> {
        self.credentials.execute(input).await
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
