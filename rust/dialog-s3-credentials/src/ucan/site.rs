//! UCAN site configuration — pure config, no credentials.

use dialog_capability::access::Access;
use dialog_capability::authorization::Authorized;
use dialog_capability::command::Command;
use dialog_capability::site::Site;
use dialog_capability::{Capability, Constraint, Effect};

use super::UcanInvocation;

/// UCAN access format — carries endpoint + delegation chain context.
///
/// This is the access format for UCAN-based authorization. It carries
/// the endpoint URL needed by the Authorize provider.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UcanAccess {
    /// The access service URL to POST invocations to.
    pub endpoint: String,
}

impl Access for UcanAccess {
    type Authorization = UcanInvocation;
}

/// A typed UCAN invocation wrapping a `UcanInvocation` (the raw authorization)
/// together with the `Capability<Fx>` it authorizes.
///
/// This is the `Site::Invocation` GAT for `UcanSite`.
pub struct UcanTypedInvocation<Fx: Effect> {
    /// The raw UCAN invocation (endpoint + chain + subject + ability).
    pub invocation: UcanInvocation,
    /// The capability this invocation authorizes.
    pub capability: Capability<Fx>,
}

impl<Fx: Effect> Command for UcanTypedInvocation<Fx>
where
    Fx::Of: Constraint,
{
    type Input = Self;
    type Output = Fx::Output;
}

impl<Fx: Effect> From<Authorized<Fx, UcanAccess>> for UcanTypedInvocation<Fx> {
    fn from(auth: Authorized<Fx, UcanAccess>) -> Self {
        UcanTypedInvocation {
            invocation: auth.authorization,
            capability: auth.capability,
        }
    }
}

/// UCAN site configuration for delegated authorization.
///
/// Contains the access service endpoint. Credentials (delegation chain)
/// are managed by the environment's credential store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UcanSite {
    /// The access service URL to POST invocations to.
    pub(crate) endpoint: String,
}

impl UcanSite {
    /// Create a new UCAN site with the given endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// Get the access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

// Blanket: route UcanTypedInvocation<Fx> to Environment's remote field.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, Fx> dialog_capability::Provider<UcanTypedInvocation<Fx>>
    for dialog_effects::environment::Environment<Local, Credentials, Remote>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    UcanTypedInvocation<Fx>: dialog_common::ConditionalSend,
    Remote: dialog_capability::Provider<UcanTypedInvocation<Fx>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: UcanTypedInvocation<Fx>) -> Fx::Output {
        self.remote.execute(input).await
    }
}

impl Site for UcanSite {
    type Access = UcanAccess;
    type Invocation<Fx: Effect> = UcanTypedInvocation<Fx>;

    fn access(&self) -> UcanAccess {
        UcanAccess {
            endpoint: self.endpoint.clone(),
        }
    }
}
