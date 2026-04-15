//! UCAN site configuration -- marker trait + address type.

use dialog_capability::access::{
    Access, Authorization as _, Authorize, AuthorizeError, FromCapability, Protocol,
};
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Ability, Capability, Constraint, Effect, Provider, SiteId, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_remote_s3::{Permit, S3Error};

// Re-export UCAN types for convenience.
pub use dialog_ucan::{Ucan, UcanInvocation};

/// UCAN authorization material for site providers.
///
/// Wraps a [`UcanInvocation`] (signed UCAN chain) that gets sent to the
/// access service to obtain a presigned URL.
#[derive(Debug, Clone)]
pub struct UcanAuthorization(pub UcanInvocation);

impl UcanAuthorization {
    /// Redeem this authorization at the access service for a presigned URL permit.
    pub async fn redeem(&self, address: &UcanAddress) -> Result<Permit, S3Error> {
        let body = self
            .0
            .to_bytes()
            .map_err(|e| S3Error::Authorization(e.to_string()))?;

        let response = reqwest::Client::new()
            .post(&address.endpoint)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(S3Error::Service(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        let body = response.bytes().await?;

        serde_ipld_dagcbor::from_slice(&body)
            .map_err(|e| S3Error::Service(format!("Failed to decode response: {}", e)))
    }
}

impl From<UcanInvocation> for UcanAuthorization {
    fn from(invocation: UcanInvocation) -> Self {
        Self(invocation)
    }
}

/// UCAN site address -- wraps the access service endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UcanAddress {
    /// The access service endpoint URL.
    pub endpoint: String,
}

impl UcanAddress {
    /// Create a new UCAN address with the given endpoint.
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

impl SiteAddress for UcanAddress {
    type Site = UcanSite;
}

impl From<UcanAddress> for SiteId {
    fn from(address: UcanAddress) -> Self {
        address.endpoint.into()
    }
}

use dialog_capability::SiteIssuer;
use dialog_capability::fork::SiteAuthorization;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> SiteAuthorization<Env> for UcanAddress
where
    Env: Provider<Authorize<Ucan>> + ConditionalSync,
{
    async fn authorize<Fx: Effect + Clone + ConditionalSend + 'static>(
        &self,
        capability: &Capability<Fx>,
        issuer: &SiteIssuer,
        env: &Env,
    ) -> Result<UcanAuthorization, AuthorizeError>
    where
        Fx::Of: Constraint,
        Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    {
        let scope = <Ucan as Protocol>::Access::from_capability(capability);

        let authorization = Subject::from(issuer.profile.clone())
            .attenuate(Access)
            .invoke(Authorize::<Ucan>::new(issuer.operator.clone(), scope))
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        Ok(UcanAuthorization::from(invocation))
    }
}

/// UCAN site configuration for delegated authorization.
///
/// A marker type -- no fields. Address info lives in `UcanAddress`.
#[derive(Debug, Clone, Copy, Default)]
pub struct UcanSite;

impl Site for UcanSite {
    type Authorization = UcanAuthorization;
    type Address = UcanAddress;
}
