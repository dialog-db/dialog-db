//! Composite remote site types for multi-transport dispatch.
//!
//! [`SiteAddress`] is an enum of concrete address types (S3, UCAN).
//! [`RemoteSite`] is the corresponding site marker that dispatches
//! authorization and execution to the appropriate backend.

use dialog_capability::access::Authorize;
use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::Acquire;
use dialog_capability::site::{self, Site, SiteIssuer};
use dialog_capability::{
    Ability, Capability, Constraint, Effect, ForkInvocation, Provider, SiteId,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::credential::{Load, Secret};
use dialog_remote_s3::{Address, S3Authorization, S3Claim};
use dialog_remote_ucan_s3::{Ucan, UcanAddress, UcanAuthorization, UcanClaim};
use serde::{Deserialize, Serialize};

/// Connection info for a remote site.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub enum SiteAddress {
    /// Direct S3 access.
    S3(Address),
    /// UCAN-based authorization via external access service.
    Ucan(UcanAddress),
}

impl From<Address> for SiteAddress {
    fn from(addr: Address) -> Self {
        Self::S3(addr)
    }
}

impl From<UcanAddress> for SiteAddress {
    fn from(addr: UcanAddress) -> Self {
        Self::Ucan(addr)
    }
}

impl From<SiteAddress> for SiteId {
    fn from(address: SiteAddress) -> Self {
        match address {
            SiteAddress::S3(addr) => addr.into(),
            SiteAddress::Ucan(addr) => addr.into(),
        }
    }
}

impl site::SiteAddress for SiteAddress {
    type Site = RemoteSite;
}

/// Composite site for multi-transport remote dispatch.
#[derive(Debug, Clone)]
pub struct RemoteSite;

/// Composite authorization material.
#[derive(Debug, Clone)]
pub enum RemoteAuthorization {
    /// S3 credentials.
    S3(S3Authorization),
    /// UCAN signed invocation.
    Ucan(UcanAuthorization),
}

/// Composite claim for remote authorization.
pub enum RemoteClaim<Fx: Effect> {
    /// S3 credential claim.
    S3(S3Claim<Fx>),
    /// UCAN capability claim.
    Ucan(UcanClaim<Fx>),
}

impl<Fx: Effect> From<(Capability<Fx>, SiteIssuer, SiteAddress)> for RemoteClaim<Fx> {
    fn from((capability, issuer, address): (Capability<Fx>, SiteIssuer, SiteAddress)) -> Self {
        match address {
            SiteAddress::S3(addr) => Self::S3((capability, issuer, addr).into()),
            SiteAddress::Ucan(addr) => Self::Ucan((capability, issuer, addr).into()),
        }
    }
}

impl Site for RemoteSite {
    type Authorization = RemoteAuthorization;
    type Address = SiteAddress;
    type Claim<Fx: Effect> = RemoteClaim<Fx>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> Acquire<Env> for RemoteClaim<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<Authorize<Ucan>> + Provider<Load<Secret>> + ConditionalSync,
    S3Claim<Fx>: ConditionalSend,
    UcanClaim<Fx>: ConditionalSend,
{
    type Site = RemoteSite;
    type Effect = Fx;

    async fn perform(self, env: &Env) -> Result<ForkInvocation<RemoteSite, Fx>, AuthorizeError> {
        match self {
            Self::S3(claim) => {
                let invocation = claim.perform(env).await?;
                Ok(ForkInvocation::new(
                    invocation.capability,
                    SiteAddress::S3(invocation.address),
                    RemoteAuthorization::S3(invocation.authorization),
                ))
            }
            Self::Ucan(claim) => {
                let invocation = claim.perform(env).await?;
                Ok(ForkInvocation::new(
                    invocation.capability,
                    SiteAddress::Ucan(invocation.address),
                    RemoteAuthorization::Ucan(invocation.authorization),
                ))
            }
        }
    }
}
