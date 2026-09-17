//! The site: the same address and authorization as the permit-based UCAN
//! remote, with an exchange that performs the operation in the request
//! that proves it.

use dialog_capability::access::{
    Access, Authorization as _, Authorize as AuthorizeEffect, AuthorizeError, FromCapability,
    Protocol, TimeRange,
};
use dialog_capability::{
    Ability, Capability, Constraint, Effect, Fork, ForkInvocation, Provider, Site, SiteFork,
    Subject,
};
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::authority::{self, OperatorExt};
use dialog_remote_ucan_s3::{Ucan, UcanAuthorization, UcanSite as PermitSite};

use crate::address::UcanAddress;

/// A UCAN site that proves and performs each operation in one request.
///
/// Holds the permit-based site beside it for the exchanges that still
/// need a permit: blob streams, and any operation a service answers
/// with a permit instead of performing.
#[derive(Debug, Clone, Default)]
pub struct UcanSite {
    permits: PermitSite,
}

impl UcanSite {
    /// The permit-based site this one completes permits through.
    pub(crate) fn permits(&self) -> &PermitSite {
        &self.permits
    }
}

impl Site for UcanSite {
    type Authorization = UcanAuthorization;
    type Address = UcanAddress;
    type Fork<Fx: Effect> = UcanFork<Fx>;
}

/// Site-owned fork wrapper for [`UcanSite`].
pub struct UcanFork<Fx: Effect>(Fork<UcanSite, Fx>);

impl<Fx: Effect> From<Fork<UcanSite, Fx>> for UcanFork<Fx> {
    fn from(fork: Fork<UcanSite, Fx>) -> Self {
        Self(fork)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for UcanFork<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<AuthorizeEffect<Ucan>> + Provider<authority::Identify> + ConditionalSync,
{
    type Site = UcanSite;
    type Effect = Fx;

    /// Mint the signed invocation for this fork's capability, the same
    /// way the permit-based site does: authority good at the moment it
    /// is presented, scoped to exactly this capability.
    async fn authorize(self, env: &Env) -> Result<ForkInvocation<UcanSite, Fx>, AuthorizeError> {
        let identity =
            authority::Identify
                .perform(env)
                .await
                .map_err(|e| AuthorizeError::Malformed {
                    detail: e.to_string(),
                })?;
        let profile = identity.profile().clone();
        let operator = identity.did();

        let scope = <Ucan as Protocol>::Access::from_capability(self.0.capability());

        let at = now_s();
        let authorization = Subject::from(profile)
            .attenuate(Access)
            .invoke(
                AuthorizeEffect::<Ucan>::new(operator, scope).during(TimeRange {
                    not_before: Some(at),
                    expiration: Some(at),
                }),
            )
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        Ok(self.0.attest(UcanAuthorization::from(invocation)))
    }
}

fn now_s() -> u64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}
