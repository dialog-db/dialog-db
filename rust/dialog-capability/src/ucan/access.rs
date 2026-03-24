//! UCAN access — authorization provider and invocation builder.
//!
//! Provides the blanket `Provider<Authorize<Fx, Ucan>>` impl and the
//! `authorize` function that builds signed UCAN invocations from
//! delegation chains.

use super::claim;
use super::issuer::Issuer;
use crate::access::{self, Authorization, AuthorizeError};
use crate::{Ability, Capability, Constraint, Provider, Subject, authority, credential};
use dialog_common::{ConditionalSend, ConditionalSync};

use super::Ucan;

/// Authorize a capability using UCAN delegation chain discovery.
///
/// 1. Discovers the current authority via `Identify`
/// 2. Constructs an [`Issuer`] from the authority chain
/// 3. Delegates to [`claim`](super::claim::claim) to find the delegation chain
///    and build a signed UCAN invocation
/// 4. Returns an [`Authorization`] containing the capability and signed invocation
pub async fn authorize<C, Env>(
    env: &Env,
    capability: Capability<C>,
) -> Result<Authorization<C, Ucan>, AuthorizeError>
where
    C: Constraint,
    Capability<C>: Ability + ConditionalSync,
    Env: Provider<authority::Identify>
        + Provider<authority::Sign>
        + Provider<credential::List<Vec<u8>>>
        + Provider<credential::Retrieve<Vec<u8>>>
        + ConditionalSync,
{
    let authority = Subject::from(capability.subject().clone())
        .invoke(authority::Identify)
        .perform(env)
        .await
        .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

    let issuer = Issuer::new(env, authority);
    let invocation = claim(env, issuer, &capability).await?;
    Ok(Authorization::new(capability, invocation))
}

/// Blanket impl: any type providing identity, signing, and credential store
/// effects can authorize UCAN capabilities.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env, Fx> Provider<access::Authorize<Fx, Ucan>> for Env
where
    Fx: crate::Effect + 'static,
    Fx::Of: Constraint,
    Fx: Clone + ConditionalSend,
    Capability<Fx>: Ability + Clone + ConditionalSend + ConditionalSync,
    access::Authorize<Fx, Ucan>: ConditionalSend + 'static,
    Env: Provider<authority::Identify>
        + Provider<authority::Sign>
        + Provider<credential::List<Vec<u8>>>
        + Provider<credential::Retrieve<Vec<u8>>>
        + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<access::Authorize<Fx, Ucan>>,
    ) -> Result<Authorization<Fx, Ucan>, AuthorizeError> {
        let auth_request = input.into_inner().constraint;
        authorize(self, auth_request.capability).await
    }
}
