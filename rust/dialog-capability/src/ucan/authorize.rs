//! UCAN authorization — builds a signed invocation from a capability.

use super::claim;
use super::issuer::Issuer;
use crate::credential::{self, Authorization, AuthorizeError};
use crate::{Ability, Capability, Constraint, Provider};
use dialog_common::ConditionalSync;

use super::Ucan;

/// Authorize a capability using UCAN delegation chain discovery.
///
/// 1. Discovers the operator identity via `credential::Identify`
/// 2. Constructs an [`Issuer`] for signing
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
    Env: Provider<credential::Identify>
        + Provider<credential::Sign>
        + Provider<credential::List<Vec<u8>>>
        + Provider<credential::Retrieve<Vec<u8>>>
        + ConditionalSync,
{
    let issuer = Issuer::for_subject(env, capability.subject().clone()).await?;
    let invocation = claim(env, issuer, &capability).await?;
    Ok(Authorization::new(capability, invocation))
}
