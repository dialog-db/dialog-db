//! UCAN access authorization -- Protocol implementation for Ucan.
//!
//! Implements [`Protocol`] for [`Ucan`], using the invoke builder to
//! produce signed UCAN invocations.

use crate::access::{Authorization, AuthorizeError, Protocol};
use crate::authority::{Identify, Sign};
use crate::storage::{Get, List};
use crate::{Ability, Capability, Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};

use super::Ucan;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Protocol for Ucan {
    type Authorization<Fx: Constraint> = super::UcanInvocation;

    async fn authorize<Fx, Env>(
        env: &Env,
        capability: Capability<Fx>,
    ) -> Result<Authorization<Fx, Self>, AuthorizeError>
    where
        Fx: Effect + Clone + ConditionalSend + 'static,
        Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
        Env: Provider<Identify> + Provider<Sign> + Provider<List> + Provider<Get> + ConditionalSync,
    {
        let invocation = Ucan::invoke(&capability).perform(env).await?;
        Ok(Authorization::new(capability, invocation))
    }
}
