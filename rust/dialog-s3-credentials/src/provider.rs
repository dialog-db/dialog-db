use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use crate::credentials::{CredentialAuthorization, Credentials};
use async_trait::async_trait;
use dialog_common::capability::{Authorized, Effect, Provider};
use dialog_common::{Capability, ConditionalSend};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Do> Provider<Authorized<Do, CredentialAuthorization>> for Credentials
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        authorized: Authorized<Do, CredentialAuthorization>,
    ) -> Result<AuthorizedRequest, AccessError> {
        authorized
            .authorization()
            .grant(authorized.capability())
            .await
    }
}
