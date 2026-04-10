//! Fork dispatch providers for Operator.

use super::Operator;
use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::access::{AuthorizeError, Prove, Retain};
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::{Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::network::Network;

/// Helper trait for effect outputs that can absorb authorization errors.
///
/// All our effects return `Result<T, E>` where `E: From<AuthorizeError>`.
/// This trait lets the Operator inject auth errors into the output.
trait FromAuthError {
    fn from_auth_error(e: AuthorizeError) -> Self;
}

impl<T, E: From<AuthorizeError>> FromAuthError for Result<T, E> {
    fn from_auth_error(e: AuthorizeError) -> Self {
        Err(E::from(e))
    }
}

#[cfg(feature = "s3")]
mod s3 {
    use super::*;
    use dialog_remote_s3::{S3, S3Authorization};

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<S, Fx> Provider<Fork<S3, Fx>> for Operator<S>
    where
        S: Clone + ConditionalSend + ConditionalSync + 'static,
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        Fx::Output: ConditionalSend,
        Fork<S3, Fx>: ConditionalSend,
        ForkInvocation<S3, Fx>: ConditionalSend,
        Network: Provider<ForkInvocation<S3, Fx>> + ConditionalSync,
        Self: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, input: Fork<S3, Fx>) -> Fx::Output {
            let (capability, address) = input.into_parts();
            let invocation =
                ForkInvocation::new(capability, address, S3Authorization::default());
            invocation.perform(&self.network).await
        }
    }
}

mod ucan {
    use super::*;
    use dialog_capability::Ability;
    use dialog_capability::access::{self, Authorization as _, Proof as _};
    use dialog_remote_ucan_s3::{UcanAuthorization, UcanSite};
    use dialog_storage::provider::space::SpaceProvider;
    use dialog_ucan::scope::Scope as UcanScope;
    use dialog_ucan::{Ucan, UcanProofChain};

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<S, Fx> Provider<Fork<UcanSite, Fx>> for Operator<S>
    where
        S: SpaceProvider + Clone + 'static + Provider<Prove<Ucan>> + Provider<Retain<Ucan>>,
        Fx: Effect + Clone + ConditionalSend + 'static,
        Fx::Of: Constraint,
        Fx::Output: ConditionalSend + FromAuthError,
        Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
        Fork<UcanSite, Fx>: ConditionalSend,
        ForkInvocation<UcanSite, Fx>: ConditionalSend,
        Network: Provider<ForkInvocation<UcanSite, Fx>> + ConditionalSync,
        Self: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, input: Fork<UcanSite, Fx>) -> Fx::Output {
            let (capability, address) = input.into_parts();

            let scope = UcanScope::invoke(&capability);

            let proof_chain: UcanProofChain =
                match dialog_capability::Subject::from(self.profile_did())
                    .attenuate(access::Access)
                    .invoke(access::Prove::<Ucan>::new(self.did(), scope))
                    .perform(self)
                    .await
                {
                    Ok(chain) => chain,
                    Err(e) => return FromAuthError::from_auth_error(e),
                };

            let authorization = match proof_chain
                .claim(self.authority.operator_signer().clone())
            {
                Ok(auth) => auth,
                Err(e) => return FromAuthError::from_auth_error(e),
            };

            let ucan_invocation = match authorization.invoke().await {
                Ok(inv) => inv,
                Err(e) => return FromAuthError::from_auth_error(e),
            };

            let invocation = ForkInvocation::new(
                capability,
                address,
                UcanAuthorization::from(ucan_invocation),
            );
            invocation.perform(&self.network).await
        }
    }
}
