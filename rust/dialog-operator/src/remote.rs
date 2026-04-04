//! Remote dispatch — routes `Fork<S, Fx>` to the appropriate site provider.
//!
//! [`Remote`] implements `Provider<Fork<S3, Fx>>` (and optionally
//! `Provider<Fork<UcanSite, Fx>>` with the `ucan` feature) by delegating
//! to the stateless site executors.

/// Remote dispatch — routes fork invocations to the appropriate site.
///
/// Both `S3` and `UcanSite` are stateless, so `Remote::default()` is all
/// you need. The Environment routes `Fork<S, Fx>` here, and this type
/// delegates to the right site provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct Remote;

#[cfg(feature = "s3")]
mod s3_dispatch {
    use super::Remote;
    use async_trait::async_trait;
    use dialog_capability::access::AuthorizeError;
    use dialog_capability::fork::Fork;
    use dialog_capability::{Constraint, Effect, Provider};
    use dialog_remote_s3::S3;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Fx> Provider<Fork<S3, Fx>> for Remote
    where
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        Fork<S3, Fx>: dialog_common::ConditionalSend,
        S3: Provider<Fork<S3, Fx>>,
    {
        async fn execute(&self, input: Fork<S3, Fx>) -> Result<Fx::Output, AuthorizeError> {
            <S3 as Provider<Fork<S3, Fx>>>::execute(&S3, input).await
        }
    }
}

#[cfg(feature = "ucan")]
mod ucan_dispatch {
    use super::Remote;
    use async_trait::async_trait;
    use dialog_capability::access::AuthorizeError;
    use dialog_capability::fork::Fork;
    use dialog_capability::{Constraint, Effect, Provider};
    use dialog_remote_ucan_s3::UcanSite;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Fx> Provider<Fork<UcanSite, Fx>> for Remote
    where
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        Fork<UcanSite, Fx>: dialog_common::ConditionalSend,
        UcanSite: Provider<Fork<UcanSite, Fx>>,
    {
        async fn execute(
            &self,
            input: Fork<UcanSite, Fx>,
        ) -> Result<Fx::Output, AuthorizeError> {
            <UcanSite as Provider<Fork<UcanSite, Fx>>>::execute(&UcanSite, input).await
        }
    }
}
