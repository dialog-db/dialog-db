//! Remote dispatch — routes `ForkInvocation<S, Fx>` to the appropriate site provider.
//!
//! [`Remote`] implements `Provider<ForkInvocation<S3, Fx>>` (and optionally
//! `Provider<ForkInvocation<UcanSite, Fx>>` with the `ucan` feature) by delegating
//! to the stateless site executors.
//!
//! The Operator builds the authorization (converting `Fork` to `ForkInvocation`)
//! before delegating here.

/// Remote dispatch — routes fork invocations to the appropriate site.
///
/// Both `S3` and `UcanSite` are stateless, so `Remote::default()` is all
/// you need. The Operator routes `ForkInvocation<S, Fx>` here after building
/// the protocol-specific authorization.
#[derive(Debug, Clone, Copy, Default)]
pub struct Remote;

#[cfg(feature = "s3")]
mod s3_dispatch {
    use super::Remote;
    use async_trait::async_trait;
    use dialog_capability::fork::ForkInvocation;
    use dialog_capability::{Constraint, Effect, Provider};
    use dialog_remote_s3::S3;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Fx> Provider<ForkInvocation<S3, Fx>> for Remote
    where
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        ForkInvocation<S3, Fx>: dialog_common::ConditionalSend,
        S3: Provider<ForkInvocation<S3, Fx>>,
    {
        async fn execute(&self, input: ForkInvocation<S3, Fx>) -> Fx::Output {
            <S3 as Provider<ForkInvocation<S3, Fx>>>::execute(&S3, input).await
        }
    }
}

#[cfg(feature = "ucan")]
mod ucan_dispatch {
    use super::Remote;
    use async_trait::async_trait;
    use dialog_capability::fork::ForkInvocation;
    use dialog_capability::{Constraint, Effect, Provider};
    use dialog_remote_ucan_s3::UcanSite;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Fx> Provider<ForkInvocation<UcanSite, Fx>> for Remote
    where
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        ForkInvocation<UcanSite, Fx>: dialog_common::ConditionalSend,
        UcanSite: Provider<ForkInvocation<UcanSite, Fx>>,
    {
        async fn execute(&self, input: ForkInvocation<UcanSite, Fx>) -> Fx::Output {
            <UcanSite as Provider<ForkInvocation<UcanSite, Fx>>>::execute(&UcanSite, input).await
        }
    }
}
