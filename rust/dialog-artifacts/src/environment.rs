//! Concrete environment composition for the repository layer.
//!
//! Provides [`Remote`], a newtype that wraps a network provider and
//! implements [`ProviderRoute`] with [`dialog_s3_credentials::Credentials`]
//! as the address type. It dispatches the unified credentials enum to
//! per-variant [`RemoteInvocation`] calls on the inner provider.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::ConditionalSend;
use dialog_effects::remote::RemoteInvocation;

pub use dialog_effects::environment::Environment;

/// Native environment: filesystem local storage with network remote.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeEnvironment<Issuer> = Environment<
    dialog_storage::provider::FileSystem,
    Remote<dialog_storage::provider::Network<Issuer>>,
>;

/// Web environment: IndexedDB local storage with network remote.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type WebEnvironment<Issuer> = Environment<
    dialog_storage::provider::IndexedDb,
    Remote<dialog_storage::provider::Network<Issuer>>,
>;

/// Test environment: in-memory local storage with emulated remote keyed by credentials.
#[cfg(any(test, feature = "helpers"))]
pub type TestEnvironment = Environment<
    dialog_storage::provider::Volatile,
    dialog_storage::provider::network::emulator::Route<dialog_s3_credentials::Credentials>,
>;

/// Remote provider that dispatches the unified
/// [`dialog_s3_credentials::Credentials`] enum to per-variant
/// [`RemoteInvocation`] calls on the inner provider.
pub struct Remote<T>(
    /// The inner network provider.
    pub T,
);

impl<T> ProviderRoute for Remote<T> {
    type Address = dialog_s3_credentials::Credentials;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<T, Fx> Provider<RemoteInvocation<Fx, dialog_s3_credentials::Credentials>> for Remote<T>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    T: Provider<RemoteInvocation<Fx, dialog_s3_credentials::s3::Credentials>>
        + ConditionalSend
        + Sync,
{
    async fn execute(
        &self,
        input: RemoteInvocation<Fx, dialog_s3_credentials::Credentials>,
    ) -> Fx::Output {
        let (capability, credentials) = input.into_parts();
        match credentials {
            dialog_s3_credentials::Credentials::S3(s3_creds) => {
                RemoteInvocation::new(capability, s3_creds)
                    .perform(&self.0)
                    .await
            }
        }
    }
}
