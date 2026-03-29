//! Grant helpers for delegation protocols.

/// UCAN delegation grant helpers.
#[cfg(feature = "ucan")]
pub mod ucan {
    use async_trait::async_trait;
    use dialog_capability::authority;
    use dialog_capability::ucan::{DelegateRequest, IssuerUnset, Ucan};
    use dialog_capability::{Ability, Provider};
    use dialog_common::ConditionalSync;
    use dialog_effects::storage;

    use super::super::OpenError;
    use super::super::builder::Permit;
    use super::super::provider::Environment;
    use crate::Credentials;
    use crate::remote::Remote;

    pub use dialog_capability::ucan::{InvokeRequest, Scope};

    /// When used as a `Permit` via `.grant()`, defaults audience to the operator
    /// and issuer to the profile signer.
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Storage> Permit<Environment<Credentials, Storage, Remote>> for DelegateRequest<IssuerUnset>
    where
        Environment<Credentials, Storage, Remote>: Provider<authority::Identify>
            + Provider<storage::Set>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
        Storage: ConditionalSync,
    {
        async fn perform(
            self,
            env: &Environment<Credentials, Storage, Remote>,
        ) -> Result<(), OpenError> {
            let audience = env.authority.operator_did();
            let issuer = env.authority.profile_signer().clone();
            self.audience(audience)
                .issuer(issuer)
                .perform(env)
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;
            Ok(())
        }
    }

    /// Convenience re-export for building delegations with the artifacts API.
    pub fn delegate(capability: &impl Ability) -> DelegateRequest<IssuerUnset> {
        Ucan::delegate(capability)
    }
}
