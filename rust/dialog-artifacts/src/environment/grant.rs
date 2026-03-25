//! Grant helpers for delegation protocols.

/// UCAN delegation grant helpers.
#[cfg(feature = "ucan")]
pub mod ucan {
    use async_trait::async_trait;
    use dialog_capability::Provider;
    use dialog_capability::credential;
    use dialog_capability::ucan::import_delegation_chain;
    use dialog_common::ConditionalSync;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::subject::Subject;

    use super::super::OpenError;
    use super::super::builder::Grant;
    use super::super::provider::Environment;
    use crate::Credentials;
    use crate::remote::Remote;

    /// UCAN delegation grant constructors.
    pub struct Ucan;

    impl Ucan {
        /// Create an unrestricted (powerline) delegation grant.
        ///
        /// Delegates all commands on any subject from the profile to the operator.
        /// Use with [`Builder::grant`](super::super::Builder::grant):
        ///
        /// ```no_run
        /// # #[cfg(all(not(target_arch = "wasm32"), feature = "ucan"))]
        /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
        /// use dialog_artifacts::environment::{Builder, grant::ucan::Ucan};
        ///
        /// let env = Builder::default()
        ///     .operator(b"alice")
        ///     .grant(Ucan::unrestricted())
        ///     .build()
        ///     .await?;
        /// # Ok(())
        /// # }
        /// ```
        pub fn unrestricted() -> Unrestricted {
            Unrestricted
        }
    }

    /// An unrestricted UCAN delegation — all commands, any subject.
    pub struct Unrestricted;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Storage> Grant<Environment<Credentials, Storage, Remote>> for Unrestricted
    where
        Storage: Provider<credential::Save<Vec<u8>>> + ConditionalSync,
    {
        async fn perform(
            self,
            env: &Environment<Credentials, Storage, Remote>,
        ) -> Result<(), OpenError> {
            let profile_signer = env.authority.profile_signer().clone();
            let operator_did = env.authority.operator_did();

            let delegation = DelegationBuilder::new()
                .issuer(profile_signer)
                .audience(env.authority.operator_signer())
                .subject(Subject::Any)
                .command(vec![])
                .try_build()
                .await
                .map_err(|e| OpenError::Key(format!("failed to build delegation: {e:?}")))?;

            let chain = DelegationChain::new(delegation);
            import_delegation_chain(&env.local, &operator_did, &chain)
                .await
                .map_err(|e| OpenError::Key(e.to_string()))
        }
    }
}
