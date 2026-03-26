//! Grant helpers for delegation protocols.

/// UCAN delegation grant helpers.
#[cfg(feature = "ucan")]
pub mod ucan {
    use async_trait::async_trait;
    use dialog_capability::Provider;
    use dialog_common::ConditionalSync;
    use dialog_effects::storage;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::subject::Subject as UcanSubject;

    use super::super::OpenError;
    use super::super::builder::Permit;
    use super::super::provider::Environment;
    use crate::Credentials;
    use crate::remote::Remote;

    /// UCAN delegation grant constructors.
    pub struct Ucan;

    impl Ucan {
        /// Create an unrestricted (powerline) delegation grant.
        ///
        /// Delegates all commands on any subject from the profile to the operator.
        pub fn unrestricted() -> Unrestricted {
            Unrestricted
        }
    }

    /// Store a delegation chain into the storage layer under the given subject.
    ///
    /// Uses the `"ucan"` store with the same key layout as the credential-based
    /// delegation storage.
    pub async fn store_delegation_chain<Env>(
        env: &Env,
        store_subject: &dialog_capability::Did,
        chain: &DelegationChain,
    ) -> Result<(), OpenError>
    where
        Env: Provider<storage::Set> + ConditionalSync,
    {
        for (cid, delegation) in chain.delegations() {
            let audience = delegation.audience();
            let key = match delegation.subject() {
                UcanSubject::Specific(did) => {
                    format!("{}/{}/{}.{}", audience, did, delegation.issuer(), cid)
                }
                UcanSubject::Any => {
                    format!("{}/_/{}.{}", audience, delegation.issuer(), cid)
                }
            };

            let bytes = serde_ipld_dagcbor::to_vec(delegation.as_ref())
                .map_err(|e| OpenError::Key(format!("delegation serialization failed: {e}")))?;

            dialog_capability::Subject::from(store_subject.clone())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("ucan"))
                .invoke(storage::Set::new(key.into_bytes(), bytes))
                .perform(env)
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;
        }
        Ok(())
    }

    /// An unrestricted UCAN delegation — all commands, any subject.
    pub struct Unrestricted;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Storage> Permit<Environment<Credentials, Storage, Remote>> for Unrestricted
    where
        Environment<Credentials, Storage, Remote>:
            Provider<storage::Set> + ConditionalSync + ConditionalSync,
        Storage: ConditionalSync,
    {
        async fn perform(
            self,
            env: &Environment<Credentials, Storage, Remote>,
        ) -> Result<(), OpenError> {
            let profile_signer = env.authority.profile_signer().clone();
            let profile_did = env.authority.profile_did();

            let delegation = DelegationBuilder::new()
                .issuer(profile_signer)
                .audience(env.authority.operator_signer())
                .subject(UcanSubject::Any)
                .command(vec![])
                .try_build()
                .await
                .map_err(|e| OpenError::Key(format!("failed to build delegation: {e:?}")))?;

            let chain = DelegationChain::new(delegation);
            store_delegation_chain(env, &profile_did, &chain).await
        }
    }
}
