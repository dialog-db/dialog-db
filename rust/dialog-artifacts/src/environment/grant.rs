//! Grant helpers for delegation protocols.

/// UCAN delegation grant helpers.
#[cfg(feature = "ucan")]
pub mod ucan {
    use async_trait::async_trait;
    use dialog_capability::access::AuthorizeError;
    use dialog_capability::ucan::{Scope, find_chain};
    use dialog_capability::{Ability, Did, Provider, Subject};
    use dialog_common::ConditionalSync;
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::storage;
    use dialog_ucan::DelegationChain;
    use dialog_ucan::delegation::builder::DelegationBuilder;
    use dialog_ucan::subject::Subject as UcanSubject;
    use dialog_ucan::time::Timestamp;
    use dialog_varsig::Principal;

    use super::super::OpenError;
    use super::super::builder::Permit;
    use super::super::provider::Environment;
    use crate::Credentials;
    use crate::remote::Remote;

    /// UCAN delegation constructors.
    pub struct Ucan;

    impl Ucan {
        /// Create an unrestricted (powerline) delegation.
        ///
        /// Equivalent to `Ucan::delegate(Subject::any())`.
        /// When used with `.grant()`, defaults the audience to the operator.
        pub fn unrestricted() -> Delegation {
            Self::delegate(Subject::any())
        }

        /// Start building a delegation for the given capability.
        ///
        /// Extracts the subject, command, and policy constraints from the
        /// capability chain. If the subject is `Subject::any()`, the
        /// delegation will be a powerline (unrestricted subject scope).
        pub fn delegate(capability: impl Ability) -> Delegation {
            Delegation {
                scope: Scope::from(&capability),
                audience: None,
                issuer: None,
                expiration: None,
                not_before: None,
            }
        }
    }

    /// A delegation being built. Configure audience and optionally issuer,
    /// then call `.perform(&env)` to sign and store.
    pub struct Delegation {
        scope: Scope,
        audience: Option<Did>,
        issuer: Option<Ed25519Signer>,
        expiration: Option<Timestamp>,
        not_before: Option<Timestamp>,
    }

    impl Delegation {
        /// Set the audience (recipient) of the delegation.
        pub fn audience(mut self, audience: impl Into<Did>) -> Self {
            self.audience = Some(audience.into());
            self
        }

        /// Set an explicit issuer. If not set, the profile signer from the
        /// environment is used.
        pub fn issuer(mut self, issuer: impl Into<Ed25519Signer>) -> Self {
            self.issuer = Some(issuer.into());
            self
        }

        /// Set when the delegation expires.
        pub fn expires(mut self, expiration: Timestamp) -> Self {
            self.expiration = Some(expiration);
            self
        }

        /// Set the earliest time the delegation becomes valid.
        pub fn not_before(mut self, not_before: Timestamp) -> Self {
            self.not_before = Some(not_before);
            self
        }

        /// Acquire the proof chain needed for this delegation.
        ///
        /// For powerline delegations (`Subject::Any`), no proof is needed
        /// and `Ok(None)` is returned. For self-signed delegations (issuer
        /// is the subject), no proof is needed either.
        ///
        /// Otherwise, searches the storage for an existing chain from
        /// subject → issuer. Returns `Err` if no valid chain can be found.
        pub async fn acquire<S>(
            &self,
            env: &Environment<Credentials, S, Remote>,
        ) -> Result<Option<DelegationChain>, OpenError>
        where
            Environment<Credentials, S, Remote>:
                Provider<storage::List> + Provider<storage::Get> + ConditionalSync,
            S: ConditionalSync,
        {
            let subject_did = match &self.scope.subject {
                UcanSubject::Any => return Ok(None),
                UcanSubject::Specific(did) => did,
            };

            let issuer_did = self
                .issuer
                .as_ref()
                .map(|s| s.did())
                .unwrap_or_else(|| env.authority.profile_did());

            if &issuer_did == subject_did {
                return Ok(None);
            }

            let command = self.scope.command.clone();
            let profile_did = env.authority.profile_did();

            let chain = find_chain(
                env,
                &profile_did,
                &issuer_did,
                subject_did,
                &command,
                self.scope.parameters.as_map(),
                &Timestamp::now(),
            )
            .await
            .map_err(|e: AuthorizeError| OpenError::Key(e.to_string()))?;

            match chain {
                Some(c) => Ok(Some(c)),
                None => Err(OpenError::Key(format!(
                    "no delegation chain found from '{}' to '{}'",
                    subject_did, issuer_did
                ))),
            }
        }

        /// Sign the delegation and store it under the profile's storage.
        ///
        /// For subject-specific delegations, first acquires the proof chain
        /// and includes it. For powerline delegations, no proof is needed.
        pub async fn perform<S>(
            self,
            env: &Environment<Credentials, S, Remote>,
        ) -> Result<(), OpenError>
        where
            Environment<Credentials, S, Remote>: Provider<storage::Set>
                + Provider<storage::List>
                + Provider<storage::Get>
                + ConditionalSync,
            S: ConditionalSync,
        {
            let proof = self.acquire(env).await?;

            let audience_did = self
                .audience
                .ok_or_else(|| OpenError::Key("delegation requires an audience".into()))?;

            let issuer = self
                .issuer
                .unwrap_or_else(|| env.authority.profile_signer().clone());

            let profile_did = env.authority.profile_did();

            let mut builder = DelegationBuilder::new()
                .issuer(issuer)
                .audience(&audience_did)
                .subject(self.scope.subject.clone())
                .command(self.scope.command.segments().clone())
                .policy(self.scope.policy());

            if let Some(exp) = self.expiration {
                builder = builder.expiration(exp);
            }
            if let Some(nbf) = self.not_before {
                builder = builder.not_before(nbf);
            }

            let delegation = builder
                .try_build()
                .await
                .map_err(|e| OpenError::Key(format!("failed to build delegation: {e:?}")))?;

            let chain = match proof {
                Some(proof_chain) => proof_chain
                    .extend(delegation)
                    .map_err(|e| OpenError::Key(format!("chain extension failed: {e}")))?,
                None => DelegationChain::new(delegation),
            };

            store_delegation_chain(env, &profile_did, &chain).await
        }
    }

    /// When used as a `Permit` via `.grant()`, defaults audience to the operator.
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl<Storage> Permit<Environment<Credentials, Storage, Remote>> for Delegation
    where
        Environment<Credentials, Storage, Remote>: Provider<storage::Set>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
        Storage: ConditionalSync,
    {
        async fn perform(
            self,
            env: &Environment<Credentials, Storage, Remote>,
        ) -> Result<(), OpenError> {
            let with_audience = if self.audience.is_some() {
                self
            } else {
                self.audience(env.authority.operator_did())
            };
            with_audience.perform(env).await
        }
    }

    /// Store a delegation chain into the storage layer under the given subject.
    pub async fn store_delegation_chain<Env>(
        env: &Env,
        store_subject: &Did,
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
}
