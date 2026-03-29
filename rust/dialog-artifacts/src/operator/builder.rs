//! Builder for constructing an Operator from a Profile.

use crate::Credentials;
use crate::environment::Environment;
use crate::profile::Profile;
use crate::remote::Remote;
use crate::storage::Storage;
use dialog_capability::Ability;
use dialog_capability::ucan::Scope;
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_varsig::Principal;

use super::Operator;

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Builder for constructing an Operator from a Profile.
///
/// Created via `Profile::operator(context)`.
pub struct OperatorBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
    allowed: Vec<Scope>,
}

impl OperatorBuilder {
    pub(crate) fn new(profile: &Profile, context: Vec<u8>) -> Self {
        Self {
            credential: profile.credential().clone(),
            context,
            allowed: Vec::new(),
        }
    }

    /// Allow a capability — creates a delegation from profile to operator.
    ///
    /// The delegation is created during `.build()`.
    pub fn allow<T: dialog_capability::Constraint>(
        mut self,
        capability: dialog_capability::Capability<T>,
    ) -> Self
    where
        dialog_capability::Capability<T>: Ability,
    {
        self.allowed.push(Scope::from(&capability));
        self
    }

    /// Set the remote dispatch provider.
    pub fn network(self, remote: Remote) -> NetworkBuilder {
        NetworkBuilder {
            credential: self.credential,
            context: self.context,
            allowed: self.allowed,
            remote,
        }
    }

    /// Build with default remote, taking stores from the given storage.
    pub async fn build(self, storage: Storage) -> Result<Operator, OperatorError> {
        self.network(Remote).build(storage).await
    }
}

/// Builder with network configured, ready to build.
pub struct NetworkBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
    allowed: Vec<Scope>,
    remote: Remote,
}

impl NetworkBuilder {
    /// Build the operator, deriving the operator key.
    ///
    /// For each `.allow()` scope, creates a UCAN delegation from
    /// profile → operator and stores it under the profile's DID.
    pub async fn build(self, storage: Storage) -> Result<Operator, OperatorError> {
        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Credentials::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer,
        );

        let operator = Environment::new(credentials.clone(), storage.take_stores(), self.remote);

        // Create delegations for allowed capabilities
        #[cfg(feature = "ucan")]
        if !self.allowed.is_empty() {
            use crate::environment::grant::ucan::store_delegation_chain;
            use dialog_ucan::DelegationChain;
            use dialog_ucan::delegation::builder::DelegationBuilder;

            let profile_did = self.credential.did();
            let operator_did = credentials.operator_did();

            for scope in &self.allowed {
                let delegation = DelegationBuilder::new()
                    .issuer(self.credential.clone())
                    .audience(&operator_did)
                    .subject(scope.subject.clone())
                    .command(scope.command.segments().clone())
                    .policy(scope.policy())
                    .try_build()
                    .await
                    .map_err(|e| OperatorError::Delegation(format!("{e:?}")))?;

                let chain = DelegationChain::new(delegation);
                store_delegation_chain(&operator, &profile_did, &chain)
                    .await
                    .map_err(|e| OperatorError::Delegation(e.to_string()))?;
            }
        }

        Ok(operator)
    }
}

async fn derive_operator(
    credential: &SignerCredential,
    context: &[u8],
) -> Result<Ed25519Signer, OperatorError> {
    let signer = Ed25519Signer::from(credential.clone());
    let export = signer
        .export()
        .await
        .map_err(|e| OperatorError::Key(e.to_string()))?;

    match export {
        KeyExport::Extractable(ref seed) => {
            let mut key_material = seed.clone();
            key_material.extend_from_slice(context);

            let derived = blake3::derive_key(OPERATOR_DERIVATION_CONTEXT, &key_material);
            Ed25519Signer::import(&derived)
                .await
                .map_err(|e| OperatorError::Key(e.to_string()))
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        KeyExport::NonExtractable { .. } => Err(OperatorError::Key(
            "derived operators require extractable profile key".into(),
        )),
    }
}

/// Errors that can occur when building an Operator.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// Key derivation or generation failed.
    #[error("Key error: {0}")]
    Key(String),

    /// Delegation creation failed.
    #[error("Delegation error: {0}")]
    Delegation(String),
}
