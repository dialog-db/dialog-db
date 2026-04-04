//! Builder for constructing an Operator from a Profile.

use crate::Authority;
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
/// Created via `Profile::derive(context)`.
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
    /// Allow a capability — creates a delegation from profile to operator.
    ///
    /// Accepts a `Capability<T>` or anything convertible to one
    /// (e.g. `Subject::any()` for powerline delegation).
    pub fn allow<T, C>(mut self, capability: C) -> Self
    where
        T: dialog_capability::Constraint,
        C: Into<dialog_capability::Capability<T>>,
        dialog_capability::Capability<T>: Ability,
    {
        let cap = capability.into();
        self.allowed.push(Scope::from(&cap));
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
        let credentials = Authority::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer,
        );

        let operator = Operator {
            authority: credentials.clone(),
            storage,
            remote: self.remote,
        };

        // Create delegations for allowed capabilities
        if !self.allowed.is_empty() {
            use dialog_capability::Subject;
            use dialog_capability::access::{Permit, Save};
            use dialog_capability_ucan::Ucan;
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

                Subject::from(profile_did.clone())
                    .attenuate(Permit)
                    .invoke(Save::<Ucan>::new(chain))
                    .perform(&operator)
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
        KeyExport::NonExtractable { .. } => {
            // Use sign-to-derive: Ed25519 signing is deterministic, so
            // signing the context produces reproducible bytes we can use
            // as key material for the derived operator.
            let mut derivation_input = OPERATOR_DERIVATION_CONTEXT.as_bytes().to_vec();
            derivation_input.extend_from_slice(context);

            use dialog_varsig::Signer;
            let signature = signer
                .sign(&derivation_input)
                .await
                .map_err(|e| OperatorError::Key(e.to_string()))?;

            let sig_bytes: [u8; 64] = signature.into();
            let derived = blake3::derive_key(OPERATOR_DERIVATION_CONTEXT, &sig_bytes);
            Ed25519Signer::import(&derived)
                .await
                .map_err(|e| OperatorError::Key(e.to_string()))
        }
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
