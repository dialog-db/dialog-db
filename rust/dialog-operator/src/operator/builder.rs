//! Builder for constructing an Operator from a Profile.

use crate::Authority;
use crate::network::Network;
use crate::profile::Profile;
use crate::profile::access::Access as ProfileAccess;
use dialog_capability::access::{Access, Authorization as _, Proof as _, Prove, Retain};
use dialog_capability::{Ability, Provider, Subject};
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::Directory;
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Scope, Ucan};
use dialog_varsig::Principal;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use dialog_varsig::Signer;

use super::Operator;

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Builder for constructing an Operator from a Profile.
pub struct OperatorBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
    allowed: Vec<Scope>,
    directory: Directory,
    network: Network,
}

impl OperatorBuilder {
    pub(crate) fn new(profile: &Profile, context: Vec<u8>) -> Self {
        Self {
            credential: profile.credential().clone(),
            context,
            allowed: Vec::new(),
            directory: Directory::Current,
            network: Network,
        }
    }

    /// Set the base directory for resolving space names.
    ///
    /// Defaults to `Directory::Current`.
    pub fn base(mut self, directory: Directory) -> Self {
        self.directory = directory;
        self
    }

    /// Allow a capability: creates a delegation from profile to operator.
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

    /// Set the network dispatch provider.
    pub fn network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    /// Build the operator, deriving the operator key.
    pub async fn build<S>(self, storage: Storage<S>) -> Result<Operator<S>, OperatorError>
    where
        S: SpaceProvider + Clone + 'static,
        S: Provider<Prove<Ucan>>,
        S: Provider<Retain<Ucan>>,
    {
        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Authority::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer,
        );

        let operator = Operator {
            authority: credentials.clone(),
            storage,
            directory: self.directory,
            network: self.network,
        };

        // Create delegations for allowed capabilities
        if !self.allowed.is_empty() {
            let profile_did = self.credential.did();
            let signer = Ed25519Signer::from(self.credential.clone());
            let access = ProfileAccess::new(&self.credential);
            let operator_did = credentials.operator_did();

            for scope in &self.allowed {
                // Prove authority (self-grant for profile)
                let proof = Subject::from(profile_did.clone())
                    .attenuate(Access)
                    .invoke(Prove::<Ucan>::new(profile_did.clone(), scope.clone()))
                    .perform(&operator)
                    .await
                    .map_err(|e| OperatorError::Delegation(e.to_string()))?;

                // Sign and delegate to operator
                let authorization = proof
                    .claim(signer.clone())
                    .map_err(|e| OperatorError::Delegation(e.to_string()))?;

                let delegation = authorization
                    .delegate(operator_did.clone())
                    .await
                    .map_err(|e| OperatorError::Delegation(e.to_string()))?;

                // Retain the delegation under the profile
                access
                    .save(delegation)
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
            let mut derivation_input = OPERATOR_DERIVATION_CONTEXT.as_bytes().to_vec();
            derivation_input.extend_from_slice(context);

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
