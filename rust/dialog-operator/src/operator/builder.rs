//! Builder for constructing an Operator from a Profile.

use crate::Authority;
use crate::profile::Profile;
use crate::remote::Remote;
use dialog_capability::access::{Claim as AccessClaim, Save as AccessSave};
use dialog_capability::{Ability, Provider};
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::Directory;
use dialog_storage::provider::environment::Environment;
use dialog_storage::provider::space::SpaceProvider;
use dialog_ucan::{Scope, Ucan};
use dialog_varsig::Principal;

use super::Operator;

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Builder for constructing an Operator from a Profile.
pub struct OperatorBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
    allowed: Vec<Scope>,
    directory: Directory,
}

impl OperatorBuilder {
    pub(crate) fn new(profile: &Profile, context: Vec<u8>) -> Self {
        Self {
            credential: profile.credential().clone(),
            context,
            allowed: Vec::new(),
            directory: Directory::Current,
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

    /// Set the remote dispatch provider.
    pub fn network(self, remote: Remote) -> NetworkBuilder {
        NetworkBuilder {
            credential: self.credential,
            context: self.context,
            allowed: self.allowed,
            directory: self.directory,
            remote,
        }
    }

    /// Build with default remote.
    pub async fn build<S>(self, env: Environment<S>) -> Result<Operator<S>, OperatorError>
    where
        S: SpaceProvider + Clone + 'static,
        S: Provider<AccessClaim<Ucan>>,
        S: Provider<AccessSave<Ucan>>,
    {
        self.network(Remote).build(env).await
    }
}

/// Builder with network configured, ready to build.
pub struct NetworkBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
    allowed: Vec<Scope>,
    directory: Directory,
    remote: Remote,
}

impl NetworkBuilder {
    /// Build the operator, deriving the operator key.
    pub async fn build<S>(self, env: Environment<S>) -> Result<Operator<S>, OperatorError>
    where
        S: SpaceProvider + Clone + 'static,
        S: Provider<AccessClaim<Ucan>>,
        S: Provider<AccessSave<Ucan>>,
    {
        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Authority::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer,
        );

        let operator = Operator {
            authority: credentials.clone(),
            env,
            directory: self.directory,
            remote: self.remote,
        };

        // Create delegations for allowed capabilities
        if !self.allowed.is_empty() {
            use dialog_capability::Subject;
            use dialog_capability::access::{Access, Save};
            use dialog_ucan::Ucan;
            use dialog_ucan_core::DelegationChain;
            use dialog_ucan_core::delegation::builder::DelegationBuilder;

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
                    .attenuate(Access)
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
