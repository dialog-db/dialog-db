//! Builder for constructing an Operator from a Profile.

use crate::Credentials;
use crate::environment::Environment;
use crate::profile::Profile;
use crate::remote::Remote;
use crate::storage::Storage;
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};

use super::Operator;

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Builder for constructing an Operator from a Profile.
///
/// Created via `Profile::operator(context)`.
pub struct OperatorBuilder {
    credential: SignerCredential,
    context: Vec<u8>,
}

impl OperatorBuilder {
    pub(crate) fn new(profile: &Profile, context: Vec<u8>) -> Self {
        Self {
            credential: profile.credential().clone(),
            context,
        }
    }

    /// Set the remote dispatch provider.
    pub fn network(self, remote: Remote) -> NetworkBuilder {
        NetworkBuilder {
            credential: self.credential,
            context: self.context,
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
    remote: Remote,
}

impl NetworkBuilder {
    /// Build the operator, deriving the operator key.
    ///
    /// Takes a [`Storage`] reference to extract the DID-routed store table.
    pub async fn build(self, storage: Storage) -> Result<Operator, OperatorError> {
        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Credentials::new(
            "operator",
            Ed25519Signer::from(self.credential),
            operator_signer,
        );

        Ok(Environment::new(
            credentials,
            storage.take_stores(),
            self.remote,
        ))
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
}
