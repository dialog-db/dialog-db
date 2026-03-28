//! Builder for constructing an Operator from a Profile.

use super::Operator;
use crate::Credentials;
use crate::environment::Environment;
use crate::profile::Profile;
use dialog_capability::storage::{Location, Mount, Mountable};
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Incomplete builder — needs `.storage()` and `.network()` before mount/build.
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

    /// Set the storage provider.
    pub fn storage<S>(self, storage: S) -> StorageBuilder<S> {
        StorageBuilder {
            credential: self.credential,
            context: self.context,
            storage,
        }
    }
}

/// Builder with storage set, needs `.network()`.
pub struct StorageBuilder<S> {
    credential: SignerCredential,
    context: Vec<u8>,
    storage: S,
}

impl<S> StorageBuilder<S> {
    /// Set the network/remote provider.
    pub fn network<R>(self, remote: R) -> MountBuilder<S, R> {
        MountBuilder {
            credential: self.credential,
            context: self.context,
            storage: self.storage,
            remote,
            mount: None,
        }
    }
}

/// Builder ready for `.mount()` and `.build()`.
pub struct MountBuilder<S, R> {
    credential: SignerCredential,
    context: Vec<u8>,
    storage: S,
    remote: R,
    mount: Option<Capability<Location>>,
}

impl<S, R> MountBuilder<S, R> {
    /// Set the storage mount location.
    ///
    /// This location will be mounted during `.build()` to produce
    /// the local storage backend.
    pub fn mount(mut self, location: Capability<Location>) -> Self {
        self.mount = Some(location);
        self
    }

    /// Build the operator, deriving the operator key and mounting storage.
    pub async fn build(self) -> Result<Operator<S::Store, R>, OperatorError>
    where
        S: Mountable + Provider<Mount<S::Store>> + ConditionalSync,
    {
        let mount_location = self.mount.ok_or(OperatorError::NoMount)?;

        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Credentials::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer,
        );

        let local = mount_location
            .clone()
            .mount::<S::Store>()
            .perform(&self.storage)
            .await
            .map_err(|e| OperatorError::Storage(e.to_string()))?;

        let env = Environment::new(credentials, local, self.remote);

        Ok(Operator {
            credential: self.credential,
            location: mount_location,
            env,
        })
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
    /// No mount location was set.
    #[error("No mount location configured — call .mount() before .build()")]
    NoMount,

    /// Storage mount failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Key derivation or generation failed.
    #[error("Key error: {0}")]
    Key(String),
}
