//! Environment builder — configure and open an environment step by step.

use super::{OpenError, provider::Environment};
use crate::credentials::open::Open;
use crate::remote::Remote;
use crate::{Credentials, Operator};
use dialog_capability::Provider;
use dialog_credentials::{Ed25519Signer, key::KeyExport};

/// Domain separation context for deriving operator keys from profile keys.
const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Builder for constructing an environment.
///
/// Configure profile, operator strategy, storage backend, and remote dispatch,
/// then call [`build`](Builder::build) or
/// [`build_with`](Builder::build_with) to open.
pub struct Builder<Storage> {
    pub(crate) profile: String,
    pub(crate) operator: Operator,
    pub(crate) storage: Storage,
    pub(crate) remote: Remote,
}

impl<Storage> Builder<Storage> {
    /// Set the profile name.
    pub fn profile(mut self, name: impl Into<String>) -> Self {
        self.profile = name.into();
        self
    }

    /// Set the operator derivation strategy.
    ///
    /// Accepts `Operator` directly, or anything convertible to it
    /// (e.g. `b"context"` for derived operator).
    pub fn operator(mut self, operator: impl Into<Operator>) -> Self {
        self.operator = operator.into();
        self
    }

    /// Set the storage backend.
    pub fn storage<S>(self, storage: S) -> Builder<S> {
        Builder {
            profile: self.profile,
            operator: self.operator,
            storage,
            remote: self.remote,
        }
    }

    /// Set the remote dispatch configuration.
    pub fn remote(mut self, remote: Remote) -> Self {
        self.remote = remote;
        self
    }
}

impl<Storage: Provider<Open>> Builder<Storage> {
    /// Build the environment, using the storage backend as the profile provider.
    pub async fn build(self) -> Result<Environment<Credentials, Storage, Remote>, OpenError> {
        let credentials = open_profile(&self.storage, &self.profile, &self.operator).await?;
        Ok(Environment::new(credentials, self.storage, self.remote))
    }

    /// Build the environment, using a custom provider for opening the profile.
    ///
    /// Use this when the profile key source differs from the storage backend,
    /// for example in tests.
    pub async fn build_with<P: Provider<Open>>(
        self,
        provider: &P,
    ) -> Result<Environment<Credentials, Storage, Remote>, OpenError> {
        let credentials = open_profile(provider, &self.profile, &self.operator).await?;
        Ok(Environment::new(credentials, self.storage, self.remote))
    }
}

async fn open_profile<P: Provider<Open>>(
    provider: &P,
    profile: &str,
    operator: &Operator,
) -> Result<Credentials, OpenError> {
    let profile_signer = Open::new(profile)
        .perform(provider)
        .await
        .map_err(|e| OpenError::Key(e.to_string()))?;

    let op = derive_operator(profile_signer.signer(), operator).await?;
    Ok(Credentials::new(profile, profile_signer.into_signer(), op))
}

async fn derive_operator(
    profile: &Ed25519Signer,
    strategy: &Operator,
) -> Result<Ed25519Signer, OpenError> {
    match strategy {
        Operator::Unique => Ed25519Signer::generate()
            .await
            .map_err(|e| OpenError::Key(e.to_string())),
        Operator::Derive(context) => {
            let export = profile
                .export()
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;

            match export {
                KeyExport::Extractable(ref seed) => {
                    let mut key_material = seed.clone();
                    key_material.extend_from_slice(context);

                    let derived = blake3::derive_key(OPERATOR_DERIVATION_CONTEXT, &key_material);
                    Ed25519Signer::import(&derived)
                        .await
                        .map_err(|e| OpenError::Key(e.to_string()))
                }
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                KeyExport::NonExtractable { .. } => Err(OpenError::Key(
                    "derived operators require extractable profile key".into(),
                )),
            }
        }
    }
}
