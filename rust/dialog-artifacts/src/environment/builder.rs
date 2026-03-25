//! Environment builder — configure and open an environment step by step.

use super::{OpenError, provider::Environment};
use crate::credentials::open::Open;
use crate::remote::Remote;
use crate::{Credentials, Operator};
use dialog_capability::Provider;
use dialog_credentials::{Ed25519Signer, key::KeyExport};

/// Domain separation context for deriving operator keys from profile keys.
const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// A grant that can be performed against a built environment.
///
/// Implement this for protocol-specific delegation commands
/// like `Ucan::unrestricted()`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Permit<Env> {
    /// Execute the grant against the environment.
    async fn perform(self, env: &Env) -> Result<(), OpenError>;
}

/// No-op permit — used as the default when no grant is specified.
pub struct NoPermit;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env: dialog_common::ConditionalSync> Permit<Env> for NoPermit {
    async fn perform(self, _env: &Env) -> Result<(), OpenError> {
        Ok(())
    }
}

/// Builder for constructing an environment.
///
/// Configure profile, operator strategy, storage backend, and remote dispatch,
/// then call [`build`](Builder::build) to open.
pub struct Builder<Storage, Permit = NoPermit> {
    pub(crate) profile: String,
    pub(crate) operator: Operator,
    pub(crate) storage: Storage,
    pub(crate) remote: Remote,
    pub(crate) permit: Permit,
}

impl<Storage> Builder<Storage, NoPermit> {
    /// Create a builder with explicit storage and defaults for everything else.
    pub(crate) fn new(storage: Storage) -> Self {
        Self {
            profile: "default".into(),
            operator: Operator::Unique,
            storage,
            remote: Remote,
            permit: NoPermit,
        }
    }

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
    pub fn storage<S>(self, storage: S) -> Builder<S, NoPermit> {
        Builder {
            profile: self.profile,
            operator: self.operator,
            storage,
            remote: self.remote,
            permit: self.permit,
        }
    }

    /// Set the remote dispatch configuration.
    pub fn remote(mut self, remote: Remote) -> Self {
        self.remote = remote;
        self
    }

    /// Set a delegation grant to execute after building the environment.
    pub fn grant<Permit>(self, permit: Permit) -> Builder<Storage, Permit> {
        Builder {
            profile: self.profile,
            operator: self.operator,
            storage: self.storage,
            remote: self.remote,
            permit,
        }
    }
}

impl<Storage, NoPermit> Builder<Storage, NoPermit>
where
    Storage: Provider<Open>,
    NoPermit: Permit<Environment<Credentials, Storage, Remote>>,
{
    /// Build the environment, executing any configured grants.
    pub async fn build(self) -> Result<Environment<Credentials, Storage, Remote>, OpenError> {
        let credentials = open_profile(&self.storage, &self.profile, &self.operator).await?;
        let env = Environment::new(credentials, self.storage, self.remote);
        self.permit.perform(&env).await?;
        Ok(env)
    }

    /// Build the environment using a custom profile provider.
    pub async fn build_with<P: Provider<Open>>(
        self,
        provider: &P,
    ) -> Result<Environment<Credentials, Storage, Remote>, OpenError> {
        let credentials = open_profile(provider, &self.profile, &self.operator).await?;
        let env = Environment::new(credentials, self.storage, self.remote);
        self.permit.perform(&env).await?;
        Ok(env)
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
