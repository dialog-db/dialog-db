//! Builder for constructing an Operator from a Profile.

use std::sync::{Arc, OnceLock};

use super::{Operator, WalkReach};
use dialog_capability::{Ability, Capability, Constraint, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_effects::storage::Directory;
use dialog_effects::{archive, blob, memory};
use dialog_identity::Authority;
use dialog_identity::Profile;
use dialog_network::Network;
use dialog_repository::{ACCESS_BRANCH, RemoteSite};
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Scope, UcanCertificate};
use dialog_ucan_core::DelegationBuilder;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use dialog_varsig::Signer;

const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Derive an operator from a profile.
///
/// An extension trait rather than an inherent method because [`Profile`]
/// lives in `dialog-identity`, below this crate, and cannot name
/// [`OperatorBuilder`].
pub trait DeriveOperator {
    /// Derive an operator from this profile with the given context seed.
    fn derive(&self, context: impl Into<Vec<u8>>) -> OperatorBuilder;
}

impl DeriveOperator for Profile {
    fn derive(&self, context: impl Into<Vec<u8>>) -> OperatorBuilder {
        OperatorBuilder::new(self, context.into())
    }
}

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
            credential: profile.signer().clone(),
            context,
            allowed: Vec::new(),
            directory: Directory::Current,
            network: Network::default(),
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
        T: Constraint,
        C: Into<Capability<T>>,
        Capability<T>: Ability,
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
    ///
    /// Every allowed scope becomes a profile-to-operator delegation held
    /// **in memory** — the session. Nothing is persisted: the operator key
    /// derives from the profile key, so identical authority re-mints on
    /// every build, and persisting it would only accumulate (one immortal
    /// certificate per session was exactly the field pathology). The
    /// operator is born on the profile repository's access branch, from
    /// which every proof of cross-party authority resolves.
    pub async fn build<S>(self, storage: Storage<S>) -> Result<Operator<S>, OperatorError>
    where
        S: SpaceProvider
            + Provider<blob::Read>
            + Provider<blob::Write>
            + Provider<blob::Import>
            + Clone
            + ConditionalSend
            + ConditionalSync
            + 'static,
    {
        let operator_signer = derive_operator(&self.credential, &self.context).await?;
        let credentials = Authority::new(
            "operator",
            Ed25519Signer::from(self.credential.clone()),
            operator_signer.clone(),
        );

        // Mint the session: one in-memory grant per allowed scope.
        let profile_signer = Ed25519Signer::from(self.credential.clone());
        let mut session = Vec::with_capacity(self.allowed.len());
        for scope in &self.allowed {
            let delegation = DelegationBuilder::new()
                .issuer(profile_signer.clone())
                .audience(&operator_signer)
                .subject(scope.subject.clone())
                .command(scope.command.segments().clone())
                .policy(scope.policy())
                .try_build()
                .await
                .map_err(|e| OperatorError::Delegation(format!("{e:?}")))?;
            session.push(UcanCertificate(delegation));
        }

        let operator = Operator {
            authority: credentials,
            storage,
            directory: self.directory,
            network: self.network,
            session: Arc::new(session),
            delegations: Arc::new(OnceLock::new()),
            chains: Arc::default(),
            reach: Arc::new(OnceLock::new()),
        };

        // Open the profile repository's access branch: the store every
        // proof resolves from and every retained delegation commits into.
        let repository = dialog_repository::Repository::from(self.credential.clone());
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await
            .map_err(|e| OperatorError::Delegation(format!("{e}")))?;
        operator
            .delegations
            .set(branch)
            .expect("freshly built operator has no access branch yet");

        // Install the walk's remote reach: the authorization walk's tree
        // and envelope reads replicate content on demand through these
        // fork effects, like any other read. The captured operator clone
        // carries NO reach of its own — the proof that authorizes such a
        // fetch resolves from what is already local, which bounds the
        // recursion a fork-inside-a-proof would otherwise open.
        let anchor = Operator {
            reach: Arc::new(OnceLock::new()),
            ..operator.clone()
        };
        let reach = WalkReach {
            get: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, archive::Get>>::execute(&anchor, input).await
                    })
                })
            },
            resolve: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, memory::Resolve>>::execute(&anchor, input).await
                    })
                })
            },
            blob_read: {
                let anchor = anchor.clone();
                Box::new(move |input| {
                    let anchor = anchor.clone();
                    Box::pin(async move {
                        Provider::<Fork<RemoteSite, blob::Read>>::execute(&anchor, input).await
                    })
                })
            },
        };
        operator
            .reach
            .set(reach)
            .unwrap_or_else(|_| unreachable!("freshly built operator has no reach yet"));

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
