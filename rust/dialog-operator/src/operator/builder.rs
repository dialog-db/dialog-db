//! Builder for constructing an Operator from a Profile.

use std::sync::{Arc, OnceLock};

use super::{Operator, WalkReach};
use dialog_capability::{Ability, Capability, Constraint, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::secret::Context;
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
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};

/// The domain-separation label operator keys derive under.
///
/// Versioned: `v2` is the key-agreement derivation that replaced signing a
/// fixed message. Bumping it re-derives every operator, which forks each
/// profile's replica lineage, so it changes only when the derivation does.
const OPERATOR_DERIVATION_CONTEXT: Context = Context::new("dialog-db/operator/v2");

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
    allowed: Vec<(Scope, Option<Timestamp>)>,
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
        self.allowed.push((Scope::from(&cap), None));
        self
    }

    /// Allow a capability until `expiration`, held only in memory.
    pub fn allow_until<T, C>(mut self, capability: C, expiration: Timestamp) -> Self
    where
        T: Constraint,
        C: Into<Capability<T>>,
        Capability<T>: Ability,
    {
        let cap = capability.into();
        self.allowed.push((Scope::from(&cap), Some(expiration)));
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
        let profile_signer = ed25519_signer(&self.credential)?;
        let operator_signer = derive_operator(&profile_signer, &self.context).await?;
        let credentials =
            Authority::new("operator", profile_signer.clone(), operator_signer.clone());

        // Mint the session: one in-memory grant per allowed scope.
        let mut session = Vec::with_capacity(self.allowed.len());
        for (scope, expiration) in &self.allowed {
            let mut builder = DelegationBuilder::new()
                .issuer(dialog_credentials::Signer::from(profile_signer.clone()))
                .audience(&operator_signer)
                .subject(scope.subject.clone())
                .command(scope.command.segments().clone())
                .policy(scope.policy());
            if let Some(expiration) = expiration {
                builder = builder.expiration(*expiration);
            }
            let delegation = builder
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
            hydration: Arc::default(),
            speculation: Arc::default(),
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

/// Extract the ed25519 signer from a credential.
///
/// Operator derivation currently assumes an ed25519 profile key (the key
/// agreement it derives through and the `did:key` operator identity are
/// ed25519-specific). A profile backed by another algorithm is rejected here
/// rather than deriving a wrong operator.
fn ed25519_signer(credential: &SignerCredential) -> Result<Ed25519Signer, OperatorError> {
    credential
        .signer()
        .as_ed25519()
        .cloned()
        .ok_or_else(|| OperatorError::Key("operator derivation requires an ed25519 profile".into()))
}

/// Derive the operator key for `context` from the profile key.
///
/// One derivation for every platform, and the same one: the profile's
/// [`Ed25519Signer::secret`] handle runs a key agreement against the identity's
/// own agreement key (see [`dialog_credentials::secret::Secret::derive`]) and
/// the result seeds the operator.
///
/// It is NOT a signature, and the distinction is the whole point. The web arm
/// used to sign a fixed message and hash the signature, which assumes a
/// signature is a pseudo-random function. Ed25519 does not promise that:
/// hedged nonces are conforming, WebKit's `Ed25519` uses them, and in Safari
/// the profile therefore derived a different operator on every page load. That
/// churns the operator DID through `Origin`, which names a sequential actor in
/// the version clock, so every session became a new replica lineage. Key
/// agreement has no nonce to hedge.
///
/// See `notes/operator-derivation.md`.
async fn derive_operator(
    signer: &Ed25519Signer,
    context: &[u8],
) -> Result<Ed25519Signer, OperatorError> {
    let seed = signer
        .secret(OPERATOR_DERIVATION_CONTEXT)
        .derive(context)
        .await
        .map_err(|e| OperatorError::Key(e.to_string()))?;

    Ed25519Signer::import(&seed)
        .await
        .map_err(|e| OperatorError::Key(e.to_string()))
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
