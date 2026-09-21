//! [`SessionBuilder`]: narrows a [`Peer`] to a [`Session`].

use std::sync::Arc;

use super::{PeerSpace, Session};
use crate::Peer;
use dialog_capability::{Ability, Capability, Constraint};
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_identity::Authority;
use dialog_ucan::{Scope, UcanCertificate};
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};
use dialog_varsig::Principal as _;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use dialog_varsig::Signer;

const SESSION_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Where a session's acting key comes from.
enum SessionKey {
    /// Derived from the peer key and this context.
    Derived(Vec<u8>),
    /// Supplied by the caller. Boxed: a credential is an order of
    /// magnitude larger than a context.
    Supplied(Box<SignerCredential>),
}

/// Builder for a [`Session`]. Created by [`Peer::session`].
pub struct SessionBuilder<S: Clone> {
    peer: Peer<S>,
    key: SessionKey,
    allowed: Vec<(Scope, Option<Timestamp>)>,
    certificates: Vec<UcanCertificate>,
}

impl<S: Clone> SessionBuilder<S> {
    pub(crate) fn derive(peer: Peer<S>, context: Vec<u8>) -> Self {
        Self {
            peer,
            key: SessionKey::Derived(context),
            allowed: Vec::new(),
            certificates: Vec::new(),
        }
    }

    /// Use `credential` as the session key instead of deriving one.
    ///
    /// The peer still mints the session's grants to it, so a supplied key
    /// is constrained exactly as a derived one is. Unlike a derived key,
    /// its DID is known before `build`, so a certificate someone else
    /// issued to it can be passed through [`grant`](Self::grant).
    pub fn credential(mut self, credential: SignerCredential) -> Self {
        self.key = SessionKey::Supplied(Box::new(credential));
        self
    }

    /// Allow a capability: the peer delegates it to the session at build.
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

    /// Hold a pre-minted certificate as a session grant.
    ///
    /// For a certificate whose audience is this session's key: one issued
    /// to a supplied credential, or to a derived DID from an earlier run.
    pub fn grant(mut self, certificate: UcanCertificate) -> Self {
        self.certificates.push(certificate);
        self
    }
}

impl<S: PeerSpace> SessionBuilder<S> {
    /// Build the session: resolve its key and mint its grants.
    ///
    /// Every allowed scope becomes a peer-to-session delegation held **in
    /// memory**. Nothing is persisted: a derived key re-mints identical
    /// authority on every build, and persisting it would only accumulate
    /// (one immortal certificate per session was exactly the field
    /// pathology).
    pub async fn build(self) -> Result<Session<S>, PeerError> {
        let peer_signer = ed25519_signer(self.peer.credential())?;
        let session_signer: dialog_credentials::Signer = match self.key {
            SessionKey::Derived(context) => derive_session(&peer_signer, &context).await?.into(),
            SessionKey::Supplied(credential) => credential.signer().clone(),
        };
        let session_did = session_signer.did();
        let authority = Authority::new("session", peer_signer.clone(), session_signer);

        let mut grants = self.certificates;
        grants.reserve(self.allowed.len());
        for (scope, expiration) in &self.allowed {
            let mut builder = DelegationBuilder::new()
                .issuer(dialog_credentials::Signer::from(peer_signer.clone()))
                .audience(&session_did)
                .subject(scope.subject.clone())
                .command(scope.command.segments().clone())
                .policy(scope.policy());
            if let Some(expiration) = expiration {
                builder = builder.expiration(*expiration);
            }
            let delegation = builder
                .try_build()
                .await
                .map_err(|e| PeerError::Delegation(format!("{e:?}")))?;
            grants.push(UcanCertificate(delegation));
        }

        Ok(Session::new(self.peer, authority, Arc::new(grants)))
    }
}

/// Extract the ed25519 signer from a credential.
///
/// Session derivation currently assumes an ed25519 peer key (the blake3
/// derivation and `did:key` session identity are ed25519-specific). A peer
/// backed by another algorithm is rejected here rather than deriving a
/// wrong session key.
fn ed25519_signer(credential: &SignerCredential) -> Result<Ed25519Signer, PeerError> {
    credential
        .signer()
        .as_ed25519()
        .cloned()
        .ok_or_else(|| PeerError::Key("session derivation requires an ed25519 peer".into()))
}

async fn derive_session(
    signer: &Ed25519Signer,
    context: &[u8],
) -> Result<Ed25519Signer, PeerError> {
    let export = signer
        .export()
        .await
        .map_err(|e| PeerError::Key(e.to_string()))?;

    match export {
        KeyExport::Extractable(ref seed) => {
            let mut key_material = seed.clone();
            key_material.extend_from_slice(context);

            let derived = blake3::derive_key(SESSION_DERIVATION_CONTEXT, &key_material);
            Ed25519Signer::import(&derived)
                .await
                .map_err(|e| PeerError::Key(e.to_string()))
        }
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        KeyExport::NonExtractable { .. } => {
            let mut derivation_input = SESSION_DERIVATION_CONTEXT.as_bytes().to_vec();
            derivation_input.extend_from_slice(context);

            let signature = signer
                .sign(&derivation_input)
                .await
                .map_err(|e| PeerError::Key(e.to_string()))?;

            let sig_bytes: [u8; 64] = signature.into();
            let derived = blake3::derive_key(SESSION_DERIVATION_CONTEXT, &sig_bytes);
            Ed25519Signer::import(&derived)
                .await
                .map_err(|e| PeerError::Key(e.to_string()))
        }
    }
}

/// Errors that can occur when opening a peer or building a session.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    /// Key derivation or generation failed.
    #[error("Key error: {0}")]
    Key(String),

    /// Delegation creation failed.
    #[error("Delegation error: {0}")]
    Delegation(String),

    /// Opening or loading the peer's credential failed.
    #[error("Open error: {0}")]
    Open(String),
}
