//! [`SessionBuilder`]: narrows a [`Peer`] to a [`Session`].

use std::sync::Arc;

use super::{PeerSpace, Session};
use crate::Peer;
use dialog_capability::{Ability, Capability, Constraint, Subject};
use dialog_credentials::key::KeyExport;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_identity::Authority;
use dialog_identity::access::Claim;
use dialog_ucan::{Scope, UcanCertificate};
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use dialog_varsig::Signer;
use dialog_varsig::{Did, Principal as _};

const SESSION_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// A scope the peer delegates to the session at build.
///
/// Made from a bare capability (unbounded, claimed by the peer) or from a
/// [`Claim`], which carries its validity window and names who claimed it.
/// A claim by anyone but the peer is refused at build: the peer is the
/// only issuer of a session's grants.
#[derive(Debug, Clone)]
pub struct Allowance {
    scope: Scope,
    issuer: Option<Did>,
    not_before: Option<Timestamp>,
    expiration: Option<Timestamp>,
}

impl<T> From<Capability<T>> for Allowance
where
    T: Constraint,
    Capability<T>: Ability,
{
    fn from(capability: Capability<T>) -> Self {
        Allowance {
            scope: Scope::from(&capability),
            issuer: None,
            not_before: None,
            expiration: None,
        }
    }
}

impl From<Subject> for Allowance {
    fn from(subject: Subject) -> Self {
        Capability::from(subject).into()
    }
}

impl<C> From<Claim<'_, C>> for Allowance
where
    C: Constraint,
    Capability<C>: Ability,
{
    fn from(claim: Claim<'_, C>) -> Self {
        Allowance {
            scope: Scope::from(claim.capability()),
            issuer: Some(claim.issuer()),
            not_before: claim.activation(),
            expiration: claim.expiration(),
        }
    }
}

/// Builder for a [`Session`]. Created by [`Peer::session`].
pub struct SessionBuilder<S: Clone> {
    peer: Peer<S>,
    credential: SignerCredential,
    allowed: Vec<Allowance>,
    certificates: Vec<UcanCertificate>,
}

impl<S: Clone> SessionBuilder<S> {
    pub(crate) fn new(peer: Peer<S>, credential: SignerCredential) -> Self {
        Self {
            peer,
            credential,
            allowed: Vec::new(),
            certificates: Vec::new(),
        }
    }

    /// The session's DID. Known before build, so a certificate someone
    /// else issued to it can be passed through [`grant`](Self::grant).
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// Allow a scope: the peer delegates it to the session at build.
    ///
    /// Takes a capability for an unbounded grant, or a [`Claim`] made
    /// through the peer's [`access`](Peer::access) for a bounded one:
    /// `peer.access().claim(cap).expires(t)`.
    pub fn allow(mut self, allowance: impl Into<Allowance>) -> Self {
        self.allowed.push(allowance.into());
        self
    }

    /// Hold a pre-minted certificate as a session grant.
    ///
    /// For a certificate whose audience is this session's key, which
    /// [`did`](Self::did) names before build.
    pub fn grant(mut self, certificate: UcanCertificate) -> Self {
        self.certificates.push(certificate);
        self
    }
}

impl<S: PeerSpace> SessionBuilder<S> {
    /// Build the session: mint its grants to its key.
    ///
    /// Every allowance becomes a peer-to-session delegation held **in
    /// memory**. Nothing is persisted: a derived key re-mints identical
    /// authority on every build, and persisting it would only accumulate
    /// (one immortal certificate per session was exactly the field
    /// pathology).
    pub async fn build(self) -> Result<Session<S>, PeerError> {
        let peer_did = self.peer.did();
        let peer_signer = self.peer.credential().signer().clone();
        let session_signer = self.credential.signer().clone();
        let session_did = session_signer.did();
        let authority = Authority::new("session", peer_signer.clone(), session_signer);

        let mut grants = self.certificates;
        grants.reserve(self.allowed.len());
        for allowance in &self.allowed {
            if let Some(issuer) = &allowance.issuer
                && *issuer != peer_did
            {
                return Err(PeerError::Delegation(format!(
                    "allowance claimed by {issuer}, not by this peer ({peer_did})"
                )));
            }
            let mut builder = DelegationBuilder::new()
                .issuer(peer_signer.clone())
                .audience(&session_did)
                .subject(allowance.scope.subject.clone())
                .command(allowance.scope.command.segments().clone())
                .policy(allowance.scope.policy());
            if let Some(not_before) = allowance.not_before {
                builder = builder.not_before(not_before);
            }
            if let Some(expiration) = allowance.expiration {
                builder = builder.expiration(expiration);
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

/// Derive a session credential from `credential` and `context`.
///
/// Deterministic per `(key, context)`. Assumes an ed25519 key (the blake3
/// derivation and `did:key` session identity are ed25519-specific); a
/// peer backed by another algorithm is rejected rather than deriving a
/// wrong key.
pub(crate) async fn derive_credential(
    credential: &SignerCredential,
    context: &[u8],
) -> Result<SignerCredential, PeerError> {
    let signer = credential
        .signer()
        .as_ed25519()
        .cloned()
        .ok_or_else(|| PeerError::Key("session derivation requires an ed25519 peer".into()))?;
    Ok(SignerCredential::from(
        derive_session(&signer, context).await?,
    ))
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
