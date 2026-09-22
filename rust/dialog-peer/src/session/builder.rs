//! [`SessionBuilder`]: narrows a [`Peer`] to a [`Session`].

use std::sync::Arc;

use super::{PeerSpace, Session};
use crate::Peer;
use dialog_capability::{Ability, Capability, Constraint, Subject};
use dialog_credentials::secret::Context;
use dialog_credentials::{Ed25519Signer, SignerCredential};
use dialog_identity::Authority;
use dialog_identity::access::Claim;
use dialog_ucan::{Scope, UcanCertificate};
use dialog_ucan_core::{DelegationBuilder, time::Timestamp};
use dialog_varsig::{Did, Principal as _};

/// The domain-separation label session keys derive under.
///
/// Versioned: `v2` is the key-agreement derivation that replaced signing a
/// fixed message. The label is the one the operator derivation used, so a
/// session derived here is the operator the same context derived before
/// the rename. Bumping it re-derives every session key, which forks each
/// peer's replica lineage, so it changes only when the derivation does.
const SESSION_DERIVATION_CONTEXT: Context = Context::new("dialog-db/operator/v2");

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
/// One derivation for every platform, and the same one: the signer runs a
/// key agreement against its own agreement key and imports the result, so
/// the session key arrives as a signer and the derived material is never a
/// value this code holds. Deterministic per `(key, context)`.
///
/// It is NOT a signature, and the distinction is the whole point. The web
/// arm used to sign a fixed message and hash the signature, which assumes
/// a signature is a pseudo-random function. Ed25519 does not promise that:
/// hedged nonces are conforming, WebKit's `Ed25519` uses them, and in
/// Safari a peer therefore derived a different session key on every page
/// load. Key agreement has no nonce to hedge. See
/// `notes/operator-derivation.md`.
///
/// Assumes an ed25519 key (the agreement it derives through and the
/// `did:key` session identity are ed25519-specific); a peer backed by
/// another algorithm is rejected rather than deriving a wrong key.
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
    signer
        .secret(SESSION_DERIVATION_CONTEXT)
        .derive(context)
        .await
        .map_err(|e| PeerError::Key(e.to_string()))
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

#[cfg(test)]
mod tests {
    use super::derive_credential;
    use dialog_credentials::{Ed25519Signer, SignerCredential};
    use dialog_varsig::Principal as _;

    /// A fixture pinning the whole derivation: a fixed peer seed and a
    /// fixed context derive one fixed session DID.
    ///
    /// The session tests show the derivation is stable within a run, which
    /// a randomized derivation would also pass on any platform whose
    /// Ed25519 does not hedge its nonce. This pins the value itself, so it
    /// fails for a change anywhere in the chain that produces it: the
    /// agreement key, the key agreement, the KDF, the context label, the
    /// seed-to-Ed25519 import, or the `did:key` encoding. It fails
    /// identically on native and wasm, which is what keeps the two
    /// platforms from deriving different keys from one peer again.
    ///
    /// The expected DID is the one the operator derivation pinned under
    /// the same label, so this also fixes the rename as identity-preserving.
    /// `dialog_credentials`' known-answer vector pins the derived secret;
    /// this pins what that secret becomes. If the derivation changes on
    /// purpose, bump `SESSION_DERIVATION_CONTEXT` and record the new DID
    /// deliberately. See `notes/operator-derivation.md`.
    #[dialog_common::test]
    async fn it_derives_a_fixed_session_did_from_a_fixed_seed() {
        // RFC 8032 test vector 1, used here only as a stable arbitrary seed.
        const PEER_SEED: [u8; 32] = [
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ];
        const CONTEXT: &[u8] = b"fixture";
        const EXPECTED_SESSION_DID: &str =
            "did:key:z6MkgAajey1H5u8MLHYnN7YUPd8Pjcvi4MhBtUqqgaRFJbJe";

        let peer = SignerCredential::from(Ed25519Signer::import(&PEER_SEED).await.unwrap());
        let session = derive_credential(&peer, CONTEXT).await.unwrap();

        assert_eq!(session.did().to_string(), EXPECTED_SESSION_DID);
    }
}
