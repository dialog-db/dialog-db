use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::{DialogArtifactsError, Entity, make_reference};
use dialog_storage::Blake3Hash;

use super::{Authority, Cause, Edition, Issuer, Origin, Signature, Version};

/// A signed, content-addressed record of a commit.
///
/// A [`Revision`]'s *payload* is the deterministic encoding of
/// `{tree, edition, subject, issuer, authority, cause}`. The signature is the
/// issuer's Ed25519 signature over `Blake3(payload)`, and the revision's
/// content address is `Blake3(payload + signature)`. The edition is not
/// free-standing state: it is derived from the revision DAG as
/// `max(edition of every version in cause) + 1` (see [`Cause::edition`]),
/// and [`Revision::verify`] enforces this structural rule alongside the
/// signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    tree: Blake3Hash,
    edition: Edition,
    subject: Entity,
    issuer: Issuer,
    authority: Authority,
    cause: Cause,
    signature: Signature,
}

impl Revision {
    /// Issue a new [`Revision`] over the given search tree root, for the
    /// repository identified by `subject`, superseding the parent revisions
    /// identified by `cause`. The revision's edition is derived from the
    /// cause, and the payload is signed with the given key (whose verifying
    /// key becomes the issuer).
    pub fn issue(
        tree: Blake3Hash,
        subject: Entity,
        authority: Authority,
        cause: Cause,
        signing_key: &SigningKey,
    ) -> Self {
        let issuer = Issuer::from(signing_key.verifying_key());
        let edition = cause.edition();
        let payload = encode_payload(&tree, &edition, &subject, &issuer, &authority, &cause);
        let signature = Signature::from(signing_key.sign(&make_reference(&payload)));

        Self {
            tree,
            edition,
            subject,
            issuer,
            authority,
            cause,
            signature,
        }
    }

    /// The root of the search tree at this revision
    pub fn tree(&self) -> &Blake3Hash {
        &self.tree
    }

    /// The causal depth of this revision
    pub fn edition(&self) -> Edition {
        self.edition
    }

    /// The DID of the repository this revision belongs to
    pub fn subject(&self) -> &Entity {
        &self.subject
    }

    /// The principal that committed and signed this revision
    pub fn issuer(&self) -> &Issuer {
        &self.issuer
    }

    /// The principal on whose behalf this revision was committed
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// The versions of the parent revisions superseded by this one
    pub fn cause(&self) -> &Cause {
        &self.cause
    }

    /// The issuer's signature over `Blake3(payload)`
    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The [`Origin`] of this revision, derived from its issuer and subject.
    /// Stored nowhere; always computed on demand.
    pub fn origin(&self) -> Origin {
        Origin::derive(&self.issuer, &self.subject)
    }

    /// The [`Version`] identifying this revision
    pub fn version(&self) -> Version {
        Version::new(self.origin(), self.edition)
    }

    /// The deterministic payload encoding covered by the signature
    pub fn payload(&self) -> Vec<u8> {
        encode_payload(
            &self.tree,
            &self.edition,
            &self.subject,
            &self.issuer,
            &self.authority,
            &self.cause,
        )
    }

    /// The content address of this revision: `Blake3(payload + signature)`.
    /// This serves as the revision's stable entity identifier (`this`) when
    /// the revision is stored as claims.
    pub fn reference(&self) -> Blake3Hash {
        make_reference([self.payload().as_slice(), self.signature.0.as_slice()].concat())
    }

    /// The content address of this revision expressed as an [`Entity`],
    /// suitable for use as the subject of the revision's own attribute claims
    pub fn entity(&self) -> Result<Entity, DialogArtifactsError> {
        use base58::ToBase58;
        format!("blake3:{}", self.reference().to_base58()).parse()
    }

    /// Verify this revision's structural integrity: the edition must match
    /// the one derived from the cause, and the signature must be a valid
    /// signature by the issuer over `Blake3(payload)`
    pub fn verify(&self) -> Result<(), DialogArtifactsError> {
        if self.edition != self.cause.edition() {
            return Err(DialogArtifactsError::InvalidSignature(format!(
                "Edition {} does not match the edition {} derived from the cause",
                self.edition,
                self.cause.edition()
            )));
        }

        let verifying_key = VerifyingKey::from_bytes(&self.issuer.0).map_err(|error| {
            DialogArtifactsError::InvalidSignature(format!("Invalid issuer key: {error}"))
        })?;

        verifying_key
            .verify_strict(
                &make_reference(self.payload()),
                &ed25519_dalek::Signature::from_bytes(&self.signature.0),
            )
            .map_err(|error| {
                DialogArtifactsError::InvalidSignature(format!("Signature mismatch: {error}"))
            })
    }
}

/// Deterministic encoding of a revision payload:
///
/// ```text
/// tree (32)
/// edition (8, big-endian)
/// subject length (8, big-endian) ++ subject (UTF-8)
/// issuer (32)
/// authority (32)
/// cause count (8, big-endian) ++ versions (40 each, sorted)
/// ```
fn encode_payload(
    tree: &Blake3Hash,
    edition: &Edition,
    subject: &Entity,
    issuer: &Issuer,
    authority: &Authority,
    cause: &Cause,
) -> Vec<u8> {
    let subject = subject.as_str().as_bytes();
    let versions = cause.versions();
    let mut bytes = Vec::with_capacity(120 + subject.len() + versions.len() * 40);

    bytes.extend_from_slice(tree);
    bytes.extend_from_slice(&edition.key_bytes());
    bytes.extend_from_slice(&(subject.len() as u64).to_be_bytes());
    bytes.extend_from_slice(subject);
    bytes.extend_from_slice(&issuer.0);
    bytes.extend_from_slice(&authority.0);
    bytes.extend_from_slice(&(versions.len() as u64).to_be_bytes());
    for version in versions {
        bytes.extend_from_slice(&version.key_bytes());
    }

    bytes
}
