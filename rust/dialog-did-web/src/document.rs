//! Minimal DID document model and verification-method key extraction.
//!
//! Only the fields needed to recover a verifying key are modeled. A
//! verification method is turned into an algorithm-agnostic
//! [`Verifier`](dialog_credentials::Verifier) by reconstructing the equivalent
//! `did:key` string (multicodec prefix + key bytes, base58btc) and reusing the
//! credential crate's tested `did:key` parser, so this crate never duplicates
//! the multicodec key parsing.

use base58::ToBase58;
use dialog_credentials::Verifier;
use dialog_varsig::Did;
use serde::Deserialize;

use crate::error::ResolveError;
use crate::verifier::MultiVerifier;

/// The ed25519 public-key multicodec prefix (`0xed 0x01`).
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// The P-256 public-key multicodec prefix (unsigned-varint of `0x1200`).
const P256_MULTICODEC: [u8; 2] = [0x80, 0x24];

/// A DID document, reduced to the verification methods.
#[derive(Debug, Clone, Deserialize)]
pub struct DidDocument {
    /// The document subject DID.
    #[serde(default)]
    pub id: Option<String>,

    /// The verification methods. May be absent or empty.
    #[serde(default, rename = "verificationMethod")]
    pub verification_method: Vec<VerificationMethod>,
}

/// A single verification method entry.
#[derive(Debug, Clone, Deserialize)]
pub struct VerificationMethod {
    /// The method identifier (e.g. `did:web:example.com#key-1`).
    #[serde(default)]
    pub id: Option<String>,

    /// The method type (e.g. `Ed25519VerificationKey2020`, `JsonWebKey2020`).
    #[serde(default, rename = "type")]
    pub type_: Option<String>,

    /// The controller DID.
    #[serde(default)]
    pub controller: Option<String>,

    /// A multibase-encoded public key (`z` + base58btc(multicodec + key)).
    #[serde(default, rename = "publicKeyMultibase")]
    pub public_key_multibase: Option<String>,

    /// A JSON Web Key public key.
    #[serde(default, rename = "publicKeyJwk")]
    pub public_key_jwk: Option<Jwk>,
}

/// The subset of a JWK needed to recover an ed25519 or P-256 public key.
#[derive(Debug, Clone, Deserialize)]
pub struct Jwk {
    /// Key type (`OKP` for ed25519, `EC` for P-256).
    #[serde(default)]
    pub kty: Option<String>,

    /// Curve (`Ed25519` or `P-256`).
    #[serde(default)]
    pub crv: Option<String>,

    /// The `x` coordinate, base64url (no padding).
    #[serde(default)]
    pub x: Option<String>,

    /// The `y` coordinate, base64url (no padding). Present for EC keys.
    #[serde(default)]
    pub y: Option<String>,
}

impl DidDocument {
    /// Recover a multi-key verifier from this document.
    ///
    /// A DID document names an *array* of verification methods, and a signature
    /// could have been produced by any of them; a resolver cannot know which.
    /// So this collects *every* supported key into a [`MultiVerifier`], which
    /// verifies a signature if any member key does. The header-authoritative
    /// signature already carries its algorithm, and each member verifier rejects
    /// a signature whose algorithm tag does not match, so trying all members
    /// only ever succeeds on a key of the right algorithm.
    ///
    /// If `fragment` is `Some`, the set is restricted to the verification method
    /// whose `id` ends with `#fragment` (the seed of a future `kid`-hint fast
    /// path); otherwise every supported method is included. A method with an
    /// unsupported key type is skipped, not fatal, as long as at least one
    /// supported key remains.
    ///
    /// The `did` is the DID this document was resolved for; the returned
    /// verifier reports it as its identity (not any single key's `did:key`).
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::NoSupportedVerificationMethod`] if no candidate
    /// method yields a usable key. If exactly one method was selected (e.g. via
    /// a fragment) and its key is unsupported, that method's
    /// [`ResolveError::UnsupportedKey`] is surfaced instead.
    pub fn verifier(
        &self,
        did: &Did,
        fragment: Option<&str>,
    ) -> Result<MultiVerifier, ResolveError> {
        let candidates: Vec<&VerificationMethod> = match fragment {
            Some(frag) => self
                .verification_method
                .iter()
                .filter(|m| method_matches_fragment(m, frag))
                .collect(),
            None => self.verification_method.iter().collect(),
        };

        if candidates.is_empty() {
            return Err(ResolveError::NoSupportedVerificationMethod);
        }

        let mut keys: Vec<Verifier> = Vec::new();
        let mut last_key_error: Option<ResolveError> = None;
        for method in candidates {
            match method.verifier() {
                Ok(verifier) => keys.push(verifier),
                Err(err @ ResolveError::UnsupportedKey(_)) => last_key_error = Some(err),
                Err(other) => return Err(other),
            }
        }

        if keys.is_empty() {
            return Err(last_key_error.unwrap_or(ResolveError::NoSupportedVerificationMethod));
        }

        Ok(MultiVerifier::new(did.clone(), keys))
    }
}

impl VerificationMethod {
    /// Recover an algorithm-agnostic verifier from this method.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::UnsupportedKey`] if the key encoding or type is
    /// not supported.
    pub fn verifier(&self) -> Result<Verifier, ResolveError> {
        if let Some(multibase) = &self.public_key_multibase {
            return verifier_from_multibase(multibase);
        }
        if let Some(jwk) = &self.public_key_jwk {
            return verifier_from_jwk(jwk);
        }
        Err(ResolveError::UnsupportedKey(
            "verification method has neither publicKeyMultibase nor publicKeyJwk".into(),
        ))
    }
}

/// Does this method's `id` select the given fragment?
fn method_matches_fragment(method: &VerificationMethod, fragment: &str) -> bool {
    let wanted = fragment.strip_prefix('#').unwrap_or(fragment);
    method.id.as_deref().is_some_and(|id| {
        id.rsplit_once('#')
            .map_or(id == wanted, |(_, frag)| frag == wanted)
    })
}

/// Build a verifier from a `z`-prefixed multibase public key.
///
/// The multibase already carries the multicodec prefix, so it is exactly the
/// tail of a `did:key`. Reconstruct the `did:key` and reuse the credential
/// crate's parser.
fn verifier_from_multibase(multibase: &str) -> Result<Verifier, ResolveError> {
    if !multibase.starts_with('z') {
        return Err(ResolveError::UnsupportedKey(format!(
            "unsupported multibase encoding (expected base58btc 'z'): {multibase}"
        )));
    }
    let did_key = format!("did:key:{multibase}");
    Verifier::from_did_key(&did_key)
        .map_err(|_| ResolveError::UnsupportedKey(format!("unsupported did:key key: {did_key}")))
}

/// Build a verifier from a JWK by re-encoding it as a `did:key`.
fn verifier_from_jwk(jwk: &Jwk) -> Result<Verifier, ResolveError> {
    let crv = jwk.crv.as_deref().unwrap_or_default();
    let did_key = match crv {
        "Ed25519" => {
            let x = decode_b64url(jwk.x.as_deref(), "x")?;
            if x.len() != 32 {
                return Err(ResolveError::UnsupportedKey(format!(
                    "Ed25519 JWK x must be 32 bytes, got {}",
                    x.len()
                )));
            }
            did_key_string(&ED25519_MULTICODEC, &x)
        }
        "P-256" => {
            let x = decode_b64url(jwk.x.as_deref(), "x")?;
            let y = decode_b64url(jwk.y.as_deref(), "y")?;
            if x.len() != 32 || y.len() != 32 {
                return Err(ResolveError::UnsupportedKey(format!(
                    "P-256 JWK x and y must be 32 bytes, got {} and {}",
                    x.len(),
                    y.len()
                )));
            }
            let compressed = compress_p256(&x, &y);
            did_key_string(&P256_MULTICODEC, &compressed)
        }
        other => {
            return Err(ResolveError::UnsupportedKey(format!(
                "unsupported JWK curve: {other}"
            )));
        }
    };

    Verifier::from_did_key(&did_key)
        .map_err(|_| ResolveError::UnsupportedKey(format!("could not build key from JWK: {crv}")))
}

/// Encode a multicodec prefix and key bytes as a `did:key:z...` string.
fn did_key_string(multicodec: &[u8], key: &[u8]) -> String {
    let mut raw = Vec::with_capacity(multicodec.len() + key.len());
    raw.extend_from_slice(multicodec);
    raw.extend_from_slice(key);
    format!("did:key:z{}", raw.as_slice().to_base58())
}

/// Compress an uncompressed P-256 point `(x, y)` to 33 bytes: a `0x02`/`0x03`
/// prefix chosen by the parity of `y`, followed by `x`.
fn compress_p256(x: &[u8], y: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(33);
    let prefix = if y.last().copied().unwrap_or(0) & 1 == 0 {
        0x02
    } else {
        0x03
    };
    out.push(prefix);
    out.extend_from_slice(x);
    out
}

/// Decode a base64url (unpadded) field, refusing an absent one.
fn decode_b64url(value: Option<&str>, field: &str) -> Result<Vec<u8>, ResolveError> {
    use base64::Engine as _;
    let value =
        value.ok_or_else(|| ResolveError::UnsupportedKey(format!("JWK missing {field}")))?;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|e| ResolveError::UnsupportedKey(format!("invalid base64url in JWK {field}: {e}")))
}
