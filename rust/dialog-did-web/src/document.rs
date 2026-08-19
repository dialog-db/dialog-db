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
#[cfg(feature = "es256")]
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
    /// # Binding the answer to the question
    ///
    /// Two checks tie the document back to the DID being resolved, because
    /// fetching a document proves only that *some* host served *some* JSON:
    ///
    /// - The document's `id` must equal `did`. Without it, a document served at
    ///   the victim's URL but naming the attacker (and carrying the attacker's
    ///   key) binds that key to the victim's DID, so the attacker's signatures
    ///   verify as the victim. Any party that can influence the response body —
    ///   a redirect to another origin, a shared host, the directory itself —
    ///   gets a full authentication bypass.
    /// - A verification method whose `controller` names a *different* DID is
    ///   excluded. The subject never authorized that key, so it must not join
    ///   this DID's key set. A method with no `controller` is taken as
    ///   controlled by the subject, which is the DID-core default.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::MalformedDocument`] if the document's `id` is
    /// absent or names a different DID, or
    /// [`ResolveError::NoSupportedVerificationMethod`] if no candidate method
    /// yields a usable key. If exactly one method was selected (e.g. via a
    /// fragment) and its key is unsupported, that method's
    /// [`ResolveError::UnsupportedKey`] is surfaced instead.
    pub fn verifier(
        &self,
        did: &Did,
        fragment: Option<&str>,
    ) -> Result<MultiVerifier, ResolveError> {
        // The document must be *this* DID's document.
        match self.id.as_deref() {
            Some(id) if id == did.as_str() => {}
            Some(id) => {
                return Err(ResolveError::MalformedDocument(format!(
                    "document id {id} does not match the resolved DID {did}"
                )));
            }
            None => {
                return Err(ResolveError::MalformedDocument(format!(
                    "document for {did} has no id"
                )));
            }
        }

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
            // A key some other DID controls is not this DID's to speak with.
            if !method.is_controlled_by(did) {
                continue;
            }
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
    /// Is this method controlled by `did`?
    ///
    /// An absent `controller` means the document subject controls the method,
    /// which is the DID-core default. A `controller` naming another DID means
    /// the key belongs to someone else and must not join this DID's key set.
    #[must_use]
    pub fn is_controlled_by(&self, did: &Did) -> bool {
        self.controller
            .as_deref()
            .is_none_or(|controller| controller == did.as_str())
    }

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
///
/// `kty` is checked against the curve rather than ignored: `crv` alone would
/// let `{"kty":"RSA","crv":"P-256",...}` be read as a P-256 key, accepting a
/// key the document does not actually declare.
fn verifier_from_jwk(jwk: &Jwk) -> Result<Verifier, ResolveError> {
    let crv = jwk.crv.as_deref().unwrap_or_default();
    let kty = jwk.kty.as_deref().unwrap_or_default();

    let expected_kty = match crv {
        "Ed25519" => "OKP",
        #[cfg(feature = "es256")]
        "P-256" => "EC",
        other => {
            return Err(ResolveError::UnsupportedKey(format!(
                "unsupported JWK curve: {other}"
            )));
        }
    };
    if kty != expected_kty {
        return Err(ResolveError::UnsupportedKey(format!(
            "JWK curve {crv} requires kty {expected_kty}, got {kty}"
        )));
    }

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
        #[cfg(feature = "es256")]
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
            let compressed = compress_p256(&x, &y)?;
            did_key_string(&P256_MULTICODEC, &compressed)
        }
        // Every other curve is refused above, where `kty` is checked against
        // the curve. Repeat the refusal rather than panicking, so a future
        // curve added to that match cannot turn into an unreachable panic.
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

/// Compress a P-256 point `(x, y)` to its 33-byte SEC1 form, refusing a point
/// that is not actually on the curve.
///
/// # Why `y` is verified rather than reduced to its parity
///
/// Compression keeps only `x` and one parity bit of `y`, because a valid curve
/// point is fully determined by them. Deriving the prefix straight from
/// `y.last() & 1` therefore *discards* `y` — so a document could publish any
/// `y` whatsoever, and as long as its low bit matched, resolution would
/// silently produce the key that `x` alone names. A JWK whose `y` does not
/// satisfy the curve equation is a malformed key, and a resolver feeding a
/// signature check must refuse it rather than reinterpret it as a different,
/// well-formed key the document never published.
///
/// Round-tripping through the uncompressed SEC1 encoding (`0x04 || x || y`)
/// hands that check to the `p256` crate, which rejects an off-curve point.
#[cfg(feature = "es256")]
fn compress_p256(x: &[u8], y: &[u8]) -> Result<Vec<u8>, ResolveError> {
    let mut uncompressed = Vec::with_capacity(1 + x.len() + y.len());
    uncompressed.push(0x04);
    uncompressed.extend_from_slice(x);
    uncompressed.extend_from_slice(y);

    let point = p256::EncodedPoint::from_bytes(&uncompressed).map_err(|e| {
        ResolveError::UnsupportedKey(format!("P-256 JWK is not a valid SEC1 point: {e}"))
    })?;
    // `from_encoded_point` is what rejects a point off the curve; the encoded
    // form above only checks the length and tag.
    let key = p256::PublicKey::try_from(&point).map_err(|_| {
        ResolveError::UnsupportedKey("P-256 JWK x and y are not a point on the curve".to_string())
    })?;

    Ok(
        p256::elliptic_curve::sec1::ToEncodedPoint::to_encoded_point(&key, true)
            .as_bytes()
            .to_vec(),
    )
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
