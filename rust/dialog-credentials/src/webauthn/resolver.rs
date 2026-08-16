//! WebAuthn P-256 `did:key` resolver.

use super::{error::WebAuthnResolveError, verifier::WebAuthnVerifier};
use dialog_varsig::{Did, Verifier, resolver::Resolver, webauthn::WebAuthnSignature};

/// Resolves WebAuthn `did:key` strings to [`WebAuthnVerifier`]s.
///
/// The WebAuthn `did:key` carries the private-use WebAuthn multicodec prefix,
/// so this resolver handles passkey identities specifically; a plain `p256-pub`
/// `did:key` is resolved by the ES256 resolver instead.
#[derive(Debug, Clone, Copy)]
pub struct WebAuthnKeyResolver;

impl Resolver<WebAuthnSignature> for WebAuthnKeyResolver {
    type Error = WebAuthnResolveError;

    async fn resolve(&self, did: &Did) -> Result<impl Verifier<WebAuthnSignature>, Self::Error> {
        let verifier: WebAuthnVerifier = did.as_str().parse()?;
        Ok(verifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use p256::ecdsa::{SigningKey, signature::Signer as _};
    use sha2::{Digest, Sha256};

    fn sign_webauthn(sk: &SigningKey, payload: &[u8]) -> WebAuthnSignature {
        let payload_hash = Sha256::digest(payload);
        let mut multihash = vec![0x12u8, 0x20];
        multihash.extend_from_slice(&payload_hash);
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&multihash);
        let client_data_json = serde_json::to_vec(&serde_json::json!({
            "type": "webauthn.get",
            "challenge": challenge,
            "origin": "https://example.com",
        }))
        .unwrap();

        let rp_id_hash = Sha256::digest(b"example.com");
        let mut authenticator_data = rp_id_hash.to_vec();
        authenticator_data.push(0x05);
        authenticator_data.extend_from_slice(&[0, 0, 0, 1]);

        let mut signed = authenticator_data.clone();
        signed.extend_from_slice(&Sha256::digest(&client_data_json));
        let ecdsa_sig: p256::ecdsa::DerSignature = sk.sign(&signed);

        WebAuthnSignature::new(
            client_data_json,
            authenticator_data,
            ecdsa_sig.to_bytes().to_vec(),
        )
    }

    #[dialog_common::test]
    async fn resolves_webauthn_did_key() {
        let sk = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let compressed = sk.verifying_key().to_encoded_point(true);
        let verifier = WebAuthnVerifier::from_sec1_bytes(compressed.as_bytes()).unwrap();
        let did: Did = verifier.to_string().parse().unwrap();

        // Resolving yields a verifier that accepts a signature from the key.
        let resolved = WebAuthnKeyResolver.resolve(&did).await.unwrap();
        let payload = b"resolve me";
        let sig = sign_webauthn(&sk, payload);
        resolved.verify(payload, &sig).await.unwrap();
    }
}
