//! The DKIM domain public key and the DNS TXT record that carries it.
//!
//! A DKIM public key lives in DNS as a TXT record at
//! `<selector>._domainkey.<domain>`. The record is a `;`-separated tag list; the
//! ones that matter are `k=` (key type: `rsa` (default) or `ed25519`) and `p=`
//! (the base64 public key). For RSA the `p=` value is a
//! SubjectPublicKeyInfo (X.509 SPKI) DER blob; for ed25519 it is the raw 32-byte
//! public key.

use super::error::DkimError;
use super::signature::SignatureAlgorithm;

/// The key type named by a DKIM DNS record's `k=` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DkimKeyType {
    /// `k=rsa` (the default when `k=` is absent).
    Rsa,
    /// `k=ed25519`.
    Ed25519,
}

/// A DKIM domain public key, parsed and ready to verify a header signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DkimPublicKey {
    /// An RSA public key, stored as its SubjectPublicKeyInfo DER bytes (exactly
    /// what the DNS `p=` tag base64-decodes to). Kept as DER so the crate has no
    /// public dependency on a specific RSA key type in its signature.
    Rsa {
        /// The SubjectPublicKeyInfo (X.509 SPKI) DER encoding of the key.
        spki_der: Vec<u8>,
    },
    /// An ed25519 public key: the raw 32-byte point.
    Ed25519 {
        /// The 32-byte ed25519 public key.
        public_key: [u8; 32],
    },
}

impl DkimPublicKey {
    /// The algorithm this key can verify.
    #[must_use]
    pub const fn key_type(&self) -> DkimKeyType {
        match self {
            Self::Rsa { .. } => DkimKeyType::Rsa,
            Self::Ed25519 { .. } => DkimKeyType::Ed25519,
        }
    }

    /// Whether this key type matches the signature's `a=` algorithm.
    #[must_use]
    pub const fn matches(&self, algorithm: SignatureAlgorithm) -> bool {
        matches!(
            (self, algorithm),
            (Self::Rsa { .. }, SignatureAlgorithm::RsaSha256)
                | (Self::Ed25519 { .. }, SignatureAlgorithm::Ed25519Sha256)
        )
    }

    /// Build an RSA key from its SubjectPublicKeyInfo DER bytes.
    #[must_use]
    pub fn rsa_from_spki_der(spki_der: Vec<u8>) -> Self {
        Self::Rsa { spki_der }
    }

    /// Build an ed25519 key from its 32-byte public key.
    #[must_use]
    pub const fn ed25519_from_bytes(public_key: [u8; 32]) -> Self {
        Self::Ed25519 { public_key }
    }

    /// Parse a DKIM DNS TXT record body (the `p=`/`k=` tag string) into a key.
    ///
    /// # Errors
    ///
    /// Returns [`DkimError::MalformedDnsRecord`] if `p=` is missing or the key
    /// type is unsupported, or [`DkimError::InvalidBase64`] /
    /// [`DkimError::InvalidPublicKey`] if the `p=` value is not a valid key.
    #[cfg(feature = "dkim")]
    pub fn from_dns_txt(record: &str) -> Result<Self, DkimError> {
        use base64::Engine;

        let mut key_type = DkimKeyType::Rsa; // `k=` defaults to rsa.
        let mut p_value: Option<String> = None;

        for chunk in record.split(';') {
            let chunk = chunk.trim();
            if chunk.is_empty() {
                continue;
            }
            let Some((tag, value)) = chunk.split_once('=') else {
                continue;
            };
            match tag.trim() {
                "k" => {
                    key_type = match value.trim() {
                        "rsa" => DkimKeyType::Rsa,
                        "ed25519" => DkimKeyType::Ed25519,
                        other => {
                            return Err(DkimError::MalformedDnsRecord(format!(
                                "unsupported key type k={other}"
                            )));
                        }
                    };
                }
                "p" => {
                    // A revoked key publishes an empty p=; treat that as an
                    // error rather than a valid key.
                    let cleaned: String = value.chars().filter(|c| !c.is_whitespace()).collect();
                    p_value = Some(cleaned);
                }
                _ => {}
            }
        }

        let p = p_value.ok_or_else(|| DkimError::MalformedDnsRecord("missing p= tag".into()))?;
        if p.is_empty() {
            return Err(DkimError::MalformedDnsRecord(
                "empty p= tag (key revoked)".into(),
            ));
        }

        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(p.as_bytes())
            .map_err(|_| DkimError::InvalidBase64("p=".into()))?;

        match key_type {
            DkimKeyType::Rsa => Ok(Self::Rsa {
                spki_der: key_bytes,
            }),
            DkimKeyType::Ed25519 => {
                let public_key: [u8; 32] = key_bytes.try_into().map_err(|_| {
                    DkimError::InvalidPublicKey("ed25519 key is not 32 bytes".into())
                })?;
                Ok(Self::Ed25519 { public_key })
            }
        }
    }
}

#[cfg(all(test, feature = "dkim"))]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn parses_ed25519_record() {
        use base64::Engine;
        let raw_key = [7u8; 32];
        let p = base64::engine::general_purpose::STANDARD.encode(raw_key);
        let record = format!("v=DKIM1; k=ed25519; p={p}");
        let key = DkimPublicKey::from_dns_txt(&record).unwrap();
        assert_eq!(key.key_type(), DkimKeyType::Ed25519);
        assert!(key.matches(SignatureAlgorithm::Ed25519Sha256));
        assert!(!key.matches(SignatureAlgorithm::RsaSha256));
    }

    #[dialog_common::test]
    fn key_type_defaults_to_rsa() {
        use base64::Engine;
        let p = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3, 4]);
        let record = format!("v=DKIM1; p={p}");
        let key = DkimPublicKey::from_dns_txt(&record).unwrap();
        assert_eq!(key.key_type(), DkimKeyType::Rsa);
    }

    #[dialog_common::test]
    fn missing_p_is_rejected() {
        assert!(matches!(
            DkimPublicKey::from_dns_txt("v=DKIM1; k=rsa"),
            Err(DkimError::MalformedDnsRecord(_))
        ));
    }

    #[dialog_common::test]
    fn empty_p_is_rejected() {
        assert!(matches!(
            DkimPublicKey::from_dns_txt("v=DKIM1; k=rsa; p="),
            Err(DkimError::MalformedDnsRecord(_))
        ));
    }

    #[dialog_common::test]
    fn folded_p_value_is_reassembled() {
        use base64::Engine;
        let raw_key = [9u8; 32];
        let p = base64::engine::general_purpose::STANDARD.encode(raw_key);
        // Split the base64 across whitespace, as long TXT records often are.
        let half = p.len() / 2;
        let record = format!("k=ed25519; p={} {}", &p[..half], &p[half..]);
        let key = DkimPublicKey::from_dns_txt(&record).unwrap();
        assert_eq!(
            key,
            DkimPublicKey::Ed25519 {
                public_key: raw_key
            }
        );
    }
}
