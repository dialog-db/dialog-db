//! RSA (PKCS#1 v1.5, SHA-256) key types, DID, and signer implementations.
//!
//! RSA is a normal algorithm arm alongside ed25519 and es256. Two key sizes are
//! supported, RSA-2048 and RSA-4096, distinguished by the varsig key-size tag
//! and by [`RsaKeySize`]. Unlike the fixed-width elliptic-curve keys, an RSA
//! public key is variable length (its modulus), so a `did:key` carries the key
//! as PKCS#1 DER after the `rsa-pub` (`0x1205`) multicodec.
//!
//! Only a native (`rsa` crate) arm is provided. Browser WebCrypto RSA support is
//! out of scope, but the key wrappers keep the same enum shape as the es256
//! module so a future `WebCrypto` arm can be added additively.

use dialog_varsig::AlgorithmTag;
use rsa::pkcs1::{
    DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPrivateKey, EncodeRsaPublicKey,
};
use rsa::pkcs1v15::{Signature as RsaCrateSignature, SigningKey, VerifyingKey};
use rsa::sha2::Sha256;
use rsa::signature::{Keypair as _, SignatureEncoding as _, Signer as _, Verifier as _};
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};

mod error;
mod resolver;
mod signer;
mod verifier;

pub use error::{RsaDidFromStrError, RsaKeyError, RsaResolveError, RsaSignerError};
pub use resolver::RsaKeyResolver;
pub use signer::RsaSigner;
pub use verifier::RsaVerifier;

/// The supported RSA key sizes.
///
/// The signature width and the varsig key-size tag both follow from this: a
/// 2048-bit key produces a 256-byte signature tagged [`AlgorithmTag::Rsa2048`],
/// a 4096-bit key a 512-byte signature tagged [`AlgorithmTag::Rsa4096`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RsaKeySize {
    /// RSA-2048 (256-byte signatures).
    Rsa2048,
    /// RSA-4096 (512-byte signatures).
    Rsa4096,
}

impl RsaKeySize {
    /// The RSA key size in bits.
    #[must_use]
    pub const fn bits(self) -> usize {
        match self {
            Self::Rsa2048 => 2048,
            Self::Rsa4096 => 4096,
        }
    }

    /// The signature width in bytes for this key size.
    #[must_use]
    pub const fn signature_len(self) -> usize {
        match self {
            Self::Rsa2048 => 256,
            Self::Rsa4096 => 512,
        }
    }

    /// The agnostic algorithm tag for this key size.
    #[must_use]
    pub const fn algorithm_tag(self) -> AlgorithmTag {
        match self {
            Self::Rsa2048 => AlgorithmTag::Rsa2048,
            Self::Rsa4096 => AlgorithmTag::Rsa4096,
        }
    }

    /// Determine the key size from a modulus byte length, if supported.
    #[must_use]
    pub const fn from_modulus_len(len: usize) -> Option<Self> {
        match len {
            256 => Some(Self::Rsa2048),
            512 => Some(Self::Rsa4096),
            _ => None,
        }
    }
}

/// An RSA verifying (public) key.
///
/// Mirrors the `Es256VerifyingKey` shape: `Native` uses the `rsa` crate. A
/// future `WebCrypto` arm can be added without changing callers.
#[derive(Debug, Clone)]
pub enum RsaVerifyingKey {
    /// Native verifying key using the `rsa` crate.
    Native {
        /// The verifying key (PKCS#1 v1.5, SHA-256).
        key: VerifyingKey<Sha256>,
        /// The key size, cached for signature-width dispatch.
        size: RsaKeySize,
    },
}

impl RsaVerifyingKey {
    /// The key size.
    #[must_use]
    pub const fn size(&self) -> RsaKeySize {
        match self {
            Self::Native { size, .. } => *size,
        }
    }

    /// Build a verifying key from an `rsa` crate public key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is neither RSA-2048 nor RSA-4096.
    pub fn from_public_key(public_key: RsaPublicKey) -> Result<Self, RsaKeyError> {
        let size = RsaKeySize::from_modulus_len(public_key.size())
            .ok_or(RsaKeyError::UnsupportedKeySize(public_key.size() * 8))?;
        Ok(Self::Native {
            key: VerifyingKey::<Sha256>::new(public_key),
            size,
        })
    }

    /// The PKCS#1 DER encoding of this public key.
    ///
    /// This is the key body carried inside an RSA `did:key`, after the
    /// `rsa-pub` multicodec.
    #[must_use]
    pub fn to_pkcs1_der(&self) -> Vec<u8> {
        match self {
            Self::Native { key, .. } => key
                .as_ref()
                .to_pkcs1_der()
                .expect("valid RSA public key encodes to PKCS#1 DER")
                .into_vec(),
        }
    }

    /// Parse a public key from PKCS#1 DER bytes (the `did:key` body).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid RSA public key of a
    /// supported size.
    pub fn from_pkcs1_der(bytes: &[u8]) -> Result<Self, RsaKeyError> {
        let public_key =
            RsaPublicKey::from_pkcs1_der(bytes).map_err(|_| RsaKeyError::InvalidPrivateKey)?;
        Self::from_public_key(public_key)
    }

    /// Verify a signature body (raw PKCS#1 v1.5 bytes) over `msg`.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if the body is the wrong width for this key
    /// size or verification fails.
    pub fn verify_bytes(&self, msg: &[u8], sig_bytes: &[u8]) -> Result<(), signature::Error> {
        match self {
            Self::Native { key, size } => {
                if sig_bytes.len() != size.signature_len() {
                    return Err(signature::Error::new());
                }
                let sig = RsaCrateSignature::try_from(sig_bytes)?;
                key.verify(msg, &sig)
            }
        }
    }
}

impl PartialEq for RsaVerifyingKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_pkcs1_der() == other.to_pkcs1_der()
    }
}

impl Eq for RsaVerifyingKey {}

/// An RSA signing key.
///
/// Enum-shaped like [`RsaVerifyingKey`]; only a `Native` arm today.
#[derive(Debug, Clone)]
pub enum RsaSigningKey {
    /// Native signing key using the `rsa` crate.
    Native {
        /// The signing key (PKCS#1 v1.5, SHA-256).
        key: SigningKey<Sha256>,
        /// The key size.
        size: RsaKeySize,
    },
}

impl RsaSigningKey {
    /// The key size.
    #[must_use]
    pub const fn size(&self) -> RsaKeySize {
        match self {
            Self::Native { size, .. } => *size,
        }
    }

    /// Build a signing key from an `rsa` crate private key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is neither RSA-2048 nor RSA-4096.
    pub fn from_private_key(private_key: RsaPrivateKey) -> Result<Self, RsaKeyError> {
        let size = RsaKeySize::from_modulus_len(private_key.size())
            .ok_or(RsaKeyError::UnsupportedKeySize(private_key.size() * 8))?;
        Ok(Self::Native {
            key: SigningKey::<Sha256>::new(private_key),
            size,
        })
    }

    /// Import a signing key from PKCS#1 DER private-key bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid RSA private key of a
    /// supported size.
    pub fn from_pkcs1_der(bytes: &[u8]) -> Result<Self, RsaKeyError> {
        let private_key =
            RsaPrivateKey::from_pkcs1_der(bytes).map_err(|_| RsaKeyError::InvalidPrivateKey)?;
        Self::from_private_key(private_key)
    }

    /// The verifying (public) key.
    #[must_use]
    pub fn verifying_key(&self) -> RsaVerifyingKey {
        match self {
            Self::Native { key, size } => RsaVerifyingKey::Native {
                key: key.verifying_key(),
                size: *size,
            },
        }
    }

    /// The PKCS#1 DER encoding of this private key.
    ///
    /// Used by the native credential export format to persist the key material.
    #[must_use]
    pub fn to_pkcs1_der(&self) -> Vec<u8> {
        match self {
            Self::Native { key, .. } => key
                .as_ref()
                .to_pkcs1_der()
                .expect("valid RSA private key encodes to PKCS#1 DER")
                .as_bytes()
                .to_vec(),
        }
    }

    /// Sign a message, returning the raw PKCS#1 v1.5 signature bytes.
    #[must_use]
    pub fn sign_bytes(&self, msg: &[u8]) -> Vec<u8> {
        match self {
            Self::Native { key, .. } => {
                let sig: RsaCrateSignature = key.sign(msg);
                sig.to_vec()
            }
        }
    }
}
