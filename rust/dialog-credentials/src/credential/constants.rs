//! Shared multicodec constants and sizes for credential export formats.
//!
//! The stored form is self-describing: a multicodec tag names the algorithm,
//! so the reader dispatches on the tag rather than assuming ed25519. ed25519
//! constants are unchanged, keeping existing stored credentials byte-identical.

/// Multicodec varint for ed25519 private key (`0x1300`).
pub const PRIVATE_TAG: &[u8] = &[0x80, 0x26];
/// Multicodec varint for ed25519 public key (`0xed`).
pub const PUBLIC_TAG: &[u8] = &[0xed, 0x01];
/// Length of an ed25519 key (private seed or public key) in bytes.
pub const KEY_SIZE: usize = 32;
/// Byte length of the ed25519 private key multicodec tag prefix.
pub const PRIVATE_TAG_SIZE: usize = PRIVATE_TAG.len();
/// Byte length of the ed25519 public key multicodec tag prefix.
pub const PUBLIC_TAG_SIZE: usize = PUBLIC_TAG.len();
/// Total size of a serialized ed25519 signer credential
/// (private tag + private seed + public tag + public key).
pub const SIGNER_EXPORT_SIZE: usize = PRIVATE_TAG_SIZE + KEY_SIZE + PUBLIC_TAG_SIZE + KEY_SIZE;
/// Total size of a serialized ed25519 verifier credential (public tag + public key).
pub const VERIFIER_EXPORT_SIZE: usize = PUBLIC_TAG_SIZE + KEY_SIZE;
/// Offset within a serialized ed25519 signer credential at which the public key
/// section begins.
pub const PUBLIC_KEY_OFFSET: usize = PRIVATE_TAG_SIZE + KEY_SIZE;

/// Multicodec varint for p256 private key (`0x1306`).
#[cfg(feature = "es256")]
pub const ES256_PRIVATE_TAG: &[u8] = &[0x86, 0x26];
/// Multicodec varint for p256 public key (`0x1200`).
#[cfg(feature = "es256")]
pub const ES256_PUBLIC_TAG: &[u8] = &[0x80, 0x24];
/// Length of a p256 private scalar in bytes.
#[cfg(feature = "es256")]
pub const ES256_PRIVATE_KEY_SIZE: usize = 32;
/// Length of a p256 compressed public point in bytes.
#[cfg(feature = "es256")]
pub const ES256_PUBLIC_KEY_SIZE: usize = 33;
/// Byte length of the p256 private key multicodec tag prefix.
#[cfg(feature = "es256")]
pub const ES256_PRIVATE_TAG_SIZE: usize = ES256_PRIVATE_TAG.len();
/// Byte length of the p256 public key multicodec tag prefix.
#[cfg(feature = "es256")]
pub const ES256_PUBLIC_TAG_SIZE: usize = ES256_PUBLIC_TAG.len();
/// Total size of a serialized p256 signer credential.
#[cfg(feature = "es256")]
pub const ES256_SIGNER_EXPORT_SIZE: usize =
    ES256_PRIVATE_TAG_SIZE + ES256_PRIVATE_KEY_SIZE + ES256_PUBLIC_TAG_SIZE + ES256_PUBLIC_KEY_SIZE;
/// Total size of a serialized p256 verifier credential.
#[cfg(feature = "es256")]
pub const ES256_VERIFIER_EXPORT_SIZE: usize = ES256_PUBLIC_TAG_SIZE + ES256_PUBLIC_KEY_SIZE;
/// Offset within a serialized p256 signer credential at which the public key
/// section begins.
#[cfg(feature = "es256")]
pub const ES256_PUBLIC_KEY_OFFSET: usize = ES256_PRIVATE_TAG_SIZE + ES256_PRIVATE_KEY_SIZE;

/// Private-use multicodec varint marking a WebAuthn P-256 public key
/// (`0x300001`). The key bytes that follow are an ordinary 33-byte compressed
/// P-256 point; only the tag distinguishes a passkey from a plain p256-pub key.
#[cfg(feature = "webauthn")]
pub const WEBAUTHN_PUBLIC_TAG: &[u8] = &[0x81, 0x80, 0xc0, 0x01];
/// Byte length of the WebAuthn public key multicodec tag prefix.
#[cfg(feature = "webauthn")]
pub const WEBAUTHN_PUBLIC_TAG_SIZE: usize = WEBAUTHN_PUBLIC_TAG.len();
/// Length of a WebAuthn (P-256) compressed public point in bytes.
#[cfg(feature = "webauthn")]
pub const WEBAUTHN_PUBLIC_KEY_SIZE: usize = 33;
/// Total size of a serialized WebAuthn verifier credential.
#[cfg(feature = "webauthn")]
pub const WEBAUTHN_VERIFIER_EXPORT_SIZE: usize =
    WEBAUTHN_PUBLIC_TAG_SIZE + WEBAUTHN_PUBLIC_KEY_SIZE;
