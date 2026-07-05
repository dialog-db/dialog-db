//! Shared multicodec constants and sizes for credential export formats.

/// Multicodec varint for ed25519 private key (0x1300).
pub const PRIVATE_TAG: &[u8] = &[0x80, 0x26];
/// Multicodec varint for ed25519 public key (0xed).
pub const PUBLIC_TAG: &[u8] = &[0xed, 0x01];
/// Length of an ed25519 key (private seed or public key) in bytes.
pub const KEY_SIZE: usize = 32;
/// Byte length of the ed25519 private key multicodec tag prefix.
pub const PRIVATE_TAG_SIZE: usize = PRIVATE_TAG.len();
/// Byte length of the ed25519 public key multicodec tag prefix.
pub const PUBLIC_TAG_SIZE: usize = PUBLIC_TAG.len();
/// Total size of a serialized signer credential
/// (private tag + private seed + public tag + public key).
pub const SIGNER_EXPORT_SIZE: usize = PRIVATE_TAG_SIZE + KEY_SIZE + PUBLIC_TAG_SIZE + KEY_SIZE;
/// Total size of a serialized verifier credential (public tag + public key).
pub const VERIFIER_EXPORT_SIZE: usize = PUBLIC_TAG_SIZE + KEY_SIZE;
/// Offset within a serialized signer credential at which the public key
/// section begins.
pub const PUBLIC_KEY_OFFSET: usize = PRIVATE_TAG_SIZE + KEY_SIZE;
