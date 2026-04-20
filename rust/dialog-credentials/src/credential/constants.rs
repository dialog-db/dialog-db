//! Shared multicodec constants and sizes for credential export formats.

/// Multicodec varint for ed25519 private key (0x1300).
pub const ED25519_PRIV_TAG: &[u8] = &[0x80, 0x26];
/// Multicodec varint for ed25519 public key (0xed).
pub const ED25519_PUB_TAG: &[u8] = &[0xed, 0x01];
pub const KEY_SIZE: usize = 32;
pub const PRIV_TAG_SIZE: usize = ED25519_PRIV_TAG.len();
pub const PUB_TAG_SIZE: usize = ED25519_PUB_TAG.len();
pub const SIGNER_EXPORT_SIZE: usize = PRIV_TAG_SIZE + KEY_SIZE + PUB_TAG_SIZE + KEY_SIZE;
pub const VERIFIER_EXPORT_SIZE: usize = PUB_TAG_SIZE + KEY_SIZE;
pub const PUB_KEY_OFFSET: usize = PRIV_TAG_SIZE + KEY_SIZE;
