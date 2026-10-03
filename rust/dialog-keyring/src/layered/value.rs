//! The wire format of a sealed value.
//!
//! ```text
//! version(1) ‖ content generation(32) ‖ ciphertext and tag
//! ```
//!
//! A value is something a node refers to by its plaintext hash but does not
//! hold: in the repository's trees, a spilled value. It is sealed under a key
//! derived from the content secret and that hash (see [`keys`](super::keys)),
//! with a fixed nonce, since each key encrypts exactly one message. Like an
//! envelope, a sealed value is stored under the hash of its own bytes, and
//! the node that refers to it records that address in its structure region,
//! so a replicator copies values along with nodes and a member finds each one
//! from the node that names it.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use dialog_common::Blake3Hash;

use super::keys::{Access, Writer};
use crate::{EpochId, KeyringError};

/// The value format this build writes and reads.
const VERSION: u8 = 1;

/// The authenticated header: version and content generation.
const HEADER: usize = 1 + 32;

/// Fixed, because each value key encrypts one message.
const NONCE: [u8; 12] = [0; 12];

/// Seal a value under `writer`'s content generation.
///
/// # Errors
///
/// Returns [`KeyringError::Crypto`] if the cipher fails.
pub(crate) fn seal(writer: &Writer, plain: &[u8]) -> Result<Vec<u8>, KeyringError> {
    let reference = Blake3Hash::hash(plain);
    let header = header(writer.content_generation());
    let sealed = Aes256Gcm::new((&writer.value_key(&reference)).into())
        .encrypt(
            Nonce::from_slice(&NONCE),
            Payload {
                msg: plain,
                aad: &header,
            },
        )
        .map_err(|error| KeyringError::Crypto(error.to_string()))?;
    let mut bytes = Vec::with_capacity(HEADER + sealed.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&sealed);
    Ok(bytes)
}

/// Open a sealed value whose plaintext hashes to `reference`.
///
/// # Errors
///
/// Returns [`KeyringError::Malformed`] or
/// [`KeyringError::UnsupportedVersion`] if the bytes are not a sealed value,
/// [`KeyringError::MissingGeneration`] without its content generation, and
/// [`KeyringError::Failed`] if it does not open under `reference`.
pub(crate) fn open(
    access: &Access,
    reference: &Blake3Hash,
    bytes: &[u8],
) -> Result<Vec<u8>, KeyringError> {
    let (head, sealed) = bytes
        .split_first_chunk::<HEADER>()
        .ok_or(KeyringError::Malformed)?;
    if head[0] != VERSION {
        return Err(KeyringError::UnsupportedVersion(head[0]));
    }
    let mut generation = [0u8; 32];
    generation.copy_from_slice(&head[1..]);
    let key = access.value_key(&EpochId::from(generation), reference)?;
    let plain = Aes256Gcm::new((&key).into())
        .decrypt(
            Nonce::from_slice(&NONCE),
            Payload {
                msg: sealed,
                aad: head,
            },
        )
        .map_err(|_| KeyringError::Failed)?;
    if &Blake3Hash::hash(&plain) != reference {
        return Err(KeyringError::Failed);
    }
    Ok(plain)
}

fn header(generation: &EpochId) -> [u8; HEADER] {
    let mut header = [0u8; HEADER];
    header[0] = VERSION;
    header[1..].copy_from_slice(generation.as_bytes());
    header
}
