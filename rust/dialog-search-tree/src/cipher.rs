//! The seam where node bytes stop being readable by the store that holds them.
//!
//! A [`ContentAddressedStorage`](crate::ContentAddressedStorage) with a cipher
//! attached keeps addressing the tree by the same content identities it always
//! did — nothing above this layer changes — while what reaches the backend is
//! ciphertext, filed under an address the backend cannot connect to any
//! content it might guess.
//!
//! # Why the address is translated rather than reused
//!
//! A node's identity is `blake3` of its bytes, and links carry that identity.
//! Storing ciphertext under that same identity would hand anyone holding the
//! store an oracle: hash a guess at a node's contents and look it up. Guessing
//! a small index node is not far-fetched.
//!
//! So the cipher maps identity to storage address with a *keyed* hash. Holders
//! of the key compute it and find the node; the backend sees an address that
//! is opaque without the key. Identities never leave the process in the clear
//! — the links that carry them live inside sealed node bodies.
//!
//! # What the tree requires of an implementation
//!
//! - **Deterministic.** One identity must always map to one address, and one
//!   plaintext must always seal to the same bytes. Two replicas that
//!   independently compute the same node have to agree, or a diff would see
//!   changes that are not there.
//! - **Synchronous.** Nodes are sealed on the persist path, which does not
//!   await. In the browser this rules out `WebCrypto` and means a software
//!   cipher.

use dialog_common::Blake3Hash;
use dialog_storage::DialogStorageError;

/// Seals node bytes and hides their addresses from the backend.
///
/// See the [module docs](self) for what an implementation has to guarantee.
pub trait NodeCipher: std::fmt::Debug + dialog_common::ConditionalSync {
    /// The address the node with this content identity is stored under.
    fn address(&self, identity: &Blake3Hash) -> Blake3Hash;

    /// Seal a node's bytes for storage.
    ///
    /// # Errors
    ///
    /// Returns [`DialogStorageError`] if the cipher fails.
    fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, DialogStorageError>;

    /// Open what [`seal`](Self::seal) produced.
    ///
    /// # Errors
    ///
    /// Returns [`DialogStorageError`] if the bytes do not open — a wrong key,
    /// an epoch this cipher cannot resolve, or tampering.
    fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, DialogStorageError>;
}
