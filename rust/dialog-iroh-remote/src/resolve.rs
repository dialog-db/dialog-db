//! Finding the material an invocation's arguments name.
//!
//! A signed invocation does not carry the bytes it authorizes. Its
//! arguments come from the capability's attenuation, and `archive::Put`
//! projects its block to a `digest` and a `checksum`, so the signature
//! covers a commitment and never the content. An access service needs
//! no more than that — it presigns a URL for `{subject}/{catalog}/{digest}`
//! and the bytes go to S3 directly — but a peer that *performs* the
//! effect has to be handed them.
//!
//! The wrong way to do that is a second field beside the container,
//! because then two things claim to be the payload and the signature
//! covers only one. [`InvocationBundle`] is the right way, and it says
//! so itself: material that is neither proof nor argument travels as its
//! own token in the same `ctn-v1` container, since "inlining those as
//! arguments gives up content addressing".
//!
//! # Content addressing does the binding
//!
//! A bundle keys every carried block by `dagcbor_cid`, which is SHA-256
//! over the block's bytes. `Put`'s attenuation projects its block to
//! `Checksum::sha256` of those same bytes. So the checksum *in the
//! signed arguments* is the address of the block in the container:
//! [`locate`] turns one into the other, and a lookup either finds the
//! block whose bytes hash to what was signed, or finds nothing.
//!
//! There is no comparison step to get wrong, and no way to substitute a
//! block: a different block has a different SHA-256, so it hashes to a
//! different CID and is simply not at the address the signature names.
//! This is why the attenuation carries a SHA-256 checksum beside the
//! BLAKE3 digest — the digest is the archive's own identity for the
//! block, the checksum is how it is addressed in transit.
//!
//! # A carried block is not an authorized one
//!
//! Presence asserts nothing, as the bundle's own documentation is careful
//! to say. A peer may be handed blocks it never asked for and must not
//! act on them. Only a block that a *verified* invocation's arguments
//! name is authorized, which is why every function here takes the
//! checksum from the arguments rather than iterating what arrived.

use dialog_common::Checksum;
use dialog_ucan_core::container::bundle::InvocationBundle;
use ipld_core::cid::Cid;
use ipld_core::cid::multihash::Multihash;

/// The DAG-CBOR codec, as [`dagcbor_cid`] uses it.
///
/// [`dagcbor_cid`]: dialog_ucan_core::cid::dagcbor_cid
const DAG_CBOR: u64 = 0x71;

/// The multihash code for SHA2-256.
const SHA2_256: u64 = 0x12;

/// Why the material an invocation named is not usable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The arguments name a checksum in an algorithm blocks are not
    /// addressed by, so no address can be derived from it.
    #[error("cannot address a block by a {algorithm} checksum")]
    Unaddressable {
        /// The algorithm the arguments used.
        algorithm: String,
    },
    /// The invocation named material the container did not carry. The
    /// sender omitted it, or sent a different block than it signed for —
    /// which is indistinguishable here, and deliberately so: both are
    /// "not the authorized bytes".
    #[error("the container carries no block at {link}")]
    Absent {
        /// The address the arguments named.
        link: Box<Cid>,
    },
}

/// The address a block with this checksum has inside a bundle.
///
/// Fails for any checksum algorithm other than SHA-256, rather than
/// guessing: an address derived from the wrong hash would miss every
/// time, and a silent miss reads as "the sender omitted the block"
/// instead of "this build cannot address it".
pub fn locate(checksum: &Checksum) -> Result<Cid, ResolveError> {
    match checksum {
        Checksum::Sha256(_) => Multihash::wrap(SHA2_256, checksum.as_bytes())
            .map(|multihash| Cid::new_v1(DAG_CBOR, multihash))
            .map_err(|_| ResolveError::Unaddressable {
                algorithm: checksum.name().to_string(),
            }),
    }
}

/// The bytes the signed `checksum` names, from the container that
/// carried the invocation.
///
/// `checksum` must come from a *verified* invocation's arguments.
/// Passing one from anywhere else resolves a block by an address nobody
/// vouched for, which asserts nothing.
pub fn payload<'a>(
    bundle: &'a InvocationBundle,
    checksum: &Checksum,
) -> Result<&'a [u8], ResolveError> {
    let link = locate(checksum)?;
    bundle.block(&link).ok_or(ResolveError::Absent {
        link: Box::new(link),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_ucan_core::cid::dagcbor_cid;

    /// The property the whole design leans on: the checksum a signature
    /// covers *is* the address of the block in the container.
    #[dialog_common::test]
    fn a_signed_checksum_is_a_block_address() {
        let block = b"the authorized block".as_slice();
        assert_eq!(
            locate(&Checksum::sha256(block)).unwrap(),
            dagcbor_cid(block),
            "a block's address must be derivable from the checksum that was signed"
        );
    }

    /// Substitution is not refused, it is impossible: different bytes
    /// hash to a different address, so they are never *at* the address
    /// the invocation named.
    #[dialog_common::test]
    fn substituted_bytes_are_at_a_different_address() {
        let authorized = b"the authorized block".as_slice();
        let substituted = b"something else entirely".as_slice();
        assert_ne!(
            locate(&Checksum::sha256(authorized)).unwrap(),
            locate(&Checksum::sha256(substituted)).unwrap()
        );
    }

    /// A checksum whose block did not travel is absent, and says which
    /// address went missing rather than failing anonymously.
    #[dialog_common::test]
    fn an_omitted_block_names_the_address_it_should_have_been_at() {
        let checksum = Checksum::sha256(b"never sent");
        let link = locate(&checksum).unwrap();
        assert_eq!(
            ResolveError::Absent {
                link: Box::new(link)
            }
            .to_string(),
            format!("the container carries no block at {link}")
        );
    }
}
