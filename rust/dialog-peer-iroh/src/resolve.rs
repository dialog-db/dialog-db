//! Finding the material an invocation's arguments name, and proving it
//! is that material.
//!
//! A signed invocation does not carry the bytes it authorizes. Its
//! arguments come from the capability's attenuation, and `archive::Put`
//! projects its block to a `digest` and a `checksum`, so the signature
//! covers two commitments and never the content. An access service needs
//! no more — it presigns a URL for `{subject}/{catalog}/{digest}` and
//! the bytes go to S3 — but a peer that *performs* the effect has to be
//! handed them, and the bytes ride in the same `ctn-v1` container as
//! their own token.
//!
//! # Two hashes, two jobs
//!
//! Both commitments are signed, and each answers a different question.
//!
//! **SHA-256 addresses the block.** A bundle keys carried blocks by
//! `dagcbor_cid`, which is SHA-256 over their bytes, and `Put` projects
//! its block to `Checksum::sha256` of those same bytes. So the checksum
//! in the signed arguments *is* the block's address in the container:
//! [`locate`] turns one into the other. This is SHA-256 only because it
//! is what S3 supports; here it earns its keep as addressing and as a
//! transport-integrity check that costs nothing extra.
//!
//! **BLAKE3 identifies the block.** It is the archive's own content
//! address — the key a block is stored under — and it is the one that
//! has to be checked.
//!
//! # Why checking BLAKE3 is not belt and braces
//!
//! Finding a block at the address the checksum names proves the sender
//! sent the bytes it said it would. It does not prove those bytes are
//! the block the invocation claims to store, because a sender signs its
//! own invocation and therefore chooses *both* commitments. Nothing
//! stops it signing a `digest` of one block and a `checksum` of another.
//!
//! A peer that skipped the BLAKE3 check would then store bytes under a
//! key that is not their hash, and a content-addressed store that has
//! lost that invariant is corrupt in a way no later read can detect: the
//! block answers to a digest it does not have, and every proof built
//! over it is wrong. The signature does not help — it is the attacker's
//! own. [`block`] is where that is refused.

use dialog_common::{Blake3Hash, Buffer, Checksum};
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
    /// sender omitted it, or sent different bytes than it signed the
    /// checksum for — indistinguishable here, and deliberately so: both
    /// are "not the authorized bytes".
    #[error("the container carries no block at {link}")]
    Absent {
        /// The address the arguments named.
        link: Box<Cid>,
    },
    /// The carried block is not the block the invocation says it is.
    ///
    /// Storing it would put bytes under a key that is not their hash and
    /// silently corrupt the archive, so this is refused rather than
    /// repaired.
    #[error("block at the signed address hashes to {found}, not the signed digest {signed}")]
    Impostor {
        /// The digest the invocation committed to.
        signed: Box<Blake3Hash>,
        /// The digest the carried bytes actually have.
        found: Box<Blake3Hash>,
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

/// The bytes at the address a signed checksum names, with nothing
/// checked beyond the address.
///
/// For material whose attenuation commits to a checksum and nothing
/// else — `archive::Import`'s per-block checksums, `memory::Publish`'s
/// content — so there is no second commitment to prove them against.
/// Prefer [`block`] wherever a digest was signed.
pub fn at<'a>(bundle: &'a InvocationBundle, checksum: &Checksum) -> Result<&'a [u8], ResolveError> {
    let link = locate(checksum)?;
    bundle.block(&link).ok_or(ResolveError::Absent {
        link: Box::new(link),
    })
}

/// The block a verified invocation's arguments name, proven to be that
/// block.
///
/// `digest` and `checksum` must both come from the *same verified*
/// invocation's arguments. The checksum finds the bytes; the digest
/// decides whether they may be used. Passing either from anywhere else
/// resolves a block against a claim nobody vouched for.
///
/// The returned [`Buffer`] has memoized the hash this checked, so the
/// archive will not pay for it twice.
pub fn block(
    bundle: &InvocationBundle,
    digest: &Blake3Hash,
    checksum: &Checksum,
) -> Result<Buffer, ResolveError> {
    let link = locate(checksum)?;
    let bytes = bundle.block(&link).ok_or(ResolveError::Absent {
        link: Box::new(link),
    })?;

    let buffer = Buffer::from(bytes);
    let found = buffer.blake3_hash();
    if found != digest {
        return Err(ResolveError::Impostor {
            signed: Box::new(digest.clone()),
            found: Box::new(found.clone()),
        });
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::Principal;
    use dialog_credentials::Ed25519Signer;
    use dialog_ucan_core::cid::dagcbor_cid;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};
    use std::collections::BTreeMap;

    /// Build a bundle the way a sender would, then round-trip it
    /// through `ctn-v1` so the tests resolve against bytes that really
    /// travelled rather than an in-memory map.
    async fn bundle_carrying(blocks: &[&[u8]]) -> InvocationBundle {
        let signer = Ed25519Signer::import(&[42u8; 32])
            .await
            .expect("a fixed test key imports");
        let did = signer.did();
        let invocation = InvocationBuilder::new()
            .issuer(signer)
            .audience(&did)
            .subject(&did)
            .command(vec!["use".to_string(), "put".to_string()])
            .arguments(BTreeMap::new())
            .proofs(vec![])
            .try_build()
            .await
            .expect("the invocation builds");

        let chain = InvocationChain::new(invocation, Default::default());
        InvocationBundle::from_chain(&chain, blocks.iter().map(|bytes| bytes.to_vec()))
            .expect("a bundle assembles from a chain")
    }

    /// The property the design leans on: the checksum a signature covers
    /// is the address of the block in the container.
    #[dialog_common::test]
    fn a_signed_checksum_is_a_block_address() {
        let bytes = b"the authorized block".as_slice();
        assert_eq!(
            locate(&Checksum::sha256(bytes)).unwrap(),
            dagcbor_cid(bytes),
            "a block's address must be derivable from the checksum that was signed"
        );
    }

    /// Substitution by a sender that signed honestly is not refused, it
    /// is impossible: different bytes hash to a different address, so
    /// they are never *at* the address the invocation named.
    #[dialog_common::test]
    fn substituted_bytes_are_at_a_different_address() {
        let authorized = b"the authorized block".as_slice();
        let substituted = b"something else entirely".as_slice();
        assert_ne!(
            locate(&Checksum::sha256(authorized)).unwrap(),
            locate(&Checksum::sha256(substituted)).unwrap()
        );
    }

    #[dialog_common::test]
    async fn the_block_that_was_signed_resolves() {
        let bytes = b"the authorized block".as_slice();
        let bundle = bundle_carrying(&[bytes]).await;

        let resolved = block(
            &bundle,
            Buffer::from(bytes).blake3_hash(),
            &Checksum::sha256(bytes),
        )
        .expect("the signed block resolves");
        assert_eq!(resolved.as_ref(), bytes);
    }

    /// The attack the BLAKE3 check exists for. A sender signs its own
    /// invocation, so it can commit to one block's digest and another
    /// block's checksum. Addressing alone would hand over the second and
    /// the archive would file it under the first.
    #[dialog_common::test]
    async fn a_block_signed_under_another_digest_is_refused() {
        let carried = b"the block that actually travelled".as_slice();
        let claimed = b"the block whose digest was signed".as_slice();
        let bundle = bundle_carrying(&[carried]).await;

        // Both commitments are the sender's to choose, and these
        // disagree: the checksum addresses `carried`, the digest names
        // `claimed`.
        let refusal = block(
            &bundle,
            Buffer::from(claimed).blake3_hash(),
            &Checksum::sha256(carried),
        )
        .expect_err("bytes that are not the signed block must not be stored under its digest");
        assert!(
            matches!(refusal, ResolveError::Impostor { .. }),
            "expected an impostor, got {refusal:?}"
        );
    }

    #[dialog_common::test]
    async fn an_omitted_block_is_absent_rather_than_wrong() {
        let bundle = bundle_carrying(&[]).await;
        let never_sent = b"never sent".as_slice();

        let refusal = block(
            &bundle,
            Buffer::from(never_sent).blake3_hash(),
            &Checksum::sha256(never_sent),
        )
        .expect_err("a block that did not travel cannot resolve");
        assert!(matches!(refusal, ResolveError::Absent { .. }));
    }
}
