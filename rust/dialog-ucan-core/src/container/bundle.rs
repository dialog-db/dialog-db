//! A second view over a `ctn-v1` container: the invocation at its root,
//! and every other token addressable as a block.
//!
//! [`InvocationChain`] reads a container as an invocation followed by
//! its proofs, and refuses anything else — a token that is not a
//! delegation is a malformed proof, and staying strict about that is
//! what stops a corrupt one being waved through as something else.
//!
//! Some commands name material that is neither proof nor argument:
//! another invocation to be redeemed later, or bytes whose only contract
//! is a checksum the arguments state. Inlining those as arguments gives
//! up content addressing, so they travel as their own tokens — and a
//! container carrying them is not an invocation chain, which is why
//! reading it as one fails.
//!
//! [`InvocationBundle`] is the reading that fits: same bytes, same
//! `ctn-v1` format, different expectation. The root token is the
//! invocation; everything else is kept verbatim and addressed by the CID
//! it hashes to. Nothing is interpreted on the way in — resolution is
//! explicit and typed at the point of use, through
//! [`resolve_invocation`](InvocationBundle::resolve_invocation) and
//! [`resolve_delegation`](InvocationBundle::resolve_delegation) — so a
//! caller says what it expects a CID to name rather than the parser
//! guessing.
//!
//! Carrying a block asserts nothing about it. Presence means a token
//! travelled, never that it is valid or authorized, exactly as a
//! revocation's `rev` and `pth` are opaque to invocation verification
//! and checked by the command that names them.

use super::{Container, ContainerError};
use crate::container::delegation::DelegationChain;
use crate::container::invocation::InvocationChain;
use crate::{Invocation, cid::dagcbor_cid};
use dialog_varsig::AnySignature;
use ipld_core::cid::Cid;
use std::collections::HashMap;

/// An invocation and the blocks its arguments name, read from a
/// `ctn-v1` container.
#[derive(Debug, Clone)]
pub struct InvocationBundle {
    invocation: Invocation<AnySignature>,
    blocks: HashMap<Cid, Vec<u8>>,
}

impl InvocationBundle {
    /// The invocation at the container's root.
    #[must_use]
    pub const fn invocation(&self) -> &Invocation<AnySignature> {
        &self.invocation
    }

    /// The raw bytes of the block `link` names.
    #[must_use]
    pub fn block(&self, link: &Cid) -> Option<&[u8]> {
        self.blocks.get(link).map(Vec::as_slice)
    }

    /// Every carried block, keyed by CID.
    #[must_use]
    pub const fn blocks(&self) -> &HashMap<Cid, Vec<u8>> {
        &self.blocks
    }

    /// The root invocation together with the carried blocks its `prf`
    /// field names, as an [`InvocationChain`] ready to verify.
    ///
    /// A bundle's container is not an invocation chain — that is the
    /// whole reason this view exists — but the invocation at its root is
    /// still an invocation with proofs, and authorizing it is the same
    /// job as ever. This assembles exactly that: the proofs it names,
    /// and none of the other blocks, which are arguments rather than
    /// authority.
    ///
    /// Errors when a named proof is absent or is not a delegation, since
    /// a chain missing its proofs cannot verify and silently dropping
    /// one would let an unproven invocation through.
    pub fn chain(&self) -> Result<InvocationChain<AnySignature>, ContainerError> {
        let mut delegations = HashMap::new();
        for link in self.invocation.proofs() {
            let bytes = self.require(link)?;
            let delegation: crate::Delegation<AnySignature> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    ContainerError::Invocation(format!("proof {link} is not a delegation: {e}"))
                })?;
            delegations.insert(*link, std::sync::Arc::new(delegation));
        }
        Ok(InvocationChain::new(self.invocation.clone(), delegations))
    }

    /// Read the block `link` names as an invocation and its proofs.
    ///
    /// For an argument naming an invocation to be redeemed later: the
    /// answer is a full [`InvocationChain`], so it verifies through the
    /// ordinary path rather than by hand.
    ///
    /// A carried block is a bare token — the same unit the enclosing
    /// container holds — so an invocation arrives without its proofs.
    /// One that needs them carries them as blocks of its own, and the
    /// caller resolves those the same way.
    pub fn resolve_invocation(
        &self,
        link: &Cid,
    ) -> Result<InvocationChain<AnySignature>, ContainerError> {
        let invocation: Invocation<AnySignature> =
            serde_ipld_dagcbor::from_slice(self.require(link)?).map_err(|e| {
                ContainerError::Invocation(format!("block {link} is not an invocation: {e}"))
            })?;
        Ok(InvocationChain::new(invocation, HashMap::new()))
    }

    /// Read the block `link` names as a delegation chain.
    pub fn resolve_delegation(&self, link: &Cid) -> Result<DelegationChain, ContainerError> {
        let delegation: crate::Delegation<AnySignature> =
            serde_ipld_dagcbor::from_slice(self.require(link)?).map_err(|e| {
                ContainerError::Invocation(format!("block {link} is not a delegation: {e}"))
            })?;
        Ok(DelegationChain::new(delegation))
    }

    fn require(&self, link: &Cid) -> Result<&[u8], ContainerError> {
        self.block(link).ok_or_else(|| {
            ContainerError::Invocation(format!("the container carries no block for {link}"))
        })
    }
}

impl TryFrom<&[u8]> for InvocationBundle {
    type Error = ContainerError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from(Container::from_bytes(bytes)?)
    }
}

impl TryFrom<Container> for InvocationBundle {
    type Error = ContainerError;

    /// Read a container as an invocation plus addressable blocks.
    ///
    /// Only the root token is interpreted, and only far enough to be an
    /// invocation. Every other token is kept as it arrived: this view
    /// makes no claim about what they are, so it cannot refuse one for
    /// being the wrong kind.
    fn try_from(container: Container) -> Result<Self, Self::Error> {
        let tokens = container.into_tokens();
        let Some((root, rest)) = tokens.split_first() else {
            return Err(ContainerError::Invocation(
                "container must contain at least an invocation".to_string(),
            ));
        };
        let invocation: Invocation<AnySignature> = serde_ipld_dagcbor::from_slice(root)
            .map_err(|e| ContainerError::Invocation(format!("failed to decode invocation: {e}")))?;

        Ok(Self {
            invocation,
            blocks: rest
                .iter()
                .map(|bytes| (dagcbor_cid(bytes), bytes.clone()))
                .collect(),
        })
    }
}

impl From<&InvocationBundle> for Container {
    /// Back to `ctn-v1`: the invocation, then its blocks.
    ///
    /// Round-trips through [`TryFrom<Container>`], so a bundle survives
    /// being forwarded — which is the point, since the material it
    /// carries is usually on its way somewhere.
    fn from(bundle: &InvocationBundle) -> Self {
        let mut tokens = Vec::with_capacity(1 + bundle.blocks.len());
        if let Ok(bytes) = serde_ipld_dagcbor::to_vec(&bundle.invocation) {
            tokens.push(bytes);
        }
        tokens.extend(bundle.blocks.values().cloned());
        Container::new(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container::invocation::tests::create_test_invocation_chain;

    /// A bundle carrying an invocation and a delegation, plus opaque
    /// bytes that are neither.
    async fn bundle() -> (InvocationBundle, Cid, Cid, Cid) {
        let (chain, _) = create_test_invocation_chain().await;
        let carried_invocation =
            serde_ipld_dagcbor::to_vec(&chain.invocation).expect("invocation encodes");
        let proof = *chain.proofs().first().expect("the chain carries a proof");
        let carried_delegation = chain
            .delegation(&proof)
            .expect("the proof is present")
            .encoded()
            .to_vec();
        let opaque = b"ciphertext: neither invocation nor delegation".to_vec();

        let (root, _) = create_test_invocation_chain().await;
        let mut tokens =
            vec![serde_ipld_dagcbor::to_vec(&root.invocation).expect("invocation encodes")];
        tokens.extend([
            carried_invocation.clone(),
            carried_delegation.clone(),
            opaque.clone(),
        ]);
        let bytes = Container::new(tokens)
            .to_bytes()
            .expect("container encodes");

        (
            InvocationBundle::try_from(bytes.as_slice()).expect("bundle decodes"),
            dagcbor_cid(&carried_invocation),
            dagcbor_cid(&carried_delegation),
            dagcbor_cid(&opaque),
        )
    }

    /// The point of the second view: the same `ctn-v1` bytes that a
    /// chain refuses are readable as a bundle, because a bundle claims
    /// nothing about what the tokens are.
    #[dialog_common::test]
    async fn it_reads_a_container_an_invocation_chain_refuses() {
        let (_, _, _, opaque) = bundle().await;
        let _ = opaque;

        let (chain, _) = create_test_invocation_chain().await;
        let mut tokens = Container::from_bytes(&chain.to_bytes().expect("chain encodes"))
            .expect("container decodes")
            .into_tokens();
        tokens.push(b"opaque".to_vec());
        let bytes = Container::new(tokens)
            .to_bytes()
            .expect("container encodes");

        assert!(
            InvocationChain::try_from(bytes.as_slice()).is_err(),
            "a chain must still refuse a token that is not a proof"
        );
        assert!(
            InvocationBundle::try_from(bytes.as_slice()).is_ok(),
            "the same bytes read as a bundle"
        );
    }

    /// Resolution is typed at the point of use: the caller says what it
    /// expects a CID to name, rather than the parser guessing.
    #[dialog_common::test]
    async fn it_resolves_blocks_as_what_the_caller_expects() {
        let (bundle, invocation, delegation, opaque) = bundle().await;

        assert!(bundle.resolve_invocation(&invocation).is_ok());
        assert!(bundle.resolve_delegation(&delegation).is_ok());
        assert!(
            bundle.block(&opaque).is_some(),
            "opaque bytes are readable as themselves"
        );
        assert!(
            bundle.resolve_invocation(&opaque).is_err(),
            "and are not an invocation just because a caller asked"
        );
    }

    #[dialog_common::test]
    async fn it_refuses_a_link_the_container_does_not_carry() {
        let (bundle, _, _, _) = bundle().await;
        let absent = dagcbor_cid(b"never travelled");

        assert!(bundle.block(&absent).is_none());
        assert!(bundle.resolve_invocation(&absent).is_err());
        assert!(bundle.resolve_delegation(&absent).is_err());
    }

    /// The root invocation still authorizes normally: its proofs are
    /// among the carried blocks, and `chain()` is how they come back
    /// together.
    #[dialog_common::test]
    async fn it_rebuilds_the_root_chain_for_verification() {
        let (original, _) = create_test_invocation_chain().await;
        let bytes = original.to_bytes().expect("chain encodes");
        let bundle = InvocationBundle::try_from(bytes.as_slice()).expect("bundle decodes");

        let rebuilt = bundle.chain().expect("the proofs are carried");
        assert_eq!(rebuilt.proofs(), original.proofs());
        for link in original.proofs() {
            assert!(
                rebuilt.delegation(link).is_some(),
                "every named proof comes back"
            );
        }
    }

    /// A bundle survives being forwarded, which is the whole reason its
    /// blocks re-encode.
    #[dialog_common::test]
    async fn it_round_trips_through_its_container() {
        let (bundle, invocation, delegation, opaque) = bundle().await;

        let bytes = Container::from(&bundle)
            .to_bytes()
            .expect("bundle container encodes");
        let restored = InvocationBundle::try_from(bytes.as_slice()).expect("bundle decodes");

        assert_eq!(restored.blocks().len(), bundle.blocks().len());
        for link in [invocation, delegation, opaque] {
            assert_eq!(
                restored.block(&link),
                bundle.block(&link),
                "every carried block survives the round trip"
            );
        }
    }
}
