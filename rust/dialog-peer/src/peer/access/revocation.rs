//! Checking a remote's word that a certificate was revoked.
//!
//! A remote that refuses a chain because a link in it was revoked names
//! the link and hands over the signed revocation. The peer retracts the
//! link only once that document holds up here: it decodes as a
//! `/ucan/revoke` of the very certificate named, its issuer stands above
//! that certificate in a chain this peer retains, and it verifies as a
//! revocation (the invocation's own signature and proofs, and the
//! evidence that its issuer could withdraw what it names). A refusal that
//! fails any of these names a certificate the peer has no reason to
//! believe was revoked, so nothing is retracted.

use dialog_capability::Did;
use dialog_credentials::DidKeyResolver;
#[cfg(target_arch = "wasm32")]
use dialog_ucan_core::future::Local as Runtime;
#[cfg(not(target_arch = "wasm32"))]
use dialog_ucan_core::future::Sendable as Runtime;
use dialog_ucan_core::{
    Container, Environment, InvocationChain, RevocationChain, UnverifiedRevocations,
    VerificationContext,
};
use dialog_varsig::AnySignature;
use ipld_core::cid::Cid;
use std::collections::HashSet;
use thiserror::Error;

/// Why a refusal's revocation did not hold up.
#[derive(Debug, Error)]
pub(crate) enum Unverified {
    /// The refusal names a certificate and carries no revocation of it.
    #[error("the refusal carries no revocation document")]
    Absent,

    /// The document is not a revocation container.
    #[error("the revocation document does not decode: {0}")]
    Malformed(String),

    /// The document revokes some other certificate than the one the
    /// refusal named.
    #[error("the revocation is of '{found}', not of the certificate the refusal named")]
    Elsewhere {
        /// The certificate the document revokes.
        found: Cid,
    },

    /// The document's issuer is not above the revoked certificate in any
    /// chain retained here, so its word does not count for that link.
    #[error("'{revoker}' does not stand above the revoked certificate in a retained chain")]
    NotEntitled {
        /// The principal that signed the revocation.
        revoker: Did,
    },

    /// The document is not a valid revocation: a signature that does not
    /// verify, a proof that does not hold, or evidence that does not show
    /// its issuer could withdraw what it names.
    #[error("the revocation does not verify: {0}")]
    Invalid(String),
}

/// A revocation document, decoded but not yet verified.
pub(crate) struct Claimed(RevocationChain<AnySignature>);

impl Claimed {
    /// Decode a refusal's revocation document, the container's base64url
    /// tagged form, and check that it revokes `certificate`.
    pub(crate) fn decode(document: &str, certificate: Cid) -> Result<Self, Unverified> {
        let container = Container::decode(document.as_bytes())
            .map_err(|error| Unverified::Malformed(error.to_string()))?;
        let chain = InvocationChain::try_from(container)
            .map_err(|error| Unverified::Malformed(error.to_string()))?;
        let revocation = RevocationChain::try_from(chain)
            .map_err(|error| Unverified::Malformed(error.to_string()))?;
        let found = revocation.revoked().to_cid();
        if found != certificate {
            return Err(Unverified::Elsewhere { found });
        }
        Ok(Self(revocation))
    }

    /// The principal that signed the revocation.
    pub(crate) fn revoker(&self) -> &Did {
        self.0.revoker()
    }

    /// Verify the revocation, given the principals that stand above the
    /// revoked certificate in the chains retained here.
    ///
    /// Signatures are checked against `did:key` principals, the way the
    /// proof walk checks the certificates it admits; a revocation signed
    /// by any other kind of principal does not verify here. Whether the
    /// revocation's own proofs were revoked is not asked: that is a
    /// question for a revocation index, which is the very party whose word
    /// is being checked.
    pub(crate) async fn verify(&self, entitled: &HashSet<Did>) -> Result<(), Unverified> {
        if !entitled.contains(self.revoker()) {
            return Err(Unverified::NotEntitled {
                revoker: self.revoker().clone(),
            });
        }
        let environment = Environment::new(
            self.0.chain().proof_store(),
            DidKeyResolver,
            UnverifiedRevocations,
        );
        self.0
            .verify::<Runtime, _, _, _>(&VerificationContext::new(&environment))
            .await
            .map_err(|error| Unverified::Invalid(error.to_string()))
    }
}

/// Minting revocations for tests, well-formed or not.
#[cfg(test)]
pub(crate) mod mint {
    use dialog_capability::Did;
    use dialog_common::ConditionalSend;
    use dialog_ucan_core::{Container, Delegation, RevocationBuilder, RevocationChain, Tag};
    use dialog_varsig::{AnySignature, Principal, Signer};
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Arc;

    /// A signer that claims one principal and signs with another's key:
    /// what a revocation whose signature does not verify is made with.
    pub(crate) struct Impostor {
        /// The principal it claims to be.
        pub(crate) claims: Did,
        /// The key it actually signs with.
        pub(crate) signer: dialog_credentials::Signer,
    }

    impl Principal for Impostor {
        fn did(&self) -> Did {
            self.claims.clone()
        }
    }

    impl Signer<AnySignature> for Impostor {
        fn sign(
            &self,
            payload: &[u8],
        ) -> impl Future<Output = Result<AnySignature, signature::Error>> + ConditionalSend
        {
            Signer::<AnySignature>::sign(&self.signer, payload)
        }
    }

    /// A revocation of `revoked` signed by `revoker`, as the container's
    /// DAG-CBOR bytes: what a service is posted and keeps.
    pub(crate) async fn document<I>(revoker: I, revoked: &Delegation<AnySignature>) -> Vec<u8>
    where
        I: Signer<AnySignature> + Principal,
    {
        let certificate = revoked.to_cid();
        let revocation = RevocationBuilder::new(revoker, certificate)
            .try_build()
            .await
            .expect("a revocation builds");
        RevocationChain::assemble(
            revocation,
            HashMap::from([(certificate, Arc::new(revoked.clone()))]),
        )
        .expect("the revocation carries what it names")
        .to_bytes()
        .expect("a revocation encodes")
    }

    /// A container's DAG-CBOR bytes in the base64url tagged form a refusal
    /// carries.
    pub(crate) fn tagged(document: &[u8]) -> String {
        let encoded = Container::from_bytes(document)
            .and_then(|container| container.encode(Tag::Base64Url))
            .expect("a container re-encodes");
        String::from_utf8(encoded).expect("the tagged form is text")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::mint::{Impostor, document, tagged};
    use super::*;
    use dialog_credentials::{Ed25519Signer, Signer};
    use dialog_ucan_core::subject::Subject;
    use dialog_ucan_core::{Delegation, DelegationBuilder};
    use dialog_varsig::Principal as _;

    async fn signer() -> Signer {
        Signer::from(Ed25519Signer::generate().await.expect("a key"))
    }

    /// `issuer` grants `audience` its storage.
    async fn grant(issuer: &Signer, audience: &Signer) -> Delegation<AnySignature> {
        DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience.did())
            .subject(Subject::Specific(issuer.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("a delegation")
    }

    #[dialog_common::test]
    async fn it_verifies_a_revocation_by_a_principal_above_the_link() {
        let space = signer().await;
        let device = signer().await;
        let link = grant(&space, &device).await;

        let claimed = Claimed::decode(
            &tagged(&document(space.clone(), &link).await),
            link.to_cid(),
        )
        .expect("decodes");
        claimed
            .verify(&HashSet::from([space.did()]))
            .await
            .expect("the space may revoke what it granted");
    }

    #[dialog_common::test]
    async fn it_refuses_a_document_that_is_not_a_revocation() {
        let space = signer().await;
        let device = signer().await;
        let link = grant(&space, &device).await;

        assert!(matches!(
            Claimed::decode("Cnot a container", link.to_cid()),
            Err(Unverified::Malformed(_))
        ));
    }

    #[dialog_common::test]
    async fn it_refuses_a_revocation_of_another_certificate() {
        let space = signer().await;
        let device = signer().await;
        let link = grant(&space, &device).await;
        let other = grant(&space, &signer().await).await;

        assert!(matches!(
            Claimed::decode(&tagged(&document(space.clone(), &other).await), link.to_cid()),
            Err(Unverified::Elsewhere { found }) if found == other.to_cid()
        ));
    }

    #[dialog_common::test]
    async fn it_refuses_a_revoker_not_above_the_link() {
        // The device may decline its own grant, and as a revocation that
        // verifies; but it does not stand above the grant, so a remote
        // cannot use its word to have the space's grant retracted.
        let space = signer().await;
        let device = signer().await;
        let link = grant(&space, &device).await;

        for revoker in [device.clone(), signer().await] {
            let claimed = Claimed::decode(
                &tagged(&document(revoker.clone(), &link).await),
                link.to_cid(),
            )
            .expect("decodes");
            assert!(matches!(
                claimed.verify(&HashSet::from([space.did()])).await,
                Err(Unverified::NotEntitled { revoker: found }) if found == revoker.did()
            ));
        }
    }

    #[dialog_common::test]
    async fn it_refuses_a_revocation_not_signed_by_the_principal_it_claims() {
        let space = signer().await;
        let device = signer().await;
        let link = grant(&space, &device).await;
        let forged = document(
            Impostor {
                claims: space.did(),
                signer: signer().await,
            },
            &link,
        )
        .await;

        let claimed = Claimed::decode(&tagged(&forged), link.to_cid()).expect("decodes");
        assert_eq!(claimed.revoker(), &space.did(), "it claims the space");
        assert!(matches!(
            claimed.verify(&HashSet::from([space.did()])).await,
            Err(Unverified::Invalid(_))
        ));
    }

    #[dialog_common::test]
    async fn it_refuses_an_intermediary_without_evidence() {
        // Above the link, but neither its issuer nor its subject: the
        // revocation must carry a witness of the revoker's authority, and
        // this one carries none.
        let space = signer().await;
        let account = signer().await;
        let device = signer().await;
        let link = DelegationBuilder::new()
            .issuer(account.clone())
            .audience(&device.did())
            .subject(Subject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .expect("a delegation");
        let middle = signer().await;

        let claimed = Claimed::decode(
            &tagged(&document(middle.clone(), &link).await),
            link.to_cid(),
        )
        .expect("decodes");
        assert!(matches!(
            claimed
                .verify(&HashSet::from([space.did(), account.did(), middle.did()]))
                .await,
            Err(Unverified::Invalid(_))
        ));
    }
}
