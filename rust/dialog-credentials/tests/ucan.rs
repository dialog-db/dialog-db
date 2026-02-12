#![cfg(feature = "ucan")]
//! Integration tests demonstrating direct UCAN interop.
//!
//! Because `Ed25519Signer` implements `Principal + Signer<Ed25519Signature>`,
//! it automatically satisfies `dialog_ucan::Issuer<Ed25519Signature>` via blanket impl.
//! No adapter types are needed.

use dialog_capability::Principal;
use dialog_credentials::ed25519::{Ed25519KeyResolver, Ed25519Signer};
use dialog_ucan::delegation::builder::DelegationBuilder;
use dialog_ucan::invocation::builder::InvocationBuilder;
use dialog_ucan::subject::Subject;

async fn test_signer(seed: u8) -> Ed25519Signer {
    Ed25519Signer::import(&[seed; 32]).await.unwrap()
}

#[dialog_common::test]
async fn issue_invocation() {
    let signer = test_signer(20).await;
    let audience = test_signer(21).await;
    let resolver = Ed25519KeyResolver;

    let invocation = InvocationBuilder::new()
        .issuer(signer.clone())
        .audience(&audience)
        .subject(&signer)
        .command(vec!["storage".to_string(), "read".to_string()])
        .proofs(vec![])
        .try_build()
        .await
        .expect("should issue invocation");

    invocation
        .verify_signature(&resolver)
        .await
        .expect("invocation signature should verify");

    assert_eq!(
        invocation.issuer().to_string(),
        Principal::did(&signer).to_string(),
    );
}

#[dialog_common::test]
async fn issue_delegation() {
    let signer = test_signer(40).await;
    let audience = test_signer(41).await;
    let resolver = Ed25519KeyResolver;

    let delegation = DelegationBuilder::new()
        .issuer(signer.clone())
        .audience(&audience)
        .subject(Subject::Specific(Principal::did(&signer)))
        .command(vec!["storage".to_string(), "write".to_string()])
        .try_build()
        .await
        .expect("should issue delegation");

    delegation
        .verify_signature(&resolver)
        .await
        .expect("delegation signature should verify");

    assert_eq!(
        delegation.issuer().to_string(),
        Principal::did(&signer).to_string(),
    );
}

#[dialog_common::test]
async fn issue_wildcard_delegation() {
    let signer = test_signer(60).await;
    let audience = test_signer(61).await;
    let resolver = Ed25519KeyResolver;

    let delegation = DelegationBuilder::new()
        .issuer(signer.clone())
        .audience(&audience)
        .subject(Subject::Any)
        .command(vec!["storage".to_string()])
        .try_build()
        .await
        .unwrap();

    assert_eq!(delegation.subject(), &Subject::Any);
    delegation.verify_signature(&resolver).await.unwrap();
}
