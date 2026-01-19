//! Integration tests for UCAN invocation verification.
//!
//! These tests use real Ed25519 cryptography to create valid and invalid
//! UCAN invocations and delegations, then verify the `verify_invocation`
//! function handles them correctly.

use std::collections::BTreeMap;

#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
use web_time::{Duration, SystemTime, UNIX_EPOCH};

use dialog_ucan::{VerificationError, verify_invocation};
use ed25519_dalek::SigningKey;
use ucan::delegation::builder::DelegationBuilder;
use ucan::delegation::subject::DelegatedSubject;
use ucan::did::{Ed25519Did, Ed25519Signer};
use ucan::invocation::builder::InvocationBuilder;
use ucan::time::timestamp::Timestamp;

/// Create a test signer from a seed byte (for deterministic tests).
fn test_signer(seed: u8) -> Ed25519Signer {
    let mut key_bytes = [0u8; 32];
    key_bytes[0] = seed;
    Ed25519Signer::new(SigningKey::from_bytes(&key_bytes))
}

/// Create a timestamp from seconds since UNIX epoch.
fn timestamp_from_secs(secs: u64) -> Timestamp {
    let time = UNIX_EPOCH + Duration::from_secs(secs);
    Timestamp::new(time).expect("valid timestamp")
}

/// Get current unix timestamp as u64.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time after epoch")
        .as_secs()
}

/// Helper to build a valid delegation from subject to operator.
fn build_delegation(
    subject_signer: &Ed25519Signer,
    operator_did: Ed25519Did,
    command: Vec<String>,
    expiration: Option<Timestamp>,
    not_before: Option<Timestamp>,
) -> (Vec<u8>, ipld_core::cid::Cid) {
    let mut builder = DelegationBuilder::new()
        .issuer(subject_signer.clone())
        .audience(operator_did)
        .subject(DelegatedSubject::Specific(*subject_signer.did()))
        .command(command);

    if let Some(exp) = expiration {
        builder = builder.expiration(exp);
    }

    if let Some(nbf) = not_before {
        builder = builder.not_before(nbf);
    }

    let delegation = builder.try_build().expect("Failed to build delegation");
    let bytes = serde_ipld_dagcbor::to_vec(&delegation).expect("Failed to serialize delegation");
    let cid = delegation.to_cid();
    (bytes, cid)
}

/// Helper to build an invocation.
fn build_invocation(
    operator_signer: &Ed25519Signer,
    subject_did: Ed25519Did,
    command: Vec<String>,
    proof_cids: Vec<ipld_core::cid::Cid>,
    expiration: Option<Timestamp>,
) -> Vec<u8> {
    let mut builder = InvocationBuilder::new()
        .issuer(operator_signer.clone())
        .audience(subject_did)
        .subject(subject_did)
        .command(command)
        .arguments(BTreeMap::new())
        .proofs(proof_cids);

    if let Some(exp) = expiration {
        builder = builder.expiration(exp);
    }

    let invocation = builder.try_build().expect("Failed to build invocation");
    serde_ipld_dagcbor::to_vec(&invocation).expect("Failed to serialize invocation")
}

// =============================================================================
// Happy Path Tests
// =============================================================================

#[tokio::test]
async fn test_valid_invocation_with_delegation() {
    // Setup: subject delegates to operator
    let subject_signer = test_signer(1);
    let operator_signer = test_signer(2);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Create delegation: subject -> operator for http/*
    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        None,
    );

    // Create invocation: operator invokes http/get on subject
    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    // Verify
    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(result.is_ok(), "Expected success, got {:?}", result);

    let verified = result.unwrap();
    assert_eq!(verified.command, vec!["http", "get"]);
    assert_eq!(verified.subject, subject_did.to_string());
    assert_eq!(verified.issuer, operator_did.to_string());
}

#[tokio::test]
async fn test_valid_invocation_with_expiration_in_future() {
    let subject_signer = test_signer(3);
    let operator_signer = test_signer(4);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Delegation expires in 1 hour
    let future_exp = timestamp_from_secs(now_secs() + 3600);

    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        Some(future_exp),
        None,
    );

    // Invocation also expires in 1 hour
    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "put".to_string()],
        vec![delegation_cid],
        Some(future_exp),
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(result.is_ok(), "Expected success, got {:?}", result);
}

// =============================================================================
// Expiration Tests
// =============================================================================

#[tokio::test]
async fn test_expired_invocation() {
    let subject_signer = test_signer(5);
    let operator_signer = test_signer(6);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        None,
    );

    // Invocation expired 1 hour ago
    let past_exp = timestamp_from_secs(now_secs().saturating_sub(3600));
    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        Some(past_exp),
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(
        matches!(result, Err(VerificationError::Expired)),
        "Expected Expired error, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_expired_delegation() {
    let subject_signer = test_signer(7);
    let operator_signer = test_signer(8);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Delegation expired 1 hour ago
    let past_exp = timestamp_from_secs(now_secs().saturating_sub(3600));
    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        Some(past_exp),
        None,
    );

    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(
        matches!(result, Err(VerificationError::ProofExpired { index: 0 })),
        "Expected ProofExpired {{ index: 0 }}, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_not_yet_valid_delegation() {
    let subject_signer = test_signer(9);
    let operator_signer = test_signer(10);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Delegation not valid until 1 hour from now
    let future_nbf = timestamp_from_secs(now_secs() + 3600);
    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        Some(future_nbf),
    );

    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(
        matches!(
            result,
            Err(VerificationError::ProofNotYetValid { index: 0 })
        ),
        "Expected ProofNotYetValid {{ index: 0 }}, got {:?}",
        result
    );
}

// =============================================================================
// Audience Mismatch Tests
// =============================================================================

#[tokio::test]
async fn test_audience_mismatch() {
    let subject_signer = test_signer(11);
    let operator_signer = test_signer(12);
    let other_signer = test_signer(13);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();
    let other_did = *other_signer.did();

    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        None,
    );

    // Invocation audience is "other" but subject is "subject" -> mismatch
    let builder = InvocationBuilder::new()
        .issuer(operator_signer.clone())
        .audience(other_did) // Wrong audience!
        .subject(subject_did)
        .command(vec!["http".to_string(), "get".to_string()])
        .arguments(BTreeMap::new())
        .proofs(vec![delegation_cid]);

    let invocation = builder.try_build().expect("Failed to build invocation");
    let invocation_bytes =
        serde_ipld_dagcbor::to_vec(&invocation).expect("Failed to serialize invocation");

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(
        matches!(
            result,
            Err(VerificationError::AudienceMismatch {
                expected: _,
                got: _
            })
        ),
        "Expected AudienceMismatch error, got {:?}",
        result
    );
}

// =============================================================================
// Command Mismatch Tests
// =============================================================================

#[tokio::test]
async fn test_command_mismatch() {
    let subject_signer = test_signer(14);
    let operator_signer = test_signer(15);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Delegation only allows http/get
    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string(), "get".to_string()],
        None,
        None,
    );

    // Invocation tries to do http/put
    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "put".to_string()], // Not allowed!
        vec![delegation_cid],
        None,
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    assert!(
        matches!(
            result,
            Err(VerificationError::CommandMismatch {
                expected: _,
                found: _
            })
        ),
        "Expected CommandMismatch error, got {:?}",
        result
    );
}

// =============================================================================
// Missing Proof Tests
// =============================================================================

#[tokio::test]
async fn test_missing_proof() {
    let subject_signer = test_signer(16);
    let operator_signer = test_signer(17);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    // Create delegation but don't include it in proofs
    let (_, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        None,
    );

    // Invocation references the delegation CID but we don't provide the bytes
    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    // Pass empty proof bytes - the delegation CID won't be found
    let result = verify_invocation(&invocation_bytes, &[]).await;
    assert!(
        matches!(result, Err(VerificationError::ProofNotFound(_))),
        "Expected ProofNotFound error, got {:?}",
        result
    );
}

// =============================================================================
// Parse Error Tests
// =============================================================================

#[tokio::test]
async fn test_invalid_invocation_bytes() {
    let result = verify_invocation(&[0, 1, 2, 3], &[]).await;
    assert!(
        matches!(result, Err(VerificationError::ParseError(_))),
        "Expected ParseError, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_invalid_delegation_bytes() {
    let subject_signer = test_signer(18);
    let operator_signer = test_signer(19);
    let subject_did = *subject_signer.did();
    let operator_did = *operator_signer.did();

    let (_, delegation_cid) = build_delegation(
        &subject_signer,
        operator_did,
        vec!["http".to_string()],
        None,
        None,
    );

    let invocation_bytes = build_invocation(
        &operator_signer,
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    // Pass garbage as delegation bytes
    let result = verify_invocation(&invocation_bytes, &[vec![0, 1, 2, 3]]).await;
    assert!(
        matches!(result, Err(VerificationError::ParseError(_))),
        "Expected ParseError for invalid delegation, got {:?}",
        result
    );
}

// =============================================================================
// Chain Validation Tests
// =============================================================================

#[tokio::test]
async fn test_wrong_issuer_in_delegation() {
    // Subject delegates to operator A, but operator B tries to invoke
    let subject_signer = test_signer(20);
    let operator_a_signer = test_signer(21);
    let operator_b_signer = test_signer(22);
    let subject_did = *subject_signer.did();
    let operator_a_did = *operator_a_signer.did();

    // Delegation is to operator A
    let (delegation_bytes, delegation_cid) = build_delegation(
        &subject_signer,
        operator_a_did,
        vec!["http".to_string()],
        None,
        None,
    );

    // But operator B tries to invoke
    let invocation_bytes = build_invocation(
        &operator_b_signer, // Wrong operator!
        subject_did,
        vec!["http".to_string(), "get".to_string()],
        vec![delegation_cid],
        None,
    );

    let result = verify_invocation(&invocation_bytes, &[delegation_bytes]).await;
    // This should fail because operator B is not authorized by the delegation
    assert!(
        result.is_err(),
        "Expected error for wrong issuer, got {:?}",
        result
    );
}

// =============================================================================
// ServiceError Conversion Tests
// =============================================================================

#[test]
fn test_verification_error_to_service_error_conversions() {
    use dialog_ucan::{ErrorCode, ServiceError};

    // Test ProofExpired conversion
    let err: ServiceError = VerificationError::ProofExpired { index: 2 }.into();
    assert_eq!(err.code, ErrorCode::ProofExpired);
    assert!(err.message.contains("Proof[2]"));
    assert_eq!(err.status_code(), 401);

    // Test ProofNotYetValid conversion
    let err: ServiceError = VerificationError::ProofNotYetValid { index: 1 }.into();
    assert_eq!(err.code, ErrorCode::ProofNotYetValid);
    assert!(err.message.contains("Proof[1]"));
    assert_eq!(err.status_code(), 401);

    // Test SubjectNotAllowed conversion
    let err: ServiceError = VerificationError::SubjectNotAllowed.into();
    assert_eq!(err.code, ErrorCode::SubjectNotAllowed);
    assert_eq!(err.status_code(), 403);

    // Test CommandMismatch conversion
    let err: ServiceError = VerificationError::CommandMismatch {
        expected: vec!["http".to_string(), "get".to_string()],
        found: vec!["http".to_string(), "put".to_string()],
    }
    .into();
    assert_eq!(err.code, ErrorCode::CommandMismatch);
    assert!(err.message.contains("get"));
    assert!(err.message.contains("put"));
    assert_eq!(err.status_code(), 403);

    // Test InvalidIssuerChain conversion
    let err: ServiceError = VerificationError::InvalidIssuerChain.into();
    assert_eq!(err.code, ErrorCode::ChainInvalid);
    assert_eq!(err.status_code(), 403);

    // Test Expired conversion
    let err: ServiceError = VerificationError::Expired.into();
    assert_eq!(err.code, ErrorCode::InvocationExpired);
    assert_eq!(err.status_code(), 401);

    // Test InternalError conversion
    let err: ServiceError = VerificationError::InternalError("test".to_string()).into();
    assert_eq!(err.code, ErrorCode::InternalError);
    assert_eq!(err.status_code(), 500);
}
